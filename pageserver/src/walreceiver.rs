//! WAL receiver manages an open connection to safekeeper, to get the WAL it streams into.
//! To do so, a current implementation needs to do the following:
//!
//! * acknowledge the timelines that it needs to stream wal into.
//! Pageserver is able to dynamically (un)load tenants on attach and detach,
//! hence WAL receiver needs to react on such events.
//!
//! * determine the way that a timeline needs WAL streaming.
//! For that, it watches specific keys in etcd broker and pulls the relevant data periodically.
//! The data is produced by safekeepers, that push it periodically and pull it to synchronize between each other.
//! Without this data, no WAL streaming is possible currently.
//!
//! Only one active WAL streaming connection is allowed at a time.
//! The connection is supposed to be updated periodically, based on safekeeper timeline data.
//!
//! * handle the actual connection and WAL streaming
//!
//!
//! ## Implementation details
//!
//! WAL receiver's implementation consists of 3 kinds of nested loops, separately handling the logic from the bullets above:
//!
//! * wal receiver main thread, containing the control async loop: timeline addition/removal and interruption of a whole thread handling.
//! The loop can exit with an error, if broker or safekeeper connection attempt limit is reached, with the backpressure mechanism.
//! All of the code inside the loop is either async or a spawn_blocking wrapper around the sync code.
//!
//! * wal receiver broker task, handling the etcd broker interactions, safekeeper selection logic and backpressure.
//! On every concequent broker/wal streamer connection attempt, the loop steps are forced to wait for some time before running,
//! increasing with the number of attempts (capped with 30s).
//!
//! Apart from the broker management, it keeps the wal streaming connection open, with the safekeeper having the most advanced timeline state.
//! The connection could be closed from safekeeper side (with error or not), could be cancelled from pageserver side from time to time.
//!
//! * wal streaming task, opening the libpq conneciton and reading the data out of it to the end, then reporting the result to the broker thread

use crate::config::PageServerConf;
use crate::repository::{Repository, Timeline};
use crate::tenant_mgr::{self, LocalTimelineUpdate, TenantState};
use crate::thread_mgr::ThreadKind;
use crate::walingest::WalIngest;
use crate::{thread_mgr, DatadirTimelineImpl};
use anyhow::{anyhow, bail, ensure, Context, Error, Result};
use bytes::BytesMut;
use fail::fail_point;
use itertools::Itertools;
use postgres_ffi::waldecoder::*;
use postgres_protocol::message::backend::ReplicationMessage;
use postgres_types::PgLsn;
use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroU32;
use std::ops::ControlFlow;
use std::str::FromStr;
use std::sync::Arc;
use std::thread_local;
use std::time::{Duration, SystemTime};
use tokio::{
    pin,
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tokio::{select, time};
use tokio_postgres::replication::ReplicationStream;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage, SimpleQueryRow};
use tokio_stream::StreamExt;
use tracing::*;
use url::Url;

use etcd_broker::{SkTimelineInfo, SkTimelineSubscription, SkTimelineSubscriptionKind};
use utils::{
    lsn::Lsn,
    pq_proto::ZenithFeedback,
    zid::{ZNodeId, ZTenantId, ZTenantTimelineId, ZTimelineId},
};

thread_local! {
    // Boolean that is true only for WAL receiver threads
    //
    // This is used in `wait_lsn` to guard against usage that might lead to a deadlock.
    pub(crate) static IS_WAL_RECEIVER: Cell<bool> = Cell::new(false);
}

pub fn init_wal_receiver_main_thread(
    conf: &'static PageServerConf,
    mut timeline_updates_receiver: mpsc::UnboundedReceiver<LocalTimelineUpdate>,
) -> anyhow::Result<()> {
    let etcd_endpoints = conf.broker_endpoints.clone();
    ensure!(
        !etcd_endpoints.is_empty(),
        "Cannot start wal receiver: etcd endpoints are empty"
    );
    let broker_prefix = &conf.broker_etcd_prefix;
    info!(
        "Starting wal receiver main thread, etdc endpoints: {}",
        etcd_endpoints.iter().map(Url::to_string).join(", ")
    );

    thread_mgr::spawn(
        ThreadKind::WalReceiver,
        None,
        None,
        "WAL receiver main thread",
        true,
        move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .context("Failed to create storage sync runtime")?;
            let mut local_timeline_wal_receivers = HashMap::new();

            loop {
                let loop_step = runtime.block_on(async {
                    select! {
                        // check for shutdown first
                        biased;
                        _ = thread_mgr::shutdown_watcher() => {
                            shutdown_all_wal_connections(&mut local_timeline_wal_receivers).await;
                            ControlFlow::Break(Ok(()))
                        },
                        step = walreceiver_main_thread_loop_step(
                            broker_prefix,
                            &etcd_endpoints,
                            &mut timeline_updates_receiver,
                            &mut local_timeline_wal_receivers,
                        ).instrument(info_span!("walreceiver_main_thread_loop_step")) => step,
                    }
                });

                match loop_step {
                    ControlFlow::Continue(()) => {}
                    ControlFlow::Break(Ok(())) => {
                        info!("Wal receiver main thread stopped successfully");
                        return Ok(());
                    }
                    ControlFlow::Break(Err(e)) => {
                        error!("WAL receiver main thread exited with an error: {e:?}");
                        return Err(e);
                    }
                }
            }
        },
    )
    .context("Failed to spawn wal receiver main thread")
}

async fn walreceiver_main_thread_loop_step<'a>(
    broker_prefix: &str,
    etcd_endpoints: &[Url],
    timeline_updates_receiver: &'a mut mpsc::UnboundedReceiver<LocalTimelineUpdate>,
    local_timeline_wal_receivers: &'a mut HashMap<
        ZTenantId,
        HashMap<ZTimelineId, TimelineWalReceiverHandles>,
    >,
) -> ControlFlow<anyhow::Result<()>, ()> {
    match timeline_updates_receiver.recv().await {
        Some(update) => {
            info!("Processing timeline update: {update:?}");
            match update {
                LocalTimelineUpdate::Remove(id) => {
                    match local_timeline_wal_receivers.get_mut(&id.tenant_id) {
                        Some(timelines) => {
                            if let Some(handles) = timelines.remove(&id.timeline_id) {
                                let wal_receiver_shutdown_result = handles.shutdown(id).await;
                                if wal_receiver_shutdown_result.is_err() {
                                    return ControlFlow::Break(wal_receiver_shutdown_result)
                                }
                            };
                            if timelines.is_empty() {
                                let state_change_result = change_tenant_state(id.tenant_id, TenantState::Idle).await;
                                if state_change_result.is_err() {
                                    return ControlFlow::Break(state_change_result)
                                }
                            }
                        }
                        None => warn!("Timeline {id} does not have a tenant entry in wal receiver main thread"),
                    };
                }
                LocalTimelineUpdate::Add(new_id, timeline) => {
                    let timelines = local_timeline_wal_receivers
                        .entry(new_id.tenant_id)
                        .or_default();

                    if timelines.is_empty() {
                        let state_change_result =
                            change_tenant_state(new_id.tenant_id, TenantState::Active).await;
                        if state_change_result.is_err() {
                            return ControlFlow::Break(state_change_result);
                        }
                    } else if let Some(old_entry_handles) = timelines.remove(&new_id.timeline_id) {
                        warn!(
                            "Readding to an existing timeline {new_id}, shutting the old wal receiver down"
                        );
                        let wal_receiver_shutdown_result = old_entry_handles.shutdown(new_id).await;
                        if wal_receiver_shutdown_result.is_err() {
                            return ControlFlow::Break(wal_receiver_shutdown_result);
                        }
                    }

                    let (connect_timeout, max_retries) =
                        match fetch_tenant_settings(new_id.tenant_id).await {
                            Ok(settings) => settings,
                            Err(e) => return ControlFlow::Break(Err(e)),
                        };

                    let mut connection_failed = true;
                    for attempt in 0..max_retries.get() {
                        backpressure(attempt, 2.0, 30.0).await;
                        match start_timeline_wal_broker(
                            broker_prefix,
                            etcd_endpoints,
                            new_id,
                            &timeline,
                            connect_timeout,
                        )
                        .await
                        {
                            Ok(new_handles) => {
                                timelines.insert(new_id.timeline_id, new_handles);
                                connection_failed = false;
                                break;
                            }
                            Err(e) => {
                                warn!("Failed to start timeline {new_id} wal receiver task: {e:#}")
                            }
                        }
                    }

                    if connection_failed {
                        return ControlFlow::Break(Err(anyhow!(
                            "Failed to start timeline {new_id} wal receiver task after {max_retries} attempts"
                        )));
                    }
                }
            }
        }
        None => {
            info!("Local timeline update channel closed, shutting down all wal connections");
            shutdown_all_wal_connections(local_timeline_wal_receivers).await;
            return ControlFlow::Break(Ok(()));
        }
    }

    ControlFlow::Continue(())
}

async fn fetch_tenant_settings(tenant_id: ZTenantId) -> anyhow::Result<(Duration, NonZeroU32)> {
    tokio::task::spawn_blocking(move || {
        let repo = tenant_mgr::get_repository_for_tenant(tenant_id)
            .with_context(|| format!("no repository found for tenant {tenant_id}"))?;
        Ok::<_, anyhow::Error>((
            repo.get_walreceiver_connect_timeout(),
            repo.get_max_walreceiver_connect_attempts(),
        ))
    })
    .await
    .with_context(|| format!("Failed to join on tenant {tenant_id} settings fetch task"))?
}

async fn change_tenant_state(tenant_id: ZTenantId, new_state: TenantState) -> anyhow::Result<()> {
    tokio::task::spawn_blocking(move || {
        tenant_mgr::set_tenant_state(tenant_id, new_state)
            .with_context(|| format!("Failed to activate tenant {tenant_id}"))
    })
    .await
    .with_context(|| format!("Failed to spawn activation task for tenant {tenant_id}"))?
}

async fn backpressure(n: u32, base: f64, max_seconds: f64) {
    if n == 0 {
        return;
    }
    let seconds_to_wait = base.powf(f64::from(n) - 1.0).min(max_seconds);
    info!("Backpressure: waiting {seconds_to_wait} seconds before proceeding with the task");
    tokio::time::sleep(Duration::from_secs_f64(seconds_to_wait)).await;
}

async fn shutdown_all_wal_connections(
    local_timeline_wal_receivers: &mut HashMap<
        ZTenantId,
        HashMap<ZTimelineId, TimelineWalReceiverHandles>,
    >,
) {
    let mut broker_join_handles = Vec::new();
    for (tenant_id, timelines) in local_timeline_wal_receivers.drain() {
        for (timeline_id, handles) in timelines {
            handles.cancellation_sender.send(()).ok();
            broker_join_handles.push((
                ZTenantTimelineId::new(tenant_id, timeline_id),
                handles.broker_join_handle,
            ));
        }
    }

    let mut tenants = HashSet::with_capacity(broker_join_handles.len());
    for (id, broker_join_handle) in broker_join_handles {
        tenants.insert(id.tenant_id);
        debug!("Waiting for wal broker for timeline {id} to finish");
        if let Err(e) = broker_join_handle.await {
            error!("Failed to join on wal broker for timeline {id}: {e}");
        }
    }
    if let Err(e) = tokio::task::spawn_blocking(move || {
        for tenant_id in tenants {
            if let Err(e) = tenant_mgr::set_tenant_state(tenant_id, TenantState::Idle) {
                error!("Failed to make tenant {tenant_id} idle: {e:?}");
            }
        }
    })
    .await
    {
        error!("Failed to spawn a task to make all tenants idle: {e:?}");
    }
}

struct TimelineWalReceiverHandles {
    broker_join_handle: JoinHandle<anyhow::Result<()>>,
    cancellation_sender: watch::Sender<()>,
}

impl TimelineWalReceiverHandles {
    async fn shutdown(self, id: ZTenantTimelineId) -> anyhow::Result<()> {
        self.cancellation_sender.send(()).context(
            "Unexpected: cancellation sender is dropped before the receiver in the loop is",
        )?;
        debug!("Waiting for wal receiver for timeline {id} to finish");
        self.broker_join_handle
            .await
            .with_context(|| format!("Failed to join the wal reveiver broker for timeline {id}"))?
            .with_context(|| format!("Wal reveiver broker for timeline {id} failed to finish"))
    }
}

async fn start_timeline_wal_broker(
    broker_prefix: &str,
    etcd_endpoints: &[Url],
    id: ZTenantTimelineId,
    timeline: &Arc<DatadirTimelineImpl>,
    connect_timeout: Duration,
) -> anyhow::Result<TimelineWalReceiverHandles> {
    let mut etcd_client = etcd_broker::Client::connect(etcd_endpoints, None)
        .await
        .context("Failed to connect to etcd")?;
    let (cancellation_sender, mut cancellation_receiver) = watch::channel(());

    let mut subscription = etcd_broker::subscribe_to_safekeeper_timeline_updates(
        &mut etcd_client,
        SkTimelineSubscriptionKind::timeline(broker_prefix.to_owned(), id),
    )
    .await
    .with_context(|| format!("Failed to subscribe for timeline {id} updates in etcd"))?;

    let timeline = Arc::clone(timeline);
    let broker_join_handle = tokio::spawn(async move {
        info!("WAL receiver broker thread had started");

        let mut wal_receiver_connection = None::<WalReceiverConnection>;
        let (walreceiver_task_completion_sender, mut walreceiver_task_completion) =
            watch::channel(Ok(()));
        let walreceiver_task_completion_sender = Arc::new(walreceiver_task_completion_sender);
        let mut connection_attempt = 0;

        loop {
            select! {
                // TODO kb add here the walreceiver and the rest

                // check for shutdown first
                biased;
                _ = cancellation_receiver.changed() => {
                    info!("Loop cancelled, shutting down");
                    break;
                }
                walreceiver_result = walreceiver_task_completion.changed() => {
                    match walreceiver_result {
                        Ok(()) => {
                            info!("WAL receiver task finished, reconnecting");
                            connection_attempt = 0;
                        }
                        Err(e) => {
                            error!("WAL receiver task failed: {e:#}, reconnecting");
                            connection_attempt += 1;
                        }
                    }
                    if let Some(connection) = wal_receiver_connection.take() {
                        if let Err(e) = connection.shutdown().await {
                            error!("Failed to shutdown wal receiver connection for timeline {id}: {e:#}");
                        }
                    }
                }
                updates = subscription.fetch_data() => {
                    if let Some(safekeeper_timeline_updates) = updates.and_then(|mut updates| updates.remove(&id)) {
                        consider_walreceiver_connection(id, safekeeper_timeline_updates, &walreceiver_task_completion_sender,
                            &timeline, connection_attempt, &mut wal_receiver_connection, connect_timeout).await;
                    } else {
                        info!("Subscription source end was dropped, no more updates are possible, shutting down");
                        break;
                    }
                },
            }
        }

        shutdown_walreceiver(subscription, wal_receiver_connection).await.with_context(|| format!("Failed to shutdown walreceiver for timeline {id}"))
    }.instrument(info_span!("timeline_walreceiver", id = %id)));

    Ok(TimelineWalReceiverHandles {
        broker_join_handle,
        cancellation_sender,
    })
}

async fn shutdown_walreceiver(
    subscription: SkTimelineSubscription,
    wal_receiver_connection: Option<WalReceiverConnection>,
) -> anyhow::Result<()> {
    subscription
        .cancel()
        .await
        .context("Failed to cancel timeline subscription in etcd")?;
    if let Some(connection) = wal_receiver_connection {
        connection
            .shutdown()
            .await
            .context("Failed to shutdown wal receiver connection")?;
    }
    Ok(())
}

async fn consider_walreceiver_connection(
    id: ZTenantTimelineId,
    safekeeper_timelines: HashMap<ZNodeId, SkTimelineInfo>,
    walreceiver_task_completion_sender: &Arc<watch::Sender<anyhow::Result<()>>>,
    timeline: &DatadirTimelineImpl,
    connection_attempt: u32,
    connection: &mut Option<WalReceiverConnection>,
    connect_timeout: Duration,
) {
    if let Some((new_safekeeper_id, new_safekeeper_timeline)) = safekeeper_timelines
        .into_iter()
        .max_by_key(|(_, info)| info.flush_lsn)
        .filter(|(_, info)| info.flush_lsn > Some(timeline.tline.get_disk_consistent_lsn()))
    {
        if let Some((new_safekeeper_connstr, new_pageserver_connstr)) = new_safekeeper_timeline
            .safekeeper_connstr
            .zip(new_safekeeper_timeline.pageserver_connstr)
        {
            let new_wal_receiver_connstr = match wal_stream_connection_string(
                id,
                &new_safekeeper_connstr,
                &new_pageserver_connstr,
            ) {
                Ok(connstr) => connstr,
                Err(e) => {
                    error!("Failed to create wal receiver connection string: {e:#}");
                    return;
                }
            };

            let should_connect = match connection {
                Some(existing_connection) => {
                    if existing_connection.safekeeper_id != new_safekeeper_id
                        || existing_connection.wal_receiver_connstr != new_wal_receiver_connstr
                    {
                        info!(
                            "Switching to different safekeeper {new_safekeeper_id} for timeline {id}",
                        );
                        if let Some(existing_connection) = connection.take() {
                            if let Err(e) = existing_connection.shutdown().await {
                                error!(
                                    "Failed to shutdown walreceiver connection for timeline {id}: {e:#}"
                                );
                            }
                        }
                        true
                    } else {
                        false
                    }
                }
                None => true,
            };

            if should_connect {
                backpressure(connection_attempt, 2.0, 30.0).await;
                let (active_wal_receiver, wal_receiver_cancellation) = spawn_walreceiver_task(
                    id,
                    new_wal_receiver_connstr.clone(),
                    Arc::clone(walreceiver_task_completion_sender),
                    connect_timeout,
                );
                *connection = Some(WalReceiverConnection {
                    wal_receiver_connstr: new_wal_receiver_connstr,
                    safekeeper_id: new_safekeeper_id,
                    wal_receiver_cancellation,
                    active_wal_receiver,
                });
            }
        }
    }
}

fn wal_stream_connection_string(
    ZTenantTimelineId {
        tenant_id,
        timeline_id,
    }: ZTenantTimelineId,
    listen_pg_addr_str: &str,
    pageserver_connstr: &str,
) -> anyhow::Result<String> {
    let sk_connstr = format!("postgresql://no_user@{listen_pg_addr_str}/no_db");
    let me_conf = sk_connstr
        .parse::<postgres::config::Config>()
        .with_context(|| {
            format!("Failed to parse pageserver connection string '{sk_connstr}' as a postgres one")
        })?;
    let (host, port) = utils::connstring::connection_host_port(&me_conf);
    Ok(format!(
        "host={host} port={port} options='-c ztimelineid={timeline_id} ztenantid={tenant_id} pageserver_connstr={pageserver_connstr}'",
    ))
}

fn spawn_walreceiver_task(
    id: ZTenantTimelineId,
    wal_producer_connstr: String,
    walreceiver_task_completion_sender: Arc<watch::Sender<anyhow::Result<()>>>,
    connect_timeout: Duration,
) -> (JoinHandle<()>, watch::Sender<()>) {
    let (cancellation_sender, mut cancellation) = watch::channel(());
    let join_handle = tokio::spawn(
        async move {
            IS_WAL_RECEIVER.with(|c| c.set(true));
            let processed_connection = handle_walreceiver_connection(
                id,
                &wal_producer_connstr,
                &mut cancellation,
                connect_timeout,
            )
            .await;
            match &processed_connection {
                Ok(()) => debug!("WAL receiver connection got closed"),
                Err(e) => error!("WAL receiver connection failed: {e:#}"),
            }
            if let Err(e) = walreceiver_task_completion_sender.send(processed_connection) {
                error!("Failed to send wal receiver task completion: {e}");
            }
        }
        .instrument(info_span!("handle_walreceiver_connection")),
    );

    (join_handle, cancellation_sender)
}

//
// We keep one WAL Receiver active per timeline.
//
struct WalReceiverConnection {
    wal_receiver_connstr: String,
    safekeeper_id: ZNodeId,
    wal_receiver_cancellation: watch::Sender<()>,
    active_wal_receiver: JoinHandle<()>,
}

impl WalReceiverConnection {
    async fn shutdown(self) -> anyhow::Result<()> {
        self.wal_receiver_cancellation
            .send(())
            .context("Unexpected: cancellation receiver is dropped before the connection is")?;
        self.active_wal_receiver.await?;
        Ok(())
    }
}

async fn handle_walreceiver_connection(
    ZTenantTimelineId {
        tenant_id,
        timeline_id,
    }: ZTenantTimelineId,
    wal_producer_connstr: &str,
    cancellation: &mut watch::Receiver<()>,
    connect_timeout: Duration,
) -> anyhow::Result<()> {
    // Connect to the database in replication mode.
    info!("connecting to {wal_producer_connstr}");
    let connect_cfg =
        format!("{wal_producer_connstr} application_name=pageserver replication=true");

    let (mut replication_client, connection) = time::timeout(
        connect_timeout,
        tokio_postgres::connect(&connect_cfg, NoTls),
    )
    .await
    .context("Timed out while waiting for walreceiver connection to open")?
    .context("Failed to open walreceiver conection")?;
    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        match time::timeout(connect_timeout, connection).await {
            Ok(Ok(())) => debug!("Walreceiver db connection closed"),
            Ok(Err(connection_error)) => error!("Connection error: {connection_error}"),
            Err(_timeout_error) => error!("Timeout error while waiting for the connection"),
        }
    });
    info!("connected!");

    // Immediately increment the gauge, then create a job to decrement it on thread exit.
    // One of the pros of `defer!` is that this will *most probably*
    // get called, even in presence of panics.
    let gauge = crate::LIVE_CONNECTIONS_COUNT.with_label_values(&["wal_receiver"]);
    gauge.inc();
    scopeguard::defer! {
        gauge.dec();
    }

    let identify = identify_system(&mut replication_client).await?;
    info!("{identify:?}");
    let end_of_wal = Lsn::from(u64::from(identify.xlogpos));
    let mut caught_up = false;

    let (repo, timeline) = tokio::task::spawn_blocking(move || {
        let repo = tenant_mgr::get_repository_for_tenant(tenant_id)
            .with_context(|| format!("no repository found for tenant {tenant_id}"))?;
        let timeline = tenant_mgr::get_local_timeline_with_load(tenant_id, timeline_id)
            .with_context(|| {
                format!("local timeline {timeline_id} not found for tenant {tenant_id}")
            })?;
        Ok::<_, anyhow::Error>((repo, timeline))
    })
    .await
    .with_context(|| format!("Failed to spawn blocking task to get repository and timeline for tenant {tenant_id} timeline {timeline_id}"))??;

    //
    // Start streaming the WAL, from where we left off previously.
    //
    // If we had previously received WAL up to some point in the middle of a WAL record, we
    // better start from the end of last full WAL record, not in the middle of one.
    let mut last_rec_lsn = timeline.get_last_record_lsn();
    let mut startpoint = last_rec_lsn;

    if startpoint == Lsn(0) {
        bail!("No previous WAL position");
    }

    // There might be some padding after the last full record, skip it.
    startpoint += startpoint.calc_padding(8u32);

    info!("last_record_lsn {last_rec_lsn} starting replication from {startpoint}, server is at {end_of_wal}...");

    let query = format!("START_REPLICATION PHYSICAL {startpoint}");

    let copy_stream = replication_client.copy_both_simple(&query).await?;
    let physical_stream = ReplicationStream::new(copy_stream);
    pin!(physical_stream);

    let mut waldecoder = WalStreamDecoder::new(startpoint);

    let mut walingest = WalIngest::new(timeline.as_ref(), startpoint)?;

    while let Some(replication_message) = {
        select! {
            // check for shutdown first
            biased;
            _ = cancellation.changed() => {
                info!("walreceiver interrupted");
                None
            }
            replication_message = physical_stream.next() => replication_message,
        }
    } {
        let replication_message = replication_message?;
        let status_update = match replication_message {
            ReplicationMessage::XLogData(xlog_data) => {
                // Pass the WAL data to the decoder, and see if we can decode
                // more records as a result.
                let data = xlog_data.data();
                let startlsn = Lsn::from(xlog_data.wal_start());
                let endlsn = startlsn + data.len() as u64;

                trace!("received XLogData between {startlsn} and {endlsn}");

                waldecoder.feed_bytes(data);

                while let Some((lsn, recdata)) = waldecoder.poll_decode()? {
                    let _enter = info_span!("processing record", lsn = %lsn).entered();

                    // It is important to deal with the aligned records as lsn in getPage@LSN is
                    // aligned and can be several bytes bigger. Without this alignment we are
                    // at risk of hitting a deadlock.
                    anyhow::ensure!(lsn.is_aligned());

                    walingest.ingest_record(&timeline, recdata, lsn)?;

                    fail_point!("walreceiver-after-ingest");

                    last_rec_lsn = lsn;
                }

                if !caught_up && endlsn >= end_of_wal {
                    info!("caught up at LSN {endlsn}");
                    caught_up = true;
                }

                timeline.tline.check_checkpoint_distance()?;

                Some(endlsn)
            }

            ReplicationMessage::PrimaryKeepAlive(keepalive) => {
                let wal_end = keepalive.wal_end();
                let timestamp = keepalive.timestamp();
                let reply_requested = keepalive.reply() != 0;

                trace!("received PrimaryKeepAlive(wal_end: {wal_end}, timestamp: {timestamp:?} reply: {reply_requested})");

                if reply_requested {
                    Some(last_rec_lsn)
                } else {
                    None
                }
            }

            _ => None,
        };

        if let Some(last_lsn) = status_update {
            let remote_index = repo.get_remote_index();
            let timeline_remote_consistent_lsn = remote_index
                .read()
                .await
                // here we either do not have this timeline in remote index
                // or there were no checkpoints for it yet
                .timeline_entry(&ZTenantTimelineId {
                    tenant_id,
                    timeline_id,
                })
                .map(|remote_timeline| remote_timeline.metadata.disk_consistent_lsn())
                // no checkpoint was uploaded
                .unwrap_or(Lsn(0));

            // The last LSN we processed. It is not guaranteed to survive pageserver crash.
            let write_lsn = u64::from(last_lsn);
            // `disk_consistent_lsn` is the LSN at which page server guarantees local persistence of all received data
            let flush_lsn = u64::from(timeline.tline.get_disk_consistent_lsn());
            // The last LSN that is synced to remote storage and is guaranteed to survive pageserver crash
            // Used by safekeepers to remove WAL preceding `remote_consistent_lsn`.
            let apply_lsn = u64::from(timeline_remote_consistent_lsn);
            let ts = SystemTime::now();

            // Send zenith feedback message.
            // Regular standby_status_update fields are put into this message.
            let zenith_status_update = ZenithFeedback {
                current_timeline_size: timeline.get_current_logical_size() as u64,
                ps_writelsn: write_lsn,
                ps_flushlsn: flush_lsn,
                ps_applylsn: apply_lsn,
                ps_replytime: ts,
            };

            debug!("zenith_status_update {zenith_status_update:?}");

            let mut data = BytesMut::new();
            zenith_status_update.serialize(&mut data)?;
            physical_stream
                .as_mut()
                .zenith_status_update(data.len() as u64, &data)
                .await?;
        }
    }

    Ok(())
}

/// Data returned from the postgres `IDENTIFY_SYSTEM` command
///
/// See the [postgres docs] for more details.
///
/// [postgres docs]: https://www.postgresql.org/docs/current/protocol-replication.html
#[derive(Debug)]
// As of nightly 2021-09-11, fields that are only read by the type's `Debug` impl still count as
// unused. Relevant issue: https://github.com/rust-lang/rust/issues/88900
#[allow(dead_code)]
struct IdentifySystem {
    systemid: u64,
    timeline: u32,
    xlogpos: PgLsn,
    dbname: Option<String>,
}

/// There was a problem parsing the response to
/// a postgres IDENTIFY_SYSTEM command.
#[derive(Debug, thiserror::Error)]
#[error("IDENTIFY_SYSTEM parse error")]
struct IdentifyError;

/// Run the postgres `IDENTIFY_SYSTEM` command
async fn identify_system(client: &mut Client) -> Result<IdentifySystem, Error> {
    let query_str = "IDENTIFY_SYSTEM";
    let response = client.simple_query(query_str).await?;

    // get(N) from row, then parse it as some destination type.
    fn get_parse<T>(row: &SimpleQueryRow, idx: usize) -> Result<T, IdentifyError>
    where
        T: FromStr,
    {
        let val = row.get(idx).ok_or(IdentifyError)?;
        val.parse::<T>().or(Err(IdentifyError))
    }

    // extract the row contents into an IdentifySystem struct.
    // written as a closure so I can use ? for Option here.
    if let Some(SimpleQueryMessage::Row(first_row)) = response.get(0) {
        Ok(IdentifySystem {
            systemid: get_parse(first_row, 0)?,
            timeline: get_parse(first_row, 1)?,
            xlogpos: get_parse(first_row, 2)?,
            dbname: get_parse(first_row, 3).ok(),
        })
    } else {
        Err(IdentifyError.into())
    }
}
