//! RTC worker thread startup and terminal shutdown.
//!
//! [`RtcWorker::start`] publishes a worker only after its UDP socket is bound.
//! [`RtcWorker::cancel`] is terminal and does not promise to drain queued command
//! or relay mailboxes. [`RtcWorker::wait_for_shutdown`] keeps the blocking thread
//! join out of the caller's async executor.
#[cfg(target_os = "linux")]
use std::{
    any::Any,
    panic::{AssertUnwindSafe, catch_unwind},
};
use std::{
    net::IpAddr,
    sync::{Arc, Mutex, atomic::Ordering, mpsc as std_mpsc},
    thread,
    time::Instant,
};

use tokio::{
    runtime::Builder as TokioRuntimeBuilder,
    sync::{mpsc, oneshot},
    task::yield_now,
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::{
    super::{
        RtcWorkerConfig, RtpProfile, bootstrap,
        commands::{RtcWorkerCommand, RtcWorkerResponse},
        state::{
            RtcSnapshotState, TransportSessionHealth,
            bitrate::BitrateRegistry,
            relay_registry::{RELAY_MAILBOX_CAPACITY, RelayPacketMailbox, sender_backlog_depth},
        },
    },
    PacketLoopConfig, RtcWorker, RtcWorkerHandle,
};
use crate::{
    Bitrate, MediaWorkerId, RtcPortRange, RtcUdpIoBackend,
    engine::media_transport::{
        ActiveSpeakerSource, MediaTransportConfig, MediaTransportDeps, ReceiverBandwidthSnapshot,
        SourcePolicySignal, TransportAdapterError, TransportBitrateSnapshot,
        TransportHealthSnapshot, TransportMediaId, TransportQualitySnapshot, TransportSessionKey,
        TransportSourceDiagnosticsSnapshot, TransportWorkerPressureSnapshot,
    },
};

struct PacketLoopStartup {
    announced_ip: IpAddr,
    rtc_port_range: RtcPortRange,
    config: PacketLoopConfig,
    bitrate_registry: Arc<Mutex<BitrateRegistry>>,
    snapshot_state: Arc<Mutex<RtcSnapshotState>>,
    inputs: super::PacketLoopInputReceivers,
}

impl PacketLoopStartup {
    async fn run(
        self,
        backend: RtcUdpIoBackend,
        startup_tx: std_mpsc::SyncSender<Result<(), TransportAdapterError>>,
    ) {
        let shared_socket = match bootstrap::bind_shared_rtc_socket(
            self.announced_ip,
            self.rtc_port_range,
            backend,
        ) {
            Ok(shared_socket) => shared_socket,
            Err(error) => {
                let _ = startup_tx.send(Err(error));
                return;
            }
        };
        if startup_tx.send(Ok(())).is_err() {
            // No `RtcWorker` can own this thread after the startup waiter
            // disappears. Returning releases the socket and packet-loop inputs.
            return;
        }
        super::run_packet_loop(
            self.config,
            shared_socket,
            self.bitrate_registry,
            self.snapshot_state,
            self.inputs,
        )
        .await;
    }
}

fn spawn_tokio_packet_loop(
    thread_name: String,
    startup: PacketLoopStartup,
) -> Result<thread::JoinHandle<()>, TransportAdapterError> {
    let (startup_tx, startup_rx) = std_mpsc::sync_channel(1);
    let thread = thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            match TokioRuntimeBuilder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()
            {
                Ok(runtime) => {
                    runtime.block_on(startup.run(RtcUdpIoBackend::Tokio, startup_tx));
                }
                Err(error) => {
                    warn!(?error, "failed to boot rtc packet loop Tokio runtime");
                    let _ = startup_tx.send(Err(TransportAdapterError::TransportUnavailable));
                }
            }
        })
        .map_err(|_error| TransportAdapterError::TransportUnavailable)?;
    wait_for_packet_loop_startup(thread, &startup_rx)
}

#[cfg(target_os = "linux")]
fn spawn_io_uring_packet_loop(
    thread_name: String,
    startup: PacketLoopStartup,
) -> Result<thread::JoinHandle<()>, TransportAdapterError> {
    let (startup_tx, startup_rx) = std_mpsc::sync_channel(1);
    let thread = thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            let startup_err_tx = startup_tx.clone();
            // `tokio_uring::start` unwraps runtime creation. Catch that panic in
            // the worker thread so startup can report failure and join the thread.
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
                tokio_uring::start(startup.run(RtcUdpIoBackend::IoUring, startup_tx));
            })) {
                warn!(
                    panic = panic_message(payload.as_ref()),
                    "rtc packet loop io_uring runtime panicked"
                );
                let _ = startup_err_tx.send(Err(TransportAdapterError::TransportUnavailable));
            }
        })
        .map_err(|_error| TransportAdapterError::TransportUnavailable)?;
    wait_for_packet_loop_startup(thread, &startup_rx)
}

fn wait_for_packet_loop_startup(
    thread: thread::JoinHandle<()>,
    startup_rx: &std_mpsc::Receiver<Result<(), TransportAdapterError>>,
) -> Result<thread::JoinHandle<()>, TransportAdapterError> {
    match startup_rx.recv() {
        Ok(Ok(())) => Ok(thread),
        Ok(Err(error)) => {
            let _ = thread.join();
            Err(error)
        }
        Err(_) => {
            let _ = thread.join();
            Err(TransportAdapterError::TransportUnavailable)
        }
    }
}

#[cfg(target_os = "linux")]
fn panic_message(payload: &(dyn Any + Send)) -> &str {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        message
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.as_str()
    } else {
        "unknown panic payload"
    }
}

impl Drop for RtcWorker {
    fn drop(&mut self) {
        let shutdown_started = self.shutdown.is_cancelled();
        self.shutdown.cancel();
        // A drop that initiates shutdown owns the blocking join. After `cancel`,
        // Drop must stay nonblocking and joins only an already-finished thread.
        // `MediaTransport::shutdown` waits asynchronously before final drop.
        if let Some(thread) = self.thread.take()
            && (!shutdown_started || thread.is_finished())
        {
            let _ = thread.join();
        }
    }
}

impl RtcWorker {
    /// Starts a worker on a dedicated OS thread and returns after its UDP socket binds.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError::TransportUnavailable`] when thread or
    /// runtime creation fails, the selected backend is unavailable or no socket
    /// can bind in `rtc_port_range`.
    pub(crate) fn start(
        config: &MediaTransportConfig,
        profile: Arc<RtpProfile>,
        rtc_port_range: RtcPortRange,
        deps: &MediaTransportDeps,
        source_policy_signal: SourcePolicySignal,
        media_id_base: u64,
        media_worker_id: MediaWorkerId,
    ) -> Result<Self, TransportAdapterError> {
        let relay_target_id =
            super::RelayTargetId::new(super::NEXT_RELAY_TARGET_ID.fetch_add(1, Ordering::Relaxed));
        let (command_tx, command_rx) = mpsc::channel(64);
        #[cfg(any(test, feature = "testing-transport"))]
        let debug_channels = super::super::test_support::RtcWorkerDebugChannels::new();
        let (relay_tx, relay_rx) = mpsc::channel(RELAY_MAILBOX_CAPACITY);
        // observability reads these side channels without entering the packet
        // loop, while authoritative state stays owned by the worker task
        let bitrate_registry = Arc::new(Mutex::new(BitrateRegistry::default()));
        let snapshot_state = Arc::new(Mutex::new(RtcSnapshotState::default()));
        let packet_loop_delay = Arc::new(super::PacketLoopDelaySnapshot::new(Instant::now()));
        let shutdown = CancellationToken::new();
        let handle = RtcWorkerHandle {
            command_tx,
            #[cfg(any(test, feature = "testing-transport"))]
            debug_handle: debug_channels.handle(),
            relay_mailbox: RelayPacketMailbox::new(relay_tx),
            bitrate_registry: Arc::clone(&bitrate_registry),
            snapshot_state: Arc::clone(&snapshot_state),
            packet_loop_delay: Arc::clone(&packet_loop_delay),
        };
        let packet_loop_inputs =
            super::PacketLoopInputReceivers::new(command_rx, relay_rx, shutdown.clone());
        #[cfg(any(test, feature = "testing-transport"))]
        let packet_loop_inputs = debug_channels.install(packet_loop_inputs);
        let metrics = &deps.metrics;
        let rtc_metrics = metrics.register_rtc_worker();
        #[cfg(test)]
        let test_source_policy_signal = source_policy_signal.clone();
        let packet_loop_config = PacketLoopConfig {
            worker: RtcWorkerConfig {
                bitrate_limits: config.bitrate_limits,
                video_bitrate_limits: config.video_bitrate_limits,
                profile,
                media_quality_interval: config.media_quality_interval,
                media_id_base,
            },
            packet_sink_registry: Arc::clone(&deps.packet_sink_registry),
            source_policy_signal,
            metrics: Arc::clone(metrics),
            rtp_metrics: metrics.register_rtp_worker_for_media_worker(media_worker_id.as_usize()),
            rtc_metrics: Arc::clone(&rtc_metrics),
            packet_loop_delay,
        };
        let startup = PacketLoopStartup {
            announced_ip: config.announced_ip,
            rtc_port_range,
            config: packet_loop_config,
            bitrate_registry,
            snapshot_state,
            inputs: packet_loop_inputs,
        };
        let thread_name = format!("rtc-packet-loop-{relay_target_id:?}");
        let thread = match config.rtc_udp_io_backend {
            RtcUdpIoBackend::Tokio => spawn_tokio_packet_loop(thread_name, startup)?,
            RtcUdpIoBackend::IoUring => {
                #[cfg(target_os = "linux")]
                {
                    spawn_io_uring_packet_loop(thread_name, startup)?
                }
                #[cfg(not(target_os = "linux"))]
                {
                    return Err(TransportAdapterError::TransportUnavailable);
                }
            }
        };
        info!(
            ?relay_target_id,
            announced_ip = %config.announced_ip,
            max_bitrate_in_bps = config.bitrate_limits.max_bitrate_in().as_bps(),
            max_bitrate_out_bps = config.bitrate_limits.max_bitrate_out().as_bps(),
            rtc_port_range_min = rtc_port_range.min(),
            rtc_port_range_max = rtc_port_range.max(),
            rtc_udp_io_backend = config.rtc_udp_io_backend.wire_name(),
            "started rtc packet loop worker"
        );
        Ok(Self {
            relay_target_id,
            handle,
            shutdown,
            thread: Some(thread),
            #[cfg(any(test, feature = "testing-transport"))]
            metrics: Arc::clone(metrics),
            rtc_metrics,
            #[cfg(test)]
            source_policy_signal: test_source_policy_signal,
        })
    }

    /// Signals terminal shutdown without waiting.
    ///
    /// An input already selected for the current turn may finish. At the next
    /// wait boundary cancellation wins over commands and relay packets still
    /// queued, so [`Self::cancel`] does not drain either mailbox.
    pub(in crate::engine::media_transport) fn cancel(&self) {
        self.shutdown.cancel();
    }

    /// Waits until the packet loop drops its command receiver and its OS thread exits.
    ///
    /// [`Self::cancel`] must run first during normal operation. The finished
    /// [`thread::JoinHandle`] remains owned by [`Drop`] so this async wait never
    /// blocks the caller's executor.
    pub(in crate::engine::media_transport) async fn wait_for_shutdown(&self) {
        self.handle.command_tx.closed().await;
        if let Some(thread) = &self.thread {
            while !thread.is_finished() {
                yield_now().await;
            }
        }
    }

    /// Enqueues one bounded-mailbox command and waits for its oneshot result.
    ///
    /// Dropping this request future before mailbox delivery completes does not
    /// enqueue the command. Dropping it after enqueue drops only the response
    /// receiver. The packet loop can still apply the command unless worker
    /// cancellation wins first.
    ///
    /// # Errors
    ///
    /// Returns the command handler error or
    /// [`TransportAdapterError::TransportUnavailable`] when command delivery or
    /// response receipt fails.
    pub async fn request_worker<T, F>(&self, build_command: F) -> Result<T, TransportAdapterError>
    where
        F: FnOnce(RtcWorkerResponse<T>) -> RtcWorkerCommand,
    {
        let (response_tx, response_rx) = oneshot::channel();
        self.handle
            .command_tx
            .send(build_command(response_tx))
            .await
            .map_err(|_error| TransportAdapterError::TransportUnavailable)?;
        response_rx
            .await
            .map_err(|_error| TransportAdapterError::TransportUnavailable)?
    }
}

impl RtcWorker {
    /// reads bitrate counters for the requested sessions
    ///
    /// an unavailable registry or missing session contributes no bitrate
    /// callers must treat the result as a recent transport observation rather
    /// than an accounting source of truth
    pub fn transport_bitrate_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot {
        let Ok(bitrate_registry) = self.handle.bitrate_registry.lock() else {
            return TransportBitrateSnapshot::default();
        };
        bitrate_registry.transport_bitrate_snapshot_at(session_keys, Instant::now())
    }

    /// reads receiver bandwidth estimates from the worker snapshot state
    ///
    /// an empty result means the worker has no current estimate or the snapshot
    /// side channel is unavailable
    /// it does not mean the room has no receivers
    pub fn receiver_bandwidth_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> ReceiverBandwidthSnapshot {
        let Ok(snapshot_state) = self.handle.snapshot_state.lock() else {
            return ReceiverBandwidthSnapshot::default();
        };
        snapshot_state.receiver_bandwidth_snapshot(session_keys)
    }

    /// reads sampled media quality from the worker snapshot state
    pub fn transport_quality_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportQualitySnapshot {
        let Ok(snapshot_state) = self.handle.snapshot_state.lock() else {
            return TransportQualitySnapshot::default();
        };
        snapshot_state.transport_quality_snapshot(session_keys)
    }

    /// builds a best-effort pressure snapshot for the whole worker
    pub fn worker_pressure_snapshot(
        &self,
        media_worker_id: MediaWorkerId,
    ) -> TransportWorkerPressureSnapshot {
        let now = Instant::now();
        let egress_bitrate = match self.handle.bitrate_registry.lock() {
            Ok(bitrate_registry) => bitrate_registry.total_egress_bitrate_snapshot_at(now),
            Err(_error) => Bitrate::zero(),
        };
        let command_backlog_depth = sender_backlog_depth(&self.handle.command_tx);
        let relay_mailbox_depth = self.handle.relay_mailbox.backlog_depth();
        TransportWorkerPressureSnapshot {
            media_worker_id,
            egress_bitrate,
            packet_loop_delay_ms: self.handle.packet_loop_delay.packet_loop_delay_ms_at(now),
            command_backlog_depth,
            relay_mailbox_depth,
            worker_pressure_score: worker_pressure_score(
                command_backlog_depth,
                self.handle.command_tx.max_capacity(),
                relay_mailbox_depth,
                RELAY_MAILBOX_CAPACITY,
            ),
        }
    }

    pub fn packet_loop_delay_ms(&self) -> Option<u64> {
        self.handle
            .packet_loop_delay
            .packet_loop_delay_ms_at(Instant::now())
    }

    /// reads the latest transport health side-channel entry for one session
    ///
    /// `None` means the snapshot lock is unavailable or no health event has
    /// been observed for the session
    pub fn session_transport_health(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        let Ok(snapshot_state) = self.handle.snapshot_state.lock() else {
            return None;
        };
        snapshot_state.transport_health(session_key)
    }

    /// Reads transport health for selected sessions under one snapshot lock.
    ///
    /// Missing sessions are omitted and an unavailable snapshot lock returns no facts
    pub fn transport_health_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportHealthSnapshot {
        let Ok(snapshot_state) = self.handle.snapshot_state.lock() else {
            return TransportHealthSnapshot::default();
        };
        snapshot_state.transport_health_snapshot(session_keys)
    }

    /// asks the packet loop for its current active-speaker source snapshot
    ///
    /// this command is read-only but still enters the worker mailbox because
    /// the source activity ordering lives beside route-control state
    /// dispatch failures return an empty snapshot
    pub async fn active_speaker_source_snapshot(&self) -> Vec<ActiveSpeakerSource> {
        self.request_worker(|response| RtcWorkerCommand::ActiveSpeakerSourceSnapshot { response })
            .await
            .unwrap_or_default()
    }

    /// Reads source activity and active-speaker facts in one worker turn.
    ///
    /// Missing sources are omitted and dispatch failure returns no facts
    pub async fn source_diagnostics_snapshot(
        &self,
        transport_media_ids: &[TransportMediaId],
    ) -> TransportSourceDiagnosticsSnapshot {
        self.request_worker(|response| RtcWorkerCommand::SourceDiagnosticsSnapshot {
            transport_media_ids: transport_media_ids.to_vec(),
            response,
        })
        .await
        .unwrap_or_default()
    }
}

/// combines command and relay mailbox saturation for diagnostics
fn worker_pressure_score(
    command_backlog_depth: usize,
    command_capacity: usize,
    relay_mailbox_depth: usize,
    relay_mailbox_capacity: usize,
) -> u8 {
    backlog_pressure_score(command_backlog_depth, command_capacity).max(backlog_pressure_score(
        relay_mailbox_depth,
        relay_mailbox_capacity,
    ))
}

/// converts one bounded mailbox depth into a percentage pressure score
///
/// zero capacity is treated as no pressure because there is no usable divisor
/// and current Tokio bounded mailboxes always report a positive capacity
fn backlog_pressure_score(backlog_depth: usize, capacity: usize) -> u8 {
    if capacity == 0 {
        return 0;
    }
    let score = backlog_depth.saturating_mul(100) / capacity;
    u8::try_from(score.min(100)).unwrap_or(100)
}
