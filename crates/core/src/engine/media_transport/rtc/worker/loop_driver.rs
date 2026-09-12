//! Turn ordering for one worker-local RTC loop.
//!
//! [`PacketLoopTurn`] retains reusable buffers while [`PacketLoopState`] owns
//! session, media and route facts. Socket receives enter through the spawned
//! [`UdpIngress`] task. All mutation stays on the worker thread.
//!
//! ```text
//! apply one command, timeout or UDP datagram
//!   -> drain dirty and timed-out sessions at the turn timestamp
//!   -> drain the bounded relay mailbox and resolve consumer feedback
//!   -> for each packet: observe facts -> plan -> flush
//!   -> finish batch observations and policy notifications
//!   -> dispatch retries still pending after packet observation
//!   -> flush staged UDP output
//!   -> wait for one input, prioritizing shutdown and commands
//! ```
//!
//! Each packet finishes before another can change decoder readiness. Origin
//! sinks precede relays and local destinations. A selected keyframe cannot
//! admit an earlier delta packet from the same batch. Batch completion runs
//! once and keyframe observations can clear retries before they become effects.
//!
//! Successful ingress marks its session for this turn's drain. Local fanout
//! occurs after draining and marks destination sessions for the next turn.
//! A relay wake stages one packet for the next bounded media drain.
//! [`super::session_drain`] owns indivisible polling and budget rollback.
//!
//! Snapshots, metrics, bitrate counters and source-policy signals expose
//! observations without granting callers access to authoritative route state.

use std::{
    mem::take,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use tokio::{
    sync::mpsc,
    time::{sleep_until, timeout},
};
use tracing::warn;

#[cfg(feature = "internal-benchmarks")]
use super::super::recovery::PendingKeyframeRequest;
use super::{
    super::{
        RtcWorkerConfig,
        control::WorkerCommandContext,
        packet_loop::{
            ForwardingEffects, PacketForwarder, drain_relay_packets,
            forwarded_packet::ForwardedPacket,
            ingress_routing::{PacketRouteDatagram, route_pkt_to_session_at},
            routing_miss::DemuxRecoveryState,
            udp::{RtcUdpSocket, UdpDatagram, UdpIngress},
        },
        recovery::{drain_due_kf_retries, flush_pending_kf_reqs_at},
        state::{PacketLoopState, RtcSnapshotState, SharedRtcSocket, bitrate::BitrateRegistry},
    },
    buffers::{MAX_RELAY_PACKETS_PER_ITERATION, PacketLoopBuffers},
    delay::{PacketLoopDelayPublisher, PacketLoopDelaySnapshot},
    input::{PacketLoopControlInput, PacketLoopInputReceivers, PacketLoopMailboxInput},
    session_drain::{SessionDrainContext, drain_ready_sessions},
};
#[cfg(feature = "internal-benchmarks")]
use crate::engine::media_transport::TransportSessionKey;
use crate::engine::{
    media_transport::SourcePolicySignal,
    metrics::{RtcMetricsRecorder, RtpMetricsRecorder, RuntimeMetrics},
    packet_sink_registry::RoomPacketSinkRegistry,
};

/// immutable configuration and shared side channels for one packet-loop worker
///
/// the config is built by `RtcWorker` when the worker is booted
/// values copied into session creation are immutable worker settings
/// `Arc` fields are shared services that the packet loop may update or query
/// without exposing direct access to `PacketLoopState`
pub struct PacketLoopConfig {
    pub worker: RtcWorkerConfig,
    pub packet_sink_registry: Arc<RoomPacketSinkRegistry>,
    pub source_policy_signal: SourcePolicySignal,
    pub metrics: Arc<RuntimeMetrics>,
    pub rtp_metrics: Arc<RtpMetricsRecorder>,
    pub rtc_metrics: Arc<RtcMetricsRecorder>,
    pub packet_loop_delay: Arc<PacketLoopDelaySnapshot>,
}

/// packet-loop info allowed to cross into the async wait phase
///
/// `PacketLoopTurn::pump` builds this after mutable `PacketLoopState` access is
/// finished
/// the driver can then await with only the next internal wakeup deadline
///
/// keeping this type narrow makes it visible when a future change tries to
/// carry session state or demux recovery state across `.await`
pub(crate) struct WaitPhaseSnapshot {
    pub next_timeout: Option<Instant>,
}

pub(crate) enum PacketLoopTurnInput {
    Timeout,
    Control(PacketLoopControlInput),
    RelayPacket,
    Datagram(UdpDatagram),
}

#[cfg(feature = "internal-benchmarks")]
pub(crate) struct BenchmarkTurnInput {
    pub packets: Vec<ForwardedPacket>,
    pub keyframe_requests: Vec<(TransportSessionKey, PendingKeyframeRequest)>,
    pub now: Instant,
}

/// private contract for one packet-loop turn
///
/// the turn owns the allocation surface and fairness budgets used by the driver
/// durable RTC state stays in `PacketLoopState` while async waits happen only
/// after the turn has released mutable worker-state borrows
pub(crate) struct PacketLoopTurn {
    buffers: PacketLoopBuffers,
    forwarder: PacketForwarder,
    delay_publisher: PacketLoopDelayPublisher,
    ready_now_budget: usize,
    udp_burst_budget: usize,
}

/// borrowed state needed to apply the input selected by the wait phase
///
/// grouping these borrows keeps the ingress function signatures small while
/// making it clear that no await happens while the context exists
pub(crate) struct PacketLoopApplyContext<'a> {
    pub packet_loop_state: &'a mut PacketLoopState,
    pub bitrate_registry: &'a Arc<Mutex<BitrateRegistry>>,
    pub snapshot_state: &'a Arc<Mutex<RtcSnapshotState>>,
    pub candidate_addr: SocketAddr,
    pub config: &'a PacketLoopConfig,
    pub demux: &'a mut DemuxRecoveryState,
    pub ingress: &'a UdpIngress,
}

impl PacketLoopTurn {
    pub fn new(started_at: Instant) -> Self {
        Self {
            buffers: PacketLoopBuffers::new(),
            forwarder: PacketForwarder::default(),
            delay_publisher: PacketLoopDelayPublisher::new(started_at),
            ready_now_budget: MAX_READY_NOW_INPUTS_BEFORE_YIELD,
            udp_burst_budget: MAX_UDP_DATAGRAMS_PER_TURN,
        }
    }

    /// Keeps pump-phase [`PacketLoopState`] mutation outside every async wait.
    pub fn pump(
        &mut self,
        state: &mut PacketLoopState,
        bitrate_registry: &Arc<Mutex<BitrateRegistry>>,
        snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
        config: &PacketLoopConfig,
        demux: &mut DemuxRecoveryState,
        inputs: &mut PacketLoopInputReceivers,
    ) -> WaitPhaseSnapshot {
        self.buffers.clear();
        // The relay wake precedes ready-session output in this turn's packet batch.
        if let Some(packet) = inputs.take_woken_relay_packet() {
            self.buffers.pending_packets.push(packet);
        }
        let (snapshot, topology_changed) = self.pump_core(
            state,
            bitrate_registry,
            snapshot_state,
            config,
            inputs.relay_rx(),
            Instant::now(),
        );
        if topology_changed {
            demux.clear_on_topology_change();
        }
        snapshot
    }

    /// benchmark variant of [`Self::pump`] that stages synthetic ingress before
    /// the production phase order runs
    ///
    /// scenario benchmarks drive the real turn phases instead of reimplementing
    /// their order, so the measured sequence cannot drift from production. the
    /// staged packets and keyframe requests stand in for the ingress and
    /// consumer feedback that a socket-free fixture cannot produce; they enter
    /// the same batch the relay mailbox and ready-session drain feed
    #[cfg(feature = "internal-benchmarks")]
    pub fn pump_for_benchmark(
        &mut self,
        state: &mut PacketLoopState,
        bitrate_registry: &Arc<Mutex<BitrateRegistry>>,
        snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
        config: &PacketLoopConfig,
        relay_rx: &mut mpsc::Receiver<ForwardedPacket>,
        input: BenchmarkTurnInput,
    ) {
        self.buffers.clear();
        self.buffers.pending_packets.extend(input.packets);
        self.buffers
            .pending_keyframe_requests
            .extend(input.keyframe_requests);
        let _ = self.pump_core(
            state,
            bitrate_registry,
            snapshot_state,
            config,
            relay_rx,
            input.now,
        );
    }

    /// returns the packets a benchmark turn staged and observed back to the
    /// fixture
    ///
    /// the fixture recycles these into its reusable packet slots for the next
    /// tick
    #[cfg(feature = "internal-benchmarks")]
    pub fn take_packets_for_benchmark(&mut self) -> Vec<ForwardedPacket> {
        take(&mut self.buffers.pending_packets)
    }

    /// reports and clears the forwarding destinations planned since the last read
    ///
    /// scenario benchmarks use this as an anti-elimination counter. the turn
    /// plans and flushes one packet at a time and clears the plan in between, so
    /// the count is accumulated during the turn rather than read off the buffers
    /// afterwards
    #[cfg(feature = "internal-benchmarks")]
    pub fn take_planned_forwards_for_benchmark(&mut self) -> usize {
        self.forwarder.take_planned_forwards_for_benchmark()
    }

    /// runs the production turn phases once the buffers hold this turn's
    /// ingress
    ///
    /// `now` is the single clock the whole turn reads, so every deadline the
    /// turn resolves belongs to the same timeline as the ingress it observes
    fn pump_core(
        &mut self,
        state: &mut PacketLoopState,
        bitrate_registry: &Arc<Mutex<BitrateRegistry>>,
        snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
        config: &PacketLoopConfig,
        relay_rx: &mut mpsc::Receiver<ForwardedPacket>,
        now: Instant,
    ) -> (WaitPhaseSnapshot, bool) {
        let session_drain_context = SessionDrainContext::new(
            snapshot_state,
            bitrate_registry,
            &config.metrics,
            &config.rtc_metrics,
            &config.source_policy_signal,
        );
        // Drain output from prior inputs before forwarding this batch. Local RTC
        // writes later in the pump requeue their sessions for the next turn.
        let topology_changed =
            drain_ready_sessions(state, &session_drain_context, &mut self.buffers, now);
        // Cap relay work so sustained cross-worker fanout cannot delay control,
        // timeout or UDP input indefinitely.
        drain_relay_packets(
            relay_rx,
            &mut self.buffers.pending_packets,
            MAX_RELAY_PACKETS_PER_ITERATION,
            &config.rtc_metrics,
        );
        state.routes.flush_remote_pkt_gates();
        flush_pending_kf_reqs_at(
            state,
            &config.rtc_metrics,
            &mut self.buffers.pending_keyframe_requests,
            &mut self.buffers.coalesced_keyframe_requests,
            now,
        );
        self.forwarder.forward_batch(
            state,
            &mut self.buffers.pending_packets,
            &ForwardingEffects {
                packet_sinks: &config.packet_sink_registry,
                source_policy_signal: &config.source_policy_signal,
                metrics: &config.metrics,
                rtp_metrics: &config.rtp_metrics,
                rtc_metrics: &config.rtc_metrics,
            },
        );
        config
            .source_policy_signal
            .mark_dirty_rooms(state.take_expired_speaker_rooms(now));
        drain_due_kf_retries(
            state,
            &config.rtc_metrics,
            &mut self.buffers.keyframe_retries,
            now,
        );
        (
            WaitPhaseSnapshot {
                next_timeout: next_timeout_deadline_at(state, now),
            },
            topology_changed,
        )
    }

    /// Sends UDP transmits staged during the pump phase.
    pub(super) async fn flush_outputs(&mut self, socket: &RtcUdpSocket) {
        self.flush_staged_transmits(socket).await;
    }

    /// waits for the next event that should resume the worker loop
    ///
    /// shutdown and control input are biased ahead of ingress receive
    pub async fn wait_for_next_input(
        &mut self,
        snapshot: WaitPhaseSnapshot,
        ingress: &mut UdpIngress,
        inputs: &mut PacketLoopInputReceivers,
        packet_loop_delay: &PacketLoopDelaySnapshot,
    ) -> Option<PacketLoopTurnInput> {
        loop {
            if inputs.shutdown_cancelled() {
                return None;
            }
            if let Some(input) = inputs.try_recv_control() {
                return Some(PacketLoopTurnInput::Control(input));
            }

            if let Some(next_timeout) = snapshot.next_timeout
                && next_timeout <= Instant::now()
                && self.ready_now_budget > 0
            {
                // timeout wakes keep due internal work moving but the budget prevents an
                // expired deadline from starving mailbox or ingress input forever
                self.ready_now_budget = self.ready_now_budget.saturating_sub(1);
                return Some(PacketLoopTurnInput::Timeout);
            }
            // after one awaited datagram wakes the loop, consume a bounded number of
            // already completed datagrams so bursts do not pay one ingress await per packet
            if let Some(next_input) = self.try_recv_queued_datagram(ingress) {
                return Some(next_input);
            }
            // heartbeat publication is the lowest-priority wait item. A continuously
            // ready mailbox or ingress queue therefore expires the health snapshot.
            let heartbeat_deadline = self.delay_publisher.deadline();
            tokio::select! {
                biased;
                next_input = inputs.recv_control_or_relay() => {
                    return next_input.map(mailbox_to_input);
                }
                next_input = self.wait_for_ingress_input(&snapshot, ingress) => {
                    return Some(next_input);
                }
                () = sleep_until(heartbeat_deadline.into()) => {
                    self.delay_publisher.observe(packet_loop_delay, Instant::now());
                }
            }
        }
    }

    /// applies the event that woke the worker after the pump phase
    ///
    /// control inputs mutate authoritative worker state and conservatively
    /// invalidate ingress demux recovery hints
    /// queued UDP datagrams can resume following turns without another ingress await
    /// but every datagram still gets a pump between inputs
    /// relay input remains in the receiver bundle until the next pump
    pub fn apply_input(
        &mut self,
        context: &mut PacketLoopApplyContext<'_>,
        next_input: PacketLoopTurnInput,
    ) {
        // real external input proves the loop yielded to its environment, so
        // immediate internal wakeups get a fresh fairness budget
        if !matches!(next_input, PacketLoopTurnInput::Timeout) {
            self.ready_now_budget = MAX_READY_NOW_INPUTS_BEFORE_YIELD;
        }

        match next_input {
            PacketLoopTurnInput::Timeout | PacketLoopTurnInput::RelayPacket => {}
            PacketLoopTurnInput::Control(command) => {
                command.dispatch(
                    context.packet_loop_state,
                    &WorkerCommandContext {
                        bitrate_registry: context.bitrate_registry,
                        snapshot_state: context.snapshot_state,
                        candidate_addr: context.candidate_addr,
                        now: Instant::now(),
                        config: &context.config.worker,
                        runtime_metrics: &context.config.metrics,
                        rtc_metrics: &context.config.rtc_metrics,
                    },
                );
                // Control can change session or ICE ownership. Clear misses before
                // a queued datagram is routed against the new topology.
                context.demux.clear_on_topology_change();
            }
            PacketLoopTurnInput::Datagram(datagram) => {
                let packet = route_datagram_to_session(
                    context.packet_loop_state,
                    context.demux,
                    &context.config.rtc_metrics,
                    datagram,
                );
                context.ingress.recycle(packet);
            }
        }
    }

    async fn flush_staged_transmits(&mut self, socket: &RtcUdpSocket) {
        for pending_transmit in self.buffers.pending_transmits_mut() {
            let packet = take(&mut pending_transmit.contents);
            if socket
                .send_to(packet, pending_transmit.destination)
                .await
                .is_err()
            {
                warn!(
                    destination = %pending_transmit.destination,
                    "failed to send packet-loop transport datagram"
                );
            }
        }
    }

    /// tries to consume one already queued UDP datagram without awaiting
    ///
    /// the burst budget allows short receive bursts while still forcing the worker
    /// back to the biased wait after a bounded number of datagrams
    fn try_recv_queued_datagram(
        &mut self,
        ingress: &mut UdpIngress,
    ) -> Option<PacketLoopTurnInput> {
        if self.udp_burst_budget == 0 {
            return None;
        }
        if let Some(datagram) = ingress.try_recv() {
            self.udp_burst_budget = self.udp_burst_budget.saturating_sub(1);
            Some(PacketLoopTurnInput::Datagram(datagram))
        } else {
            self.udp_burst_budget = MAX_UDP_DATAGRAMS_PER_TURN;
            None
        }
    }

    /// Cancelling this wait leaves socket receive running because [`UdpIngress`]
    /// owns the in-flight I/O.
    async fn wait_for_ingress_input(
        &mut self,
        info: &WaitPhaseSnapshot,
        ingress: &mut UdpIngress,
    ) -> PacketLoopTurnInput {
        let receive = ingress.recv();
        let result = if let Some(next_timeout) = info.next_timeout {
            // an exhausted timeout budget converts an already-due deadline into
            // a minimum ingress wait
            // that gives the biased `select!` a real executor yield before the
            // budget is replenished
            match timeout(ingress_wait_duration(next_timeout), receive).await {
                Ok(result) => result,
                Err(_elapsed) => {
                    self.ready_now_budget = MAX_READY_NOW_INPUTS_BEFORE_YIELD;
                    return PacketLoopTurnInput::Timeout;
                }
            }
        } else {
            receive.await
        };
        // successful receives reset the burst budget around this first datagram
        // so follow-up queued datagrams can be tried without awaiting again
        match result {
            Some(datagram) => {
                // one datagram is already consumed from the new burst
                self.udp_burst_budget = MAX_UDP_DATAGRAMS_PER_TURN.saturating_sub(1);
                PacketLoopTurnInput::Datagram(datagram)
            }
            None => PacketLoopTurnInput::Timeout,
        }
    }
}

const MAX_UDP_DATAGRAMS_PER_TURN: usize = 16;
const MAX_READY_NOW_INPUTS_BEFORE_YIELD: usize = 32;

/// runs the worker-local media packet loop until shutdown or command-channel close
///
/// # Concurrency
///
/// this task owns `PacketLoopState`, demux recovery hints, `UdpIngress` and
/// `PacketLoopBuffers`
/// other tasks communicate with it through channels, shared read-side snapshots
/// and cancellation
/// no `MutexGuard` is held across socket sends or receives
///
/// # Hot-path behavior
///
/// the loop batches work in turns
/// a turn may produce many transmits and forwards but it waits for only one
/// next external input before looping
/// relay draining is bounded so a relay burst cannot starve commands or socket
/// ingress indefinitely
pub async fn run_packet_loop(
    config: PacketLoopConfig,
    mut shared_socket: SharedRtcSocket,
    bitrate_registry: Arc<Mutex<BitrateRegistry>>,
    snapshot_state: Arc<Mutex<RtcSnapshotState>>,
    mut inputs: PacketLoopInputReceivers,
) {
    // transport media ids must start from the worker-assigned range so relay
    // maps can use the media id alone across workers
    let mut packet_loop_state = PacketLoopState {
        next_media_id: config.worker.media_id_base,
        ..PacketLoopState::default()
    };
    // demux recovery is cached outside durable RTC state because any topology
    // command can invalidate source-address or ICE-fragment ownership
    let mut demux = DemuxRecoveryState::new();
    let mut turn = PacketLoopTurn::new(Instant::now());
    let mut next_input = None;

    loop {
        if let Some(input) = next_input.take() {
            turn.apply_input(
                &mut PacketLoopApplyContext {
                    packet_loop_state: &mut packet_loop_state,
                    bitrate_registry: &bitrate_registry,
                    snapshot_state: &snapshot_state,
                    candidate_addr: shared_socket.candidate_addr,
                    config: &config,
                    demux: &mut demux,
                    ingress: &shared_socket.ingress,
                },
                input,
            );
        }

        let snapshot = turn.pump(
            &mut packet_loop_state,
            &bitrate_registry,
            &snapshot_state,
            &config,
            &mut demux,
            &mut inputs,
        );

        // Await socket I/O only after `pump` releases its mutable
        // `PacketLoopState` borrow.
        turn.flush_outputs(&shared_socket.socket).await;

        let input = turn
            .wait_for_next_input(
                snapshot,
                &mut shared_socket.ingress,
                &mut inputs,
                &config.packet_loop_delay,
            )
            .await;
        let Some(input) = input else {
            // shutdown fired or the command receiver closed
            // both cases end the worker rather than spinning on media
            return;
        };
        next_input = Some(input);
    }
}

fn route_datagram_to_session(
    packet_loop_state: &mut PacketLoopState,
    demux: &mut DemuxRecoveryState,
    rtc_metrics: &RtcMetricsRecorder,
    datagram: UdpDatagram,
) -> Vec<u8> {
    let UdpDatagram {
        source_addr,
        candidate_addr,
        received_at,
        packet,
    } = datagram;
    route_pkt_to_session_at(
        packet_loop_state,
        demux,
        rtc_metrics,
        PacketRouteDatagram::new(source_addr, candidate_addr, packet.as_slice(), received_at),
    );
    packet
}

#[cfg(feature = "internal-benchmarks")]
pub fn route_queued_ingress_datagrams_for_benchmark(
    packet_loop_state: &mut PacketLoopState,
    demux: &mut DemuxRecoveryState,
    rtc_metrics: &RtcMetricsRecorder,
    ingress: &mut UdpIngress,
    max_datagrams: usize,
) -> usize {
    let mut routed = 0;
    while routed < max_datagrams {
        let Some(datagram) = ingress.try_recv() else {
            break;
        };
        let packet = route_datagram_to_session(packet_loop_state, demux, rtc_metrics, datagram);
        ingress.recycle(packet);
        routed += 1;
    }
    routed
}

/// returns the next time the loop must wake without external input
///
/// dirty sessions are always due immediately because a previous input or local
/// send has queued more `str0m` output
/// otherwise returns the earliest `str0m`, keyframe retry or active-speaker
/// expiry deadline
#[cfg(test)]
pub(super) fn next_timeout_deadline(state: &mut PacketLoopState) -> Option<Instant> {
    next_timeout_deadline_at(state, Instant::now())
}

fn next_timeout_deadline_at(state: &mut PacketLoopState, now: Instant) -> Option<Instant> {
    if state.has_dirty_sessions() {
        return Some(now);
    }
    [
        state.next_timeout_deadline(),
        state.routes.next_kf_deadline(),
        state.routes.next_active_speaker_deadline(now),
    ]
    .into_iter()
    .flatten()
    .min()
}

fn mailbox_to_input(input: PacketLoopMailboxInput) -> PacketLoopTurnInput {
    match input {
        PacketLoopMailboxInput::Control(command) => PacketLoopTurnInput::Control(command),
        PacketLoopMailboxInput::Relay => PacketLoopTurnInput::RelayPacket,
    }
}

/// prevents an expired deadline from becoming a spin loop
///
/// a deadline that is already due becomes a one millisecond timeout
/// this gives Tokio a scheduling point before the next timeout turn
fn ingress_wait_duration(next_timeout: Instant) -> Duration {
    let timeout_duration = next_timeout.saturating_duration_since(Instant::now());
    if timeout_duration.is_zero() {
        Duration::from_millis(1)
    } else {
        timeout_duration
    }
}
