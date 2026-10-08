//! Sequential room-control and packet work on one RTC worker state.
//!
//! Production command dispatch changes the source state read by the next
//! production packet-loop turn. A relay receiver checks actual delivery while
//! no mailbox wait or separate worker thread enters the instruction count.
//! The opaque payload isolates activity-gated relay forwarding rather than
//! video codec parsing, sockets or concurrent scheduling.

use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Instant,
};

use o_sfu_router::rtp::{MediaStream as RouterRtpParameters, StreamBinding};
use str0m::media::{Frequency, Mid};
use tokio::sync::{mpsc, oneshot};

use super::super::{
    RtcWorkerConfig, RtpProfile,
    commands::{RtcWorkerCommand, WorkerMediaControlBatch, WorkerMediaControlBatchOutcome},
    control::{WorkerCommandContext, handle_worker_command},
    packet_loop::forwarded_packet::ForwardedPacket,
    state::{
        PacketLoopState, RtcSnapshotState,
        bitrate::BitrateRegistry,
        relay_registry::{RelayPacketMailbox, RelayTargetId},
        slots::SessionHandle,
    },
    test_support::{
        BenchmarkPacketStaging, BenchmarkStreamIdentity, prepare_source_session,
        restage_packet_for_benchmark, sample_local_forwarded_packet_for_benchmark,
        test_transport_session_key,
    },
    worker::{BenchmarkTurnInput, PacketLoopConfig, PacketLoopDelaySnapshot, PacketLoopTurn},
};
use crate::{
    Bitrate, CodecPreferences, MediaCodecFlags, SessionBitrateLimits, VideoBitrateLimits,
    engine::{
        UserId,
        media_transport::{
            ProducerActivity, SourceActivityRevision, SourceActivityUpdate, SourcePolicySignal,
            TransportMediaId, TransportResult, TransportSourceKey,
            route_control::ProducerRouteControl,
        },
        metrics::RuntimeMetrics,
        packet_sink_registry::RoomPacketSinkRegistry,
    },
};

const ROUNDS: usize = 16;
const PACKETS_PER_PHASE: usize = 16;
const SOURCE_SSRC: u32 = 91_001;
const PAYLOAD: &[u8] = b"relay-activity";

/// Packets observed by one sequential packet and command sample.
pub const INTERLEAVED_RELAY_PACKETS: usize = ROUNDS * 2 * PACKETS_PER_PHASE;

type ActivityResponse = oneshot::Receiver<TransportResult<WorkerMediaControlBatchOutcome>>;

/// Fixed source-activity updates and local RTP batches on one worker state.
///
/// Each round pauses the producer for 16 packets then resumes it for 16.
/// Setup prepares real source RTC state, one active relay target and every
/// packet and command. Relay readback and fixture teardown occur after measurement.
pub struct InterleavedRelayActivityBenchFixture {
    state: PacketLoopState,
    turn: PacketLoopTurn,
    config: PacketLoopConfig,
    bitrate_registry: Arc<Mutex<BitrateRegistry>>,
    snapshot_state: Arc<Mutex<RtcSnapshotState>>,
    inbound_relay_rx: mpsc::Receiver<ForwardedPacket>,
    _inbound_relay_tx: mpsc::Sender<ForwardedPacket>,
    outbound_relay_rx: mpsc::Receiver<ForwardedPacket>,
    source_media_id: TransportMediaId,
    commands: VecDeque<RtcWorkerCommand>,
    responses: Vec<ActivityResponse>,
    packets: VecDeque<Vec<ForwardedPacket>>,
    now: Instant,
}

impl InterleavedRelayActivityBenchFixture {
    /// Builds a producer, relay and fixed control and packet inputs.
    ///
    /// # Panics
    ///
    /// Panics if the RTC producer or RTP profile cannot be built or if the
    /// warm-up command and relay packet do not complete as expected.
    #[must_use]
    #[expect(
        clippy::too_many_lines,
        reason = "one linear constructor keeps the fixed state, warm-up and prepared inputs together"
    )]
    pub fn activity_gate() -> Self {
        let source_session = test_transport_session_key(131, 0, 132, UserId::Integer(133));
        let mut state = PacketLoopState::default();
        let source_media_id = prepare_source_session(
            &mut state,
            &source_session,
            Mid::from("cam-up"),
            SOURCE_SSRC,
        );
        state.refresh_producer_ssrcs(
            &source_session,
            Mid::from("cam-up"),
            &RouterRtpParameters::new(
                vec![],
                vec![],
                vec![StreamBinding::new().with_ssrc(SOURCE_SSRC)],
            ),
        );
        let source = TransportSourceKey::new(source_session.clone(), source_media_id);
        let session_handle = state
            .users
            .handle_for_key(&source_session)
            .expect("benchmark producer needs a session handle");
        let now = Instant::now();
        let mut bitrate_registry = BitrateRegistry::default();
        let counter =
            bitrate_registry.register_incoming_media(&source_session, source_media_id, now);
        state.register_incoming_bitrate_counter(source_media_id, counter);
        let (outbound_tx, outbound_relay_rx) = mpsc::channel(INTERLEAVED_RELAY_PACKETS);
        let target_id = RelayTargetId::new(1);
        state.routes.add_relay_target(
            source_media_id,
            target_id,
            RelayPacketMailbox::new(outbound_tx),
        );
        state
            .routes
            .set_relay_target_active(source_media_id, target_id, true);
        let (inbound_relay_tx, inbound_relay_rx) = mpsc::channel(1);
        let metrics = Arc::new(RuntimeMetrics::default());
        let config = PacketLoopConfig {
            worker: RtcWorkerConfig {
                bitrate_limits: SessionBitrateLimits::new(
                    Bitrate::from_mbps(8),
                    Bitrate::from_mbps(10),
                ),
                video_bitrate_limits: VideoBitrateLimits::default(),
                profile: Arc::new(
                    RtpProfile::compile(MediaCodecFlags::default(), CodecPreferences::default())
                        .expect("benchmark RTP profile should compile"),
                ),
                media_quality_interval: None,
                media_id_base: 0,
            },
            packet_sink_registry: Arc::new(RoomPacketSinkRegistry::default()),
            source_policy_signal: SourcePolicySignal::default(),
            metrics: Arc::clone(&metrics),
            rtp_metrics: metrics.register_rtp_worker(),
            rtc_metrics: metrics.register_rtc_worker(),
            packet_loop_delay: Arc::new(PacketLoopDelaySnapshot::new(now)),
        };
        let mut fixture = Self {
            state,
            turn: PacketLoopTurn::new(now),
            config,
            bitrate_registry: Arc::new(Mutex::new(bitrate_registry)),
            snapshot_state: Arc::new(Mutex::new(RtcSnapshotState::default())),
            inbound_relay_rx,
            _inbound_relay_tx: inbound_relay_tx,
            outbound_relay_rx,
            source_media_id,
            commands: VecDeque::with_capacity(ROUNDS * 2),
            responses: Vec::with_capacity(ROUNDS * 2),
            packets: VecDeque::with_capacity(ROUNDS * 2),
            now,
        };
        let mut revision = SourceActivityRevision::default();
        // Exercise both turn branches before measurement, then empty the relay.
        for (activity, sequence) in [
            (ProducerActivity::Inactive, 1),
            (ProducerActivity::Active, 2),
        ] {
            revision = revision.next();
            let (command, mut response) = activity_command(&source, activity, revision);
            fixture.run_phase(command, vec![packet(session_handle, sequence, now)]);
            assert_eq!(
                fixture.state.routes.source_is_active(source_media_id),
                activity.is_active()
            );
            let Ok(Ok(WorkerMediaControlBatchOutcome::Applied(results))) = response.try_recv()
            else {
                panic!("benchmark warm-up activity update failed");
            };
            assert!(matches!(results.as_slice(), [Ok(())]));
        }
        let warm_packet = fixture
            .outbound_relay_rx
            .try_recv()
            .expect("warm relay packet");
        assert_eq!(warm_packet.payload(), payload_for_sequence(2));
        assert!(matches!(
            fixture.outbound_relay_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        for phase in 0..ROUNDS * 2 {
            revision = revision.next();
            let activity = if phase % 2 == 0 {
                ProducerActivity::Inactive
            } else {
                ProducerActivity::Active
            };
            let (command, response) = activity_command(&source, activity, revision);
            fixture.commands.push_back(command);
            fixture.responses.push(response);
            fixture.packets.push_back(
                (0..PACKETS_PER_PHASE)
                    .map(|index| {
                        let sequence = 3 + phase * PACKETS_PER_PHASE + index;
                        packet(session_handle, sequence, now)
                    })
                    .collect(),
            );
        }
        fixture
    }

    fn run_phase(&mut self, command: RtcWorkerCommand, packets: Vec<ForwardedPacket>) {
        handle_worker_command(
            &mut self.state,
            &WorkerCommandContext {
                bitrate_registry: &self.bitrate_registry,
                snapshot_state: &self.snapshot_state,
                candidate_addr: SocketAddr::from(([127, 0, 0, 1], 47_000)),
                now: self.now,
                config: &self.config.worker,
                runtime_metrics: &self.config.metrics,
                rtc_metrics: &self.config.rtc_metrics,
            },
            command,
        );
        self.turn.pump_for_benchmark(
            &mut self.state,
            &self.bitrate_registry,
            &self.snapshot_state,
            &self.config,
            &mut self.inbound_relay_rx,
            BenchmarkTurnInput {
                packets,
                keyframe_requests: Vec::new(),
                now: self.now,
            },
        );
    }

    /// Executes 32 production command dispatches and 512 packet observations.
    /// Call this once per fixture because it consumes the prepared inputs.
    ///
    /// # Panics
    ///
    /// Panics if setup omitted an input or this method is called again.
    pub fn run(&mut self) {
        for _ in 0..ROUNDS * 2 {
            let command = self.commands.pop_front().expect("benchmark command exists");
            let packets = self
                .packets
                .pop_front()
                .expect("benchmark packet batch exists");
            self.run_phase(command, packets);
        }
    }

    /// Checks command outcomes and exact active-phase relay payload and order.
    /// Call this once per fixture because it consumes command outcomes and relay packets.
    ///
    /// # Panics
    ///
    /// Panics if this method is called again or if a command failed or a packet
    /// was lost, reordered or forwarded while the source was inactive.
    pub fn assert_coverage(&mut self) {
        assert!(self.state.routes.source_is_active(self.source_media_id));
        for response in &mut self.responses {
            let Ok(Ok(WorkerMediaControlBatchOutcome::Applied(results))) = response.try_recv()
            else {
                panic!("benchmark command did not return an applied outcome");
            };
            assert!(
                matches!(results.as_slice(), [Ok(())]),
                "benchmark activity update failed"
            );
        }
        for phase in (1..ROUNDS * 2).step_by(2) {
            for index in 0..PACKETS_PER_PHASE {
                let relayed = self
                    .outbound_relay_rx
                    .try_recv()
                    .expect("active-phase RTP must reach the relay");
                let sequence = 3 + phase * PACKETS_PER_PHASE + index;
                assert_eq!(relayed.payload(), payload_for_sequence(sequence));
            }
        }
        assert!(matches!(
            self.outbound_relay_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }
}

fn activity_command(
    source: &TransportSourceKey,
    activity: ProducerActivity,
    revision: SourceActivityRevision,
) -> (RtcWorkerCommand, ActivityResponse) {
    let (response, receiver) = oneshot::channel();
    (
        RtcWorkerCommand::ApplyMediaControlBatch {
            batch: WorkerMediaControlBatch::ProducerActivity(vec![(
                0,
                ProducerRouteControl {
                    source: source.clone(),
                    update: SourceActivityUpdate::new(activity, revision),
                },
            )]),
            response,
        },
        receiver,
    )
}

fn packet(session_handle: SessionHandle, sequence: usize, now: Instant) -> ForwardedPacket {
    let mut packet = sample_local_forwarded_packet_for_benchmark(
        session_handle,
        "cam-up",
        None,
        BenchmarkStreamIdentity {
            ssrc: SOURCE_SSRC,
            payload_type: 111,
            clock_rate: Frequency::NINETY_KHZ,
        },
        Arc::from(payload_for_sequence(sequence)),
    );
    restage_packet_for_benchmark(
        &mut packet,
        BenchmarkPacketStaging {
            sequence_number: u64::try_from(sequence).expect("fixed sequence fits u64"),
            rtp_timestamp: u32::try_from(sequence).expect("fixed sequence fits u32") * 3_000,
            ..BenchmarkPacketStaging::default()
        },
        None,
        now,
    );
    packet
}

fn payload_for_sequence(sequence: usize) -> Vec<u8> {
    let mut payload = Vec::with_capacity(PAYLOAD.len() + 2);
    payload.extend_from_slice(PAYLOAD);
    payload.extend_from_slice(
        &u16::try_from(sequence)
            .expect("fixed sequence fits u16")
            .to_be_bytes(),
    );
    payload
}
