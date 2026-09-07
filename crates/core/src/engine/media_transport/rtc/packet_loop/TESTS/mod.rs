//! Regression coverage for packet-loop contracts.
//!
//! These tests exercise the packet-loop helpers at their boundary points:
//! ingress demux caching, relay fanout, packet sink accounting, route-control
//! observations, keyframe feedback coalescing and scheduling deadlines. They
//! avoid running a full async worker unless the contract under
//! test requires worker scheduling behavior.

use std::{
    collections::BTreeSet,
    future::Future,
    net::{SocketAddr, UdpSocket as StdUdpSocket},
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use o_sfu_rfc::{
    rtp::{self, CodecName},
    webrtc,
};
use o_sfu_router::{
    MediaKind as RouterMediaKind,
    rtp::{MediaFormat, MediaStream as RouterRtpParameters, PayloadType},
};
use o_sfu_telemetry::schema::event::TRANSPORT_HEALTH_CHANGED;
use serde_json::Value;
use str0m::{
    Event, IceConnectionState, IceCreds, Input, Output,
    ice::{StunMessage, TransId},
    media::{KeyframeRequestKind, MediaKind, Mid, Pt, Rid},
    net::{Protocol, Receive},
    rtp::Ssrc,
    stats::{MediaEgressStats, MediaIngressStats},
};
use tokio::{
    runtime::Builder,
    sync::{mpsc, oneshot},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use super::{
    buffers::{MAX_RELAY_PACKETS_PER_ITERATION, PacketLoopBuffers},
    delay::PacketLoopDelaySnapshot,
    event_observation::{RtcEventContext, observe_rtc_event},
    forward_flush::{
        drain_relay_packets, finish_incoming_stats, flush_packet_forwards, record_incoming_packet,
        record_incoming_stats,
    },
    ingress_routing::{PacketRouteDatagram, route_pkt_to_session_at},
    input::{PacketLoopInputReceivers, PacketLoopMailboxInput},
    keyframe_requests::{PendingKeyframeRequest, drain_due_kf_retries, flush_pending_kf_reqs_at},
    loop_driver::{
        PacketLoopApplyContext, PacketLoopConfig, PacketLoopTurn, PacketLoopTurnInput,
        WaitPhaseSnapshot,
    },
    udp::{RtcUdpSocket, UdpIngress},
};
use crate::{
    Bitrate, CodecPreferences, MediaCodecFlags, RtcUdpIoBackend, VideoBitrateLimits,
    engine::{
        RoomInstanceId, UserId,
        media_transport::{
            ProducerActivity, SourceActivityRevision, SourceActivityUpdate, SourcePolicySignal,
            TransportMediaId, TransportSessionKey, TransportSourceKey,
            rtc::{
                RtcWorker, RtcWorkerConfig, RtpProfile,
                bitrate::BitrateRegistry,
                bootstrap, codec,
                commands::{RemoteSourceControl, RouteControlRequest, RtcWorkerCommand},
                forwarding_destination::ForwardingDestination,
                local_send_rewrite::SourceRtpIdentity,
                media_registry::RegisteredMediaHandle,
                relay_registry::{RelayPacketMailbox, RelayTargetId},
                route_control::PacketLayerGate,
                routing_miss::PacketLoopRoutingMissKey,
                slots::ConsumerStreamHandle,
                source_route::MediaRouteDestination,
                state::{
                    PacketLoopState, RTCP_INGRESS_BUDGET_CAPACITY_BYTES, RtcNackTotals,
                    RtcSnapshotState, SharedRtcSocket,
                },
                test_support::{
                    DebugProbe, DebugProbeRequest, collect_ready_session_keys,
                    prepare_source_session_with_rid, sample_already_relayed_audio_packet_at,
                    sample_already_relayed_packet, sample_forwarded_packet,
                    sample_forwarded_packet_with_audio_activity, sample_forwarded_packet_with_rid,
                    sample_forwarded_packet_without_mid, sample_local_forwarded_packet,
                    sample_local_repaired_packet, sample_rtp_packet, serialize_stun_message,
                    test_transport_session_key,
                },
            },
        },
        metrics::{
            RtcMetricsRecorder, RtcNackDirection, RtpForwardDestinationKind, RtpMetricsRecorder,
            RuntimeMetrics, test_support::RuntimeMetricsSnapshotTestExt,
        },
        packet_sink_registry::{
            PacketSink as MediaPacketSink, PacketSinkRouteCache, RegisteredPacketSink,
            RoomPacketSinkRegistry,
        },
        room::TESTS::tracing::{assert_exact, capture},
    },
};

struct CountingSink {
    packets: AtomicUsize,
    last_packet: Mutex<(Option<TransportSessionKey>, Vec<u8>)>,
}

const TEST_REMOTE_ICE_UFRAG: &str = "remote-ufrag";
const TEST_REMOTE_ICE_PASSWORD: &str = "remote-password";

impl CountingSink {
    fn new() -> Self {
        Self {
            packets: AtomicUsize::new(0),
            last_packet: Mutex::new((None, Vec::new())),
        }
    }

    fn last_packet(&self) -> (Option<TransportSessionKey>, Vec<u8>) {
        self.last_packet
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl MediaPacketSink for CountingSink {
    fn record_packet(
        &self,
        session_key: &TransportSessionKey,
        _transport_media_id: TransportMediaId,
        _received_at: Instant,
        payload: &[u8],
    ) {
        self.packets.fetch_add(1, Ordering::Relaxed);
        let mut last_packet = self
            .last_packet
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        last_packet.0 = Some(session_key.clone());
        payload.clone_into(&mut last_packet.1);
    }
}

struct IngressRoutingHarness {
    packet_loop_state: PacketLoopState,
    demux: super::super::routing_miss::DemuxRecoveryState,
    metrics: RuntimeMetrics,
    rtc_metrics: Arc<RtcMetricsRecorder>,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
}

impl IngressRoutingHarness {
    fn new(source_port: u16, candidate_port: u16) -> Self {
        let metrics = RuntimeMetrics::default();
        let rtc_metrics = metrics.register_rtc_worker();
        Self {
            packet_loop_state: PacketLoopState::default(),
            demux: super::super::routing_miss::DemuxRecoveryState::new(),
            metrics,
            rtc_metrics,
            source_addr: SocketAddr::from(([127, 0, 0, 1], source_port)),
            candidate_addr: SocketAddr::from(([127, 0, 0, 1], candidate_port)),
        }
    }

    fn route(&mut self, packet: &[u8]) {
        self.route_at(packet, Instant::now());
    }

    fn route_at(&mut self, packet: &[u8], now: Instant) {
        route_pkt_to_session_at(
            &mut self.packet_loop_state,
            &mut self.demux,
            &self.rtc_metrics,
            PacketRouteDatagram::new(self.source_addr, self.candidate_addr, packet, now),
        );
    }
}

struct MarkSessionDirtyProbe {
    session_key: TransportSessionKey,
}

impl DebugProbe for MarkSessionDirtyProbe {
    type Output = ();

    fn inspect(
        self,
        state: &mut PacketLoopState,
        _context: &super::super::worker::WorkerCommandContext<'_>,
    ) -> Self::Output {
        state.mark_session_dirty(&self.session_key);
    }
}

fn drain_ready_sessions(state: &mut PacketLoopState) -> Vec<TransportSessionKey> {
    collect_ready_session_keys(state, Instant::now())
}

#[expect(
    clippy::expect_used,
    reason = "test setup helpers should fail loudly when a required RTC fixture cannot be built"
)]
fn create_rtc_session(state: &mut PacketLoopState, session: &TransportSessionKey, port: u16) {
    let created = bootstrap::ensure_session_rtc_state(
        &mut state.users,
        session,
        SocketAddr::from(([127, 0, 0, 1], port)),
        Bitrate::from_mbps(10),
    )
    .expect("test session should enter RTC state");
    assert!(created, "test session should be newly created");
}

fn authenticated_binding_packet(
    state: &mut PacketLoopState,
    session_key: &TransportSessionKey,
) -> Result<Vec<u8>, &'static str> {
    let local_ice_credentials = state
        .users
        .get_mut(session_key)
        .map(|session_state| {
            let mut api = session_state.rtc.direct_api();
            api.set_remote_ice_credentials(IceCreds {
                ufrag: TEST_REMOTE_ICE_UFRAG.to_owned(),
                pass: TEST_REMOTE_ICE_PASSWORD.to_owned(),
            });
            api.local_ice_credentials()
        })
        .ok_or("session state missing after creation")?;
    let username = format!(
        "{}{separator}{TEST_REMOTE_ICE_UFRAG}",
        local_ice_credentials.ufrag,
        separator = webrtc::ice::USERNAME_FRAGMENT_SEPARATOR,
    );
    serialize_stun_message(
        &StunMessage::binding_request(&username, TransId::new(), true, 1, 1, true),
        Some(local_ice_credentials.pass.as_bytes()),
    )
    .ok_or("failed to serialize STUN binding request")
}

fn exhaust_rtcp_ingress_budget(
    state: &mut PacketLoopState,
    session_key: &TransportSessionKey,
    now: Instant,
) -> Result<(), &'static str> {
    let Some(session_state) = state.users.get_mut(session_key) else {
        return Err("session state missing before RTCP budget exhaustion");
    };
    if !session_state
        .rtcp_ingress_budget
        .try_charge(RTCP_INGRESS_BUDGET_CAPACITY_BYTES, now)
    {
        return Err("fresh RTCP ingress budget should admit its full capacity");
    }
    Ok(())
}

enum RtcpBudgetDropRoute {
    Cached,
    Recovered,
}

struct PreparedRtcpBudgetDrop {
    harness: IngressRoutingHarness,
    session_key: TransportSessionKey,
    rtcp: [u8; rtp::RTCP_RECEIVER_REPORT_WITHOUT_BLOCKS_BYTES],
    now: Instant,
}

fn prepare_authenticated_rtcp_budget_drop(
    route: &RtcpBudgetDropRoute,
) -> Result<PreparedRtcpBudgetDrop, &'static str> {
    let mut harness = IngressRoutingHarness::new(45_051, 45_050);
    let session_key = test_transport_session_key(51, 0, 60, UserId::Integer(61));

    create_rtc_session(
        &mut harness.packet_loop_state,
        &session_key,
        harness.candidate_addr.port(),
    );
    let binding = authenticated_binding_packet(&mut harness.packet_loop_state, &session_key)?;
    let now = Instant::now() + Duration::from_secs(1);
    harness.route_at(&binding, now);
    poll_rtc_until_timeout(&mut harness.packet_loop_state, &session_key)?;
    assert_eq!(
        drain_ready_sessions(&mut harness.packet_loop_state),
        vec![session_key.clone()]
    );
    // A receiver report with no report blocks is the smallest valid RTCP
    // packet accepted after ICE nomination.
    // https://www.rfc-editor.org/rfc/rfc3550.html#section-6.4.2
    let rtcp = rtp::rtcp_receiver_report_without_report_blocks(rtp::Ssrc::from(0));
    let receive = Receive::new(
        Protocol::Udp,
        harness.source_addr,
        harness.candidate_addr,
        &rtcp,
    )
    .map_err(|_error| "candidate RTCP should classify")?;
    assert!(
        harness
            .packet_loop_state
            .users
            .get(&session_key)
            .ok_or("session state missing after ICE nomination")?
            .rtc
            .accepts(&Input::Receive(now, receive)),
        "nominated source should accept candidate RTCP"
    );
    if let RtcpBudgetDropRoute::Recovered = route {
        harness
            .packet_loop_state
            .remote_addr_demux
            .forget_remote_addr(harness.source_addr);
    }
    exhaust_rtcp_ingress_budget(&mut harness.packet_loop_state, &session_key, now)?;
    Ok(PreparedRtcpBudgetDrop {
        harness,
        session_key,
        rtcp,
        now,
    })
}

fn poll_rtc_until_timeout(
    state: &mut PacketLoopState,
    session_key: &TransportSessionKey,
) -> Result<(), &'static str> {
    let Some(session_state) = state.users.get_mut(session_key) else {
        return Err("session state missing before RTC output poll");
    };
    loop {
        match session_state.rtc.poll_output() {
            Ok(Output::Timeout(_)) => return Ok(()),
            Ok(_) => {}
            Err(_error) => return Err("RTC output poll failed"),
        }
    }
}

fn bind_std_socket() -> Result<StdUdpSocket, &'static str> {
    let socket = StdUdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .map_err(|_error| "UDP socket should bind")?;
    socket
        .set_nonblocking(true)
        .map_err(|_error| "UDP socket should become nonblocking")?;
    Ok(socket)
}

fn test_socket() -> Result<SharedRtcSocket, &'static str> {
    let socket = bind_std_socket()?;
    let addr = socket
        .local_addr()
        .map_err(|_error| "test socket should have a local addr")?;
    let socket = RtcUdpSocket::from_std(socket, RtcUdpIoBackend::Tokio)
        .map_err(|_error| "test socket should convert")?;
    Ok(SharedRtcSocket {
        ingress: UdpIngress::new(socket.clone(), addr, addr),
        socket,
        candidate_addr: addr,
    })
}

fn run_packet_loop_io_test(
    test: impl Future<Output = Result<(), &'static str>>,
) -> Result<(), &'static str> {
    Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .map_err(|_error| "test runtime should build")?
        .block_on(test)
}

#[expect(
    clippy::expect_used,
    reason = "test setup helpers should fail loudly when a required RTC fixture is missing"
)]
fn declare_video_tx(
    state: &mut PacketLoopState,
    session: &TransportSessionKey,
    port: u16,
    mid: Mid,
    ssrc: u32,
) {
    create_rtc_session(state, session, port);
    let session_state = state
        .users
        .get_mut(session)
        .expect("test session should be registered");
    let mut direct_api = session_state.rtc.direct_api();
    direct_api.declare_media(mid, MediaKind::Video);
    direct_api.declare_stream_tx(Ssrc::from(ssrc), None, mid, None);
}

fn insert_open_route(
    state: &mut PacketLoopState,
    src_media: TransportMediaId,
    consumer_session: &TransportSessionKey,
    consumer_media: TransportMediaId,
    consumer_stream: ConsumerStreamHandle,
    consumer_mid: Mid,
) {
    insert_consumer_route_fixture(
        state,
        src_media,
        MediaRouteDestination {
            dest_session: consumer_session.clone(),
            dest_transport_media_id: consumer_media,
            dest_stream: consumer_stream,
            dest_mid: consumer_mid,
            dest_payload_type: None,
            repair_enabled: false,
            active: true,
            requires_decoder_refresh: false,
            delivery_generation: 0,
            packet_gate: PacketLayerGate::Open,
            pending_gate: None,
        },
    );
}

fn insert_consumer_route_fixture(
    state: &mut PacketLoopState,
    src_media: TransportMediaId,
    destination: MediaRouteDestination,
) {
    let dest_session = destination.dest_session.clone();
    let dest_mid = destination.dest_mid;
    let dest_transport_media_id = destination.dest_transport_media_id;
    let dst_idx = state.routes.add_consumer_route(src_media, destination);
    state.set_consumer_dst_idx(
        &dest_session,
        dest_mid,
        dest_transport_media_id,
        src_media,
        Some(dst_idx),
    );
}

fn register_producer_media(
    state: &mut PacketLoopState,
    producer_session: &TransportSessionKey,
    producer_mid: &str,
) -> TransportMediaId {
    state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: producer_session.clone(),
        mid: Mid::from(producer_mid),
    })
}

fn register_consumer_media(
    state: &mut PacketLoopState,
    consumer_session: &TransportSessionKey,
    consumer_mid: Mid,
    src_media: TransportMediaId,
) -> TransportMediaId {
    state.register_media_handle(RegisteredMediaHandle::Consumer {
        session_key: consumer_session.clone(),
        mid: consumer_mid,
        src_media,
    })
}

fn register_consumer_route_fixture(
    state: &mut PacketLoopState,
    consumer_session: &TransportSessionKey,
    consumer_mid: &str,
    src_media: TransportMediaId,
    active: bool,
    packet_gate: PacketLayerGate,
    pending_gate: Option<PacketLayerGate>,
) {
    let consumer_mid = Mid::from(consumer_mid);
    let consumer_media = register_consumer_media(state, consumer_session, consumer_mid, src_media);
    insert_consumer_route_fixture(
        state,
        src_media,
        MediaRouteDestination {
            dest_session: consumer_session.clone(),
            dest_transport_media_id: consumer_media,
            dest_stream: ConsumerStreamHandle::default(),
            dest_mid: consumer_mid,
            dest_payload_type: None,
            repair_enabled: false,
            active,
            requires_decoder_refresh: false,
            delivery_generation: 0,
            packet_gate,
            pending_gate,
        },
    );
}

fn register_open_consumer_route_fixture(
    state: &mut PacketLoopState,
    consumer_session: &TransportSessionKey,
    consumer_mid: &str,
    src_media: TransportMediaId,
) {
    register_consumer_route_fixture(
        state,
        consumer_session,
        consumer_mid,
        src_media,
        true,
        PacketLayerGate::Open,
        None,
    );
}

fn push_keyframe_request(
    buffers: &mut PacketLoopBuffers,
    consumer_session: TransportSessionKey,
    consumer_mid: &str,
    kind: KeyframeRequestKind,
) {
    push_keyframe_request_with_rid(buffers, consumer_session, consumer_mid, None, kind);
}

fn push_keyframe_request_with_rid(
    buffers: &mut PacketLoopBuffers,
    consumer_session: TransportSessionKey,
    consumer_mid: &str,
    feedback_rid: Option<Rid>,
    kind: KeyframeRequestKind,
) {
    buffers.pending_keyframe_requests.push((
        consumer_session,
        PendingKeyframeRequest {
            consumer_mid: Mid::from(consumer_mid),
            consumer_rid: feedback_rid,
            kind,
        },
    ));
}

fn set_source_route_active(
    state: &mut PacketLoopState,
    src_media: TransportMediaId,
    active: bool,
) -> bool {
    state.routes.set_source_active(src_media, active).is_ok()
}

#[expect(
    clippy::panic,
    reason = "test setup helpers should fail loudly when a fixture queues the wrong command"
)]
fn drain_remote_packet_gate_setup(control_rx: &mut mpsc::Receiver<RtcWorkerCommand>) {
    loop {
        match control_rx.try_recv() {
            Ok(RtcWorkerCommand::RouteControl {
                request: RouteControlRequest::SetRemoteSourcePacketGate { .. },
                response: None,
            }) => {}
            Ok(_) => panic!("expected only remote packet-gate setup commands"),
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                break;
            }
        }
    }
}

#[expect(
    clippy::panic,
    reason = "test assertion helpers should fail loudly when the command shape is wrong"
)]
fn recv_remote_keyframe_request(
    control_rx: &mut mpsc::Receiver<RtcWorkerCommand>,
) -> (
    TransportSourceKey,
    RelayTargetId,
    Option<Rid>,
    KeyframeRequestKind,
) {
    loop {
        match control_rx.try_recv() {
            Ok(RtcWorkerCommand::RouteControl {
                request:
                    RouteControlRequest::RequestRemoteKeyframe {
                        source,
                        target_id,
                        rid,
                        kind,
                    },
                response: None,
            }) => return (source, target_id, rid, kind),
            Ok(RtcWorkerCommand::RouteControl {
                request: RouteControlRequest::SetRemoteSourcePacketGate { .. },
                response: None,
            }) => {}
            Ok(_) => panic!("expected a remote keyframe request"),
            Err(mpsc::error::TryRecvError::Empty) => {
                panic!("expected a remote keyframe request but channel was empty")
            }
            Err(mpsc::error::TryRecvError::Disconnected) => {
                panic!("expected a remote keyframe request but channel was disconnected")
            }
        }
    }
}

fn assert_remote_keyframe_request(
    control_rx: &mut mpsc::Receiver<RtcWorkerCommand>,
    source_session: &TransportSessionKey,
    src_media: TransportMediaId,
    target_id: RelayTargetId,
    rid: Option<Rid>,
    kind: KeyframeRequestKind,
) {
    let (source, actual_target_id, actual_rid, actual_kind) =
        recv_remote_keyframe_request(control_rx);
    assert_eq!(source.session_key(), source_session);
    assert_eq!(source.transport_media_id(), src_media);
    assert_eq!(actual_target_id, target_id);
    assert_eq!(actual_rid, rid);
    assert_eq!(actual_kind, kind);
}

#[expect(
    clippy::panic,
    reason = "test assertion helpers should fail loudly when an unexpected command is queued"
)]
fn assert_no_remote_keyframe_request(control_rx: &mut mpsc::Receiver<RtcWorkerCommand>) {
    match control_rx.try_recv() {
        Err(mpsc::error::TryRecvError::Empty) => {}
        Err(mpsc::error::TryRecvError::Disconnected) => {
            panic!("remote keyframe command channel disconnected")
        }
        Ok(_) => panic!("unexpected remote keyframe request"),
    }
}

#[derive(Clone, Copy)]
enum RouteFeedbackGate {
    Open,
    Rid(&'static str),
    RidWithPending {
        effective: &'static str,
        pending: &'static str,
    },
    Block,
    BlockWithPending(&'static str),
}

impl RouteFeedbackGate {
    fn gates(self) -> (PacketLayerGate, Option<PacketLayerGate>) {
        match self {
            Self::Open => (PacketLayerGate::Open, None),
            Self::Rid(rid) => (PacketLayerGate::Rid(Rid::from(rid)), None),
            Self::RidWithPending { effective, pending } => (
                PacketLayerGate::Rid(Rid::from(effective)),
                Some(PacketLayerGate::Rid(Rid::from(pending))),
            ),
            Self::Block => (PacketLayerGate::Block, None),
            Self::BlockWithPending(rid) => (
                PacketLayerGate::Block,
                Some(PacketLayerGate::Rid(Rid::from(rid))),
            ),
        }
    }
}

#[derive(Clone, Copy)]
enum RouteFeedbackExpectation {
    Drop,
    SourceWide,
    Rid(&'static str),
}

#[derive(Clone, Copy)]
struct RouteFeedbackCase {
    id: u64,
    active: bool,
    gate: RouteFeedbackGate,
    feedback_rid: Option<&'static str>,
    expected: RouteFeedbackExpectation,
    kind: KeyframeRequestKind,
}

fn route_feedback_cases() -> [RouteFeedbackCase; 6] {
    [
        RouteFeedbackCase {
            id: 92,
            active: true,
            gate: RouteFeedbackGate::Open,
            feedback_rid: None,
            expected: RouteFeedbackExpectation::SourceWide,
            kind: KeyframeRequestKind::Fir,
        },
        RouteFeedbackCase {
            id: 93,
            active: true,
            gate: RouteFeedbackGate::Rid("hi"),
            feedback_rid: None,
            expected: RouteFeedbackExpectation::Rid("hi"),
            kind: KeyframeRequestKind::Pli,
        },
        RouteFeedbackCase {
            id: 94,
            active: true,
            gate: RouteFeedbackGate::RidWithPending {
                effective: "lo",
                pending: "hi",
            },
            feedback_rid: None,
            expected: RouteFeedbackExpectation::Rid("hi"),
            kind: KeyframeRequestKind::Pli,
        },
        RouteFeedbackCase {
            id: 95,
            active: true,
            gate: RouteFeedbackGate::BlockWithPending("hi"),
            feedback_rid: None,
            expected: RouteFeedbackExpectation::Rid("hi"),
            kind: KeyframeRequestKind::Pli,
        },
        RouteFeedbackCase {
            id: 96,
            active: true,
            gate: RouteFeedbackGate::Block,
            feedback_rid: Some("hi"),
            expected: RouteFeedbackExpectation::Drop,
            kind: KeyframeRequestKind::Pli,
        },
        RouteFeedbackCase {
            id: 97,
            active: false,
            gate: RouteFeedbackGate::Rid("hi"),
            feedback_rid: None,
            expected: RouteFeedbackExpectation::Drop,
            kind: KeyframeRequestKind::Pli,
        },
    ]
}

fn populate_forward_routes(
    state: &PacketLoopState,
    packet_sinks: &RoomPacketSinkRegistry,
    metrics: &RtcMetricsRecorder,
    pending_packets: &mut [super::super::forwarded_packet::ForwardedPacket],
    forwards: &mut Vec<super::super::forwarding_destination::ForwardingDestination>,
) {
    let mut packet_sink_cache = PacketSinkRouteCache::default();
    packet_sink_cache.refresh_from(packet_sinks);
    for packet in pending_packets {
        super::super::forwarding_planner::plan_forwards(
            state,
            &packet_sink_cache,
            metrics,
            packet,
            forwards,
        );
    }
}

fn flush_only_packet_forwards(
    state: &mut PacketLoopState,
    metrics: &RuntimeMetrics,
    rtp_metrics: &RtpMetricsRecorder,
    rtc_recorder: &RtcMetricsRecorder,
    buffers: &PacketLoopBuffers,
) {
    assert_eq!(buffers.pending_packets.len(), 1);
    if let Some(packet) = buffers.pending_packets.first() {
        flush_packet_forwards(
            state,
            metrics,
            rtp_metrics,
            rtc_recorder,
            packet,
            &buffers.forwards,
        );
    }
}

struct PacketLoopHarness {
    state: PacketLoopState,
    buffers: PacketLoopBuffers,
    metrics: RuntimeMetrics,
    rtp_metrics: Arc<RtpMetricsRecorder>,
    rtc_metrics: Arc<RtcMetricsRecorder>,
}

impl PacketLoopHarness {
    fn new() -> Self {
        let metrics = RuntimeMetrics::default();
        Self {
            state: PacketLoopState::default(),
            buffers: PacketLoopBuffers::new(),
            rtp_metrics: metrics.register_rtp_worker(),
            rtc_metrics: metrics.register_rtc_worker(),
            metrics,
        }
    }

    #[expect(
        clippy::expect_used,
        reason = "test setup helpers should fail loudly when a required remote keyframe fixture cannot be built"
    )]
    fn remote_keyframe_source(
        &mut self,
        src_media: TransportMediaId,
        source_session: &TransportSessionKey,
        target_id: RelayTargetId,
        capacity: usize,
    ) -> mpsc::Receiver<RtcWorkerCommand> {
        let (control_tx, control_rx) = mpsc::channel(capacity);
        let source = TransportSourceKey::new(source_session.clone(), src_media);
        self.state
            .routes
            .register_remote_source(
                &source,
                RemoteSourceControl::new(control_tx, target_id, Arc::clone(&self.rtc_metrics)),
            )
            .expect("remote source should register");
        assert_eq!(
            self.state.routes.apply_source_activity(
                src_media,
                SourceActivityUpdate::new(
                    ProducerActivity::Active,
                    SourceActivityRevision::default(),
                ),
            ),
            Ok(true)
        );
        control_rx
    }

    fn assert_only_packet(&self) {
        assert_eq!(
            self.buffers.pending_packets.len(),
            1,
            "forward fixture should contain exactly one packet"
        );
    }

    fn add_recording_sink(&mut self, src_media: TransportMediaId, sink: &Arc<CountingSink>) {
        self.assert_only_packet();
        self.buffers
            .forwards
            .push(ForwardingDestination::from_packet_sink(
                src_media,
                RegisteredPacketSink::new(
                    Arc::<CountingSink>::clone(sink),
                    RtpForwardDestinationKind::Recording,
                ),
            ));
    }

    fn add_relay(&mut self, src_media: TransportMediaId, target: RelayPacketMailbox) {
        self.assert_only_packet();
        self.buffers
            .forwards
            .push(ForwardingDestination::from_relay_target(src_media, target));
    }

    fn add_local(&mut self, src_media: TransportMediaId, dst_idx: usize) {
        self.assert_only_packet();
        self.buffers
            .forwards
            .push(ForwardingDestination::from_local_route_destination(
                src_media, dst_idx,
            ));
    }
}

fn packet_loop_config_for_test() -> PacketLoopConfig {
    #![expect(
        clippy::panic,
        reason = "packet-loop test configs cannot return Result and must fail loudly when no RTC ports are available"
    )]

    let metrics = Arc::new(RuntimeMetrics::default());
    let outbound_recorder = metrics.register_rtp_worker();
    let datagram_recorder = metrics.register_rtc_worker();
    PacketLoopConfig {
        worker: RtcWorkerConfig {
            bitrate_limits: crate::SessionBitrateLimits::new(
                Bitrate::from_mbps(8),
                Bitrate::from_mbps(10),
            ),
            video_bitrate_limits: VideoBitrateLimits::default(),
            profile: Arc::new(
                RtpProfile::compile(MediaCodecFlags::default(), CodecPreferences::default())
                    .unwrap_or_else(|_error| panic!("test RTP profile should compile")),
            ),
            media_quality_interval: None,
            media_id_base: 0,
        },
        packet_sink_registry: Arc::new(RoomPacketSinkRegistry::default()),
        source_policy_signal: SourcePolicySignal::default(),
        metrics,
        rtp_metrics: outbound_recorder,
        rtc_metrics: datagram_recorder,
        packet_loop_delay: Arc::new(PacketLoopDelaySnapshot::new(Instant::now())),
    }
}

#[test]
fn recent_miss_cache_skips_repeated_scans_for_the_same_source() {
    let mut harness = IngressRoutingHarness::new(45_001, 45_000);
    let packet = sample_rtp_packet(1, 11);

    harness.route(&packet);
    harness.route(&packet);

    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 1);
    assert_eq!(snapshot.rtc_datagram_drops_no_user(), 1);
    assert_eq!(snapshot.rtc_datagram_drops_recent_miss_cache(), 1);
    assert_eq!(snapshot.rtc_datagram_drops_malformed(), 0);
}

#[test]
fn recent_miss_cache_clears_on_topology_change() {
    let mut harness = IngressRoutingHarness::new(45_011, 45_010);
    let packet = sample_rtp_packet(2, 22);

    harness.route(&packet);
    harness.demux.clear_on_topology_change();
    harness.route(&packet);

    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 2);
    assert_eq!(snapshot.rtc_datagram_drops_no_user(), 2);
    assert_eq!(snapshot.rtc_datagram_drops_recent_miss_cache(), 0);
}

#[test]
fn recent_miss_cache_does_not_skip_different_packets_from_the_same_source() {
    let mut harness = IngressRoutingHarness::new(45_021, 45_020);

    harness.route(&sample_rtp_packet(3, 33));
    harness.route(&sample_rtp_packet(4, 44));

    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 2);
    assert_eq!(snapshot.rtc_datagram_drops_no_user(), 2);
    assert_eq!(snapshot.rtc_datagram_drops_recent_miss_cache(), 0);
    assert_eq!(snapshot.rtc_datagram_drops_source_rate_limited(), 0);
}

#[test]
fn source_rate_limiter_bounds_varied_unknown_source_misses() {
    let mut harness = IngressRoutingHarness::new(45_026, 45_025);

    for (sequence, ssrc) in [
        (5_u16, 55_u32),
        (6, 66),
        (7, 77),
        (8, 88),
        (9, 99),
        (10, 110),
    ] {
        harness.route(&sample_rtp_packet(sequence, ssrc));
    }

    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 4);
    assert_eq!(snapshot.rtc_datagram_drops_no_user(), 4);
    assert_eq!(snapshot.rtc_datagram_drops_recent_miss_cache(), 0);
    assert_eq!(snapshot.rtc_datagram_drops_source_rate_limited(), 2);
}

#[test]
fn malformed_udp_datagram_counts_as_malformed_drop_without_scan_metrics() {
    let mut harness = IngressRoutingHarness::new(45_031, 45_030);

    harness.route(&[0x01, 0x02, 0x03]);

    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 0);
    assert_eq!(snapshot.rtc_datagram_scan_users(), 0);
    assert_eq!(snapshot.rtc_datagram_drops_malformed(), 1);
    assert_eq!(snapshot.rtc_datagram_drops_no_user(), 0);
    assert_eq!(snapshot.rtc_datagram_drops_source_rate_limited(), 0);
}

#[test]
fn malformed_sources_do_not_allocate_zero_session_recovery_state() {
    let mut harness = IngressRoutingHarness::new(45_101, 45_100);

    for source_port in 45_101..45_614 {
        harness.source_addr.set_port(source_port);
        harness.route(&[0x01, 0x02, 0x03]);
    }

    assert_eq!(harness.demux.tracked_source_count(), 0);
    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 0);
    assert_eq!(snapshot.rtc_datagram_drops_malformed(), 513);
}

#[test]
fn multi_session_unknown_source_recovery_drops_without_whole_session_scan() {
    let mut harness = IngressRoutingHarness::new(45_041, 45_040);
    let first_session = test_transport_session_key(51, 0, 52, UserId::Integer(53));
    let second_session = test_transport_session_key(51, 0, 54, UserId::Integer(55));
    let packet = [22, 0, 0, 0];

    create_rtc_session(
        &mut harness.packet_loop_state,
        &first_session,
        harness.candidate_addr.port(),
    );
    create_rtc_session(
        &mut harness.packet_loop_state,
        &second_session,
        harness.candidate_addr.port(),
    );

    harness.route(&packet);

    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 1);
    assert_eq!(snapshot.rtc_datagram_scan_users(), 0);
    assert_eq!(snapshot.rtc_datagram_drops_no_user(), 1);
}

#[test]
fn indexed_route_stays_cached_without_touching_recent_miss_state() -> Result<(), &'static str> {
    let mut harness = IngressRoutingHarness::new(45_046, 45_045);
    let session_key = test_transport_session_key(51, 0, 56, UserId::Integer(57));

    create_rtc_session(
        &mut harness.packet_loop_state,
        &session_key,
        harness.candidate_addr.port(),
    );
    assert!(
        harness
            .packet_loop_state
            .remote_addr_demux
            .remember_remote_addr(harness.source_addr, &session_key)
    );
    let packet = authenticated_binding_packet(&mut harness.packet_loop_state, &session_key)?;
    let miss_key =
        PacketLoopRoutingMissKey::new(harness.source_addr, harness.candidate_addr, &packet);
    harness
        .demux
        .record_miss(miss_key, &packet, harness.source_addr, Instant::now());

    assert!(harness.demux.should_skip_scan(miss_key, &packet));

    harness.route(&packet);

    assert_eq!(
        drain_ready_sessions(&mut harness.packet_loop_state),
        vec![session_key.clone()]
    );
    assert_eq!(
        harness
            .packet_loop_state
            .remote_addr_demux
            .session_key_for_remote_addr(harness.source_addr),
        Some(&session_key)
    );
    assert!(harness.demux.should_skip_scan(miss_key, &packet));
    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_datagram_routes_indexed(), 1);
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 0);
    assert_eq!(snapshot.rtc_datagram_drops_recent_miss_cache(), 0);
    Ok(())
}

#[test]
fn cached_rtcp_budget_drop_keeps_the_session_and_source_pin() -> Result<(), &'static str> {
    let PreparedRtcpBudgetDrop {
        mut harness,
        session_key,
        rtcp,
        now,
    } = prepare_authenticated_rtcp_budget_drop(&RtcpBudgetDropRoute::Cached)?;

    harness.route_at(&rtcp, now);

    assert!(drain_ready_sessions(&mut harness.packet_loop_state).is_empty());
    assert!(harness.packet_loop_state.users.contains_key(&session_key));
    assert_eq!(
        harness
            .packet_loop_state
            .remote_addr_demux
            .session_key_for_remote_addr(harness.source_addr),
        Some(&session_key)
    );
    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_datagram_routes_indexed(), 1);
    assert_eq!(snapshot.rtc_rtcp_ingress_budget_drops(), 1);
    Ok(())
}

#[test]
fn recovered_rtcp_budget_drop_relearns_the_source_pin() -> Result<(), &'static str> {
    let PreparedRtcpBudgetDrop {
        mut harness,
        session_key,
        rtcp,
        now,
    } = prepare_authenticated_rtcp_budget_drop(&RtcpBudgetDropRoute::Recovered)?;

    harness.route_at(&rtcp, now);

    assert!(drain_ready_sessions(&mut harness.packet_loop_state).is_empty());
    assert!(harness.packet_loop_state.users.contains_key(&session_key));
    assert_eq!(
        harness
            .packet_loop_state
            .remote_addr_demux
            .session_key_for_remote_addr(harness.source_addr),
        Some(&session_key)
    );
    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_datagram_routes_scan(), 2);
    assert_eq!(snapshot.rtc_rtcp_ingress_budget_drops(), 1);
    Ok(())
}

#[test]
fn stale_indexed_route_clears_worker_pin() {
    let mut harness = IngressRoutingHarness::new(45_048, 45_047);
    let stale_session_key = test_transport_session_key(51, 0, 58, UserId::Integer(59));

    assert!(
        harness
            .packet_loop_state
            .remote_addr_demux
            .remember_remote_addr(harness.source_addr, &stale_session_key)
    );
    harness.route(&sample_rtp_packet(11, 111));

    assert!(
        harness
            .packet_loop_state
            .remote_addr_demux
            .session_key_for_remote_addr(harness.source_addr)
            .is_none()
    );
    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 1);
    assert_eq!(snapshot.rtc_datagram_drops_no_user(), 1);
}

#[test]
fn recording_forward_destination_captures_packets_without_bypassing_the_contract() {
    let producer_session = test_transport_session_key(18, 0, 19, UserId::Integer(20));
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let sink = Arc::new(CountingSink::new());
    let mut harness = PacketLoopHarness::new();
    register_producer_media(&mut harness.state, &producer_session, "aud-up");

    packet_sink_registry.register_room(
        producer_session.room_instance_id(),
        Arc::<CountingSink>::clone(&sink),
        RtpForwardDestinationKind::Recording,
    );
    harness
        .buffers
        .pending_packets
        .push(sample_forwarded_packet(
            producer_session,
            "aud-up",
            b"payload",
        ));

    populate_forward_routes(
        &harness.state,
        &packet_sink_registry,
        harness.rtc_metrics.as_ref(),
        &mut harness.buffers.pending_packets,
        &mut harness.buffers.forwards,
    );
    flush_only_packet_forwards(
        &mut harness.state,
        &harness.metrics,
        &harness.rtp_metrics,
        &harness.rtc_metrics,
        &harness.buffers,
    );

    assert_eq!(harness.buffers.forwards.len(), 1);
    assert_eq!(sink.packets.load(Ordering::Relaxed), 1);
    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtp_payload_bytes_egress(), 0);
    assert_eq!(snapshot.rtp_forwarded_packets_recording(), 1);
}

#[test]
fn record_incoming_stats_learns_dynamic_rid_ssrc_bindings_from_rtp_extensions() {
    let producer_session = test_transport_session_key(88, 0, 89, UserId::Integer(90));
    let mut state = PacketLoopState::default();
    let src_media = register_producer_media(&mut state, &producer_session, "cam-up");
    let metrics = RuntimeMetrics::default();
    let packet_recorder = metrics.register_rtp_worker();
    let control_recorder = metrics.register_rtc_worker();
    let mut buffers = PacketLoopBuffers::new();

    buffers
        .pending_packets
        .push(sample_forwarded_packet_with_rid(
            producer_session.clone(),
            "cam-up",
            Some("hi"),
            b"payload",
        ));
    record_incoming_stats(
        &mut state,
        &SourcePolicySignal::default(),
        &control_recorder,
        &packet_recorder,
        &mut buffers,
    );

    let mut packet_without_extensions =
        sample_forwarded_packet_without_mid(producer_session, 4321, b"payload");
    assert_eq!(
        packet_without_extensions.resolve_src_media(&state),
        Some(src_media)
    );
    assert_eq!(
        packet_without_extensions
            .resolve_facts(&state)
            .and_then(|facts| facts.rid),
        Some(Rid::from("hi"))
    );
}

#[test]
fn selected_rid_keyframe_does_not_admit_an_earlier_delta_from_the_same_batch() {
    let producer_session = test_transport_session_key(188, 0, 189, UserId::Integer(190));
    let consumer_session = test_transport_session_key(188, 0, 191, UserId::Integer(192));
    let selected_rid = Rid::from("hi");
    let mut harness = PacketLoopHarness::new();
    let src_media = prepare_source_session_with_rid(
        &mut harness.state,
        &producer_session,
        Mid::from("cam-up"),
        4_321,
        Some(selected_rid),
    );
    harness.state.routes.refresh_packet_inspector(
        src_media,
        &RouterRtpParameters::new(
            vec![MediaFormat::new(
                RouterMediaKind::Video,
                CodecName::Vp8,
                PayloadType::new(111),
                90_000,
            )],
            vec![],
            vec![],
        ),
    );
    let consumer_mid = Mid::from("cam-down");
    let consumer_media = register_consumer_media(
        &mut harness.state,
        &consumer_session,
        consumer_mid,
        src_media,
    );
    insert_consumer_route_fixture(
        &mut harness.state,
        src_media,
        MediaRouteDestination {
            dest_session: consumer_session,
            dest_transport_media_id: consumer_media,
            dest_stream: ConsumerStreamHandle::default(),
            dest_mid: consumer_mid,
            dest_payload_type: None,
            repair_enabled: false,
            active: true,
            requires_decoder_refresh: true,
            delivery_generation: 0,
            packet_gate: PacketLayerGate::Block,
            pending_gate: Some(PacketLayerGate::Rid(selected_rid)),
        },
    );
    let mut packets = [
        sample_forwarded_packet_with_rid(
            producer_session.clone(),
            "cam-up",
            Some("hi"),
            &[
                0x10, 0x31, 0x00, 0x00, 0x9d, 0x01, 0x2a, 0x80, 0x02, 0x68, 0x01,
            ],
        ),
        sample_forwarded_packet_with_rid(
            producer_session,
            "cam-up",
            Some("hi"),
            &[
                0x10, 0x30, 0x00, 0x00, 0x9d, 0x01, 0x2a, 0x80, 0x02, 0x68, 0x01,
            ],
        ),
    ];
    let packet_sinks = PacketSinkRouteCache::default();

    for (packet_index, packet) in packets.iter_mut().enumerate() {
        record_incoming_packet(
            &mut harness.state,
            &harness.rtc_metrics,
            &harness.rtp_metrics,
            &mut harness.buffers,
            packet,
        );
        super::super::forwarding_planner::plan_forwards(
            &harness.state,
            &packet_sinks,
            &harness.rtc_metrics,
            packet,
            &mut harness.buffers.forwards,
        );
        if packet_index == 0 {
            assert!(harness.buffers.forwards.is_empty());
        }
    }
    finish_incoming_stats(
        &mut harness.state,
        &SourcePolicySignal::default(),
        &harness.rtc_metrics,
        &mut harness.buffers,
    );

    assert_eq!(harness.buffers.forwards.len(), 1);
}

#[test]
fn inactive_source_ignores_late_rid_readiness_and_first_ingress_feedback() {
    let producer_session = test_transport_session_key(89, 0, 90, UserId::Integer(91));
    let consumer_session = test_transport_session_key(89, 0, 92, UserId::Integer(93));
    let producer_mid = Mid::from("cam-up");
    let selected_rid = Rid::from("hi");
    let mut state = PacketLoopState::default();
    let src_media = prepare_source_session_with_rid(
        &mut state,
        &producer_session,
        producer_mid,
        4_321,
        Some(selected_rid),
    );
    state.routes.refresh_packet_inspector(
        src_media,
        &RouterRtpParameters::new(
            vec![MediaFormat::new(
                RouterMediaKind::Video,
                CodecName::H264,
                PayloadType::new(111),
                90_000,
            )],
            vec![],
            vec![],
        ),
    );
    register_consumer_route_fixture(
        &mut state,
        &consumer_session,
        "cam-down",
        src_media,
        true,
        PacketLayerGate::Block,
        Some(PacketLayerGate::Rid(selected_rid)),
    );
    assert!(set_source_route_active(&mut state, src_media, false));
    let metrics = RuntimeMetrics::default();
    let packet_recorder = metrics.register_rtp_worker();
    let control_recorder = metrics.register_rtc_worker();
    let mut buffers = PacketLoopBuffers::new();
    buffers
        .pending_packets
        .push(sample_forwarded_packet_with_rid(
            producer_session,
            "cam-up",
            Some("hi"),
            &[0x65, 0x88],
        ));

    record_incoming_stats(
        &mut state,
        &SourcePolicySignal::default(),
        &control_recorder,
        &packet_recorder,
        &mut buffers,
    );

    assert!(state.routes.local_route(src_media).is_some_and(|route| {
        route.destinations.iter().any(|destination| {
            destination.packet_gate == PacketLayerGate::Block
                && destination.pending_gate == Some(PacketLayerGate::Rid(selected_rid))
        })
    }));
    assert!(drain_ready_sessions(&mut state).is_empty());
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_forwarded(), 0);
    assert_eq!(snapshot.rtc_keyframe_requests_forwarded(), 0);
    assert_eq!(snapshot.rtp_payload_bytes_ingress(), 2);
}

#[test]
fn flush_packet_forwards_records_non_local_forwarding_volume_by_destination() {
    let source_session = test_transport_session_key(118, 0, 119, UserId::Integer(120));
    let src_media = TransportMediaId::new(121);
    let mut harness = PacketLoopHarness::new();
    let sink = Arc::new(CountingSink::new());
    let (relay_mailbox, mut intra_node_rx) = RelayPacketMailbox::channel_for_test();

    harness
        .buffers
        .pending_packets
        .push(sample_already_relayed_packet(
            source_session,
            src_media,
            "aud-up",
            b"payload",
        ));
    harness.add_recording_sink(src_media, &sink);
    harness.add_relay(src_media, relay_mailbox);
    flush_only_packet_forwards(
        &mut harness.state,
        &harness.metrics,
        &harness.rtp_metrics,
        &harness.rtc_metrics,
        &harness.buffers,
    );

    assert_eq!(sink.packets.load(Ordering::Relaxed), 1);
    assert!(intra_node_rx.try_recv().is_ok());

    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtp_forwarded_packets_local_rtc(), 0);
    assert_eq!(snapshot.rtp_forwarded_packets_recording(), 1);
    assert_eq!(snapshot.rtp_forwarded_packets_intra_node_relay(), 1);
    assert_eq!(snapshot.rtp_forwarded_payload_bytes_local_rtc(), 0);
    assert_eq!(snapshot.rtp_forwarded_payload_bytes_recording(), 7);
    assert_eq!(snapshot.rtp_forwarded_payload_bytes_intra_node_relay(), 7);
    assert_eq!(snapshot.rtp_payload_bytes_egress(), 0);
    assert_eq!(snapshot.rtc_relay_enqueue_intra_node_enqueued(), 1);
    assert_eq!(snapshot.rtc_relay_mailbox_depth_samples(), 1);
    assert_eq!(snapshot.rtc_relay_mailbox_depth_observed(), 1);
}

#[test]
fn flush_packet_forwards_records_packet_sink_source_key_for_local_packet()
-> Result<(), &'static str> {
    let source_session = test_transport_session_key(130, 0, 131, UserId::Integer(132));
    let src_media = TransportMediaId::new(133);
    let mut harness = PacketLoopHarness::new();
    create_rtc_session(&mut harness.state, &source_session, 9);
    let source_handle = harness
        .state
        .users
        .handle_for_key(&source_session)
        .ok_or("source handle missing after session setup")?;
    let sink = Arc::new(CountingSink::new());

    harness
        .buffers
        .pending_packets
        .push(sample_local_forwarded_packet(
            source_handle,
            "aud-up",
            b"payload",
        ));
    harness.add_recording_sink(src_media, &sink);

    flush_only_packet_forwards(
        &mut harness.state,
        &harness.metrics,
        &harness.rtp_metrics,
        &harness.rtc_metrics,
        &harness.buffers,
    );

    assert_eq!(sink.last_packet().0, Some(source_session.clone()));
    let _removed = harness.state.users.remove(&source_session);
    flush_only_packet_forwards(
        &mut harness.state,
        &harness.metrics,
        &harness.rtp_metrics,
        &harness.rtc_metrics,
        &harness.buffers,
    );
    assert_eq!(sink.packets.load(Ordering::Relaxed), 1);
    assert_eq!(
        harness.metrics.snapshot().rtp_forwarded_packets_recording(),
        2
    );
    Ok(())
}

#[test]
fn flush_packet_forwards_records_closed_relays_and_keeps_later_destinations() {
    let source_session = test_transport_session_key(119, 0, 120, UserId::Integer(121));
    let src_media = TransportMediaId::new(122);
    let mut harness = PacketLoopHarness::new();
    let sink = Arc::new(CountingSink::new());
    let (relay_mailbox, relay_rx) = RelayPacketMailbox::channel_for_test();

    harness
        .buffers
        .pending_packets
        .push(sample_already_relayed_packet(
            source_session,
            src_media,
            "aud-up",
            b"payload",
        ));

    drop(relay_rx);
    harness.add_relay(src_media, relay_mailbox);
    harness.add_recording_sink(src_media, &sink);
    flush_only_packet_forwards(
        &mut harness.state,
        &harness.metrics,
        &harness.rtp_metrics,
        &harness.rtc_metrics,
        &harness.buffers,
    );

    let snapshot = harness.metrics.snapshot();
    assert_eq!(sink.packets.load(Ordering::Relaxed), 1);
    assert_eq!(snapshot.rtc_relay_enqueue_intra_node_closed(), 1);
    assert_eq!(snapshot.rtc_relay_mailbox_depth_samples(), 1);
    assert_eq!(snapshot.rtp_forwarded_packets_intra_node_relay(), 0);
    assert_eq!(snapshot.rtp_relay_overload_drops_intra_node_relay(), 0);
}

#[test]
fn flush_packet_forwards_queues_normalized_repair_across_destinations() -> Result<(), &'static str>
{
    const PRIMARY_SSRC: u32 = 4_321;

    let repair_sequence = u64::from(rtp::RTP_SEQUENCE_NUMBER_MODULUS);
    let producer_session = test_transport_session_key(219, 0, 222, UserId::Integer(223));
    let consumer_session = test_transport_session_key(219, 0, 224, UserId::Integer(225));
    let consumer_mid = Mid::from("cam-down");
    let normalized_payload = b"normalized-primary-payload";
    let src_media = TransportMediaId::new(226);
    let mut harness = PacketLoopHarness::new();
    create_rtc_session(&mut harness.state, &producer_session, 45_052);
    let source_handle = harness
        .state
        .users
        .handle_for_key(&producer_session)
        .ok_or("source handle missing after session setup")?;
    declare_video_tx(
        &mut harness.state,
        &consumer_session,
        45_053,
        consumer_mid,
        227_001,
    );
    let consumer = harness
        .state
        .users
        .get_mut(&consumer_session)
        .ok_or("consumer session missing after setup")?;
    let consumer_stream = consumer.consumer_streams.allocate(consumer_mid);
    // A repair is admissible only after primary RTP exposes its gap.
    let mut project_primary = |sequence_number: u64, timestamp: u32| {
        consumer
            .consumer_streams
            .project_identity(
                consumer_stream,
                SourceRtpIdentity {
                    delivery_generation: 0,
                    ssrc: Ssrc::from(PRIMARY_SSRC),
                    seq_no: sequence_number.into(),
                    timestamp,
                    was_repair: false,
                },
                codec::PacketIdentity::default(),
            )
            .ok_or("primary packet should project")
    };
    project_primary(repair_sequence - 1, 10_000)?;
    let after_gap = project_primary(repair_sequence + 1, 12_000)?;
    insert_open_route(
        &mut harness.state,
        src_media,
        &consumer_session,
        TransportMediaId::new(227),
        consumer_stream,
        consumer_mid,
    );
    let packet =
        sample_local_repaired_packet(source_handle, "cam-up", repair_sequence, normalized_payload);
    harness.buffers.pending_packets.push(packet);
    let sink = Arc::new(CountingSink::new());
    let (relay_mailbox, mut relay_rx) = RelayPacketMailbox::channel_for_test();
    harness.add_local(src_media, 0);
    harness.add_relay(src_media, relay_mailbox);
    harness.add_recording_sink(src_media, &sink);

    flush_only_packet_forwards(
        &mut harness.state,
        &harness.metrics,
        &harness.rtp_metrics,
        &harness.rtc_metrics,
        &harness.buffers,
    );

    let local_write = harness
        .state
        .users
        .get(&consumer_session)
        .and_then(|session| session.last_local_write.as_ref())
        .ok_or("local repair should be written")?;
    assert!(local_write.0.is_next(after_gap.seq_no));
    assert_eq!(local_write.1.as_ref(), normalized_payload);
    assert_eq!(
        drain_ready_sessions(&mut harness.state),
        vec![consumer_session]
    );
    assert_eq!(
        sink.last_packet(),
        (Some(producer_session), normalized_payload.to_vec())
    );
    let relay_packet = relay_rx
        .try_recv()
        .map_err(|_error| "relay should receive the repaired packet")?;
    assert_eq!(relay_packet.payload(), normalized_payload);
    assert_eq!(
        relay_packet.repair_identity(),
        Some((
            Pt::from(111),
            Ssrc::from(PRIMARY_SSRC),
            repair_sequence.into()
        ))
    );
    Ok(())
}

#[test]
fn flush_packet_forwards_drops_stale_local_consumer_stream_handle() -> Result<(), &'static str> {
    let producer_session = test_transport_session_key(219, 0, 220, UserId::Integer(221));
    let consumer_session = test_transport_session_key(219, 0, 222, UserId::Integer(223));
    let consumer_mid = Mid::from("cam-down");
    let mut harness = PacketLoopHarness::new();
    declare_video_tx(
        &mut harness.state,
        &consumer_session,
        45_053,
        consumer_mid,
        224_001,
    );
    let egress_bitrate = harness
        .state
        .users
        .get(&consumer_session)
        .map(|session| Arc::clone(&session.egress_bitrate))
        .ok_or("consumer session missing after setup")?;

    let packet = sample_forwarded_packet(producer_session, "cam-up", b"payload");
    let received_at = packet.received_at();
    harness.buffers.pending_packets.push(packet);
    let src_media = TransportMediaId::new(225);
    insert_open_route(
        &mut harness.state,
        src_media,
        &consumer_session,
        TransportMediaId::new(224),
        ConsumerStreamHandle::default(),
        consumer_mid,
    );
    harness.add_local(src_media, 0);

    flush_only_packet_forwards(
        &mut harness.state,
        &harness.metrics,
        &harness.rtp_metrics,
        &harness.rtc_metrics,
        &harness.buffers,
    );

    assert!(drain_ready_sessions(&mut harness.state).is_empty());
    assert_eq!(
        harness.metrics.snapshot().rtp_forwarded_packets_local_rtc(),
        0
    );
    assert_eq!(egress_bitrate.last_observed_age(received_at), None);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn transport_health_events_use_session_room_id_and_deduplicate() {
    let _guard = capture().await;
    let worker = RtcWorker::default();
    let session = test_transport_session_key(318, 0, 319, UserId::Integer(7));
    assert!(
        worker
            .create_initial_session_offer("public-room-uuid", &session)
            .await
            .is_ok(),
        "session offer should be created"
    );

    let handle = worker.test_handle();
    let probe_session = session.clone();
    let room_id = handle
        .debug_handle
        .probe(
            move |state: &PacketLoopState, _: &super::super::worker::WorkerCommandContext<'_>| {
                state
                    .users
                    .get(&probe_session)
                    .map(|session| Arc::clone(&session.room_id))
            },
        )
        .await
        .flatten();
    assert!(room_id.is_some(), "session should reach the packet loop");
    let Some(room_id) = room_id else {
        return;
    };
    assert_eq!(room_id.as_ref(), "public-room-uuid");
    let rtc_metrics = worker.metrics.register_rtc_worker();
    let mut nack_totals = RtcNackTotals::default();

    let mut observe = |event| {
        observe_rtc_event(RtcEventContext {
            snapshot_state: &handle.snapshot_state,
            metrics: &worker.metrics,
            rtc_metrics: &rtc_metrics,
            nack_totals: &mut nack_totals,
            source_policy_signal: &worker.source_policy_signal,
            room_id: &room_id,
            session_key: &session,
            event,
        });
    };
    observe(&Event::Connected);
    observe(&Event::Connected);
    observe(&Event::IceConnectionStateChange(
        IceConnectionState::Disconnected,
    ));

    for (from, to) in [(None, "connected"), (Some("connected"), "disconnected")] {
        let mut fields = vec![
            ("room_id", Value::from("public-room-uuid")),
            ("user_id", Value::from("7")),
            ("media_worker_id", Value::from(0)),
            ("to", Value::from(to)),
        ];
        if let Some(from) = from {
            fields.push(("from", Value::from(from)));
        }
        assert_exact(TRANSPORT_HEALTH_CHANGED, &fields);
    }
}

#[test]
fn media_stats_record_exact_nack_deltas_per_stream() {
    let metrics = RuntimeMetrics::default();
    let rtc_metrics = metrics.register_rtc_worker();
    let snapshot_state = Arc::new(Mutex::new(RtcSnapshotState::default()));
    let source_policy_signal = SourcePolicySignal::default();
    let mut nack_totals = RtcNackTotals::default();
    let session = test_transport_session_key(320, 0, 321, UserId::Integer(8));
    let mid = Mid::from("video");
    let first_rid = Some(Rid::from("f"));
    let second_rid = Some(Rid::from("h"));
    let now = Instant::now();
    let ingress = |rid, nacks| {
        Event::MediaIngressStats(MediaIngressStats {
            mid,
            rid,
            bytes: 1,
            packets: 1,
            firs: 0,
            plis: 0,
            nacks,
            jitter: 0,
            rtt: None,
            loss: None,
            timestamp: now,
            remote: None,
        })
    };
    let egress = |nacks| {
        Event::MediaEgressStats(MediaEgressStats {
            mid,
            rid: first_rid,
            bytes: 1,
            packets: 1,
            firs: 0,
            plis: 0,
            nacks,
            rtt: None,
            loss: None,
            timestamp: now,
            remote: None,
        })
    };
    let mut observe = |event| {
        observe_rtc_event(RtcEventContext {
            snapshot_state: &snapshot_state,
            metrics: &metrics,
            rtc_metrics: &rtc_metrics,
            nack_totals: &mut nack_totals,
            source_policy_signal: &source_policy_signal,
            room_id: "room",
            session_key: &session,
            event: &event,
        });
    };

    observe(ingress(first_rid, 2));
    observe(ingress(second_rid, 3));
    observe(ingress(first_rid, 5));
    observe(egress(3));
    observe(egress(4));
    observe(egress(1));

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_nacks(RtcNackDirection::SentToPublisher), 8);
    assert_eq!(
        snapshot.rtc_nacks(RtcNackDirection::ReceivedFromSubscriber),
        5
    );
}

#[test]
fn packet_loop_dirty_marks_are_unique_until_the_session_is_drained() {
    let mut state = PacketLoopState::default();
    let session = test_transport_session_key(318, 0, 319, UserId::Integer(320));
    let now = Instant::now();

    create_rtc_session(&mut state, &session, 45_052);
    state.mark_session_dirty(&session);
    state.mark_session_dirty(&session);
    let ready_sessions = collect_ready_session_keys(&mut state, now);

    assert_eq!(ready_sessions, vec![session.clone()]);

    state.mark_session_dirty(&session);
    let ready_sessions = collect_ready_session_keys(&mut state, now);

    assert_eq!(ready_sessions, vec![session]);
}

#[test]
fn packet_loop_wakes_immediately_when_forwarding_marks_a_session_dirty() {
    let mut state = PacketLoopState::default();
    let session = test_transport_session_key(318, 0, 319, UserId::Integer(320));
    let future_timeout = Instant::now() + Duration::from_secs(30);

    create_rtc_session(&mut state, &session, 45_053);
    state.update_session_timeout(&session, Some(future_timeout));
    state.mark_session_dirty(&session);

    let deadline = super::loop_driver::next_timeout_deadline(&mut state);

    assert!(deadline.is_some_and(|deadline| deadline <= Instant::now()));
}

#[test]
fn due_relay_observation_expires_in_same_pump() -> Result<(), &'static str> {
    run_packet_loop_io_test(async {
        let mut state = PacketLoopState::default();
        let snapshot_state = Arc::new(Mutex::new(RtcSnapshotState::default()));
        let bitrate_registry = Arc::new(Mutex::new(BitrateRegistry::default()));
        let config = packet_loop_config_for_test();
        let mut demux = super::super::routing_miss::DemuxRecoveryState::new();
        let subscription = config.source_policy_signal.subscribe();
        let session = test_transport_session_key(400, 0, 401, UserId::Integer(402));
        let source_id = TransportMediaId::new(403);
        let (control_tx, _control_rx) = mpsc::channel(1);
        state
            .routes
            .register_remote_source(
                &TransportSourceKey::new(session.clone(), source_id),
                RemoteSourceControl::new(
                    control_tx,
                    RelayTargetId::new(1),
                    Arc::clone(&config.rtc_metrics),
                ),
            )
            .map_err(|_error| "remote source should register")?;

        let (_command_tx, command_rx) = mpsc::channel(1);
        let (relay_tx, relay_rx) = mpsc::channel(1);
        let mut inputs =
            PacketLoopInputReceivers::new(command_rx, relay_rx, CancellationToken::new());
        let observed_at = Instant::now()
            .checked_sub(Duration::from_millis(300))
            .ok_or("test instant should support a short subtraction")?;
        relay_tx
            .send(sample_already_relayed_audio_packet_at(
                session.clone(),
                source_id,
                observed_at,
            ))
            .await
            .map_err(|_error| "relay packet should enqueue")?;

        let started_at = Instant::now();
        let snapshot = PacketLoopTurn::new(started_at).pump(
            &mut state,
            &bitrate_registry,
            &snapshot_state,
            &config,
            &mut demux,
            &mut inputs,
        );

        assert!(
            state
                .routes
                .active_speaker_sources(Instant::now())
                .is_empty()
        );
        assert_eq!(
            subscription.take_pending_updates(),
            BTreeSet::from([session.room_instance_id()])
        );
        assert!(snapshot.next_timeout.is_none());
        Ok(())
    })
}

#[test]
fn packet_loop_waits_for_one_control_then_pumps_before_the_next_control() -> Result<(), &'static str>
{
    run_packet_loop_io_test(async {
        let mut state = PacketLoopState::default();
        let snapshot_state = Arc::new(Mutex::new(RtcSnapshotState::default()));
        let bitrate_registry = Arc::new(Mutex::new(BitrateRegistry::default()));
        let config = packet_loop_config_for_test();
        let mut demux = super::super::routing_miss::DemuxRecoveryState::new();
        let mut turn = PacketLoopTurn::new(Instant::now());
        let (_command_tx, command_rx) = mpsc::channel(1);
        let (_relay_tx, relay_rx) = mpsc::channel(1);
        let (probe_tx, probe_rx) = mpsc::channel(2);
        let mut inputs =
            PacketLoopInputReceivers::new(command_rx, relay_rx, CancellationToken::new())
                .with_probe_receiver(probe_rx);
        let session = test_transport_session_key(418, 0, 419, UserId::Integer(420));
        let mut shared_socket = test_socket()?;
        let (dirty_response, dirty_result) = oneshot::channel();
        let (queued_response, _queued_result) = oneshot::channel();
        bootstrap::ensure_session_rtc_state(
            &mut state.users,
            &session,
            shared_socket.candidate_addr,
            Bitrate::from_mbps(10),
        )
        .map_err(|_error| "RTC state should initialize")?;
        let source_addr = SocketAddr::from(([127, 0, 0, 1], 42_100));
        let packet = sample_rtp_packet(421, 422);
        let miss_key =
            PacketLoopRoutingMissKey::new(source_addr, shared_socket.candidate_addr, &packet);
        demux.record_miss(miss_key, &packet, source_addr, Instant::now());
        assert!(demux.should_skip_scan(miss_key, &packet));
        for response in [dirty_response, queued_response] {
            probe_tx
                .send(DebugProbeRequest::new(
                    MarkSessionDirtyProbe {
                        session_key: session.clone(),
                    },
                    response,
                ))
                .await
                .map_err(|_error| "dirty probe should enqueue")?;
        }

        let first_input = turn
            .wait_for_next_input(
                WaitPhaseSnapshot { next_timeout: None },
                &mut shared_socket.ingress,
                &mut inputs,
                &config.packet_loop_delay,
            )
            .await
            .ok_or("first queued control input should wake the turn")?;

        turn.apply_input(
            &mut PacketLoopApplyContext {
                packet_loop_state: &mut state,
                bitrate_registry: &bitrate_registry,
                snapshot_state: &snapshot_state,
                candidate_addr: shared_socket.candidate_addr,
                config: &config,
                demux: &mut demux,
                ingress: &shared_socket.ingress,
            },
            first_input,
        );
        assert!(!demux.should_skip_scan(miss_key, &packet));
        dirty_result
            .await
            .map_err(|_error| "dirty probe response should arrive")?;
        assert!(state.has_dirty_sessions());

        let snapshot = turn.pump(
            &mut state,
            &bitrate_registry,
            &snapshot_state,
            &config,
            &mut demux,
            &mut inputs,
        );

        assert!(!state.has_dirty_sessions());

        let second_input = turn
            .wait_for_next_input(
                snapshot,
                &mut shared_socket.ingress,
                &mut inputs,
                &config.packet_loop_delay,
            )
            .await
            .ok_or("second control input should wake after the pump")?;

        assert!(matches!(second_input, PacketLoopTurnInput::Control(_)));
        Ok(())
    })
}

#[test]
fn due_heartbeat_yields_to_ready_control_without_reporting_health() -> Result<(), &'static str> {
    run_packet_loop_io_test(async {
        let started_at = Instant::now()
            .checked_sub(Duration::from_millis(250))
            .ok_or("test instant should support a short subtraction")?;
        let mut state = PacketLoopState::default();
        let snapshot_state = Arc::new(Mutex::new(RtcSnapshotState::default()));
        let bitrate_registry = Arc::new(Mutex::new(BitrateRegistry::default()));
        let config = packet_loop_config_for_test();
        let mut demux = super::super::routing_miss::DemuxRecoveryState::new();
        let mut turn = PacketLoopTurn::new(started_at);
        let (_command_tx, command_rx) = mpsc::channel(1);
        let (_relay_tx, relay_rx) = mpsc::channel(1);
        let (probe_tx, probe_rx) = mpsc::channel(1);
        let mut inputs =
            PacketLoopInputReceivers::new(command_rx, relay_rx, CancellationToken::new())
                .with_probe_receiver(probe_rx);
        let mut shared_socket = test_socket()?;
        let (response, _result) = oneshot::channel();
        assert!(
            probe_tx
                .send(DebugProbeRequest::new(
                    MarkSessionDirtyProbe {
                        session_key: test_transport_session_key(422, 0, 423, UserId::Integer(424),),
                    },
                    response,
                ))
                .await
                .is_ok()
        );

        let snapshot = turn.pump(
            &mut state,
            &bitrate_registry,
            &snapshot_state,
            &config,
            &mut demux,
            &mut inputs,
        );
        turn.flush_outputs(&shared_socket.socket).await;
        let input = turn
            .wait_for_next_input(
                snapshot,
                &mut shared_socket.ingress,
                &mut inputs,
                &config.packet_loop_delay,
            )
            .await;

        assert!(matches!(input, Some(PacketLoopTurnInput::Control(_))));
        assert_eq!(
            config
                .packet_loop_delay
                .packet_loop_delay_ms_at(Instant::now()),
            Some(0)
        );
        Ok(())
    })
}

#[test]
fn heartbeat_wake_does_not_create_a_timeout_turn() -> Result<(), &'static str> {
    run_packet_loop_io_test(async {
        let started_at = Instant::now()
            .checked_sub(Duration::from_millis(250))
            .ok_or("test instant should support a short subtraction")?;
        let mut state = PacketLoopState::default();
        let snapshot_state = Arc::new(Mutex::new(RtcSnapshotState::default()));
        let bitrate_registry = Arc::new(Mutex::new(BitrateRegistry::default()));
        let config = packet_loop_config_for_test();
        let mut demux = super::super::routing_miss::DemuxRecoveryState::new();
        let mut turn = PacketLoopTurn::new(started_at);
        let (_command_tx, command_rx) = mpsc::channel(1);
        let (_relay_tx, relay_rx) = mpsc::channel(1);
        let mut inputs =
            PacketLoopInputReceivers::new(command_rx, relay_rx, CancellationToken::new());
        let mut shared_socket = test_socket()?;

        let snapshot = turn.pump(
            &mut state,
            &bitrate_registry,
            &snapshot_state,
            &config,
            &mut demux,
            &mut inputs,
        );
        turn.flush_outputs(&shared_socket.socket).await;
        let wait = timeout(
            Duration::from_millis(20),
            turn.wait_for_next_input(
                snapshot,
                &mut shared_socket.ingress,
                &mut inputs,
                &config.packet_loop_delay,
            ),
        )
        .await;

        assert!(
            wait.is_err(),
            "heartbeat-only wake should stay in the wait phase"
        );
        assert_eq!(
            config
                .packet_loop_delay
                .packet_loop_delay_ms_at(Instant::now()),
            None
        );
        Ok(())
    })
}

#[test]
fn packet_loop_wait_takes_one_completed_datagram_per_turn() -> Result<(), &'static str> {
    run_packet_loop_io_test(async {
        let mut state = PacketLoopState::default();
        let snapshot_state = Arc::new(Mutex::new(RtcSnapshotState::default()));
        let bitrate_registry = Arc::new(Mutex::new(BitrateRegistry::default()));
        let config = packet_loop_config_for_test();
        let mut demux = super::super::routing_miss::DemuxRecoveryState::new();
        let mut shared_socket = test_socket()?;
        let socket_addr = shared_socket.candidate_addr;
        let sender = StdUdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .map_err(|_error| "sender socket should bind")?;
        let (_command_tx, command_rx) = mpsc::channel(1);
        let (_relay_tx, relay_rx) = mpsc::channel(1);
        let mut inputs =
            PacketLoopInputReceivers::new(command_rx, relay_rx, CancellationToken::new());
        let mut turn = PacketLoopTurn::new(Instant::now());
        assert!(sender.send_to(b"one", socket_addr).is_ok());
        assert!(sender.send_to(b"second", socket_addr).is_ok());

        let input = turn
            .wait_for_next_input(
                WaitPhaseSnapshot { next_timeout: None },
                &mut shared_socket.ingress,
                &mut inputs,
                &config.packet_loop_delay,
            )
            .await
            .ok_or("queued datagram should wake the turn")?;

        let first_payload = match input {
            PacketLoopTurnInput::Datagram(ref datagram) => datagram.packet.clone(),
            _ => return Err("queued datagram should become a datagram input"),
        };
        turn.apply_input(
            &mut PacketLoopApplyContext {
                packet_loop_state: &mut state,
                bitrate_registry: &bitrate_registry,
                snapshot_state: &snapshot_state,
                candidate_addr: shared_socket.candidate_addr,
                config: &config,
                demux: &mut demux,
                ingress: &shared_socket.ingress,
            },
            input,
        );
        let snapshot = turn.pump(
            &mut state,
            &bitrate_registry,
            &snapshot_state,
            &config,
            &mut demux,
            &mut inputs,
        );

        let second_input = timeout(
            Duration::from_secs(1),
            turn.wait_for_next_input(
                snapshot,
                &mut shared_socket.ingress,
                &mut inputs,
                &config.packet_loop_delay,
            ),
        )
        .await
        .map_err(|_error| "second datagram should remain available for a later turn")?
        .ok_or("second datagram input should be delivered")?;

        let second_payload = match second_input {
            PacketLoopTurnInput::Datagram(datagram) => datagram.packet,
            _ => return Err("second queued datagram should become a datagram input"),
        };

        assert_ne!(first_payload, second_payload);
        assert!([b"one".as_slice(), b"second".as_slice()].contains(&first_payload.as_slice()));
        assert!([b"one".as_slice(), b"second".as_slice()].contains(&second_payload.as_slice()));
        Ok(())
    })
}

#[tokio::test]
async fn packet_loop_mailbox_wake_enters_next_pump_with_bounded_relay_drain() {
    let source_session = test_transport_session_key(26, 0, 27, UserId::Integer(28));
    let src_media = TransportMediaId::new(29);
    let (_command_tx, command_rx) = mpsc::channel(1);
    let packet_count = MAX_RELAY_PACKETS_PER_ITERATION + 2;
    let (relay_tx, relay_rx) = mpsc::channel(packet_count);
    let mut inputs = PacketLoopInputReceivers::new(command_rx, relay_rx, CancellationToken::new());
    let config = packet_loop_config_for_test();
    let mut state = PacketLoopState::default();
    let bitrate_registry = Arc::new(Mutex::new(BitrateRegistry::default()));
    let snapshot_state = Arc::new(Mutex::new(RtcSnapshotState::default()));
    let mut demux = super::super::routing_miss::DemuxRecoveryState::new();
    let mut turn = PacketLoopTurn::new(Instant::now());

    for _ in 0..packet_count {
        assert!(
            relay_tx
                .send(sample_already_relayed_packet(
                    source_session.clone(),
                    src_media,
                    "aud-up",
                    b"payload",
                ))
                .await
                .is_ok()
        );
    }

    assert!(matches!(
        inputs.recv_control_or_relay().await,
        Some(PacketLoopMailboxInput::Relay)
    ));
    turn.pump(
        &mut state,
        &bitrate_registry,
        &snapshot_state,
        &config,
        &mut demux,
        &mut inputs,
    );
    assert_eq!(inputs.relay_rx().len(), 1);
    let snapshot = config.metrics.snapshot();
    assert_eq!(
        usize::try_from(snapshot.rtp_payload_bytes_ingress()),
        Ok((MAX_RELAY_PACKETS_PER_ITERATION + 1) * b"payload".len())
    );
    assert_eq!(
        usize::try_from(snapshot.rtc_relay_drained_packets()),
        Ok(MAX_RELAY_PACKETS_PER_ITERATION)
    );
    assert_eq!(snapshot.rtc_relay_drain_cap_hits(), 1);

    turn.pump(
        &mut state,
        &bitrate_registry,
        &snapshot_state,
        &config,
        &mut demux,
        &mut inputs,
    );
    assert!(inputs.relay_rx().is_empty());
    assert_eq!(
        usize::try_from(config.metrics.snapshot().rtp_payload_bytes_ingress()),
        Ok(packet_count * b"payload".len())
    );
}

#[test]
fn silent_audio_packets_are_dropped_from_routed_fanout_after_transport_activity_tracking() {
    let producer_session = test_transport_session_key(28, 0, 29, UserId::Integer(30));
    let consumer_session = test_transport_session_key(28, 0, 31, UserId::Integer(32));
    let mut harness = PacketLoopHarness::new();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let src_media = register_producer_media(&mut harness.state, &producer_session, "aud-up");
    let consumer_mid = Mid::from("aud-down");
    let consumer_media = register_consumer_media(
        &mut harness.state,
        &consumer_session,
        consumer_mid,
        src_media,
    );
    harness.state.routes.add_consumer_route(
        src_media,
        MediaRouteDestination {
            dest_session: consumer_session.clone(),
            dest_transport_media_id: consumer_media,
            dest_stream: ConsumerStreamHandle::default(),
            dest_mid: consumer_mid,
            dest_payload_type: None,
            repair_enabled: false,
            active: true,
            requires_decoder_refresh: false,
            delivery_generation: 0,
            packet_gate: PacketLayerGate::Open,
            pending_gate: None,
        },
    );
    harness.state.set_consumer_dst_idx(
        &consumer_session,
        consumer_mid,
        consumer_media,
        src_media,
        Some(0),
    );
    harness
        .buffers
        .pending_packets
        .push(sample_forwarded_packet_with_audio_activity(
            producer_session,
            "aud-up",
            Some(false),
            Some(-72),
            b"payload",
        ));

    record_incoming_stats(
        &mut harness.state,
        &SourcePolicySignal::default(),
        &harness.rtc_metrics,
        &harness.rtp_metrics,
        &mut harness.buffers,
    );
    populate_forward_routes(
        &harness.state,
        &packet_sink_registry,
        harness.rtc_metrics.as_ref(),
        &mut harness.buffers.pending_packets,
        &mut harness.buffers.forwards,
    );

    assert!(harness.buffers.forwards.is_empty());
    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_layer_dropped(), 1);
    assert_eq!(snapshot.rtc_route_control_layer_allowed(), 0);
}

#[test]
fn repeated_active_audio_packets_do_not_republish_source_policy_dirty_room() {
    let producer_session = test_transport_session_key(39, 0, 40, UserId::Integer(41));
    let room_instance_id = producer_session.room_instance_id();
    let mut state = PacketLoopState::default();
    register_producer_media(&mut state, &producer_session, "aud-up");
    let metrics = RuntimeMetrics::default();
    let packet_recorder = metrics.register_rtp_worker();
    let control_recorder = metrics.register_rtc_worker();
    let source_policy_signal = SourcePolicySignal::default();
    let subscription = source_policy_signal.subscribe();
    let mut buffers = PacketLoopBuffers::new();

    buffers
        .pending_packets
        .push(sample_forwarded_packet_with_audio_activity(
            producer_session.clone(),
            "aud-up",
            Some(true),
            Some(-30),
            b"payload",
        ));
    record_incoming_stats(
        &mut state,
        &source_policy_signal,
        &control_recorder,
        &packet_recorder,
        &mut buffers,
    );
    assert_eq!(
        subscription.take_pending_updates(),
        BTreeSet::from([room_instance_id])
    );

    buffers.clear();
    buffers
        .pending_packets
        .push(sample_forwarded_packet_with_audio_activity(
            producer_session,
            "aud-up",
            Some(true),
            Some(-30),
            b"payload",
        ));
    record_incoming_stats(
        &mut state,
        &source_policy_signal,
        &control_recorder,
        &packet_recorder,
        &mut buffers,
    );
    assert!(subscription.take_pending_updates().is_empty());
}

#[test]
fn active_audio_rank_change_publishes_source_policy_dirty_room() {
    let first_producer_session = test_transport_session_key(42, 0, 43, UserId::Integer(44));
    let second_producer_session = test_transport_session_key(42, 0, 45, UserId::Integer(46));
    let room_instance_id = first_producer_session.room_instance_id();
    let mut state = PacketLoopState::default();
    let first_transport_media_id =
        register_producer_media(&mut state, &first_producer_session, "aud-first");
    let second_transport_media_id =
        register_producer_media(&mut state, &second_producer_session, "aud-second");
    let shared_observed_at = Instant::now();
    assert!(state.routes.observe_audio_activity(
        first_transport_media_id,
        Some(true),
        Some(-30),
        shared_observed_at,
    ));
    assert!(state.routes.observe_audio_activity(
        second_transport_media_id,
        Some(true),
        Some(-10),
        shared_observed_at,
    ));
    let metrics = RuntimeMetrics::default();
    let packet_recorder = metrics.register_rtp_worker();
    let control_recorder = metrics.register_rtc_worker();
    let source_policy_signal = SourcePolicySignal::default();
    let subscription = source_policy_signal.subscribe();
    let mut buffers = PacketLoopBuffers::new();

    buffers
        .pending_packets
        .push(sample_forwarded_packet_with_audio_activity(
            first_producer_session,
            "aud-first",
            Some(true),
            Some(-1),
            b"payload",
        ));
    record_incoming_stats(
        &mut state,
        &source_policy_signal,
        &control_recorder,
        &packet_recorder,
        &mut buffers,
    );

    assert_eq!(
        subscription.take_pending_updates(),
        BTreeSet::from([room_instance_id])
    );
}

#[test]
fn packet_loop_buffers_coalesce_source_policy_dirty_rooms_before_signal_flush() {
    let mut buffers = PacketLoopBuffers::new();
    let signal = SourcePolicySignal::default();
    let subscription = signal.subscribe();
    buffers.mark_source_policy_dirty(RoomInstanceId::from_raw(41));
    buffers.mark_source_policy_dirty(RoomInstanceId::from_raw(41));
    buffers.mark_source_policy_dirty(RoomInstanceId::from_raw(42));

    buffers.flush_source_policy_dirty(&signal);

    assert_eq!(
        subscription.take_pending_updates(),
        BTreeSet::from([RoomInstanceId::from_raw(41), RoomInstanceId::from_raw(42),])
    );
    assert!(subscription.take_pending_updates().is_empty());
}

#[test]
fn drain_relay_packets_ingests_owned_forwarded_packets_from_the_mailbox() {
    let source_session = test_transport_session_key(25, 0, 26, UserId::Integer(27));
    let packet = sample_forwarded_packet(source_session.clone(), "aud-up", b"payload");
    let (mailbox, mut relay_rx) = RelayPacketMailbox::channel_for_test();
    let mut pending_packets = Vec::new();
    let metrics = RuntimeMetrics::default();
    let rtc_metrics = metrics.register_rtc_worker();

    mailbox.forward_packet(
        &PacketLoopState::default(),
        &packet,
        TransportMediaId::new(17),
    );
    drain_relay_packets(
        &mut relay_rx,
        &mut pending_packets,
        MAX_RELAY_PACKETS_PER_ITERATION,
        &rtc_metrics,
    );

    assert_eq!(pending_packets.len(), 1);
    let forwarded = pending_packets.first_mut();
    assert!(forwarded.is_some());
    let Some(forwarded) = forwarded else {
        return;
    };
    assert_eq!(
        forwarded.src_key(&PacketLoopState::default()),
        Some(&source_session)
    );
    assert_eq!(forwarded.payload(), b"payload");
    assert_eq!(
        forwarded.resolve_src_media(&PacketLoopState::default()),
        Some(TransportMediaId::new(17))
    );
    assert_eq!(metrics.snapshot().rtc_relay_drained_packets(), 1);
}

#[test]
fn drain_relay_packets_stops_at_the_configured_cap() {
    let source_session = test_transport_session_key(26, 0, 27, UserId::Integer(28));
    let packet = sample_forwarded_packet(source_session, "aud-up", b"payload");
    let (mailbox, mut relay_rx) = RelayPacketMailbox::channel_for_test();
    let mut pending_packets = Vec::new();
    let metrics = RuntimeMetrics::default();
    let rtc_metrics = metrics.register_rtc_worker();

    mailbox.forward_packet(
        &PacketLoopState::default(),
        &packet,
        TransportMediaId::new(18),
    );
    mailbox.forward_packet(
        &PacketLoopState::default(),
        &packet,
        TransportMediaId::new(18),
    );

    let drained = drain_relay_packets(&mut relay_rx, &mut pending_packets, 1, &rtc_metrics);

    assert_eq!(drained, 1);
    assert_eq!(pending_packets.len(), 1);
    assert!(relay_rx.try_recv().is_ok());
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_relay_drain_batches(), 1);
    assert_eq!(snapshot.rtc_relay_drained_packets(), 1);
    assert_eq!(snapshot.rtc_relay_drain_cap_hits(), 1);
}

#[test]
fn flush_packet_forwards_records_relay_overload_drops() {
    let source_session = test_transport_session_key(29, 0, 30, UserId::Integer(31));
    let src_media = TransportMediaId::new(32);
    let mut harness = PacketLoopHarness::new();
    let (relay_mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test_with_capacity(1);

    harness
        .buffers
        .pending_packets
        .push(sample_already_relayed_packet(
            source_session.clone(),
            src_media,
            "aud-up",
            b"payload",
        ));

    relay_mailbox.forward_packet(
        &harness.state,
        &sample_forwarded_packet(source_session, "aud-up", b"prefill"),
        src_media,
    );
    harness.add_relay(src_media, relay_mailbox);

    flush_only_packet_forwards(
        &mut harness.state,
        &harness.metrics,
        &harness.rtp_metrics,
        &harness.rtc_metrics,
        &harness.buffers,
    );

    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtp_forwarded_packets_intra_node_relay(), 0);
    assert_eq!(snapshot.rtp_relay_overload_drops_intra_node_relay(), 1);
    assert_eq!(snapshot.rtc_relay_enqueue_intra_node_overloaded(), 1);
    assert_eq!(snapshot.rtc_relay_mailbox_depth_samples(), 1);
    assert_eq!(snapshot.rtc_relay_mailbox_depth_observed(), 1);
}

#[test]
fn flush_pending_kf_reqs_follow_route_scoped_feedback() {
    for case in route_feedback_cases() {
        let source_session =
            test_transport_session_key(case.id, 0, case.id + 100, UserId::Integer(1000));
        let consumer_session =
            test_transport_session_key(case.id, 1, case.id + 200, UserId::Integer(2000));
        let src_media = TransportMediaId::new(case.id);
        let target_id = RelayTargetId::new(case.id);
        let mut harness = PacketLoopHarness::new();
        let mut control_rx =
            harness.remote_keyframe_source(src_media, &source_session, target_id, 1);
        let (packet_gate, pending_gate) = case.gate.gates();
        register_consumer_route_fixture(
            &mut harness.state,
            &consumer_session,
            "cam-down",
            src_media,
            case.active,
            packet_gate,
            pending_gate,
        );
        drain_remote_packet_gate_setup(&mut control_rx);
        push_keyframe_request_with_rid(
            &mut harness.buffers,
            consumer_session,
            "cam-down",
            case.feedback_rid.map(Rid::from),
            case.kind,
        );

        flush_pending_kf_reqs_at(
            &mut harness.state,
            harness.rtc_metrics.as_ref(),
            &mut harness.buffers,
            Instant::now(),
        );

        match case.expected {
            RouteFeedbackExpectation::Drop => {
                assert_no_remote_keyframe_request(&mut control_rx);
                assert_eq!(harness.metrics.snapshot().rtc_route_control_forwarded(), 0);
            }
            RouteFeedbackExpectation::SourceWide => {
                assert_remote_keyframe_request(
                    &mut control_rx,
                    &source_session,
                    src_media,
                    target_id,
                    None,
                    case.kind,
                );
                assert_eq!(harness.metrics.snapshot().rtc_route_control_forwarded(), 1);
            }
            RouteFeedbackExpectation::Rid(rid) => {
                assert_remote_keyframe_request(
                    &mut control_rx,
                    &source_session,
                    src_media,
                    target_id,
                    Some(Rid::from(rid)),
                    case.kind,
                );
                assert_eq!(harness.metrics.snapshot().rtc_route_control_forwarded(), 1);
            }
        }
        assert_eq!(harness.metrics.snapshot().rtc_route_control_absorbed(), 0);
        assert_no_remote_keyframe_request(&mut control_rx);
    }
}

#[test]
fn flush_pending_kf_reqs_coalesces_duplicate_remote_requests() {
    let source_session = test_transport_session_key(81, 0, 82, UserId::Integer(83));
    let first_consumer_session = test_transport_session_key(81, 1, 84, UserId::Integer(85));
    let second_consumer_session = test_transport_session_key(81, 1, 86, UserId::Integer(87));
    let src_media = TransportMediaId::new(101);
    let target_id = RelayTargetId::new(4);
    let mut harness = PacketLoopHarness::new();
    let mut control_rx = harness.remote_keyframe_source(src_media, &source_session, target_id, 2);

    register_open_consumer_route_fixture(
        &mut harness.state,
        &first_consumer_session,
        "cam-down",
        src_media,
    );
    register_open_consumer_route_fixture(
        &mut harness.state,
        &second_consumer_session,
        "cam-down-2",
        src_media,
    );
    drain_remote_packet_gate_setup(&mut control_rx);
    push_keyframe_request(
        &mut harness.buffers,
        first_consumer_session,
        "cam-down",
        KeyframeRequestKind::Pli,
    );
    push_keyframe_request(
        &mut harness.buffers,
        second_consumer_session,
        "cam-down-2",
        KeyframeRequestKind::Fir,
    );

    flush_pending_kf_reqs_at(
        &mut harness.state,
        harness.rtc_metrics.as_ref(),
        &mut harness.buffers,
        Instant::now(),
    );

    assert_remote_keyframe_request(
        &mut control_rx,
        &source_session,
        src_media,
        target_id,
        None,
        KeyframeRequestKind::Fir,
    );
    assert_no_remote_keyframe_request(&mut control_rx);
    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_forwarded(), 1);
    assert_eq!(snapshot.rtc_route_control_absorbed(), 0);
}

#[test]
fn flush_pending_kf_reqs_coalesces_each_rid_separately() {
    let source_session = test_transport_session_key(84, 0, 85, UserId::Integer(86));
    let first_consumer_session = test_transport_session_key(84, 1, 87, UserId::Integer(88));
    let second_consumer_session = test_transport_session_key(84, 1, 89, UserId::Integer(90));
    let src_media = TransportMediaId::new(104);
    let first_rid = Rid::from("lo");
    let second_rid = Rid::from("hi");
    let target_id = RelayTargetId::new(6);
    let mut harness = PacketLoopHarness::new();
    let mut control_rx = harness.remote_keyframe_source(src_media, &source_session, target_id, 4);
    register_consumer_route_fixture(
        &mut harness.state,
        &first_consumer_session,
        "cam-down-1",
        src_media,
        true,
        PacketLayerGate::Rid(first_rid),
        None,
    );
    register_consumer_route_fixture(
        &mut harness.state,
        &second_consumer_session,
        "cam-down-2",
        src_media,
        true,
        PacketLayerGate::Rid(second_rid),
        None,
    );
    drain_remote_packet_gate_setup(&mut control_rx);
    push_keyframe_request(
        &mut harness.buffers,
        first_consumer_session.clone(),
        "cam-down-1",
        KeyframeRequestKind::Pli,
    );
    push_keyframe_request(
        &mut harness.buffers,
        second_consumer_session.clone(),
        "cam-down-2",
        KeyframeRequestKind::Pli,
    );
    push_keyframe_request(
        &mut harness.buffers,
        first_consumer_session,
        "cam-down-1",
        KeyframeRequestKind::Fir,
    );
    push_keyframe_request(
        &mut harness.buffers,
        second_consumer_session,
        "cam-down-2",
        KeyframeRequestKind::Fir,
    );

    flush_pending_kf_reqs_at(
        &mut harness.state,
        harness.rtc_metrics.as_ref(),
        &mut harness.buffers,
        Instant::now(),
    );

    let mut requests = Vec::new();
    for _ in 0..2 {
        let (source, actual_target_id, rid, kind) = recv_remote_keyframe_request(&mut control_rx);
        assert_eq!(source.session_key(), &source_session);
        assert_eq!(source.transport_media_id(), src_media);
        assert_eq!(actual_target_id, target_id);
        requests.push((rid, kind));
    }
    assert!(requests.contains(&(Some(first_rid), KeyframeRequestKind::Fir)));
    assert!(requests.contains(&(Some(second_rid), KeyframeRequestKind::Fir)));
    assert_no_remote_keyframe_request(&mut control_rx);
    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_forwarded(), 2);
    assert_eq!(snapshot.rtc_route_control_absorbed(), 0);
}

#[test]
fn flush_pending_kf_reqs_keeps_sources_separate_for_same_rid() {
    let first_source_session = test_transport_session_key(91, 0, 92, UserId::Integer(93));
    let second_source_session = test_transport_session_key(91, 2, 94, UserId::Integer(95));
    let first_consumer_session = test_transport_session_key(91, 1, 96, UserId::Integer(97));
    let second_consumer_session = test_transport_session_key(91, 1, 98, UserId::Integer(99));
    let first_src_media = TransportMediaId::new(105);
    let second_src_media = TransportMediaId::new(106);
    let rid = Rid::from("hi");
    let first_target_id = RelayTargetId::new(7);
    let second_target_id = RelayTargetId::new(8);
    let mut harness = PacketLoopHarness::new();
    let mut first_control_rx =
        harness.remote_keyframe_source(first_src_media, &first_source_session, first_target_id, 2);
    let mut second_control_rx = harness.remote_keyframe_source(
        second_src_media,
        &second_source_session,
        second_target_id,
        2,
    );
    register_consumer_route_fixture(
        &mut harness.state,
        &first_consumer_session,
        "cam-down-1",
        first_src_media,
        true,
        PacketLayerGate::Rid(rid),
        None,
    );
    register_consumer_route_fixture(
        &mut harness.state,
        &second_consumer_session,
        "cam-down-2",
        second_src_media,
        true,
        PacketLayerGate::Rid(rid),
        None,
    );
    drain_remote_packet_gate_setup(&mut first_control_rx);
    drain_remote_packet_gate_setup(&mut second_control_rx);
    push_keyframe_request(
        &mut harness.buffers,
        first_consumer_session,
        "cam-down-1",
        KeyframeRequestKind::Pli,
    );
    push_keyframe_request(
        &mut harness.buffers,
        second_consumer_session,
        "cam-down-2",
        KeyframeRequestKind::Pli,
    );

    flush_pending_kf_reqs_at(
        &mut harness.state,
        harness.rtc_metrics.as_ref(),
        &mut harness.buffers,
        Instant::now(),
    );

    assert_remote_keyframe_request(
        &mut first_control_rx,
        &first_source_session,
        first_src_media,
        first_target_id,
        Some(rid),
        KeyframeRequestKind::Pli,
    );
    assert_remote_keyframe_request(
        &mut second_control_rx,
        &second_source_session,
        second_src_media,
        second_target_id,
        Some(rid),
        KeyframeRequestKind::Pli,
    );
    assert_no_remote_keyframe_request(&mut first_control_rx);
    assert_no_remote_keyframe_request(&mut second_control_rx);
}

#[test]
fn keyframe_tracker_cancels_due_retry_when_route_goes_inactive() {
    let source_session = test_transport_session_key(187, 0, 188, UserId::Integer(189));
    let consumer_session = test_transport_session_key(187, 1, 190, UserId::Integer(191));
    let src_media = TransportMediaId::new(1870);
    let target_id = RelayTargetId::new(187);
    let now = Instant::now();
    let mut harness = PacketLoopHarness::new();
    let mut control_rx = harness.remote_keyframe_source(src_media, &source_session, target_id, 2);
    register_open_consumer_route_fixture(
        &mut harness.state,
        &consumer_session,
        "cam-down",
        src_media,
    );
    drain_remote_packet_gate_setup(&mut control_rx);

    push_keyframe_request(
        &mut harness.buffers,
        consumer_session.clone(),
        "cam-down",
        KeyframeRequestKind::Pli,
    );
    flush_pending_kf_reqs_at(
        &mut harness.state,
        harness.rtc_metrics.as_ref(),
        &mut harness.buffers,
        now,
    );
    assert_remote_keyframe_request(
        &mut control_rx,
        &source_session,
        src_media,
        target_id,
        None,
        KeyframeRequestKind::Pli,
    );
    push_keyframe_request(
        &mut harness.buffers,
        consumer_session.clone(),
        "cam-down",
        KeyframeRequestKind::Pli,
    );
    flush_pending_kf_reqs_at(
        &mut harness.state,
        harness.rtc_metrics.as_ref(),
        &mut harness.buffers,
        now + Duration::from_millis(100),
    );
    assert!(set_source_route_active(
        &mut harness.state,
        src_media,
        false
    ));

    drain_due_kf_retries(
        &mut harness.state,
        harness.rtc_metrics.as_ref(),
        &mut harness.buffers,
        now + Duration::from_secs(1),
    );

    assert_no_remote_keyframe_request(&mut control_rx);
    assert!(set_source_route_active(&mut harness.state, src_media, true));
    push_keyframe_request(
        &mut harness.buffers,
        consumer_session,
        "cam-down",
        KeyframeRequestKind::Pli,
    );
    flush_pending_kf_reqs_at(
        &mut harness.state,
        harness.rtc_metrics.as_ref(),
        &mut harness.buffers,
        now + Duration::from_secs(1) + Duration::from_millis(10),
    );

    assert_remote_keyframe_request(
        &mut control_rx,
        &source_session,
        src_media,
        target_id,
        None,
        KeyframeRequestKind::Pli,
    );
    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_keyframe_requests_absorbed(), 1);
    assert_eq!(snapshot.rtc_keyframe_requests_retried(), 0);
}
