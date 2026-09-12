use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use str0m::{
    Rtc,
    format::{Codec, PayloadParams},
    media::{KeyframeRequestKind, MediaKind, Mid, Pt},
    rtp::{RtpWrite, Ssrc},
};
use tokio::sync::oneshot;

#[path = "session_drain_peer.rs"]
mod peer;
use self::peer::{
    TestDatagram, capture_compound_nack, connect_rtc_pair, deliver_rtp, drain_mutation, take_rtcp,
    take_written_rtp,
};
use super::{super::buffers::PacketLoopBuffers, *};
use crate::{
    Bitrate, CodecPreferences, MediaCodecFlags, SessionBitrateLimits, VideoBitrateLimits,
    engine::{
        UserId,
        media_transport::{
            ReceiverBweTargetUpdate, SourcePolicySignal, TransportMediaId,
            rtc::{
                RtcWorkerConfig,
                bootstrap::test_support::ensure_session_rtc_state,
                codec::RtpProfile,
                commands::{RtcWorkerCommand, WorkerMediaControlBatch},
                consumer_egress::test_support::RTX_CACHE_LIFETIME,
                control::{WorkerCommandContext, handle_worker_command},
                packet_loop::{
                    forwarded_packet::ForwardedPacket,
                    forwarding_destination::{ForwardingDestination, LocalRtcPacketDestination},
                    ingress_routing::{PacketRouteDatagram, route_pkt_to_session_at},
                    routing_miss::DemuxRecoveryState,
                },
                state::{
                    PacketLoopState, RtcSnapshotState, TransportSessionHealth, muxed_rtp_ssrc,
                    route_control::PacketLayerGate,
                    slots::ConsumerStreamHandle,
                    source_route::{DecoderDelivery, MediaRouteDestination},
                },
                test_support::{
                    sample_forwarded_packet, sample_forwarded_packet_without_mid,
                    test_transport_session_key,
                },
            },
        },
        metrics::{self, RuntimeMetrics, test_support::RuntimeMetricsSnapshotTestExt},
    },
};

const MISSING_PACKETS_PER_STREAM: u16 = 100;
const NACK_INTERVAL: Duration = Duration::from_millis(33);

#[test]
fn output_budget_rejects_the_first_transmit_past_the_private_packet_limit() {
    let mut packet_budget = SessionOutputBudget::new(SESSION_OUTPUT_LIMITS);
    for _ in 0..SESSION_DRAIN_MAX_TRANSMITS {
        assert_eq!(packet_budget.try_charge(0), Ok(()));
    }
    assert_eq!(
        packet_budget.try_charge(0),
        Err(RtcOutputBudgetLimit::Packets)
    );
}

#[test]
fn output_budget_rejects_the_first_byte_past_the_private_payload_limit() {
    let mut byte_budget = SessionOutputBudget::new(SESSION_OUTPUT_LIMITS);
    assert_eq!(
        byte_budget.try_charge(SESSION_DRAIN_MAX_PAYLOAD_BYTES),
        Ok(())
    );
    assert_eq!(
        byte_budget.try_charge(1),
        Err(RtcOutputBudgetLimit::PayloadBytes)
    );

    let bytes_per_transmit = SESSION_DRAIN_MAX_PAYLOAD_BYTES / SESSION_DRAIN_MAX_TRANSMITS;
    let mut joint_budget = SessionOutputBudget::new(SESSION_OUTPUT_LIMITS);
    for _ in 0..SESSION_DRAIN_MAX_TRANSMITS {
        assert_eq!(joint_budget.try_charge(bytes_per_transmit), Ok(()));
    }
    assert_eq!(
        joint_budget.try_charge(1),
        Err(RtcOutputBudgetLimit::PacketsAndPayloadBytes)
    );
}

#[test]
fn session_drain_rollback_preserves_the_prior_session_prefix() {
    let healthy_session = test_transport_session_key(1, 0, 2, UserId::Integer(3));
    let offender_session = test_transport_session_key(1, 0, 4, UserId::Integer(5));
    let destination = SocketAddr::from(([127, 0, 0, 1], 46_000));
    let mut buffers = PacketLoopBuffers::new();
    buffers.push_pending_transmit(destination, b"healthy-transmit".to_vec());
    buffers.pending_packets.push(sample_forwarded_packet(
        healthy_session.clone(),
        "healthy",
        b"healthy-packet",
    ));
    buffers.pending_keyframe_requests.push((
        healthy_session.clone(),
        PendingKeyframeRequest {
            consumer_mid: Mid::from("healthy"),
            consumer_rid: None,
            kind: KeyframeRequestKind::Pli,
        },
    ));
    let checkpoint = buffers.checkpoint_session_drain();

    buffers.push_pending_transmit(destination, b"offender-transmit".to_vec());
    buffers.pending_packets.push(sample_forwarded_packet(
        offender_session.clone(),
        "offender",
        b"offender-packet",
    ));
    buffers.pending_keyframe_requests.push((
        offender_session,
        PendingKeyframeRequest {
            consumer_mid: Mid::from("offender"),
            consumer_rid: None,
            kind: KeyframeRequestKind::Fir,
        },
    ));

    buffers.rollback_session_drain(&checkpoint);

    assert_eq!(buffers.pending_transmits.len(), 1);
    assert_eq!(
        buffers
            .pending_transmits
            .first()
            .map(|transmit| transmit.contents.as_slice()),
        Some(b"healthy-transmit".as_slice())
    );
    assert_eq!(buffers.pending_packets.len(), 1);
    assert_eq!(
        buffers
            .pending_packets
            .first()
            .map(ForwardedPacket::payload),
        Some(b"healthy-packet".as_slice())
    );
    assert_eq!(buffers.pending_keyframe_requests.len(), 1);
    assert_eq!(
        buffers
            .pending_keyframe_requests
            .first()
            .map(|(session_key, _request)| session_key),
        Some(&healthy_session)
    );
}

struct LocalWriteDrainFixture {
    state: PacketLoopState,
    peer: Rtc,
    consumer: TransportSessionKey,
    stream: ConsumerStreamHandle,
    destination: LocalRtcPacketDestination,
    candidate_addr: SocketAddr,
    primary: Ssrc,
    repair: Ssrc,
    now: Instant,
}

impl LocalWriteDrainFixture {
    fn new(media_kind: MediaKind, repair_enabled: bool) -> Result<Self, &'static str> {
        let consumer = test_transport_session_key(7, 0, 8, UserId::Integer(9));
        let mut state = PacketLoopState::default();
        let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 46_010));
        let (now, payload_type, peer) = connect_local_write_session(
            &mut state,
            &consumer,
            candidate_addr,
            media_kind,
            repair_enabled,
        )?;
        let mid = Mid::from("retired-write");
        let stream = state
            .users
            .get_mut(&consumer)
            .ok_or("consumer RTC should remain registered")?
            .consumer_streams
            .allocate(mid);
        let src_media = TransportMediaId::new(10);
        let dst_idx = state.routes.add_consumer_route(
            src_media,
            MediaRouteDestination {
                dest_session: consumer.clone(),
                dest_transport_media_id: TransportMediaId::new(11),
                dest_stream: stream,
                dest_mid: mid,
                dest_payload_type: Some(payload_type),
                repair_enabled,
                active: true,
                delivery: DecoderDelivery::fixture(false, PacketLayerGate::Open, None),
            },
        );
        let ForwardingDestination::LocalRtc(destination) =
            ForwardingDestination::from_local_route_destination(src_media, dst_idx)
        else {
            return Err("local write fixture requires a local RTC destination");
        };
        Ok(Self {
            state,
            peer,
            consumer,
            stream,
            destination,
            candidate_addr,
            primary: Ssrc::from(9_001),
            repair: Ssrc::from(9_002),
            now,
        })
    }

    fn queue_source(&mut self, source_ssrc: u32) {
        let packet = sample_forwarded_packet_without_mid(
            test_transport_session_key(12, 0, 13, UserId::Integer(14)),
            source_ssrc,
            b"payload",
        );
        assert_eq!(self.destination.send(&mut self.state, &packet), Some(7));
    }

    fn deliver_primary(&mut self, buffers: PacketLoopBuffers) -> Result<(), &'static str> {
        let transmit = buffers
            .pending_transmits
            .into_iter()
            .find(|transmit| muxed_rtp_ssrc(&transmit.contents) == Some(self.primary))
            .ok_or("drain should emit one primary RTP packet")?;
        deliver_rtp(
            &mut self.peer,
            &TestDatagram::udp(self.candidate_addr, transmit.destination, transmit.contents),
            self.now,
        )
    }

    fn queue_gap(&mut self) -> Result<(), &'static str> {
        self.queue_source(5_001);
        let first = self.drain();
        self.deliver_primary(first)?;
        self.queue_source(5_002);
        self.drain();
        self.queue_source(5_003);
        let tail = self.drain();
        self.deliver_primary(tail)
    }

    fn has_repair(&self, buffers: &PacketLoopBuffers) -> bool {
        buffers
            .pending_transmits
            .iter()
            .any(|transmit| muxed_rtp_ssrc(&transmit.contents) == Some(self.repair))
    }

    fn rtx_cache_is_armed(&self) -> Result<bool, &'static str> {
        self.state
            .users
            .get(&self.consumer)
            .map(|session| session.consumer_streams.rtx_cache_is_armed(self.stream))
            .ok_or("consumer RTC should remain registered")
    }

    fn has_rtp(buffers: &PacketLoopBuffers) -> bool {
        buffers
            .pending_transmits
            .iter()
            .any(|transmit| muxed_rtp_ssrc(&transmit.contents).is_some())
    }

    fn nack_gap(&mut self) -> Result<PacketLoopBuffers, &'static str> {
        self.now += NACK_INTERVAL;
        let feedback = capture_compound_nack(&mut self.peer, self.now, 1, 1)?;
        if !feedback
            .reports
            .iter()
            .all(|(ssrc, _, _)| *ssrc == *self.primary)
        {
            return Err("peer NACK should target the primary stream");
        }
        feedback.datagram.deliver(
            &mut self
                .state
                .users
                .get_mut(&self.consumer)
                .ok_or("consumer RTC should remain registered for NACK")?
                .rtc,
            self.now,
        )?;
        self.state.mark_session_dirty(&self.consumer);
        Ok(self.drain())
    }

    fn route_nack(&mut self, datagram: &TestDatagram, received_at: Instant) {
        let metrics = RuntimeMetrics::default();
        let rtc_metrics = metrics.register_rtc_worker();
        route_pkt_to_session_at(
            &mut self.state,
            &mut DemuxRecoveryState::new(),
            &rtc_metrics,
            PacketRouteDatagram::new(
                datagram.source,
                datagram.destination,
                &datagram.contents,
                received_at,
            ),
        );
        assert!(self.state.has_dirty_sessions());
    }

    fn drain(&mut self) -> PacketLoopBuffers {
        let metrics = RuntimeMetrics::default();
        let rtc_metrics = metrics.register_rtc_worker();
        let bitrate_registry = Arc::new(Mutex::new(BitrateRegistry::default()));
        let snapshot_state = Arc::new(Mutex::new(RtcSnapshotState::default()));
        let source_policy_signal = SourcePolicySignal::default();
        let context = SessionDrainContext::new(
            &snapshot_state,
            &bitrate_registry,
            &metrics,
            &rtc_metrics,
            &source_policy_signal,
        );
        let mut buffers = PacketLoopBuffers::new();
        assert!(!drain_ready_sessions(
            &mut self.state,
            &context,
            &mut buffers,
            self.now,
        ));
        buffers
    }
}

fn connect_local_write_session(
    state: &mut PacketLoopState,
    consumer: &TransportSessionKey,
    candidate_addr: SocketAddr,
    media_kind: MediaKind,
    repair_enabled: bool,
) -> Result<(Instant, Pt, Rtc), &'static str> {
    let peer_addr = SocketAddr::from(([127, 0, 0, 1], 46_011));
    ensure_session_rtc_state(
        &mut state.users,
        consumer,
        candidate_addr,
        Bitrate::from_mbps(10),
    )
    .map_err(|_error| "consumer RTC state should initialize")?;
    let profile = RtpProfile::compile(MediaCodecFlags::default(), CodecPreferences::default())
        .map_err(|_error| "test RTP profile should compile")?;
    let started_at = Instant::now();
    let mut peer = profile
        .session_config()
        .enable_raw_packets(true)
        .build(started_at);
    let server = state
        .users
        .get_mut(consumer)
        .map(|session| &mut session.rtc)
        .ok_or("consumer RTC should exist")?;
    let now = connect_rtc_pair(
        server,
        &mut peer,
        candidate_addr,
        peer_addr,
        started_at + Duration::from_secs(1),
    )?;
    let codec = match media_kind {
        MediaKind::Audio => Codec::Opus,
        MediaKind::Video => Codec::Vp8,
    };
    let payload_type = server
        .codec_config()
        .find(|params| params.spec().codec == codec)
        .map(PayloadParams::pt)
        .ok_or("media payload type should exist")?;
    let mid = Mid::from("retired-write");
    let primary = Ssrc::from(9_001);
    let repair = Ssrc::from(9_002);
    {
        let mut api = server.direct_api();
        api.declare_media(mid, media_kind);
        api.declare_stream_tx(primary, repair_enabled.then_some(repair), mid, None)
            .set_unpaced(true);
    }
    drain_mutation(server)?;
    {
        let mut api = peer.direct_api();
        api.declare_media(mid, media_kind);
        api.expect_stream_rx(primary, repair_enabled.then_some(repair), mid, None);
    }
    drain_mutation(&mut peer)?;
    Ok((now, payload_type, peer))
}

#[test]
fn invalidated_local_write_rotates_on_its_real_transmit() -> Result<(), &'static str> {
    let mut fixture = LocalWriteDrainFixture::new(MediaKind::Video, true)?;
    fixture.queue_source(5_001);
    let baseline = fixture.drain();
    fixture.deliver_primary(baseline)?;

    fixture.queue_source(5_002);
    let session = fixture
        .state
        .users
        .get_mut(&fixture.consumer)
        .ok_or("consumer RTC should remain registered")?;
    assert_eq!(
        session.consumer_streams.rtx_write_counts(fixture.stream),
        Some((1, 0))
    );
    session.invalidate_rtx_stream(fixture.stream);
    assert_eq!(
        session.consumer_streams.rtx_write_counts(fixture.stream),
        Some((1, 1))
    );

    let stale = fixture.drain();
    assert!(!stale.pending_transmits.is_empty());
    let session = fixture
        .state
        .users
        .get_mut(&fixture.consumer)
        .ok_or("consumer RTC should remain registered after drain")?;
    assert_eq!(
        session.consumer_streams.rtx_write_counts(fixture.stream),
        Some((0, 0))
    );
    assert!(!session.consumer_streams.rtx_cache_is_armed(fixture.stream));

    fixture.queue_source(5_003);
    let after_gap = fixture.drain();
    fixture.deliver_primary(after_gap)?;
    let retransmits = fixture.nack_gap()?;
    assert!(!fixture.has_repair(&retransmits));
    Ok(())
}

#[test]
fn nack_admitted_before_cache_deadline_retransmits_before_expiry() -> Result<(), &'static str> {
    let mut fixture = LocalWriteDrainFixture::new(MediaKind::Video, true)?;
    fixture.queue_gap()?;
    let deadline = fixture.now + RTX_CACHE_LIFETIME;
    let nack = capture_compound_nack(&mut fixture.peer, fixture.now + NACK_INTERVAL, 1, 1)?;

    let before_deadline = deadline
        .checked_sub(Duration::from_nanos(1))
        .ok_or("cache deadline should follow fixture time")?;
    fixture.route_nack(&nack.datagram, before_deadline);
    fixture.now = deadline;
    let retransmits = fixture.drain();

    assert!(fixture.has_repair(&retransmits));
    assert!(!fixture.rtx_cache_is_armed()?);
    Ok(())
}

#[test]
fn dirty_drain_expires_cache_without_rtcp_input() -> Result<(), &'static str> {
    let mut fixture = LocalWriteDrainFixture::new(MediaKind::Video, true)?;
    fixture.queue_source(5_001);
    fixture.drain();
    assert!(fixture.rtx_cache_is_armed()?);

    fixture.now += RTX_CACHE_LIFETIME;
    fixture.state.mark_session_dirty(&fixture.consumer);
    fixture.drain();
    assert!(!fixture.rtx_cache_is_armed()?);
    Ok(())
}

#[test]
fn expired_local_rtx_cache_drops_nack_without_blocking_pli() -> Result<(), &'static str> {
    let mut fixture = LocalWriteDrainFixture::new(MediaKind::Video, true)?;
    fixture.queue_gap()?;
    assert!(fixture.rtx_cache_is_armed()?);

    let deadline = fixture.now + RTX_CACHE_LIFETIME;
    let nack = capture_compound_nack(&mut fixture.peer, fixture.now + NACK_INTERVAL, 1, 1)?;
    fixture.route_nack(&nack.datagram, deadline);
    fixture.now = deadline;
    let retransmits = fixture.drain();
    assert!(!fixture.has_repair(&retransmits));
    assert!(!fixture.rtx_cache_is_armed()?);

    let mid = Mid::from("retired-write");
    fixture
        .peer
        .direct_api()
        .stream_rx_by_mid(mid, None)
        .ok_or("peer receive stream should exist")?
        .request_keyframe(KeyframeRequestKind::Pli);
    let keyframe_feedback = take_rtcp(&mut fixture.peer, fixture.now)?;
    let session = fixture
        .state
        .users
        .get_mut(&fixture.consumer)
        .ok_or("consumer RTC should remain registered for PLI")?;
    for feedback in keyframe_feedback {
        feedback.deliver(&mut session.rtc, fixture.now)?;
    }
    fixture.state.mark_session_dirty(&fixture.consumer);
    let feedback = fixture.drain();
    assert!(matches!(
        feedback.pending_keyframe_requests.as_slice(),
        [(session_key, request)]
            if session_key == &fixture.consumer
                && request.consumer_mid == mid
                && request.kind == KeyframeRequestKind::Pli
    ));
    Ok(())
}

#[test]
fn rtx_ratio_cap_suppresses_repeated_nack_before_cache_expiry() -> Result<(), &'static str> {
    let mut fixture = LocalWriteDrainFixture::new(MediaKind::Video, true)?;
    fixture.queue_gap()?;
    // The gap drain samples the ratio before any resend. The first NACK at
    // 33 ms reuses that sample, then the second observes the resend debt.
    let first = fixture.nack_gap()?;
    assert!(fixture.has_repair(&first));
    let capped = fixture.nack_gap()?;
    assert!(!LocalWriteDrainFixture::has_rtp(&capped));
    assert!(
        fixture
            .state
            .users
            .get(&fixture.consumer)
            .is_some_and(|session| session.consumer_streams.rtx_cache_is_armed(fixture.stream))
    );
    Ok(())
}

#[test]
fn audio_and_video_without_repair_do_not_answer_authenticated_nack() -> Result<(), &'static str> {
    for media_kind in [MediaKind::Audio, MediaKind::Video] {
        let mut fixture = LocalWriteDrainFixture::new(media_kind, false)?;
        fixture.queue_gap()?;
        let retransmits = fixture.nack_gap()?;
        assert!(!LocalWriteDrainFixture::has_rtp(&retransmits));
    }
    Ok(())
}

struct NackDrainFixture {
    state: PacketLoopState,
    peer: Rtc,
    offender: TransportSessionKey,
    sibling: TransportSessionKey,
    candidate_addr: SocketAddr,
    source_addr: SocketAddr,
    now: Instant,
    metrics: RuntimeMetrics,
    rtc_metrics: Arc<RtcMetricsRecorder>,
    bitrate_registry: Arc<Mutex<BitrateRegistry>>,
    snapshot_state: Arc<Mutex<RtcSnapshotState>>,
    source_policy_signal: SourcePolicySignal,
    config: RtcWorkerConfig,
    buffers: PacketLoopBuffers,
}

impl NackDrainFixture {
    fn new(stream_count: u32, payload_len: usize) -> Result<Self, &'static str> {
        let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 46_100));
        let source_addr = SocketAddr::from(([127, 0, 0, 1], 46_101));
        let offender = test_transport_session_key(10, 0, 11, UserId::Integer(12));
        let sibling = test_transport_session_key(10, 0, 13, UserId::Integer(14));
        let mut state = PacketLoopState::default();
        for session_key in [&offender, &sibling] {
            ensure_session_rtc_state(
                &mut state.users,
                session_key,
                candidate_addr,
                Bitrate::from_mbps(10),
            )
            .map_err(|_error| "RTC state should initialize")?;
        }

        let profile = Arc::new(
            RtpProfile::compile(MediaCodecFlags::default(), CodecPreferences::default())
                .map_err(|_error| "test RTP profile should compile")?,
        );
        let started_at = Instant::now();
        let mut peer = profile
            .session_config()
            .enable_raw_packets(true)
            .build(started_at);
        let server = state
            .users
            .get_mut(&offender)
            .map(|session| &mut session.rtc)
            .ok_or("offender RTC should exist")?;
        let now = connect_rtc_pair(
            server,
            &mut peer,
            candidate_addr,
            source_addr,
            started_at + Duration::from_secs(1),
        )?;
        configure_nack_streams(server, &mut peer, stream_count, payload_len, now)?;

        assert!(
            state
                .remote_addr_demux
                .remember_remote_addr(source_addr, &offender)
        );
        let metrics = RuntimeMetrics::default();
        let rtc_metrics = metrics.register_rtc_worker();
        let offender_egress = state
            .users
            .get(&offender)
            .map(|session| Arc::clone(&session.egress_bitrate))
            .ok_or("offender RTC should exist before drain")?;
        let bitrate_registry = Arc::new(Mutex::new(BitrateRegistry::default()));
        bitrate_registry
            .lock()
            .map_err(|_error| "bitrate registry should lock")?
            .register_session_egress(&offender, offender_egress);
        let snapshot_state = Arc::new(Mutex::new(RtcSnapshotState::default()));
        for session_key in [&offender, &sibling] {
            let previous = snapshot_state
                .lock()
                .map_err(|_error| "snapshot state should lock")?
                .set_transport_health(session_key, TransportSessionHealth::Connected);
            metrics.record_transport_health_transition(
                previous.map(metrics::transport_health_state),
                Some(metrics::transport_health_state(
                    TransportSessionHealth::Connected,
                )),
            );
        }

        Ok(Self {
            state,
            peer,
            offender,
            sibling,
            candidate_addr,
            source_addr,
            now,
            metrics,
            rtc_metrics,
            bitrate_registry,
            snapshot_state,
            source_policy_signal: SourcePolicySignal::default(),
            config: RtcWorkerConfig {
                bitrate_limits: SessionBitrateLimits::new(
                    Bitrate::from_mbps(8),
                    Bitrate::from_mbps(10),
                ),
                video_bitrate_limits: VideoBitrateLimits::default(),
                profile,
                media_quality_interval: None,
                media_id_base: 0,
            },
            buffers: PacketLoopBuffers::new(),
        })
    }

    fn route_nack(&mut self, datagram: &TestDatagram, now: Instant) {
        assert_eq!(datagram.source, self.source_addr);
        assert_eq!(datagram.destination, self.candidate_addr);
        route_pkt_to_session_at(
            &mut self.state,
            &mut DemuxRecoveryState::new(),
            &self.rtc_metrics,
            PacketRouteDatagram::new(
                datagram.source,
                datagram.destination,
                &datagram.contents,
                now,
            ),
        );
    }

    fn drain(&mut self, now: Instant) -> bool {
        self.drain_with_limits(now, SESSION_OUTPUT_LIMITS)
    }

    fn drain_with_limits(&mut self, now: Instant, output_limits: SessionOutputLimits) -> bool {
        let mut context = SessionDrainContext::new(
            &self.snapshot_state,
            &self.bitrate_registry,
            &self.metrics,
            &self.rtc_metrics,
            &self.source_policy_signal,
        );
        context.output_limits = output_limits;
        drain_ready_sessions(&mut self.state, &context, &mut self.buffers, now)
    }

    fn apply_receiver_bwe_target(&mut self, now: Instant, target: Bitrate) {
        let (response, _outcome) = oneshot::channel();
        handle_worker_command(
            &mut self.state,
            &WorkerCommandContext {
                bitrate_registry: &self.bitrate_registry,
                snapshot_state: &self.snapshot_state,
                candidate_addr: self.candidate_addr,
                now,
                config: &self.config,
                runtime_metrics: &self.metrics,
                rtc_metrics: &self.rtc_metrics,
            },
            RtcWorkerCommand::ApplyMediaControlBatch {
                batch: WorkerMediaControlBatch::ReceiverBwe(vec![(
                    0,
                    ReceiverBweTargetUpdate::new(self.offender.clone(), target),
                )]),
                response,
            },
        );
    }
}

fn configure_nack_streams(
    server: &mut Rtc,
    peer: &mut Rtc,
    stream_count: u32,
    payload_len: usize,
    now: Instant,
) -> Result<(), &'static str> {
    let payload_type = server
        .codec_config()
        .find(|params| params.spec().codec == Codec::Vp8 && params.resend().is_some())
        .map(PayloadParams::pt)
        .ok_or("VP8 with RTX payload types should exist")?;
    let payload: Arc<[u8]> = vec![0; payload_len].into();
    for index in 0..stream_count {
        let mid = Mid::from(format!("nack-{index}").as_str());
        let primary = Ssrc::from(10_000 + index * 2);
        let repair = Ssrc::from(10_001 + index * 2);
        {
            let mut api = server.direct_api();
            api.declare_media(mid, MediaKind::Video);
            api.declare_stream_tx(primary, Some(repair), mid, None)
                .set_unpaced(true);
        }
        drain_mutation(server)?;
        {
            let mut api = peer.direct_api();
            api.declare_media(mid, MediaKind::Video);
            api.expect_stream_rx(primary, Some(repair), mid, None);
        }
        drain_mutation(peer)?;
        cache_nackable_gap(server, peer, mid, primary, payload_type, &payload, now)?;
        server
            .direct_api()
            .stream_tx_by_mid(mid, None)
            .ok_or("server transmit stream should exist")?
            .set_unpaced(false);
        drain_mutation(server)?;
    }
    Ok(())
}

fn cache_nackable_gap(
    server: &mut Rtc,
    peer: &mut Rtc,
    mid: Mid,
    ssrc: Ssrc,
    payload_type: Pt,
    payload: &Arc<[u8]>,
    now: Instant,
) -> Result<(), &'static str> {
    let first = write_nackable_rtp(server, mid, ssrc, payload_type, payload, 0, now)?;
    deliver_rtp(peer, &first, now)?;
    for sequence_number in 1..=MISSING_PACKETS_PER_STREAM {
        let _dropped = write_nackable_rtp(
            server,
            mid,
            ssrc,
            payload_type,
            payload,
            sequence_number,
            now,
        )?;
    }
    let tail = write_nackable_rtp(
        server,
        mid,
        ssrc,
        payload_type,
        payload,
        MISSING_PACKETS_PER_STREAM + 1,
        now,
    )?;
    deliver_rtp(peer, &tail, now)
}

fn write_nackable_rtp(
    server: &mut Rtc,
    mid: Mid,
    ssrc: Ssrc,
    payload_type: Pt,
    payload: &Arc<[u8]>,
    sequence_number: u16,
    now: Instant,
) -> Result<TestDatagram, &'static str> {
    server
        .direct_api()
        .stream_tx_by_mid(mid, None)
        .ok_or("server transmit stream should exist")?
        .write_rtp(
            RtpWrite::new(
                payload_type,
                u64::from(sequence_number).into(),
                u32::from(sequence_number) * 3_000,
                now,
                Arc::clone(payload),
            )
            .nackable(true),
        );
    take_written_rtp(server, now, ssrc, sequence_number)
}

fn assert_output_budget_metrics(metrics: &RuntimeMetrics, exhausted: Option<RtcOutputBudgetLimit>) {
    let snapshot = metrics.snapshot();
    for limit in [
        RtcOutputBudgetLimit::Packets,
        RtcOutputBudgetLimit::PayloadBytes,
        RtcOutputBudgetLimit::PacketsAndPayloadBytes,
    ] {
        assert_eq!(
            snapshot.rtc_output_budget_exhaustions(limit),
            u64::from(exhausted == Some(limit))
        );
    }
    assert_eq!(
        snapshot.rtc_output_budget_session_closes(),
        u64::from(exhausted.is_some())
    );
    assert_eq!(snapshot.rtc_rtcp_ingress_budget_drops(), 0);
}

fn assert_authenticated_nack_exhaustion(
    output_limits: SessionOutputLimits,
    expected_limit: RtcOutputBudgetLimit,
) -> Result<(), &'static str> {
    let mut fixture = NackDrainFixture::new(1, 1)?;
    let offender_handle = fixture
        .state
        .users
        .handle_for_key(&fixture.offender)
        .ok_or("offender handle should exist")?;
    let sibling_handle = fixture
        .state
        .users
        .handle_for_key(&fixture.sibling)
        .ok_or("sibling handle should exist")?;
    fixture.state.mark_session_dirty(&fixture.sibling);
    let nack_at = fixture.now + NACK_INTERVAL;
    let feedback = capture_compound_nack(
        &mut fixture.peer,
        nack_at,
        1,
        u32::from(MISSING_PACKETS_PER_STREAM),
    )?;
    fixture.route_nack(&feedback.datagram, nack_at);
    fixture
        .buffers
        .push_pending_transmit(fixture.candidate_addr, b"healthy-prefix".to_vec());

    assert!(fixture.drain_with_limits(nack_at, output_limits));
    assert!(!fixture.state.users.contains_key(&fixture.offender));
    assert!(fixture.state.users.contains_key(&fixture.sibling));
    let snapshot = fixture
        .snapshot_state
        .lock()
        .map_err(|_error| "snapshot state should lock after drain")?;
    assert_eq!(
        snapshot.transport_health(&fixture.offender),
        Some(TransportSessionHealth::Disconnected)
    );
    assert_eq!(
        snapshot.transport_health(&fixture.sibling),
        Some(TransportSessionHealth::Connected)
    );
    drop(snapshot);
    assert!(!fixture.state.has_dirty_sessions());
    assert!(
        fixture
            .state
            .users
            .get_by_handle(offender_handle)
            .is_none_or(|session| session.next_timeout.is_none())
    );
    assert!(
        fixture
            .state
            .users
            .get_by_handle(sibling_handle)
            .is_some_and(|session| session.next_timeout.is_some())
    );
    assert_eq!(fixture.buffers.pending_transmits.len(), 1);
    assert_eq!(
        fixture
            .buffers
            .pending_transmits
            .first()
            .map(|transmit| transmit.contents.as_slice()),
        Some(b"healthy-prefix".as_slice())
    );
    assert!(
        fixture
            .state
            .remote_addr_demux
            .session_key_for_remote_addr(fixture.source_addr)
            .is_none()
    );
    assert!(
        !fixture
            .bitrate_registry
            .lock()
            .map_err(|_error| "bitrate registry should lock after drain")?
            .egress_bitrates_by_session
            .contains_key(&fixture.offender)
    );
    assert_output_budget_metrics(&fixture.metrics, Some(expected_limit));
    worker_close_session(
        &mut fixture.state,
        &fixture.bitrate_registry,
        &fixture.snapshot_state,
        &fixture.offender,
        SessionCloseDisposition::OwnerClose,
        &fixture.metrics,
    );
    let snapshot = fixture
        .snapshot_state
        .lock()
        .map_err(|_error| "snapshot state should lock after owner close")?;
    assert_eq!(snapshot.transport_health(&fixture.offender), None);
    assert_eq!(
        snapshot.transport_health(&fixture.sibling),
        Some(TransportSessionHealth::Connected)
    );
    drop(snapshot);
    Ok(())
}

#[test]
fn authenticated_nack_packet_exhaustion_isolates_only_the_offender() -> Result<(), &'static str> {
    assert_authenticated_nack_exhaustion(
        SessionOutputLimits {
            transmits: 0,
            payload_bytes: SESSION_DRAIN_MAX_PAYLOAD_BYTES,
        },
        RtcOutputBudgetLimit::Packets,
    )
}

#[test]
fn authenticated_nack_payload_exhaustion_isolates_only_the_offender() -> Result<(), &'static str> {
    assert_authenticated_nack_exhaustion(
        SessionOutputLimits {
            transmits: SESSION_DRAIN_MAX_TRANSMITS,
            payload_bytes: 0,
        },
        RtcOutputBudgetLimit::PayloadBytes,
    )
}

#[test]
fn authenticated_compound_nack_remains_below_production_output_limits() -> Result<(), &'static str>
{
    let mut fixture = NackDrainFixture::new(6, 1)?;
    let nack_at = fixture.now + NACK_INTERVAL;
    let feedback = capture_compound_nack(&mut fixture.peer, nack_at, 6, 600)?;
    fixture.route_nack(&feedback.datagram, nack_at);

    assert!(!fixture.drain(nack_at));
    let transmit_count = fixture.buffers.pending_transmits.len();
    let payload_bytes = fixture
        .buffers
        .pending_transmits
        .iter()
        .map(|transmit| transmit.contents.len())
        .sum::<usize>();
    assert_eq!(transmit_count, 1);
    assert!(transmit_count < SESSION_DRAIN_MAX_TRANSMITS);
    assert!(payload_bytes < SESSION_DRAIN_MAX_PAYLOAD_BYTES);
    assert!(fixture.state.users.contains_key(&fixture.offender));
    assert!(fixture.state.users.contains_key(&fixture.sibling));
    assert_output_budget_metrics(&fixture.metrics, None);
    Ok(())
}

#[test]
fn repeated_nack_rounds_remain_paced_and_below_host_limits() -> Result<(), &'static str> {
    let mut fixture = NackDrainFixture::new(2, 1_000)?;
    let initial_bwe_target = Bitrate::from_mbps(2);
    let reduced_bwe_target = Bitrate::from_kbps(500);
    fixture.apply_receiver_bwe_target(fixture.now, initial_bwe_target);
    let first_nack_at = fixture.now + NACK_INTERVAL;
    let mut expected_reports = None;
    let mut largest_drain = 0;
    let mut largest_payload = 0;

    for round in 0_u32..5 {
        let nack_at = first_nack_at + NACK_INTERVAL * round;
        if round == 2 {
            fixture.apply_receiver_bwe_target(nack_at, reduced_bwe_target);
        }
        let feedback = capture_compound_nack(&mut fixture.peer, nack_at, 2, 200)?;
        if let Some(reports) = &expected_reports {
            assert_eq!(&feedback.reports, reports);
        } else {
            expected_reports = Some(feedback.reports.clone());
        }
        fixture.buffers.clear();
        fixture.route_nack(&feedback.datagram, nack_at);
        assert!(!fixture.drain(nack_at));
        let transmit_count = fixture.buffers.pending_transmits.len();
        let payload_bytes = fixture
            .buffers
            .pending_transmits
            .iter()
            .map(|transmit| transmit.contents.len())
            .sum::<usize>();
        assert!(transmit_count < SESSION_DRAIN_MAX_TRANSMITS);
        assert!(payload_bytes < SESSION_DRAIN_MAX_PAYLOAD_BYTES);
        largest_drain = largest_drain.max(transmit_count);
        largest_payload = largest_payload.max(payload_bytes);
    }

    assert!(largest_drain > 0);
    assert!(largest_payload > 0);
    assert!(largest_drain < usize::from(MISSING_PACKETS_PER_STREAM) * 2);
    assert!(fixture.state.users.contains_key(&fixture.offender));
    let offender = fixture
        .state
        .users
        .get(&fixture.offender)
        .ok_or("offender should remain after bounded NACK rounds")?;
    assert_eq!(offender.receiver_bwe_target, Some(reduced_bwe_target));
    assert_eq!(offender.receiver_bwe_str0m_update_count, 2);
    assert_output_budget_metrics(&fixture.metrics, None);
    Ok(())
}
