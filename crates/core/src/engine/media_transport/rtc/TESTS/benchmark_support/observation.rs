use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use o_sfu_rfc::rtp::CodecName;
use o_sfu_router::{
    MediaKind as RouterMediaKind,
    rtp::{MediaFormat, MediaStream as RouterRtpParameters, PayloadType, StreamBinding},
};
use str0m::{
    media::{Mid, Pt, Rid},
    rtp::{Ssrc, Vp8Descriptor},
};

use super::super::{
    codec,
    packet_loop::{PacketForwarder, record_incoming_stats_for_benchmark},
    state::{PacketLoopState, bitrate::BitrateRegistry},
    test_support::{
        BenchmarkPacketStaging, BenchmarkStreamIdentity, prepare_source_session_with_rid,
        reset_packet_resolution, restage_packet_for_benchmark,
        sample_local_forwarded_packet_for_benchmark,
        sample_local_forwarded_packet_without_mid_for_benchmark, test_transport_session_key,
    },
    worker::PacketLoopBuffers,
};
use crate::engine::{
    UserId,
    media_transport::{SourcePolicySignal, SourcePolicyUpdateSubscription},
    metrics::{RtcMetricsRecorder, RtpMetricsRecorder, RuntimeMetrics},
};

const INCOMING_OBSERVATION_TURNS: usize = 512;
const VP8_DESCRIPTOR_BYTES: usize = 6;
const VP8_KEYFRAME: &[u8] = &[
    0x90, 0xe0, 0x80, 0x02, 0x09, 0x00, 0x00, 0x00, 0x00, 0x9d, 0x01, 0x2a, 0x80, 0x02, 0x68, 0x01,
];
const VP8_INTERFRAME: &[u8] = &[0x90, 0xe0, 0x80, 0x03, 0x0a, 0x20, 0x01, 0x00, 0x00];

/// fixed packet-observation fixture for packet-loop ingress benchmarks
///
/// Setup declares one local RTC producer with a RID-only negotiated encoding,
/// one incoming bitrate counter and two reusable RTP packets. The first packet
/// learns the encoding's SSRC from authenticated MID and RID metadata. The
/// second omits both extensions and resolves through that learned SSRC.
pub struct IncomingObservationBenchFixture {
    state: PacketLoopState,
    buffers: PacketLoopBuffers,
    forwarder: PacketForwarder,
    source_policy_signal: SourcePolicySignal,
    source_policy_updates: SourcePolicyUpdateSubscription,
    route_metrics: Arc<RtcMetricsRecorder>,
    rtp_metrics: Arc<RtpMetricsRecorder>,
}

impl IncomingObservationBenchFixture {
    #[must_use]
    pub fn mid_rid_then_ssrc() -> Self {
        Self::build(
            b"observed-payload",
            b"steady-payload",
            &RouterRtpParameters::new(vec![], vec![], vec![StreamBinding::new().with_rid("hi")]),
        )
    }

    /// # Panics
    ///
    /// Panics when the static VP8 packets do not match the negotiated fixture.
    #[must_use]
    pub fn negotiated_vp8() -> Self {
        let parameters = RouterRtpParameters::new(
            vec![MediaFormat::new(
                RouterMediaKind::Video,
                CodecName::Vp8,
                PayloadType::new(111),
                90_000,
            )],
            vec![],
            vec![StreamBinding::new().with_rid("hi")],
        );
        assert!(
            negotiated_vp8_payloads_are_valid(&parameters),
            "negotiated VP8 benchmark payloads must be valid"
        );
        Self::build(VP8_KEYFRAME, VP8_INTERFRAME, &parameters)
    }

    #[expect(
        clippy::expect_used,
        reason = "the fixed benchmark fixture must fail if RTC setup is incomplete"
    )]
    fn build(
        first_payload: &[u8],
        second_payload: &[u8],
        parameters: &RouterRtpParameters,
    ) -> Self {
        let source_session = test_transport_session_key(101, 0, 102, UserId::Integer(103));
        let mid = Mid::from("cam-up");
        let mut state = PacketLoopState::default();
        let src_media = prepare_source_session_with_rid(
            &mut state,
            &source_session,
            mid,
            4321,
            Some(Rid::from("hi")),
        );
        state.refresh_producer_ssrcs(&source_session, mid, parameters);
        assert!(
            state
                .producer_binding_for_ssrc(&source_session, Ssrc::from(4321))
                .is_none(),
            "observation benchmark must learn the SSRC from its first packet"
        );
        let session_handle = state
            .users
            .handle_for_key(&source_session)
            .expect("observation benchmark session must have a local handle");

        let now = Instant::now();
        let mut bitrate_registry = BitrateRegistry::default();
        let bitrate_counter =
            bitrate_registry.register_incoming_media(&source_session, src_media, now);
        state.register_incoming_bitrate_counter(src_media, bitrate_counter);

        let metrics = RuntimeMetrics::default();
        let route_metrics = metrics.register_rtc_worker();
        let rtp_metrics = metrics.register_rtp_worker();
        let source_policy_signal = SourcePolicySignal::default();
        let source_policy_updates = source_policy_signal.subscribe();
        let mut buffers = PacketLoopBuffers::new();
        let identity = BenchmarkStreamIdentity {
            ssrc: 4321,
            payload_type: 111,
        };
        let mut first_packet = sample_local_forwarded_packet_for_benchmark(
            session_handle,
            "cam-up",
            Some("hi"),
            identity,
            Arc::from(first_payload),
        );
        restage_packet_for_benchmark(
            &mut first_packet,
            BenchmarkPacketStaging {
                sequence_number: 1,
                rtp_timestamp: 1234,
                voice_activity: Some(true),
                audio_level: Some(-24),
                ..BenchmarkPacketStaging::default()
            },
            None,
            now,
        );
        buffers.pending_packets.push(first_packet);
        let mut second_packet = sample_local_forwarded_packet_without_mid_for_benchmark(
            session_handle,
            "cam-up",
            Some("hi"),
            identity,
            Arc::from(second_payload),
        );
        restage_packet_for_benchmark(
            &mut second_packet,
            BenchmarkPacketStaging {
                sequence_number: 2,
                rtp_timestamp: 4234,
                ..BenchmarkPacketStaging::default()
            },
            None,
            now + Duration::from_millis(1),
        );
        buffers.pending_packets.push(second_packet);

        Self {
            state,
            buffers,
            forwarder: PacketForwarder::default(),
            source_policy_signal,
            source_policy_updates,
            route_metrics,
            rtp_metrics,
        }
    }

    #[must_use]
    pub fn observe_turns(&mut self) -> usize {
        for _ in 0..INCOMING_OBSERVATION_TURNS {
            for packet in &mut self.buffers.pending_packets {
                reset_packet_resolution(packet);
            }
            record_incoming_stats_for_benchmark(
                &mut self.state,
                &self.source_policy_signal,
                &self.route_metrics,
                &self.rtp_metrics,
                &mut self.forwarder,
                &mut self.buffers.pending_packets,
            );
        }
        self.source_policy_updates.take_pending_updates().len()
    }

    /// # Panics
    ///
    /// Panics if the first packet did not learn its SSRC or the second packet
    /// did not resolve through that learned binding.
    #[expect(
        clippy::expect_used,
        clippy::panic,
        reason = "fixture validation must fail when the observation path is skipped"
    )]
    pub fn assert_observation_coverage(&self) {
        let [first, second] = self.buffers.pending_packets.as_slice() else {
            panic!("observation fixture must contain two packets");
        };
        let source_key = first
            .src_key(&self.state)
            .expect("first packet must have a source session");
        let first_media = first
            .cached_facts()
            .expect("first packet must have observed source facts")
            .src_media;
        assert_eq!(
            self.state
                .producer_binding_for_ssrc(source_key, Ssrc::from(4321)),
            Some((first_media, Some("hi".into()))),
            "first packet must learn the negotiated RID and SSRC"
        );
        assert_eq!(
            self.state
                .incoming_bitrate_counters
                .get(&first_media)
                .and_then(|counter| counter.last_observed_age(second.received_at())),
            Some(Duration::ZERO),
            "SSRC-only packet must reach the incoming bitrate observer"
        );
    }
}

fn negotiated_vp8_payloads_are_valid(parameters: &RouterRtpParameters) -> bool {
    let (Ok(keyframe), Ok(interframe)) = (
        Vp8Descriptor::parse(VP8_KEYFRAME),
        Vp8Descriptor::parse(VP8_INTERFRAME),
    ) else {
        return false;
    };
    let inspector = codec::PacketInspector::from_parameters(parameters);
    let keyframe_packet = inspector.inspect(Pt::from(111), VP8_KEYFRAME, true);
    let interframe_packet = inspector.inspect(Pt::from(111), VP8_INTERFRAME, true);

    VP8_KEYFRAME
        .get(VP8_DESCRIPTOR_BYTES..VP8_DESCRIPTOR_BYTES + 10)
        .is_some()
        && VP8_INTERFRAME
            .get(VP8_DESCRIPTOR_BYTES..VP8_DESCRIPTOR_BYTES + 3)
            .is_some()
        && keyframe.picture_id() == Some(2)
        && keyframe.tl0_pic_idx() == Some(9)
        && keyframe.starts_keyframe(VP8_KEYFRAME)
        && keyframe_packet.decoder_refresh()
        && interframe.picture_id() == Some(3)
        && interframe.tl0_pic_idx() == Some(10)
        && !interframe.starts_keyframe(VP8_INTERFRAME)
        && !interframe_packet.decoder_refresh()
}
