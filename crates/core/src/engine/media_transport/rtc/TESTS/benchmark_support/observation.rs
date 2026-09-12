use std::{sync::Arc, time::Instant};

use o_sfu_rfc::rtp::CodecName;
use o_sfu_router::{
    MediaKind as RouterMediaKind,
    rtp::{MediaFormat, MediaStream as RouterRtpParameters, PayloadType},
};
use str0m::{
    media::{Mid, Pt},
    rtp::Vp8Descriptor,
};

use super::super::{
    codec,
    packet_loop::{PacketForwarder, record_incoming_stats_for_benchmark},
    state::{PacketLoopState, bitrate::BitrateRegistry},
    test_support::{
        MediaWorkerScenario, reset_packet_resolution,
        sample_forwarded_packet_with_rid_and_audio_activity, sample_forwarded_packet_without_mid,
        test_transport_session_key,
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
/// setup registers one producer, one incoming bitrate counter and two reusable
/// RTP packets. the first packet carries MID, RID and audio metadata while the
/// second relies on the SSRC binding learned from the first packet
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
        Self::build(b"observed-payload", b"steady-payload", None)
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
            vec![],
        );
        assert!(
            negotiated_vp8_payloads_are_valid(&parameters),
            "negotiated VP8 benchmark payloads must be valid"
        );
        Self::build(VP8_KEYFRAME, VP8_INTERFRAME, Some(parameters))
    }

    fn build(
        first_payload: &[u8],
        second_payload: &[u8],
        parameters: Option<RouterRtpParameters>,
    ) -> Self {
        let source_session = test_transport_session_key(101, 0, 102, UserId::Integer(103));
        let mut state = PacketLoopState::default();
        let mut scenario = MediaWorkerScenario::new(&mut state);
        let src_media = scenario.source(source_session.clone(), Mid::from("cam-up"));
        if let Some(parameters) = parameters {
            state
                .routes
                .refresh_packet_inspector(src_media, &parameters);
        }

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
        buffers
            .pending_packets
            .push(sample_forwarded_packet_with_rid_and_audio_activity(
                source_session.clone(),
                "cam-up",
                Some("hi"),
                Some(true),
                Some(-24),
                first_payload,
            ));
        buffers
            .pending_packets
            .push(sample_forwarded_packet_without_mid(
                source_session,
                4321,
                second_payload,
            ));

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
