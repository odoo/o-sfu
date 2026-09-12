use std::{
    collections::BTreeSet,
    slice,
    sync::{Arc, Mutex, PoisonError},
    time::Instant,
};

use o_sfu_rfc::rtp::CodecName;
use o_sfu_router::{
    MediaKind as RouterMediaKind,
    rtp::{MediaFormat, MediaStream, PayloadType},
};
use str0m::media::{Mid, Rid};
use tokio::sync::mpsc;

use super::{ForwardingEffects, PacketForwarder};
use crate::engine::{
    RoomInstanceId, UserId,
    media_transport::{
        SourcePolicySignal, TransportMediaId, TransportSessionKey,
        rtc::{
            packet_loop::forwarded_packet::ForwardedPacket,
            state::{
                PacketLoopState,
                bitrate::MediaBitrateCounter,
                media_registry::RegisteredMediaHandle,
                relay_registry::{RelayPacketMailbox, RelayTargetId},
                route_control::PacketLayerGate,
                slots::ConsumerStreamHandle,
                source_route::{DecoderDelivery, MediaRouteDestination},
            },
            test_support::{
                prepare_source_session_with_rid, sample_forwarded_packet,
                sample_forwarded_packet_with_rid, test_transport_session_key,
            },
        },
    },
    metrics::{
        RtcMetricsRecorder, RtpForwardDestinationKind, RtpMetricsRecorder, RuntimeMetrics,
        test_support::RuntimeMetricsSnapshotTestExt,
    },
    packet_sink_registry::{PacketSink, RoomPacketSinkRegistry},
};

#[derive(Default)]
struct CapturingSink(Mutex<Vec<(TransportMediaId, Vec<u8>)>>);

impl CapturingSink {
    fn packets(&self) -> Vec<(TransportMediaId, Vec<u8>)> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl PacketSink for CapturingSink {
    fn record_packet(
        &self,
        _session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        _received_at: Instant,
        payload: &[u8],
    ) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((transport_media_id, payload.to_vec()));
    }
}

struct ForwardingHarness {
    state: PacketLoopState,
    forwarder: PacketForwarder,
    packet_sinks: RoomPacketSinkRegistry,
    source_policy_signal: SourcePolicySignal,
    metrics: RuntimeMetrics,
    rtp_metrics: Arc<RtpMetricsRecorder>,
    rtc_metrics: Arc<RtcMetricsRecorder>,
}

impl ForwardingHarness {
    fn new() -> Self {
        let metrics = RuntimeMetrics::default();
        let rtp = metrics.register_rtp_worker();
        let control = metrics.register_rtc_worker();
        Self {
            state: PacketLoopState::default(),
            forwarder: PacketForwarder::default(),
            packet_sinks: RoomPacketSinkRegistry::default(),
            source_policy_signal: SourcePolicySignal::default(),
            metrics,
            rtp_metrics: rtp,
            rtc_metrics: control,
        }
    }

    fn register_source(&mut self, session: &TransportSessionKey) -> TransportMediaId {
        self.state
            .register_media_handle(RegisteredMediaHandle::Producer {
                session_key: session.clone(),
                mid: Mid::from("aud-up"),
            })
    }

    fn register_video_with_pending_consumer(
        &mut self,
        producer: &TransportSessionKey,
        consumer: TransportSessionKey,
        selected_rid: Rid,
    ) -> TransportMediaId {
        let src_media = prepare_source_session_with_rid(
            &mut self.state,
            producer,
            Mid::from("cam-up"),
            4_321,
            Some(selected_rid),
        );
        self.state.routes.refresh_packet_inspector(
            src_media,
            &MediaStream::new(
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
        self.state.register_incoming_bitrate_counter(
            src_media,
            Arc::new(MediaBitrateCounter::new(Instant::now())),
        );
        let consumer_mid = Mid::from("cam-down");
        let consumer_media = self
            .state
            .register_media_handle(RegisteredMediaHandle::Consumer {
                session_key: consumer.clone(),
                mid: consumer_mid,
                src_media,
            });
        self.state.routes.add_consumer_route(
            src_media,
            MediaRouteDestination {
                dest_session: consumer,
                dest_transport_media_id: consumer_media,
                dest_stream: ConsumerStreamHandle::default(),
                dest_mid: consumer_mid,
                dest_payload_type: None,
                repair_enabled: false,
                active: true,
                delivery: DecoderDelivery::new(true, PacketLayerGate::Rid(selected_rid)),
            },
        );
        src_media
    }

    fn completed_scratch_capacities(&self) -> [usize; 5] {
        assert_eq!(
            [
                self.forwarder.forwards.len(),
                self.forwarder.observed_rids.len(),
                self.forwarder.pending_first_video_keyframes.len(),
                self.forwarder.rid_readiness_changed_sources.len(),
                self.forwarder.dirty_source_policy_channel_ids.len(),
            ],
            [0; 5]
        );
        [
            self.forwarder.forwards.capacity(),
            self.forwarder.observed_rids.capacity(),
            self.forwarder.pending_first_video_keyframes.capacity(),
            self.forwarder.rid_readiness_changed_sources.capacity(),
            self.forwarder.dirty_source_policy_channel_ids.capacity(),
        ]
    }

    fn forward(&mut self, packets: &mut [ForwardedPacket]) {
        self.forwarder.forward_batch(
            &mut self.state,
            packets,
            &ForwardingEffects {
                packet_sinks: &self.packet_sinks,
                source_policy_signal: &self.source_policy_signal,
                metrics: &self.metrics,
                rtp_metrics: &self.rtp_metrics,
                rtc_metrics: &self.rtc_metrics,
            },
        );
    }
}

#[test]
fn batch_does_not_reuse_a_previous_packets_sink_or_media() {
    let first_session = test_transport_session_key(41, 0, 1, UserId::Integer(1));
    let second_session = test_transport_session_key(42, 0, 2, UserId::Integer(2));
    let mut harness = ForwardingHarness::new();
    let first_media = harness.register_source(&first_session);
    let second_media = harness.register_source(&second_session);
    let first_sink = Arc::new(CapturingSink::default());
    let second_sink = Arc::new(CapturingSink::default());
    harness.packet_sinks.register_room(
        first_session.room_instance_id(),
        Arc::<CapturingSink>::clone(&first_sink),
        RtpForwardDestinationKind::Recording,
    );
    harness.packet_sinks.register_room(
        second_session.room_instance_id(),
        Arc::<CapturingSink>::clone(&second_sink),
        RtpForwardDestinationKind::Recording,
    );
    harness.forward(&mut [
        sample_forwarded_packet(first_session.clone(), "aud-up", b"first"),
        sample_forwarded_packet(second_session, "aud-up", b"second"),
        sample_forwarded_packet(first_session, "aud-up", b"first-again"),
    ]);
    assert_eq!(
        first_sink.packets(),
        vec![
            (first_media, b"first".to_vec()),
            (first_media, b"first-again".to_vec())
        ]
    );
    assert_eq!(
        second_sink.packets(),
        vec![(second_media, b"second".to_vec())]
    );
    assert_eq!(
        harness.metrics.snapshot().rtp_forwarded_packets_recording(),
        3
    );
    assert!(harness.forwarder.forwards.is_empty());
}

#[test]
fn successive_batches_refresh_added_replaced_and_removed_sinks() {
    let session = test_transport_session_key(43, 0, 3, UserId::Integer(3));
    let mut harness = ForwardingHarness::new();
    let media = harness.register_source(&session);
    let first_sink = Arc::new(CapturingSink::default());
    let replacement_sink = Arc::new(CapturingSink::default());
    harness.forward(&mut [sample_forwarded_packet(
        session.clone(),
        "aud-up",
        b"before",
    )]);
    harness.packet_sinks.register_room(
        session.room_instance_id(),
        Arc::<CapturingSink>::clone(&first_sink),
        RtpForwardDestinationKind::Recording,
    );
    harness.forward(&mut [sample_forwarded_packet(session.clone(), "aud-up", b"first")]);
    harness.packet_sinks.register_room(
        session.room_instance_id(),
        Arc::<CapturingSink>::clone(&replacement_sink),
        RtpForwardDestinationKind::Recording,
    );
    harness.forward(&mut [sample_forwarded_packet(
        session.clone(),
        "aud-up",
        b"replacement",
    )]);
    harness
        .packet_sinks
        .unregister_room(session.room_instance_id());
    harness.forward(&mut [sample_forwarded_packet(session, "aud-up", b"after")]);
    assert_eq!(first_sink.packets(), vec![(media, b"first".to_vec())]);
    assert_eq!(
        replacement_sink.packets(),
        vec![(media, b"replacement".to_vec())]
    );
    assert_eq!(
        harness.metrics.snapshot().rtp_forwarded_packets_recording(),
        2
    );
    assert!(harness.forwarder.forwards.is_empty());
}

#[test]
fn batch_keeps_delta_before_refresh_blocked_and_reuses_completed_observation_scratch()
-> Result<(), &'static str> {
    const DELTA: &[u8] = &[0x10, 0x31, 0, 0, 0x9d, 1, 0x2a, 0x80, 2, 0x68, 1];
    const KEYFRAME: &[u8] = &[0x10, 0x30, 0, 0, 0x9d, 1, 0x2a, 0x80, 2, 0x68, 1];
    let producer = test_transport_session_key(44, 0, 4, UserId::Integer(4));
    let consumer = test_transport_session_key(44, 0, 5, UserId::Integer(5));
    let selected_rid = Rid::from("hi");
    let mut harness = ForwardingHarness::new();
    harness.forwarder = PacketForwarder {
        forwards: Vec::new(),
        observed_rids: Vec::new(),
        pending_first_video_keyframes: Vec::new(),
        rid_readiness_changed_sources: Vec::new(),
        dirty_source_policy_channel_ids: Vec::new(),
        ..PacketForwarder::default()
    };
    let src_media = harness.register_video_with_pending_consumer(&producer, consumer, selected_rid);
    let (relay, mut relay_rx) = RelayPacketMailbox::channel_for_test();
    let target_id = RelayTargetId::new(1);
    // The relay shares local demand, so the pending decoder gate blocks both.
    harness
        .state
        .routes
        .add_relay_target(src_media, target_id, relay);
    harness
        .state
        .routes
        .set_relay_target_active(src_media, target_id, true);
    let sink = Arc::new(CapturingSink::default());
    harness.packet_sinks.register_room(
        producer.room_instance_id(),
        Arc::<CapturingSink>::clone(&sink),
        RtpForwardDestinationKind::Recording,
    );
    let updates = harness.source_policy_signal.subscribe();
    let mut packets = [
        sample_forwarded_packet_with_rid(producer.clone(), "cam-up", Some("hi"), DELTA),
        sample_forwarded_packet_with_rid(producer.clone(), "cam-up", Some("hi"), KEYFRAME),
    ];
    harness.forward(&mut packets);
    let forwarded = relay_rx
        .try_recv()
        .or(Err("selected keyframe should reach the relay"))?;
    assert_eq!(forwarded.payload(), KEYFRAME);
    assert!(matches!(
        relay_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert_eq!(
        sink.packets(),
        vec![(src_media, DELTA.to_vec()), (src_media, KEYFRAME.to_vec())]
    );
    assert_eq!(
        updates.take_pending_updates(),
        BTreeSet::from([producer.room_instance_id()])
    );
    let snapshot = harness.metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_layer_dropped(), 1);
    assert_eq!(snapshot.rtc_route_control_layer_allowed(), 1);
    assert_eq!(snapshot.rtp_forwarded_packets_intra_node_relay(), 1);
    let scratch_capacities = harness.completed_scratch_capacities();
    assert!(scratch_capacities.into_iter().all(|capacity| capacity > 0));
    let [mut delta, _] = packets;
    harness.forward(slice::from_mut(&mut delta));
    let forwarded = relay_rx
        .try_recv()
        .or(Err("delta should pass after decoder readiness"))?;
    assert_eq!(forwarded.payload(), DELTA);
    assert!(matches!(
        relay_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert_eq!(
        harness
            .metrics
            .snapshot()
            .rtp_forwarded_packets_intra_node_relay(),
        2
    );
    assert!(updates.take_pending_updates().is_empty());
    assert_eq!(harness.completed_scratch_capacities(), scratch_capacities);
    Ok(())
}

#[test]
fn packet_forwarder_coalesces_source_policy_dirty_rooms_before_signal_flush() {
    let source_policy_signal = SourcePolicySignal::default();
    let updates = source_policy_signal.subscribe();
    let mut forwarder = PacketForwarder::default();
    forwarder.dirty_source_policy_channel_ids.extend([
        RoomInstanceId::from_raw(41),
        RoomInstanceId::from_raw(41),
        RoomInstanceId::from_raw(42),
    ]);
    forwarder.flush_source_policy_dirty(&source_policy_signal);
    assert_eq!(
        updates.take_pending_updates(),
        BTreeSet::from([RoomInstanceId::from_raw(41), RoomInstanceId::from_raw(42)])
    );
    assert!(forwarder.dirty_source_policy_channel_ids.is_empty());
    assert!(updates.take_pending_updates().is_empty());
}
