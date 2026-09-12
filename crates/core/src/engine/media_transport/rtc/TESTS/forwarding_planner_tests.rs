use std::{slice, sync::Arc, time::Instant};

use str0m::media::Mid;

use super::{
    super::{
        packet_loop::{
            ForwardingDestination, PacketGateDecision, forwarded_packet::ForwardedPacket,
            plan_forwards as plan_pkt_forwards,
        },
        state::{
            PacketLoopState,
            relay_registry::{RelayPacketMailbox, RelayTargetId},
            route_control::PacketLayerGate,
        },
        test_support::{
            MediaWorkerScenario, sample_already_relayed_packet, sample_forwarded_packet,
            sample_forwarded_packet_with_rid, test_transport_session_key,
        },
    },
    fixtures::RuntimeMetricsSnapshotTestExt,
};
use crate::engine::{
    UserId,
    media_transport::{TransportMediaId, TransportSessionKey},
    metrics::{RtcRouteControlOutcome, RtpForwardDestinationKind, RuntimeMetrics},
    packet_sink_registry::{
        PacketSink as MediaPacketSink, PacketSinkRouteCache, RoomPacketSinkRegistry,
    },
};

struct PlannerSink;

impl MediaPacketSink for PlannerSink {
    fn record_packet(
        &self,
        _session_key: &TransportSessionKey,
        _transport_media_id: TransportMediaId,
        _received_at: Instant,
        _payload: &[u8],
    ) {
    }
}

fn populate_forward_routes(
    state: &PacketLoopState,
    packet_sinks: &RoomPacketSinkRegistry,
    metrics: &RuntimeMetrics,
    pending_packets: &mut [ForwardedPacket],
    forwards: &mut Vec<ForwardingDestination>,
) {
    let mut packet_sink_cache = PacketSinkRouteCache::default();
    packet_sink_cache.refresh_from(packet_sinks);
    let rtc_metrics = metrics.register_rtc_worker();
    for packet in pending_packets {
        let visits_origin = packet.visits_origin_sinks();
        let Some(facts) = packet.resolve_facts(state) else {
            continue;
        };
        if let Some(decision) = plan_pkt_forwards(
            facts,
            visits_origin,
            &state.routes,
            &packet_sink_cache,
            forwards,
        ) {
            rtc_metrics.record_rtc_route_control(match decision {
                PacketGateDecision::Allowed => RtcRouteControlOutcome::LayerAllowed,
                PacketGateDecision::Dropped => RtcRouteControlOutcome::LayerDropped,
            });
        }
    }
}

fn local_destination_session<'a>(
    state: &'a PacketLoopState,
    destination: &ForwardingDestination,
) -> Option<&'a TransportSessionKey> {
    let (src_media, dst_idx) = destination.local_route()?;
    state
        .routes
        .local_route(src_media)?
        .destinations
        .get(dst_idx)
        .map(|destination| &destination.dest_session)
}

enum ExpectedForward<'a> {
    Local(&'a TransportSessionKey),
    PacketSink,
    Kind(RtpForwardDestinationKind),
}

fn plan_forwards(
    state: &PacketLoopState,
    packet_sinks: &RoomPacketSinkRegistry,
    metrics: &RuntimeMetrics,
    mut pending_packets: Vec<ForwardedPacket>,
) -> Vec<ForwardingDestination> {
    let mut forwards = Vec::new();
    populate_forward_routes(
        state,
        packet_sinks,
        metrics,
        &mut pending_packets,
        &mut forwards,
    );
    forwards
}

fn assert_forward_plan(
    state: &PacketLoopState,
    forwards: &[ForwardingDestination],
    expected: &[ExpectedForward<'_>],
) {
    assert_eq!(forwards.len(), expected.len());
    for (forward, expected) in forwards.iter().zip(expected) {
        match expected {
            ExpectedForward::Local(session) => assert!(matches!(
                forward,
                destination if local_destination_session(state, destination) == Some(*session)
            )),
            ExpectedForward::PacketSink => {
                assert!(matches!(forward, ForwardingDestination::PacketSink(_)));
            }
            ExpectedForward::Kind(kind) => {
                assert_eq!(forward.metrics_kind(), *kind);
            }
        }
    }
}

#[test]
fn plan_forwards_keeps_recording_and_local_rtc_destinations_together() {
    let producer_session = test_transport_session_key(21, 0, 22, UserId::Integer(23));
    let consumer_session = test_transport_session_key(21, 0, 22, UserId::Integer(24));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let src_media = scenario.source(producer_session.clone(), Mid::from("aud-up"));
    let consumer_media =
        scenario.destination(src_media, consumer_session.clone(), Mid::from("aud-down"));
    packet_sink_registry.register_room(
        producer_session.room_instance_id(),
        Arc::new(PlannerSink),
        RtpForwardDestinationKind::Recording,
    );
    let forwards = plan_forwards(
        &state,
        &packet_sink_registry,
        &metrics,
        vec![sample_forwarded_packet(
            producer_session.clone(),
            "aud-up",
            b"payload",
        )],
    );

    assert_forward_plan(
        &state,
        &forwards,
        &[
            ExpectedForward::PacketSink,
            ExpectedForward::Kind(RtpForwardDestinationKind::LocalRtc),
        ],
    );
    assert!(
        state
            .routes
            .remove_consumer_route(src_media, &consumer_session, consumer_media)
            .is_some()
    );
    let forwards = plan_forwards(
        &state,
        &packet_sink_registry,
        &metrics,
        vec![sample_forwarded_packet(
            producer_session,
            "aud-up",
            b"payload",
        )],
    );

    assert_forward_plan(&state, &forwards, &[ExpectedForward::PacketSink]);
}

#[test]
fn plan_forwards_preserves_dense_fanout() {
    const DESTINATION_COUNT: usize = 128;

    let producer_session = test_transport_session_key(25, 0, 26, UserId::Integer(27));
    let consumer_session = test_transport_session_key(25, 0, 28, UserId::Integer(29));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let src_media = scenario.source(producer_session.clone(), Mid::from("cam-up"));
    for _ in 0..DESTINATION_COUNT {
        scenario.destination(src_media, consumer_session.clone(), Mid::from("cam-down"));
    }
    let forwards = plan_forwards(
        &state,
        &packet_sink_registry,
        &metrics,
        vec![sample_forwarded_packet(
            producer_session,
            "cam-up",
            b"payload",
        )],
    );

    assert_eq!(forwards.len(), DESTINATION_COUNT);
}

#[test]
fn plan_forwards_skips_inactive_consumer_destinations() {
    let producer_session = test_transport_session_key(29, 0, 30, UserId::Integer(31));
    let inactive_consumer_session = test_transport_session_key(29, 0, 32, UserId::Integer(33));
    let active_consumer_session = test_transport_session_key(29, 0, 34, UserId::Integer(35));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let src_media = scenario.source(producer_session.clone(), Mid::from("cam-up"));
    let inactive_transport_media_id = scenario.destination(
        src_media,
        inactive_consumer_session.clone(),
        Mid::from("cam-down-inactive"),
    );
    scenario.destination(
        src_media,
        active_consumer_session.clone(),
        Mid::from("cam-down-active"),
    );

    state
        .routes
        .set_consumer_active(
            src_media,
            0,
            &inactive_consumer_session,
            inactive_transport_media_id,
            false,
        )
        .unwrap();

    let forwards = plan_forwards(
        &state,
        &packet_sink_registry,
        &metrics,
        vec![sample_forwarded_packet(
            producer_session,
            "cam-up",
            b"payload",
        )],
    );

    assert_forward_plan(
        &state,
        &forwards,
        &[ExpectedForward::Local(&active_consumer_session)],
    );
}

#[test]
fn plan_forwards_plans_relay_destinations_without_displacing_local_rtc_flush_order() {
    let producer_session = test_transport_session_key(31, 0, 32, UserId::Integer(33));
    let consumer_session = test_transport_session_key(31, 0, 32, UserId::Integer(34));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let (first_relay_mailbox, _first_relay_rx) = RelayPacketMailbox::channel_for_test();
    let (second_relay_mailbox, _second_relay_rx) = RelayPacketMailbox::channel_for_test();
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let src_media = scenario.source(producer_session.clone(), Mid::from("aud-up"));
    scenario.destination(src_media, consumer_session, Mid::from("aud-down"));
    packet_sink_registry.register_room(
        producer_session.room_instance_id(),
        Arc::new(PlannerSink),
        RtpForwardDestinationKind::Recording,
    );
    state
        .routes
        .add_relay_target(src_media, RelayTargetId::new(1), first_relay_mailbox);
    state
        .routes
        .set_relay_target_active(src_media, RelayTargetId::new(1), true);
    state
        .routes
        .add_relay_target(src_media, RelayTargetId::new(2), second_relay_mailbox);
    state
        .routes
        .set_relay_target_active(src_media, RelayTargetId::new(2), true);
    let forwards = plan_forwards(
        &state,
        &packet_sink_registry,
        &metrics,
        vec![sample_forwarded_packet(
            producer_session,
            "aud-up",
            b"payload",
        )],
    );

    assert_forward_plan(
        &state,
        &forwards,
        &[
            ExpectedForward::PacketSink,
            ExpectedForward::Kind(RtpForwardDestinationKind::IntraNodeRelay),
            ExpectedForward::Kind(RtpForwardDestinationKind::IntraNodeRelay),
            ExpectedForward::Kind(RtpForwardDestinationKind::LocalRtc),
        ],
    );
}

#[test]
fn plan_forwards_gates_relay_only_sources_without_removing_targets() {
    let producer_session = test_transport_session_key(35, 0, 36, UserId::Integer(37));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let (relay_mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test();
    let src_media =
        MediaWorkerScenario::new(&mut state).source(producer_session.clone(), Mid::from("cam-up"));
    state
        .routes
        .add_relay_target(src_media, RelayTargetId::new(1), relay_mailbox);
    state
        .routes
        .set_relay_target_active(src_media, RelayTargetId::new(1), true);

    assert!(state.routes.set_source_active(src_media, false).is_ok());
    assert!(state.routes.local_route(src_media).is_none());
    assert_eq!(state.routes.active_relay_target_count(src_media), 1);
    let forwards = plan_forwards(
        &state,
        &packet_sink_registry,
        &metrics,
        vec![sample_forwarded_packet(
            producer_session.clone(),
            "cam-up",
            b"payload",
        )],
    );
    assert_forward_plan(&state, &forwards, &[]);

    assert!(state.routes.set_source_active(src_media, true).is_ok());
    assert_eq!(state.routes.active_relay_target_count(src_media), 1);
    let forwards = plan_forwards(
        &state,
        &packet_sink_registry,
        &metrics,
        vec![sample_forwarded_packet(
            producer_session,
            "cam-up",
            b"payload",
        )],
    );
    assert_forward_plan(
        &state,
        &forwards,
        &[ExpectedForward::Kind(
            RtpForwardDestinationKind::IntraNodeRelay,
        )],
    );
}

#[test]
fn plan_forwards_keeps_relay_packets_out_of_recording_and_second_hop_relay_sinks() {
    let producer_session = test_transport_session_key(41, 0, 42, UserId::Integer(43));
    let consumer_session = test_transport_session_key(41, 1, 44, UserId::Integer(45));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let (relay_mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test();
    let src_media = TransportMediaId::new(51);
    let mut scenario = MediaWorkerScenario::new(&mut state);
    scenario.destination(src_media, consumer_session, Mid::from("aud-down"));
    packet_sink_registry.register_room(
        producer_session.room_instance_id(),
        Arc::new(PlannerSink),
        RtpForwardDestinationKind::Recording,
    );
    state
        .routes
        .add_relay_target(src_media, RelayTargetId::new(1), relay_mailbox);
    state
        .routes
        .set_relay_target_active(src_media, RelayTargetId::new(1), true);
    let forwards = plan_forwards(
        &state,
        &packet_sink_registry,
        &metrics,
        vec![sample_already_relayed_packet(
            producer_session,
            src_media,
            "aud-up",
            b"payload",
        )],
    );

    assert_forward_plan(
        &state,
        &forwards,
        &[ExpectedForward::Kind(RtpForwardDestinationKind::LocalRtc)],
    );
}

#[test]
fn plan_forwards_only_relays_the_registered_source_media() {
    let first_producer_session = test_transport_session_key(52, 0, 53, UserId::Integer(54));
    let second_producer_session = test_transport_session_key(52, 0, 53, UserId::Integer(55));
    let remote_consumer_session = test_transport_session_key(52, 1, 56, UserId::Integer(57));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let (relay_mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test();
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let first_src_media = scenario.source(first_producer_session.clone(), Mid::from("aud-up-1"));
    let second_src_media = scenario.source(second_producer_session.clone(), Mid::from("aud-up-2"));
    scenario.destination(
        first_src_media,
        remote_consumer_session,
        Mid::from("aud-down"),
    );
    state
        .routes
        .add_relay_target(first_src_media, RelayTargetId::new(1), relay_mailbox);
    state
        .routes
        .set_relay_target_active(first_src_media, RelayTargetId::new(1), true);
    let pending_packets = vec![
        sample_forwarded_packet(first_producer_session, "aud-up-1", b"payload-1"),
        sample_forwarded_packet(second_producer_session, "aud-up-2", b"payload-2"),
    ];
    let mut forwards = Vec::new();
    let mut pending_packets = pending_packets;

    populate_forward_routes(
        &state,
        &packet_sink_registry,
        &metrics,
        &mut pending_packets,
        &mut forwards,
    );

    assert_forward_plan(
        &state,
        &forwards,
        &[
            ExpectedForward::Kind(RtpForwardDestinationKind::IntraNodeRelay),
            ExpectedForward::Kind(RtpForwardDestinationKind::LocalRtc),
        ],
    );
    assert_eq!(
        pending_packets
            .get_mut(1)
            .and_then(|packet| packet.resolve_src_media(&state)),
        Some(second_src_media)
    );
}

#[test]
fn plan_forwards_enforces_per_consumer_rid_gates_after_aggregate_admits() {
    let producer_session = test_transport_session_key(81, 0, 82, UserId::Integer(83));
    let lo_consumer_session = test_transport_session_key(81, 0, 82, UserId::Integer(84));
    let hi_consumer_session = test_transport_session_key(81, 0, 82, UserId::Integer(85));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let src_media = scenario.source(producer_session.clone(), Mid::from("cam-up"));
    scenario.destination_with_gate(
        src_media,
        lo_consumer_session.clone(),
        Mid::from("cam-down-lo"),
        PacketLayerGate::Rid("lo".into()),
    );
    scenario.destination_with_gate(
        src_media,
        hi_consumer_session.clone(),
        Mid::from("cam-down-hi"),
        PacketLayerGate::Rid("hi".into()),
    );
    let mut pending_packets = vec![
        sample_forwarded_packet_with_rid(
            producer_session.clone(),
            "cam-up",
            Some("hi"),
            b"hi-packet",
        ),
        sample_forwarded_packet_with_rid(producer_session, "cam-up", Some("lo"), b"lo-packet"),
    ];
    let mut forwards = Vec::new();

    populate_forward_routes(
        &state,
        &packet_sink_registry,
        &metrics,
        &mut pending_packets,
        &mut forwards,
    );

    assert_forward_plan(
        &state,
        &forwards,
        &[
            ExpectedForward::Local(&hi_consumer_session),
            ExpectedForward::Local(&lo_consumer_session),
        ],
    );
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_layer_allowed(), 2);
    assert_eq!(snapshot.rtc_route_control_layer_dropped(), 0);
}

#[test]
fn plan_forwards_enforces_per_relay_target_gates_after_aggregate_admits() {
    let producer_session = test_transport_session_key(91, 0, 92, UserId::Integer(93));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let (hi_mailbox, _hi_rx) = RelayPacketMailbox::channel_for_test();
    let (lo_mailbox, _lo_rx) = RelayPacketMailbox::channel_for_test();
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let src_media = scenario.source(producer_session.clone(), Mid::from("cam-up"));
    let hi_target_id = RelayTargetId::new(1);
    let lo_target_id = RelayTargetId::new(2);
    state
        .routes
        .add_relay_target(src_media, hi_target_id, hi_mailbox);
    state
        .routes
        .set_relay_target_active(src_media, hi_target_id, true);
    state
        .routes
        .add_relay_target(src_media, lo_target_id, lo_mailbox);
    state
        .routes
        .set_relay_target_active(src_media, lo_target_id, true);
    state
        .routes
        .set_relay_pkt_gate(src_media, hi_target_id, PacketLayerGate::Rid("hi".into()));
    state
        .routes
        .set_relay_pkt_gate(src_media, lo_target_id, PacketLayerGate::Rid("lo".into()));
    let mut pending_packets = vec![
        sample_forwarded_packet_with_rid(
            producer_session.clone(),
            "cam-up",
            Some("hi"),
            b"hi-packet",
        ),
        sample_forwarded_packet_with_rid(producer_session, "cam-up", Some("lo"), b"lo-packet"),
    ];
    let mut forwards = Vec::new();

    populate_forward_routes(
        &state,
        &packet_sink_registry,
        &metrics,
        &mut pending_packets,
        &mut forwards,
    );

    assert_forward_plan(
        &state,
        &forwards,
        &[
            ExpectedForward::Kind(RtpForwardDestinationKind::IntraNodeRelay),
            ExpectedForward::Kind(RtpForwardDestinationKind::IntraNodeRelay),
        ],
    );
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_layer_allowed(), 2);
    assert_eq!(snapshot.rtc_route_control_layer_dropped(), 0);
}

#[test]
fn plan_forwards_keeps_staged_relay_gates_through_activation() {
    let producer_session = test_transport_session_key(94, 0, 95, UserId::Integer(96));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let src_media =
        MediaWorkerScenario::new(&mut state).source(producer_session.clone(), Mid::from("cam-up"));
    let hi_target_id = RelayTargetId::new(1);
    let lo_target_id = RelayTargetId::new(2);
    let unrestricted_target_id = RelayTargetId::new(3);
    let (hi_mailbox, mut hi_rx) = RelayPacketMailbox::channel_for_test();
    let (unrestricted_mailbox, mut unrestricted_rx) = RelayPacketMailbox::channel_for_test();
    state
        .routes
        .set_relay_pkt_gate(src_media, hi_target_id, PacketLayerGate::Rid("hi".into()));
    state
        .routes
        .add_relay_target(src_media, unrestricted_target_id, unrestricted_mailbox);
    state
        .routes
        .set_relay_target_active(src_media, unrestricted_target_id, true);
    assert_eq!(
        state.routes.effective_packet_gate(src_media),
        Some(PacketLayerGate::Rid("hi".into()))
    );
    state
        .routes
        .set_relay_pkt_gate(src_media, lo_target_id, PacketLayerGate::Rid("lo".into()));
    assert_eq!(
        state.routes.effective_packet_gate(src_media),
        Some(PacketLayerGate::Open)
    );
    state
        .routes
        .add_relay_target(src_media, hi_target_id, hi_mailbox);
    for (active, rid, expect_hi) in [
        (false, "hi", false),
        (true, "hi", true),
        (true, "lo", false),
    ] {
        state
            .routes
            .set_relay_target_active(src_media, hi_target_id, active);
        let mut packet = sample_forwarded_packet_with_rid(
            producer_session.clone(),
            "cam-up",
            Some(rid),
            b"packet",
        );
        let mut forwards = Vec::new();
        populate_forward_routes(
            &state,
            &packet_sink_registry,
            &metrics,
            slice::from_mut(&mut packet),
            &mut forwards,
        );
        assert_eq!(forwards.len(), 1 + usize::from(expect_hi));
        for forward in forwards {
            let ForwardingDestination::Relay(destination) = forward else {
                panic!("relay-only source planned another destination");
            };
            assert!(destination.send(&state, &packet).is_some());
        }
        assert_eq!(unrestricted_rx.try_recv().unwrap().payload(), b"packet");
        if expect_hi {
            assert_eq!(hi_rx.try_recv().unwrap().payload(), b"packet");
        } else {
            assert!(hi_rx.try_recv().is_err());
        }
    }
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_layer_allowed(), 3);
    assert_eq!(snapshot.rtc_route_control_layer_dropped(), 0);
}

#[test]
fn plan_forwards_gates_only_the_selected_source_media() {
    let gated_producer_session = test_transport_session_key(61, 0, 62, UserId::Integer(63));
    let open_producer_session = test_transport_session_key(61, 0, 62, UserId::Integer(64));
    let gated_consumer_session = test_transport_session_key(61, 0, 62, UserId::Integer(65));
    let open_consumer_session = test_transport_session_key(61, 0, 62, UserId::Integer(66));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let (relay_mailbox, _relay_rx) = RelayPacketMailbox::channel_for_test();
    let mut scenario = MediaWorkerScenario::new(&mut state);
    let gated_src_media = scenario.source(gated_producer_session.clone(), Mid::from("cam-up"));
    let open_src_media = scenario.source(open_producer_session.clone(), Mid::from("screen-up"));
    scenario.destination(
        gated_src_media,
        gated_consumer_session,
        Mid::from("cam-down"),
    );
    scenario.destination(
        open_src_media,
        open_consumer_session.clone(),
        Mid::from("screen-down"),
    );
    state
        .routes
        .set_local_pkt_gate(gated_src_media, Some(PacketLayerGate::Rid("hi".into())));
    packet_sink_registry.register_room(
        gated_producer_session.room_instance_id(),
        Arc::new(PlannerSink),
        RtpForwardDestinationKind::Recording,
    );
    state
        .routes
        .add_relay_target(gated_src_media, RelayTargetId::new(1), relay_mailbox);
    state
        .routes
        .set_relay_target_active(gated_src_media, RelayTargetId::new(1), true);
    let forwards = plan_forwards(
        &state,
        &packet_sink_registry,
        &metrics,
        vec![
            sample_forwarded_packet_with_rid(
                gated_producer_session,
                "cam-up",
                Some("lo"),
                b"camera-packet",
            ),
            sample_forwarded_packet(open_producer_session, "screen-up", b"screen-packet"),
        ],
    );

    assert_forward_plan(
        &state,
        &forwards,
        &[
            ExpectedForward::PacketSink,
            ExpectedForward::PacketSink,
            ExpectedForward::Local(&open_consumer_session),
        ],
    );
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_layer_dropped(), 1);
    assert_eq!(snapshot.rtc_route_control_layer_allowed(), 1);
}

#[test]
fn plan_forwards_omits_gate_metrics_without_routed_destinations() {
    let producer_session = test_transport_session_key(71, 0, 72, UserId::Integer(73));
    let consumer_session = test_transport_session_key(71, 0, 72, UserId::Integer(74));
    let mut state = PacketLoopState::default();
    let packet_sink_registry = RoomPacketSinkRegistry::default();
    let metrics = RuntimeMetrics::default();
    let src_media =
        MediaWorkerScenario::new(&mut state).source(producer_session.clone(), Mid::from("aud-up"));
    let mut packets = vec![sample_forwarded_packet(
        producer_session.clone(),
        "aud-up",
        b"payload",
    )];
    let mut forwards = Vec::new();

    populate_forward_routes(
        &state,
        &packet_sink_registry,
        &metrics,
        &mut packets,
        &mut forwards,
    );
    assert_forward_plan(&state, &forwards, &[]);

    packet_sink_registry.register_room(
        producer_session.room_instance_id(),
        Arc::new(PlannerSink),
        RtpForwardDestinationKind::Recording,
    );
    populate_forward_routes(
        &state,
        &packet_sink_registry,
        &metrics,
        &mut packets,
        &mut forwards,
    );
    assert_forward_plan(&state, &forwards, &[ExpectedForward::PacketSink]);
    forwards.clear();

    let consumer_media = MediaWorkerScenario::new(&mut state).destination(
        src_media,
        consumer_session.clone(),
        Mid::from("aud-down"),
    );
    state
        .routes
        .set_consumer_active(src_media, 0, &consumer_session, consumer_media, false)
        .unwrap();
    state
        .routes
        .set_local_pkt_gate(src_media, Some(PacketLayerGate::Block));
    populate_forward_routes(
        &state,
        &packet_sink_registry,
        &metrics,
        &mut packets,
        &mut forwards,
    );
    assert_forward_plan(&state, &forwards, &[ExpectedForward::PacketSink]);
    forwards.clear();

    packets[0] = sample_forwarded_packet(producer_session, "unknown", b"payload");
    populate_forward_routes(
        &state,
        &packet_sink_registry,
        &metrics,
        &mut packets,
        &mut forwards,
    );
    assert_forward_plan(&state, &forwards, &[]);
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.rtc_route_control_layer_allowed(), 0);
    assert_eq!(snapshot.rtc_route_control_layer_dropped(), 0);
}
