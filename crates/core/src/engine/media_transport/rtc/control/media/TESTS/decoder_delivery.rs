use std::time::{Duration, Instant};

use str0m::media::{Mid, Rid};

use super::{
    Bitrate, ConsumerActivity, ConsumerRouteControl, KeyframeRequestKind, ProducerActivity,
    RuntimeMetricsSnapshotTestExt, SourceActivityRevision, SourceActivityUpdate,
    SourceKeyframeRequest, TransportConsumerRoute, TransportSourceKey, WorkerMediaControlBatch,
    WorkerMediaControlBatchOutcome, apply_media_control_batch, apply_source_activity,
    arm_route_repair, assert_consumer_packet_gate, drain_ready_sessions,
    fixtures::{LocalVideoRoute, prepare_pending_selected_rid_route, set_consumer_packet_gate_at},
    install_video_route_with_gate, retry_source_kf, route_repair_is_armed,
    test_consumer_session_key,
};
use crate::engine::media_transport::rtc::state::route_control::PacketLayerGate;

#[test]
fn changed_selection_refreshes_fallback_until_consumer_teardown() {
    let fallback_rid = Rid::from("lo");
    let mut route = LocalVideoRoute::with_rid(533, 88_923, fallback_rid);
    let consumer = route.consumer_route();
    let now = Instant::now();
    set_consumer_packet_gate_at(
        &mut route.state,
        &consumer,
        PacketLayerGate::Rid(Rid::from("hi")),
        now,
    );
    assert!(route.observe_rid_ready(fallback_rid, true, now));
    route.assert_packet_gate(
        PacketLayerGate::Rid(fallback_rid),
        Some(PacketLayerGate::Rid(Rid::from("hi"))),
    );
    let changed_selection = PacketLayerGate::Rid(Rid::from("mid"));
    set_consumer_packet_gate_at(&mut route.state, &consumer, changed_selection, now);
    assert!(!route.observe_rid_ready(fallback_rid, false, now + Duration::from_millis(10)));
    route.assert_packet_gate(PacketLayerGate::Block, Some(changed_selection));
    route.assert_source_ready();
    assert_eq!(route.metrics.snapshot().rtc_route_control_forwarded(), 1);
    route.state.remove_consumer_route(
        &route.consumer_session,
        route.consumer_media,
        route.src_media,
    );
    retry_source_kf(
        &mut route.state,
        &route.rtc_metrics,
        SourceKeyframeRequest {
            src_media: route.src_media,
            rid: Some(fallback_rid),
            kind: KeyframeRequestKind::Pli,
        },
        now + Duration::from_secs(1),
    );
    assert!(drain_ready_sessions(&mut route.state).is_empty());
    assert_eq!(route.metrics.snapshot().rtc_route_control_forwarded(), 1);
}

#[test]
fn routes_without_pending_selection_do_not_request_fallback_refresh() {
    for packet_gate in [PacketLayerGate::Block, PacketLayerGate::Open] {
        let fallback_rid = Rid::from("lo");
        let mut route = LocalVideoRoute::with_rid_gate(532, 88_922, fallback_rid, packet_gate);
        assert!(!route.observe_rid_ready(fallback_rid, false, Instant::now()));
        assert!(drain_ready_sessions(&mut route.state).is_empty());
        assert_eq!(route.metrics.snapshot().rtc_route_control_forwarded(), 0);
    }
}

#[test]
fn inactive_selection_does_not_request_fallback_refresh() -> Result<(), &'static str> {
    let mut route = prepare_pending_selected_rid_route();
    let consumer_media = route
        .state
        .routes
        .local_route(route.src_media)
        .and_then(|entry| entry.destinations.first())
        .map(|destination| destination.dest_transport_media_id)
        .ok_or("selected RID fixture should have a destination")?;
    let consumer = TransportConsumerRoute::new(
        route.consumer_session.clone(),
        consumer_media,
        TransportSourceKey::new(route.source_session.clone(), route.src_media),
    );
    let now = Instant::now();
    let _ = apply_media_control_batch(
        &mut route.state,
        &route.rtc_metrics,
        Bitrate::from_mbps(10),
        now,
        WorkerMediaControlBatch::ConsumerFollowUp(vec![(
            0,
            ConsumerRouteControl::new(consumer).activity(ConsumerActivity::Inactive),
        )]),
    );
    assert!(!route.observe_rid_ready(route.fallback_rid, false, now));
    assert!(drain_ready_sessions(&mut route.state).is_empty());
    assert!(
        !route
            .state
            .routes
            .has_local_kf_demand(route.src_media, Some(route.fallback_rid))
    );
    Ok(())
}

#[test]
fn selected_keyframe_suppresses_sibling_fallback_for_that_packet() -> Result<(), &'static str> {
    for activity in [ConsumerActivity::Active, ConsumerActivity::Inactive] {
        let mut route = prepare_pending_selected_rid_route();
        let now = Instant::now();
        let source = TransportSourceKey::new(route.source_session.clone(), route.src_media);
        let selected_media = route
            .state
            .routes
            .local_route(route.src_media)
            .and_then(|entry| entry.destinations.first())
            .map(|destination| destination.dest_transport_media_id)
            .ok_or("selected RID fixture should have a destination")?;
        let selected_route = TransportConsumerRoute::new(
            route.consumer_session.clone(),
            selected_media,
            source.clone(),
        );
        let sibling_session = test_consumer_session_key(233);
        let sibling_media = install_video_route_with_gate(
            &mut route.state,
            route.src_media,
            &sibling_session,
            Mid::from("cam-down-sibling"),
            PacketLayerGate::Open,
        );
        let sibling_route =
            TransportConsumerRoute::new(sibling_session.clone(), sibling_media, source);
        set_consumer_packet_gate_at(
            &mut route.state,
            &sibling_route,
            PacketLayerGate::Rid(route.fallback_rid),
            now,
        );
        let WorkerMediaControlBatchOutcome::Consumers(results) = apply_media_control_batch(
            &mut route.state,
            &route.rtc_metrics,
            Bitrate::from_mbps(10),
            now,
            WorkerMediaControlBatch::ConsumerFollowUp(vec![(
                0,
                ConsumerRouteControl::new(selected_route).activity(activity),
            )]),
        ) else {
            return Err("consumer activity should return consumer outcomes");
        };
        assert_eq!(results.len(), 1);
        assert!(results.iter().all(|result| result.error().is_none()));
        assert!(route.observe_rid_ready(route.selected_rid, true, now));
        route.assert_packet_gate(PacketLayerGate::Rid(route.selected_rid), None);
        assert_consumer_packet_gate(
            &route.state,
            route.src_media,
            &sibling_session,
            &PacketLayerGate::Block,
            Some(&PacketLayerGate::Rid(route.fallback_rid)),
        );
        assert!(route.observe_rid_ready(route.selected_rid, true, now + Duration::from_millis(1),));
        assert_consumer_packet_gate(
            &route.state,
            route.src_media,
            &sibling_session,
            &PacketLayerGate::Rid(route.selected_rid),
            Some(&PacketLayerGate::Rid(route.fallback_rid)),
        );
    }
    Ok(())
}

#[test]
fn unchanged_and_rejected_source_activity_preserve_repair() -> Result<(), &'static str> {
    let mut route = LocalVideoRoute::new(97, 97_000);
    let now = Instant::now();
    let stream = arm_route_repair(&mut route, now)?;
    let source = TransportSourceKey::new(route.source_session.clone(), route.src_media);
    let revision = SourceActivityRevision::default().next();
    for update in [
        SourceActivityUpdate::new(ProducerActivity::Active, revision),
        SourceActivityUpdate::new(
            ProducerActivity::Inactive,
            SourceActivityRevision::default(),
        ),
        SourceActivityUpdate::new(ProducerActivity::Active, revision),
    ] {
        apply_source_activity(
            &mut route.state,
            &route.rtc_metrics,
            source.clone(),
            update,
            now,
        );
        assert!(route.state.routes.source_is_active(route.src_media));
        assert!(route_repair_is_armed(&mut route, stream));
    }
    apply_source_activity(
        &mut route.state,
        &route.rtc_metrics,
        source,
        SourceActivityUpdate::new(ProducerActivity::Inactive, revision.next()),
        now,
    );
    assert!(!route.state.routes.source_is_active(route.src_media));
    assert!(!route_repair_is_armed(&mut route, stream));
    Ok(())
}
