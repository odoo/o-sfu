use std::time::Duration;

use o_sfu_model::WebSocketCloseCode;

use super::{PROMETHEUS_CONTENT_TYPE, render_prometheus};
use crate::metrics::{
    BudgetSolverOutcome, HttpRoute, METRIC_FAMILY_COUNT, RoomGaugeValues, RtcDatagramDropReason,
    RtcDatagramRoutePath, RtcKeyframeRequestOutcome, RtcNackDirection, RtcOutputBudgetLimit,
    RtcProducerSsrcBindingOutcome, RtcRelayEnqueueResult, RtcRemoteControlDropKind,
    RtcRemotePacketGateConvergence, RtcRouteControlOutcome, RtpDecoderRefreshScope,
    RtpForwardDestinationKind, RuntimeMetrics, SourceSelectionKind, TransportHealthState,
    TransportIceState, WsSessionLoopExitReason,
};

fn assert_http_and_websocket_metrics(rendered: &str) {
    assert!(rendered.contains("# TYPE osfu_http_noop_requests_total counter"));
    assert!(rendered.contains("osfu_http_noop_requests_total 1"));
    assert!(rendered.contains("osfu_http_metrics_requests_total 1"));
    assert!(rendered.contains("# TYPE osfu_http_inflight_requests gauge"));
    assert!(rendered.contains("osfu_http_inflight_requests{route=\"noop\"} 0"));
    assert!(rendered.contains("# TYPE osfu_http_request_duration_seconds histogram"));
    assert!(rendered.contains("osfu_http_request_duration_seconds_count{route=\"noop\"} 1"));
    assert!(
        rendered.contains("osfu_ws_handshake_rejections_total{close_code=\"protocol_error\"} 1")
    );
    assert!(rendered.contains("# TYPE osfu_ws_handshake_duration_seconds histogram"));
    assert!(rendered.contains("osfu_ws_handshake_duration_seconds_count 1"));
    assert!(rendered.contains("osfu_ws_auth_duration_seconds_count 1"));
    assert!(rendered.contains("osfu_ws_user_initialize_duration_seconds_count 1"));
    assert!(
        rendered.contains("osfu_ws_user_loop_exits_total{reason=\"transport_disconnected\"} 1")
    );
    assert!(rendered.contains("osfu_ws_bus_batches_total{direction=\"received\"} 1"));
    assert!(rendered.contains("osfu_ws_bus_envelopes_total{direction=\"received\"} 2"));
    assert!(rendered.contains("osfu_ws_bus_failures_total{kind=\"send\"} 1"));
}

fn assert_live_and_recording_metrics(rendered: &str) {
    for family in [
        "# HELP osfu_rooms_active Current number of active rooms owned by this runtime.\n# TYPE osfu_rooms_active gauge\nosfu_rooms_active 1\n",
        "# HELP osfu_users_active Current number of active room users owned by this runtime.\n# TYPE osfu_users_active gauge\nosfu_users_active 2\n",
        "# HELP osfu_publications_active Current number of committed or pending published media entries owned by this runtime.\n# TYPE osfu_publications_active gauge\nosfu_publications_active 3\n",
        "# HELP osfu_subscriptions_active Current number of committed or pending consumer subscriptions owned by this runtime.\n# TYPE osfu_subscriptions_active gauge\nosfu_subscriptions_active 4\n",
        "# HELP osfu_subscription_intent_evictions_total Total absent publisher targets evicted from bounded receiver subscription intent.\n# TYPE osfu_subscription_intent_evictions_total counter\nosfu_subscription_intent_evictions_total 5\n",
        "# HELP osfu_recording_rooms_active Current number of rooms with an active recording user.\n# TYPE osfu_recording_rooms_active gauge\nosfu_recording_rooms_active 1\n",
    ] {
        assert_eq!(rendered.matches(family).count(), 1);
    }
    for name in [
        "osfu_rooms_active",
        "osfu_users_active",
        "osfu_publications_active",
        "osfu_subscriptions_active",
        "osfu_subscription_intent_evictions_total",
        "osfu_recording_rooms_active",
    ] {
        assert_eq!(rendered.matches(&format!("\n{name}")).count(), 1);
    }
    assert!(rendered.contains("osfu_transport_users_active 1"));
    assert!(rendered.contains("osfu_transport_health_users{state=\"connected\"} 1"));
    assert!(
        rendered.contains("osfu_recording_actions_total{action=\"start\",outcome=\"accepted\"} 1")
    );
    assert!(
        rendered.contains("osfu_recording_actions_total{action=\"stop\",outcome=\"rejected\"} 1")
    );
    assert!(rendered.contains("osfu_recording_captured_packets_total 1"));
    assert!(rendered.contains("osfu_recording_captured_streams_total 1"));
}

fn assert_transport_lifecycle_metrics(rendered: &str) {
    assert!(
        rendered
            .contains("osfu_transport_health_transitions_total{from=\"unset\",to=\"connected\"} 1")
    );
    assert!(rendered.contains("osfu_rtp_packets_total{direction=\"ingress\"} 1"));
    assert!(rendered.contains("osfu_rtp_payload_bytes_total{direction=\"egress\"} 900"));
    assert!(rendered.contains("osfu_rtp_decoder_refreshes_total{scope=\"rid\"} 1"));
    assert!(rendered.contains("osfu_rtp_decoder_refreshes_total{scope=\"source\"} 1"));
    assert!(rendered.contains("osfu_rtp_forwarded_packets_total{destination=\"local_rtc\"} 1"));
    assert!(
        rendered.contains("osfu_rtp_forwarded_payload_bytes_total{destination=\"recording\"} 700")
    );
    assert!(rendered.contains("osfu_rtc_route_control_total{outcome=\"absorbed\"} 1"));
    assert!(rendered.contains("osfu_rtc_route_control_total{outcome=\"forwarded\"} 1"));
    assert_rtc_keyframe_request_metrics(rendered);
    assert!(rendered.contains("osfu_source_selection_updates_total{selector=\"encoding\"} 1"));
    assert!(rendered.contains("osfu_budget_solver_outcomes_total{outcome=\"degraded\"} 1"));
    assert!(rendered.contains("osfu_transport_ice_state_changes_total{state=\"checking\"} 1"));
    assert!(rendered.contains("osfu_transport_ice_state_changes_total{state=\"connected\"} 1"));
    assert!(rendered.contains("osfu_transport_dtls_connected_total 1"));
    assert!(rendered.contains("osfu_transport_user_lifetime_seconds_bucket{le=\"1\"} 0"));
    assert!(rendered.contains("osfu_transport_user_lifetime_seconds_bucket{le=\"10\"} 1"));
    assert!(rendered.contains("osfu_transport_user_lifetime_seconds_bucket{le=\"+Inf\"} 1"));
    assert!(rendered.contains("osfu_transport_user_lifetime_seconds_sum 1.5"));
    assert!(rendered.contains("osfu_transport_user_lifetime_seconds_count 1"));
    assert!(rendered.contains("osfu_transport_cleanup_failures_total{kind=\"terminal\"} 1"));
}

fn assert_rtc_keyframe_request_metrics(rendered: &str) {
    assert!(rendered.contains("osfu_rtc_keyframe_requests_total{outcome=\"forwarded\"} 1"));
    assert!(rendered.contains("osfu_rtc_keyframe_requests_total{outcome=\"absorbed\"} 1"));
    assert!(rendered.contains("osfu_rtc_keyframe_requests_total{outcome=\"retry\"} 1"));
    assert!(rendered.contains("osfu_rtc_keyframe_requests_total{outcome=\"cleared\"} 1"));
}

fn assert_rtc_recovery_metrics(rendered: &str) {
    for sample in [
        "osfu_rtc_nacks_total{direction=\"sent_to_publisher\"} 1",
        "osfu_rtc_nacks_total{direction=\"received_from_subscriber\"} 1",
        "osfu_rtc_rtx_packets_total{direction=\"received_from_publisher\"} 1",
        "osfu_rtc_rtx_payload_bytes_total{direction=\"received_from_publisher\"} 1200",
        "osfu_rtc_rtcp_ingress_budget_drops_total 1",
        "osfu_rtc_output_budget_exhaustions_total{limit=\"packets\"} 1",
        "osfu_rtc_output_budget_exhaustions_total{limit=\"payload_bytes\"} 1",
        "osfu_rtc_output_budget_exhaustions_total{limit=\"packets_and_payload_bytes\"} 1",
        "osfu_rtc_output_budget_session_closes_total 1",
    ] {
        assert!(rendered.contains(sample));
    }
    assert!(!rendered.contains("direction=\"sent_to_subscriber\""));
}

fn sample_metrics() -> RuntimeMetrics {
    let metrics = RuntimeMetrics::default();
    drop(metrics.track_http_request(HttpRoute::Noop));
    drop(metrics.track_http_request(HttpRoute::Metrics));
    metrics.record_ws_connection_accepted();
    metrics.record_ws_handshake_rejection(Some(WebSocketCloseCode::ProtocolError));
    metrics.record_ws_user_loop_exit(WsSessionLoopExitReason::TransportDisconnected);
    metrics.record_ws_bus_batch_received(2);
    metrics.record_ws_bus_send_failure();
    metrics.record_subscription_intent_evictions(3);
    metrics.record_subscription_intent_evictions(2);
    drop(metrics.track_ws_handshake());
    drop(metrics.track_ws_authentication());
    drop(metrics.track_ws_user_initialization());
    metrics.add_active_transport_users(1);
    metrics.record_transport_health_transition(None, Some(TransportHealthState::Connected));
    metrics.record_recording_start_accepted();
    metrics.record_recording_stop_rejected();
    metrics.record_recording_captured_packet();
    metrics.record_recording_captured_stream();
    let packet_recorder = metrics.register_rtp_worker();
    packet_recorder.record_ingress(1200);
    packet_recorder.record_egress(900);
    packet_recorder.record_decoder_refresh(RtpDecoderRefreshScope::Rid);
    packet_recorder.record_decoder_refresh(RtpDecoderRefreshScope::Source);
    packet_recorder.record_forwarded(RtpForwardDestinationKind::LocalRtc, 900);
    packet_recorder.record_forwarded(RtpForwardDestinationKind::Recording, 700);
    packet_recorder.record_forwarded(RtpForwardDestinationKind::IntraNodeRelay, 500);
    metrics.record_transport_ice_state_change(TransportIceState::Checking);
    metrics.record_transport_ice_state_change(TransportIceState::Connected);
    metrics.record_transport_dtls_connected();
    metrics.record_transport_user_lifetime(Duration::from_millis(1500));
    metrics.record_transport_cleanup_failure();
    let control_recorder = metrics.register_rtc_worker();
    control_recorder.record_rtc_datagram_route(RtcDatagramRoutePath::Indexed);
    control_recorder.record_rtc_datagram_route(RtcDatagramRoutePath::Scan);
    control_recorder.record_rtc_datagram_drop(RtcDatagramDropReason::Malformed);
    control_recorder.record_rtc_datagram_fallback_scan(4);
    control_recorder.record_rtc_nacks(RtcNackDirection::SentToPublisher, 1);
    control_recorder.record_rtc_nacks(RtcNackDirection::ReceivedFromSubscriber, 1);
    control_recorder.record_rtc_rtx_received_from_publisher(1200);
    control_recorder.record_rtc_rtcp_ingress_budget_drop();
    control_recorder.record_rtc_output_budget_exhaustion(RtcOutputBudgetLimit::Packets);
    control_recorder.record_rtc_output_budget_exhaustion(RtcOutputBudgetLimit::PayloadBytes);
    control_recorder
        .record_rtc_output_budget_exhaustion(RtcOutputBudgetLimit::PacketsAndPayloadBytes);
    control_recorder.record_rtc_output_budget_session_close();
    control_recorder.record_rtc_route_control(RtcRouteControlOutcome::Absorbed);
    control_recorder.record_rtc_route_control(RtcRouteControlOutcome::Forwarded);
    control_recorder.record_rtc_route_control(RtcRouteControlOutcome::RouteGatedRelayDrop);
    control_recorder.record_rtc_route_control(RtcRouteControlOutcome::LayerAllowed);
    control_recorder.record_rtc_route_control(RtcRouteControlOutcome::LayerDropped);
    control_recorder.record_rtc_keyframe_request(RtcKeyframeRequestOutcome::Forwarded);
    control_recorder.record_rtc_keyframe_request(RtcKeyframeRequestOutcome::Absorbed);
    control_recorder.record_rtc_keyframe_request(RtcKeyframeRequestOutcome::Retry);
    control_recorder.record_rtc_keyframe_request(RtcKeyframeRequestOutcome::Cleared);
    control_recorder.record_rtc_relay_enqueue(RtcRelayEnqueueResult::IntraNodeEnqueued);
    control_recorder.record_rtc_relay_enqueue(RtcRelayEnqueueResult::IntraNodeOverloaded);
    control_recorder.record_rtc_relay_enqueue(RtcRelayEnqueueResult::IntraNodeClosed);
    control_recorder.record_rtc_relay_mailbox_depth(7);
    control_recorder.record_rtc_relay_drain_batch(4, true);
    control_recorder.record_rtc_remote_control_drop(RtcRemoteControlDropKind::Keyframe);
    control_recorder.record_rtc_remote_control_drop(RtcRemoteControlDropKind::PacketGate);
    control_recorder
        .record_rtc_remote_packet_gate_convergence(RtcRemotePacketGateConvergence::Retry);
    control_recorder
        .record_rtc_remote_packet_gate_convergence(RtcRemotePacketGateConvergence::Flushed);
    metrics.record_source_selection_update(SourceSelectionKind::Encoding);
    metrics.record_budget_solver_outcome(BudgetSolverOutcome::Degraded);
    metrics.record_budget_solver_outcome(BudgetSolverOutcome::Paused);
    metrics.record_budget_solver_outcome(BudgetSolverOutcome::Resumed);
    metrics
}

fn sample_room_gauges() -> RoomGaugeValues {
    RoomGaugeValues {
        rooms: 1,
        users: 2,
        publications: 3,
        subscriptions: 4,
        recording_rooms: 1,
    }
}

#[test]
fn prometheus_export_renders_existing_metric_families() {
    let rendered = render_prometheus(&sample_metrics(), sample_room_gauges());

    assert_eq!(METRIC_FAMILY_COUNT, 81);
    for prefix in ["# HELP ", "# TYPE "] {
        assert_eq!(
            rendered
                .lines()
                .filter(|line| line.starts_with(prefix))
                .count(),
            METRIC_FAMILY_COUNT
        );
    }
    assert_eq!(
        PROMETHEUS_CONTENT_TYPE,
        "text/plain; version=0.0.4; charset=utf-8"
    );
    assert_http_and_websocket_metrics(&rendered);
    assert_live_and_recording_metrics(&rendered);
    assert_transport_lifecycle_metrics(&rendered);
}

#[test]
fn producer_ssrc_binding_metrics_aggregate_workers_with_bounded_outcomes() {
    let metrics = RuntimeMetrics::default();
    let first_worker = metrics.register_rtc_worker();
    let second_worker = metrics.register_rtc_worker();
    first_worker.record_rtc_producer_ssrc_binding(RtcProducerSsrcBindingOutcome::Learned);
    second_worker.record_rtc_producer_ssrc_binding(RtcProducerSsrcBindingOutcome::Learned);
    first_worker.record_rtc_producer_ssrc_binding(RtcProducerSsrcBindingOutcome::Replaced);
    second_worker.record_rtc_producer_ssrc_binding(RtcProducerSsrcBindingOutcome::Rejected);
    let rendered = render_prometheus(&metrics, RoomGaugeValues::default());
    let samples = rendered
        .lines()
        .filter(|line| line.starts_with("osfu_rtc_producer_ssrc_bindings_total"))
        .collect::<Vec<_>>();
    assert_eq!(
        samples,
        [
            "osfu_rtc_producer_ssrc_bindings_total{outcome=\"learned\"} 2",
            "osfu_rtc_producer_ssrc_bindings_total{outcome=\"replaced\"} 1",
            "osfu_rtc_producer_ssrc_bindings_total{outcome=\"rejected\"} 1",
        ]
    );
}

#[test]
fn prometheus_export_keeps_rtp_shape_for_worker_recorders() {
    let metrics = RuntimeMetrics::default();
    let first_worker = metrics.register_rtp_worker_for_media_worker(0);
    let second_worker = metrics.register_rtp_worker_for_media_worker(1);

    first_worker.record_ingress(1200);
    first_worker.record_egress(900);
    first_worker.record_forwarded(RtpForwardDestinationKind::LocalRtc, 900);
    second_worker.record_ingress(300);
    second_worker.record_forwarded(RtpForwardDestinationKind::Recording, 300);

    let rendered = render_prometheus(&metrics, RoomGaugeValues::default());

    assert!(rendered.contains("osfu_rtp_packets_total{direction=\"ingress\"} 2"));
    assert!(rendered.contains("osfu_rtp_payload_bytes_total{direction=\"ingress\"} 1500"));
    assert!(rendered.contains("osfu_rtp_forwarded_packets_total{destination=\"local_rtc\"} 1"));
    assert!(
        rendered.contains("osfu_rtp_forwarded_payload_bytes_total{destination=\"recording\"} 300")
    );
    assert!(
        rendered.contains(
            "osfu_worker_rtp_packets_total{media_worker_id=\"0\",direction=\"ingress\"} 1"
        )
    );
    assert!(
        rendered.contains(
            "osfu_worker_rtp_packets_total{media_worker_id=\"1\",direction=\"ingress\"} 1"
        )
    );
    assert!(rendered.contains(
        "osfu_worker_rtp_payload_bytes_total{media_worker_id=\"0\",direction=\"egress\"} 900"
    ));
    assert!(rendered.contains(
        "osfu_worker_rtp_forwarded_packets_total{media_worker_id=\"0\",destination=\"local_rtc\"} 1"
    ));
    assert!(
        rendered
            .contains("osfu_worker_rtp_forwarded_payload_bytes_total{media_worker_id=\"1\",destination=\"recording\"} 300")
    );
}

#[test]
fn prometheus_export_renders_rtc_datagram_metric_families() {
    let rendered = render_prometheus(&sample_metrics(), sample_room_gauges());

    assert!(rendered.contains("osfu_rtc_datagram_routes_total{path=\"indexed\"} 1"));
    assert!(rendered.contains("osfu_rtc_datagram_routes_total{path=\"scan\"} 1"));
    assert!(rendered.contains("osfu_rtc_datagram_drops_total{reason=\"malformed\"} 1"));
    assert!(rendered.contains("osfu_rtc_datagram_fallback_scans_total 1"));
    assert!(rendered.contains("osfu_rtc_datagram_scan_users_total 4"));
    assert_rtc_recovery_metrics(&rendered);
    assert!(
        rendered.contains("osfu_rtc_route_control_total{outcome=\"route_gated_relay_drop\"} 1")
    );
    assert!(rendered.contains("osfu_rtc_route_control_total{outcome=\"layer_allowed\"} 1"));
    assert!(rendered.contains("osfu_rtc_route_control_total{outcome=\"layer_dropped\"} 1"));
    assert!(rendered.contains(
        "osfu_rtc_relay_enqueues_total{target=\"intra_node_relay\",outcome=\"enqueued\"} 1"
    ));
    assert!(rendered.contains(
        "osfu_rtc_relay_enqueues_total{target=\"intra_node_relay\",outcome=\"overloaded\"} 1"
    ));
    assert!(rendered.contains(
        "osfu_rtc_relay_enqueues_total{target=\"intra_node_relay\",outcome=\"closed\"} 1"
    ));
    assert!(rendered.contains("osfu_rtc_relay_mailbox_depth_samples_total 1"));
    assert!(rendered.contains("osfu_rtc_relay_mailbox_depth_observed_total 7"));
    assert!(rendered.contains("osfu_rtc_relay_drain_batches_total 1"));
    assert!(rendered.contains("osfu_rtc_relay_drained_packets_total 4"));
    assert!(rendered.contains("osfu_rtc_relay_drain_cap_hits_total 1"));
    assert!(rendered.contains("osfu_rtc_remote_control_drops_total{kind=\"keyframe\"} 1"));
    assert!(rendered.contains("osfu_rtc_remote_control_drops_total{kind=\"packet_gate\"} 1"));
    assert!(
        rendered.contains("osfu_rtc_remote_packet_gate_convergence_total{outcome=\"retry\"} 1")
    );
    assert!(
        rendered.contains("osfu_rtc_remote_packet_gate_convergence_total{outcome=\"flushed\"} 1")
    );
}
