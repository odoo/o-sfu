use std::time::Duration;

use o_sfu_model::WebSocketCloseCode;

use super::{
    BudgetSolverOutcome, HttpRoute, MediaQualityLossDirection, MediaQualitySample, MetricName,
    RtcDatagramDropReason, RtcDatagramRoutePath, RtcKeyframeRequestOutcome, RtcNackDirection,
    RtcOutputBudgetLimit, RtcRelayEnqueueResult, RtcRemoteControlDropKind,
    RtcRemotePacketGateConvergence, RtcRouteControlOutcome, RtpDecoderRefreshScope,
    RtpForwardDestinationKind, RtpRelayDropKind, RuntimeMetrics, RuntimeMetricsSnapshot,
    SourceSelectionKind, TransportHealthState, TransportIceState, WsSessionLoopExitReason,
    test_support::{RuntimeMetricsSnapshotLookup, RuntimeMetricsSnapshotTestExt},
};

fn assert_recording_metrics(snapshot: &RuntimeMetricsSnapshot) {
    assert_eq!(snapshot.recording_start_accepted(), 1);
    assert_eq!(snapshot.recording_captured_packets(), 1);
    assert_eq!(snapshot.recording_captured_streams(), 1);
}

fn assert_transport_lifecycle_metrics(snapshot: &RuntimeMetricsSnapshot) {
    assert_eq!(
        snapshot.transport_health_transitions_unset_to_connected(),
        1
    );
    assert_eq!(
        snapshot.transport_health_transitions_unset_to_disconnected(),
        0
    );
    assert_eq!(
        snapshot.transport_health_transitions_connected_to_disconnected(),
        0
    );
    assert_eq!(
        snapshot.transport_health_transitions_disconnected_to_connected(),
        0
    );
    assert_eq!(
        snapshot.transport_health_transitions_connected_to_unset(),
        0
    );
    assert_eq!(
        snapshot.transport_health_transitions_disconnected_to_unset(),
        0
    );
    assert_eq!(snapshot.transport_ice_state_changes_new(), 0);
    assert_eq!(snapshot.transport_ice_state_changes_checking(), 1);
    assert_eq!(snapshot.transport_ice_state_changes_connected(), 1);
    assert_eq!(snapshot.transport_ice_state_changes_completed(), 0);
    assert_eq!(snapshot.transport_ice_state_changes_disconnected(), 0);
    assert_eq!(snapshot.transport_dtls_connected(), 1);
    assert_eq!(snapshot.transport_user_lifetime_le_1_second(), 0);
    assert_eq!(snapshot.transport_user_lifetime_le_10_seconds(), 1);
    assert_eq!(snapshot.transport_user_lifetime_le_60_seconds(), 1);
    assert_eq!(snapshot.transport_user_lifetime_le_300_seconds(), 1);
    assert_eq!(snapshot.transport_user_lifetime_count(), 1);
    assert_eq!(snapshot.transport_user_lifetime_sum_micros(), 1_500_000);
    assert_eq!(
        snapshot.counter_value(
            MetricName::TransportCleanupFailuresTotal,
            &[("kind", "terminal")]
        ),
        1
    );
}

fn assert_rtp_metrics(snapshot: &RuntimeMetricsSnapshot) {
    assert_eq!(snapshot.rtp_packets_ingress(), 1);
    assert_eq!(snapshot.rtp_packets_egress(), 1);
    assert_eq!(snapshot.rtp_payload_bytes_ingress(), 1200);
    assert_eq!(snapshot.rtp_payload_bytes_egress(), 900);
    assert_eq!(snapshot.rtp_decoder_refreshes_rid(), 1);
    assert_eq!(snapshot.rtp_decoder_refreshes_source(), 1);
}

fn assert_forwarding_volume_metrics(snapshot: &RuntimeMetricsSnapshot) {
    assert_eq!(snapshot.rtp_forwarded_packets_local_rtc(), 1);
    assert_eq!(snapshot.rtp_forwarded_packets_recording(), 1);
    assert_eq!(snapshot.rtp_forwarded_packets_intra_node_relay(), 1);
    assert_eq!(snapshot.rtp_forwarded_payload_bytes_local_rtc(), 900);
    assert_eq!(snapshot.rtp_forwarded_payload_bytes_recording(), 700);
    assert_eq!(snapshot.rtp_forwarded_payload_bytes_intra_node_relay(), 500);
    assert_eq!(snapshot.rtp_relay_overload_drops_intra_node_relay(), 1);
}

fn assert_rtc_datagram_and_route_control_metrics(snapshot: &RuntimeMetricsSnapshot) {
    assert_rtc_datagram_metrics(snapshot);
    assert_rtc_recovery_metrics(snapshot);
    assert_rtc_route_control_metrics(snapshot);
    assert_rtc_relay_pressure_metrics(snapshot);
    assert_rtc_remote_control_metrics(snapshot);
}

fn assert_rtc_recovery_metrics(snapshot: &RuntimeMetricsSnapshot) {
    assert_eq!(snapshot.rtc_nacks(RtcNackDirection::SentToPublisher), 1);
    assert_eq!(
        snapshot.rtc_nacks(RtcNackDirection::ReceivedFromSubscriber),
        1
    );
    assert_eq!(snapshot.rtc_rtx_packets_received_from_publisher(), 1);
    assert_eq!(
        snapshot.rtc_rtx_payload_bytes_received_from_publisher(),
        1200
    );
    assert_eq!(snapshot.rtc_rtcp_ingress_budget_drops(), 1);
    for limit in [
        RtcOutputBudgetLimit::Packets,
        RtcOutputBudgetLimit::PayloadBytes,
        RtcOutputBudgetLimit::PacketsAndPayloadBytes,
    ] {
        assert_eq!(snapshot.rtc_output_budget_exhaustions(limit), 1);
    }
    assert_eq!(snapshot.rtc_output_budget_session_closes(), 1);
}

fn assert_aggregated_rtc_recovery_metrics(snapshot: &RuntimeMetricsSnapshot) {
    assert_eq!(snapshot.rtc_nacks(RtcNackDirection::SentToPublisher), 5);
    assert_eq!(snapshot.rtc_rtx_packets_received_from_publisher(), 2);
    assert_eq!(
        snapshot.rtc_rtx_payload_bytes_received_from_publisher(),
        1200
    );
    assert_eq!(snapshot.rtc_rtcp_ingress_budget_drops(), 2);
    assert_eq!(
        snapshot.rtc_output_budget_exhaustions(RtcOutputBudgetLimit::Packets),
        2
    );
    assert_eq!(snapshot.rtc_output_budget_session_closes(), 2);
}

fn assert_rtc_datagram_metrics(snapshot: &RuntimeMetricsSnapshot) {
    assert_eq!(snapshot.rtc_datagram_routes_indexed(), 1);
    assert_eq!(snapshot.rtc_datagram_routes_scan(), 1);
    assert_eq!(snapshot.rtc_datagram_drops_recent_miss_cache(), 1);
    assert_eq!(snapshot.rtc_datagram_drops_source_rate_limited(), 1);
    assert_eq!(snapshot.rtc_datagram_drops_no_user(), 1);
    assert_eq!(snapshot.rtc_datagram_drops_malformed(), 1);
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 1);
    assert_eq!(snapshot.rtc_datagram_scan_users(), 3);
}

fn assert_rtc_route_control_metrics(snapshot: &RuntimeMetricsSnapshot) {
    assert_eq!(snapshot.rtc_route_control_absorbed(), 1);
    assert_eq!(snapshot.rtc_route_control_forwarded(), 1);
    assert_eq!(snapshot.rtc_route_control_route_gated_relay_drops(), 1);
    assert_eq!(snapshot.rtc_route_control_layer_allowed(), 1);
    assert_eq!(snapshot.rtc_route_control_layer_dropped(), 1);
    assert_eq!(snapshot.rtc_keyframe_requests_forwarded(), 1);
    assert_eq!(snapshot.rtc_keyframe_requests_absorbed(), 1);
    assert_eq!(snapshot.rtc_keyframe_requests_retried(), 1);
    assert_eq!(snapshot.rtc_keyframe_requests_cleared(), 1);
}

fn assert_rtc_relay_pressure_metrics(snapshot: &RuntimeMetricsSnapshot) {
    assert_eq!(snapshot.rtc_relay_enqueue_intra_node_enqueued(), 1);
    assert_eq!(snapshot.rtc_relay_enqueue_intra_node_overloaded(), 1);
    assert_eq!(snapshot.rtc_relay_enqueue_intra_node_closed(), 1);
    assert_eq!(snapshot.rtc_relay_mailbox_depth_samples(), 1);
    assert_eq!(snapshot.rtc_relay_mailbox_depth_observed(), 7);
    assert_eq!(snapshot.rtc_relay_drain_batches(), 1);
    assert_eq!(snapshot.rtc_relay_drained_packets(), 4);
    assert_eq!(snapshot.rtc_relay_drain_cap_hits(), 1);
}

fn assert_rtc_remote_control_metrics(snapshot: &RuntimeMetricsSnapshot) {
    assert_eq!(snapshot.rtc_remote_control_keyframe_drops(), 1);
    assert_eq!(snapshot.rtc_remote_control_packet_gate_drops(), 1);
    assert_eq!(snapshot.rtc_remote_packet_gate_retries(), 1);
    assert_eq!(snapshot.rtc_remote_packet_gate_flushes(), 1);
}

fn assert_source_selection_metrics(snapshot: &RuntimeMetricsSnapshot) {
    for (selector, expected) in [("open", 0), ("encoding", 1)] {
        assert_eq!(
            snapshot.counter_value(
                MetricName::SourceSelectionUpdatesTotal,
                &[("selector", selector)]
            ),
            expected
        );
    }
    for outcome in ["degraded", "paused", "resumed"] {
        assert_eq!(
            snapshot.counter_value(
                MetricName::BudgetSolverOutcomesTotal,
                &[("outcome", outcome)]
            ),
            1
        );
    }
}

fn assert_control_plane_metrics(snapshot: &RuntimeMetricsSnapshot) {
    assert_eq!(snapshot.http_inflight().noop, 1);
    assert_eq!(snapshot.http_inflight().metrics, 0);
    assert_eq!(snapshot.http_request_duration().noop.count, 0);
    assert_eq!(snapshot.http_request_duration().metrics.count, 1);
    assert_eq!(snapshot.ws_handshake_duration().count, 1);
    assert_eq!(snapshot.ws_auth_duration().count, 1);
    assert_eq!(snapshot.ws_user_initialize_duration().count, 1);
}

fn record_http_metrics(metrics: &RuntimeMetrics) -> impl Drop + '_ {
    drop(metrics.track_http_request(HttpRoute::Room));
    metrics.record_http_room_success();
    metrics.record_http_room_unauthorized();
    drop(metrics.track_http_request(HttpRoute::Disconnect));
    metrics.record_http_disconnect_success();
    metrics.record_http_disconnect_bad_request();
    metrics.record_http_disconnect_unprocessable_entity();
    drop(metrics.track_http_request(HttpRoute::Metrics));
    metrics.track_http_request(HttpRoute::Noop)
}

fn record_websocket_metrics(metrics: &RuntimeMetrics) {
    metrics.record_ws_connection_accepted();
    metrics.record_ws_handshake_credentials_received();
    metrics.record_ws_handshake_rejection(Some(WebSocketCloseCode::AuthTimeout));
    metrics.record_ws_user_joined();
    metrics.record_ws_user_loop_started();
    metrics.record_ws_user_loop_exit(WsSessionLoopExitReason::UserClosed);
    metrics.record_ws_bus_batch_received(3);
    metrics.record_ws_bus_invalid_input_failure();
    metrics.record_ws_bus_unsupported_feature_failure();
    metrics.record_ws_bus_client_request();
    metrics.record_ws_bus_client_message();
    metrics.record_ws_bus_batch_sent(2);
    metrics.record_ws_bus_batches_sent(2, 65);
    metrics.record_ws_bus_send_failure();
    drop(metrics.track_ws_handshake());
    drop(metrics.track_ws_authentication());
    drop(metrics.track_ws_user_initialization());
}

fn assert_http_metrics(snapshot: &RuntimeMetricsSnapshot) {
    assert_eq!(snapshot.http_noop_requests(), 1);
    assert_eq!(snapshot.http_room_requests(), 1);
    assert_eq!(snapshot.http_room_success(), 1);
    assert_eq!(snapshot.http_room_unauthorized(), 1);
    assert_eq!(snapshot.http_disconnect_requests(), 1);
    assert_eq!(snapshot.http_disconnect_success(), 1);
    assert_eq!(snapshot.http_disconnect_bad_request(), 1);
    assert_eq!(snapshot.http_disconnect_unprocessable_entity(), 1);
    assert_eq!(snapshot.http_metrics_requests(), 1);
}

fn assert_websocket_metrics(snapshot: &RuntimeMetricsSnapshot) {
    assert_eq!(snapshot.ws_connections_accepted(), 1);
    assert_eq!(snapshot.ws_handshake_credentials_received(), 1);
    assert_eq!(snapshot.ws_handshake_rejected_timeout(), 1);
    assert_eq!(snapshot.ws_handshake_rejected_protocol_error(), 0);
    assert_eq!(snapshot.ws_users_joined(), 1);
    assert_eq!(snapshot.ws_user_loops_started(), 1);
    assert_eq!(snapshot.ws_user_loop_exits_user_closed(), 1);
    assert_eq!(snapshot.ws_user_loop_exits_ping_timeout(), 0);
    assert_eq!(snapshot.ws_user_loop_exits_transport_disconnected(), 0);
    assert_eq!(snapshot.ws_bus_parse_failures(), 2);
    assert_eq!(snapshot.ws_bus_invalid_input_failures(), 1);
    assert_eq!(snapshot.ws_bus_unsupported_feature_failures(), 1);
    assert_eq!(snapshot.ws_bus_batches_received(), 1);
    assert_eq!(snapshot.ws_bus_envelopes_received(), 3);
    assert_eq!(snapshot.ws_bus_client_requests(), 1);
    assert_eq!(snapshot.ws_bus_client_messages(), 1);
    assert_eq!(snapshot.ws_bus_batches_sent(), 3);
    assert_eq!(snapshot.ws_bus_envelopes_sent(), 67);
    assert_eq!(snapshot.ws_bus_send_failures(), 1);
}

#[test]
fn metrics_snapshot_tracks_http_and_websocket_counters() {
    let metrics = RuntimeMetrics::default();
    let _request = record_http_metrics(&metrics);
    record_websocket_metrics(&metrics);

    let snapshot = metrics.snapshot();

    assert_http_metrics(&snapshot);
    assert_websocket_metrics(&snapshot);
    assert_control_plane_metrics(&snapshot);
}

#[test]
fn metric_guards_finish_dropped_operations() {
    let metrics = RuntimeMetrics::default();
    let request = metrics.track_http_request(HttpRoute::Room);
    assert_eq!(
        metrics
            .snapshot()
            .gauge_value(MetricName::HttpInflightRequests, &[("route", "room")]),
        1
    );

    drop(request);
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.http_room_requests(), 1);
    assert_eq!(
        snapshot.gauge_value(MetricName::HttpInflightRequests, &[("route", "room")]),
        0
    );
    assert_eq!(
        snapshot
            .histogram_count_value(MetricName::HttpRequestDurationSeconds, &[("route", "room")]),
        1
    );

    let ws_counts = || {
        let snapshot = metrics.snapshot();
        [
            snapshot.ws_handshake_duration().count,
            snapshot.ws_auth_duration().count,
            snapshot.ws_user_initialize_duration().count,
        ]
    };
    drop(metrics.track_ws_handshake());
    assert_eq!(ws_counts(), [1, 0, 0]);
    drop(metrics.track_ws_authentication());
    assert_eq!(ws_counts(), [1, 1, 0]);
    drop(metrics.track_ws_user_initialization());
    assert_eq!(ws_counts(), [1, 1, 1]);
}

#[test]
fn metrics_snapshot_tracks_websocket_outbound_queue_pressure() {
    let metrics = RuntimeMetrics::default();
    metrics.add_ws_outbound_queued_messages(2);
    metrics.record_ws_outbound_queue_overflow();

    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.ws_outbound_queued_messages(), 2);
    assert_eq!(snapshot.ws_outbound_queue_overflows(), 1);
}

#[test]
fn metrics_snapshot_tracks_live_gauges_and_rtp_counters() {
    let metrics = RuntimeMetrics::default();
    metrics.add_active_transport_users(1);
    metrics.record_transport_health_transition(None, Some(TransportHealthState::Connected));
    metrics.record_recording_start_accepted();
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
    metrics.record_rtp_relay_overload_drop(RtpRelayDropKind::IntraNodeRelay);
    metrics.record_transport_ice_state_change(TransportIceState::Checking);
    metrics.record_transport_ice_state_change(TransportIceState::Connected);
    metrics.record_transport_dtls_connected();
    metrics.record_transport_user_lifetime(Duration::from_millis(1500));
    metrics.record_transport_cleanup_failure();
    let control_recorder = metrics.register_rtc_worker();
    control_recorder.record_rtc_datagram_route(RtcDatagramRoutePath::Indexed);
    control_recorder.record_rtc_datagram_route(RtcDatagramRoutePath::Scan);
    control_recorder.record_rtc_datagram_drop(RtcDatagramDropReason::RecentMissCache);
    control_recorder.record_rtc_datagram_drop(RtcDatagramDropReason::SourceRateLimited);
    control_recorder.record_rtc_datagram_drop(RtcDatagramDropReason::NoUser);
    control_recorder.record_rtc_datagram_drop(RtcDatagramDropReason::Malformed);
    control_recorder.record_rtc_datagram_fallback_scan(3);
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

    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.active_transport_users(), 1);
    assert_eq!(snapshot.connected_transport_users(), 1);
    assert_eq!(snapshot.disconnected_transport_users(), 0);
    assert_recording_metrics(&snapshot);
    assert_transport_lifecycle_metrics(&snapshot);
    assert_rtp_metrics(&snapshot);
    assert_forwarding_volume_metrics(&snapshot);
    assert_rtc_datagram_and_route_control_metrics(&snapshot);
    assert_source_selection_metrics(&snapshot);
}

#[test]
fn metrics_snapshot_keeps_rtp_counts_after_worker_handle_drop() {
    let metrics = RuntimeMetrics::default();
    {
        let worker = metrics.register_rtp_worker();
        worker.record_ingress(100);
    }
    let replacement_worker = metrics.register_rtp_worker();
    replacement_worker.record_ingress(50);

    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.rtp_packets_ingress(), 2);
    assert_eq!(snapshot.rtp_payload_bytes_ingress(), 150);
}

#[test]
fn metrics_snapshot_aggregates_worker_local_rtc_recorders() {
    let metrics = RuntimeMetrics::default();
    let first_worker = metrics.register_rtc_worker();
    let second_worker = metrics.register_rtc_worker();

    first_worker.record_rtc_datagram_route(RtcDatagramRoutePath::Indexed);
    first_worker.record_rtc_datagram_drop(RtcDatagramDropReason::NoUser);
    first_worker.record_rtc_datagram_fallback_scan(3);
    first_worker.record_rtc_nacks(RtcNackDirection::SentToPublisher, 2);
    first_worker.record_rtc_rtx_received_from_publisher(700);
    first_worker.record_rtc_rtcp_ingress_budget_drop();
    first_worker.record_rtc_output_budget_exhaustion(RtcOutputBudgetLimit::Packets);
    first_worker.record_rtc_output_budget_session_close();
    first_worker.record_rtc_route_control(RtcRouteControlOutcome::Forwarded);
    first_worker.record_rtc_keyframe_request(RtcKeyframeRequestOutcome::Retry);
    first_worker.record_rtc_relay_enqueue(RtcRelayEnqueueResult::IntraNodeEnqueued);
    first_worker.record_rtc_relay_mailbox_depth(3);
    first_worker.record_rtc_relay_drain_batch(2, true);
    first_worker.record_rtc_remote_control_drop(RtcRemoteControlDropKind::PacketGate);
    first_worker.record_rtc_remote_packet_gate_convergence(RtcRemotePacketGateConvergence::Retry);
    second_worker.record_rtc_datagram_drop(RtcDatagramDropReason::Malformed);
    second_worker.record_rtc_datagram_route(RtcDatagramRoutePath::Scan);
    second_worker.record_rtc_nacks(RtcNackDirection::SentToPublisher, 3);
    second_worker.record_rtc_rtx_received_from_publisher(500);
    second_worker.record_rtc_rtcp_ingress_budget_drop();
    second_worker.record_rtc_output_budget_exhaustion(RtcOutputBudgetLimit::Packets);
    second_worker.record_rtc_output_budget_session_close();
    second_worker.record_rtc_route_control(RtcRouteControlOutcome::Absorbed);
    second_worker.record_rtc_keyframe_request(RtcKeyframeRequestOutcome::Cleared);
    second_worker.record_rtc_relay_enqueue(RtcRelayEnqueueResult::IntraNodeClosed);
    second_worker.record_rtc_relay_mailbox_depth(4);
    second_worker.record_rtc_relay_drain_batch(3, false);
    second_worker.record_rtc_remote_control_drop(RtcRemoteControlDropKind::Keyframe);
    second_worker
        .record_rtc_remote_packet_gate_convergence(RtcRemotePacketGateConvergence::Flushed);

    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.rtc_datagram_routes_indexed(), 1);
    assert_eq!(snapshot.rtc_datagram_routes_scan(), 1);
    assert_eq!(snapshot.rtc_datagram_drops_no_user(), 1);
    assert_eq!(snapshot.rtc_datagram_drops_malformed(), 1);
    assert_eq!(snapshot.rtc_datagram_fallback_scans(), 1);
    assert_eq!(snapshot.rtc_datagram_scan_users(), 3);
    assert_aggregated_rtc_recovery_metrics(&snapshot);
    assert_eq!(snapshot.rtc_route_control_forwarded(), 1);
    assert_eq!(snapshot.rtc_route_control_absorbed(), 1);
    assert_eq!(snapshot.rtc_keyframe_requests_retried(), 1);
    assert_eq!(snapshot.rtc_keyframe_requests_cleared(), 1);
    assert_eq!(snapshot.rtc_relay_enqueue_intra_node_enqueued(), 1);
    assert_eq!(snapshot.rtc_relay_enqueue_intra_node_closed(), 1);
    assert_eq!(snapshot.rtc_relay_mailbox_depth_samples(), 2);
    assert_eq!(snapshot.rtc_relay_mailbox_depth_observed(), 7);
    assert_eq!(snapshot.rtc_relay_drain_batches(), 2);
    assert_eq!(snapshot.rtc_relay_drained_packets(), 5);
    assert_eq!(snapshot.rtc_relay_drain_cap_hits(), 1);
    assert_eq!(snapshot.rtc_remote_control_keyframe_drops(), 1);
    assert_eq!(snapshot.rtc_remote_control_packet_gate_drops(), 1);
    assert_eq!(snapshot.rtc_remote_packet_gate_retries(), 1);
    assert_eq!(snapshot.rtc_remote_packet_gate_flushes(), 1);
}

#[test]
fn metrics_snapshot_keeps_rtc_counts_after_worker_handle_drop() {
    let metrics = RuntimeMetrics::default();
    {
        let worker = metrics.register_rtc_worker();
        worker.record_rtc_datagram_drop(RtcDatagramDropReason::NoUser);
    }
    let replacement_worker = metrics.register_rtc_worker();
    replacement_worker.record_rtc_datagram_drop(RtcDatagramDropReason::NoUser);

    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.rtc_datagram_drops_no_user(), 2);
}

#[test]
fn transport_health_transition_updates_connected_and_disconnected_gauges() {
    let metrics = RuntimeMetrics::default();

    metrics.record_transport_health_transition(None, Some(TransportHealthState::Connected));
    metrics.record_transport_health_transition(
        Some(TransportHealthState::Connected),
        Some(TransportHealthState::Disconnected),
    );
    metrics.record_transport_health_transition(Some(TransportHealthState::Disconnected), None);

    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.connected_transport_users(), 0);
    assert_eq!(snapshot.disconnected_transport_users(), 0);
    assert_eq!(
        snapshot.transport_health_transitions_unset_to_connected(),
        1
    );
    assert_eq!(
        snapshot.transport_health_transitions_connected_to_disconnected(),
        1
    );
    assert_eq!(
        snapshot.transport_health_transitions_disconnected_to_unset(),
        1
    );
    assert_eq!(
        snapshot.transport_health_transitions_unset_to_disconnected(),
        0
    );
    assert_eq!(
        snapshot.transport_health_transitions_disconnected_to_connected(),
        0
    );
    assert_eq!(
        snapshot.transport_health_transitions_connected_to_unset(),
        0
    );
}

#[test]
fn transport_lifecycle_metrics_track_ice_and_dtls_events() {
    let metrics = RuntimeMetrics::default();

    metrics.record_transport_ice_state_change(TransportIceState::New);
    metrics.record_transport_ice_state_change(TransportIceState::Checking);
    metrics.record_transport_ice_state_change(TransportIceState::Connected);
    metrics.record_transport_ice_state_change(TransportIceState::Completed);
    metrics.record_transport_ice_state_change(TransportIceState::Disconnected);
    metrics.record_transport_dtls_connected();
    metrics.record_transport_user_lifetime(Duration::from_secs(301));

    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.transport_ice_state_changes_new(), 1);
    assert_eq!(snapshot.transport_ice_state_changes_checking(), 1);
    assert_eq!(snapshot.transport_ice_state_changes_connected(), 1);
    assert_eq!(snapshot.transport_ice_state_changes_completed(), 1);
    assert_eq!(snapshot.transport_ice_state_changes_disconnected(), 1);
    assert_eq!(snapshot.transport_dtls_connected(), 1);
    assert_eq!(snapshot.transport_user_lifetime_le_1_second(), 0);
    assert_eq!(snapshot.transport_user_lifetime_le_10_seconds(), 0);
    assert_eq!(snapshot.transport_user_lifetime_le_60_seconds(), 0);
    assert_eq!(snapshot.transport_user_lifetime_le_300_seconds(), 0);
    assert_eq!(snapshot.transport_user_lifetime_count(), 1);
    assert_eq!(snapshot.transport_user_lifetime_sum_micros(), 301_000_000);
}

#[test]
fn histograms_preserve_inclusive_bounds_and_overflow() {
    let metrics = RuntimeMetrics::default();
    for duration in [Duration::from_secs(5), Duration::from_secs(6)] {
        metrics.ws_auth_duration.observe(duration);
        metrics.record_media_quality_rtt(MediaQualitySample::Peer, duration);
    }

    let snapshot = metrics.snapshot();
    for (name, labels) in [
        (MetricName::WsAuthDurationSeconds, &[][..]),
        (
            MetricName::MediaQualityRttSeconds,
            &[("sample", "peer")][..],
        ),
    ] {
        assert_eq!(snapshot.histogram_bucket_value(name, labels, "5"), 1);
        assert_eq!(snapshot.histogram_count_value(name, labels), 2);
        assert_eq!(
            snapshot.histogram_sum_micros_value(name, labels),
            11_000_000
        );
    }

    let rendered = super::render_prometheus_text(&metrics, super::RoomGaugeValues::default());
    for expected in [
        "osfu_ws_auth_duration_seconds_bucket{le=\"+Inf\"} 2",
        "osfu_media_quality_rtt_seconds_bucket{sample=\"peer\",le=\"+Inf\"} 2",
    ] {
        assert!(rendered.lines().any(|line| line == expected));
    }
}

#[test]
fn metrics_snapshot_tracks_sampled_media_quality() {
    let metrics = RuntimeMetrics::default();

    metrics.record_media_quality_sample(MediaQualitySample::Peer);
    metrics.record_media_quality_sample(MediaQualitySample::MediaIngress);
    metrics.record_media_quality_rtt(MediaQualitySample::Peer, Duration::from_millis(120));
    metrics.record_media_quality_loss_ppm(MediaQualityLossDirection::Ingress, 25_000);
    metrics.record_media_quality_loss_ppm(MediaQualityLossDirection::Egress, 40_000);
    metrics.record_media_quality_bwe_bps(1_250_000);
    metrics.record_media_quality_jitter_rtp_timestamp_units(180);

    let snapshot = metrics.snapshot();

    assert_eq!(
        snapshot.counter_value(MetricName::MediaQualitySamplesTotal, &[("sample", "peer")]),
        1
    );
    assert_eq!(
        snapshot.counter_value(
            MetricName::MediaQualitySamplesTotal,
            &[("sample", "media_ingress")]
        ),
        1
    );
    assert_eq!(
        snapshot.histogram_bucket_value(
            MetricName::MediaQualityRttSeconds,
            &[("sample", "peer")],
            "0.25"
        ),
        1
    );
    assert_eq!(
        snapshot.histogram_count_value(MetricName::MediaQualityRttSeconds, &[("sample", "peer")]),
        1
    );
    assert_eq!(
        snapshot.counter_value(
            MetricName::MediaQualityLossPpmObservedTotal,
            &[("direction", "ingress")]
        ),
        25_000
    );
    assert_eq!(
        snapshot.counter_value(
            MetricName::MediaQualityLossObservationsTotal,
            &[("direction", "ingress")]
        ),
        1
    );
    assert_eq!(
        snapshot.counter_value(
            MetricName::MediaQualityLossPpmObservedTotal,
            &[("direction", "egress")]
        ),
        40_000
    );
    assert_eq!(
        snapshot.counter_value(MetricName::MediaQualityBweBpsObservedTotal, &[]),
        1_250_000
    );
    assert_eq!(
        snapshot.counter_value(MetricName::MediaQualityBweObservationsTotal, &[]),
        1
    );
    assert_eq!(
        snapshot.counter_value(
            MetricName::MediaQualityJitterRtpTimestampUnitsObservedTotal,
            &[]
        ),
        180
    );
    assert_eq!(
        snapshot.counter_value(MetricName::MediaQualityJitterObservationsTotal, &[]),
        1
    );
}

#[test]
fn handshake_rejection_buckets_are_distinct() {
    let metrics = RuntimeMetrics::default();
    metrics.record_ws_handshake_rejection(Some(WebSocketCloseCode::AuthFailed));
    metrics.record_ws_handshake_rejection(Some(WebSocketCloseCode::ProtocolError));
    metrics.record_ws_handshake_rejection(Some(WebSocketCloseCode::RoomFull));
    metrics.record_ws_handshake_rejection(Some(WebSocketCloseCode::Error));

    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.ws_handshake_rejected_authentication_failed(), 1);
    assert_eq!(snapshot.ws_handshake_rejected_protocol_error(), 1);
    assert_eq!(snapshot.ws_handshake_rejected_room_full(), 1);
    assert_eq!(snapshot.ws_handshake_rejected_error(), 1);
}
