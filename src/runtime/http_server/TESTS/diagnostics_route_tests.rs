#![allow(
    clippy::panic,
    reason = "diagnostics route tests use panic for malformed response failures"
)]
#![expect(
    clippy::indexing_slicing,
    clippy::too_many_lines,
    reason = "the diagnostics route tests keep one end-to-end diagnostics scenario with direct field assertions"
)]

use std::{
    collections::BTreeSet,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use o_sfu_protocol::wire::StreamType;
use o_sfu_router::test_support::rtp_samples::{
    sample_client_rtp_capabilities, sample_simulcast_video_rtp_parameters,
};
use o_sfu_telemetry::diagnostics::DiagnosticsTransportHealth;
use serde_json::Value;

use super::fixtures::*;
use crate::{
    application::stream_catalog::source_publish_intent_for_stream_type,
    core::{
        prelude::MediaSession,
        server::{
            room::JoinUserRequest,
            session::{UserId, UserInfo, UserPermissions},
            transport::{TransportQualitySample, TransportSessionHealth},
        },
    },
    runtime::room::Room,
};

const TEST_DIAGNOSTICS_ROOM: &str = "test-room";
const TEST_DIAGNOSTICS_USER: i64 = 1;

fn diagnostics_route_paths() -> [String; 8] {
    let user_id = TEST_DIAGNOSTICS_USER.to_string();
    [
        route::diagnostics::SUMMARY.to_owned(),
        route::diagnostics::ROOMS.to_owned(),
        route::diagnostics::WORKERS.to_owned(),
        route::diagnostics::ROOM.replace("{uuid}", TEST_DIAGNOSTICS_ROOM),
        route::diagnostics::ROOM_USERS.replace("{uuid}", TEST_DIAGNOSTICS_ROOM),
        route::diagnostics::ROOM_GRAPH.replace("{uuid}", TEST_DIAGNOSTICS_ROOM),
        route::diagnostics::USER_GRAPH
            .replace("{uuid}", TEST_DIAGNOSTICS_ROOM)
            .replace("{id}", &user_id),
        room_user_path(TEST_DIAGNOSTICS_ROOM, &user_id),
    ]
}

fn room_user_path(room_id: &str, user_key: &str) -> String {
    route::diagnostics::ROOM_USER
        .replace("{uuid}", room_id)
        .replace("{id}", user_key)
}

fn assert_graph_ids(graph: &Value, room_id: &str, expected: [&str; 2]) {
    for (collection, expected) in ["nodes", "edges"].into_iter().zip(expected) {
        let Some(entries) = graph[collection].as_array() else {
            panic!("graph collection should be an array");
        };
        let actual = entries
            .iter()
            .map(|entry| {
                let Some(id) = entry["id"].as_str() else {
                    panic!("graph entry should have an id");
                };
                id.replace(room_id, "{room}")
            })
            .collect::<BTreeSet<_>>();
        let expected = expected
            .split_ascii_whitespace()
            .map(str::to_owned)
            .collect::<BTreeSet<_>>();
        assert_eq!(entries.len(), expected.len());
        assert_eq!(actual, expected);
    }
}

fn graph_edge_direction<'a>(graph: &'a Value, id: &str) -> Option<&'a str> {
    graph["edges"]
        .as_array()?
        .iter()
        .find(|edge| edge["id"] == id)?["detail__direction"]
        .as_str()
}

async fn diagnostics_status(
    state: &RuntimeState,
    path: &str,
    authorization: Option<&str>,
    expected_status: StatusCode,
) -> TestResult {
    let mut builder = Request::get(path);
    if let Some(authorization) = authorization {
        builder = builder.header(header::AUTHORIZATION, authorization);
    }
    route_status(
        state,
        builder,
        Body::empty(),
        expected_status,
        "diagnostics request should complete",
    )
    .await
}

async fn diagnostics_json<T>(state: &RuntimeState, path: impl AsRef<str>) -> TestResult<T>
where
    T: DeserializeOwned,
{
    route_json(
        state,
        Request::get(path.as_ref()),
        Body::empty(),
        StatusCode::OK,
        "diagnostics request should succeed",
    )
    .await
}

async fn serve_diagnostics_room(
    test_state: &RuntimeTestState,
    issuer: &str,
    remote_address: &str,
) -> TestResult<Arc<Room>> {
    require_ok(
        test_state
            .room_manager
            .serve_room(
                issuer,
                TEST_ROOM_KEY,
                &RoomConfig::default(),
                Some(remote_address),
            )
            .await,
        "test room should be served",
    )
}

async fn join_room_user(
    state: &RuntimeState,
    room: &Room,
    user_id: UserId,
) -> TestResult<(MediaSession, UserOutboundReceiver)> {
    let (tx, rx) = test_outbound_sender(state);
    let session = require_ok(
        state
            .sfu_core
            .admit_user(
                room.uuid(),
                JoinUserRequest {
                    user_id,
                    label: None,
                    permissions: UserPermissions::default(),
                    sender: tx,
                },
            )
            .await,
        "user should join diagnostics room",
    )?;
    Ok((session, rx))
}

async fn make_session_ready(
    room: &Room,
    user_id: &UserId,
    media_transport: &MediaTransport,
) -> TestResult {
    require_some(
        create_transport_session_offer(room, user_id, media_transport).await,
        "transport session offer should be created",
    )?;
    assert!(
        room.test_api()
            .mark_session_ready(user_id, sample_client_rtp_capabilities(), media_transport)
            .await
    );
    Ok(())
}

async fn publish_camera(
    room: &Room,
    user_id: &UserId,
    media_transport: &MediaTransport,
) -> TestResult {
    make_session_ready(room, user_id, media_transport).await?;
    require_some(
        room.test_api()
            .publish_intent(
                user_id,
                &source_publish_intent_for_stream_type(StreamType::Camera),
                sample_simulcast_video_rtp_parameters(None),
                media_transport,
            )
            .await,
        "media publish intent should commit",
    )?;
    Ok(())
}

#[tokio::test]
async fn diagnostics_routes_are_forbidden_without_token_on_public_listener() -> TestResult {
    let mut state = test_state();
    state.config.http.bind_address = SocketAddr::from(([0, 0, 0, 0], 8070));

    for path in diagnostics_route_paths() {
        diagnostics_status(&state, &path, None, StatusCode::FORBIDDEN).await?;
    }
    Ok(())
}

#[tokio::test]
async fn diagnostics_routes_require_the_configured_bearer_token() -> TestResult {
    let mut state = test_state();
    state.config.http.bind_address = SocketAddr::from(([0, 0, 0, 0], 8070));
    state.config.diagnostics.auth_token = Some(String::from("operator-secret"));

    for path in diagnostics_route_paths() {
        diagnostics_status(&state, &path, None, StatusCode::UNAUTHORIZED).await?;
        diagnostics_status(
            &state,
            &path,
            Some("Basic operator-secret"),
            StatusCode::UNAUTHORIZED,
        )
        .await?;
        diagnostics_status(
            &state,
            &path,
            Some("jwt operator-secret"),
            StatusCode::UNAUTHORIZED,
        )
        .await?;
    }

    diagnostics_status(
        &state,
        route::diagnostics::SUMMARY,
        Some("Bearer operator-secret"),
        StatusCode::OK,
    )
    .await
}

#[tokio::test]
async fn diagnostics_routes_return_current_room_and_user_details() -> TestResult {
    let test_state = test_state_with_handles();
    let room = serve_diagnostics_room(&test_state, "issuer-a", "203.0.113.10").await?;
    let alice_user_id = UserId::Integer(1);
    let bob_user_id = UserId::Integer(2);
    let carol_user_id = UserId::Integer(3);
    let (alice_session, alice_rx) =
        join_room_user(&test_state.state, &room, alice_user_id.clone()).await?;
    let (_, bob_rx) = join_room_user(&test_state.state, &room, bob_user_id.clone()).await?;
    let (_, carol_rx) = join_room_user(&test_state.state, &room, carol_user_id.clone()).await?;
    let _receivers = [alice_rx, bob_rx, carol_rx];
    make_session_ready(&room, &bob_user_id, &test_state.media_transport).await?;
    make_session_ready(&room, &carol_user_id, &test_state.media_transport).await?;
    publish_camera(&room, &alice_user_id, &test_state.media_transport).await?;
    for _ in 0..2 {
        alice_session.update_info(UserInfo::default()).await;
    }
    let source_capture = room.diagnostics_detail_capture().await;
    let session_key = |user_id: &UserId| {
        source_capture
            .session_keys()
            .iter()
            .find(|key| key.user_id() == user_id)
            .cloned()
    };
    let alice_key = require_some(session_key(&alice_user_id), "alice key should be captured")?;
    let bob_key = require_some(session_key(&bob_user_id), "bob key should be captured")?;
    let source_key = require_some(
        source_capture.source_keys().next().cloned(),
        "published source key should be captured",
    )?;
    let transport_test = test_state.media_transport.test_api();
    transport_test.set_session_transport_health(&alice_key, TransportSessionHealth::Connected);
    transport_test.set_session_transport_health(&bob_key, TransportSessionHealth::Disconnected);
    transport_test.set_session_transport_quality(
        &alice_key,
        TransportQualitySample {
            latest_bwe_bps: Some(1_234_567),
            rtt_ms: Some(42),
            sample_count: 4,
            ..Default::default()
        },
    );
    let observed_at = Instant::now();
    for (elapsed, bytes) in [
        (Duration::ZERO, 62),
        (Duration::from_millis(500), 63),
        (Duration::from_secs(1), 1),
    ] {
        transport_test
            .record_incoming_media(&source_key, bytes, observed_at + elapsed)
            .await;
    }
    let source_requests = || transport_test.source_diagnostics_request_count();

    let room_summaries: Vec<DiagnosticsRoomSummary> =
        diagnostics_json(&test_state.state, route::diagnostics::ROOMS).await?;
    assert_eq!(room_summaries.len(), 1);
    assert_eq!(room_summaries[0].user_count, 3);
    assert_eq!(room_summaries[0].source_count, 1);
    assert_eq!(room_summaries[0].publication_count, 1);
    assert_eq!(room_summaries[0].subscription_count, 2);
    assert_eq!(room_summaries[0].transport.connected, 1);
    assert_eq!(room_summaries[0].transport.disconnected, 1);
    assert_eq!(room_summaries[0].transport.unknown, 1);
    assert_eq!(room_summaries[0].transport.total, 3);

    let room_users: Vec<DiagnosticsUserSummary> = diagnostics_json(
        &test_state.state,
        route::diagnostics::ROOM_USERS.replace("{uuid}", room.uuid()),
    )
    .await?;
    assert_eq!(room_users.len(), 3);
    let alice_summary = require_some(
        room_users.iter().find(|user| user.user_key == "1"),
        "alice summary should be listed",
    )?;
    assert_eq!(alice_summary.room_id, room.uuid());
    assert_eq!(alice_summary.publication_count, 1);
    assert_eq!(alice_summary.subscription_count, 0);
    assert_eq!(alice_summary.media_worker_id, 0);
    assert_eq!(
        alice_summary.health,
        Some(DiagnosticsTransportHealth::Connected)
    );
    assert_eq!(alice_summary.incoming_bitrate_bps, 1_000);
    assert_eq!(alice_summary.camera_incoming_bitrate_bps, 1_000);

    transport_test.set_packet_loop_delays_ms(vec![Some(7)]);
    let worker_json: Value =
        diagnostics_json(&test_state.state, route::diagnostics::WORKERS).await?;
    assert_eq!(worker_json[0]["pressure"]["packetLoopDelayMs"], 7);
    assert!(worker_json[0]["pressure"].get("packetLoopLagMs").is_none());
    let worker_summaries: Vec<DiagnosticsWorkerSummary> = serde_json::from_value(worker_json)?;
    assert_eq!(worker_summaries.len(), 1);
    let worker_summary = &worker_summaries[0];
    assert_eq!(worker_summary.media_worker_id, 0);
    assert_eq!(worker_summary.room_count, 1);
    assert_eq!(worker_summary.user_count, 3);
    assert_eq!(worker_summary.publication_count, 1);
    assert_eq!(worker_summary.subscription_count, 2);
    assert_eq!(worker_summary.connected_user_count, 1);
    assert_eq!(worker_summary.disconnected_user_count, 1);
    assert_eq!(worker_summary.unknown_user_count, 1);
    assert_eq!(worker_summary.pressure.egress_bitrate_bps, 0);
    assert_eq!(worker_summary.pressure.packet_loop_delay_ms, Some(7));
    assert_eq!(worker_summary.pressure.command_backlog_depth, 0);
    assert_eq!(worker_summary.pressure.relay_mailbox_depth, 0);
    assert_eq!(worker_summary.pressure.worker_pressure_score, 0);
    transport_test.set_packet_loop_delays_ms(vec![None]);
    let unavailable_worker_json: Value =
        diagnostics_json(&test_state.state, route::diagnostics::WORKERS).await?;
    assert!(unavailable_worker_json[0]["pressure"]["packetLoopDelayMs"].is_null());

    let summary: DiagnosticsSummaryResponse =
        diagnostics_json(&test_state.state, route::diagnostics::SUMMARY).await?;
    assert_eq!(summary.rooms_active, 1);
    assert_eq!(summary.users_active, 3);
    assert_eq!(summary.publications_active, 1);
    assert_eq!(summary.subscriptions_active, 2);
    assert_eq!(summary.transport.connected, 1);
    assert_eq!(summary.transport.disconnected, 1);
    assert_eq!(summary.transport.unknown, 1);
    assert_eq!(source_requests(), 0);

    let detail: DiagnosticsRoomDetail = diagnostics_json(
        &test_state.state,
        route::diagnostics::ROOM.replace("{uuid}", room.uuid()),
    )
    .await?;
    assert_eq!(detail.summary.uuid, room.uuid());
    assert_eq!(detail.summary.remote_address, "203.0.113.10");
    assert_eq!(detail.summary.recording_state.recording, Some(false));
    assert_eq!(detail.summary.recording_state.audio, Some(false));
    assert_eq!(detail.summary.recording_state.transcription, Some(false));
    assert_eq!(detail.summary.recording_state.video, Some(false));
    assert_eq!(detail.users.len(), 3);
    assert_eq!(detail.sources.len(), 1);
    assert_eq!(detail.sources[0].source_id, 1);
    assert_eq!(detail.sources[0].current_incoming_bitrate_bps, 1_000);
    assert_eq!(detail.sources[0].encodings.len(), 2);
    assert_eq!(detail.sources[0].encodings[0].rid.as_deref(), Some("lo"));
    assert_eq!(detail.sources[0].encodings[1].rid.as_deref(), Some("hi"));
    let alice_detail = require_some(
        detail
            .users
            .iter()
            .find(|user| user.user_id == alice_user_id),
        "alice detail should be present",
    )?;
    let alice_transport = &alice_detail.transport;
    let alice_quality = &alice_transport.quality_summary;
    assert_eq!(
        alice_transport.health,
        Some(DiagnosticsTransportHealth::Connected)
    );
    assert_eq!(alice_quality.current_incoming_bitrate.total, 1_000);
    assert!(alice_quality.sampled_metrics_available);
    assert_eq!(alice_quality.latest_bwe_bps, Some(1_234_567));
    assert_eq!(alice_quality.rtt_ms, Some(42));
    assert_eq!(alice_quality.sample_count, 4);
    assert_eq!(source_requests(), 1);

    let room_graph: Value = diagnostics_json(
        &test_state.state,
        route::diagnostics::ROOM_GRAPH.replace("{uuid}", room.uuid()),
    )
    .await?;
    assert_graph_ids(
        &room_graph,
        room.uuid(),
        [
            "room:{room} session:{room}:1 session:{room}:2 session:{room}:3 source:{room}:1",
            "member:{room}:1 member:{room}:2 member:{room}:3 publish:{room}:1
             download:{room}:1:2 download:{room}:1:3",
        ],
    );
    assert_eq!(source_requests(), 2);

    diagnostics_status(
        &test_state.state,
        &format!("/internal/diagnostics/node-graph/channels/{}", room.uuid()),
        None,
        StatusCode::NOT_FOUND,
    )
    .await?;

    let alice_graph: Value = diagnostics_json(
        &test_state.state,
        route::diagnostics::USER_GRAPH
            .replace("{uuid}", room.uuid())
            .replace("{id}", alice_user_id.path_segment().as_ref()),
    )
    .await?;
    assert_graph_ids(
        &alice_graph,
        room.uuid(),
        [
            "user:{room}:1 worker:0 source:{room}:1 user:{room}:2 user:{room}:3",
            "transport:{room}:1:0 publish:{room}:1 transport:{room}:2:0 deliver:{room}:1:2
             consume:{room}:1:2 transport:{room}:3:0 deliver:{room}:1:3 consume:{room}:1:3",
        ],
    );
    assert_eq!(
        graph_edge_direction(&alice_graph, &format!("deliver:{}:1:2", room.uuid())),
        Some("outbound")
    );
    assert_eq!(
        graph_edge_direction(&alice_graph, &format!("deliver:{}:1:3", room.uuid())),
        Some("outbound")
    );
    assert_eq!(source_requests(), 3);

    let bob_graph: Value = diagnostics_json(
        &test_state.state,
        route::diagnostics::USER_GRAPH
            .replace("{uuid}", room.uuid())
            .replace("{id}", bob_user_id.path_segment().as_ref()),
    )
    .await?;
    assert_graph_ids(
        &bob_graph,
        room.uuid(),
        [
            "user:{room}:2 worker:0 user:{room}:1 source:{room}:1",
            "transport:{room}:2:0 transport:{room}:1:0 publish:{room}:1
             deliver:{room}:1:2 consume:{room}:1:2",
        ],
    );
    assert_eq!(
        graph_edge_direction(&bob_graph, &format!("deliver:{}:1:2", room.uuid())),
        Some("inbound")
    );
    assert_eq!(source_requests(), 4);

    let session_detail: DiagnosticsUserDetail = diagnostics_json(
        &test_state.state,
        room_user_path(room.uuid(), alice_user_id.path_segment().as_ref()),
    )
    .await?;
    assert_eq!(session_detail.room_id, room.uuid());
    assert_eq!(
        session_detail.recording_state,
        detail.summary.recording_state
    );
    assert_eq!(session_detail.user.user_id, alice_user_id);
    assert_eq!(session_detail.user.publications.len(), 1);
    assert_eq!(session_detail.user.publications[0].source_id, 1);
    assert_eq!(session_detail.user.publications[0].encoding_ids.len(), 2);
    assert_eq!(&session_detail.user.transport, alice_transport);

    let bob_session_detail: DiagnosticsUserDetail = diagnostics_json(
        &test_state.state,
        room_user_path(room.uuid(), bob_user_id.path_segment().as_ref()),
    )
    .await?;
    assert_eq!(bob_session_detail.user.subscriptions.len(), 1);
    let subscription = &bob_session_detail.user.subscriptions[0];
    let selection = &subscription.selection;
    assert_eq!(subscription.source_id, 1);
    assert!(selection.selected_encoding_id.is_some());
    assert_eq!(selection.selected_rid.as_deref(), Some("hi"));
    assert_eq!(
        selection.selection_reason,
        DiagnosticsSourceSelectionReason::ReceiverAdaptation
    );
    assert_eq!(
        selection.latest_receiver_bandwidth_estimate_bps,
        Some(10_000_000)
    );
    assert_eq!(selection.selected_video_budget_bps, Some(10_000_000));
    assert_eq!(selection.active_video_route_count, 1);
    assert_eq!(selection.selected_video_bitrate_bps, 900_000);
    assert_eq!(source_requests(), 4);

    Ok(())
}

#[tokio::test]
async fn diagnostics_user_details_are_scoped_to_the_requested_room() -> TestResult {
    let test_state = test_state_with_handles();
    let first_room = serve_diagnostics_room(&test_state, "issuer-a", "203.0.113.10").await?;
    let second_room = serve_diagnostics_room(&test_state, "issuer-b", "203.0.113.11").await?;
    assert_ne!(first_room.uuid(), second_room.uuid());
    let (_, first_rx) = join_room_user(&test_state.state, &first_room, UserId::Integer(7)).await?;
    let (_, second_rx) =
        join_room_user(&test_state.state, &second_room, UserId::Integer(7)).await?;
    let _receivers = [first_rx, second_rx];

    publish_camera(
        &first_room,
        &UserId::Integer(7),
        &test_state.media_transport,
    )
    .await?;

    let first_detail: DiagnosticsUserDetail =
        diagnostics_json(&test_state.state, room_user_path(first_room.uuid(), "7")).await?;
    let second_detail: DiagnosticsUserDetail =
        diagnostics_json(&test_state.state, room_user_path(second_room.uuid(), "7")).await?;
    assert_eq!(first_detail.room_id, first_room.uuid());
    assert_eq!(first_detail.user.user_id, UserId::Integer(7));
    assert_eq!(first_detail.user.publications.len(), 1);
    assert_eq!(second_detail.room_id, second_room.uuid());
    assert_eq!(second_detail.user.user_id, UserId::Integer(7));
    assert!(second_detail.user.publications.is_empty());
    for path in [
        room_user_path("missing-room", "404"),
        room_user_path(first_room.uuid(), "404"),
        "/internal/diagnostics/users/7".to_owned(),
    ] {
        diagnostics_status(&test_state.state, &path, None, StatusCode::NOT_FOUND).await?;
    }
    Ok(())
}

#[tokio::test]
async fn diagnostics_user_detail_tracks_replacement_and_teardown() -> TestResult {
    let test_state = test_state_with_handles();
    let room = serve_diagnostics_room(&test_state, "issuer-replacement", "203.0.113.12").await?;
    let user_id = UserId::Integer(9);
    let (_, _first_rx) = join_room_user(&test_state.state, &room, user_id.clone()).await?;
    let (replacement_tx, _replacement_rx) = test_outbound_sender(&test_state.state);
    let replacement_connection = require_ok(
        room.test_api()
            .join_user(
                user_id.clone(),
                None,
                UserPermissions::default(),
                replacement_tx,
            )
            .await,
        "replacement user should join",
    )?;
    let path = room_user_path(room.uuid(), "9");
    let detail: DiagnosticsUserDetail = diagnostics_json(&test_state.state, &path).await?;
    assert_eq!(detail.room_id, room.uuid());
    assert_eq!(detail.user.user_id, user_id);
    assert_eq!(
        detail.user.transport.connection_id,
        replacement_connection.as_u64()
    );
    assert!(
        test_state
            .room_manager
            .close_session(
                room.uuid(),
                &user_id,
                replacement_connection,
                &test_state.media_transport,
            )
            .await
    );
    diagnostics_status(&test_state.state, &path, None, StatusCode::NOT_FOUND).await
}
