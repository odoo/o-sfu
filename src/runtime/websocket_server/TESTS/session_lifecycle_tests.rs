use super::fixtures::*;
use crate::runtime::media_transport::TransportSessionHealth;

#[tokio::test]
async fn websocket_sends_ping_frames_and_accepts_pongs() -> TestResult {
    let server = TestServerBuilder::new()
        .user_timeout_ms(200)
        .ping_interval_ms(20)
        .spawn_required()
        .await?;
    let room = require_some(
        create_room(&server, "issuer-ping", CreateRoomQuery::default()).await,
        "test room should be served",
    )?;
    let user_id = UserId::Integer(410);
    let token = room_token(&room, user_id.clone())?;
    let (mut websocket, _welcome) = require_some(
        authenticate_and_read_welcome(&server, &token).await,
        "user should authenticate",
    )?;
    require_some(
        complete_initial_negotiation(&mut websocket, "v=0\r\ns=ping-answer\r\n").await,
        "initial negotiation should complete",
    )?;

    let ping_payload = require_some(
        timeout(Duration::from_secs(1), read_websocket_ping(&mut websocket)).await?,
        "server should send a websocket ping promptly",
    )?;
    websocket
        .send(tungstenite::Message::Pong(ping_payload.into()))
        .await?;

    assert!(
        timeout(Duration::from_millis(80), read_close_code(&mut websocket))
            .await
            .is_err(),
        "user should remain open after answering ping"
    );
    require_some(
        close_socket_and_wait_for_session_cleanup(&mut websocket, &room, &user_id).await,
        "closing the socket should clean the session",
    )?;
    Ok(())
}

#[tokio::test]
async fn websocket_closes_when_pong_times_out() -> TestResult {
    let server = TestServerBuilder::new()
        .user_timeout_ms(30)
        .ping_interval_ms(15)
        .spawn_required()
        .await?;
    let room = require_some(
        create_room(&server, "issuer-ping-timeout", CreateRoomQuery::default()).await,
        "test room should be served",
    )?;
    let user_id = UserId::Integer(411);
    let token = room_token(&room, user_id.clone())?;
    let mut websocket = require_some(
        authenticate_silent_with_jwt(&server, &token).await,
        "silent user should authenticate",
    )?;
    require_some(
        read_silent_welcome(&mut websocket).await,
        "welcome should arrive",
    )?;
    require_some(
        wait_for_silent_protocol_server_request(&mut websocket).await,
        "initial offer should arrive",
    )?;

    sleep(Duration::from_millis(80)).await;

    assert_eq!(
        timeout(
            Duration::from_secs(1),
            read_silent_close_code(&mut websocket)
        )
        .await?,
        Some(CloseCode::Error)
    );

    require_some(
        wait_for_session_cleanup(&room, &user_id).await,
        "pong timeout should clean the session",
    )?;
    let metrics = server.state.metrics.snapshot();
    assert_eq!(metrics.ws_user_loop_exits_ping_timeout(), 1);
    Ok(())
}

#[tokio::test]
async fn shutdown_waits_for_in_flight_mutations_and_retires_sessions_once() -> TestResult {
    let server = TestServerBuilder::new()
        .room_size(1)
        .spawn_required()
        .await?;
    let room = require_some(
        create_room(&server, "issuer-shutdown", CreateRoomQuery::default()).await,
        "test room should be served",
    )?;
    let admitted_id = UserId::Integer(414);
    let admitted_token = room_token(&room, admitted_id.clone())?;
    let (mut admitted, _welcome) = require_some(
        authenticate_and_read_welcome(&server, &admitted_token).await,
        "admitted user should join",
    )?;
    let (request_id, request) = require_some(
        wait_for_protocol_server_request(&mut admitted).await,
        "admitted user should receive an offer",
    )?;
    let mut pending = require_some(
        connect_websocket(&server).await,
        "pending websocket should connect",
    )?;

    let gate = Arc::new(JoinPlacementTestGate::new(1));
    server
        .room_manager
        .set_join_placement_gate_for_test(Arc::clone(&gate));
    let joining_id = UserId::Integer(416);
    let joining_token = room_token(&room, joining_id.clone())?;
    let mut joining = require_some(
        authenticate_with_jwt(&server, &joining_token).await,
        "joining user should authenticate",
    )?;
    timeout(Duration::from_secs(1), gate.hold_all_ready()).await?;

    let worker_release = require_some(
        server.media_transport.test_api().pause_first_worker().await,
        "transport worker should pause",
    )?;
    require_some(
        respond_to_protocol_negotiation_request_with_test_rtc(
            &mut admitted,
            request_id,
            request,
            "v=0\r\ns=shutdown-answer\r\n",
        )
        .await,
        "first user should answer the offer",
    )?;
    require_some(
        wait_until(|| {
            server
                .media_transport
                .worker_pressure_snapshots()
                .first()
                .is_some_and(|worker| worker.command_backlog_depth > 0)
        })
        .await,
        "answer should reach the transport worker",
    )?;

    server.state.session_tasks.close();
    let mut session_drain = Box::pin(server.state.session_tasks.wait());
    server.state.session_shutdown.cancel();
    assert_eq!(
        read_close_code_promptly(&mut pending).await,
        Some(CloseCode::Away)
    );
    for websocket in [&mut admitted, &mut joining] {
        assert!(
            timeout(Duration::from_millis(30), read_close_code(websocket))
                .await
                .is_err()
        );
    }
    assert!(
        timeout(Duration::from_millis(30), &mut session_drain)
            .await
            .is_err()
    );
    gate.release_all().await;
    assert_eq!(
        read_close_code_promptly(&mut joining).await,
        Some(CloseCode::Away)
    );
    worker_release.send(())?;
    assert_eq!(
        read_close_code_promptly(&mut admitted).await,
        Some(CloseCode::Away)
    );
    timeout(Duration::from_secs(1), session_drain).await?;
    assert!(!room.test_api().has_session(&admitted_id).await);
    assert!(!room.test_api().has_session(&joining_id).await);
    let metrics = server.state.metrics.snapshot();
    assert_eq!(metrics.active_transport_users(), 0);
    assert_eq!(metrics.ws_user_loop_exits_runtime_shutdown(), 1);
    assert_eq!(metrics.ws_handshake_rejected_timeout(), 0);
    assert_eq!(metrics.ws_handshake_rejected_authentication_failed(), 0);
    assert_eq!(metrics.ws_handshake_rejected_protocol_error(), 0);
    assert_eq!(metrics.ws_handshake_rejected_room_full(), 0);
    assert_eq!(metrics.ws_handshake_rejected_error(), 0);
    Ok(())
}

#[tokio::test]
async fn websocket_closes_when_rtc_transport_disconnects() -> TestResult {
    let server = TestServerBuilder::new()
        .user_timeout_ms(200)
        .ping_interval_ms(20)
        .spawn_required()
        .await?;
    let room = require_some(
        create_room(&server, "issuer-rtc-disconnect", CreateRoomQuery::default()).await,
        "test room should be served",
    )?;
    let user_id = UserId::Integer(412);
    let mut websocket = require_some(
        setup_negotiated_session(&server, &room, user_id.clone()).await,
        "negotiated user should start",
    )?;
    assert_transport_disconnect(&server, &room, &user_id, &mut websocket).await
}

#[tokio::test]
async fn websocket_closes_when_rtc_transport_disconnects_during_initial_negotiation() -> TestResult
{
    let server = TestServerBuilder::new()
        .user_timeout_ms(200)
        .ping_interval_ms(20)
        .spawn_required()
        .await?;
    let room = require_some(
        create_room(
            &server,
            "issuer-rtc-disconnect-negotiating",
            CreateRoomQuery::default(),
        )
        .await,
        "test room should be served",
    )?;
    let user_id = UserId::Integer(413);
    let token = room_token(&room, user_id.clone())?;
    let mut websocket = require_some(
        authenticate_with_jwt(&server, &token).await,
        "negotiating user should authenticate",
    )?;
    require_some(read_welcome(&mut websocket).await, "welcome should arrive")?;
    require_some(
        read_protocol_server_batch(&mut websocket).await,
        "initial offer should arrive",
    )?;
    assert_transport_disconnect(&server, &room, &user_id, &mut websocket).await
}

#[tokio::test]
async fn stale_replaced_socket_close_cleans_only_the_stale_transport_user() -> TestResult {
    let server = TestServerBuilder::new().spawn_required().await?;
    let room = require_some(
        create_room(&server, "issuer-a", CreateRoomQuery::default()).await,
        "test room should be served",
    )?;
    let user_id = UserId::Integer(260);
    let mut first_socket = require_some(
        setup_negotiated_session(&server, &room, user_id.clone()).await,
        "first user should negotiate",
    )?;
    assert!(
        wait_for_active_transport_users(&server, 1).await.is_some(),
        "first negotiated socket should create one transport user"
    );
    let mut second_socket = require_some(
        setup_negotiated_session(&server, &room, user_id.clone()).await,
        "replacement user should negotiate",
    )?;

    assert_eq!(
        read_close_code(&mut first_socket).await,
        Some(CloseCode::Library(4108))
    );
    assert!(
        wait_for_active_transport_users(&server, 1).await.is_some(),
        "stale socket close should clean only the replaced transport user"
    );
    assert!(
        room.test_api().has_session(&user_id).await,
        "replacement session should stay live after stale socket cleanup"
    );

    second_socket.close(None).await?;
    assert!(
        wait_for_session_cleanup(&room, &user_id).await.is_some(),
        "closing the replacement socket should remove the final session"
    );
    assert!(
        wait_for_active_transport_users(&server, 0).await.is_some(),
        "closing the replacement socket should clean the final transport user"
    );
    Ok(())
}

fn room_token(room: &Room, user_id: UserId) -> TestResult<String> {
    require_some(
        signed_connect_claims(TEST_ROOM_KEY, room.uuid(), user_id),
        "connect token should sign",
    )
}

async fn assert_transport_disconnect(
    server: &TestServer,
    room: &Room,
    user_id: &UserId,
    websocket: &mut TestWebSocket,
) -> TestResult {
    let connection_id = require_some(
        room.test_api().user_connection_id(user_id).await,
        "user connection should exist",
    )?;
    let session_key = room.transport_user_key(user_id, connection_id).await;
    server
        .media_transport
        .test_api()
        .set_session_transport_health(&session_key, TransportSessionHealth::Disconnected);
    assert_eq!(
        timeout(Duration::from_secs(1), read_close_code(websocket)).await?,
        Some(CloseCode::Error)
    );
    require_some(
        wait_for_session_cleanup(room, user_id).await,
        "transport disconnect should clean the session",
    )?;
    require_some(
        wait_for_active_transport_users(server, 0).await,
        "transport disconnect should clean the transport user",
    )
}

async fn wait_for_active_transport_users(server: &TestServer, expected: i64) -> Option<()> {
    wait_until(|| server.state.metrics.snapshot().active_transport_users() == expected).await
}

async fn wait_until(mut condition: impl FnMut() -> bool) -> Option<()> {
    timeout(Duration::from_secs(1), async {
        while !condition() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .ok()
}
