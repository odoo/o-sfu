use std::iter::repeat_n;

use o_sfu_protocol::wire::WebSocketCloseCode;
use tokio::net::TcpSocket;
use tungstenite::http::StatusCode;

use super::fixtures::*;
use crate::runtime::auth::{MAX_JWT_TOKEN_BYTES, duration_since_epoch};

// deliberately creates a startup snapshot large enough to exercise outbound backpressure
const SLOW_READER_PEER_COUNT: usize = 48;
const SLOW_READER_USER_ID_BYTES: usize = 512 * 1024;
const SLOW_READER_RECV_BUFFER_BYTES: u32 = 1024;

#[tokio::test]
async fn websocket_rejects_pre_auth_connections_over_configured_capacity() {
    let server = TestServerBuilder::new()
        .pre_auth_capacity(1, 2)
        .spawn()
        .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let first = connect_websocket(&server).await;
    assert!(first.is_some());
    let Some(mut first) = first else {
        return;
    };

    let rejected = timeout(Duration::from_millis(200), connect_async(server.url())).await;
    assert!(
        rejected.is_ok(),
        "pre-auth cap rejection should complete promptly: {rejected:?}"
    );
    let Some(result) = rejected.ok() else {
        return;
    };
    match result {
        Err(tungstenite::Error::Http(response)) => {
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        }
        other => panic!("expected websocket HTTP rejection, got {other:?}"),
    }

    assert!(first.close(None).await.is_ok());
}

#[tokio::test]
async fn websocket_rejects_pre_auth_connections_over_origin_capacity() {
    let server = TestServerBuilder::new()
        .pre_auth_capacity(2, 1)
        .spawn()
        .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let first = connect_websocket(&server).await;
    assert!(first.is_some());
    let Some(mut first) = first else {
        return;
    };

    let rejected = timeout(Duration::from_millis(200), connect_async(server.url())).await;
    assert!(
        rejected.is_ok(),
        "per-origin pre-auth cap rejection should complete promptly: {rejected:?}"
    );
    let Some(result) = rejected.ok() else {
        return;
    };
    match result {
        Err(tungstenite::Error::Http(response)) => {
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        }
        other => panic!("expected websocket HTTP rejection, got {other:?}"),
    }

    assert!(first.close(None).await.is_ok());
}

#[tokio::test]
async fn websocket_pre_auth_origin_cap_allows_distinct_trusted_origins() {
    let server = TestServerBuilder::new()
        .pre_auth_capacity(2, 1)
        .trust_proxy_headers(true)
        .spawn()
        .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let first = connect_websocket_with_forwarded_for(&server, "198.51.100.24").await;
    assert!(first.is_some());
    let Some(mut first) = first else {
        return;
    };
    let second = connect_websocket_with_forwarded_for(&server, "203.0.113.8").await;
    assert!(
        second.is_some(),
        "distinct trusted origins should each receive one pre-auth slot"
    );
    let Some(mut second) = second else {
        return;
    };

    assert!(first.close(None).await.is_ok());
    assert!(second.close(None).await.is_ok());
}

#[tokio::test]
async fn websocket_times_out_when_client_never_authenticates() {
    let server = TestServerBuilder::new()
        .authentication_timeout_ms(25)
        .spawn()
        .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let websocket = connect_websocket(&server).await;
    assert!(websocket.is_some());
    let Some(mut websocket) = websocket else {
        return;
    };

    assert_eq!(
        read_close_code_promptly(&mut websocket).await,
        Some(CloseCode::Library(u16::from(
            WebSocketCloseCode::AuthTimeout
        )))
    );
    let metrics = server.state.metrics.snapshot();
    assert_eq!(metrics.ws_connections_accepted(), 1);
    assert_eq!(metrics.ws_handshake_rejected_timeout(), 1);
}

#[tokio::test]
async fn websocket_pre_auth_permit_is_released_after_auth_timeout() {
    let server = TestServerBuilder::new()
        .authentication_timeout_ms(25)
        .pre_auth_capacity(1, 1)
        .spawn()
        .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let first = connect_websocket(&server).await;
    assert!(first.is_some());
    let Some(mut first) = first else {
        return;
    };

    assert_eq!(
        read_close_code(&mut first).await,
        Some(CloseCode::Library(4107)),
    );
    sleep(Duration::from_millis(20)).await;

    let second = connect_websocket(&server).await;
    assert!(
        second.is_some(),
        "pre-auth permit should be reusable after auth timeout"
    );
}

#[tokio::test]
async fn websocket_pre_auth_permit_is_released_after_auth_failure() {
    let server = TestServerBuilder::new()
        .pre_auth_capacity(1, 1)
        .spawn()
        .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let first = connect_websocket(&server).await;
    assert!(first.is_some());
    let Some(mut first) = first else {
        return;
    };

    let send_result = first
        .send(tungstenite::Message::Text(
            serde_json::to_string(&vec![serde_json::json!({
                "t": "info",
                "p": {},
            })])
            .unwrap_or_default()
            .into(),
        ))
        .await;
    assert!(send_result.is_ok());
    assert_eq!(read_close_code(&mut first).await, Some(CloseCode::Protocol));
    sleep(Duration::from_millis(20)).await;

    let second = connect_websocket(&server).await;
    assert!(
        second.is_some(),
        "pre-auth permit should be reusable after auth failure"
    );
}

#[tokio::test]
async fn websocket_pre_auth_permit_is_released_after_early_client_close() {
    let server = TestServerBuilder::new()
        .pre_auth_capacity(1, 1)
        .spawn()
        .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let first = connect_websocket(&server).await;
    assert!(first.is_some());
    let Some(mut first) = first else {
        return;
    };

    assert!(first.close(None).await.is_ok());
    sleep(Duration::from_millis(20)).await;

    let second = connect_websocket(&server).await;
    assert!(
        second.is_some(),
        "pre-auth permit should be reusable after early client close"
    );
}

#[tokio::test]
async fn websocket_closes_authenticated_user_when_room_is_full() -> TestResult {
    let server = TestServerBuilder::new()
        .room_size(1)
        .spawn_required()
        .await?;
    let room = require_some(
        create_room(&server, "issuer-full-room", CreateRoomQuery::default()).await,
        "test room should be served",
    )?;
    let alice_id = UserId::Integer(1);
    let bob_id = UserId::Integer(2);
    let alice_token = require_some(
        signed_connect_claims(TEST_ROOM_KEY, room.uuid(), alice_id.clone()),
        "alice connect JWT should sign",
    )?;
    let bob_token = require_some(
        signed_connect_claims(TEST_ROOM_KEY, room.uuid(), bob_id.clone()),
        "bob connect JWT should sign",
    )?;
    let wrong_key_token = require_some(
        signed_connect_claims(OTHER_ROOM_KEY, room.uuid(), bob_id.clone()),
        "wrong-key connect JWT should sign",
    )?;
    let (mut alice, _welcome) = require_some(
        authenticate_and_read_welcome(&server, &alice_token).await,
        "alice should authenticate",
    )?;
    let mut wrong_key = require_some(
        authenticate_with_room(&server, &wrong_key_token, Some(room.uuid())).await,
        "wrong-key websocket should connect before auth rejection",
    )?;

    assert_eq!(
        read_close_code(&mut wrong_key).await,
        Some(CloseCode::Library(u16::from(
            WebSocketCloseCode::AuthFailed
        )))
    );
    assert!(
        !server
            .room_manager
            .test_api()
            .has_session(room.uuid(), &bob_id)
            .await
    );
    let metrics = server.state.metrics.snapshot();
    assert_eq!(metrics.ws_handshake_credentials_received(), 2);
    assert_eq!(metrics.ws_users_joined(), 1);
    assert_eq!(metrics.ws_handshake_rejected_authentication_failed(), 1);
    assert_eq!(metrics.ws_handshake_rejected_room_full(), 0);

    let mut bob = require_some(
        authenticate_with_jwt(&server, &bob_token).await,
        "bob should connect before room-full rejection",
    )?;

    assert_eq!(
        read_close_code_promptly(&mut bob).await,
        Some(CloseCode::Library(u16::from(WebSocketCloseCode::RoomFull)))
    );

    assert!(
        server
            .room_manager
            .test_api()
            .has_session(room.uuid(), &alice_id)
            .await
    );
    assert!(
        !server
            .room_manager
            .test_api()
            .has_session(room.uuid(), &bob_id)
            .await
    );
    let metrics = server.state.metrics.snapshot();
    assert_eq!(metrics.ws_handshake_credentials_received(), 3);
    assert_eq!(metrics.ws_users_joined(), 1);
    assert_eq!(metrics.ws_handshake_rejected_authentication_failed(), 1);
    assert_eq!(metrics.ws_handshake_rejected_room_full(), 1);
    assert!(alice.close(None).await.is_ok());
    Ok(())
}

#[tokio::test]
async fn websocket_pre_auth_permit_is_released_after_auth_success() {
    let server = TestServerBuilder::new()
        .pre_auth_capacity(1, 1)
        .spawn()
        .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(&server, "issuer-a", CreateRoomQuery::default()).await;
    assert!(room.is_some(), "test room should be served");
    let Some(room) = room else {
        return;
    };
    let token = signed_connect_claims(TEST_ROOM_KEY, room.uuid(), UserId::Integer(77));
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let authenticated = authenticate_and_read_welcome(&server, &token).await;
    assert!(authenticated.is_some());
    let Some((mut first, _welcome)) = authenticated else {
        return;
    };

    let second = connect_websocket(&server).await;
    assert!(
        second.is_some(),
        "pre-auth permit should be reusable after auth succeeds"
    );
    assert!(first.close(None).await.is_ok());
}

#[tokio::test]
async fn websocket_startup_send_timeout_releases_room_membership() {
    let server = TestServerBuilder::new()
        .room_size(SLOW_READER_PEER_COUNT + 1)
        .spawn()
        .await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(
        &server,
        "issuer-startup-send-timeout",
        CreateRoomQuery::default(),
    )
    .await;
    assert!(room.is_some(), "test room should be served");
    let Some(room) = room else {
        return;
    };
    let mut peer_receivers = Vec::with_capacity(SLOW_READER_PEER_COUNT);
    for index in 0..SLOW_READER_PEER_COUNT {
        let joined = join_large_snapshot_peer(&server, &room, index, &mut peer_receivers).await;
        assert!(joined.is_some(), "large startup snapshot peer should join");
    }
    assert_eq!(
        server.state.room_manager.room_gauges().await.users,
        SLOW_READER_PEER_COUNT,
    );
    let token = signed_connect_claims(TEST_ROOM_KEY, room.uuid(), UserId::Integer(999));
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let auth_payload = encode_protocol_auth(AuthPayload {
        jwt: secrecy::SecretString::from(token),
        channel: Some(room.uuid().to_owned()),
    });
    assert!(auth_payload.is_some());
    let Some(auth_payload) = auth_payload else {
        return;
    };
    let slow_websocket = connect_slow_reader(&server).await;
    assert!(slow_websocket.is_some());
    let Some(mut slow_websocket) = slow_websocket else {
        return;
    };
    let sent = slow_websocket
        .send(tungstenite::Message::Text(auth_payload.into()))
        .await;
    assert!(sent.is_ok());

    let cleaned = timeout(Duration::from_secs(8), async {
        let mut saw_slow_user_join = false;
        loop {
            let metrics = server.state.metrics.snapshot();
            saw_slow_user_join |= metrics.ws_users_joined() == 1;
            if saw_slow_user_join
                && server.state.room_manager.room_gauges().await.users == SLOW_READER_PEER_COUNT
            {
                return true;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert_eq!(
        cleaned.ok(),
        Some(true),
        "startup send timeout should clean the admitted user"
    );
    let metrics = server.state.metrics.snapshot();
    assert_eq!(metrics.ws_user_loops_started(), 0);
}

#[tokio::test]
async fn websocket_authenticates_odoo_internal_session_identity() -> TestResult {
    let server = TestServerBuilder::new().spawn_required().await?;
    let room = require_some(
        create_room(&server, "odoo-internal", CreateRoomQuery::default()).await,
        "test room should be served",
    )?;
    let claims = serde_json::json!({
        "sfu_channel_uuid": room.uuid(),
        "session_id": 170,
        "user_id": 42,
        "label": "Alice",
        "ice_servers": [],
        "permissions": { "audioRecording": true },
        "exp": duration_since_epoch().as_secs() + 8 * 60 * 60,
    });
    let token = sign(&claims, &secrecy::SecretString::from(TEST_ROOM_KEY))?;
    let mut websocket = require_some(
        authenticate_with_jwt(&server, secrecy::ExposeSecret::expose_secret(&token)).await,
        "internal-user websocket should connect",
    )?;
    require_some(
        read_welcome(&mut websocket).await,
        "Odoo internal user should authenticate",
    )?;
    assert!(
        server
            .room_manager
            .test_api()
            .has_session(room.uuid(), &UserId::Integer(170))
            .await
    );
    assert!(
        !server
            .room_manager
            .test_api()
            .has_session(room.uuid(), &UserId::Integer(42))
            .await
    );
    Ok(())
}

#[tokio::test]
async fn websocket_authenticates_odoo_guest_session_identity() -> TestResult {
    let server = TestServerBuilder::new().spawn_required().await?;
    let room = require_some(
        create_room(&server, "odoo-guest", CreateRoomQuery::default()).await,
        "test room should be served",
    )?;
    let token = sign(
        &serde_json::json!({
            "sfu_channel_uuid": room.uuid(),
            "session_id": "171",
            "label": "Guest",
            "ice_servers": [],
            "permissions": {},
            "exp": duration_since_epoch().as_secs() + 8 * 60 * 60,
        }),
        &secrecy::SecretString::from(TEST_ROOM_KEY),
    )?;
    let mut websocket = require_some(
        authenticate_with_jwt(&server, secrecy::ExposeSecret::expose_secret(&token)).await,
        "guest websocket should connect",
    )?;
    require_some(
        read_welcome(&mut websocket).await,
        "Odoo guest should authenticate",
    )?;
    assert!(
        server
            .room_manager
            .test_api()
            .has_session(room.uuid(), &UserId::Integer(171))
            .await
    );
    Ok(())
}

#[tokio::test]
async fn websocket_rejects_missing_or_malformed_participant_identity() -> TestResult {
    let server = TestServerBuilder::new().spawn_required().await?;
    let room = require_some(
        create_room(&server, "invalid-identity", CreateRoomQuery::default()).await,
        "test room should be served",
    )?;
    for mut claims in [
        serde_json::json!({}),
        serde_json::json!({ "session_id": [] }),
    ] {
        let object = require_some(
            claims.as_object_mut(),
            "credential claims should be an object",
        )?;
        object.insert("room_id".to_owned(), serde_json::json!(room.uuid()));
        object.insert(
            "exp".to_owned(),
            serde_json::json!(duration_since_epoch().as_secs() + 60),
        );
        let token = sign(&claims, &secrecy::SecretString::from(TEST_ROOM_KEY))?;
        let mut websocket = require_some(
            authenticate_with_room(
                &server,
                secrecy::ExposeSecret::expose_secret(&token),
                Some(room.uuid()),
            )
            .await,
            "invalid-identity websocket should connect",
        )?;
        assert_eq!(
            read_close_code_promptly(&mut websocket).await,
            Some(CloseCode::Library(u16::from(
                WebSocketCloseCode::AuthFailed
            )))
        );
    }
    assert_eq!(server.state.metrics.snapshot().ws_users_joined(), 0);
    Ok(())
}

#[tokio::test]
async fn websocket_authenticates_legacy_room_scoped_token_with_explicit_room_id() {
    let server = TestServerBuilder::new().spawn().await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(&server, "issuer-a", CreateRoomQuery::default()).await;
    assert!(room.is_some(), "test room should be served");
    let Some(room) = room else {
        return;
    };
    let token =
        signed_legacy_channel_scoped_connect_claims(TEST_ROOM_KEY, UserId::Integer(17), None);
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let authenticated = authenticate_with_room(&server, &token, Some(room.uuid())).await;
    assert!(authenticated.is_some());
    let Some(mut websocket) = authenticated else {
        return;
    };

    let welcome = read_welcome(&mut websocket).await;
    assert!(welcome.is_some(), "welcome payload should exist");
}

#[tokio::test]
async fn websocket_rejects_explicit_room_id_that_disagrees_with_claims() {
    let server = TestServerBuilder::new().spawn().await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let first_room = create_room(&server, "issuer-a", CreateRoomQuery::default()).await;
    let second_room = create_room(&server, "issuer-b", CreateRoomQuery::default()).await;
    assert!(
        first_room.is_some() && second_room.is_some(),
        "test rooms should be served"
    );
    let (Some(first_room), Some(second_room)) = (first_room, second_room) else {
        return;
    };
    let token = signed_connect_claims(TEST_ROOM_KEY, first_room.uuid(), UserId::Integer(8));
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let authenticated = authenticate_with_room(&server, &token, Some(second_room.uuid())).await;
    assert!(authenticated.is_some());
    let Some(mut websocket) = authenticated else {
        return;
    };

    assert_eq!(
        read_close_code(&mut websocket).await,
        Some(CloseCode::Library(4106)),
    );
}

#[tokio::test]
async fn websocket_rejects_explicit_room_token_signed_with_another_key() {
    let server = TestServerBuilder::new().spawn().await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(&server, "issuer-a", CreateRoomQuery::default()).await;
    assert!(room.is_some(), "test room should be served");
    let Some(room) = room else {
        return;
    };
    let token = signed_connect_claims(OTHER_ROOM_KEY, room.uuid(), UserId::Integer(19));
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };
    let authenticated = authenticate_with_room(&server, &token, Some(room.uuid())).await;
    assert!(authenticated.is_some());
    let Some(mut websocket) = authenticated else {
        return;
    };

    assert_eq!(
        read_close_code(&mut websocket).await,
        Some(CloseCode::Library(4106)),
    );
}

#[tokio::test]
async fn websocket_rejects_oversized_auth_token_with_auth_failure() {
    let server = TestServerBuilder::new().spawn().await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let room = create_room(&server, "issuer-a", CreateRoomQuery::default()).await;
    assert!(room.is_some(), "test room should be served");
    let Some(room) = room else {
        return;
    };
    let token = "a".repeat(MAX_JWT_TOKEN_BYTES + 1);
    let authenticated = authenticate_with_room(&server, &token, Some(room.uuid())).await;
    assert!(authenticated.is_some());
    let Some(mut websocket) = authenticated else {
        return;
    };

    assert_eq!(
        read_close_code(&mut websocket).await,
        Some(CloseCode::Library(4106)),
    );
}

#[tokio::test]
async fn websocket_rejects_non_auth_handshake_frame_with_protocol_metric() {
    let server = TestServerBuilder::new().spawn().await;
    assert!(server.is_some());
    let Some(server) = server else {
        return;
    };
    let websocket = connect_websocket(&server).await;
    assert!(websocket.is_some());
    let Some(mut websocket) = websocket else {
        return;
    };

    let send_result = websocket
        .send(tungstenite::Message::Text(
            serde_json::to_string(&vec![serde_json::json!({
                "t": "info",
                "p": {},
            })])
            .unwrap_or_default()
            .into(),
        ))
        .await;
    assert!(send_result.is_ok());

    assert_eq!(
        read_close_code(&mut websocket).await,
        Some(CloseCode::Protocol),
    );

    sleep(Duration::from_millis(20)).await;
    let metrics = server.state.metrics.snapshot();
    assert_eq!(metrics.ws_handshake_credentials_received(), 0);
    assert_eq!(metrics.ws_handshake_rejected_protocol_error(), 1);
    assert_eq!(metrics.ws_handshake_rejected_error(), 0);
}

async fn connect_slow_reader(
    server: &TestServer,
) -> Option<tokio_tungstenite::WebSocketStream<TcpStream>> {
    let socket = if server.addr.is_ipv4() {
        TcpSocket::new_v4().ok()?
    } else {
        TcpSocket::new_v6().ok()?
    };
    socket
        .set_recv_buffer_size(SLOW_READER_RECV_BUFFER_BYTES)
        .ok()?;
    let stream = socket.connect(server.addr).await.ok()?;
    let websocket = tokio_tungstenite::client_async(server.url(), stream)
        .await
        .ok()?;
    Some(websocket.0)
}

async fn join_large_snapshot_peer(
    server: &TestServer,
    room: &Arc<Room>,
    index: usize,
    peer_receivers: &mut Vec<UserOutboundReceiver>,
) -> Option<()> {
    let (sender, receiver) = UserOutboundSender::channel_with_limits(
        UserOutboundQueueLimits::new(1, 1024),
        Arc::clone(&server.state.metrics),
    );
    server
        .room_manager
        .join_user(
            room.uuid(),
            JoinUserRequest {
                user_id: large_user_id(index),
                label: None,
                permissions: UserPermissions::default(),
                sender,
            },
            &server.media_transport,
        )
        .await
        .ok()?;
    peer_receivers.push(receiver);
    Some(())
}

fn large_user_id(index: usize) -> UserId {
    let mut value = String::with_capacity(SLOW_READER_USER_ID_BYTES + 32);
    value.push_str("slow-reader-peer-");
    value.push_str(&index.to_string());
    value.push('-');
    value.extend(repeat_n('x', SLOW_READER_USER_ID_BYTES));
    UserId::String(value)
}
