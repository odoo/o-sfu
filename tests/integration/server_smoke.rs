#![allow(
    clippy::panic,
    reason = "integration tests use panic-based assertions for clear failures"
)]

use std::{collections::BTreeMap, future::Future, net::SocketAddr, pin::Pin};

use o_sfu::{config::Config, http::route};
use o_sfu_protocol::wire::{
    ClientBroadcastPayload, ClientMessage, DownloadStates, ServerMessage, ServerRequest,
    SubscribePayload, UserId,
};
use o_sfu_tests::support::{
    TEST_ROOM_KEY, TestResult, TestServer, connect_websocket, create_room,
    disconnect_sessions_via_http, metrics_text,
    protocol_harness::{ProtocolWebSocketClient, connect_protocol_pair, read_until_server_message},
    read_close_code, require_some, signed_connect_claims, spawn_test_server,
    spawn_test_server_with_listener, test_config,
};
use reqwest::StatusCode;
use str0m::change::SdpOffer;
use tokio::{
    net::TcpListener,
    time::{Duration, timeout},
};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

const SLOW_CONSUMER_BATCH_LEN: usize = 64;
const SLOW_CONSUMER_PAYLOAD_BYTES: usize = 1_024;
const OPERATOR_ROUTES: [&str; 3] = [
    route::v1::STATS,
    route::METRICS,
    route::diagnostics::SUMMARY,
];

async fn server_with_room(issuer: &str) -> TestResult<(TestServer, String)> {
    server_with_configured_room(test_config(1_000, 10), issuer).await
}

async fn server_with_configured_room(
    config: Config,
    issuer: &str,
) -> TestResult<(TestServer, String)> {
    let server = spawn_test_server(config).await?;
    let room = room(&server, issuer).await?;
    Ok((server, room))
}

async fn room(server: &TestServer, issuer: &str) -> TestResult<String> {
    require_some(
        create_room(server, issuer, TEST_ROOM_KEY).await,
        "room should be created",
    )
}

fn token(room: &str, user_id: UserId) -> TestResult<String> {
    require_some(
        signed_connect_claims(TEST_ROOM_KEY, room, user_id),
        "connect token should sign",
    )
}

async fn client_in_room(
    server: &TestServer,
    room: &str,
    user_id: UserId,
) -> TestResult<ProtocolWebSocketClient> {
    let token = token(room, user_id)?;
    require_some(
        ProtocolWebSocketClient::authenticate_with_room(server, &token, room).await,
        "websocket client should authenticate",
    )
}

type ProtocolPairFuture<'a> = Pin<
    Box<dyn Future<Output = TestResult<(ProtocolWebSocketClient, ProtocolWebSocketClient)>> + 'a>,
>;

fn protocol_pair<'a>(
    server: &'a TestServer,
    room: &'a str,
    first_user_id: UserId,
    second_user_id: UserId,
) -> ProtocolPairFuture<'a> {
    Box::pin(async move {
        let first_token = token(room, first_user_id)?;
        let second_token = token(room, second_user_id.clone())?;
        require_some(
            Box::pin(connect_protocol_pair(
                server,
                &first_token,
                &second_token,
                second_user_id,
            ))
            .await,
            "protocol pair should connect",
        )
    })
}

async fn initial_offer(client: &mut ProtocolWebSocketClient) -> TestResult<ServerRequest> {
    let welcome = require_some(client.read_welcome().await, "welcome should be sent")?;
    assert!(welcome.features.rtc);
    let (_request_id, request) = require_some(
        client.read_server_request().await,
        "initial offer should be sent",
    )?;
    Ok(request)
}

#[tokio::test]
async fn runtime_shutdown_drains_pending_and_admitted_websockets() -> TestResult {
    let (server, room) = server_with_room("issuer-runtime-shutdown").await?;
    let mut pending = require_some(
        connect_websocket(&server).await,
        "pending websocket should connect",
    )?;
    let mut admitted = client_in_room(&server, &room, UserId::Integer(700)).await?;
    assert!(matches!(
        initial_offer(&mut admitted).await?,
        ServerRequest::Offer(_)
    ));

    server.stop();
    let pending_close = timeout(Duration::from_secs(1), read_close_code(&mut pending)).await?;
    let admitted_close = timeout(Duration::from_secs(1), admitted.read_close_code()).await?;
    assert_eq!(pending_close, Some(CloseCode::Away));
    assert_eq!(admitted_close, Some(CloseCode::Away));
    assert!(connect_websocket(&server).await.is_none());
    server.join().await
}

#[tokio::test]
async fn operator_access_uses_listener_address() -> TestResult {
    let mut loopback_config = test_config(1_000, 10);
    loopback_config.http.bind_address = SocketAddr::from(([127, 0, 0, 1], 0));
    let wildcard_listener = TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], 0))).await?;
    let wildcard_server = spawn_test_server_with_listener(&loopback_config, wildcard_listener)?;

    for path in OPERATOR_ROUTES {
        let response = reqwest::get(format!("{}{path}", wildcard_server.http_base_url())).await?;
        assert_eq!(response.status(), StatusCode::FORBIDDEN, "{path}");
    }
    let path = route::v1::NOOP;
    let response = reqwest::get(format!("{}{path}", wildcard_server.http_base_url())).await?;
    assert_eq!(response.status(), StatusCode::OK, "{path}");
    wildcard_server.join().await?;

    let mut wildcard_config = test_config(1_000, 10);
    wildcard_config.http.bind_address = SocketAddr::from(([0, 0, 0, 0], 0));
    let loopback_listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
    let loopback_server = spawn_test_server_with_listener(&wildcard_config, loopback_listener)?;

    for path in OPERATOR_ROUTES {
        let response = reqwest::get(format!("{}{path}", loopback_server.http_base_url())).await?;
        assert_eq!(response.status(), StatusCode::OK, "{path}");
    }
    loopback_server.join().await
}

#[tokio::test]
async fn websocket_welcome_and_initial_offer_expose_real_rtc_transport_details() -> TestResult {
    let config = test_config(1_000, 10);
    let rtc_port_range = config.transport.rtc_port_range;
    let (server, room) = server_with_configured_room(config, "issuer-a").await?;
    let mut client = client_in_room(&server, &room, UserId::Integer(701)).await?;
    let request = initial_offer(&mut client).await?;
    let ServerRequest::Offer(payload) = request else {
        panic!("expected offer request");
    };
    assert!(payload.sdp.contains("a=ice-lite"));
    assert!(payload.sdp.contains("a=ice-options:trickle"));
    let parsed_offer = require_some(
        SdpOffer::from_sdp_string(&payload.sdp).ok(),
        "offer should contain parseable SDP",
    )?;
    assert!(parsed_offer.session.end_of_candidates());
    assert_eq!(payload.sdp.matches("a=end-of-candidates").count(), 1);
    assert!(payload.sdp.contains("a=fingerprint:sha-256"));
    assert!(payload.sdp.contains("a=candidate:"));
    assert!(payload.sdp.contains("127.0.0.1"));
    let candidate_line = require_some(
        payload
            .sdp
            .lines()
            .find(|line| line.starts_with("a=candidate:")),
        "expected SDP candidate line",
    )?;
    assert!(candidate_line.contains(" 127.0.0.1 "));
    let port = require_some(
        candidate_line
            .split_whitespace()
            .nth(5)
            .and_then(|value| value.parse::<u16>().ok()),
        "candidate should expose an RTC port",
    )?;
    assert!(rtc_port_range.ports().any(|candidate| candidate == port));
    Ok(())
}

#[tokio::test]
async fn websocket_candidate_free_answer_establishes_rtc_transport() -> TestResult {
    let (server, room) = server_with_room("issuer-candidate-free-answer").await?;
    let mut client = client_in_room(&server, &room, UserId::Integer(703)).await?;

    require_some(client.read_welcome().await, "welcome should be sent")?;
    require_some(
        client.finish_initial_negotiation_without_candidates().await,
        "candidate-free answer should be accepted",
    )?;
    require_some(
        client.wait_until_connected(Duration::from_secs(5)).await,
        "candidate-free answer should establish ICE and DTLS",
    )?;
    Ok(())
}

#[tokio::test]
async fn websocket_offer_advertises_configured_public_ip_in_rtc_mode() -> TestResult {
    let mut config = test_config(1_000, 10);
    config.transport.announced_ip = "203.0.113.44"
        .parse()
        .unwrap_or(config.transport.announced_ip);
    let (server, room) = server_with_configured_room(config, "issuer-public-ip").await?;
    let mut client = client_in_room(&server, &room, UserId::Integer(702)).await?;
    let request = initial_offer(&mut client).await?;
    let ServerRequest::Offer(payload) = request else {
        panic!("expected offer request");
    };
    assert!(payload.sdp.contains("a=ice-lite"));
    assert!(payload.sdp.contains("203.0.113.44"));
    if let Some(candidate_line) = payload
        .sdp
        .lines()
        .find(|line| line.starts_with("a=candidate:"))
    {
        assert!(candidate_line.contains(" 203.0.113.44 "));
    } else {
        panic!("expected SDP candidate line");
    }
    Ok(())
}

#[tokio::test]
async fn websocket_slow_consumer_overflow_allows_reconnection() -> TestResult {
    let (server, room) = server_with_configured_room(
        slow_consumer_overflow_config(),
        "issuer-slow-consumer-overflow",
    )
    .await?;
    let (mut slow, mut driver) =
        protocol_pair(&server, &room, UserId::Integer(31), UserId::Integer(32)).await?;
    let reconnect_token = token(&room, UserId::Integer(31))?;

    require_some(
        driver.send_messages(slow_consumer_broadcast_batch()).await,
        "slow-consumer batch should send",
    )?;

    let slow_close = timeout(Duration::from_secs(5), slow.read_close_code())
        .await
        .ok()
        .flatten();
    assert_eq!(slow_close, Some(CloseCode::Library(4110)));

    let departed = read_until_server_message(&mut driver, Duration::from_secs(1), |message| {
        matches!(message, ServerMessage::PeerLeft(payload) if payload.user_id == UserId::Integer(31))
    })
    .await;
    require_some(departed, "driver should observe slow peer departure")?;

    let reconnected =
        ProtocolWebSocketClient::authenticate_and_negotiate(&server, &reconnect_token).await;
    let (reconnected, _welcome) = require_some(reconnected, "slow peer should reconnect")?;
    let rejoined = read_until_server_message(&mut driver, Duration::from_secs(1), |message| {
        matches!(message, ServerMessage::PeerJoined(payload) if payload.user_id == UserId::Integer(31))
    })
    .await;
    require_some(rejoined, "driver should observe slow peer rejoin")?;
    require_some(reconnected.close().await, "reconnected peer should close")?;

    let metrics = metrics_text(&server).await;
    let metrics = require_some(metrics, "metrics text should be exposed")?;
    assert!(metric_value(&metrics, "osfu_ws_outbound_queue_overflows_total").unwrap_or(0) > 0);
    assert_eq!(
        metric_value(
            &metrics,
            "osfu_ws_user_loop_exits_total{reason=\"outbound_queue_overflow\"}"
        ),
        Some(1)
    );
    Ok(())
}

#[tokio::test]
async fn numeric_string_user_ids_share_one_runtime_identity() -> TestResult {
    let (server, room) = server_with_room("issuer-runtime-user-id-normalization").await?;
    let string_token = token(&room, UserId::String("42".to_owned()))?;
    let (mut observer, mut numeric_user) =
        protocol_pair(&server, &room, UserId::Integer(7), UserId::Integer(42)).await?;

    let (mut replacement, _welcome) = require_some(
        ProtocolWebSocketClient::authenticate_and_negotiate(&server, &string_token).await,
        "string-id replacement should negotiate",
    )?;

    assert_peer_left(&mut observer, UserId::Integer(42)).await?;
    assert_peer_joined(&mut observer, UserId::Integer(42)).await?;
    assert_eq!(
        numeric_user.read_close_code().await,
        Some(CloseCode::Library(4108))
    );

    require_some(
        observer
            .send_message(ClientMessage::Subscribe(SubscribePayload {
                user_id: UserId::String("42".to_owned()),
                states: DownloadStates {
                    audio: Some(true),
                    ..DownloadStates::default()
                },
            }))
            .await,
        "observer should subscribe using string user id",
    )?;

    assert_diagnostics_user(&server, &room, 42).await?;

    let status = disconnect_sessions_via_http(
        &server,
        BTreeMap::from([(room.clone(), vec![UserId::String("42".to_owned())])]),
    )
    .await;
    assert_eq!(status, Some(StatusCode::OK));
    assert_eq!(
        replacement.read_close_code().await,
        Some(CloseCode::Library(4108))
    );
    assert_peer_left(&mut observer, UserId::Integer(42)).await?;
    require_some(observer.close().await, "observer should close")?;
    assert!(server.wait_for_room_absence(&room).await);
    Ok(())
}

async fn assert_peer_left(client: &mut ProtocolWebSocketClient, user_id: UserId) -> TestResult {
    let message = read_until_server_message(
        client,
        Duration::from_secs(1),
        |message| matches!(message, ServerMessage::PeerLeft(payload) if payload.user_id == user_id),
    )
    .await;
    require_some(message, "peer departure should be delivered")?;
    Ok(())
}

async fn assert_peer_joined(client: &mut ProtocolWebSocketClient, user_id: UserId) -> TestResult {
    let message = read_until_server_message(client, Duration::from_secs(1), |message| {
        matches!(message, ServerMessage::PeerJoined(payload) if payload.user_id == user_id)
    })
    .await;
    require_some(message, "peer join should be delivered")?;
    Ok(())
}

async fn assert_diagnostics_user(server: &TestServer, room_id: &str, user_id: i64) -> TestResult {
    let response = reqwest::Client::new()
        .get(format!(
            "{}/internal/diagnostics/rooms/{room_id}/users/{user_id}",
            server.http_base_url(),
        ))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload = response.json::<serde_json::Value>().await?;
    assert_eq!(
        payload.get("roomId").and_then(serde_json::Value::as_str),
        Some(room_id)
    );
    assert_eq!(
        payload
            .get("user")
            .and_then(|user| user.get("userId"))
            .and_then(serde_json::Value::as_i64),
        Some(user_id)
    );
    Ok(())
}

#[tokio::test]
async fn bulk_disconnected_socket_cannot_broadcast_after_logical_removal() -> TestResult {
    let (server, room) = server_with_room("issuer-disconnect-guard").await?;
    let (mut alice, mut bob) =
        protocol_pair(&server, &room, UserId::Integer(21), UserId::Integer(22)).await?;

    let status = disconnect_sessions_via_http(
        &server,
        BTreeMap::from([(room.clone(), vec![UserId::Integer(22)])]),
    )
    .await;
    assert_eq!(status, Some(StatusCode::OK));

    let _ = bob
        .send_broadcast(serde_json::json!({ "text": "post-kick" }))
        .await;
    let message = alice
        .read_server_message_with_timeout(Duration::from_secs(1))
        .await;
    let message = require_some(message, "alice should receive peer departure")?;
    if let ServerMessage::PeerLeft(payload) = message {
        assert_eq!(payload.user_id, UserId::Integer(22));
    } else {
        panic!("expected bulk disconnect to surface peerleft before any stale broadcast");
    }
    assert_eq!(
        alice
            .read_server_message_with_timeout(Duration::from_millis(150))
            .await,
        None,
        "bulk-disconnected sockets must not squeeze extra broadcast traffic through after removal"
    );
    assert_eq!(bob.read_close_code().await, Some(CloseCode::Library(4108)));
    require_some(alice.close().await, "alice should close")?;
    assert!(server.wait_for_room_absence(&room).await);
    Ok(())
}

fn slow_consumer_overflow_config() -> Config {
    let mut config = test_config(1_000, 10);
    config.user.outbound_queue_capacity = 1;
    config.user.outbound_queue_byte_capacity = 64 * 1024;
    config
}

fn slow_consumer_broadcast_batch() -> Vec<ClientMessage> {
    let payload = "x".repeat(SLOW_CONSUMER_PAYLOAD_BYTES);
    (0..SLOW_CONSUMER_BATCH_LEN)
        .map(|index| {
            ClientMessage::Broadcast(ClientBroadcastPayload {
                message: serde_json::json!({
                    "index": index,
                    "payload": payload.clone(),
                }),
            })
        })
        .collect()
}

fn metric_value(metrics_text: &str, sample_name: &str) -> Option<u64> {
    metrics_text
        .lines()
        .find_map(|line| line.strip_prefix(sample_name)?.trim().parse().ok())
}
