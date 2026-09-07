use std::fmt::Debug;

use serde_json::Value;

use super::fixtures::*;
use crate::runtime::websocket_server::io::{MAX_CLIENT_BATCH_ENVELOPES, MAX_CLIENT_FRAME_BYTES};

fn require_some<T>(value: Option<T>, context: &str) -> T {
    value.unwrap_or_else(|| panic!("{context}"))
}

fn require_ok<T, E: Debug>(value: Result<T, E>, context: &str) -> T {
    match value {
        Ok(value) => value,
        Err(error) => panic!("{context}: {error:?}"),
    }
}

fn encode_client_frame(envelopes: &[Value]) -> String {
    require_ok(
        serde_json::to_string(envelopes),
        "test client frame should serialize",
    )
}

async fn send_text_frame(websocket: &mut TestWebSocket, frame: String, context: &str) {
    let send_result = websocket
        .send(tungstenite::Message::Text(frame.into()))
        .await;
    assert!(send_result.is_ok(), "{context}: {send_result:?}");
}

async fn authenticated_protocol_websocket(
    issuer: &str,
    user_id: UserId,
) -> (TestServer, Arc<Room>, TestWebSocket) {
    let server = require_some(
        TestServerBuilder::new().spawn().await,
        "test websocket server should start",
    );
    let room = require_some(
        create_room(&server, issuer, CreateRoomQuery::default()).await,
        "test room should be served",
    );
    let token = require_some(
        signed_connect_claims(TEST_ROOM_KEY, room.uuid(), user_id),
        "connect JWT should sign",
    );
    let mut websocket = require_some(
        authenticate_with_jwt(&server, &token).await,
        "websocket should authenticate",
    );
    assert!(
        read_welcome(&mut websocket).await.is_some(),
        "authenticated websocket should receive welcome"
    );
    (server, room, websocket)
}

#[tokio::test]
async fn websocket_rejects_unknown_protocol_envelope_tag() {
    let (server, _room, mut websocket) =
        authenticated_protocol_websocket("issuer-a", UserId::Integer(12)).await;
    let invalid_request = encode_client_frame(&[serde_json::json!({
        "t": "not-a-real-message",
        "p": {},
    })]);
    send_text_frame(
        &mut websocket,
        invalid_request,
        "invalid protocol envelope should still send",
    )
    .await;

    assert_eq!(
        read_close_code(&mut websocket).await,
        Some(CloseCode::Protocol),
    );

    let metrics = server.state.metrics.snapshot();
    assert_eq!(metrics.ws_bus_parse_failures(), 1);
    assert_eq!(metrics.ws_bus_invalid_input_failures(), 0);
    assert_eq!(metrics.ws_bus_unsupported_feature_failures(), 1);
}

#[tokio::test]
async fn websocket_rejects_invalid_json_payload() {
    let (server, _room, mut websocket) =
        authenticated_protocol_websocket("issuer-invalid-json", UserId::Integer(14)).await;
    send_text_frame(
        &mut websocket,
        "not-json".to_owned(),
        "invalid payload should still send",
    )
    .await;

    assert_eq!(
        read_close_code(&mut websocket).await,
        Some(CloseCode::Protocol),
    );

    let metrics = server.state.metrics.snapshot();
    assert_eq!(metrics.ws_bus_parse_failures(), 1);
    assert_eq!(metrics.ws_bus_invalid_input_failures(), 1);
    assert_eq!(metrics.ws_bus_unsupported_feature_failures(), 0);
}

#[tokio::test]
async fn websocket_rejects_oversized_auth_frame() {
    let server = require_some(
        TestServerBuilder::new().spawn().await,
        "test websocket server should start",
    );
    let mut websocket = require_some(
        connect_websocket(&server).await,
        "websocket should connect before auth",
    );
    let oversized_jwt = "x".repeat(MAX_CLIENT_FRAME_BYTES);
    let oversized_auth = encode_client_frame(&[serde_json::json!({
        "t": "auth",
        "p": {
            "jwt": oversized_jwt,
        },
    })]);
    send_text_frame(
        &mut websocket,
        oversized_auth,
        "oversized auth payload should still send",
    )
    .await;

    assert_eq!(
        read_close_code(&mut websocket).await,
        Some(CloseCode::Error),
    );

    let metrics = server.state.metrics.snapshot();
    assert_eq!(metrics.ws_handshake_rejected_error(), 1);
}

#[tokio::test]
async fn websocket_rejects_batches_over_protocol_envelope_limit() {
    let (server, _room, mut websocket) =
        authenticated_protocol_websocket("issuer-batch-limit", UserId::Integer(18)).await;
    let oversized_envelopes = (0..=MAX_CLIENT_BATCH_ENVELOPES)
        .map(|_| serde_json::json!({ "t": "info", "p": {} }))
        .collect::<Vec<_>>();
    let oversized_batch = encode_client_frame(&oversized_envelopes);
    send_text_frame(
        &mut websocket,
        oversized_batch,
        "oversized envelope batch should still send",
    )
    .await;

    assert_eq!(
        read_close_code(&mut websocket).await,
        Some(CloseCode::Protocol),
    );

    let metrics = server.state.metrics.snapshot();
    assert_eq!(metrics.ws_bus_parse_failures(), 1);
    assert_eq!(metrics.ws_bus_invalid_input_failures(), 1);
    assert_eq!(metrics.ws_bus_unsupported_feature_failures(), 0);
}

#[tokio::test]
async fn websocket_rejects_mismatched_negotiation_response_kind() {
    let (_server, _room, mut websocket) =
        authenticated_protocol_websocket("issuer-mismatched-negotiation", UserId::Integer(21))
            .await;
    let (request_id, request) = require_some(
        wait_for_protocol_server_request(&mut websocket).await,
        "initial offer should be sent",
    );
    assert!(matches!(request, ServerRequest::Offer(_)));
    let sdp = require_some(
        test_rtc_answer_sdp(&request),
        "test rtc answer should be valid for the offer",
    );
    let envelope = require_ok(
        ClientEnvelope::Response {
            response_to: request_id,
            response: ClientResponse::Renegotiate(SessionDescriptionPayload {
                sdp,
                upload_slots: Vec::new(),
            }),
        }
        .into_envelope(),
        "mismatched response should encode",
    );
    let frame = require_ok(
        serde_json::to_string(&[envelope]),
        "mismatched response frame should serialize",
    );
    send_text_frame(
        &mut websocket,
        frame,
        "mismatched negotiation response should still send",
    )
    .await;

    assert_eq!(
        read_close_code_promptly(&mut websocket).await,
        Some(CloseCode::Protocol),
    );
}

#[tokio::test]
async fn invalid_protocol_initial_answer_closes_before_user_negotiates() {
    let (_server, room, mut websocket) =
        authenticated_protocol_websocket("issuer-invalid-publish-answer", UserId::Integer(91))
            .await;
    let publish_frame = encode_client_frame(&[serde_json::json!({
        "t": "publish",
        "p": {
            "type": "camera",
        },
    })]);
    send_text_frame(
        &mut websocket,
        publish_frame,
        "protocol publish intent should still send",
    )
    .await;

    let (request_id, request) = require_some(
        wait_for_protocol_server_request(&mut websocket).await,
        "initial offer should be sent",
    );
    assert!(matches!(request, ServerRequest::Offer(_)));
    assert!(
        respond_to_protocol_negotiation_request(
            &mut websocket,
            request_id,
            request,
            "invalid-answer",
        )
        .await
        .is_some(),
        "invalid answer should still round-trip through the websocket"
    );

    assert_eq!(
        read_close_code_promptly(&mut websocket).await,
        Some(CloseCode::Protocol),
    );
    assert!(
        !room
            .test_api()
            .is_stream_published(
                &UserId::Integer(91),
                &stream_id_for_stream_type(StreamType::Camera),
            )
            .await,
        "invalid initial answer must not let queued publish state commit through a fallback-ready user"
    );
}
