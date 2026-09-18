use axum::extract::ws::Message;
use o_sfu_protocol::wire::WebSocketCloseCode;
use secrecy::ExposeSecret;
use serde_json::json;

use super::{MAX_CLIENT_FRAME_BYTES, parse_auth_payload};

#[test]
fn parse_auth_payload_accepts_single_auth_message() {
    let frame = Message::Text(
        serde_json::to_string(&vec![json!({
            "t": "auth",
            "p": {
                "jwt": "token",
                "channel": "room-1",
            },
        })])
        .unwrap_or_default()
        .into(),
    );

    let payload = parse_auth_payload(frame);
    assert!(payload.is_ok());
    let Some(payload) = payload.ok() else {
        return;
    };
    assert_eq!(payload.jwt.expose_secret(), "token");
    assert_eq!(payload.channel.as_deref(), Some("room-1"));
}

#[test]
fn parse_auth_payload_rejects_generated_non_auth_first_frames() {
    let cases = [
        Message::Text("not-json".into()),
        Message::Text(
            serde_json::to_string(&vec![json!({
                "t": "info",
                "p": {},
            })])
            .unwrap_or_default()
            .into(),
        ),
        Message::Text(
            serde_json::to_string(&vec![
                json!({
                    "t": "auth",
                    "p": { "jwt": "token-a" },
                }),
                json!({
                    "t": "auth",
                    "p": { "jwt": "token-b" },
                }),
            ])
            .unwrap_or_default()
            .into(),
        ),
        Message::Binary(vec![0xff].into()),
        Message::Text("x".repeat(MAX_CLIENT_FRAME_BYTES + 1).into()),
        Message::Binary(vec![b'x'; MAX_CLIENT_FRAME_BYTES + 1].into()),
        Message::Ping(Vec::new().into()),
    ];

    for frame in cases {
        assert_eq!(
            parse_auth_payload(frame),
            Err(WebSocketCloseCode::ProtocolError)
        );
    }
}

#[test]
fn parse_auth_payload_treats_close_frame_as_clean_shutdown() {
    assert_eq!(
        parse_auth_payload(Message::Close(None)),
        Err(WebSocketCloseCode::Clean)
    );
}
