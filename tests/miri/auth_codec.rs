//! covers untrusted boundaries:
//! jwt parsing and verification, websocket batch decoding and signaling envelope
//! translation
//! serde, base64 and crypto internals are places we could catch UB with miri

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use o_sfu::{
    auth::{
        AuthenticationError, MAX_JWT_TOKEN_BYTES, RegisteredJwtClaims, WebSocketConnectClaims,
        sign, verify,
    },
    websocket::{
        ClientBatchDecodeError, ClientBatchDecodeFailureKind, MAX_CLIENT_FRAME_BYTES,
        decode_client_batch,
    },
};
use o_sfu_protocol::wire::{
    ClientEnvelope, ClientMessage, ClientResponse, DownloadStates, EnvelopeDecodeError,
    PeerInfoPayload, RecordingActionResult, RequestId, ServerEnvelope, ServerMessage,
    ServerResponse, SessionDescriptionPayload, StreamIntentPayload, StreamType, SubscribePayload,
    UserId, UserInfo, UserPermissions,
};
use secrecy::SecretString;

const TEST_AUTH_KEY: &str = "u6bsUQEWrHdKIuYplirRnbBmLbrKV5PxKG7DtA71mng=";

fn sample_websocket_claims() -> WebSocketConnectClaims {
    WebSocketConnectClaims {
        registered: RegisteredJwtClaims::default(),
        room_id: "room-1".to_owned(),
        user_id: UserId::String("peer-7".to_owned()),
        label: Some("Peer 7".to_owned()),
        permissions: Some(UserPermissions::default()),
    }
}

fn replace_token_segment(token: &str, segment_index: usize, replacement: &str) -> Option<String> {
    let mut parts = token.split('.').map(str::to_owned).collect::<Vec<_>>();
    if parts.len() != 3 {
        return None;
    }
    let part = parts.get_mut(segment_index)?;
    replacement.clone_into(part);
    Some(parts.join("."))
}

fn mutate_signature(token: &str) -> Option<String> {
    let mut parts = token.split('.').map(str::to_owned).collect::<Vec<_>>();
    if parts.len() != 3 {
        return None;
    }
    let signature = parts.get_mut(2)?;
    let mut chars = signature.chars().collect::<Vec<_>>();
    let last = chars.last_mut()?;
    *last = if *last == 'A' { 'B' } else { 'A' };
    *signature = chars.into_iter().collect();
    Some(parts.join("."))
}

#[test]
fn jwt_round_trips_with_sign_and_verify() {
    let claims = sample_websocket_claims();

    assert_eq!(
        verify::<WebSocketConnectClaims>("", &SecretString::from(TEST_AUTH_KEY.to_owned())),
        Err(AuthenticationError::InvalidJwtFormat)
    );

    let token = sign(&claims, &SecretString::from(TEST_AUTH_KEY.to_owned()));
    assert_eq!(token.as_ref().map(|_| ()), Ok(()));
    let Some(token) = token.ok() else {
        return;
    };

    assert_eq!(
        verify::<WebSocketConnectClaims>(&token, &SecretString::from(TEST_AUTH_KEY.to_owned())),
        Ok(claims)
    );
}

#[test]
fn malformed_jwt_shapes_return_invalid_format() {
    for token in [
        "header.payload",
        "header.payload.signature.extra",
        ".payload.signature",
        "header..signature",
        "header.payload.",
    ] {
        assert_eq!(
            verify::<WebSocketConnectClaims>(token, &SecretString::from(TEST_AUTH_KEY.to_owned())),
            Err(AuthenticationError::InvalidJwtFormat)
        );
    }
}

#[test]
fn jwt_segment_failures_are_classified_semantically() {
    let claims = sample_websocket_claims();
    let valid_token = sign(&claims, &SecretString::from(TEST_AUTH_KEY.to_owned()));
    assert_eq!(valid_token.as_ref().map(|_| ()), Ok(()));
    let Some(valid_token) = valid_token.ok() else {
        return;
    };

    let invalid_header_base64 = replace_token_segment(&valid_token, 0, "%%%");
    assert_eq!(invalid_header_base64.as_ref().map(|_| ()), Some(()));
    let Some(invalid_header_base64) = invalid_header_base64 else {
        return;
    };
    assert_eq!(
        verify::<WebSocketConnectClaims>(
            &invalid_header_base64,
            &SecretString::from(TEST_AUTH_KEY.to_owned())
        ),
        Err(AuthenticationError::InvalidBase64Encoding)
    );

    let invalid_header_json = replace_token_segment(&valid_token, 0, &URL_SAFE_NO_PAD.encode(b"{"));
    assert_eq!(invalid_header_json.as_ref().map(|_| ()), Some(()));
    let Some(invalid_header_json) = invalid_header_json else {
        return;
    };
    assert_eq!(
        verify::<WebSocketConnectClaims>(
            &invalid_header_json,
            &SecretString::from(TEST_AUTH_KEY.to_owned())
        ),
        Err(AuthenticationError::InvalidJsonPayload)
    );

    let invalid_claims_json = replace_token_segment(&valid_token, 1, &URL_SAFE_NO_PAD.encode(b"{"));
    assert_eq!(invalid_claims_json.as_ref().map(|_| ()), Some(()));
    let Some(invalid_claims_json) = invalid_claims_json else {
        return;
    };
    assert_eq!(
        verify::<WebSocketConnectClaims>(
            &invalid_claims_json,
            &SecretString::from(TEST_AUTH_KEY.to_owned())
        ),
        Err(AuthenticationError::InvalidSignature)
    );

    let unsupported_algorithm = replace_token_segment(
        &valid_token,
        0,
        &URL_SAFE_NO_PAD.encode(br#"{"alg":"none"}"#),
    );
    assert_eq!(unsupported_algorithm.as_ref().map(|_| ()), Some(()));
    let Some(unsupported_algorithm) = unsupported_algorithm else {
        return;
    };
    assert_eq!(
        verify::<WebSocketConnectClaims>(
            &unsupported_algorithm,
            &SecretString::from(TEST_AUTH_KEY.to_owned())
        ),
        Err(AuthenticationError::UnsupportedAlgorithm("none".to_owned()))
    );

    let invalid_signature = mutate_signature(&valid_token);
    assert_eq!(invalid_signature.as_ref().map(|_| ()), Some(()));
    let Some(invalid_signature) = invalid_signature else {
        return;
    };
    assert_eq!(
        verify::<WebSocketConnectClaims>(
            &invalid_signature,
            &SecretString::from(TEST_AUTH_KEY.to_owned())
        ),
        Err(AuthenticationError::InvalidSignature)
    );
}

#[test]
fn jwt_rejects_oversized_token_before_parsing() {
    let token = "a".repeat(MAX_JWT_TOKEN_BYTES + 1);

    assert_eq!(
        verify::<WebSocketConnectClaims>(&token, &SecretString::from(TEST_AUTH_KEY.to_owned())),
        Err(AuthenticationError::TokenTooLarge {
            actual: MAX_JWT_TOKEN_BYTES + 1,
            limit: MAX_JWT_TOKEN_BYTES,
        })
    );
}

#[test]
fn decode_client_batch_rejects_oversized_frame_before_json_decode() {
    let oversized_payload = "x".repeat(MAX_CLIENT_FRAME_BYTES + 1);

    assert_eq!(
        decode_client_batch(&oversized_payload),
        Err(ClientBatchDecodeError::FrameTooLarge {
            actual: MAX_CLIENT_FRAME_BYTES + 1,
            limit: MAX_CLIENT_FRAME_BYTES,
        })
    );
}

#[test]
fn decode_client_batch_classifies_invalid_and_unsupported_inputs() {
    let cases = [
        (
            r#"[{"t":"not-a-real-message","p":{}}]"#,
            Err(ClientBatchDecodeError::InvalidEnvelope(
                EnvelopeDecodeError::UnknownTag("not-a-real-message".to_owned()),
            )),
            ClientBatchDecodeFailureKind::UnsupportedFeature,
        ),
        (
            r#"[{"t":"info","p":{},"q":"1","r":"2"}]"#,
            Err(ClientBatchDecodeError::InvalidRoutingMetadata),
            ClientBatchDecodeFailureKind::InvalidInput,
        ),
        (
            r#"[{"t":"broadcast"}]"#,
            Err(ClientBatchDecodeError::InvalidEnvelope(
                EnvelopeDecodeError::InvalidPayload("broadcast".to_owned()),
            )),
            ClientBatchDecodeFailureKind::InvalidInput,
        ),
    ];

    for (payload, expected_error, expected_kind) in cases {
        let error = decode_client_batch(payload);
        assert_eq!(error, expected_error);
        assert_eq!(
            error.err().map(|decode_error| decode_error.kind()),
            Some(expected_kind)
        );
    }
}

#[test]
fn signaling_codecs_round_trip_publish_subscribe_and_responses() {
    let publish = ClientEnvelope::Message(ClientMessage::Publish(StreamIntentPayload {
        stream_type: StreamType::Camera,
    }));
    let subscribe = ClientEnvelope::Message(ClientMessage::Subscribe(SubscribePayload {
        user_id: UserId::String("peer-2".to_owned()),
        states: DownloadStates {
            audio: Some(true),
            camera: Some(false),
            screen: None,
            ..DownloadStates::default()
        },
    }));
    let answer = ClientEnvelope::Response {
        response_to: RequestId::new("offer-1"),
        response: ClientResponse::Offer(SessionDescriptionPayload {
            sdp: "v=0\r\ns=answer\r\n".to_owned(),
            upload_slots: Vec::new(),
        }),
    };

    for envelope in [publish, subscribe, answer] {
        let encoded = envelope.clone().into_envelope();
        assert!(encoded.is_ok());
        let Some(encoded) = encoded.ok() else {
            return;
        };
        assert_eq!(ClientEnvelope::decode(encoded), Ok(envelope));
    }

    let server_response = ServerEnvelope::Response {
        response_to: RequestId::new("recording-1"),
        response: ServerResponse::StartRecording(RecordingActionResult { ok: true }),
    };
    let encoded = server_response.clone().into_envelope();
    assert!(encoded.is_ok());
    let Some(encoded) = encoded.ok() else {
        return;
    };
    assert_eq!(ServerEnvelope::decode(encoded), Ok(server_response));
}

#[test]
fn signaling_codecs_round_trip_info_messages_with_structured_json() {
    let session_info = UserInfo {
        is_camera_on: Some(true),
        is_screen_sharing_on: Some(false),
        is_raising_hand: Some(true),
        ..UserInfo::default()
    };
    let client_info = ClientEnvelope::Message(ClientMessage::Info(session_info.clone()));
    let encoded = client_info.clone().into_envelope();
    assert!(encoded.is_ok());
    let Some(encoded) = encoded.ok() else {
        return;
    };
    assert_eq!(ClientEnvelope::decode(encoded), Ok(client_info));

    let server_info = ServerEnvelope::Message(ServerMessage::PeerInfo(PeerInfoPayload {
        user_id: UserId::String("peer-3".to_owned()),
        info: session_info,
    }));
    let encoded = server_info.clone().into_envelope();
    assert!(encoded.is_ok());
    let Some(encoded) = encoded.ok() else {
        return;
    };
    assert_eq!(ServerEnvelope::decode(encoded), Ok(server_info));
}
