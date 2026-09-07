use std::{collections::BTreeMap, time::Duration};

use base64::Engine as _;
use o_sfu_protocol::wire::{UserId, UserPermissions};
use o_sfu_rfc::jwt::{ALGORITHM_HS256, JwtAudience, JwtHeader, TYPE_JWT, URL_SAFE_NO_PAD};
use serde::Serialize;
use serde_json::json;

use super::{
    AuthenticationError, HttpDisconnectClaims, HttpRoomClaims, MAX_JWT_TOKEN_BYTES,
    RegisteredJwtClaims, WebSocketConnectClaims, decode_key, derive_key_from_seed,
    duration_since_epoch, sign, sign_hs256, validate_registered_claims_at, verify,
};

const TEST_AUTH_KEY: &str = "u6bsUQEWrHdKIuYplirRnbBmLbrKV5PxKG7DtA71mng=";

#[test]
fn jwt_claims_round_trip() -> serde_json::Result<()> {
    let room_claims = HttpRoomClaims {
        registered: RegisteredJwtClaims {
            iss: Some("https://odoo.example.com".to_owned()),
            aud: Some(JwtAudience::Multiple(vec![
                "urn:odoo:sfu".to_owned(),
                "urn:odoo:recording".to_owned(),
            ])),
            exp: Some(1_744_000_000_u64.into()),
            ..RegisteredJwtClaims::default()
        },
        key: Some("Y2hhbm5lbC1rZXk=".to_owned()),
        key_seed: None,
    };
    let expected_room_claims = json!({
        "iss": "https://odoo.example.com",
        "aud": ["urn:odoo:sfu", "urn:odoo:recording"],
        "exp": 1_744_000_000,
        "key": "Y2hhbm5lbC1rZXk="
    });
    assert_eq!(serde_json::to_value(&room_claims)?, expected_room_claims);
    assert_eq!(
        serde_json::from_value::<HttpRoomClaims>(expected_room_claims)?,
        room_claims
    );

    let disconnect_claims = HttpDisconnectClaims {
        registered: RegisteredJwtClaims::default(),
        user_ids_by_room: BTreeMap::from([(
            "31dcc5dc-4d26-453e-9bca-ab1f5d268303".to_owned(),
            vec![UserId::Integer(7), UserId::String("guest-3".to_owned())],
        )]),
    };
    let expected_disconnect_claims = json!({
        "userIdsByRoom": {
            "31dcc5dc-4d26-453e-9bca-ab1f5d268303": [7, "guest-3"]
        }
    });
    assert_eq!(
        serde_json::to_value(&disconnect_claims)?,
        expected_disconnect_claims
    );
    assert_eq!(
        serde_json::from_value::<HttpDisconnectClaims>(expected_disconnect_claims)?,
        disconnect_claims
    );
    assert_eq!(
        serde_json::from_value::<HttpDisconnectClaims>(json!({
            "sessionIdsByChannel": {
                "31dcc5dc-4d26-453e-9bca-ab1f5d268303": [7, "guest-3"]
            }
        }))?,
        disconnect_claims
    );

    let websocket_claims = WebSocketConnectClaims {
        registered: RegisteredJwtClaims::default(),
        room_id: "31dcc5dc-4d26-453e-9bca-ab1f5d268303".to_owned(),
        user_id: UserId::Integer(42),
        label: Some("Alice".to_owned()),
        permissions: Some(UserPermissions {
            transcription: Some(false),
            audio_recording: Some(true),
            video_recording: Some(false),
        }),
    };
    let expected_websocket_claims = json!({
        "room_id": "31dcc5dc-4d26-453e-9bca-ab1f5d268303",
        "user_id": 42,
        "label": "Alice",
        "permissions": {
            "transcription": false,
            "audioRecording": true,
            "videoRecording": false
        }
    });
    assert_eq!(
        serde_json::to_value(&websocket_claims)?,
        expected_websocket_claims
    );
    assert_eq!(
        serde_json::from_value::<WebSocketConnectClaims>(expected_websocket_claims)?,
        websocket_claims
    );
    assert_eq!(
        serde_json::from_value::<WebSocketConnectClaims>(json!({
            "sfu_channel_uuid": "31dcc5dc-4d26-453e-9bca-ab1f5d268303",
            "session_id": 42,
            "label": "Alice",
            "permissions": {
                "transcription": false,
                "audioRecording": true,
                "videoRecording": false
            }
        }))?,
        websocket_claims
    );

    Ok(())
}

#[test]
fn sign_and_verify_round_trip() {
    let claims = HttpRoomClaims {
        registered: RegisteredJwtClaims {
            iss: Some("https://odoo.example.com".to_owned()),
            ..RegisteredJwtClaims::default()
        },
        key: Some("Y2hhbm5lbC1rZXk=".to_owned()),
        key_seed: None,
    };
    let token = sign(&claims, TEST_AUTH_KEY);
    assert!(token.is_ok());
    let Some(token) = token.ok() else {
        return;
    };
    let verified = verify::<HttpRoomClaims>(&token, TEST_AUTH_KEY);
    assert!(verified.is_ok());
    let Some(verified) = verified.ok() else {
        return;
    };
    assert_eq!(verified, claims);
}

#[test]
fn sign_and_verify_round_trip_with_uuid_like_channel_key() {
    let claims = HttpRoomClaims {
        registered: RegisteredJwtClaims {
            iss: Some("https://odoo.example.com".to_owned()),
            ..RegisteredJwtClaims::default()
        },
        key: Some("123e4567-e89b-12d3-a456-426614174000".to_owned()),
        key_seed: None,
    };
    let token = sign(&claims, "123e4567-e89b-12d3-a456-426614174000");
    assert!(token.is_ok());
    let Some(token) = token.ok() else {
        return;
    };
    let verified = verify::<HttpRoomClaims>(&token, "123e4567-e89b-12d3-a456-426614174000");
    assert!(verified.is_ok());
    let Some(verified) = verified.ok() else {
        return;
    };
    assert_eq!(verified, claims);
}

#[test]
fn decode_key_matches_legacy_uuid_like_channel_key_bytes() {
    let decoded = decode_key("123e4567-e89b-12d3-a456-426614174000");
    assert_eq!(
        decoded.ok(),
        Some(vec![
            0xd7, 0x6d, 0xde, 0xe3, 0x9e, 0xbb, 0xf9, 0xef, 0x3d, 0x6f, 0xed, 0x76, 0x77, 0x7f,
            0x9a, 0xe3, 0x9e, 0xbe, 0xe3, 0x6e, 0xba, 0xd7, 0x8d, 0x7b, 0xe3, 0x4d, 0x34,
        ])
    );
}

#[test]
fn verify_rejects_expired_token() {
    let now = duration_since_epoch().as_secs();
    let claims = HttpRoomClaims {
        registered: RegisteredJwtClaims {
            exp: Some(now.saturating_sub(1).into()),
            ..RegisteredJwtClaims::default()
        },
        key: None,
        key_seed: None,
    };
    let token = sign(&claims, TEST_AUTH_KEY);
    assert!(token.is_ok());
    let Some(token) = token.ok() else {
        return;
    };
    let error = verify::<HttpRoomClaims>(&token, TEST_AUTH_KEY).err();
    assert!(error.is_some());
    let Some(error) = error else {
        return;
    };
    assert_eq!(error, AuthenticationError::TokenExpired);
}

#[test]
fn verify_rejects_token_when_exp_matches_current_second() {
    let now = duration_since_epoch().as_secs();
    let claims = HttpRoomClaims {
        registered: RegisteredJwtClaims {
            exp: Some(now.into()),
            ..RegisteredJwtClaims::default()
        },
        key: None,
        key_seed: None,
    };
    let token = sign(&claims, TEST_AUTH_KEY);
    assert!(token.is_ok());
    let Some(token) = token.ok() else {
        return;
    };
    let error = verify::<HttpRoomClaims>(&token, TEST_AUTH_KEY).err();
    assert_eq!(error, Some(AuthenticationError::TokenExpired));
}

#[test]
fn registered_claims_use_subsecond_time() -> serde_json::Result<()> {
    let now = Duration::new(1_744_000_000, 500_000_000);
    let cases = [
        (
            r#"{"exp":1744000000.5}"#,
            Err(AuthenticationError::TokenExpired),
        ),
        (r#"{"exp":1744000000.6}"#, Ok(())),
        (r#"{"nbf":1744000000.5}"#, Ok(())),
        (
            r#"{"nbf":1744000000.6}"#,
            Err(AuthenticationError::TokenNotYetValid),
        ),
        (r#"{"iat":1744000060.5}"#, Ok(())),
        (
            r#"{"iat":1744000060.6}"#,
            Err(AuthenticationError::TokenIssuedInFuture),
        ),
    ];

    for (raw, expected) in cases {
        let claims = serde_json::from_str(raw)?;
        assert_eq!(validate_registered_claims_at(&claims, now), expected);
    }
    Ok(())
}

#[test]
fn verify_handles_fractional_expiration() {
    for (claims, expected) in [
        (r#"{"exp":4000000000.5}"#, None),
        (r#"{"exp":0.5}"#, Some(AuthenticationError::TokenExpired)),
    ] {
        let token = sign_raw_claims_token_for_test(claims.as_bytes(), TEST_AUTH_KEY);
        assert!(token.is_some());
        if let Some(token) = token {
            assert_eq!(
                verify::<HttpRoomClaims>(&token, TEST_AUTH_KEY).err(),
                expected
            );
        }
    }
}

#[test]
fn sign_emits_jose_base64url_segments() {
    let claims = HttpRoomClaims {
        registered: RegisteredJwtClaims::default(),
        key: Some("Y2hhbm5lbC1rZXk=".to_owned()),
        key_seed: None,
    };

    let token = sign(&claims, TEST_AUTH_KEY);
    assert!(token.is_ok());
    let Some(token) = token.ok() else {
        return;
    };

    let segments = token.split('.').collect::<Vec<_>>();
    assert_eq!(segments.len(), 3);
    for segment in segments {
        assert!(!segment.contains('='));
        assert!(URL_SAFE_NO_PAD.decode(segment.as_bytes()).is_ok());
    }
}

#[test]
fn verify_accepts_jose_base64url_token_without_typ_header() {
    let claims = HttpRoomClaims {
        registered: RegisteredJwtClaims::default(),
        key: Some("Y2hhbm5lbC1rZXk=".to_owned()),
        key_seed: None,
    };

    let token = sign_token_for_test(&claims, TEST_AUTH_KEY, None);
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };

    let verified = verify::<HttpRoomClaims>(&token, TEST_AUTH_KEY);
    assert_eq!(verified.ok(), Some(claims));
}

#[test]
fn verify_accepts_jose_base64url_token_with_typ_header() {
    let claims = HttpRoomClaims {
        registered: RegisteredJwtClaims::default(),
        key: Some("Y2hhbm5lbC1rZXk=".to_owned()),
        key_seed: None,
    };

    let token = sign_token_for_test(&claims, TEST_AUTH_KEY, Some("JWT"));
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };

    let verified = verify::<HttpRoomClaims>(&token, TEST_AUTH_KEY);
    assert_eq!(verified.ok(), Some(claims));
}

#[test]
fn verify_rejects_generated_invalid_token_shapes() {
    for token in [
        "",
        "header",
        "header.claims",
        ".claims.sig",
        "header..sig",
        "header.claims.",
        "a.b.c.d",
    ] {
        let error = verify::<HttpRoomClaims>(token, TEST_AUTH_KEY).err();
        assert_eq!(error, Some(AuthenticationError::InvalidJwtFormat));
    }
}

#[test]
fn verify_rejects_oversized_token_before_jwt_parsing() {
    let token = "a".repeat(MAX_JWT_TOKEN_BYTES + 1);

    let error = verify::<HttpRoomClaims>(&token, TEST_AUTH_KEY).err();
    assert_eq!(
        error,
        Some(AuthenticationError::TokenTooLarge {
            actual: MAX_JWT_TOKEN_BYTES + 1,
            limit: MAX_JWT_TOKEN_BYTES,
        })
    );
}

#[test]
fn verify_does_not_parse_claims_before_signature_verification() {
    let claims = HttpRoomClaims {
        registered: RegisteredJwtClaims::default(),
        key: None,
        key_seed: None,
    };
    let token = sign(&claims, TEST_AUTH_KEY);
    assert!(token.is_ok());
    let Some(token) = token.ok() else {
        return;
    };
    let invalid_claims_json = replace_token_segment(&token, 1, &URL_SAFE_NO_PAD.encode(b"{"));
    assert!(invalid_claims_json.is_some());
    let Some(invalid_claims_json) = invalid_claims_json else {
        return;
    };

    let error = verify::<HttpRoomClaims>(&invalid_claims_json, TEST_AUTH_KEY).err();
    assert_eq!(error, Some(AuthenticationError::InvalidSignature));
}

#[test]
fn verify_rejects_signed_invalid_claims_json_after_signature_verification() {
    let token = sign_raw_claims_token_for_test(b"{", TEST_AUTH_KEY);
    assert!(token.is_some());
    let Some(token) = token else {
        return;
    };

    let error = verify::<HttpRoomClaims>(&token, TEST_AUTH_KEY).err();
    assert_eq!(error, Some(AuthenticationError::InvalidJsonPayload));
}

fn sign_token_for_test<T: Serialize>(
    claims: &T,
    key_b64: &str,
    typ: Option<&str>,
) -> Option<String> {
    let key = decode_key(key_b64).ok()?;
    let header = JwtHeader {
        alg: ALGORITHM_HS256.to_owned(),
        typ: typ.map(str::to_owned),
    };
    let header_json = serde_json::to_vec(&header).ok()?;
    let claims_json = serde_json::to_vec(claims).ok()?;
    let header_b64 = URL_SAFE_NO_PAD.encode(header_json);
    let claims_b64 = URL_SAFE_NO_PAD.encode(claims_json);
    let signed_data = format!("{header_b64}.{claims_b64}");
    let signature = sign_hs256(signed_data.as_bytes(), &key).ok()?;
    let signature_b64 = URL_SAFE_NO_PAD.encode(signature);
    Some(format!("{signed_data}.{signature_b64}"))
}

fn sign_raw_claims_token_for_test(claims_json: &[u8], key_b64: &str) -> Option<String> {
    let key = decode_key(key_b64).ok()?;
    let header = JwtHeader {
        alg: ALGORITHM_HS256.to_owned(),
        typ: Some(TYPE_JWT.to_owned()),
    };
    let header_json = serde_json::to_vec(&header).ok()?;
    let header_b64 = URL_SAFE_NO_PAD.encode(header_json);
    let claims_b64 = URL_SAFE_NO_PAD.encode(claims_json);
    let signed_data = format!("{header_b64}.{claims_b64}");
    let signature = sign_hs256(signed_data.as_bytes(), &key).ok()?;
    let signature_b64 = URL_SAFE_NO_PAD.encode(signature);
    Some(format!("{signed_data}.{signature_b64}"))
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

#[test]
fn derive_key_from_seed_produces_deterministic_key() {
    let key_b64 = TEST_AUTH_KEY;
    let seed = "Y2hhbm5lbC1rZXk=";
    let derived_key = derive_key_from_seed(key_b64, seed).ok();
    assert!(derived_key.is_some());
    let Some(derived_key) = derived_key else {
        return;
    };
    assert_ne!(derived_key, key_b64);
    let derived_key_again = derive_key_from_seed(key_b64, seed).ok();
    assert_eq!(derived_key_again, Some(derived_key));
}

#[test]
fn derive_key_from_seed_works_with_unpadded_seed() {
    let key_b64 = TEST_AUTH_KEY;
    let seed = "Y2hhbm5lbC1rZXk=";
    let seed_unpadded = seed.trim_end_matches('=');
    let derived_key = derive_key_from_seed(key_b64, seed_unpadded).ok();
    assert!(derived_key.is_some());
    let Some(derived_key) = derived_key else {
        return;
    };
    let derived_key_padded = derive_key_from_seed(key_b64, seed).ok();
    assert_eq!(derived_key_padded, Some(derived_key));
}

#[test]
fn derive_key_from_seed_rejects_invalid_base64() {
    let key_b64 = TEST_AUTH_KEY;
    let invalid_seed = "invalid-base64!";
    let derived_key = derive_key_from_seed(key_b64, invalid_seed).err();
    assert_eq!(
        derived_key,
        Some(AuthenticationError::InvalidBase64Encoding)
    );
}
