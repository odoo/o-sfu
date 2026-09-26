use super::*;

#[test]
fn protocol_close_codes_follow_phase_nine_contract() {
    assert_eq!(u16::from(WebSocketCloseCode::AuthFailed), 4106);
    assert_eq!(u16::from(WebSocketCloseCode::AuthTimeout), 4107);
    assert_eq!(u16::from(WebSocketCloseCode::Kicked), 4108);
    assert_eq!(u16::from(WebSocketCloseCode::RoomFull), 4109);
    assert_eq!(u16::from(WebSocketCloseCode::Overloaded), 4110);
    assert_eq!(
        WebSocketCloseCode::from_u16(4106),
        Some(WebSocketCloseCode::AuthFailed)
    );
    assert_eq!(
        WebSocketCloseCode::from_u16(4109),
        Some(WebSocketCloseCode::RoomFull)
    );
    assert_eq!(
        WebSocketCloseCode::from_u16(4110),
        Some(WebSocketCloseCode::Overloaded)
    );
    assert_eq!(WebSocketCloseCode::from_u16(4999), None);
}

#[test]
fn protocol_decode_rejects_envelopes_with_both_request_and_response_ids() {
    assert_eq!(
        decode_envelope_batch(r#"[{"t":"ping","q":"1","r":"2"}]"#, 1),
        Err(EnvelopeBatchDecodeError::InvalidRoutingMetadata)
    );
}
