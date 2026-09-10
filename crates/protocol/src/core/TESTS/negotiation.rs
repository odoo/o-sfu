use super::*;

#[test]
fn protocol_core_emits_negotiation_command_and_accepts_matching_answer() -> Result<(), String> {
    let mut core = authenticated_core();
    let offer_frame = server_offer_frame("offer-1", "v=0\r\ns=offer\r\n")?;
    let offer_commands = core.on_ws_message(&offer_frame);
    assert_eq!(
        offer_commands.as_slice(),
        &[Command::ApplyNegotiation {
            request_id: RequestId::new("offer-1"),
            kind: NegotiationKind::Offer,
            sdp: String::from("v=0\r\ns=offer\r\n"),
            upload_slots: Vec::new(),
        }]
    );
    let answer_commands = core.submit_negotiation_answer(
        &RequestId::new("offer-1"),
        NegotiationKind::Offer,
        "v=0\r\ns=answer\r\n",
    );
    assert_sent_response(
        &answer_commands,
        "offer-1",
        ClientResponse::Offer(SessionDescriptionPayload {
            sdp: String::from("v=0\r\ns=answer\r\n"),
            upload_slots: Vec::new(),
        }),
    );
    Ok(())
}

#[test]
fn protocol_core_rejects_overlapping_negotiation_requests() -> Result<(), String> {
    let mut core = authenticated_core();
    let first_offer = server_offer_frame("offer-1", "v=0\r\ns=offer-1\r\n")?;
    let second_offer = server_offer_frame("offer-2", "v=0\r\ns=offer-2\r\n")?;
    let _ = core.on_ws_message(&first_offer);
    let commands = core.on_ws_message(&second_offer);
    assert_eq!(
        commands.as_slice(),
        &[Command::CloseWebSocket {
            code: u16::from(WebSocketCloseCode::ProtocolError),
        }]
    );
    Ok(())
}

#[test]
fn protocol_core_rejects_initial_offer_after_transport_ready() -> Result<(), String> {
    let mut core = connected_core();
    let late_offer = server_offer_frame("offer-1", "v=0\r\ns=late-offer\r\n")?;
    let commands = core.on_ws_message(&late_offer);
    assert_eq!(
        commands.as_slice(),
        &[Command::CloseWebSocket {
            code: u16::from(WebSocketCloseCode::ProtocolError),
        }]
    );
    Ok(())
}

#[test]
fn protocol_core_rejects_renegotiation_before_transport_ready() -> Result<(), String> {
    let mut core = authenticated_core();
    let renegotiation_frame =
        server_renegotiation_frame("renegotiate-1", "v=0\r\ns=renegotiate\r\n")?;
    let commands = core.on_ws_message(&renegotiation_frame);
    assert_eq!(
        commands.as_slice(),
        &[Command::CloseWebSocket {
            code: u16::from(WebSocketCloseCode::ProtocolError),
        }]
    );
    Ok(())
}

#[test]
fn protocol_core_waits_for_initial_answer_before_transport_ready() -> Result<(), String> {
    let mut core = authenticated_core();
    let offer_frame = server_offer_frame("offer-1", "v=0\r\ns=offer\r\n")?;
    let _ = core.on_ws_message(&offer_frame);
    assert!(core.on_transport_ready().is_empty());
    assert_eq!(core.state(), ConnectionState::Authenticated);
    let answer_commands = core.submit_negotiation_answer(
        &RequestId::new("offer-1"),
        NegotiationKind::Offer,
        "v=0\r\ns=answer\r\n",
    );
    assert_sent_response(
        &answer_commands,
        "offer-1",
        ClientResponse::Offer(SessionDescriptionPayload {
            sdp: String::from("v=0\r\ns=answer\r\n"),
            upload_slots: Vec::new(),
        }),
    );
    assert_eq!(
        core.on_transport_ready().as_slice(),
        &[Command::EmitStateChange {
            state: ConnectionState::Connected,
            cause: None,
        }]
    );
    assert_eq!(core.state(), ConnectionState::Connected);
    Ok(())
}

#[test]
fn protocol_core_accepts_renegotiation_after_transport_ready() -> Result<(), String> {
    let mut core = connected_core();
    let renegotiation_frame =
        server_renegotiation_frame("renegotiate-1", "v=0\r\ns=renegotiate\r\n")?;
    let commands = core.on_ws_message(&renegotiation_frame);
    assert_eq!(
        commands.as_slice(),
        &[Command::ApplyNegotiation {
            request_id: RequestId::new("renegotiate-1"),
            kind: NegotiationKind::Renegotiate,
            sdp: String::from("v=0\r\ns=renegotiate\r\n"),
            upload_slots: Vec::new(),
        }]
    );
    let answer_commands = core.submit_negotiation_answer(
        &RequestId::new("renegotiate-1"),
        NegotiationKind::Renegotiate,
        "v=0\r\ns=answer\r\n",
    );
    assert_sent_response(
        &answer_commands,
        "renegotiate-1",
        ClientResponse::Renegotiate(SessionDescriptionPayload {
            sdp: String::from("v=0\r\ns=answer\r\n"),
            upload_slots: Vec::new(),
        }),
    );
    Ok(())
}

#[test]
fn protocol_core_keeps_pending_negotiation_after_mismatched_answer() -> Result<(), String> {
    let mut core = authenticated_core();
    let offer_frame = server_offer_frame("offer-1", "v=0\r\ns=offer\r\n")?;
    let _ = core.on_ws_message(&offer_frame);
    assert!(
        core.submit_negotiation_answer(
            &RequestId::new("offer-1"),
            NegotiationKind::Renegotiate,
            "v=0\r\ns=stale-answer\r\n",
        )
        .is_empty()
    );
    let answer_commands = core.submit_negotiation_answer(
        &RequestId::new("offer-1"),
        NegotiationKind::Offer,
        "v=0\r\ns=answer\r\n",
    );
    assert_sent_response(
        &answer_commands,
        "offer-1",
        ClientResponse::Offer(SessionDescriptionPayload {
            sdp: String::from("v=0\r\ns=answer\r\n"),
            upload_slots: Vec::new(),
        }),
    );
    Ok(())
}

fn authenticated_core() -> ProtocolCore {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.accept_welcome(sample_welcome_payload());
    core
}

fn connected_core() -> ProtocolCore {
    let mut core = authenticated_core();
    let _ = core.on_transport_ready();
    core
}

fn server_offer_frame(request_id: &str, sdp: &str) -> Result<String, String> {
    server_negotiation_frame(
        request_id,
        ServerRequest::Offer(SessionDescriptionPayload {
            sdp: String::from(sdp),
            upload_slots: Vec::new(),
        }),
    )
}

fn server_renegotiation_frame(request_id: &str, sdp: &str) -> Result<String, String> {
    server_negotiation_frame(
        request_id,
        ServerRequest::Renegotiate(SessionDescriptionPayload {
            sdp: String::from(sdp),
            upload_slots: Vec::new(),
        }),
    )
}

fn server_negotiation_frame(request_id: &str, request: ServerRequest) -> Result<String, String> {
    encode_server_batch(ServerEnvelope::Request {
        request_id: RequestId::new(request_id),
        request,
    })
}

fn assert_sent_response(commands: &[Command], request_id: &str, response: ClientResponse) {
    assert_sent_client_envelopes(
        commands,
        vec![ClientEnvelope::Response {
            response_to: RequestId::new(request_id),
            response,
        }],
    );
}
