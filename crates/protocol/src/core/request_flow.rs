use super::{
    Command, Commands, FlushMode, NegotiationKind, NegotiationRejection, PendingRequest,
    PendingRequestKind, ProtocolCore, REQUEST_TIMEOUT_MS, close_for_protocol_error,
};
use crate::signaling::{
    ClientEnvelope, ClientRequest, ClientResponse, RecordingOptions, RequestId, ServerRequest,
    ServerResponse, SessionDescriptionPayload,
};

pub(super) fn start_recording(core: &mut ProtocolCore, options: RecordingOptions) -> Commands {
    begin_request(
        core,
        ClientRequest::StartRecording(options),
        PendingRequestKind::StartRecording,
    )
}

pub(super) fn stop_recording(core: &mut ProtocolCore) -> Commands {
    begin_request(
        core,
        ClientRequest::StopRecording,
        PendingRequestKind::StopRecording,
    )
}

pub(super) fn submit_negotiation_answer(
    core: &mut ProtocolCore,
    request_id: &RequestId,
    kind: NegotiationKind,
    sdp: impl Into<String>,
) -> Commands {
    if !core.can_send_client_messages() || !core.phase.resolve_negotiation(request_id, kind) {
        return Vec::new();
    }
    let sdp = sdp.into();
    let response = match kind {
        NegotiationKind::Offer => ClientResponse::Offer(SessionDescriptionPayload {
            sdp,
            upload_slots: Vec::new(),
        }),
        NegotiationKind::Renegotiate => ClientResponse::Renegotiate(SessionDescriptionPayload {
            sdp,
            upload_slots: Vec::new(),
        }),
    };
    let Some(envelope) = ClientEnvelope::Response {
        response_to: request_id.clone(),
        response,
    }
    .into_envelope()
    .ok() else {
        return Vec::new();
    };
    core.enqueue_envelope(envelope, FlushMode::Immediate)
}

pub(super) fn handle_server_request(
    core: &mut ProtocolCore,
    request_id: RequestId,
    request: ServerRequest,
) -> Commands {
    match request {
        ServerRequest::Offer(payload) => {
            handle_negotiation_request(core, request_id, NegotiationKind::Offer, payload)
        }
        ServerRequest::Renegotiate(payload) => {
            handle_negotiation_request(core, request_id, NegotiationKind::Renegotiate, payload)
        }
    }
}

pub(super) fn handle_server_response(
    core: &mut ProtocolCore,
    response_to: &RequestId,
    response: ServerResponse,
) -> Commands {
    match response {
        ServerResponse::StartRecording(payload) => core.request_tracker.resolve_response(
            response_to,
            PendingRequestKind::StartRecording,
            payload.ok,
        ),
        ServerResponse::StopRecording(payload) => core.request_tracker.resolve_response(
            response_to,
            PendingRequestKind::StopRecording,
            payload.ok,
        ),
    }
}

fn handle_negotiation_request(
    core: &mut ProtocolCore,
    request_id: RequestId,
    kind: NegotiationKind,
    payload: SessionDescriptionPayload,
) -> Commands {
    match core.phase.accept_negotiation(&request_id, kind) {
        Ok(()) => {}
        Err(NegotiationRejection::Ignored) => return Vec::new(),
        Err(NegotiationRejection::ProtocolError) => {
            return close_for_protocol_error();
        }
    }
    vec![Command::ApplyNegotiation {
        request_id,
        kind,
        sdp: payload.sdp,
        upload_slots: payload.upload_slots,
    }]
}

fn begin_request(
    core: &mut ProtocolCore,
    request: ClientRequest,
    kind: PendingRequestKind,
) -> Commands {
    if !core.can_send_client_messages() {
        return Vec::new();
    }
    let Some(request_start) = core.request_tracker.try_begin(kind) else {
        return Vec::new();
    };
    let request_id = request_start.request_id;
    let pending_request = PendingRequest {
        request_id: request_id.clone(),
        timeout_timer_id: request_start.timeout_timer_id.raw(),
        timeout_ms: REQUEST_TIMEOUT_MS,
    };
    let Some(envelope) = ClientEnvelope::Request {
        request_id,
        request,
    }
    .into_envelope()
    .ok() else {
        return Vec::new();
    };

    let mut commands = vec![Command::BeginPendingRequest {
        request: pending_request,
    }];
    commands.extend(core.enqueue_envelope(envelope, FlushMode::Batched));
    commands
}
