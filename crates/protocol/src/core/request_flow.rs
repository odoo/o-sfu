use super::{
    Command, Commands, FlushMode, NegotiationKind, NegotiationRejection, PendingRequest,
    PendingRequestKind, ProtocolCore, REQUEST_TIMEOUT_MS, close_for_protocol_error,
};
use crate::signaling::{
    ClientEnvelope, ClientRequest, ClientResponse, RecordingOptions, RequestId, ServerRequest,
    ServerResponse, SessionDescriptionPayload,
};

impl ProtocolCore {
    pub fn start_recording(&mut self, options: RecordingOptions) -> Vec<Command> {
        begin_request(self, ClientRequest::StartRecording(options))
    }

    pub fn stop_recording(&mut self) -> Vec<Command> {
        begin_request(self, ClientRequest::StopRecording)
    }

    /// Replies to the currently pending negotiation request.
    ///
    /// The host must echo the exact `request_id` and `kind` from
    /// [`Command::ApplyNegotiation`]. Mismatches are ignored so a stale or
    /// reordered SDP answer cannot accidentally resolve the wrong negotiation.
    pub fn submit_negotiation_answer(
        &mut self,
        request_id: &RequestId,
        kind: NegotiationKind,
        sdp: impl Into<String>,
    ) -> Vec<Command> {
        if !self.phase.resolve_negotiation(request_id, kind) {
            return Vec::new();
        }
        let payload = SessionDescriptionPayload {
            sdp: sdp.into(),
            upload_slots: Vec::new(),
        };
        let response = match kind {
            NegotiationKind::Offer => ClientResponse::Offer(payload),
            NegotiationKind::Renegotiate => ClientResponse::Renegotiate(payload),
        };
        let Some(envelope) = ClientEnvelope::Response {
            response_to: request_id.clone(),
            response,
        }
        .into_envelope()
        .ok() else {
            return Vec::new();
        };
        self.outbound_batch.enqueue(envelope, FlushMode::Immediate)
    }
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

fn begin_request(core: &mut ProtocolCore, request: ClientRequest) -> Commands {
    if !core.phase.can_send_client_messages() {
        return Vec::new();
    }
    let kind = match &request {
        ClientRequest::StartRecording(_) => PendingRequestKind::StartRecording,
        ClientRequest::StopRecording => PendingRequestKind::StopRecording,
    };
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
    commands.extend(core.outbound_batch.enqueue(envelope, FlushMode::Batched));
    commands
}
