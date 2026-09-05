use std::collections::BTreeMap;

use o_sfu_protocol::{
    host::NegotiationKind,
    wire::{
        ClientResponse, DownloadStates, NegotiationUploadEncoding, NegotiationUploadSlot,
        RequestId, ServerEnvelope, ServerRequest, SessionDescriptionPayload, StreamType, UserId,
    },
};
use tracing::{Span, field, instrument, warn};

use super::{User, UserError, UserOutput};
use crate::{
    application::stream_catalog::{DiscussStream, source_publish_intent_for_stream_type},
    core::prelude::{
        Bitrate, MediaSession, NegotiationOffer, SessionError, SfuCoreError,
        SourceDeactivateIntent, SourcePublishIntent,
    },
    runtime::telemetry::schema::event as telemetry_event,
};

impl User {
    pub(super) async fn complete_negotiation(
        &mut self,
        response_to: RequestId,
        response: ClientResponse,
    ) -> Result<UserOutput, UserError> {
        let result = self.media.answer(response_to, response).await;
        result.map_err(|e| self.answer_error(e))
    }

    #[instrument(
        name = "transport.renegotiate",
        skip_all,
        fields(
            room_id = %self.room_id(),
            user_id = %self.user_id().path_segment(),
            connection_id = self.connection_id().as_u64()
        )
    )]
    pub(super) async fn renegotiate(&mut self) -> Result<UserOutput, UserError> {
        self.reject_stale_connection().await?;
        let result = self.media.renegotiate().await;
        result.map_err(|e| self.negotiation_error(NegotiationKind::Renegotiate, None, e))
    }

    #[instrument(
        name = "publish.intent",
        skip_all,
        fields(
            room_id = %self.room_id(),
            user_id = %self.user_id().path_segment(),
            connection_id = self.connection_id().as_u64(),
            ?stream_type,
            active
        )
    )]
    pub(super) async fn set_publication_active(
        &mut self,
        stream_type: StreamType,
        active: bool,
    ) -> Result<UserOutput, UserError> {
        if active {
            let intent = source_publish_intent_for_stream_type(stream_type);
            return self
                .media
                .publish(intent)
                .await
                .map_err(|error| self.publish_error(stream_type, error));
        }
        let intent = DiscussStream::for_type(stream_type).deactivate_intent();
        Ok(self.media.deactivate_publication(intent).await)
    }

    #[instrument(
        name = "subscribe.intent",
        skip_all,
        fields(
            room_id = %self.room_id(),
            user_id = %self.user_id().path_segment(),
            connection_id = self.connection_id().as_u64(),
            target_session_id = field::Empty,
            source_count = field::Empty
        )
    )]
    pub(super) async fn subscribe(
        &self,
        target_user_id: UserId,
        states: DownloadStates,
    ) -> Result<UserOutput, UserError> {
        let target_user_id = target_user_id.normalized_for_runtime();
        let span = Span::current();
        span.record(
            "target_session_id",
            field::display(target_user_id.path_segment()),
        );
        let source_intents = DiscussStream::all()
            .filter_map(|stream| stream.subscription_intent_if_requested(&states))
            .collect::<BTreeMap<_, _>>();
        span.record("source_count", source_intents.len());
        let session = self.media.session();
        let result = session.subscribe(&target_user_id, &source_intents).await;
        result.map_err(|error| self.subscribe_error(&target_user_id, error))?;
        Ok(UserOutput::new())
    }

    #[instrument(
        name = "transport.offer.create",
        skip_all,
        fields(
            room_id = %self.room_id(),
            user_id = %self.user_id().path_segment(),
            connection_id = self.connection_id().as_u64()
        )
    )]
    pub(super) async fn run_initial_offer(&mut self) -> Result<UserOutput, UserError> {
        let result = self.media.establish().await;
        result.map_err(|e| self.negotiation_error(NegotiationKind::Offer, None, e))
    }
}

pub(super) struct ServerMediaNegotiation {
    session: MediaSession,
    next: u64,
    pending: Option<(RequestId, NegotiationKind)>,
}

enum AnswerError {
    Protocol(RequestId, &'static str),
    Session(NegotiationKind, RequestId, SessionError),
}

const EMPTY_SDP_ANSWER_LOG: &str = "received empty SDP answer for negotiation request";
const UNKNOWN_ANSWER_LOG: &str = "received negotiation answer for an unknown or stale request";

impl ServerMediaNegotiation {
    pub(super) fn new(session: MediaSession) -> Self {
        Self {
            session,
            next: 0,
            pending: None,
        }
    }

    pub(super) const fn session(&self) -> &MediaSession {
        &self.session
    }

    async fn establish(&mut self) -> Result<UserOutput, SessionError> {
        let offer = self.session.establish().await?;
        Ok(self.issue(NegotiationKind::Offer, offer))
    }

    async fn renegotiate(&mut self) -> Result<UserOutput, SessionError> {
        let offer = self.session.renegotiate().await?;
        Ok(self.issue(NegotiationKind::Renegotiate, offer))
    }

    async fn publish(&mut self, intent: SourcePublishIntent) -> Result<UserOutput, SessionError> {
        let offer = self.session.publish(intent).await?;
        Ok(self.issue(NegotiationKind::Renegotiate, offer))
    }

    async fn deactivate_publication(&mut self, intent: SourceDeactivateIntent) -> UserOutput {
        self.session.deactivate_publication(intent).await;
        UserOutput::new()
    }

    async fn answer(
        &mut self,
        response_to: RequestId,
        response: ClientResponse,
    ) -> Result<UserOutput, AnswerError> {
        let (kind, answer) = match response {
            ClientResponse::Offer(answer) => (NegotiationKind::Offer, answer),
            ClientResponse::Renegotiate(answer) => (NegotiationKind::Renegotiate, answer),
        };
        if answer.sdp.is_empty() {
            return Err(AnswerError::Protocol(response_to, EMPTY_SDP_ANSWER_LOG));
        }
        if !self.expects(&response_to, kind) {
            return Err(AnswerError::Protocol(response_to, UNKNOWN_ANSWER_LOG));
        }
        let offer = match self.session.answer(&answer.sdp).await {
            Ok(offer) => offer,
            Err(error) => return Err(AnswerError::Session(kind, response_to, error)),
        };
        self.pending = None;
        Ok(self.issue(NegotiationKind::Renegotiate, offer))
    }

    pub(super) async fn close(&mut self) {
        self.session.close().await;
    }

    fn issue(&mut self, kind: NegotiationKind, offer: Option<NegotiationOffer>) -> UserOutput {
        let Some(offer) = offer else {
            return UserOutput::new();
        };
        let req_id = RequestId::new(format!("server-{}", self.next));
        self.next = self.next.saturating_add(1);
        let payload = session_description_payload(offer);
        let request = match kind {
            NegotiationKind::Offer => ServerRequest::Offer(payload),
            NegotiationKind::Renegotiate => ServerRequest::Renegotiate(payload),
        };
        self.pending = Some((req_id.clone(), kind));
        vec![ServerEnvelope::Request {
            request_id: req_id,
            request,
        }]
    }

    fn expects(&self, id: &RequestId, kind: NegotiationKind) -> bool {
        matches!(self.pending.as_ref(), Some((req_id, pending_kind)) if req_id == id && *pending_kind == kind)
    }
}

fn session_description_payload(offer: NegotiationOffer) -> SessionDescriptionPayload {
    SessionDescriptionPayload {
        sdp: offer.sdp,
        upload_slots: offer
            .upload_slots
            .into_iter()
            .map(|slot| NegotiationUploadSlot {
                mid: slot.mid,
                kind: slot.kind,
                codecs: slot.codecs,
                simulcast_encodings: slot
                    .simulcast_encodings
                    .into_iter()
                    .map(|encoding| NegotiationUploadEncoding {
                        rid: encoding.rid,
                        max_bitrate: encoding.max_bitrate.map(Bitrate::as_bps),
                        resolution_scale: encoding.resolution_scale,
                        max_framerate: encoding.max_framerate,
                    })
                    .collect(),
            })
            .collect(),
    }
}

impl User {
    fn answer_error(&self, error: AnswerError) -> UserError {
        match error {
            AnswerError::Protocol(response_to, message) => {
                warn!(
                    user_id = ?self.user_id(),
                    connection_id = ?self.connection_id(),
                    remote_address = self.remote_address.as_ref(),
                    ?response_to,
                    "{message}"
                );
                UserError::ProtocolViolation
            }
            AnswerError::Session(kind, response_to, error) => {
                self.negotiation_error(kind, Some(&response_to), error)
            }
        }
    }

    fn negotiation_error(
        &self,
        kind: NegotiationKind,
        response_to: Option<&RequestId>,
        error: SessionError,
    ) -> UserError {
        let operation = match kind {
            NegotiationKind::Offer => "initial_offer_create",
            NegotiationKind::Renegotiate => "renegotiation_offer_create",
        };
        let outcome = match error {
            SessionError::NoPendingRequest => "no_pending_media_request",
            SessionError::Core(error) if error.is_client_error() => "client_negotiation_error",
            SessionError::Core(_) => "transport_error",
        };
        warn!(
            event = telemetry_event::NEGOTIATION_FAILED,
            operation,
            outcome,
            user_id = ?self.user_id(),
            connection_id = ?self.connection_id(),
            remote_address = self.remote_address.as_ref(),
            response_to = ?response_to,
            ?error,
            "media session command failed"
        );
        user_error(error)
    }

    fn publish_error(&self, stream_type: StreamType, error: SessionError) -> UserError {
        warn!(
            event = telemetry_event::PUBLISH_ABORTED,
            operation = "publish_intent",
            outcome = "publish_rejected",
            user_id = ?self.user_id(),
            connection_id = ?self.connection_id(),
            remote_address = self.remote_address.as_ref(),
            ?stream_type,
            ?error,
            "media session command failed"
        );
        user_error(error)
    }

    fn subscribe_error(&self, target_user_id: &UserId, error: SessionError) -> UserError {
        let outcome = match error {
            SessionError::Core(SfuCoreError::SubscriptionUpdateRejected) => "stale_connection",
            SessionError::NoPendingRequest | SessionError::Core(_) => "subscription_failed",
        };
        warn!(
            event = telemetry_event::SUBSCRIBE_REJECTED,
            operation = "consume_prepare",
            outcome,
            user_id = ?self.user_id(),
            connection_id = ?self.connection_id(),
            remote_address = self.remote_address.as_ref(),
            ?target_user_id,
            ?error,
            "media session command failed"
        );
        user_error(error)
    }
}

fn user_error(error: SessionError) -> UserError {
    if error.is_client_error() {
        UserError::ProtocolViolation
    } else {
        UserError::InternalError
    }
}
