//! [`SfuCore::admit_user`] returns one [`MediaSession`] per admitted room connection.
//!
//! The session sequences offer/answer, publication, subscription, recording and
//! cleanup without exposing room or transport internals.
//!
//! ```text
//! SfuCore::admit_user -> MediaSession
//!
//! establish             -> offer -> answer
//! publish               -> [offer -> answer]?
//! deactivate_publication -> cancel pending or suppress committed source
//! subscribe             -> persist intent and reconcile eligible routes
//! close                 -> remove only the current connection
//! ```
//!
//! `&mut MediaSession` serializes negotiation. [`MediaSession::publish`] queues
//! the first intent for each stream while an offer awaits its answer. A successful
//! [`MediaSession::answer`] may return the follow-up offer.
use std::{collections::BTreeMap, mem::replace, sync::Arc};

pub use crate::engine::media_transport::{
    SessionOffer as NegotiationOffer, SessionUploadEncoding as UploadEncoding,
    SessionUploadSlot as UploadSlot,
};
use crate::{
    ConnectionId,
    engine::{
        AvailableFeatures, JsonPayload, PeerSnapshot, RecordingOptions, RecordingState, UserId,
        UserInfo,
        media_transport::{
            MediaTransport, TransportAdapterError, TransportSessionHealth, TransportSessionKey,
        },
        room::{
            BroadcastPayloadError, DeactivateIntentOutcome, JoinUserRequest, PublishIntentOutcome,
            Room, RoomManager, RoomManagerJoinError, RoomUserOperation,
        },
        source_model::{
            SourceDeactivateIntent, SourcePublishIntent, SourceSubscriptionIntent, UserStreamId,
        },
    },
};

/// media-session negotiation phase
///
/// queued publishes live beside the offer state so a user action that arrives
/// while a browser answer is pending cannot interleave with the in-flight SDP
/// exchange
#[derive(Debug, Default)]
enum SessionPhase {
    /// no initial offer has been created yet
    #[default]
    BeforeInitialOffer,
    /// no offer is awaiting an answer
    Stable,
    /// one offer has been sent and must be answered before the next offer
    WaitingForAnswer(InFlightOffer),
}

/// in-flight offer state plus mutations deferred until its answer is accepted
#[derive(Debug)]
struct InFlightOffer {
    purpose: SessionOfferPurpose,
    queued_publishes: BTreeMap<UserStreamId, SourcePublishIntent>,
    follow_up_renegotiation: bool,
}

impl SessionPhase {
    fn can_stage_publish(&self) -> bool {
        !matches!(self, Self::WaitingForAnswer(_))
    }

    fn has_queued_publish(&self, stream_id: &UserStreamId) -> bool {
        matches!(
            self,
            Self::WaitingForAnswer(pending)
                if pending.queued_publishes.contains_key(stream_id)
        )
    }

    fn queue_publish(&mut self, intent: SourcePublishIntent) {
        if let Self::WaitingForAnswer(pending) = self {
            let stream_id = intent.stream_id().clone();
            pending.queued_publishes.insert(stream_id, intent);
        }
    }

    fn remove_queued_publish(&mut self, stream_id: &UserStreamId) -> bool {
        let Self::WaitingForAnswer(pending) = self else {
            return false;
        };
        pending.queued_publishes.remove(stream_id).is_some()
    }

    fn clear_queued_publishes(&mut self) {
        if let Self::WaitingForAnswer(pending) = self {
            pending.queued_publishes.clear();
        }
    }

    fn request_renegotiation(&mut self) -> bool {
        match self {
            Self::BeforeInitialOffer => false,
            Self::Stable => true,
            Self::WaitingForAnswer(pending) => {
                pending.follow_up_renegotiation = true;
                false
            }
        }
    }

    fn mark_follow_up_renegotiation(&mut self) {
        if let Self::WaitingForAnswer(pending) = self {
            pending.follow_up_renegotiation = true;
        }
    }

    fn wait_for_answer(&mut self, purpose: SessionOfferPurpose) {
        *self = Self::WaitingForAnswer(InFlightOffer {
            purpose,
            queued_publishes: BTreeMap::new(),
            follow_up_renegotiation: false,
        });
    }

    #[expect(
        clippy::unreachable,
        reason = "answer validates the phase before awaiting with exclusive session access"
    )]
    fn complete_answer(&mut self) -> InFlightOffer {
        match replace(self, Self::Stable) {
            Self::WaitingForAnswer(pending) => pending,
            _ => unreachable!("answer completion requires an in-flight offer"),
        }
    }
}

/// reason an offer is waiting for an answer
#[derive(Debug)]
enum SessionOfferPurpose {
    EstablishSession,
    RefreshSession,
}

/// error returned by [`MediaSession`] operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SessionError {
    /// [`MediaSession::answer`] was called without an in-flight offer
    #[error("no pending media request")]
    NoPendingRequest,
    /// lower core operation failed or rejected the request
    #[error(transparent)]
    Core(#[from] SfuCoreError),
}

impl SessionError {
    /// whether the runtime should report the error as a client or protocol fault
    ///
    /// transport failures that indicate malformed client input are client
    /// errors, while infrastructure failures are internal errors
    #[must_use]
    pub const fn is_client_error(self) -> bool {
        match self {
            Self::NoPendingRequest => true,
            Self::Core(error) => error.is_client_error(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SfuCoreError {
    /// media transport command failed
    #[error("transport operation failed")]
    Transport(#[source] TransportAdapterError),
    /// accepted answer did not yield client capabilities needed by room state
    #[error("capability projection failed")]
    CapabilityProjection(#[source] TransportAdapterError),
    /// initial answer was valid transport input but stale for room state
    #[error("session negotiation rejected")]
    SessionNegotiationRejected,
    /// refresh answer was valid transport input but stale for room state
    #[error("session refresh rejected")]
    SessionRefreshRejected,
    /// subscription intent targeted a stale or invalid room connection
    #[error("subscription update rejected")]
    SubscriptionUpdateRejected,
}

impl SfuCoreError {
    /// whether this error should close the client as a protocol fault
    #[must_use]
    pub const fn is_client_error(self) -> bool {
        matches!(
            self,
            Self::Transport(TransportAdapterError::InvalidInput)
                | Self::CapabilityProjection(_)
                | Self::SessionNegotiationRejected
                | Self::SessionRefreshRejected
                | Self::SubscriptionUpdateRejected
        )
    }
}

/// A cloneable core handle that admits users into room-bound [`MediaSession`]s.
#[derive(Debug, Clone)]
pub struct SfuCore {
    media_transport: MediaTransport,
    rooms: Arc<RoomManager>,
}

/// One admitted user connection in one room.
///
/// Room mutations revalidate the connection before committing room state.
/// [`close`](Self::close) cannot remove a replacement connection and drains
/// connection-scoped staged media when this session is current.
///
/// Futures returned by [`establish`](Self::establish), [`answer`](Self::answer),
/// [`publish`](Self::publish),
/// [`deactivate_publication`](Self::deactivate_publication),
/// [`renegotiate`](Self::renegotiate), [`subscribe`](Self::subscribe),
/// [`update_info`](Self::update_info) and [`close`](Self::close) are not
/// cancellation safe. Once polled, await them to completion. Negotiation can
/// otherwise leave room and transport state out of step with the local phase.
/// Other mutations can leave committed room state without its transport or
/// output effects.
///
/// Call [`close`](Self::close) before dropping the session. `Drop` performs no
/// room or transport cleanup.
#[derive(Debug)]
pub struct MediaSession {
    core: SfuCore,
    room: Arc<Room>,
    transport_user_key: TransportSessionKey,
    phase: SessionPhase,
    closed: bool,
}

impl SfuCore {
    #[must_use]
    pub fn new(media_transport: MediaTransport, rooms: Arc<RoomManager>) -> Self {
        Self {
            media_transport,
            rooms,
        }
    }

    /// Admits one room user and returns its connection-owning media session.
    ///
    /// An existing `request.user_id` replaces its current connection.
    /// Replacement room and transport effects finish before return.
    /// This future is not cancellation safe. Once polled, await it to completion.
    /// Dropping it after membership commits can leave room or replacement
    /// transport effects unfinished.
    ///
    /// # Errors
    ///
    /// Returns [`RoomManagerJoinError::MissingRoom`] when `room_id` is not
    /// current. Returns [`RoomManagerJoinError::RoomFull`] when a new user
    /// exceeds room capacity. Returns [`RoomManagerJoinError::NoUsableWorker`]
    /// when no configured worker is usable. Returns [`RoomManagerJoinError::RouterState`]
    /// when router placement cannot commit.
    ///
    /// # Panics
    ///
    /// Panics when existing relay state refers to an uncommitted source
    /// placement.
    pub async fn admit_user(
        &self,
        room_id: &str,
        request: JoinUserRequest,
    ) -> Result<MediaSession, RoomManagerJoinError> {
        let admission = self
            .rooms
            .join_user(room_id, request, &self.media_transport)
            .await?;
        Ok(MediaSession {
            core: self.clone(),
            room: admission.room,
            transport_user_key: admission.transport_session_key,
            phase: SessionPhase::default(),
            closed: false,
        })
    }
}

impl MediaSession {
    /// creates the first browser offer for this connection
    ///
    /// returns `Ok(None)` after the initial offer has already been requested
    /// this lets reconnect or duplicate-start paths retry safely without
    /// creating a second initial offer
    ///
    /// # Errors
    ///
    /// returns [`SessionError::Core`] when the transport cannot create the
    /// initial offer
    pub async fn establish(&mut self) -> Result<Option<NegotiationOffer>, SessionError> {
        if !matches!(self.phase, SessionPhase::BeforeInitialOffer) {
            return Ok(None);
        }
        let offer = self
            .core
            .media_transport
            .create_initial_session_offer(self.room.uuid(), &self.transport_user_key)
            .await
            .map_err(SfuCoreError::Transport)?;
        self.phase
            .wait_for_answer(SessionOfferPurpose::EstablishSession);
        Ok(Some(offer))
    }

    /// accepts the answer for the pending offer and commits any ready room work
    ///
    /// a rejection before the worker consumes its pending offer leaves the
    /// application round in place so a direct caller may retry it
    /// failures after the worker consumes that offer do not promise retry even
    /// when the RTC backend rejects the answer
    /// when queued publish intent needs another SDP round the returned offer
    /// must be sent to the client before the next answer
    ///
    /// # Errors
    ///
    /// returns [`SessionError::NoPendingRequest`] when no offer is pending
    /// returns [`SessionError::Core`] when answer application fails, capability
    /// projection fails or room state rejects the accepted answer as stale
    pub async fn answer(&mut self, sdp: &str) -> Result<Option<NegotiationOffer>, SessionError> {
        if !matches!(self.phase, SessionPhase::WaitingForAnswer(_)) {
            return Err(SessionError::NoPendingRequest);
        }
        let applied_answer = self
            .core
            .media_transport
            .apply_session_answer(&self.transport_user_key, sdp)
            .await
            .map_err(SfuCoreError::Transport)?;
        let InFlightOffer {
            purpose,
            queued_publishes,
            follow_up_renegotiation,
        } = self.phase.complete_answer();
        match purpose {
            SessionOfferPurpose::EstablishSession => {
                let client_capabilities = applied_answer.client_capabilities().cloned().ok_or(
                    SfuCoreError::CapabilityProjection(TransportAdapterError::InvalidInput),
                )?;
                self.room_operation()
                    .apply_session_negotiated(
                        client_capabilities,
                        applied_answer.declined_consumers(),
                    )
                    .await
                    .ok_or(SfuCoreError::SessionNegotiationRejected)?;
            }
            SessionOfferPurpose::RefreshSession => {
                self.room_operation()
                    .apply_session_refreshed(applied_answer.declined_consumers())
                    .await
                    .ok_or(SfuCoreError::SessionRefreshRejected)?;
            }
        }
        self.room_operation()
            .commit_staged_publishes(&applied_answer)
            .await;
        let staged = self.stage_queued_publishes(queued_publishes).await?;
        if staged || follow_up_renegotiation {
            return self.renegotiate().await;
        }
        Ok(None)
    }

    /// applies publish intent for one user stream
    ///
    /// returns no offer when the intent is already queued, already active or
    /// must wait for an in-flight answer
    /// returns an offer when the browser must answer a new offer before the
    /// publication can commit
    ///
    /// # Errors
    ///
    /// returns [`SessionError::Core`] when the media backend cannot stage a
    /// publish that needs negotiation
    pub async fn publish(
        &mut self,
        intent: SourcePublishIntent,
    ) -> Result<Option<NegotiationOffer>, SessionError> {
        if self.phase.has_queued_publish(intent.stream_id()) {
            return Ok(None);
        }
        match self
            .start_publish(&intent, self.phase.can_stage_publish())
            .await?
        {
            PublishIntentOutcome::Noop | PublishIntentOutcome::Activated => Ok(None),
            PublishIntentOutcome::Queue => {
                self.phase.queue_publish(intent);
                Ok(None)
            }
            PublishIntentOutcome::Staged => self.renegotiate().await,
        }
    }

    /// deactivates one publication without changing negotiated media
    ///
    /// a queued first publication is cancelled
    /// a staged first publication is rolled back and its pending answer creates
    /// the cleanup offer
    /// a committed publication keeps its source identity, routes and negotiated
    /// MID until session teardown
    pub async fn deactivate_publication(&mut self, intent: SourceDeactivateIntent) {
        if self.phase.remove_queued_publish(intent.stream_id()) {
            return;
        }
        match self.room_operation().deactivate_publication(&intent).await {
            DeactivateIntentOutcome::RolledBack => {
                self.phase.mark_follow_up_renegotiation();
            }
            DeactivateIntentOutcome::Deactivated | DeactivateIntentOutcome::Noop => {}
        }
    }

    /// closes this media session and removes its room connection if still current
    ///
    /// the call is idempotent
    /// it returns `true` only when the room manager removed the current
    /// connection
    /// current-session cleanup drains connection-scoped staged media through
    /// room state
    /// stale sessions do not remove a replacement connection for the same user
    pub async fn close(&mut self) -> bool {
        if self.closed {
            return false;
        }
        self.phase.clear_queued_publishes();
        let did_close = self
            .core
            .rooms
            .close_session(
                self.room_id(),
                self.user_id(),
                self.connection_id(),
                &self.core.media_transport,
            )
            .await;
        self.closed = true;
        did_close
    }

    /// creates a refresh offer when the stable session needs renegotiation
    ///
    /// returns `Ok(None)` before the initial offer, while an answer is pending
    /// or when the transport reports that the requested refresh is unsupported
    /// a call made while an answer is pending records that another offer should
    /// be created after the answer commits
    ///
    /// # Errors
    ///
    /// returns [`SessionError::Core`] when the transport rejects
    /// renegotiation
    pub async fn renegotiate(&mut self) -> Result<Option<NegotiationOffer>, SessionError> {
        if !self.phase.request_renegotiation() {
            return Ok(None);
        }
        let offer = match self
            .core
            .media_transport
            .create_session_renegotiation_offer(&self.transport_user_key)
            .await
        {
            Ok(offer) => offer,
            Err(TransportAdapterError::UnsupportedFeature) => return Ok(None),
            Err(error) => return Err(SfuCoreError::Transport(error).into()),
        };
        self.phase
            .wait_for_answer(SessionOfferPurpose::RefreshSession);
        Ok(Some(offer))
    }

    /// applies receiver intent for sources published by another user
    ///
    /// subscription intent is remembered even when no producer is currently
    /// routable
    /// once negotiation makes the receiver consumable, room effects create the
    /// missing consumer routes
    ///
    /// # Errors
    ///
    /// returns [`SessionError::Core`] when room state rejects this connection
    /// as stale
    pub async fn subscribe(
        &self,
        target_user_id: &UserId,
        intents: &BTreeMap<UserStreamId, SourceSubscriptionIntent>,
    ) -> Result<(), SessionError> {
        self.room_operation()
            .apply_receiver_intent(target_user_id, intents)
            .await
            .ok_or(SfuCoreError::SubscriptionUpdateRejected)?;
        Ok(())
    }

    /// returns `None` when the transport has no endpoint for this session key
    #[must_use]
    pub fn endpoint_health(&self) -> Option<TransportSessionHealth> {
        self.core
            .media_transport
            .session_transport_health(&self.transport_user_key)
    }

    #[must_use]
    pub fn user_id(&self) -> &UserId {
        self.transport_user_key.user_id()
    }

    #[must_use]
    pub const fn connection_id(&self) -> ConnectionId {
        self.transport_user_key.connection_id()
    }

    #[must_use]
    pub fn room_id(&self) -> &str {
        self.room.uuid()
    }

    pub async fn is_current_connection(&self) -> bool {
        self.room
            .has_connection(self.user_id(), self.connection_id())
            .await
    }

    #[must_use]
    pub fn available_features(&self) -> AvailableFeatures {
        self.room.available_features()
    }

    pub async fn recording_state(&self) -> RecordingState {
        self.room.recording_state().await
    }

    /// Returns snapshots for current room users other than this session's user.
    pub async fn peer_snapshots(&self) -> Vec<PeerSnapshot> {
        self.room.user_snapshots_except(self.user_id()).await
    }

    fn room_operation(&self) -> RoomUserOperation<'_> {
        self.room.user_operation(
            self.user_id(),
            self.connection_id(),
            &self.core.media_transport,
        )
    }

    async fn start_publish(
        &self,
        intent: &SourcePublishIntent,
        can_stage: bool,
    ) -> Result<PublishIntentOutcome, SfuCoreError> {
        self.room_operation()
            .start_publish(intent, can_stage)
            .await
            .map_err(SfuCoreError::Transport)
    }

    /// Updates user information for this room connection.
    ///
    /// Missing or stale connections are ignored. `is_camera_on` and
    /// `is_screen_sharing_on` are derived from publication state and discarded
    /// from `info`.
    pub async fn update_info(&self, info: UserInfo) {
        self.room
            .update_user_info(
                self.user_id(),
                self.connection_id(),
                &self.core.media_transport,
                info,
            )
            .await;
    }

    /// Broadcasts `message` to every other current room user.
    ///
    /// Missing or stale sender connections return `Ok(())` without delivery.
    ///
    /// # Errors
    ///
    /// Returns [`BroadcastPayloadError::TooLarge`] when the serialized payload
    /// exceeds the room broadcast limit. Returns
    /// [`BroadcastPayloadError::JsonSerialization`] when JSON serialization
    /// fails.
    pub async fn broadcast(&self, message: JsonPayload) -> Result<(), BroadcastPayloadError> {
        self.room
            .broadcast(self.user_id(), self.connection_id(), message)
            .await
    }

    /// Rejects recording start because no persistent recording backend is enabled.
    ///
    /// `options` has no effect. The request records a rejection and returns
    /// `false`.
    #[must_use]
    #[expect(
        clippy::unused_async,
        reason = "keeps the public MediaSession recording facade async while disabled recording is synchronous"
    )]
    pub async fn start_recording(&self, options: RecordingOptions) -> bool {
        self.room
            .apply_recording_start(self.user_id(), self.connection_id(), options)
    }

    /// Rejects recording stop because no persistent recording backend is enabled.
    ///
    /// The request records a rejection and returns `false`.
    #[must_use]
    #[expect(
        clippy::unused_async,
        reason = "keeps the public MediaSession recording facade async while disabled recording is synchronous"
    )]
    pub async fn stop_recording(&self) -> bool {
        self.room
            .apply_recording_stop(self.user_id(), self.connection_id())
    }

    async fn stage_queued_publishes(
        &self,
        queued: BTreeMap<UserStreamId, SourcePublishIntent>,
    ) -> Result<bool, SessionError> {
        let mut staged = false;
        for intent in queued.into_values() {
            if self.start_publish(&intent, true).await? == PublishIntentOutcome::Staged {
                staged = true;
            }
        }
        Ok(staged)
    }
}
