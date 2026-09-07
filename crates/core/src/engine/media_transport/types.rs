//! media transport boundary values shared by room state, server code and RTC workers

use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use o_sfu_rfc::webrtc::MediaKind;
use o_sfu_router::rtp::{MediaCapabilities, MediaStream as RouterRtpParameters};
use thiserror::Error;

use crate::{Bitrate, ConnectionId, MediaWorkerId, RoomInstanceId, engine::UserId};

/// Exact identity of one committed transport session.
///
/// `room_instance` isolates consecutive lifecycles of the same application room.
/// `connection` rejects work for a replaced session and `media_worker` selects
/// the worker that owns the RTC state. `UserId` alone provides none of these
/// guarantees.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TransportSessionKey {
    room_instance: RoomInstanceId,
    media_worker: MediaWorkerId,
    connection: ConnectionId,
    user: Arc<UserId>,
}

impl TransportSessionKey {
    #[must_use]
    pub fn new(
        room_instance_id: RoomInstanceId,
        media_worker_id: MediaWorkerId,
        connection_id: ConnectionId,
        user_id: impl Into<Arc<UserId>>,
    ) -> Self {
        Self {
            room_instance: room_instance_id,
            media_worker: media_worker_id,
            connection: connection_id,
            user: user_id.into(),
        }
    }

    #[must_use]
    pub const fn room_instance_id(&self) -> RoomInstanceId {
        self.room_instance
    }

    #[must_use]
    pub const fn media_worker_id(&self) -> MediaWorkerId {
        self.media_worker
    }

    #[must_use]
    pub const fn connection_id(&self) -> ConnectionId {
        self.connection
    }

    #[must_use]
    pub fn user_id(&self) -> &UserId {
        self.user.as_ref()
    }
}

pub type TransportResult<T> = Result<T, TransportAdapterError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum TransportAdapterError {
    #[error("transport unavailable")]
    TransportUnavailable,
    #[error("invalid transport input")]
    InvalidInput,
    #[error("unsupported transport feature")]
    UnsupportedFeature,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportSessionHealth {
    Connected,
    Disconnected,
}

/// producer-side transport activity state
///
/// this is transport execution policy, not room membership
/// the room remains
/// responsible for deciding whether a source should be considered published or
/// visible to participants
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerActivity {
    /// RTP from this producer should be forwarded when routes allow it
    Active,
    /// RTP from this producer should not be forwarded until reactivated
    Inactive,
}

impl ProducerActivity {
    #[must_use]
    pub const fn from_active(active: bool) -> Self {
        if active { Self::Active } else { Self::Inactive }
    }

    #[must_use]
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Active)
    }
}

/// Room-authored order for producer activity effects.
///
/// Transport effects run after the room lock is released and can reach worker
/// replicas out of order. Workers ignore older revisions so delayed effects
/// cannot restore superseded activity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd)]
pub(crate) struct SourceActivityRevision(u64);

impl SourceActivityRevision {
    #[must_use]
    pub(crate) const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SourceActivityUpdate {
    activity: ProducerActivity,
    revision: SourceActivityRevision,
}

impl SourceActivityUpdate {
    #[must_use]
    pub(crate) const fn new(activity: ProducerActivity, revision: SourceActivityRevision) -> Self {
        Self { activity, revision }
    }

    #[must_use]
    pub(crate) const fn activity(self) -> ProducerActivity {
        self.activity
    }

    #[must_use]
    pub(crate) const fn revision(self) -> SourceActivityRevision {
        self.revision
    }
}

/// consumer-side transport activity state
///
/// a consumer can be inactive while the room still owns the subscription
/// that distinction lets source policy pause delivery without deleting the
/// negotiated transport route
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerActivity {
    /// RTP may be delivered when this consumer's packet gate also allows it
    Active,
    /// RTP delivery to this consumer is paused
    Inactive,
}

impl ConsumerActivity {
    #[must_use]
    pub const fn from_active(active: bool) -> Self {
        if active { Self::Active } else { Self::Inactive }
    }

    #[must_use]
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Active)
    }
}

/// Transport facts materialized while applying one negotiated SDP answer.
///
/// Producer RTP parameters and declined consumer media are answer-derived.
/// Returning both lets room state commit or release the corresponding graph
/// realization from the same accepted answer.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AppliedSessionAnswer {
    client_capabilities: Option<MediaCapabilities>,
    negotiated_producers: BTreeMap<TransportMediaId, AppliedProducer>,
    declined_consumers: Vec<TransportMediaId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedProducer {
    rtp_parameters: RouterRtpParameters,
    upload_encodings: Vec<SessionUploadEncoding>,
}

impl AppliedProducer {
    #[must_use]
    pub fn new(
        rtp_parameters: RouterRtpParameters,
        upload_encodings: Vec<SessionUploadEncoding>,
    ) -> Self {
        Self {
            rtp_parameters,
            upload_encodings,
        }
    }

    #[must_use]
    pub const fn rtp_parameters(&self) -> &RouterRtpParameters {
        &self.rtp_parameters
    }

    #[must_use]
    pub fn upload_encodings(&self) -> &[SessionUploadEncoding] {
        &self.upload_encodings
    }
}

impl AppliedSessionAnswer {
    #[must_use]
    pub fn from_negotiated_producers(
        negotiated_producer_parameters: impl IntoIterator<
            Item = (TransportMediaId, RouterRtpParameters),
        >,
    ) -> Self {
        Self {
            negotiated_producers: negotiated_producer_parameters
                .into_iter()
                .map(|(transport_media_id, rtp_parameters)| {
                    (
                        transport_media_id,
                        AppliedProducer::new(rtp_parameters, Vec::new()),
                    )
                })
                .collect(),
            ..Self::default()
        }
    }

    #[must_use]
    pub fn from_negotiated_producer_details(
        negotiated_producers: impl IntoIterator<Item = (TransportMediaId, AppliedProducer)>,
    ) -> Self {
        Self {
            negotiated_producers: negotiated_producers.into_iter().collect(),
            ..Self::default()
        }
    }

    pub(crate) fn with_declined_consumers(
        mut self,
        declined_consumers: Vec<TransportMediaId>,
    ) -> Self {
        self.declined_consumers = declined_consumers;
        self
    }

    pub(crate) fn with_client_capabilities(
        mut self,
        capabilities: Option<MediaCapabilities>,
    ) -> Self {
        self.client_capabilities = capabilities;
        self
    }

    pub(crate) const fn client_capabilities(&self) -> Option<&MediaCapabilities> {
        self.client_capabilities.as_ref()
    }

    pub(crate) fn declined_consumers(&self) -> &[TransportMediaId] {
        &self.declined_consumers
    }

    #[must_use]
    pub fn negotiated_producer_parameters(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<&RouterRtpParameters> {
        self.negotiated_producers
            .get(&transport_media_id)
            .map(AppliedProducer::rtp_parameters)
    }

    #[must_use]
    pub fn negotiated_producer_upload_encodings(
        &self,
        transport_media_id: TransportMediaId,
    ) -> &[SessionUploadEncoding] {
        self.negotiated_producers
            .get(&transport_media_id)
            .map_or(&[], AppliedProducer::upload_encodings)
    }
}

/// Point-in-time bitrate measurement aggregated across one or more transport users.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TransportBitrateSnapshot {
    pub total: Bitrate,
    pub per_media: Vec<(TransportMediaId, Bitrate)>,
}

/// Latest receiver-side bandwidth estimates keyed by transport user.
///
/// These values are produced by the WebRTC egress BWE path and consumed by
/// room media policy. They are cold-path control-plane facts, not packet
/// loop routing state.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReceiverBandwidthSnapshot {
    pub per_session: Vec<(TransportSessionKey, Bitrate)>,
}

/// Latest sampled transport-quality facts keyed by transport user.
///
/// These values come from str0m stats events and are intended for diagnostics.
/// Prometheus receives only aggregate counters and histograms so user identity
/// never becomes a metrics label.
pub type TransportQualitySnapshot = BTreeMap<TransportSessionKey, TransportQualitySample>;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransportQualitySample {
    pub latest_bwe_bps: Option<u64>,
    pub rtt_ms: Option<u64>,
    pub ingress_loss_ppm: Option<u64>,
    pub egress_loss_ppm: Option<u64>,
    pub egress_jitter_rtp_timestamp_units: Option<u64>,
    pub sample_count: u64,
}

/// Latest transport health keyed by the requested sessions.
pub type TransportHealthSnapshot = BTreeMap<TransportSessionKey, TransportSessionHealth>;

/// Packet activity and active-speaker observations for requested sources.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TransportSourceDiagnosticsSnapshot {
    pub activity: Vec<TransportSourceActivity>,
    pub active_speaker_diagnostics: Vec<ActiveSpeakerSourceDiagnostic>,
}

/// Recent packet and decoder-refresh age for one producer media id.
///
/// The source-level fields answer whether any packets are still arriving for a
/// publication. RID entries answer whether a selected simulcast layer is still
/// producing packets and decoder refreshes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportSourceActivity {
    transport_media_id: TransportMediaId,
    last_packet_age: Duration,
    last_keyframe_age: Option<Duration>,
    rids: Vec<TransportRidActivity>,
}

impl TransportSourceActivity {
    #[must_use]
    pub fn new(
        transport_media_id: TransportMediaId,
        last_packet_age: Duration,
        last_keyframe_age: Option<Duration>,
        rids: Vec<TransportRidActivity>,
    ) -> Self {
        Self {
            transport_media_id,
            last_packet_age,
            last_keyframe_age,
            rids,
        }
    }

    #[must_use]
    pub const fn transport_media_id(&self) -> TransportMediaId {
        self.transport_media_id
    }

    #[must_use]
    pub const fn last_packet_age(&self) -> Duration {
        self.last_packet_age
    }

    #[must_use]
    pub const fn last_keyframe_age(&self) -> Option<Duration> {
        self.last_keyframe_age
    }

    #[must_use]
    pub fn rids(&self) -> &[TransportRidActivity] {
        &self.rids
    }
}

/// Recent packet and decoder-refresh age for one producer RID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportRidActivity {
    rid: String,
    last_packet_age: Duration,
    last_keyframe_age: Option<Duration>,
}

impl TransportRidActivity {
    #[must_use]
    pub fn new(
        rid: String,
        last_packet_age: Duration,
        last_keyframe_age: Option<Duration>,
    ) -> Self {
        Self {
            rid,
            last_packet_age,
            last_keyframe_age,
        }
    }

    #[must_use]
    pub fn rid(&self) -> &str {
        &self.rid
    }

    #[must_use]
    pub const fn last_packet_age(&self) -> Duration {
        self.last_packet_age
    }

    #[must_use]
    pub const fn last_keyframe_age(&self) -> Option<Duration> {
        self.last_keyframe_age
    }
}

/// Transport-observed pressure for one local media worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportWorkerPressureSnapshot {
    pub media_worker_id: MediaWorkerId,
    pub egress_bitrate: Bitrate,
    pub packet_loop_delay_ms: Option<u64>,
    pub command_backlog_depth: usize,
    pub relay_mailbox_depth: usize,
    pub worker_pressure_score: u8,
}

/// Worker-allocated key for one media realization.
///
/// Relay and diagnostics paths carry this key across workers. It is not a room
/// publication identity and callers must retain its [`TransportSourceKey`] or
/// [`TransportSessionKey`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
pub struct TransportMediaId(u64);

impl TransportMediaId {
    #[must_use]
    pub fn new(raw: u64) -> Self {
        Self(raw)
    }

    #[must_use]
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

/// Transport-observed active speaker keyed by the producing media source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveSpeakerSource {
    transport_media_id: TransportMediaId,
    observed_at: Instant,
    last_audio_level_dbov: Option<i8>,
}

impl ActiveSpeakerSource {
    #[must_use]
    pub const fn new(transport_media_id: TransportMediaId, observed_at: Instant) -> Self {
        Self {
            transport_media_id,
            observed_at,
            last_audio_level_dbov: None,
        }
    }

    #[must_use]
    pub const fn with_audio_level(
        transport_media_id: TransportMediaId,
        observed_at: Instant,
        last_audio_level_dbov: Option<i8>,
    ) -> Self {
        Self {
            transport_media_id,
            observed_at,
            last_audio_level_dbov,
        }
    }

    #[must_use]
    pub const fn transport_media_id(self) -> TransportMediaId {
        self.transport_media_id
    }

    #[must_use]
    pub const fn observed_at(self) -> Instant {
        self.observed_at
    }

    #[must_use]
    pub const fn last_audio_level_dbov(self) -> Option<i8> {
        self.last_audio_level_dbov
    }
}

/// Diagnostic state for the transport-owned active-speaker policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActiveSpeakerActivityState {
    Active,
    Idle,
    Blocked,
    RecentlyExpired,
}

/// Reason attached to one transport-owned active-speaker diagnostic state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActiveSpeakerActivityReason {
    Vad,
    AudioLevel,
    AudioLevelWarmup,
    VadFalse,
    LowNoise,
    BelowSpeechThreshold,
    MissingAudioMetadata,
    Expired,
    NoMetadata,
}

/// Read-only explanation for one audio source's active-speaker policy state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveSpeakerSourceDiagnostic {
    transport_media_id: TransportMediaId,
    state: ActiveSpeakerActivityState,
    reason: ActiveSpeakerActivityReason,
    last_audio_level_dbov: Option<i8>,
    confidence_observations: u8,
    hold_remaining: Option<Duration>,
}

impl ActiveSpeakerSourceDiagnostic {
    #[must_use]
    pub const fn new(
        transport_media_id: TransportMediaId,
        state: ActiveSpeakerActivityState,
        reason: ActiveSpeakerActivityReason,
        last_audio_level_dbov: Option<i8>,
        confidence_observations: u8,
        hold_remaining: Option<Duration>,
    ) -> Self {
        Self {
            transport_media_id,
            state,
            reason,
            last_audio_level_dbov,
            confidence_observations,
            hold_remaining,
        }
    }

    #[must_use]
    pub const fn transport_media_id(self) -> TransportMediaId {
        self.transport_media_id
    }

    #[must_use]
    pub const fn state(self) -> ActiveSpeakerActivityState {
        self.state
    }

    #[must_use]
    pub const fn reason(self) -> ActiveSpeakerActivityReason {
        self.reason
    }

    #[must_use]
    pub const fn last_audio_level_dbov(self) -> Option<i8> {
        self.last_audio_level_dbov
    }

    #[must_use]
    pub const fn confidence_observations(self) -> u8 {
        self.confidence_observations
    }

    #[must_use]
    pub const fn hold_remaining(self) -> Option<Duration> {
        self.hold_remaining
    }
}

/// Packet gate applied by transport to one published source.
///
/// Room policy decides in source-domain terms. The transport boundary receives
/// only the packet-facing selection needed to build the worker-native gate
/// without knowing room layout, source identity or relay placement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourcePacketGate {
    Open,
    Rid(String),
}

/// Relay route mutation applied by the media transport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportRelayRouteEffect {
    pub source: TransportSourceKey,
    pub target_media_worker_id: MediaWorkerId,
    pub action: TransportRelayRouteAction,
}

/// relay-route transport activity state
///
/// inactive relay routes keep their target registration but stop source-worker
/// fanout to that target
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelayRouteActivity {
    Active,
    Inactive,
}

impl RelayRouteActivity {
    #[must_use]
    pub const fn from_active(active: bool) -> Self {
        if active { Self::Active } else { Self::Inactive }
    }

    #[must_use]
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Active)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportRelayRouteAction {
    Install,
    Release,
    SetActivity(RelayRouteActivity),
}

/// producer-side source identity owned by the transport boundary
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TransportSourceKey {
    source_session_key: TransportSessionKey,
    source_transport_media_id: TransportMediaId,
}

impl TransportSourceKey {
    #[must_use]
    pub fn new(
        source_session_key: TransportSessionKey,
        source_transport_media_id: TransportMediaId,
    ) -> Self {
        Self {
            source_session_key,
            source_transport_media_id,
        }
    }

    #[must_use]
    pub fn session_key(&self) -> &TransportSessionKey {
        &self.source_session_key
    }

    #[must_use]
    pub const fn transport_media_id(&self) -> TransportMediaId {
        self.source_transport_media_id
    }

    #[must_use]
    pub const fn room_instance_id(&self) -> RoomInstanceId {
        self.source_session_key.room_instance_id()
    }
}

/// consumer-to-source route identity owned by the transport boundary
///
/// carrying these fields together keeps room code from passing source and
/// receiver ids as adjacent positional arguments
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportConsumerRoute {
    consumer_session_key: TransportSessionKey,
    consumer_transport_media_id: TransportMediaId,
    source: TransportSourceKey,
}

impl TransportConsumerRoute {
    #[must_use]
    pub fn new(
        consumer_session_key: TransportSessionKey,
        consumer_transport_media_id: TransportMediaId,
        source: TransportSourceKey,
    ) -> Self {
        Self {
            consumer_session_key,
            consumer_transport_media_id,
            source,
        }
    }

    #[must_use]
    pub fn consumer_session_key(&self) -> &TransportSessionKey {
        &self.consumer_session_key
    }

    #[must_use]
    pub const fn consumer_transport_media_id(&self) -> TransportMediaId {
        self.consumer_transport_media_id
    }

    #[must_use]
    pub fn source_session_key(&self) -> &TransportSessionKey {
        self.source.session_key()
    }

    #[must_use]
    pub const fn source_transport_media_id(&self) -> TransportMediaId {
        self.source.transport_media_id()
    }

    #[must_use]
    pub fn source(&self) -> &TransportSourceKey {
        &self.source
    }

    #[must_use]
    pub fn is_single_room(&self) -> bool {
        self.consumer_session_key.room_instance_id() == self.source.room_instance_id()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiverBweTargetUpdate {
    session_key: TransportSessionKey,
    target: Bitrate,
}

impl ReceiverBweTargetUpdate {
    #[must_use]
    pub fn new(session_key: TransportSessionKey, target: Bitrate) -> Self {
        Self {
            session_key,
            target,
        }
    }

    #[must_use]
    pub fn session_key(&self) -> &TransportSessionKey {
        &self.session_key
    }

    #[must_use]
    pub const fn target(&self) -> Bitrate {
        self.target
    }
}

/// server-authored SDP offer plus upload metadata for the client
///
/// the SDP is transport state
/// callers should send it to the client unchanged and use `upload_slots` only
/// to project browser-facing source setup hints
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionOffer {
    /// do not parse this outside the transport boundary for routing decisions
    pub sdp: String,
    /// media sections the client may publish after applying the offer
    pub upload_slots: Vec<SessionUploadSlot>,
}

impl SessionOffer {
    #[must_use]
    pub fn new(sdp: String) -> Self {
        Self {
            sdp,
            upload_slots: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_upload_slots(mut self, upload_slots: Vec<SessionUploadSlot>) -> Self {
        self.upload_slots = upload_slots;
        self
    }

    #[must_use]
    pub fn into_parts(self) -> (String, Vec<SessionUploadSlot>) {
        (self.sdp, self.upload_slots)
    }
}

/// one offered upload media section
///
/// `mid` binds this slot to the SDP media section
/// `kind`, `codecs` and `simulcast_encodings` are compatibility metadata for
/// the protocol layer
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionUploadSlot {
    /// SDP media section id for this upload slot
    pub mid: String,
    /// audio or video kind accepted on this media section
    pub kind: MediaKind,
    /// codec names accepted for this upload slot
    pub codecs: Vec<String>,
    /// simulcast layers the client may announce for this upload slot
    pub simulcast_encodings: Vec<SessionUploadEncoding>,
}

/// one upload encoding offered to the client
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionUploadEncoding {
    /// RTP stream id that should appear in RID or simulcast signaling
    pub rid: String,
    /// optional send bitrate ceiling for this encoding
    pub max_bitrate: Option<Bitrate>,
    /// optional inverse scale from source resolution
    pub resolution_scale: Option<u16>,
    /// optional frame-rate ceiling for this encoding
    pub max_framerate: Option<u16>,
}
