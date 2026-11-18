//! Rooms, media sessions and packet forwarding for the SFU server.
//!
//! Core is, well... the "core" of the SFU. It is where the main job of the SFU happens,
//! where packets are routed, where we decide who gets what and how... Where the async stuff
//! gets nasty and where performance is critical. It is also more business agnostic than the
//! orchestration layer (/src).
//!
//! Core accepts publication and subscription intent from signaling, maintains media routes
//! between participants and forwards packets through WebRTC transports. Authentication and
//! signaling messages belong to the server runtime. Callers must authenticate users before
//! admission.
//!
//! Session operations are available through [`SfuCore`](prelude::SfuCore) and
//! [`MediaSession`](prelude::MediaSession) in [`prelude`]. [`server`] integration APIs handle
//! room management, transport configuration, metrics and packet sinks. [`Bitrate`] and
//! process-local identifiers live at the crate root.
//!
//! # Rooms and media transport
//!
//! [`RoomManager`](server::room::RoomManager) coordinates room creation, admission and removal.
//! [`SfuCore::admit_user`](prelude::SfuCore::admit_user) returns a
//! [`MediaSession`](prelude::MediaSession) for an admitted connection. Reconnecting replaces
//! the previous connection, so closing an obsolete session cannot remove its replacement.
//!
//! Room state records publications and subscriptions, while the pure [`o_sfu_router`] state
//! machine maintains routing topology. Director selects active routes and video encodings per
//! receiver from layout preferences, observed speakers and bandwidth estimates. Codec-specific
//! packet inspection and rewriting belong to media transport.
//!
//! [`MediaTransport`](server::transport::MediaTransport) applies room and Director decisions to
//! RTC workers. Workers run on separate threads with `str0m` connections and UDP sockets.
//! Packet loops forward media through projected routes and packet gates without deciding room
//! policy. Room state guards are released before awaiting transport and signaling effects.
//!
//! ```text
//!         MediaSession               dirty rooms, snapshots <───────────────┐
//!      one per connection                       │                           │
//!               │                               │                           │
//!               v                               v                           │
//!          transition                      policy turn                      │
//!               │                               │                           │
//!               ├────────> RoomState <──────────┤                           │
//!               │          one RwLock           │                           │
//!               v                               v                           │
//!          RoomEffects                  gates, activity,                    │
//!               │                       bandwidth targets                   │
//!               │                               │                           │
//!               └───────> MediaTransport <──────┘                           │
//!                               │                                           │
//!                               │ commands                                  │ [[ main runtime ]] Tokio tasks on thread pool
//! - - - - - - - - - - - - - - - │ - one command mailbox per worker - - - -  │ - - -
//!                               │                                           │ [[ media workers ]] 1 worker per thread
//!          ┌────────────────────┼─────────────────────┐                     │
//!          │                    v                     │                     │
//!          │                 control                  │                     │
//!          │                    │ writes gates        │                     │
//!          │                    v                     │                     │
//!          │             PacketLoopState              │                     │
//!          │                    │ read per packet     │                     │
//!          │                    v                     │                     │
//! UDP/Relay ════════════> packet_loop ═══════════════════> UDP, relay       │
//!  Input   │                    │                     │      Output         │
//!          │                    v                     │                     │
//!          │              observations ───────────────┼─────────────────────┘
//!          │                                          │
//!          └──── RtcWorker, one OS thread each ───────┘
//! ```
//!
//! # Using core
//!
//! ## Construct the services
//!
//! 1. Call [`MediaTransport::build`](server::transport::MediaTransport::build) with
//!    [`MediaTransportConfig`](server::transport::MediaTransportConfig) and
//!    [`MediaTransportDeps`](server::transport::MediaTransportDeps). Configuration sets the
//!    worker count, advertised IP, ports, codecs and bitrate limits, and dependencies supply
//!    shared metrics and a packet-sink registry. Once built, every worker has bound its UDP
//!    socket. Per-connection RTC state is created during negotiation.
//! 2. Create [`RoomRuntimePolicy`](server::room::RoomRuntimePolicy) from the transport's
//!    [`router_rtp_capabilities`](server::transport::MediaTransport::router_rtp_capabilities).
//!    Pass this policy, shared metrics, reservation TTL and departure grace to
//!    [`RoomManager::new`](server::room::RoomManager::new).
//! 3. Wrap that room manager in an `Arc` and pass it with a transport clone to
//!    [`SfuCore::new`](prelude::SfuCore::new). Keep the room manager and transport handles
//!    for room administration, background work and shutdown.
//!
//! ## Create rooms and admit connections
//!
//! [`RoomManager::serve_room`](server::room::RoomManager::serve_room) reserves a room for an
//! issuer from validated key bytes and [`RoomConfig`](server::room::RoomConfig). Matching
//! reservations reuse the current room, while a conflicting key or configuration returns
//! [`RoomManagerServeError::ConflictingReservation`](server::room::RoomManagerServeError::ConflictingReservation).
//! Use [`Room::uuid`](server::room::Room::uuid) as the room ID for admission and lookup.
//!
//! For each authenticated connection, create an outbound queue with
//! [`UserOutboundSender::channel_with_limits`](server::room::UserOutboundSender::channel_with_limits).
//! Pass its sender in [`JoinUserRequest`](server::room::JoinUserRequest), along with user
//! identity, permissions and MID-reuse support, to
//! [`SfuCore::admit_user`](prelude::SfuCore::admit_user). Admission returns a
//! [`MediaSession`](prelude::MediaSession) or a
//! [`RoomManagerJoinError`](server::room::RoomManagerJoinError). Keep the receiver in the
//! connection's signaling loop and handle its events as described below.
//!
//! ## Drive a media session
//!
//! | Operation | Caller responsibility |
//! | --- | --- |
//! | [`establish`](prelude::MediaSession::establish) | Request initial SDP negotiation after admission. Send returned offers to the client. |
//! | [`answer`](prelude::MediaSession::answer) | Apply a client SDP answer to the pending negotiation and send any follow-up offer. |
//! | [`publish`](prelude::MediaSession::publish) | Submit a [`SourcePublishIntent`](prelude::SourcePublishIntent) and handle any returned offer. |
//! | [`subscribe`](prelude::MediaSession::subscribe) | Pass a publisher's user ID and a map from [`UserStreamId`](prelude::UserStreamId) to [`SourceSubscriptionIntent`](prelude::SourceSubscriptionIntent). |
//! | [`deactivate_publication`](prelude::MediaSession::deactivate_publication) | Submit [`SourceDeactivateIntent`](prelude::SourceDeactivateIntent) to cancel pending publication or suppress committed media. Committed sources keep their identity and negotiated media. |
//! | [`renegotiate`](prelude::MediaSession::renegotiate) | Request another SDP exchange when a remote-track update requires negotiation. |
//! | [`update_info`](prelude::MediaSession::update_info), [`broadcast`](prelude::MediaSession::broadcast) | Update participant information or send an application message to other room users. |
//! | [`peer_snapshots`](prelude::MediaSession::peer_snapshots), [`endpoint_health`](prelude::MediaSession::endpoint_health) | Read current peers or transport health for connection status. |
//! | [`close`](prelude::MediaSession::close) | Await connection cleanup before dropping the session, including after errors. |
//!
//! Build publication intent from a stream ID, media kind and
//! [`SourcePolicy`](prelude::SourcePolicy). Translate product concepts such as camera and
//! screen sharing into these values before calling core. Subscription updates are partial:
//! `None` preserves an existing active or layout preference. Core keeps subscription intent
//! while publishers are absent or not yet negotiated.
//!
//! Negotiation and subscription failures return [`SessionError`](prelude::SessionError).
//! Use [`is_client_error`](prelude::SessionError::is_client_error) to distinguish client or
//! protocol faults from infrastructure failures. Broadcast failures return
//! [`BroadcastPayloadError`](server::room::BroadcastPayloadError).
//!
//! ## Handle outbound events
//!
//! Drain [`UserOutboundReceiver::recv_event`](server::room::UserOutboundReceiver::recv_event)
//! alongside client input. [`UserOutboundEvent::Message`](server::room::UserOutboundEvent::Message)
//! delivers one of these outputs:
//!
//! - [`UserOutbound::Message`](server::room::UserOutbound::Message): translate room events
//!   into signaling messages for the client.
//! - [`UserOutbound::RemoteTracks`](server::room::UserOutbound::RemoteTracks): save the snapshot's
//!   `requires_negotiation` flag, pass the snapshot to
//!   [`fresh_remote_tracks`](prelude::MediaSession::fresh_remote_tracks) and send any returned
//!   track metadata. Call [`renegotiate`](prelude::MediaSession::renegotiate) when the flag is set.
//! - [`UserOutbound::Close`](server::room::UserOutbound::Close): stop the connection and await
//!   session cleanup.
//!
//! On [`Overflow`](server::room::UserOutboundEvent::Overflow) or
//! [`Closed`](server::room::UserOutboundEvent::Closed), stop draining and close the
//! connection. Overflow takes precedence over queued output, so continuing to drain would
//! hide the loss of required signaling events.
//!
//! ## Run background work and administration
//!
//! Run one task to drain
//! [`source_policy_subscription`](server::transport::MediaTransport::source_policy_subscription)
//! and pass room IDs from [`wait_for_update`](server::transport::SourcePolicyUpdateSubscription::wait_for_update)
//! to [`sync_source_packet_selection_policies_for_runtime_ids`](server::room::RoomManager::sync_source_packet_selection_policies_for_runtime_ids).
//! Await each policy update to completion. Call
//! [`check_expired_room_reservations`](server::room::RoomManager::check_expired_room_reservations)
//! periodically to remove expired reservations and rooms whose departure grace has elapsed.
//!
//! Use [`disconnect_users`](server::room::RoomManager::disconnect_users) for administrative
//! removal and [`stats_snapshots`](server::room::RoomManager::stats_snapshots) for room statistics.
//! [`available_features`](prelude::MediaSession::available_features) reports room capabilities.
//! Recording start and stop currently return `false` because no persistent recording backend
//! is enabled.
//!
//! # Negotiation and cleanup
//!
//! [`MediaSession`](prelude::MediaSession) serializes negotiation through mutable access. Core
//! creates SDP offers and accepts client answers, with at most one negotiation pending.
//! Publishing during initial negotiation is deferred until client capabilities are known, and
//! later media changes can require renegotiation.
//!
//! Forward [`NegotiationOffer`](prelude::NegotiationOffer) upload slots and SDP to clients.
//! Send remote-track metadata before SDP so clients can identify receiving media sections.
//! [`MediaSession::answer`](prelude::MediaSession::answer) may return another
//! [`NegotiationOffer`](prelude::NegotiationOffer). Keep exchanging offers and answers
//! until negotiation finishes.
//!
//! ```no_run
//! use o_sfu_core::prelude::{MediaSession, NegotiationOffer, SessionError};
//!
//! # async fn exchange(_: NegotiationOffer) -> String { unimplemented!() }
//! async fn negotiate(session: &mut MediaSession) -> Result<(), SessionError> {
//!     let mut pending = session.establish().await?;
//!     while let Some(offer) = pending {
//!         // The signaling adapter sends the metadata and offer, then awaits the answer.
//!         let answer = exchange(offer).await;
//!         pending = session.answer(&answer).await?;
//!     }
//!     Ok(())
//! }
//! ```
//!
//! The same exchange applies to [`publish`](prelude::MediaSession::publish) and
//! [`renegotiate`](prelude::MediaSession::renegotiate) results. `None` means there is no new
//! SDP to send, including when changes wait on a pending negotiation.
//!
//! Admission and session mutations have cancellation restrictions documented on
//! [`SfuCore::admit_user`](prelude::SfuCore::admit_user) and
//! [`MediaSession`](prelude::MediaSession). Await these operations to completion once polled so
//! committed state receives its transport and signaling effects. Await
//! [`MediaSession::close`](prelude::MediaSession::close) before dropping a session, even on
//! error paths, because `Drop` performs no room or transport cleanup.
//! [`MediaTransport::shutdown`](server::transport::MediaTransport::shutdown) cancels RTC
//! workers and waits for their threads to exit.

use std::fmt::{self, Display, Formatter};

pub use o_sfu_router::{ConnectionId, MediaWorkerId};

mod engine;
mod options;
pub mod prelude;
pub mod server;
mod sfu;

pub(crate) use options::{
    AudioCodecPreference, CodecPreferences, MediaCodecFlags, RoomMediaLimits, RoomWorkerPolicy,
    RtcPortRange, RtcTcpConfig, RtcUdpIoBackend, RuntimeFeatureFlags, SessionBitrateLimits,
    VideoAdaptationTuning, VideoBitrateLimits, VideoCodecPreference,
};

/// Media bitrate in bits per second, with decimal kilo- and megabit units.
///
/// Unit conversion and addition saturate at `u64::MAX`. Subtraction saturates
/// at zero.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Bitrate(u64);

impl Bitrate {
    #[must_use]
    pub const fn from_bps(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn from_kbps(value: u64) -> Self {
        Self(value.saturating_mul(1_000))
    }

    #[must_use]
    pub const fn from_mbps(value: u64) -> Self {
        Self(value.saturating_mul(1_000_000))
    }

    #[must_use]
    pub const fn zero() -> Self {
        Self(0)
    }

    #[must_use]
    pub const fn as_bps(self) -> u64 {
        self.0
    }

    #[must_use]
    pub const fn saturating_add(self, other: Self) -> Self {
        Self(self.0.saturating_add(other.0))
    }

    #[must_use]
    pub const fn saturating_sub(self, other: Self) -> Self {
        Self(self.0.saturating_sub(other.0))
    }

    /// Divides the bitrate, rounding down to whole bits per second.
    ///
    /// A zero divisor returns zero.
    #[must_use]
    pub const fn divided_by(self, divisor: u64) -> Self {
        match self.0.checked_div(divisor) {
            Some(value) => Self(value),
            None => Self::zero(),
        }
    }
}

/// Process-local generation tag for one room lifecycle.
///
/// The tag separates one runtime allocation from its application room identity
/// in transport keys and telemetry. Uniqueness depends on callers sharing one
/// allocation counter and not exhausting it. [`Self::from_raw`] does not check
/// uniqueness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RoomInstanceId(u64);

impl RoomInstanceId {
    /// Allocates the next room generation tag.
    ///
    /// The counter saturates, so repeated calls return `u64::MAX` after
    /// exhaustion.
    #[must_use]
    pub fn allocate(next_room_instance_id: &mut u64) -> Self {
        let room_instance_id = Self(*next_room_instance_id);
        *next_room_instance_id = next_room_instance_id.saturating_add(1);
        room_instance_id
    }

    #[must_use]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl Display for RoomInstanceId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}
