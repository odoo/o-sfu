//! Room lifecycle, membership and media-route orchestration.
//!
//! [`RoomManager`] publishes current rooms and coordinates admission, worker
//! placement and empty-room removal. Each [`Room`] coordinates [`membership`],
//! the desired [`media_graph`] and per-user [`outbound`] signaling.
//!
//! State transitions capture post-lock transport effects. [`source_policy`]
//! recomputes route activity and packet selection from room and transport
//! snapshots after state changes or coalesced policy wakeups. [`read_model`]
//! captures room facts for diagnostics before they are combined with transport
//! snapshots.

#[cfg(any(test, feature = "testing-transport"))]
#[expect(non_snake_case, reason = "test modules map to local TESTS directories")]
pub(crate) mod TESTS;
mod definition;
mod directory;
mod effects;
mod factory;
mod instance;
mod manager;
mod media_graph;
mod membership;
mod outbound;
mod placement;
mod read_model;
mod recording;
mod source_policy;
mod state;
mod transition;

#[cfg(any(test, feature = "testing-transport"))]
pub use TESTS::api::{NegotiatedPublish, RoomManagerTestApi, RoomTestApi};
pub use factory::{RoomAdmissionPolicy, RoomConfig, RoomRuntimePolicy};
pub(crate) use instance::RoomUserOperation;
use instance::SourcePolicyGuard;
pub use instance::{
    Room, RoomJoinError, RoomManagerJoinError, RoomManagerServeError, RoomMediaCounts,
};
#[cfg(any(test, feature = "testing-transport"))]
pub use manager::JoinPlacementTestGate;
pub use manager::{
    RoomManager, RoomUserAdmission, RuntimeRoomDirectorySnapshot, RuntimeRoomStatsSnapshot,
};
#[cfg(any(test, feature = "testing-transport"))]
pub use media_graph::ConsumerRouteState;
pub use membership::{JoinUserRequest, RoomUserPermissions, UserCloseReason};
pub use outbound::{
    BroadcastPayload, BroadcastPayloadError, DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY,
    DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY, MAX_BROADCAST_PAYLOAD_BYTES, RemoteTrackProjection,
    RemoteTrackSnapshot, RoomEventMessage, UserOutbound, UserOutboundEvent, UserOutboundOverflow,
    UserOutboundOverflowKind, UserOutboundQueueLimits, UserOutboundReceiver, UserOutboundSendError,
    UserOutboundSender,
};
pub use placement::{RoomRuntimeContext, RouterPlacement, RouterPlacements, RouterPlacementsError};
pub use read_model::{
    IncomingBitrateSnapshot, RoomDetailCapture, RoomOverviewCapture, RoomUserCapture,
    RoomUserStatsSnapshot, RoomUsersCapture,
};
#[cfg(feature = "internal-benchmarks")]
pub use source_policy::run_source_policy_turn_for_benchmark;
pub(crate) use transition::{DeactivateIntentOutcome, PublishIntentOutcome};

#[cfg(any(test, feature = "testing-transport"))]
pub use self::effects::batch::RoomEffectContext;
