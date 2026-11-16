//! Room lifecycle, membership and media-route orchestration.
//!
//! [`RoomManager`] publishes current rooms and coordinates admission, worker
//! placement and empty-room removal. Each [`Room`] coordinates [`membership`],
//! the desired [`media_graph`] and per-user [`outbound`] signaling.
//!
//! State transitions capture post-lock transport effects. [`director`](super::director)
//! recomputes route activity and packet selection from room and transport
//! snapshots after state changes or coalesced policy wakeups. [`read_model`]
//! captures room facts for diagnostics before they are combined with transport
//! snapshots.

#[cfg(test)]
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
mod state;
#[cfg(any(test, feature = "testing-transport"))]
pub mod test_support;
mod transition;

pub use factory::{RoomAdmissionPolicy, RoomConfig, RoomRuntimePolicy};
pub(crate) use instance::RoomUserOperation;
pub use instance::{
    Room, RoomJoinError, RoomManagerJoinError, RoomManagerServeError, RoomMediaCounts,
};
pub use manager::{
    RoomManager, RoomUserAdmission, RuntimeRoomDirectorySnapshot, RuntimeRoomStatsSnapshot,
};
pub use membership::{JoinUserRequest, UserCloseReason};
pub use outbound::{
    BroadcastPayload, BroadcastPayloadError, DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY,
    DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY, RemoteTrack, RemoteTrackSnapshot, RoomEventMessage,
    UserOutbound, UserOutboundEvent, UserOutboundOverflow, UserOutboundOverflowKind,
    UserOutboundQueueLimits, UserOutboundReceiver, UserOutboundSendError, UserOutboundSender,
};
pub use placement::{RoomRuntimeContext, RouterPlacement};
pub use read_model::{
    IncomingBitrateSnapshot, RoomDetailCapture, RoomOverviewCapture, RoomUserCapture,
    RoomUserStatsSnapshot, RoomUsersCapture,
};
pub(crate) use transition::{DeactivateIntentOutcome, PublishIntentOutcome};

// Director plans from and commits to these during a source policy turn.
pub(in crate::engine) use self::{
    effects::transport::RoomRouteEffects,
    instance::SourcePolicyGuard,
    media_graph::{ConsumerRouteView, PendingUpgrade, SubscriptionKey},
    outbound::MessageFanout,
    state::{ActiveUser, RoomState},
};
