use std::{fmt, sync::Arc};

use tokio::sync::{Mutex as AsyncMutex, MutexGuard, RwLock};

use super::{definition::RoomDefinition, factory::RoomInit, state::RoomState};
use crate::{
    RoomWorkerPolicy,
    engine::{
        AvailableFeatures, ConnectionId, MediaWorkerId, PeerSnapshot, RecordingState,
        RoomInstanceId, UserId,
        media_transport::{MediaTransport, TransportSessionKey},
        metrics::RuntimeMetrics,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RoomJoinError {
    #[error("room is full")]
    RoomFull,
    #[error("router state error")]
    RouterState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RoomManagerJoinError {
    #[error("room not found")]
    MissingRoom,
    #[error("room is full")]
    RoomFull,
    #[error("router state error")]
    RouterState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RoomManagerServeError {
    #[error("conflicting room reservation")]
    ConflictingReservation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RoomMediaCounts {
    pub publications: usize,
    pub subscriptions: usize,
}

#[derive(Clone, Copy)]
pub(crate) struct RoomUserOperation<'a> {
    pub room: &'a Room,
    pub user_id: &'a UserId,
    pub connection_id: ConnectionId,
    pub media_transport: &'a MediaTransport,
}

/// Holds publication and source-policy ordering for one room.
///
/// Guarded effects derive their room from this guard so another room's lock
/// cannot authorize them. Keep the guard across each state commit and its
/// effects, including every publication resolved by one answer.
#[must_use = "dropping this guard releases source-policy ordering"]
pub(super) struct SourcePolicyGuard<'a> {
    room: &'a Room,
    _lock: MutexGuard<'a, ()>,
}

impl SourcePolicyGuard<'_> {
    pub const fn room(&self) -> &Room {
        self.room
    }
}

pub struct Room {
    pub(super) definition: RoomDefinition,
    pub(super) metrics: Arc<RuntimeMetrics>,
    /// Serializes publication commits and activity changes with source-policy
    /// turns so their transport effects cannot overtake each other.
    source_policy_turn: AsyncMutex<()>,
    pub(super) state: RwLock<RoomState>,
}

impl Room {
    pub(super) fn new(init: RoomInit) -> Self {
        let RoomInit {
            runtime_context,
            runtime_policy,
            issuer,
            key,
            config,
            metrics,
        } = init;
        let definition =
            RoomDefinition::new(&runtime_context, &runtime_policy, issuer, key, config);
        Self {
            definition,
            metrics,
            source_policy_turn: AsyncMutex::new(()),
            state: RwLock::new(RoomState::new(
                &runtime_context,
                runtime_policy.admission_policy,
                runtime_policy.media_limits,
                runtime_policy.video_adaptation_tuning,
                runtime_policy.router_rtp_capabilities,
            )),
        }
    }

    pub(crate) fn user_operation<'a>(
        &'a self,
        user_id: &'a UserId,
        connection_id: ConnectionId,
        media_transport: &'a MediaTransport,
    ) -> RoomUserOperation<'a> {
        RoomUserOperation {
            room: self,
            user_id,
            connection_id,
            media_transport,
        }
    }

    /// Serializes publication commits and effects with source-policy turns.
    ///
    /// Call guarded operations while this guard is held. Acquiring another
    /// source-policy guard for this room before releasing it would deadlock.
    pub(super) async fn lock_source_policy(&self) -> SourcePolicyGuard<'_> {
        SourcePolicyGuard {
            room: self,
            _lock: self.source_policy_turn.lock().await,
        }
    }

    #[must_use]
    pub fn uuid(&self) -> &str {
        self.definition.uuid()
    }

    #[must_use]
    pub fn issuer(&self) -> &str {
        self.definition.issuer()
    }

    #[must_use]
    pub fn key(&self) -> &str {
        self.definition.key()
    }

    #[must_use]
    pub(crate) fn available_features(&self) -> AvailableFeatures {
        self.definition.available_features()
    }

    pub async fn recording_state(&self) -> RecordingState {
        self.state.read().await.recording_state()
    }

    pub(crate) async fn user_snapshots_except(
        &self,
        excluded_user_id: &UserId,
    ) -> Vec<PeerSnapshot> {
        self.state
            .read()
            .await
            .user_snapshots_except(excluded_user_id)
    }

    #[must_use]
    pub fn web_rtc_enabled(&self) -> bool {
        self.definition.web_rtc_enabled()
    }

    /// Returns the transport key for an exact committed router placement.
    ///
    /// # Panics
    ///
    /// Panics when `user_id` and `connection_id` have no committed router
    /// placement.
    #[must_use]
    pub async fn transport_user_key(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> TransportSessionKey {
        self.state
            .read()
            .await
            .transport_user_key(user_id, connection_id)
    }

    pub fn room_worker_policy(&self) -> RoomWorkerPolicy {
        self.definition.room_worker_policy()
    }

    #[must_use]
    pub(crate) fn instance_id(&self) -> RoomInstanceId {
        self.definition.instance_id()
    }
}

impl fmt::Debug for Room {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let media_worker_id = self
            .state
            .try_read()
            .ok()
            .and_then(|state| state.assigned_primary_media_worker_id())
            .map(MediaWorkerId::as_usize);
        formatter
            .debug_struct("Room")
            .field("instance_id", &self.definition.instance_id())
            .field("media_worker_id", &media_worker_id)
            .field("uuid", &self.definition.uuid())
            .field("issuer", &self.definition.issuer())
            .field("web_rtc_enabled", &self.definition.web_rtc_enabled())
            .finish_non_exhaustive()
    }
}
