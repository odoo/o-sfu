//! Membership commit and effect boundary.
//!
//! [`JoinCommit`], [`ConnectionCloseCommit`] and
//! [`DisconnectCommit`](crate::engine::room::state::DisconnectCommit) capture
//! authoritative membership changes and the work consumed by [`RoomEffects`].
//! Effects execute after the room-state write guard is released.

#[cfg(any(test, feature = "testing-transport"))]
use o_sfu_router::rtp::MediaCapabilities;
use o_sfu_telemetry::schema::event as telemetry_event;
use tracing::{info, warn};

use super::{
    BroadcastPayloadError, Room, RoomJoinError, UserOutboundSender,
    effects::batch::{RoomEffectContext, RoomEffects},
    media_graph::CommittedTransportReceipt,
    placement::JoinAdmissionTurn,
    state::ConnectionCloseCommit,
};
use crate::engine::{
    ConnectionId, MediaWorkerId, UserId, UserInfo, UserPermissions,
    media_transport::MediaTransport, room::state::JoinCommit,
};

/// Compatibility marker that collapses every authenticated [`UserPermissions`] value.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RoomUserPermissions;

impl From<UserPermissions> for RoomUserPermissions {
    fn from(_value: UserPermissions) -> Self {
        Self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserCloseReason {
    Replaced,
    RemovedByRuntime,
}

/// Existing users replace their current connection without consuming another admission slot.
pub struct JoinUserRequest {
    pub user_id: UserId,
    /// Ignored by room admission.
    pub label: Option<String>,
    /// Ignored by room admission.
    pub permissions: UserPermissions,
    pub sender: UserOutboundSender,
}

impl Room {
    /// Commits the admission turn with the room router.
    ///
    /// # Errors
    ///
    /// Returns [`RoomJoinError::RoomFull`] when a new user exceeds capacity.
    /// Returns [`RoomJoinError::RouterState`] when placement cannot commit.
    pub(super) async fn commit_admission(
        &self,
        admission: JoinAdmissionTurn<'_, impl FnOnce() -> o_sfu_router::RouterId>,
        context: RoomEffectContext<'_>,
    ) -> Result<JoinCommit, RoomJoinError> {
        let joined_fanout = context.user_joined_fanout();
        admission.commit(self, joined_fanout).await
    }

    /// Executes context-enabled [`RoomEffects`] before returning the committed receipt
    pub(super) async fn finalize_admission(
        &self,
        commit: JoinCommit,
        context: RoomEffectContext<'_>,
    ) -> CommittedTransportReceipt {
        let JoinCommit {
            receipt,
            effects,
            transport_plan,
        } = commit;
        RoomEffects::from_join(effects, transport_plan)
            .execute(self, context)
            .await;
        let session = &receipt.transport_session_key;
        info!(
            event = telemetry_event::USER_JOINED,
            room_id = self.uuid(),
            user_id = %session.user_id().path_segment(),
            connection_id = session.connection_id().as_u64(),
            media_worker_id = session.media_worker_id().as_usize(),
            "user joined room"
        );
        receipt
    }

    /// Returns `true` only when `connection_id` removed the current room user.
    pub(crate) async fn remove_user(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        media_transport: &MediaTransport,
    ) -> bool {
        self.remove_user_with_teardown(
            user_id,
            connection_id,
            RoomEffectContext::runtime(media_transport),
        )
        .await
    }

    /// Returns `true` only when `connection_id` was current. A stale committed
    /// placement may still be retired before returning `false`.
    pub async fn remove_user_with_teardown(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        context: RoomEffectContext<'_>,
    ) -> bool {
        let commit = {
            let mut state = self.state.write().await;
            state.close_connection(user_id, connection_id)
        };
        let Some(commit) = commit else {
            return false;
        };
        let (removed_current_user, media_worker_id) = match &commit {
            ConnectionCloseCommit::Current {
                session_teardown, ..
            } => (
                true,
                session_teardown
                    .as_ref()
                    .map(|teardown| teardown.session_key().media_worker_id()),
            ),
            ConnectionCloseCommit::StalePlacement { .. } => (false, None),
        };
        RoomEffects::from_connection_close(commit)
            .execute(self, context)
            .await;
        if removed_current_user {
            info!(
                event = telemetry_event::USER_CLOSED,
                room_id = self.uuid(),
                user_id = %user_id.path_segment(),
                connection_id = connection_id.as_u64(),
                media_worker_id = media_worker_id.map(MediaWorkerId::as_usize),
                "user closed"
            );
        }
        removed_current_user
    }

    /// Captures recipients in the state snapshot that validates `connection_id`
    /// as current for `sender_id`. Missing or stale senders are ignored.
    ///
    /// # Errors
    ///
    /// Returns [`BroadcastPayloadError`] when the payload exceeds the room
    /// broadcast byte limit or cannot be measured as serialized JSON.
    pub(crate) async fn broadcast(
        &self,
        sender_id: &UserId,
        connection_id: ConnectionId,
        message: serde_json::Value,
    ) -> Result<(), BroadcastPayloadError> {
        let fanout = {
            let state = self.state.read().await;
            state.broadcast_fanout(sender_id, connection_id, message)
        }?;
        if let Some(fanout) = fanout {
            fanout.emit();
        }
        Ok(())
    }

    pub(crate) async fn has_connection(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> bool {
        self.state.read().await.user_connection_id(user_id) == Some(connection_id)
    }

    /// Ignores missing or stale connections. Publication transitions remain
    /// authoritative for camera and screen-sharing presence.
    pub(crate) async fn update_user_info(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        media_transport: &MediaTransport,
        mut info: UserInfo,
    ) {
        info.is_camera_on = None;
        info.is_screen_sharing_on = None;
        let commit = {
            let mut state = self.state.write().await;
            state.apply_presence_update(user_id, connection_id, &info)
        };
        if let Some(commit) = commit {
            RoomEffects::from_presence(commit)
                .execute(self, RoomEffectContext::runtime(media_transport))
                .await;
        } else {
            warn!(
                ?user_id,
                connection_id = ?connection_id,
                ?info,
                "user info update was rejected by room state"
            );
        }
    }

    /// Missing users are ignored.
    ///
    /// # Panics
    ///
    /// Panics if a current room user has no committed router placement.
    pub(crate) async fn disconnect_users(
        &self,
        user_ids: &[UserId],
        media_transport: &MediaTransport,
    ) -> usize {
        self.disconnect_users_with_teardown(user_ids, RoomEffectContext::runtime(media_transport))
            .await
    }

    /// Removes current sessions in one state commit and ignores missing users.
    ///
    /// Returns the number of sessions actually torn down.
    ///
    /// # Panics
    ///
    /// Panics if a current room user has no committed router placement.
    pub async fn disconnect_users_with_teardown(
        &self,
        user_ids: &[UserId],
        context: RoomEffectContext<'_>,
    ) -> usize {
        let commit = {
            let mut state = self.state.write().await;
            state.apply_disconnect_users(user_ids)
        };
        let sessions = commit
            .session_teardowns
            .iter()
            .map(|teardown| teardown.session_key().clone())
            .collect::<Vec<_>>();
        RoomEffects::from_disconnect(commit)
            .execute(self, context)
            .await;
        for session in &sessions {
            info!(
                event = telemetry_event::USER_DISCONNECTED,
                room_id = self.uuid(),
                user_id = %session.user_id().path_segment(),
                connection_id = session.connection_id().as_u64(),
                media_worker_id = session.media_worker_id().as_usize(),
                "user disconnected"
            );
        }
        sessions.len()
    }

    #[cfg(any(test, feature = "testing-transport"))]
    /// Returns `None` for a missing or stale connection. The first accepted
    /// capabilities commit realizes receiver routes waiting on negotiation.
    pub async fn apply_session_negotiated(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        capabilities: MediaCapabilities,
        media_port: &MediaTransport,
    ) -> Option<()> {
        self.user_operation(user_id, connection_id, media_port)
            .apply_session_negotiated(capabilities, &[])
            .await
    }

    #[cfg(test)]
    pub(super) async fn user_count(&self) -> usize {
        self.state.read().await.user_count()
    }

    pub(super) async fn is_empty(&self) -> bool {
        self.state.read().await.is_empty()
    }
}
