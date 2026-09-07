use o_sfu_router::{
    rtp::MediaCapabilities, test_support::rtp_samples::sample_client_rtp_capabilities,
};

use super::{
    super::super::{
        JoinUserRequest, RoomEffectContext, RoomJoinError, UserOutboundSender,
        media_graph::CommittedTransportReceipt, placement::JoinAdmissionTurn,
    },
    RoomTestApi,
};
use crate::engine::{
    ConnectionId, UserId, UserPermissions,
    media_transport::{MediaTransport, TransportAdapterError},
};

impl RoomTestApi<'_> {
    /// # Errors
    ///
    /// returns [`RoomJoinError`] when admission or routing rejects the user
    pub async fn join_user(
        self,
        user_id: UserId,
        label: Option<String>,
        permissions: UserPermissions,
        sender: UserOutboundSender,
    ) -> Result<ConnectionId, RoomJoinError> {
        self.join_user_with_packet_loop_delays(
            user_id,
            label,
            permissions,
            sender,
            vec![Some(0); self.room.room_worker_policy().max_local_routers()],
        )
        .await
    }

    /// # Errors
    ///
    /// returns [`RoomJoinError`] when admission or routing rejects the user
    pub async fn join_user_with_packet_loop_delays(
        self,
        user_id: UserId,
        label: Option<String>,
        permissions: UserPermissions,
        sender: UserOutboundSender,
        delays_ms: Vec<Option<u64>>,
    ) -> Result<ConnectionId, RoomJoinError> {
        let spillover_router_id = self.room.placement_usage_snapshot().await.next_router();
        let request = JoinUserRequest {
            user_id,
            label,
            permissions,
            sender,
        };
        self.admit_session(
            JoinAdmissionTurn::for_test(request, delays_ms, spillover_router_id),
            RoomEffectContext::state_only(None),
        )
        .await
        .map(|receipt| receipt.transport_session_key.connection_id())
    }

    /// # Errors
    ///
    /// returns [`RoomJoinError`] when admission or routing rejects the user
    pub async fn join_session_without_transport_teardown(
        self,
        user_id: UserId,
        label: Option<String>,
        permissions: UserPermissions,
        sender: UserOutboundSender,
        media_transport: &MediaTransport,
    ) -> Result<ConnectionId, RoomJoinError> {
        let spillover_router_id = self.room.placement_usage_snapshot().await.next_router();
        let request = JoinUserRequest {
            user_id,
            label,
            permissions,
            sender,
        };
        self.admit_session(
            JoinAdmissionTurn::for_test(
                request,
                media_transport.packet_loop_delays_ms(),
                spillover_router_id,
            ),
            RoomEffectContext::state_only(Some(media_transport)),
        )
        .await
        .map(|receipt| receipt.transport_session_key.connection_id())
    }

    async fn admit_session(
        self,
        admission: JoinAdmissionTurn<'_, impl FnOnce() -> o_sfu_router::RouterId>,
        context: RoomEffectContext<'_>,
    ) -> Result<CommittedTransportReceipt, RoomJoinError> {
        let commit = self.room.commit_admission(admission, context).await?;
        Ok(self.room.finalize_admission(commit, context).await)
    }

    /// # Errors
    ///
    /// returns [`TransportAdapterError`] when the session or initial offer is absent
    pub async fn make_session_ready(
        self,
        user_id: &UserId,
        media_transport: &MediaTransport,
    ) -> Result<(), TransportAdapterError> {
        let connection_id = self.connection_id(user_id).await?;
        let session_key = self.room.transport_user_key(user_id, connection_id).await;
        media_transport
            .create_initial_session_offer("test-room", &session_key)
            .await?;
        if self
            .mark_session_ready(user_id, sample_client_rtp_capabilities(), media_transport)
            .await
        {
            Ok(())
        } else {
            Err(TransportAdapterError::InvalidInput)
        }
    }

    pub async fn mark_session_ready(
        self,
        user_id: &UserId,
        capabilities: MediaCapabilities,
        media_transport: &MediaTransport,
    ) -> bool {
        let Ok(connection_id) = self.connection_id(user_id).await else {
            return false;
        };
        self.room
            .apply_session_negotiated(user_id, connection_id, capabilities, media_transport)
            .await
            .is_some()
    }

    pub async fn refresh_session(self, user_id: &UserId, media_transport: &MediaTransport) -> bool {
        let Ok(connection_id) = self.connection_id(user_id).await else {
            return false;
        };
        self.room
            .user_operation(user_id, connection_id, media_transport)
            .apply_session_refreshed(&[])
            .await
            .is_some()
    }

    async fn connection_id(self, user_id: &UserId) -> Result<ConnectionId, TransportAdapterError> {
        self.room
            .state
            .read()
            .await
            .user_connection_id(user_id)
            .ok_or(TransportAdapterError::InvalidInput)
    }
}
