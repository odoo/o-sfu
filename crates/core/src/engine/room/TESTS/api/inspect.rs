use super::{super::super::media_graph::ConsumerRouteState, RoomTestApi};
#[cfg(test)]
use crate::engine::source_model::PublishedSourceId;
use crate::engine::{
    ConnectionId, TestSourceKind, UserId, UserInfo,
    media_transport::TransportMediaId,
    source_model::{
        UserStreamId,
        test_support::{source_kind_for_stream_id, stream_id_for_source},
    },
};

impl RoomTestApi<'_> {
    pub async fn session_client_rtp_codec_names(self, user_id: &UserId) -> Option<Vec<String>> {
        self.room
            .state
            .read()
            .await
            .session_client_rtp_codec_names(user_id)
    }

    pub async fn user_connection_id(self, user_id: &UserId) -> Option<ConnectionId> {
        self.room.state.read().await.user_connection_id(user_id)
    }

    pub async fn producer_count(self) -> usize {
        self.room.state.read().await.producer_count()
    }

    pub async fn consumer_count(self) -> usize {
        self.room.state.read().await.consumer_count()
    }

    pub async fn first_published_transport_media_id(self) -> Option<TransportMediaId> {
        self.room
            .state
            .read()
            .await
            .first_published_transport_media_id()
    }

    pub async fn producer_transport_media_id(
        self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: TestSourceKind,
    ) -> Option<TransportMediaId> {
        self.room.state.read().await.producer_transport_media_id(
            user_id,
            connection_id,
            stream_type,
        )
    }

    pub async fn has_published_source(
        self,
        owner_user_id: &UserId,
        owner_connection_id: ConnectionId,
        stream_type: TestSourceKind,
    ) -> bool {
        self.room
            .state
            .read()
            .await
            .published_source_id(
                owner_user_id,
                owner_connection_id,
                &stream_id_for_source(stream_type),
            )
            .is_some()
    }

    pub async fn producer_stream_type_for_transport_media_id(
        self,
        transport_media_id: TransportMediaId,
    ) -> Option<TestSourceKind> {
        self.room
            .state
            .read()
            .await
            .producer_stream_id_for_transport_media_id(transport_media_id)
            .and_then(|stream_id| source_kind_for_stream_id(&stream_id))
    }

    #[cfg(test)]
    pub async fn source_id_for_owner_stream(
        self,
        owner_user_id: &UserId,
        stream_type: TestSourceKind,
    ) -> Option<PublishedSourceId> {
        self.room
            .state
            .read()
            .await
            .source_id_for_owner_stream(owner_user_id, stream_type)
    }

    pub async fn user_info_snapshot(self, user_id: &UserId) -> Option<(UserId, UserInfo)> {
        self.room.state.read().await.user_info_snapshot(user_id)
    }

    pub async fn has_session(self, user_id: &UserId) -> bool {
        self.room.state.read().await.has_session(user_id)
    }

    pub async fn is_stream_published(self, user_id: &UserId, stream_id: &UserStreamId) -> bool {
        self.room
            .state
            .read()
            .await
            .published_source_id_for_user(user_id, stream_id)
            .is_some()
    }

    pub async fn consumer_route_state(
        self,
        consumer_user_id: &UserId,
        producer_user_id: &UserId,
        stream_id: &UserStreamId,
    ) -> Option<ConsumerRouteState> {
        self.room.state.read().await.consumer_route_state(
            consumer_user_id,
            producer_user_id,
            stream_id,
        )
    }

    #[must_use]
    pub fn recording_address(&self) -> Option<&str> {
        self.room.definition.recording_address()
    }

    pub async fn home_media_worker_id(self, user_id: &UserId) -> Option<usize> {
        let state = self.room.state.read().await;
        let connection_id = state.user_connection_id(user_id)?;
        Some(
            state
                .topology
                .router()
                .media_worker_id_for_connection(connection_id)
                .as_usize(),
        )
    }

    pub async fn router_count(self) -> usize {
        self.room.state.read().await.router_count()
    }
}
