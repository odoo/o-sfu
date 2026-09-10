use o_sfu_router::rtp::MediaStream as RouterRtpParameters;

#[cfg(test)]
use super::Room;
use super::{RoomUserOperation, StagedPublish};
use crate::engine::source_model::UserStreamId;
#[cfg(test)]
use crate::engine::{ConnectionId, UserId};
#[cfg(test)]
use crate::engine::{TestSourceKind, media_transport::TransportMediaId};

impl StagedPublish {
    pub(crate) async fn commit_with_parameters(
        self,
        operation: RoomUserOperation<'_>,
        rtp: RouterRtpParameters,
    ) -> Option<UserStreamId> {
        let guard = operation.room.lock_source_policy().await;
        self.commit_rtp_guarded(&guard, operation.media_transport, rtp, &[])
            .await
    }
}

#[cfg(test)]
impl Room {
    pub(crate) async fn staged_count(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> usize {
        self.state
            .read()
            .await
            .staged_publishes
            .staged_count(user_id, connection_id)
    }

    pub(crate) async fn staged_media_id(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: TestSourceKind,
    ) -> Option<TransportMediaId> {
        self.state.read().await.staged_publishes.staged_media_id(
            user_id,
            connection_id,
            stream_type,
        )
    }
}
