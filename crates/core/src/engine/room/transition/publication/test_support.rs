use o_sfu_model::UserId;
use o_sfu_router::rtp::MediaStream as RouterRtpParameters;

#[cfg(test)]
use super::PublishStageOutcome;
use super::{RoomUserOperation, StagedPublish};
#[cfg(test)]
use crate::engine::{media_transport::TransportAdapterError, source_model::SourcePublishIntent};
#[cfg(test)]
use crate::engine::{
    media_transport::TransportMediaId, source_model::test_support::TestSourceKind,
};
use crate::{
    ConnectionId,
    engine::{room::Room, source_model::UserStreamId},
};

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

#[cfg(test)]
impl RoomUserOperation<'_> {
    pub async fn stage_negotiated_publish(
        self,
        intent: &SourcePublishIntent,
    ) -> Result<PublishStageOutcome, TransportAdapterError> {
        let Some(validated_descriptor) = ({
            let state = self.room.state.read().await;
            state.validate_publish(self.user_id, self.connection_id, intent)
        }) else {
            return Ok(PublishStageOutcome::Rejected);
        };
        self.stage_validated_publish(validated_descriptor).await
    }
}

impl Room {
    #[must_use]
    pub async fn has_staged_publish(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_id: &UserStreamId,
    ) -> bool {
        self.state
            .read()
            .await
            .staged_publishes
            .contains(user_id, connection_id, stream_id)
    }
}
