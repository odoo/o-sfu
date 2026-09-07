use std::collections::BTreeMap;

use o_sfu_router::{
    negotiation::derive_consumable_rtp_parameters, rtp::MediaStream as RouterRtpParameters,
};
use tracing::warn;

use super::{
    super::super::{DeactivateIntentOutcome, transition::StagedPublish},
    RoomTestApi,
};
use crate::engine::{
    ConnectionId, TestSourceKind, UserId,
    media_transport::{MediaTransport, TransportMediaId},
    source_model::{
        SourceDeactivateIntent, SourcePublishIntent, SourceSubscriptionIntent, UserStreamId,
        test_support::source_publish_intent_for_source,
    },
};

#[derive(Debug, Clone)]
pub struct NegotiatedPublish {
    pub connection_id: ConnectionId,
    pub stream_type: TestSourceKind,
    pub transport_media_id: TransportMediaId,
    pub consumable_rtp_parameters: RouterRtpParameters,
}

impl RoomTestApi<'_> {
    pub async fn publish_negotiated_track(
        self,
        user_id: &UserId,
        publish: NegotiatedPublish,
        media_transport: &MediaTransport,
    ) -> Option<UserStreamId> {
        let intent = source_publish_intent_for_source(publish.stream_type);
        let validated_descriptor = {
            let state = self.room.state.read().await;
            state.validate_publish(user_id, publish.connection_id, &intent)?
        };
        StagedPublish::new(validated_descriptor, publish.transport_media_id)
            .commit_with_parameters(
                self.room
                    .user_operation(user_id, publish.connection_id, media_transport),
                publish.consumable_rtp_parameters,
            )
            .await
    }

    pub async fn publish_track(
        self,
        user_id: &UserId,
        stream_type: TestSourceKind,
        producer_rtp_parameters: RouterRtpParameters,
        media_transport: &MediaTransport,
    ) -> Option<UserStreamId> {
        let intent = source_publish_intent_for_source(stream_type);
        self.publish_intent(user_id, &intent, producer_rtp_parameters, media_transport)
            .await
    }

    pub async fn publish_intent(
        self,
        user_id: &UserId,
        intent: &SourcePublishIntent,
        producer_rtp_parameters: RouterRtpParameters,
        media_transport: &MediaTransport,
    ) -> Option<UserStreamId> {
        let (publisher_connection_id, capabilities) = {
            let state = self.room.state.read().await;
            let user = state.users.get(user_id)?;
            user.parsed_client_rtp_capabilities.as_ref()?;
            (user.connection_id, state.router_rtp_capabilities())
        };
        let consumable_rtp_parameters =
            derive_consumable_rtp_parameters(&producer_rtp_parameters, &capabilities)
                .map_err(|error| {
                    warn!(
                        ?user_id,
                        ?error,
                        "failed to derive consumable RTP parameters for producer"
                    );
                })
                .ok()?;
        let validated_descriptor = {
            let state = self.room.state.read().await;
            state.validate_publish(user_id, publisher_connection_id, intent)?
        };
        let session_key = self
            .room
            .transport_user_key(user_id, publisher_connection_id)
            .await;
        let transport_media_id = match media_transport
            .publish_media(&session_key, intent.media_kind(), &producer_rtp_parameters)
            .await
        {
            Ok(id) => id,
            Err(_error) => {
                warn!(
                    ?user_id,
                    connection_id = ?publisher_connection_id,
                    stream_id = %intent.stream_id(),
                    "media transport rejected publish media declaration"
                );
                return None;
            }
        };
        StagedPublish::new(validated_descriptor, transport_media_id)
            .commit_with_parameters(
                self.room
                    .user_operation(user_id, publisher_connection_id, media_transport),
                consumable_rtp_parameters,
            )
            .await
    }

    pub async fn deactivate_publication(
        self,
        user_id: &UserId,
        stream_id: &UserStreamId,
        media_transport: &MediaTransport,
    ) -> bool {
        let Some(connection_id) = self.user_connection_id(user_id).await else {
            return false;
        };
        self.room
            .user_operation(user_id, connection_id, media_transport)
            .deactivate_publication(&SourceDeactivateIntent::new(stream_id.clone()))
            .await
            != DeactivateIntentOutcome::Noop
    }

    pub async fn update_subscription(
        self,
        receiver_id: &UserId,
        source_id: &UserId,
        intents: &BTreeMap<UserStreamId, SourceSubscriptionIntent>,
        media_transport: &MediaTransport,
    ) -> bool {
        let Some(connection_id) = self.user_connection_id(receiver_id).await else {
            return false;
        };
        self.room
            .user_operation(receiver_id, connection_id, media_transport)
            .apply_receiver_intent(source_id, intents)
            .await
            .is_some()
    }
}
