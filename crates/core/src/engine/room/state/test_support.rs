use super::shared::RoomState;
#[cfg(test)]
use crate::engine::source_model::{PublishedSourceId, SourceEncodingDescriptor, SourceEncodingId};
use crate::engine::{
    ConnectionId, TestSourceKind, UserId, media_transport::TransportMediaId,
    source_model::test_support::stream_id_for_source,
};

impl RoomState {
    pub fn session_client_rtp_codec_names(&self, user_id: &UserId) -> Option<Vec<String>> {
        self.users
            .get(user_id)
            .and_then(|user| user.parsed_client_rtp_capabilities.as_ref())
            .map(|capabilities| {
                capabilities
                    .codecs()
                    .map(|codec| codec.codec_name().to_owned())
                    .collect()
            })
    }

    pub fn producer_count(&self) -> usize {
        self.media_counts().publications
    }

    pub fn consumer_count(&self) -> usize {
        self.topology.consumer_count()
    }

    pub fn has_session(&self, user_id: &UserId) -> bool {
        self.users.contains_key(user_id)
    }

    pub fn router_count(&self) -> usize {
        self.topology.router().router_count()
    }

    pub fn first_published_transport_media_id(&self) -> Option<TransportMediaId> {
        self.topology.first_published_transport_media_id()
    }

    pub fn producer_transport_media_id(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        stream_type: TestSourceKind,
    ) -> Option<TransportMediaId> {
        self.topology.producer_transport_media_id(
            user_id,
            connection_id,
            &stream_id_for_source(stream_type),
        )
    }

    #[cfg(test)]
    pub fn inspect_source_id_for_transport_media_id(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<PublishedSourceId> {
        self.topology
            .source_for_transport_media(transport_media_id)
            .map(|source| source.descriptor.source_id())
    }

    #[cfg(test)]
    pub fn source_id_for_owner_stream(
        &self,
        owner_user_id: &UserId,
        stream_type: TestSourceKind,
    ) -> Option<PublishedSourceId> {
        self.topology
            .source_id_for_owner_stream(owner_user_id, &stream_id_for_source(stream_type))
    }

    #[cfg(test)]
    pub fn inspect_source_encoding_ids_for_transport_media_id(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<Vec<SourceEncodingId>> {
        self.topology
            .source_for_transport_media(transport_media_id)
            .map(|source| {
                source
                    .descriptor
                    .encodings()
                    .map(SourceEncodingDescriptor::encoding_id)
                    .collect()
            })
    }
}
