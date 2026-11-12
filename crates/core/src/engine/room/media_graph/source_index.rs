use std::collections::{BTreeMap, BTreeSet};

use o_sfu_model::UserId;

use super::{PublishedSource, SourceKey, remove_from_index_set};
#[cfg(any(test, feature = "testing-transport"))]
use crate::ConnectionId;
use crate::engine::{
    media_transport::TransportMediaId,
    source_model::{
        ActiveSpeakerGroup, ActiveSpeakerSourceRole, PublishedSourceId, SourceEncodingId,
        UserStreamId,
    },
};

#[derive(Debug)]
pub(super) struct PublishedSources {
    records: BTreeMap<PublishedSourceId, PublishedSource>,
    id_by_key: BTreeMap<SourceKey, PublishedSourceId>,
    ids_by_owner: BTreeMap<UserId, BTreeSet<PublishedSourceId>>,
    id_by_transport: BTreeMap<TransportMediaId, PublishedSourceId>,
    next_id: u64,
    next_encoding_id: u64,
}

impl Default for PublishedSources {
    fn default() -> Self {
        Self {
            records: BTreeMap::new(),
            id_by_key: BTreeMap::new(),
            ids_by_owner: BTreeMap::new(),
            id_by_transport: BTreeMap::new(),
            next_id: 1,
            next_encoding_id: 1,
        }
    }
}

impl PublishedSources {
    pub(super) fn allocate_id(&mut self) -> PublishedSourceId {
        PublishedSourceId::allocate(&mut self.next_id)
    }

    pub(super) fn allocate_encoding_id(&mut self) -> SourceEncodingId {
        SourceEncodingId::allocate(&mut self.next_encoding_id)
    }

    pub(super) fn publication_count(&self) -> usize {
        self.records.len()
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &PublishedSource> {
        self.records.values()
    }

    pub(super) fn source(&self, id: PublishedSourceId) -> Option<&PublishedSource> {
        self.records.get(&id)
    }

    pub(super) fn source_mut(&mut self, id: PublishedSourceId) -> Option<&mut PublishedSource> {
        self.records.get_mut(&id)
    }

    pub(super) fn source_for_transport(&self, media: TransportMediaId) -> Option<&PublishedSource> {
        self.source(*self.id_by_transport.get(&media)?)
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) fn first_transport_media_id(&self) -> Option<TransportMediaId> {
        self.records
            .values()
            .next()
            .map(|source| source.transport.transport_media_id())
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) fn transport_media_id(
        &self,
        user: &UserId,
        connection: ConnectionId,
        stream: &UserStreamId,
    ) -> Option<TransportMediaId> {
        let source = self.source(*self.id_by_key.get(&SourceKey::new(user, stream))?)?;
        (source.transport.session_key().connection_id() == connection)
            .then(|| source.transport.transport_media_id())
    }

    pub(super) fn id_for_owner_stream(
        &self,
        owner: &UserId,
        stream: &UserStreamId,
    ) -> Option<PublishedSourceId> {
        self.id_by_key.get(&SourceKey::new(owner, stream)).copied()
    }

    pub(super) fn owner_has_promotable_source_in_group(
        &self,
        owner: &UserId,
        group: ActiveSpeakerGroup,
    ) -> bool {
        self.ids_for_owner(owner)
            .filter_map(|id| self.source(id))
            .filter(|source| source.active)
            .map(|source| &source.descriptor)
            .any(|source| {
                source.policy().active_speaker().is_some_and(|policy| {
                    policy.group() == group && policy.role() == ActiveSpeakerSourceRole::Promotable
                })
            })
    }

    pub(super) fn insert(&mut self, source: PublishedSource) {
        let id = source.descriptor.source_id();
        let owner = source.descriptor.owner().user_id().clone();
        self.id_by_key
            .insert(SourceKey::new(&owner, source.descriptor.stream_id()), id);
        self.ids_by_owner.entry(owner).or_default().insert(id);
        self.id_by_transport
            .insert(source.transport.transport_media_id(), id);
        self.records.insert(id, source);
    }

    pub(super) fn ids_for_owner(
        &self,
        user: &UserId,
    ) -> impl Iterator<Item = PublishedSourceId> + '_ {
        self.ids_by_owner
            .get(user)
            .into_iter()
            .flat_map(|ids| ids.iter().copied())
    }

    pub(super) fn remove(&mut self, id: PublishedSourceId) -> Option<PublishedSource> {
        let source = self.records.remove(&id)?;
        let owner = source.descriptor.owner().user_id();
        self.id_by_key
            .remove(&SourceKey::new(owner, source.descriptor.stream_id()));
        remove_from_index_set(&mut self.ids_by_owner, owner, &id);
        self.id_by_transport
            .remove(&source.transport.transport_media_id());
        Some(source)
    }
}
