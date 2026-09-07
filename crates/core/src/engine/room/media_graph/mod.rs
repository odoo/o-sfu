use std::collections::{BTreeMap, BTreeSet};

use o_sfu_router::{ConsumerId, MediaKind, rtp, topology::RoutedProducerId};

use crate::engine::{
    UserId,
    media_transport::{
        SourceActivityRevision, TransportConsumerRoute, TransportMediaId, TransportSourceKey,
    },
    source_model::{ConsumerSourceSelection, PublishedSourceDescriptor, UserStreamId},
};

mod consumer_setup;
mod producer;
mod route_graph;
mod source_index;
mod subscription;
mod topology;

#[cfg(test)]
#[expect(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;
#[cfg(test)]
#[path = "TESTS/route_graph.rs"]
mod route_graph_tests;

#[cfg(any(test, feature = "testing-transport"))]
pub use self::subscription::ConsumerRouteState;
pub(super) use self::{
    consumer_setup::{
        CommittedConsumerSetup, ConsumerSetupOrigin, ConsumerSetupOutcome, ConsumerSetupTarget,
        DeclaredConsumerSetup, PendingConsumerSetup,
    },
    producer::{ProducerActivityCommit, PublishCommit, PublishIntentPlan, ValidatedPublish},
    route_graph::PendingUpgrade,
    subscription::{ReceiverRouteActivity, ReceiverRouteCommit, ReceiverRouteWork},
    topology::{
        CommittedTransportReceipt, RoomTopology, SessionPlacementCommit, SessionPlacementRejection,
    },
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct SubscriptionKey {
    pub receiver: UserId,
    pub publisher: UserId,
    pub stream: UserStreamId,
}

impl SubscriptionKey {
    pub fn new(receiver: &UserId, publisher: &UserId, stream: &UserStreamId) -> Self {
        Self {
            receiver: receiver.clone(),
            publisher: publisher.clone(),
            stream: stream.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct SourceKey {
    owner_user_id: UserId,
    stream_id: UserStreamId,
}

#[derive(Debug)]
pub(super) struct PublishedSource {
    pub descriptor: PublishedSourceDescriptor,
    pub transport: TransportSourceKey,
    pub rtp: rtp::MediaStream,
    pub routed: RoutedProducerId,
    pub active: bool,
    pub activity_revision: SourceActivityRevision,
}

#[derive(Debug, Clone)]
pub(super) struct ConsumerRouteView<'a> {
    pub key: &'a SubscriptionKey,
    pub route: &'a TransportConsumerRoute,
    pub mid: &'a str,
    /// Borrow holds so policy snapshots do not copy deadlines for every route.
    pub pending_upgrade: Option<&'a PendingUpgrade>,
    pub source: &'a PublishedSource,
    pub selection: ConsumerSourceSelection,
}

impl ConsumerRouteView<'_> {
    pub fn target(&self) -> ConsumerRouteTarget {
        ConsumerRouteTarget::new(
            self.route.clone(),
            self.key.stream.clone(),
            self.source.descriptor.media_kind(),
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct PendingConsumerRouteView<'a> {
    pub source: &'a PublishedSource,
    pub selection: ConsumerSourceSelection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerRouteTarget {
    transport_route: TransportConsumerRoute,
    stream_id: UserStreamId,
    kind: MediaKind,
}

impl ConsumerRouteTarget {
    fn new(
        transport_route: TransportConsumerRoute,
        stream_id: UserStreamId,
        kind: MediaKind,
    ) -> Self {
        Self {
            transport_route,
            stream_id,
            kind,
        }
    }

    pub const fn transport_route(&self) -> &TransportConsumerRoute {
        &self.transport_route
    }

    pub const fn consumer_media_id(&self) -> TransportMediaId {
        self.transport_route.consumer_transport_media_id()
    }

    pub fn producer_user_id(&self) -> &UserId {
        self.transport_route.source_session_key().user_id()
    }

    pub const fn source_media_id(&self) -> TransportMediaId {
        self.transport_route.source_transport_media_id()
    }

    pub fn stream_id(&self) -> &UserStreamId {
        &self.stream_id
    }

    pub fn request_keyframe_after_activity(&self, active: bool) -> bool {
        active && self.kind == MediaKind::Video
    }
}

impl SourceKey {
    pub fn new(owner_user_id: &UserId, stream_id: &UserStreamId) -> Self {
        Self {
            owner_user_id: owner_user_id.clone(),
            stream_id: stream_id.clone(),
        }
    }
}

fn remove_from_index_set<K, V>(index: &mut BTreeMap<K, BTreeSet<V>>, key: &K, value: &V)
where
    K: Ord,
    V: Ord,
{
    let Some(values) = index.get_mut(key) else {
        return;
    };
    values.remove(value);
    if values.is_empty() {
        index.remove(key);
    }
}
