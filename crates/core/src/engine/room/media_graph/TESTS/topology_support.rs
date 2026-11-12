use o_sfu_model::UserId;

use super::{super::SubscriptionKey, RoomTopology};
use crate::engine::source_model::{ConsumerSourceSelection, PublishedSourceId};

impl RoomTopology {
    pub(in crate::engine::room) fn source_selection_for_test(
        &self,
        receiver: &UserId,
        source_id: PublishedSourceId,
    ) -> Option<ConsumerSourceSelection> {
        let source = self.sources.source(source_id)?;
        self.route_graph.selection(
            &SubscriptionKey::new(
                receiver,
                source.descriptor.owner().user_id(),
                source.descriptor.stream_id(),
            ),
            source_id,
        )
    }

    pub(in crate::engine::room::media_graph) fn remove_source_for_test(
        &mut self,
        source_id: PublishedSourceId,
    ) -> bool {
        self.remove_source(source_id).is_some()
    }
}
