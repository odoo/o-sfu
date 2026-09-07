use std::collections::{BTreeMap, BTreeSet};

use o_sfu_router::MediaKind;

use super::super::input::SourcePolicySnapshot;
use crate::{
    Bitrate,
    engine::{
        UserId, VideoLayoutIntent,
        media_transport::TransportConsumerRoute,
        room::{
            media_graph::{PendingUpgrade, SubscriptionKey},
            state::RoomState,
        },
        source_model::{
            ConsumerSourceSelection, PublishedSourceDescriptor, SourceAdaptationPolicy,
            SourceRoomPolicySelector,
        },
    },
};

pub(super) fn receiver_video_routes<'a>(
    state: &RoomState,
    input: &SourcePolicySnapshot<'a>,
) -> Vec<ReceiverVideoRouteInput<'a>> {
    let mut visible_scalable_route_counts = BTreeMap::new();
    let mut routes = Vec::with_capacity(input.routes.len());
    for route in &input.routes {
        let source = &route.source.descriptor;
        if source.media_kind() != MediaKind::Video
            || (source.policy().adaptation() == SourceAdaptationPolicy::None
                && source.policy().video_bitrate_cap().is_none())
        {
            continue;
        }
        let layout_role = state.receiver_video_layout_role(
            &route.key.receiver,
            source,
            &input.featured_source_user_ids,
        );
        if source.policy().adaptation() == SourceAdaptationPolicy::ScalableVideo
            && layout_role.counts_toward_visible_budget()
        {
            *visible_scalable_route_counts
                .entry(route.key.receiver.clone())
                .or_default() += 1;
        }
        routes.push(ReceiverVideoRouteInput {
            user_count: input.user_count,
            source,
            key: route.key,
            route: route.route,
            current_selection: route.selection,
            pending_upgrade: route.pending_upgrade,
            layout_role,
            visible_scalable_route_count: 1,
            active_speaker_rank: input
                .active_speaker_rank_by_user
                .get(source.owner().user_id())
                .copied(),
            receiver_bandwidth: input
                .receiver_bandwidth_by_connection
                .get(&route.route.consumer_session_key().connection_id())
                .copied(),
            source_bitrate: input
                .source_bitrate_by_media
                .get(&route.route.source_transport_media_id())
                .copied(),
            audio_budget_reserve: input
                .audio_reserve_by_connection
                .get(&route.route.consumer_session_key().connection_id())
                .copied()
                .unwrap_or_else(Bitrate::zero),
        });
    }
    for route in &mut routes {
        route.visible_scalable_route_count = visible_scalable_route_counts
            .get(&route.key.receiver)
            .copied()
            .unwrap_or(1);
    }
    routes
}

#[derive(Debug)]
pub(super) struct ReceiverVideoRouteInput<'a> {
    pub(super) user_count: usize,
    pub(super) source: &'a PublishedSourceDescriptor,
    pub(super) key: &'a SubscriptionKey,
    pub(super) route: &'a TransportConsumerRoute,
    pub(super) current_selection: ConsumerSourceSelection,
    pub(super) pending_upgrade: Option<&'a PendingUpgrade>,
    pub(super) layout_role: SourceRoomPolicySelector,
    pub(super) visible_scalable_route_count: usize,
    pub(super) active_speaker_rank: Option<usize>,
    pub(super) receiver_bandwidth: Option<Bitrate>,
    pub(super) source_bitrate: Option<Bitrate>,
    pub(super) audio_budget_reserve: Bitrate,
}

impl RoomState {
    #[must_use]
    pub(in crate::engine::room) fn receiver_video_layout_role(
        &self,
        consumer_user_id: &UserId,
        source: &PublishedSourceDescriptor,
        active_speaker_source_user_ids: &BTreeSet<UserId>,
    ) -> SourceRoomPolicySelector {
        let preference = layout_preference(self, consumer_user_id, source);
        source
            .policy()
            .layout()
            .map_or(SourceRoomPolicySelector::Hidden, |policy| {
                policy.resolve(
                    preference,
                    active_speaker_source_user_ids.contains(source.owner().user_id()),
                )
            })
    }

    #[must_use]
    pub(in crate::engine::room) fn diagnostics_video_layout_role(
        &self,
        consumer_user_id: &UserId,
        source: &PublishedSourceDescriptor,
    ) -> Option<SourceRoomPolicySelector> {
        let policy = source.policy().layout()?;
        let preference = layout_preference(self, consumer_user_id, source);
        let active_speaker = self
            .users
            .get(source.owner().user_id())
            .is_some_and(|user| user.featured() == Some(true));
        Some(policy.resolve(preference, active_speaker))
    }
}

fn layout_preference(
    state: &RoomState,
    consumer_user_id: &UserId,
    source: &PublishedSourceDescriptor,
) -> Option<VideoLayoutIntent> {
    state
        .topology
        .subscription_intent(&SubscriptionKey::new(
            consumer_user_id,
            source.owner().user_id(),
            source.stream_id(),
        ))
        .layout()
}
