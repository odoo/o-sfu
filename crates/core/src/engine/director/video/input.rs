use std::collections::BTreeSet;

use o_sfu_model::{UserId, VideoLayoutIntent};
use o_sfu_router::MediaKind;

use super::{super::input::SourcePolicySnapshot, solver::VideoAdmissionRank};
use crate::{
    Bitrate,
    engine::{
        media_transport::TransportConsumerRoute,
        room::{PendingUpgrade, RoomState, SubscriptionKey},
        source_model::{
            ConsumerSourceSelection, PublishedSourceDescriptor, PublishedSourceId,
            SourceAdaptationPolicy, SourceRoomPolicySelector, SourceRoutePriority,
        },
    },
};

pub(super) fn receiver_video_routes<'a>(
    state: &RoomState,
    input: &SourcePolicySnapshot<'a>,
) -> Vec<ReceiverVideoRouteInput<'a>> {
    let mut routes = Vec::with_capacity(input.routes.len());
    for route in &input.routes {
        let source = &route.source.descriptor;
        if source.media_kind() != MediaKind::Video
            || (source.policy().adaptation() == SourceAdaptationPolicy::None
                && source.policy().video_bitrate_cap().is_none())
        {
            continue;
        }
        let layout_role = receiver_video_layout_role(
            state,
            &route.key.receiver,
            source,
            &input.featured_source_user_ids,
        );
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
    for receiver_routes in
        routes.chunk_by_mut(|left, right| left.key.receiver == right.key.receiver)
    {
        let visible_scalable_route_count = receiver_routes
            .iter()
            .filter(|route| {
                route.source.policy().adaptation() == SourceAdaptationPolicy::ScalableVideo
                    && route.layout_role.counts_toward_visible_budget()
            })
            .count()
            .max(1);
        for route in receiver_routes {
            route.visible_scalable_route_count = visible_scalable_route_count;
        }
    }
    routes
}

/// Video route with adaptation or a source bitrate cap.
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

/// Orders consumer setups before their first policy turn.
///
/// Setup has no active-speaker snapshot, so video ranks by layout role alone
/// and the next policy turn applies the speaker order. Audio shares the
/// priority of pinned or featured video and the source id breaks their tie.
#[must_use]
pub(in crate::engine) fn initial_video_admission_rank(
    state: &RoomState,
    receiver: &UserId,
    kind: MediaKind,
    source_id: PublishedSourceId,
) -> VideoAdmissionRank {
    if kind != MediaKind::Video {
        return VideoAdmissionRank::new(SourceRoutePriority::PinnedOrFeatured, None, source_id);
    }
    let Some(source) = state.topology.source_descriptor(source_id) else {
        return VideoAdmissionRank::new(SourceRoutePriority::HiddenOrOverflow, None, source_id);
    };
    VideoAdmissionRank::new(
        receiver_video_layout_role(state, receiver, source, &BTreeSet::new()).priority(),
        None,
        source_id,
    )
}

/// Layout role shown in diagnostics. The committed featured flag stands in for
/// the speaker observations of a policy turn.
#[must_use]
pub(in crate::engine) fn diagnostics_video_layout_role(
    state: &RoomState,
    receiver: &UserId,
    source: &PublishedSourceDescriptor,
) -> Option<SourceRoomPolicySelector> {
    let policy = source.policy().layout()?;
    let preference = layout_preference(state, receiver, source);
    let active_speaker = state
        .users
        .get(source.owner().user_id())
        .is_some_and(|user| user.featured() == Some(true));
    Some(policy.resolve(preference, active_speaker))
}

fn receiver_video_layout_role(
    state: &RoomState,
    receiver: &UserId,
    source: &PublishedSourceDescriptor,
    active_speaker_source_user_ids: &BTreeSet<UserId>,
) -> SourceRoomPolicySelector {
    let preference = layout_preference(state, receiver, source);
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

fn layout_preference(
    state: &RoomState,
    receiver: &UserId,
    source: &PublishedSourceDescriptor,
) -> Option<VideoLayoutIntent> {
    state
        .topology
        .subscription_intent(&SubscriptionKey::new(
            receiver,
            source.owner().user_id(),
            source.stream_id(),
        ))
        .layout()
}
