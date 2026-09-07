use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet},
};

use o_sfu_router::MediaKind;

use super::action::FeaturedUserUpdate;
use crate::{
    Bitrate, RoomMediaLimits, VideoAdaptationTuning,
    engine::{
        ConnectionId, UserId,
        media_transport::{
            ActiveSpeakerSource, ReceiverBandwidthSnapshot, TransportBitrateSnapshot,
            TransportMediaId,
        },
        room::{
            media_graph::ConsumerRouteView,
            state::{ActiveUser, RoomState},
        },
    },
};

const ACTIVE_SPEAKER_FEATURED_CLEAR_LIMIT: usize = 5;

#[derive(Debug)]
pub(super) struct SourcePolicySnapshot<'a> {
    pub(super) routes: Vec<ConsumerRouteView<'a>>,
    pub(super) receiver_bandwidth_by_connection: BTreeMap<ConnectionId, Bitrate>,
    pub(super) source_bitrate_by_media: BTreeMap<TransportMediaId, Bitrate>,
    pub(super) limited_audio_media_ids: BTreeSet<TransportMediaId>,
    pub(super) deaf_receiver_connection_ids: BTreeSet<ConnectionId>,
    pub(super) featured_source_user_ids: BTreeSet<UserId>,
    pub(super) active_speaker_rank_by_user: BTreeMap<UserId, usize>,
    pub(super) featured_user_updates: Vec<FeaturedUserUpdate>,
    pub(super) user_count: usize,
    pub(super) media_limits: RoomMediaLimits,
    pub(super) video_adaptation_tuning: VideoAdaptationTuning,
    pub(super) audio_reserve_by_connection: BTreeMap<ConnectionId, Bitrate>,
}

impl<'a> SourcePolicySnapshot<'a> {
    pub(super) fn from_state(
        room: &'a RoomState,
        active_speaker_sources: &[ActiveSpeakerSource],
        receiver_bandwidth_snapshot: &ReceiverBandwidthSnapshot,
        source_bitrate_snapshot: &TransportBitrateSnapshot,
    ) -> Self {
        let ranked_sources = rank_room_active_speakers(room, active_speaker_sources);
        let media_limits = room.media_limits;
        let tuning = room.video_adaptation_tuning;
        let admitted_audio_speakers = admitted_audio_media_ids(
            room,
            &ranked_sources,
            media_limits.max_active_audio_speakers(),
        );
        let deaf_receiver_connection_ids = deaf_receiver_connection_ids(room);
        let mut featured_source_user_ids = BTreeSet::new();
        let mut active_speaker_rank_by_user = BTreeMap::new();
        let mut desired_featured_user_id = None;
        for (index, user_id) in ranked_sources
            .iter()
            .filter_map(|source| {
                room.topology
                    .active_speaker_detector_owner(source.transport_media_id())
            })
            .enumerate()
        {
            if index == 0 {
                desired_featured_user_id = Some(user_id.clone());
            }
            // The clear limit counts eligible sources before owner deduplication.
            if index < ACTIVE_SPEAKER_FEATURED_CLEAR_LIMIT {
                featured_source_user_ids.insert(user_id.clone());
            }
            let next_rank = active_speaker_rank_by_user.len();
            active_speaker_rank_by_user
                .entry(user_id)
                .or_insert(next_rank);
        }
        let featured_user_updates = featured_user_updates(room, desired_featured_user_id.as_ref());
        // Include policy-paused routes so later turns can resume them. Filtering
        // on `delivery_active()` would make a policy pause self-perpetuating.
        let routes = room
            .committed_consumer_routes()
            .filter(|route| route.source.active && route.selection.active())
            .collect::<Vec<_>>();
        let audio_reserve_by_connection = audio_reserve_by_connection(
            &routes,
            &admitted_audio_speakers,
            &deaf_receiver_connection_ids,
            tuning.audio_reserve_per_speaker,
        );
        Self {
            routes,
            receiver_bandwidth_by_connection: receiver_bandwidth_by_connection(
                receiver_bandwidth_snapshot,
            ),
            source_bitrate_by_media: source_bitrate_snapshot.per_media.iter().copied().collect(),
            limited_audio_media_ids: ranked_sources
                .iter()
                .map(|source| source.transport_media_id())
                .filter(|media_id| !admitted_audio_speakers.contains(media_id))
                .collect(),
            deaf_receiver_connection_ids,
            featured_source_user_ids,
            active_speaker_rank_by_user,
            featured_user_updates,
            user_count: room.user_count(),
            media_limits,
            video_adaptation_tuning: tuning,
            audio_reserve_by_connection,
        }
    }
}

/// Bandwidth reserved for admitted audio before video budgeting, per receiver
/// connection.
///
/// Each receiver reserves `per_speaker` for every admitted audio route it
/// actually consumes, so a receiver that disabled audio, deafened itself (or a
/// publisher with no consumer routes) reserves nothing and keeps its full video
/// budget. The reserve is fixed per route, so it is deterministic and
/// independent of policy-turn cadence. A zero per-speaker rate disables the
/// reservation and returns an empty map.
fn audio_reserve_by_connection(
    routes: &[ConsumerRouteView<'_>],
    admitted_audio_media_ids: &BTreeSet<TransportMediaId>,
    deaf_receiver_connection_ids: &BTreeSet<ConnectionId>,
    per_speaker: Bitrate,
) -> BTreeMap<ConnectionId, Bitrate> {
    if per_speaker.as_bps() == 0 {
        return BTreeMap::new();
    }
    let mut reserve_by_connection = BTreeMap::new();
    for route in routes {
        if route.source.descriptor.media_kind() != MediaKind::Audio
            || !admitted_audio_media_ids.contains(&route.route.source_transport_media_id())
        {
            continue;
        }
        let connection_id = route.route.consumer_session_key().connection_id();
        if deaf_receiver_connection_ids.contains(&connection_id) {
            continue;
        }
        let reserve = reserve_by_connection
            .entry(connection_id)
            .or_insert_with(Bitrate::zero);
        *reserve = reserve.saturating_add(per_speaker);
    }
    reserve_by_connection
}

fn receiver_bandwidth_by_connection(
    snapshot: &ReceiverBandwidthSnapshot,
) -> BTreeMap<ConnectionId, Bitrate> {
    snapshot
        .per_session
        .iter()
        .map(|(session, estimate)| (session.connection_id(), *estimate))
        .collect()
}

/// Filters and ranks a list of active speaker sources for a room.
///
/// **Ranking Criteria:**
/// 1. **Recency:** Most recently active first (highest `observed_at`).
/// 2. **Loudness:** Highest audio level first (`last_audio_level_dbov`).
/// 3. **Tie-breaker:** Transport media ID.
///
/// Only retains sources that are active and present in the current room topology.
fn rank_room_active_speakers(
    room: &RoomState,
    sources: &[ActiveSpeakerSource],
) -> Vec<ActiveSpeakerSource> {
    let mut sources = sources.to_vec();
    sources.retain(|source| {
        room.topology
            .source_for_transport_media(source.transport_media_id())
            .is_some_and(|source| source.active)
    });
    sources.sort_unstable_by_key(|source| {
        (
            Reverse(source.observed_at()),
            Reverse(source.last_audio_level_dbov().unwrap_or(i8::MIN)),
            source.transport_media_id().as_u64(),
        )
    });
    sources
}

fn user_for_source<'a>(
    room: &'a RoomState,
    source: &ActiveSpeakerSource,
) -> Option<&'a ActiveUser> {
    room.topology
        .source_for_transport_media(source.transport_media_id())
        .and_then(|published_source| {
            room.users
                .get(published_source.descriptor.owner().user_id())
        })
}

/// Takes audio media IDs from `sources` up to the provided `limit`,
/// prioritizing participants who are currently screen sharing.
fn admitted_audio_media_ids(
    room: &RoomState,
    sources: &[ActiveSpeakerSource],
    limit: usize,
) -> BTreeSet<TransportMediaId> {
    let mut admitted = BTreeSet::new();
    let mut deferred = Vec::with_capacity(limit);
    for source in sources {
        if admitted.len() == limit {
            break;
        }
        let media_id = source.transport_media_id();
        // prioritize participants who are currently screen sharing
        if user_for_source(room, source).is_some_and(ActiveUser::is_screensharing) {
            admitted.insert(media_id);
        } else if deferred.len() < limit - admitted.len() {
            deferred.push(media_id);
        }
    }
    admitted.extend(deferred.into_iter().take(limit - admitted.len()));
    admitted
}

fn deaf_receiver_connection_ids(room: &RoomState) -> BTreeSet<ConnectionId> {
    room.users
        .values()
        .filter(|user| user.is_deaf())
        .map(|user| user.connection_id)
        .collect()
}

fn featured_user_updates(
    room: &RoomState,
    desired_featured_user_id: Option<&UserId>,
) -> Vec<FeaturedUserUpdate> {
    if desired_featured_user_id.is_none()
        && !room.users.values().any(|user| user.featured().is_some())
    {
        return Vec::new();
    }
    room.users
        .iter()
        .filter_map(|(user_id, user)| {
            let current_featured = user.featured();
            let desired_featured = match desired_featured_user_id {
                Some(featured_user_id) => Some(featured_user_id == user_id),
                None if current_featured.is_some() => Some(false),
                None => None,
            };
            (desired_featured != current_featured).then(|| FeaturedUserUpdate {
                user_id: user_id.clone(),
                connection_id: user.connection_id,
                featured: desired_featured,
            })
        })
        .collect()
}
