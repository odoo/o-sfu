//! Two-phase room diagnostics.
//!
//! Capture methods copy room facts and transport keys under the room read guard.
//! Callers collect transport snapshots after releasing that guard, then
//! projection methods combine both views. The result is not an atomic room and
//! transport snapshot.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

use o_sfu_rfc::rtp::Ssrc;
use o_sfu_router::{MediaKind, rtp::MediaFormat};
use o_sfu_telemetry::diagnostics::{
    DiagnosticsActiveSpeaker, DiagnosticsActiveSpeakerReason, DiagnosticsActiveSpeakerState,
    DiagnosticsIncomingBitrate, DiagnosticsMediaKind, DiagnosticsPendingUpgrade,
    DiagnosticsPolicyPauseReason, DiagnosticsPublication, DiagnosticsQualitySummary,
    DiagnosticsRouteState, DiagnosticsSource, DiagnosticsSourceEncoding,
    DiagnosticsSourceSelection, DiagnosticsSourceSelectionReason, DiagnosticsSourceSelector,
    DiagnosticsSubscription, DiagnosticsTransportCounts, DiagnosticsUserSummary,
    DiagnosticsUserTransport, DiagnosticsUserView, DiagnosticsVideoLayoutRole,
    DiagnosticsVideoRoutePriority, DiagnosticsWorkerSummary,
};

use super::{Room, RoomMediaCounts, media_graph::PendingUpgrade, state::RoomState};
use crate::{
    Bitrate,
    engine::{
        ConnectionId, MediaWorkerId, RecordingState, UserId, UserInfo,
        media_transport::{
            ActiveSpeakerActivityReason, ActiveSpeakerActivityState, ActiveSpeakerSourceDiagnostic,
            MediaTransport, TransportBitrateSnapshot, TransportHealthSnapshot, TransportMediaId,
            TransportQualitySample, TransportQualitySnapshot, TransportRidActivity,
            TransportSessionHealth, TransportSessionKey, TransportSourceActivity,
            TransportSourceDiagnosticsSnapshot, TransportSourceKey,
        },
        observability::diagnostics_transport_health,
        source_model::{
            ConsumerSourceSelection, PolicyPauseReason, PublishedSourceDescriptor,
            SourceEncodingDescriptor, SourceEncodingId, SourceRoomPolicySelector,
            SourceRoutePriority, SourceSelector, UserStreamId,
        },
    },
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IncomingBitrateSnapshot {
    pub total: u64,
    pub by_stream: BTreeMap<UserStreamId, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomUserStatsSnapshot {
    pub incoming_bitrate: IncomingBitrateSnapshot,
    pub count: u64,
    pub active_stream_counts: BTreeMap<UserStreamId, u64>,
}

/// Passive facts required by summary and room-list diagnostics.
#[derive(Debug)]
pub struct RoomOverviewCapture {
    /// Current publication and subscription counts.
    pub media_counts: RoomMediaCounts,
    /// Primary media worker assigned to the room.
    pub primary_media_worker_id: Option<MediaWorkerId>,
    /// Current room recording facts.
    pub recording_state: RecordingState,
    /// Active transport sessions in the room.
    pub session_keys: Vec<TransportSessionKey>,
}

impl RoomOverviewCapture {
    #[must_use]
    pub fn transport_counts(&self, health: &TransportHealthSnapshot) -> DiagnosticsTransportCounts {
        let mut counts = DiagnosticsTransportCounts::default();
        for session_key in &self.session_keys {
            let count = match health.get(session_key) {
                Some(TransportSessionHealth::Connected) => &mut counts.connected,
                Some(TransportSessionHealth::Disconnected) => &mut counts.disconnected,
                None => &mut counts.unknown,
            };
            *count = count.saturating_add(1);
        }
        counts.total = self.session_keys.len();
        counts
    }
}

/// Passive room user inventory used by room-user and worker diagnostics.
#[derive(Debug)]
pub struct RoomUsersCapture {
    primary_media_worker_id: Option<MediaWorkerId>,
    users: Vec<CapturedUserSummary>,
}

impl RoomUsersCapture {
    pub fn session_keys(&self) -> impl Iterator<Item = &TransportSessionKey> {
        self.users.iter().map(|user| &user.session_key)
    }

    #[must_use]
    pub fn into_user_summaries(
        self,
        room_id: &str,
        bitrate: &TransportBitrateSnapshot,
        health: &TransportHealthSnapshot,
        stream_ids: [&str; 3],
    ) -> Vec<DiagnosticsUserSummary> {
        let bitrate_by_media = bitrate_by_media(bitrate);
        self.users
            .into_iter()
            .map(|user| user_summary(room_id, user, &bitrate_by_media, health, stream_ids))
            .collect()
    }

    pub fn add_to_worker_summaries(
        self,
        health: &TransportHealthSnapshot,
        workers: &mut BTreeMap<usize, DiagnosticsWorkerSummary>,
    ) {
        if self.users.is_empty() {
            let worker_id = self
                .primary_media_worker_id
                .map_or(0, MediaWorkerId::as_usize);
            let worker = workers
                .entry(worker_id)
                .or_insert_with(|| worker_summary(worker_id));
            worker.room_count = worker.room_count.saturating_add(1);
            return;
        }
        let mut room_workers = BTreeSet::new();
        for user in self.users {
            let worker_id = user.session_key.media_worker_id().as_usize();
            let worker = workers
                .entry(worker_id)
                .or_insert_with(|| worker_summary(worker_id));
            if room_workers.insert(worker_id) {
                worker.room_count = worker.room_count.saturating_add(1);
            }
            worker.user_count = worker.user_count.saturating_add(1);
            worker.publication_count = worker
                .publication_count
                .saturating_add(user.publications.len());
            worker.subscription_count = worker
                .subscription_count
                .saturating_add(user.subscription_count);
            match health.get(&user.session_key) {
                Some(TransportSessionHealth::Connected) => {
                    worker.connected_user_count = worker.connected_user_count.saturating_add(1);
                }
                Some(TransportSessionHealth::Disconnected) => {
                    worker.disconnected_user_count =
                        worker.disconnected_user_count.saturating_add(1);
                }
                None => worker.unknown_user_count = worker.unknown_user_count.saturating_add(1),
            }
        }
    }
}

/// Passive room state required by room detail and graph diagnostics.
#[derive(Debug)]
pub struct RoomDetailCapture {
    overview: RoomOverviewCapture,
    sources: Vec<CapturedSource>,
    users: Vec<CapturedUser>,
}

impl RoomDetailCapture {
    #[must_use]
    pub fn session_keys(&self) -> &[TransportSessionKey] {
        &self.overview.session_keys
    }

    pub fn source_keys(&self) -> impl Iterator<Item = &TransportSourceKey> {
        self.sources.iter().map(|source| &source.source_key)
    }

    #[must_use]
    pub fn into_views(
        self,
        bitrate: &TransportBitrateSnapshot,
        quality: &TransportQualitySnapshot,
        health: &TransportHealthSnapshot,
        source_diagnostics: &TransportSourceDiagnosticsSnapshot,
    ) -> (
        RoomOverviewCapture,
        Vec<DiagnosticsUserView>,
        Vec<DiagnosticsSource>,
    ) {
        let bitrate_by_media = bitrate_by_media(bitrate);
        let users = self
            .users
            .into_iter()
            .map(|user| user_view(user, &bitrate_by_media, quality, health))
            .collect();
        let activity_by_media = source_diagnostics
            .activity
            .iter()
            .map(|activity| (activity.transport_media_id(), activity))
            .collect::<BTreeMap<_, _>>();
        let speaker_diagnostics_by_media = source_diagnostics
            .active_speaker_diagnostics
            .iter()
            .map(|speaker| (speaker.transport_media_id(), *speaker))
            .collect::<BTreeMap<_, _>>();
        let sources = self
            .sources
            .iter()
            .map(|source| {
                source_view(
                    source,
                    &bitrate_by_media,
                    &activity_by_media,
                    &speaker_diagnostics_by_media,
                )
            })
            .collect();
        (self.overview, users, sources)
    }
}

/// Passive room and user facts for one room-scoped user lookup.
#[derive(Debug)]
pub struct RoomUserCapture {
    recording_state: RecordingState,
    user: CapturedUser,
}

impl RoomUserCapture {
    #[must_use]
    pub fn session_key(&self) -> &TransportSessionKey {
        &self.user.session_key
    }

    #[must_use]
    pub fn into_view(
        self,
        bitrate: &TransportBitrateSnapshot,
        quality: &TransportQualitySnapshot,
        health: &TransportHealthSnapshot,
    ) -> (RecordingState, DiagnosticsUserView) {
        let bitrate_by_media = bitrate_by_media(bitrate);
        (
            self.recording_state,
            user_view(self.user, &bitrate_by_media, quality, health),
        )
    }
}

#[derive(Debug)]
struct CapturedUser {
    publications: Vec<DiagnosticsPublication>,
    session_key: TransportSessionKey,
    subscriptions: Vec<DiagnosticsSubscription>,
    user_id: UserId,
    user_info: UserInfo,
    video_soft_pause_remaining_ms: Option<u64>,
}

#[derive(Debug)]
struct CapturedUserSummary {
    publications: Vec<(UserStreamId, TransportMediaId)>,
    session_key: TransportSessionKey,
    subscription_count: usize,
    user_id: UserId,
}

#[derive(Debug)]
struct CapturedSource {
    active: bool,
    descriptor: PublishedSourceDescriptor,
    source_key: TransportSourceKey,
}

impl Room {
    pub(crate) async fn session_stats_snapshot(
        &self,
        transport: &MediaTransport,
    ) -> RoomUserStatsSnapshot {
        let state = self.state.read().await;
        let session_keys = transport_session_keys(&state);
        let transport_snapshot = transport.transport_bitrate_snapshot(&session_keys);
        let mut incoming_bitrate = IncomingBitrateSnapshot {
            total: transport_snapshot.total.as_bps(),
            ..Default::default()
        };
        for (transport_media_id, bits) in transport_snapshot.per_media {
            let Some(stream_id) =
                state.producer_stream_id_for_transport_media_id(transport_media_id)
            else {
                continue;
            };
            let entry = incoming_bitrate.by_stream.entry(stream_id).or_default();
            *entry = entry.saturating_add(bits.as_bps());
        }
        let (count, active_stream_counts) = state.user_stats_counts();
        drop(state);
        RoomUserStatsSnapshot {
            incoming_bitrate,
            count,
            active_stream_counts,
        }
    }

    pub async fn diagnostics_overview_capture(&self) -> RoomOverviewCapture {
        let state = self.state.read().await;
        overview_capture(&state, transport_session_keys(&state))
    }

    pub async fn diagnostics_users_capture(&self) -> RoomUsersCapture {
        let state = self.state.read().await;
        RoomUsersCapture {
            primary_media_worker_id: state.assigned_primary_media_worker_id(),
            users: captured_user_summaries(&state),
        }
    }

    pub async fn diagnostics_detail_capture(&self) -> RoomDetailCapture {
        let state = self.state.read().await;
        let now = Instant::now();
        let users = state
            .transport_user_entries()
            .filter_map(|(user_id, connection_id)| {
                captured_user(&state, user_id.clone(), connection_id, now)
            })
            .collect::<Vec<_>>();
        let session_keys = users.iter().map(|user| user.session_key.clone()).collect();
        let sources = state
            .topology
            .published_sources()
            .map(|source| CapturedSource {
                active: source.active,
                descriptor: source.descriptor.clone(),
                source_key: source.transport.clone(),
            })
            .collect();
        let overview = overview_capture(&state, session_keys);
        drop(state);
        RoomDetailCapture {
            overview,
            sources,
            users,
        }
    }

    pub async fn diagnostics_user_capture(&self, user_key: &str) -> Option<RoomUserCapture> {
        let state = self.state.read().await;
        let now = Instant::now();
        let (user_id, connection_id) = state
            .transport_user_entries()
            .find(|(user_id, _)| user_id.path_segment().as_ref() == user_key)?;
        Some(RoomUserCapture {
            recording_state: state.recording_state(),
            user: captured_user(&state, user_id.clone(), connection_id, now)?,
        })
    }
}

fn overview_capture(
    state: &RoomState,
    session_keys: Vec<TransportSessionKey>,
) -> RoomOverviewCapture {
    RoomOverviewCapture {
        media_counts: state.media_counts(),
        primary_media_worker_id: state.assigned_primary_media_worker_id(),
        recording_state: state.recording_state(),
        session_keys,
    }
}

fn captured_user_summaries(state: &RoomState) -> Vec<CapturedUserSummary> {
    state
        .transport_user_entries()
        .map(|(user_id, connection_id)| CapturedUserSummary {
            publications: state
                .topology
                .published_sources()
                .filter(|publication| {
                    publication.descriptor.owner().user_id() == user_id
                        && publication.transport.session_key().connection_id() == connection_id
                })
                .map(|publication| {
                    (
                        publication.descriptor.stream_id().clone(),
                        publication.transport.transport_media_id(),
                    )
                })
                .collect(),
            session_key: state.transport_user_key(user_id, connection_id),
            subscription_count: state
                .topology
                .committed_consumer_routes_for_user(user_id)
                .filter(|route| route.route.consumer_session_key().connection_id() == connection_id)
                .count()
                .saturating_add(
                    state
                        .topology
                        .pending_consumer_routes_for_user(user_id)
                        .count(),
                ),
            user_id: user_id.clone(),
        })
        .collect()
}

fn captured_user(
    state: &RoomState,
    user_id: UserId,
    connection_id: ConnectionId,
    now: Instant,
) -> Option<CapturedUser> {
    Some(CapturedUser {
        publications: diagnostics_publications(state, &user_id, connection_id),
        session_key: state.transport_user_key(&user_id, connection_id),
        subscriptions: diagnostics_subscriptions(state, &user_id, connection_id, now),
        video_soft_pause_remaining_ms: state
            .user_for_connection(&user_id, connection_id)?
            .video_soft_pause_deadline
            .map(|deadline| duration_millis(deadline.saturating_duration_since(now))),
        user_info: state.user_info_snapshot(&user_id)?.1,
        user_id,
    })
}

fn diagnostics_publications(
    state: &RoomState,
    user_id: &UserId,
    connection_id: ConnectionId,
) -> Vec<DiagnosticsPublication> {
    state
        .topology
        .published_sources()
        .filter(|publication| {
            publication.descriptor.owner().user_id() == user_id
                && publication.transport.session_key().connection_id() == connection_id
        })
        .map(|publication| {
            let source = &publication.descriptor;
            DiagnosticsPublication {
                active: publication.active,
                encoding_ids: source
                    .encodings()
                    .map(|encoding| encoding.encoding_id().as_u64())
                    .collect(),
                media_kind: media_kind(source.media_kind()),
                source_id: source.source_id().as_u64(),
                stream_id: source.stream_id().to_string(),
                transport_media_id: Some(publication.transport.transport_media_id().as_u64()),
            }
        })
        .collect()
}

fn diagnostics_subscriptions(
    state: &RoomState,
    user_id: &UserId,
    connection_id: ConnectionId,
    now: Instant,
) -> Vec<DiagnosticsSubscription> {
    let project = |source: &PublishedSourceDescriptor,
                   route_selection: ConsumerSourceSelection,
                   pending_upgrade: Option<PendingUpgrade>,
                   consumer_media: Option<TransportMediaId>,
                   source_media: TransportMediaId,
                   route_state| {
        let layout = state.diagnostics_video_layout_role(user_id, source);
        DiagnosticsSubscription {
            consumer_transport_media_id: consumer_media.map(TransportMediaId::as_u64),
            layout_priority: layout.map(|role| role.priority().into()),
            layout_role: layout.map(Into::into),
            producer_user_id: source.owner().user_id().clone(),
            selection: selection(source, route_selection, pending_upgrade, now),
            source_id: source.source_id().as_u64(),
            source_transport_media_id: Some(source_media.as_u64()),
            state: route_state,
            stream_id: source.stream_id().to_string(),
        }
    };
    let mut subscriptions = state
        .topology
        .committed_consumer_routes_for_user(user_id)
        .filter(|route| route.route.consumer_session_key().connection_id() == connection_id)
        .map(|route| {
            let source = &route.source.descriptor;
            let route_state = if route.source.active && route.selection.delivery_active() {
                DiagnosticsRouteState::Active
            } else {
                DiagnosticsRouteState::Inactive
            };
            project(
                source,
                route.selection,
                route.pending_upgrade.copied(),
                Some(route.route.consumer_transport_media_id()),
                route.route.source_transport_media_id(),
                route_state,
            )
        })
        .collect::<Vec<_>>();
    subscriptions.extend(
        state
            .topology
            .pending_consumer_routes_for_user(user_id)
            .map(|route| {
                let source = &route.source.descriptor;
                project(
                    source,
                    route.selection,
                    None,
                    None,
                    route.source.transport.transport_media_id(),
                    DiagnosticsRouteState::Pending,
                )
            }),
    );
    subscriptions
}

fn user_summary(
    room_id: &str,
    user: CapturedUserSummary,
    bitrate_by_media: &BTreeMap<u64, u64>,
    health: &TransportHealthSnapshot,
    stream_ids: [&str; 3],
) -> DiagnosticsUserSummary {
    let [audio_stream_id, camera_stream_id, screen_stream_id] = stream_ids;
    let mut audio = 0_u64;
    let mut camera = 0_u64;
    let mut screen = 0_u64;
    let mut total = 0_u64;
    for (stream_id, media_id) in &user.publications {
        let bitrate = bitrate_by_media
            .get(&media_id.as_u64())
            .copied()
            .unwrap_or_default();
        total = total.saturating_add(bitrate);
        let stream = match stream_id.as_str() {
            value if value == audio_stream_id => &mut audio,
            value if value == camera_stream_id => &mut camera,
            value if value == screen_stream_id => &mut screen,
            _ => continue,
        };
        *stream = stream.saturating_add(bitrate);
    }
    DiagnosticsUserSummary {
        audio_incoming_bitrate_bps: audio,
        camera_incoming_bitrate_bps: camera,
        connection_id: user.session_key.connection_id().as_u64(),
        health: health
            .get(&user.session_key)
            .copied()
            .map(diagnostics_transport_health),
        incoming_bitrate_bps: total,
        media_worker_id: user.session_key.media_worker_id().as_usize(),
        publication_count: user.publications.len(),
        room_id: room_id.to_owned(),
        screen_incoming_bitrate_bps: screen,
        subscription_count: user.subscription_count,
        user_key: user.user_id.path_segment().into_owned(),
        user_id: user.user_id,
    }
}

fn worker_summary(media_worker_id: usize) -> DiagnosticsWorkerSummary {
    DiagnosticsWorkerSummary {
        media_worker_id,
        ..Default::default()
    }
}

fn user_view(
    user: CapturedUser,
    bitrate_by_media: &BTreeMap<u64, u64>,
    quality: &TransportQualitySnapshot,
    health: &TransportHealthSnapshot,
) -> DiagnosticsUserView {
    let transport = DiagnosticsUserTransport {
        video_soft_pause_remaining_ms: user.video_soft_pause_remaining_ms,
        connection_id: user.session_key.connection_id().as_u64(),
        health: health
            .get(&user.session_key)
            .copied()
            .map(diagnostics_transport_health),
        media_worker_id: user.session_key.media_worker_id().as_usize(),
        quality_summary: quality_summary(
            incoming_bitrate(&user.publications, bitrate_by_media),
            quality.get(&user.session_key).copied(),
        ),
    };
    DiagnosticsUserView {
        publications: user.publications,
        subscriptions: user.subscriptions,
        transport,
        user_id: user.user_id,
        user_info: user.user_info,
    }
}

fn bitrate_by_media(snapshot: &TransportBitrateSnapshot) -> BTreeMap<u64, u64> {
    snapshot
        .per_media
        .iter()
        .map(|(media, bitrate)| (media.as_u64(), bitrate.as_bps()))
        .collect()
}

fn incoming_bitrate(
    publications: &[DiagnosticsPublication],
    bitrate_by_media: &BTreeMap<u64, u64>,
) -> DiagnosticsIncomingBitrate {
    let mut incoming = DiagnosticsIncomingBitrate::default();
    for publication in publications {
        let bitrate = publication
            .transport_media_id
            .and_then(|media| bitrate_by_media.get(&media))
            .copied()
            .unwrap_or_default();
        incoming.total = incoming.total.saturating_add(bitrate);
        let stream = incoming
            .by_stream_bps
            .entry(publication.stream_id.clone())
            .or_default();
        *stream = stream.saturating_add(bitrate);
    }
    incoming
}

fn quality_summary(
    current_incoming_bitrate: DiagnosticsIncomingBitrate,
    quality: Option<TransportQualitySample>,
) -> DiagnosticsQualitySummary {
    let quality = quality.unwrap_or_default();
    DiagnosticsQualitySummary {
        current_incoming_bitrate,
        sampled_metrics_available: quality.sample_count > 0,
        latest_bwe_bps: quality.latest_bwe_bps,
        rtt_ms: quality.rtt_ms,
        ingress_loss_ppm: quality.ingress_loss_ppm,
        egress_loss_ppm: quality.egress_loss_ppm,
        egress_jitter_rtp_timestamp_units: quality.egress_jitter_rtp_timestamp_units,
        sample_count: quality.sample_count,
    }
}

fn source_view(
    source: &CapturedSource,
    bitrate_by_media: &BTreeMap<u64, u64>,
    activity_by_media: &BTreeMap<TransportMediaId, &TransportSourceActivity>,
    speaker_diagnostics_by_media: &BTreeMap<TransportMediaId, ActiveSpeakerSourceDiagnostic>,
) -> DiagnosticsSource {
    let descriptor = &source.descriptor;
    let media_id = source.source_key.transport_media_id();
    let activity = activity_by_media.get(&media_id).copied();
    DiagnosticsSource {
        active: source.active,
        active_speaker: active_speaker(descriptor, media_id, speaker_diagnostics_by_media),
        current_incoming_bitrate_bps: bitrate_by_media
            .get(&media_id.as_u64())
            .copied()
            .unwrap_or_default(),
        encodings: descriptor
            .encodings()
            .map(|encoding| source_encoding(encoding, activity))
            .collect(),
        last_packet_age_ms: activity.map(|value| duration_millis(value.last_packet_age())),
        last_keyframe_age_ms: activity
            .and_then(TransportSourceActivity::last_keyframe_age)
            .map(duration_millis),
        media_kind: media_kind(descriptor.media_kind()),
        mid: descriptor.mid().map(|mid| mid.as_str().to_owned()),
        owner_user_id: descriptor.owner().user_id().clone(),
        source_id: descriptor.source_id().as_u64(),
        stream_id: descriptor.stream_id().to_string(),
        transport_media_id: Some(media_id.as_u64()),
        video_bitrate_cap_bps: descriptor.policy().video_bitrate_cap().map(Bitrate::as_bps),
    }
}

fn source_encoding(
    encoding: &SourceEncodingDescriptor,
    activity: Option<&TransportSourceActivity>,
) -> DiagnosticsSourceEncoding {
    let format = encoding.negotiated_format();
    let rid_activity = encoding
        .rid()
        .and_then(|rid| rid_activity(activity, rid.as_str()));
    DiagnosticsSourceEncoding {
        codec: format.map(|value| value.codec_name().to_owned()),
        encoding_id: encoding.encoding_id().as_u64(),
        max_bitrate_bps: encoding.max_bitrate().map(Bitrate::as_bps),
        resolution_scale: encoding.resolution_scale(),
        max_framerate: encoding.max_framerate(),
        policy_role: encoding
            .policy_role()
            .map(|role| role.as_wire_value().to_owned()),
        payload_type: format.map(MediaFormat::payload_type),
        primary_ssrc: encoding.primary_ssrc().map(Ssrc::value),
        repair_ssrc: encoding.repair_ssrc().map(Ssrc::value),
        rid: encoding.rid().map(|rid| rid.as_str().to_owned()),
        last_packet_age_ms: rid_activity.map(|value| duration_millis(value.last_packet_age())),
        last_keyframe_age_ms: rid_activity
            .and_then(TransportRidActivity::last_keyframe_age)
            .map(duration_millis),
    }
}

fn rid_activity<'a>(
    activity: Option<&'a TransportSourceActivity>,
    rid: &str,
) -> Option<&'a TransportRidActivity> {
    activity?.rids().iter().find(|value| value.rid() == rid)
}

fn selection(
    source: &PublishedSourceDescriptor,
    selection: ConsumerSourceSelection,
    pending_upgrade: Option<PendingUpgrade>,
    now: Instant,
) -> DiagnosticsSourceSelection {
    let selected_encoding_id = selection.selector().selected_encoding();
    let budget = selection.budget();
    let selected_encoding = selected_encoding_id.and_then(|id| source.encoding(id));
    let (selector, selection_reason) = match selection.selector() {
        SourceSelector::Open => (
            DiagnosticsSourceSelector::Open,
            DiagnosticsSourceSelectionReason::Open,
        ),
        SourceSelector::Encoding(_) => (
            DiagnosticsSourceSelector::Encoding,
            DiagnosticsSourceSelectionReason::ReceiverAdaptation,
        ),
    };
    DiagnosticsSourceSelection {
        active: selection.active(),
        active_video_route_count: budget.active_video_route_count(),
        latest_receiver_bandwidth_estimate_bps: budget
            .latest_receiver_bandwidth()
            .map(Bitrate::as_bps),
        policy_allows_delivery: selection.policy_allows_delivery(),
        policy_pause_reason: selection.policy_pause_reason().map(Into::into),
        pending_upgrade: pending_upgrade.map(|pending| DiagnosticsPendingUpgrade {
            selector: match pending.selector {
                SourceSelector::Open => DiagnosticsSourceSelector::Open,
                SourceSelector::Encoding(_) => DiagnosticsSourceSelector::Encoding,
            },
            encoding_id: pending
                .selector
                .selected_encoding()
                .map(SourceEncodingId::as_u64),
            remaining_ms: duration_millis(pending.deadline.saturating_duration_since(now)),
        }),
        selection_reason,
        selector,
        selected_estimated_bitrate_bps: selected_encoding
            .and_then(SourceEncodingDescriptor::max_bitrate)
            .map(Bitrate::as_bps),
        selected_video_bitrate_bps: budget.selected_video_bitrate().as_bps(),
        selected_video_budget_bps: budget.selected_video_budget().map(Bitrate::as_bps),
        selected_encoding_id: selected_encoding_id.map(SourceEncodingId::as_u64),
        selected_rid: selected_encoding
            .and_then(SourceEncodingDescriptor::rid)
            .map(|rid| rid.as_str().to_owned()),
    }
}

fn active_speaker(
    source: &PublishedSourceDescriptor,
    media_id: TransportMediaId,
    diagnostics: &BTreeMap<TransportMediaId, ActiveSpeakerSourceDiagnostic>,
) -> Option<DiagnosticsActiveSpeaker> {
    (source.media_kind() == MediaKind::Audio).then(|| {
        diagnostics
            .get(&media_id)
            .copied()
            .map_or_else(DiagnosticsActiveSpeaker::idle, active_speaker_snapshot)
    })
}

fn media_kind(value: MediaKind) -> DiagnosticsMediaKind {
    match value {
        MediaKind::Audio => DiagnosticsMediaKind::Audio,
        MediaKind::Video => DiagnosticsMediaKind::Video,
    }
}

fn active_speaker_snapshot(diagnostic: ActiveSpeakerSourceDiagnostic) -> DiagnosticsActiveSpeaker {
    DiagnosticsActiveSpeaker {
        state: diagnostic.state().into(),
        reason: diagnostic.reason().into(),
        last_audio_level_dbov: diagnostic.last_audio_level_dbov(),
        confidence_observations: diagnostic.confidence_observations(),
        hold_remaining_ms: diagnostic.hold_remaining().map(duration_millis),
    }
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn transport_session_keys(state: &RoomState) -> Vec<TransportSessionKey> {
    state
        .transport_user_entries()
        .map(|(user_id, connection_id)| state.transport_user_key(user_id, connection_id))
        .collect()
}

impl From<PolicyPauseReason> for DiagnosticsPolicyPauseReason {
    fn from(value: PolicyPauseReason) -> Self {
        match value {
            PolicyPauseReason::BudgetPressure => Self::BudgetPressure,
            PolicyPauseReason::HiddenTile => Self::HiddenTile,
            PolicyPauseReason::OverflowTile => Self::OverflowTile,
            PolicyPauseReason::MissingUsableLayer => Self::MissingUsableLayer,
            PolicyPauseReason::AudioSpeakerLimit => Self::AudioSpeakerLimit,
            PolicyPauseReason::ReceiverDeafened => Self::ReceiverDeafened,
            PolicyPauseReason::VideoDownloadLimit => Self::VideoDownloadLimit,
            PolicyPauseReason::SourceBitrateLimit => Self::SourceBitrateLimit,
        }
    }
}

impl From<SourceRoomPolicySelector> for DiagnosticsVideoLayoutRole {
    fn from(value: SourceRoomPolicySelector) -> Self {
        match value {
            SourceRoomPolicySelector::Pinned => Self::Pinned,
            SourceRoomPolicySelector::Featured => Self::Featured,
            SourceRoomPolicySelector::ReadableDetail => Self::ReadableDetail,
            SourceRoomPolicySelector::ActiveSpeaker => Self::ActiveSpeaker,
            SourceRoomPolicySelector::VisibleThumbnail => Self::VisibleThumbnail,
            SourceRoomPolicySelector::Hidden => Self::Hidden,
            SourceRoomPolicySelector::Overflow => Self::Overflow,
        }
    }
}

impl From<SourceRoutePriority> for DiagnosticsVideoRoutePriority {
    fn from(value: SourceRoutePriority) -> Self {
        match value {
            SourceRoutePriority::PinnedOrFeatured => Self::PinnedOrFeatured,
            SourceRoutePriority::ReadableDetail => Self::ReadableDetail,
            SourceRoutePriority::ActiveSpeaker => Self::ActiveSpeaker,
            SourceRoutePriority::VisibleThumbnail => Self::VisibleThumbnail,
            SourceRoutePriority::HiddenOrOverflow => Self::HiddenOrOverflow,
        }
    }
}

impl From<ActiveSpeakerActivityState> for DiagnosticsActiveSpeakerState {
    fn from(value: ActiveSpeakerActivityState) -> Self {
        match value {
            ActiveSpeakerActivityState::Active => Self::Active,
            ActiveSpeakerActivityState::Idle => Self::Idle,
            ActiveSpeakerActivityState::Blocked => Self::Blocked,
            ActiveSpeakerActivityState::RecentlyExpired => Self::RecentlyExpired,
        }
    }
}

impl From<ActiveSpeakerActivityReason> for DiagnosticsActiveSpeakerReason {
    fn from(value: ActiveSpeakerActivityReason) -> Self {
        match value {
            ActiveSpeakerActivityReason::Vad => Self::Vad,
            ActiveSpeakerActivityReason::AudioLevel => Self::AudioLevel,
            ActiveSpeakerActivityReason::AudioLevelWarmup => Self::AudioLevelWarmup,
            ActiveSpeakerActivityReason::VadFalse => Self::VadFalse,
            ActiveSpeakerActivityReason::LowNoise => Self::LowNoise,
            ActiveSpeakerActivityReason::BelowSpeechThreshold => Self::BelowSpeechThreshold,
            ActiveSpeakerActivityReason::MissingAudioMetadata => Self::MissingAudioMetadata,
            ActiveSpeakerActivityReason::Expired => Self::Expired,
            ActiveSpeakerActivityReason::NoMetadata => Self::NoMetadata,
        }
    }
}

#[cfg(test)]
#[path = "TESTS/read_model.rs"]
mod tests;
