use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Instant,
};

use o_sfu_router::rtp::{MediaCapabilities, MediaCapabilities as RouterRtpCapabilities};

use super::super::{
    RoomAdmissionPolicy, RoomMediaCounts, RoomUserPermissions,
    media_graph::{ConsumerRouteView, RoomTopology},
    outbound::{
        OutboundSender, RemoteTrackProjection, RemoteTrackSnapshot, VersionedRemoteTrackSnapshot,
    },
    transition::StagedPublishes,
};
use crate::{
    RoomMediaLimits, VideoAdaptationTuning,
    engine::{
        ConnectionId, MediaWorkerId, PeerSnapshot, RecordingState, UserId, UserInfo,
        media_transport::TransportSessionKey, room::placement::PlacementSnapshot,
        source_model::UserStreamId,
    },
};

#[derive(Debug)]
pub struct RoomState {
    pub(super) admission_policy: RoomAdmissionPolicy,
    pub media_limits: RoomMediaLimits,
    pub video_adaptation_tuning: VideoAdaptationTuning,
    pub users: BTreeMap<UserId, ActiveUser>,
    /// rejects stale async callbacks from previous connections
    pub(super) next_connection_id: u64,
    pub next_consumer_id: u64,
    next_track_snapshot_revision: u64,
    pub(super) recording_state: RecordingState,
    pub(in crate::engine::room) staged_publishes: StagedPublishes,
    pub(in crate::engine::room) topology: RoomTopology,
}

#[derive(Debug)]
pub struct ActiveUser {
    pub(super) user_id: Arc<UserId>,
    pub(super) permissions: RoomUserPermissions,
    pub(super) info: UserInfo,
    pub(super) server_featured: Option<bool>,
    pub parsed_client_rtp_capabilities: Option<RouterRtpCapabilities>,
    pub connection_id: ConnectionId,
    /// Continuous receiver pressure survives victim changes within this connection.
    pub(in crate::engine::room) video_soft_pause_deadline: Option<Instant>,
    pub sender: OutboundSender,
}

impl ActiveUser {
    pub(super) fn reset_presentation(&mut self) {
        self.info = UserInfo::default();
        self.server_featured = None;
    }

    pub(super) fn apply_info_update(&mut self, info: &UserInfo) {
        self.info.apply_partial_update(info);
    }

    pub(in crate::engine::room) const fn featured(&self) -> Option<bool> {
        self.server_featured
    }

    pub(in crate::engine::room) const fn is_deaf(&self) -> bool {
        matches!(self.info.is_deaf, Some(true))
    }

    pub(in crate::engine::room) const fn is_screensharing(&self) -> bool {
        matches!(self.info.is_screen_sharing_on, Some(true))
    }

    pub(in crate::engine::room) fn set_featured(&mut self, featured: Option<bool>) {
        self.server_featured = featured;
    }

    pub(super) fn project_info(&self) -> UserInfo {
        self.info
            .clone()
            .with_featured(self.server_featured)
            .snapshot_complete()
    }
}

impl RoomState {
    pub fn new(
        runtime_context: &super::super::RoomRuntimeContext,
        admission_policy: RoomAdmissionPolicy,
        media_limits: RoomMediaLimits,
        video_adaptation_tuning: VideoAdaptationTuning,
        router_rtp_capabilities: MediaCapabilities,
    ) -> Self {
        Self {
            admission_policy,
            media_limits,
            video_adaptation_tuning,
            users: BTreeMap::new(),
            next_connection_id: 0,
            next_consumer_id: 1,
            next_track_snapshot_revision: 1,
            recording_state: RecordingState {
                recording: Some(false),
                audio: Some(false),
                transcription: Some(false),
                video: Some(false),
            },
            staged_publishes: StagedPublishes::default(),
            topology: RoomTopology::new(runtime_context, router_rtp_capabilities),
        }
    }

    pub fn user_for_connection(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Option<&ActiveUser> {
        let user = self.users.get(user_id)?;
        if user.connection_id != connection_id {
            return None;
        }
        Some(user)
    }

    pub fn user_mut_for_connection(
        &mut self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Option<&mut ActiveUser> {
        let user = self.users.get_mut(user_id)?;
        if user.connection_id != connection_id {
            return None;
        }
        Some(user)
    }

    pub fn recording_state(&self) -> RecordingState {
        self.recording_state.clone()
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn router_rtp_capabilities(&self) -> MediaCapabilities {
        self.topology.router().rtp_capabilities().clone()
    }

    pub fn transport_user_entries(&self) -> impl Iterator<Item = (&UserId, ConnectionId)> {
        self.users
            .iter()
            .map(|(user_id, user)| (user_id, user.connection_id))
    }

    /// Returns the transport key for an exact committed router placement.
    ///
    /// Use [`Self::committed_transport_user_key`] when placement may be stale.
    ///
    /// # Panics
    ///
    /// Panics when `user_id` and `connection_id` have no committed router
    /// placement.
    pub fn transport_user_key(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> TransportSessionKey {
        if let Some(user) = self.user_for_connection(user_id, connection_id) {
            return self
                .topology
                .transport_user_key(Arc::clone(&user.user_id), connection_id);
        }
        self.topology
            .transport_user_key(user_id.clone(), connection_id)
    }

    /// Returns `None` unless the exact router placement remains committed.
    pub fn committed_transport_user_key(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
    ) -> Option<TransportSessionKey> {
        if let Some(user) = self.user_for_connection(user_id, connection_id) {
            return self
                .topology
                .committed_transport_user_key(Arc::clone(&user.user_id), connection_id);
        }
        self.topology
            .committed_transport_user_key(user_id.clone(), connection_id)
    }

    pub fn placement_usage_snapshot(&self) -> PlacementSnapshot {
        self.topology.router().placement_snapshot()
    }

    pub fn assigned_primary_media_worker_id(&self) -> Option<MediaWorkerId> {
        self.topology.router().primary_worker()
    }

    pub(in crate::engine::room) fn committed_consumer_routes(
        &self,
    ) -> impl Iterator<Item = ConsumerRouteView<'_>> {
        self.topology.committed_consumer_routes().filter(|route| {
            self.user_connection_id(&route.key.receiver)
                == Some(route.route.consumer_session_key().connection_id())
        })
    }

    pub fn user_connection_id(&self, user_id: &UserId) -> Option<ConnectionId> {
        self.users.get(user_id).map(|user| user.connection_id)
    }

    pub fn user_count(&self) -> usize {
        self.users.len()
    }

    pub fn user_snapshots_except(&self, excluded_user_id: &UserId) -> Vec<PeerSnapshot> {
        self.users
            .iter()
            .filter(|(user_id, _session)| *user_id != excluded_user_id)
            .map(|(user_id, user)| PeerSnapshot {
                user_id: user_id.clone(),
                info: user.project_info(),
            })
            .collect()
    }

    pub fn user_info_snapshot(&self, user_id: &UserId) -> Option<(UserId, UserInfo)> {
        let user = self.users.get(user_id)?;
        Some((user_id.clone(), user.project_info()))
    }

    pub(in crate::engine::room) fn remote_track_snapshot_for_user(
        &mut self,
        user_id: &UserId,
        requires_negotiation: bool,
    ) -> VersionedRemoteTrackSnapshot {
        let revision = self.next_track_snapshot_revision;
        self.next_track_snapshot_revision = revision.saturating_add(1);
        let connection_id = self.user_connection_id(user_id);
        let snapshot = RemoteTrackSnapshot {
            tracks: self
                .topology
                .committed_consumer_routes_for_user(user_id)
                .filter(|route| {
                    connection_id == Some(route.route.consumer_session_key().connection_id())
                })
                .map(|route| {
                    let source = &route.source.descriptor;
                    RemoteTrackProjection {
                        consumer_mid: route.mid.to_owned(),
                        user_id: source.owner().user_id().clone(),
                        stream_id: source.stream_id().clone(),
                        producer_active: route.source.active,
                    }
                })
                .collect(),
            requires_negotiation,
        };
        VersionedRemoteTrackSnapshot { snapshot, revision }
    }

    pub(in crate::engine::room) fn remote_track_snapshots_for_users(
        &mut self,
        user_ids: BTreeSet<UserId>,
        requires_negotiation: bool,
    ) -> Vec<(OutboundSender, VersionedRemoteTrackSnapshot)> {
        user_ids
            .into_iter()
            .filter_map(|user_id| {
                Some((
                    self.users.get(&user_id)?.sender.clone(),
                    self.remote_track_snapshot_for_user(&user_id, requires_negotiation),
                ))
            })
            .collect()
    }

    pub fn user_stats_counts(&self) -> (u64, BTreeMap<UserStreamId, u64>) {
        (
            u64::try_from(self.users.len()).unwrap_or(u64::MAX),
            self.topology.active_stream_user_counts(),
        )
    }

    pub fn media_counts(&self) -> RoomMediaCounts {
        self.topology.media_counts()
    }

    pub fn is_empty(&self) -> bool {
        self.users.is_empty()
    }
}
