//! Current-room registry and lifecycle coordinator.
//!
//! [`RoomManager`] publishes one current room per issuer, admits WebSocket
//! sessions and runs background room work. Lease admission holds the directory
//! read guard to prove currentness and releases it before room work begins.
//! Empty-room removal waits for every accepted lease to finish.

#[cfg(any(test, feature = "testing-transport"))]
use std::sync::Mutex;
use std::{collections::BTreeSet, future::Future, sync::Arc, time::Duration};

use o_sfu_telemetry::schema::event as telemetry_event;
use tokio::sync::RwLock;
use tracing::{info, warn};

#[cfg(any(test, feature = "testing-transport"))]
pub use super::placement::JoinPlacementTestGate;
use super::{
    Room, RoomConfig, RoomJoinError, RoomManagerJoinError, RoomRuntimePolicy,
    RoomUserStatsSnapshot,
    directory::{RoomDirectory, RoomDirectoryEntry, RoomLifecycleLease},
    effects::batch::RoomEffectContext,
    factory::RoomFactory,
    membership::JoinUserRequest,
    placement::JoinAdmissionTurn,
    source_policy::SourcePolicyTurn,
};
use crate::engine::{
    ConnectionId, RoomInstanceId, UserId,
    media_transport::{MediaTransport, TransportSessionKey},
    metrics::{RoomGaugeValues, RuntimeMetrics},
    room::instance::RoomManagerServeError,
};

#[cfg(any(test, feature = "testing-transport"))]
#[path = "TESTS/support.rs"]
mod test_support;

/// operator-facing room stats assembled from directory and transport snapshots
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeRoomStatsSnapshot {
    /// room publication timestamp in RFC 3339 UTC format
    pub create_date: String,
    /// room uuid returned by `/v1/channel`
    pub uuid: String,
    /// first create request address or `unknown` when unavailable
    pub remote_address: String,
    /// live user, stream and bitrate stats read after the directory snapshot
    pub users_stats: RoomUserStatsSnapshot,
    /// room creation flag exposed by `/v1/stats`
    pub web_rtc_enabled: bool,
}

/// committed admission result returned after join-side effects run
#[derive(Debug, Clone)]
pub struct RoomUserAdmission {
    /// current room that accepted the session
    pub room: Arc<Room>,
    /// room-local connection id assigned to the admitted user
    pub connection_id: ConnectionId,
    /// transport key used by [`crate::prelude::SfuCore`] to build
    /// [`crate::prelude::MediaSession`]
    pub transport_session_key: TransportSessionKey,
}

/// current room directory row used by diagnostics views
#[derive(Debug, Clone)]
pub struct RuntimeRoomDirectorySnapshot {
    /// current room for this directory row
    pub room: Arc<Room>,
    /// room publication timestamp in RFC 3339 UTC format
    pub create_date: String,
    /// first create request address or `unknown` when unavailable
    pub remote_address: String,
}

fn retrieve_room_reservation(
    directory: &RoomDirectory,
    issuer: &str,
    key: &str,
    config: &RoomConfig,
) -> Result<Option<Arc<Room>>, RoomManagerServeError> {
    let Some(entry) = directory.entry_by_issuer(issuer) else {
        return Ok(None);
    };
    if !entry.room.definition.matches_reservation(key, config) {
        warn!(
            event = telemetry_event::ROOM_RESERVATION_CONFLICT,
            room_id = entry.room.uuid(),
            issuer,
            config = ?config,
            "conflicting reservation request"
        );
        return Err(RoomManagerServeError::ConflictingReservation);
    }
    entry.lifecycle.renew_reservation();
    Ok(Some(Arc::clone(&entry.room)))
}

/// Coordinates current room admission and lifecycle.
#[derive(Debug)]
pub struct RoomManager {
    directory: RwLock<RoomDirectory>,
    factory: RoomFactory,
    reservation_ttl: Duration,
    #[cfg(any(test, feature = "testing-transport"))]
    join_placement_gate: Mutex<Option<Arc<JoinPlacementTestGate>>>,
}

impl RoomManager {
    /// builds a room manager with an empty directory
    ///
    #[must_use]
    pub fn new(
        runtime_policy: RoomRuntimePolicy,
        metrics: Arc<RuntimeMetrics>,
        reservation_ttl: Duration,
    ) -> Self {
        let factory = RoomFactory::new(runtime_policy, metrics);
        Self {
            directory: RwLock::new(RoomDirectory::default()),
            factory,
            reservation_ttl,
            #[cfg(any(test, feature = "testing-transport"))]
            join_placement_gate: Mutex::new(None),
        }
    }

    /// Returns the current room for `issuer` or publishes a new reservation.
    ///
    /// The first reservation fixes `key`, `config` and `remote_address`.
    /// Matching requests return the same room and renew an outstanding
    /// reservation without rearming one retired by a successful join.
    ///
    /// # Errors
    ///
    /// Returns [`RoomManagerServeError::ConflictingReservation`] when the
    /// current room has a different `key` or `config`.
    pub async fn serve_room(
        &self,
        issuer: &str,
        key: &str,
        config: &RoomConfig,
        remote_address: Option<&str>,
    ) -> Result<Arc<Room>, RoomManagerServeError> {
        {
            let directory = self.directory.read().await;
            if let Some(room) = retrieve_room_reservation(&directory, issuer, key, config)? {
                return Ok(room);
            }
        }
        let mut directory = self.directory.write().await;
        if let Some(room) = retrieve_room_reservation(&directory, issuer, key, config)? {
            return Ok(room);
        }
        let room = self.factory.create(issuer, key, config);
        directory.insert(Arc::clone(&room), remote_address, self.reservation_ttl);
        drop(directory);
        info!(
            event = telemetry_event::ROOM_CREATED,
            room_id = room.uuid(),
            remote_address = remote_address.unwrap_or("unknown"),
            web_rtc_enabled = config.web_rtc_enabled,
            "room created"
        );
        Ok(room)
    }

    /// returns the current room for a public room uuid
    pub async fn get_by_uuid(&self, uuid: &str) -> Option<Arc<Room>> {
        let directory = self.directory.read().await;
        directory.get_by_uuid(uuid)
    }

    /// builds `/v1/stats` rows from one directory snapshot
    ///
    /// the directory lock is released before transport stats are read, so the
    /// returned rows are best-effort runtime observations rather than a global
    /// transaction across room and media state
    pub async fn stats_snapshots(
        &self,
        media_transport: &MediaTransport,
    ) -> Vec<RuntimeRoomStatsSnapshot> {
        let entries = self.directory_entries().await;
        let mut snapshots = Vec::with_capacity(entries.len());
        for entry in entries {
            snapshots.push(self.entry_stats_snapshot(entry, media_transport).await);
        }
        snapshots
    }

    /// returns current directory rows for room diagnostics
    pub async fn directory_snapshots(&self) -> Vec<RuntimeRoomDirectorySnapshot> {
        self.directory_entries()
            .await
            .into_iter()
            .map(|entry| RuntimeRoomDirectorySnapshot {
                room: entry.room,
                create_date: entry.create_date,
                remote_address: entry.remote_address,
            })
            .collect()
    }

    /// Returns counts from rooms in one directory snapshot.
    ///
    /// Room states are read sequentially after the directory lock is released.
    /// Removed rooms may contribute once. New rooms appear on the next call.
    pub async fn room_gauges(&self) -> RoomGaugeValues {
        let rooms = self.directory.read().await.rooms();
        let mut gauges = RoomGaugeValues {
            rooms: rooms.len(),
            ..RoomGaugeValues::default()
        };
        for room in rooms {
            let state = room.state.read().await;
            let media = state.media_counts();
            gauges.users = gauges.users.saturating_add(state.user_count());
            gauges.publications = gauges.publications.saturating_add(media.publications);
            gauges.subscriptions = gauges.subscriptions.saturating_add(media.subscriptions);
            gauges.recording_rooms = gauges
                .recording_rooms
                .saturating_add(usize::from(state.recording_state().recording == Some(true)));
        }
        gauges
    }

    /// returns one current directory row for room diagnostics
    pub async fn directory_snapshot(&self, room_id: &str) -> Option<RuntimeRoomDirectorySnapshot> {
        let entry = self.entry(room_id).await?;
        Some(RuntimeRoomDirectorySnapshot {
            room: entry.room,
            create_date: entry.create_date,
            remote_address: entry.remote_address,
        })
    }

    /// recalculates packet-selection policy for rooms dirtied by media activity
    ///
    /// empty input is a no-op. rooms that left the current directory before the
    /// drain are skipped. committed route work is executed after each policy
    /// plan so transport routing and accepted selector state stay in sync
    pub async fn sync_source_packet_selection_policies_for_runtime_ids(
        &self,
        room_instance_ids: &BTreeSet<RoomInstanceId>,
        media_transport: &MediaTransport,
    ) {
        if room_instance_ids.is_empty() {
            return;
        }
        let rooms = self
            .directory_entries_for_instance_ids(room_instance_ids)
            .await;
        if rooms.is_empty() {
            return;
        }
        let active_speaker_sources = media_transport.active_speaker_source_snapshot().await;
        for room in rooms {
            SourcePolicyTurn::packet_selection()
                .execute(&room, Some(media_transport), Some(&active_speaker_sources))
                .await;
        }
    }

    /// Admits one WebSocket connection into a current room.
    ///
    /// Returns after join-side room effects complete.
    ///
    /// # Errors
    ///
    /// Returns [`RoomManagerJoinError::MissingRoom`] when `room_id` is not
    /// current. Returns [`RoomManagerJoinError::RoomFull`] when a new user
    /// exceeds room capacity. Returns [`RoomManagerJoinError::RouterState`] when
    /// router placement cannot commit.
    pub async fn join_user(
        &self,
        room_id: &str,
        request: JoinUserRequest,
        media_transport: &MediaTransport,
    ) -> Result<RoomUserAdmission, RoomManagerJoinError> {
        let mutation = self
            .begin_current_room_mutation(room_id)
            .await
            .ok_or(RoomManagerJoinError::MissingRoom)?;
        let room = Arc::clone(&mutation.room);

        let admission = JoinAdmissionTurn::from_factory(request, media_transport, &self.factory);
        #[cfg(any(test, feature = "testing-transport"))]
        let admission = admission.with_gate(self.join_placement_gate_for_test());
        let join_commit = match room
            .commit_admission(admission, RoomEffectContext::runtime(media_transport))
            .await
        {
            Ok(commit) => commit,
            Err(err) => {
                self.finish_session_mutation(room_id, mutation, false).await;
                return Err(match err {
                    RoomJoinError::RoomFull => RoomManagerJoinError::RoomFull,
                    RoomJoinError::RouterState => RoomManagerJoinError::RouterState,
                });
            }
        };
        // Retire the reservation only after membership commits. Failed admission
        // must leave its deadline intact for the reaper.
        mutation.lease.clear_expiration();
        let receipt = room
            .finalize_admission(join_commit, RoomEffectContext::runtime(media_transport))
            .await;

        self.finish_session_mutation(room_id, mutation, false).await;
        Ok(RoomUserAdmission {
            room,
            connection_id: receipt.transport_session_key.connection_id(),
            transport_session_key: receipt.transport_session_key,
        })
    }

    /// closes one room connection and then re-checks empty-room removal
    ///
    /// returns `false` when the room is missing or the connection was not
    /// removed by this call. the empty current room can still be removed after
    /// stale or already-completed teardown
    pub async fn close_session(
        &self,
        room_id: &str,
        user_id: &UserId,
        connection_id: ConnectionId,
        media_transport: &MediaTransport,
    ) -> bool {
        let Some(did_remove_active_session) = self
            .run_current_room_mutation(
                room_id,
                |room| async move {
                    room.remove_user(user_id, connection_id, media_transport)
                        .await
                },
                true,
            )
            .await
        else {
            return false;
        };
        did_remove_active_session
    }

    /// disconnects selected users from a current room and removes it if empty
    ///
    /// missing rooms are ignored because the caller's disconnect intent is
    /// already satisfied
    pub async fn disconnect_users(
        &self,
        room_id: &str,
        user_ids: &[UserId],
        media_transport: &MediaTransport,
    ) {
        let _ = self
            .run_current_room_mutation(
                room_id,
                |room| async move {
                    room.disconnect_users(user_ids, media_transport).await;
                },
                true,
            )
            .await;
    }

    /// Claims and removes expired reservations with no active room mutations.
    pub async fn check_expired_room_reservations(&self) {
        let mut directory = self.directory.write().await;
        for entry in directory.entries() {
            if entry.lifecycle.claim_expired_reservation() {
                directory.remove_if_current(entry.room.uuid(), &entry.room);
                info!(
                    event = telemetry_event::ROOM_RESERVATION_EXPIRED,
                    room_id = entry.room.uuid(),
                    "room reservation expired"
                );
            }
        }
        drop(directory);
    }

    #[cfg(test)]
    pub(super) async fn with_current_room<T, F, Fut>(&self, room_id: &str, action: F) -> Option<T>
    where
        F: FnOnce(Arc<Room>) -> Fut,
        Fut: Future<Output = T>,
    {
        self.run_current_room_mutation(room_id, action, false).await
    }

    async fn run_current_room_mutation<T, F, Fut>(
        &self,
        room_id: &str,
        action: F,
        remove_if_empty: bool,
    ) -> Option<T>
    where
        F: FnOnce(Arc<Room>) -> Fut,
        Fut: Future<Output = T>,
    {
        let mutation = self.begin_current_room_mutation(room_id).await?;
        let output = action(Arc::clone(&mutation.room)).await;
        self.finish_session_mutation(room_id, mutation, remove_if_empty)
            .await;
        Some(output)
    }

    async fn finish_session_mutation(
        &self,
        room_id: &str,
        mutation: CurrentRoomMutation,
        remove_if_empty: bool,
    ) {
        let CurrentRoomMutation { room, lease } = mutation;
        // Read emptiness after this mutation. A later accepted mutation can
        // supply the final removal proof. Dropping the final lease supplies no
        // proof and cancels the pending claim.
        if lease.finish(remove_if_empty, room.is_empty().await) {
            self.directory
                .write()
                .await
                .remove_if_current(room_id, &room);
        }
    }

    /// Accepts work while the directory read guard proves the row is current.
    ///
    /// The returned lease protects subsequent room work without retaining the
    /// directory guard.
    async fn begin_current_room_mutation(&self, room_id: &str) -> Option<CurrentRoomMutation> {
        let directory = self.directory.read().await;
        let entry = directory.entry(room_id)?;
        let lease = entry.lifecycle.begin()?;
        let room = Arc::clone(&entry.room);
        drop(directory);
        Some(CurrentRoomMutation { room, lease })
    }

    async fn entry(&self, room_id: &str) -> Option<RoomDirectoryEntry> {
        let directory = self.directory.read().await;
        directory.entry(room_id).cloned()
    }

    async fn directory_entries(&self) -> Vec<RoomDirectoryEntry> {
        let directory = self.directory.read().await;
        directory.entries()
    }

    async fn directory_entries_for_instance_ids(
        &self,
        room_instance_ids: &BTreeSet<RoomInstanceId>,
    ) -> Vec<Arc<Room>> {
        let directory = self.directory.read().await;
        room_instance_ids
            .iter()
            .filter_map(|room_instance_id| directory.get_by_instance_id(*room_instance_id))
            .collect()
    }

    async fn entry_stats_snapshot(
        &self,
        entry: RoomDirectoryEntry,
        media_transport: &MediaTransport,
    ) -> RuntimeRoomStatsSnapshot {
        let room = entry.room;
        let users_stats = room.session_stats_snapshot(media_transport).await;
        RuntimeRoomStatsSnapshot {
            create_date: entry.create_date,
            uuid: room.uuid().to_owned(),
            remote_address: entry.remote_address,
            users_stats,
            web_rtc_enabled: room.web_rtc_enabled(),
        }
    }
}

/// lease plus room pointer accepted from the current directory row
///
/// dropping the lease without [`RoomManager::finish_session_mutation`] releases
/// admission but never removes the room
#[derive(Debug)]
struct CurrentRoomMutation {
    room: Arc<Room>,
    lease: RoomLifecycleLease,
}
