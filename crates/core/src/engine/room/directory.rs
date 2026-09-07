//! Current-room indexes and lifecycle leases.
//!
//! [`RoomDirectory`] indexes one current room by UUID, issuer and instance ID.
//! Each entry shares a [`RoomLifecycle`] gate. Accepted leases defer empty-room
//! removal until the final mutation finishes. Reservation expiry removes only
//! idle entries claimed by that gate.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::time::Instant;

use super::Room;
use crate::engine::{RoomInstanceId, sync::lock_unpoisoned};

const UNKNOWN_REMOTE_ADDRESS: &str = "unknown";

fn rfc3339_now() -> String {
    match OffsetDateTime::now_utc().format(&Rfc3339) {
        Ok(timestamp) => timestamp,
        Err(_error) => String::from("1970-01-01T00:00:00Z"),
    }
}

/// directory row for one current room instance
///
/// Cloned entries share the lifecycle gate. Manager admission holds the directory
/// read guard until that gate accepts a lease, then releases it before room work.
#[derive(Debug, Clone)]
pub(crate) struct RoomDirectoryEntry {
    pub room: Arc<Room>,
    pub lifecycle: RoomLifecycle,
    pub create_date: String,
    pub remote_address: String,
}

impl RoomDirectoryEntry {
    fn new(room: Arc<Room>, remote_address: Option<&str>, reservation_ttl: Duration) -> Self {
        Self {
            room,
            lifecycle: RoomLifecycle::new(reservation_ttl),
            create_date: rfc3339_now(),
            remote_address: remote_address.unwrap_or(UNKNOWN_REMOTE_ADDRESS).to_owned(),
        }
    }
}

/// mutable state behind one directory entry's lifecycle lease gate
///
/// this lock is synchronous and short lived
/// callers may hold a
/// [`RoomLifecycleLease`] while awaiting, but this mutex is only held while a
/// lease is accepted or released
#[derive(Debug)]
struct RoomLifecycleState {
    /// accepted room work that has not finished or been dropped
    active_mutations: usize,
    /// empty-room removal request waiting for accepted work to drain
    remove_when_idle: bool,
    /// terminal marker set once one finisher wins directory removal
    closing: bool,
    /// reservation deadline, or `None` once a successful join retired it
    expires_at: Option<Instant>,
    /// lease length this reservation was published with and is renewed by
    reservation_ttl: Duration,
}

impl RoomLifecycleState {
    fn new(reservation_ttl: Duration) -> Self {
        Self {
            active_mutations: 0,
            remove_when_idle: false,
            closing: false,
            expires_at: Some(Instant::now() + reservation_ttl),
            reservation_ttl,
        }
    }
}

/// cloneable admission gate for the current room stored in one directory row
///
/// this type coordinates manager-level liveness only
/// room membership ordering
/// remains owned by [`Room`] and its state transition methods
#[derive(Debug, Clone)]
pub(crate) struct RoomLifecycle {
    state: Arc<Mutex<RoomLifecycleState>>,
}

impl RoomLifecycle {
    pub(crate) fn new(reservation_ttl: Duration) -> Self {
        Self {
            state: Arc::new(Mutex::new(RoomLifecycleState::new(reservation_ttl))),
        }
    }

    /// atomically claims cleanup responsibility for an expired, idle reservation.
    pub(crate) fn claim_expired_reservation(&self) -> bool {
        let mut state = lock_unpoisoned(&self.state);
        let is_expired = state.expires_at.is_some_and(|t| Instant::now() >= t);
        if !state.closing && state.active_mutations == 0 && is_expired {
            state.closing = true;
            drop(state);
            return true;
        }
        false
    }

    /// extends a room reservation without rearming it
    pub(crate) fn renew_reservation(&self) {
        let mut state = lock_unpoisoned(&self.state);
        if state.expires_at.is_some() {
            state.expires_at = Some(Instant::now() + state.reservation_ttl);
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(crate) fn expire_reservation_now_for_test(&self) {
        lock_unpoisoned(&self.state).expires_at = Some(Instant::now());
    }

    #[cfg(any(test, feature = "testing-transport"))]
    #[must_use]
    pub(crate) fn has_reservation_deadline_for_test(&self) -> bool {
        lock_unpoisoned(&self.state).expires_at.is_some()
    }

    /// Accepts a lease unless removal is pending or already claimed.
    ///
    /// Returns `None` when removal is pending or claimed or the lease count
    /// cannot increase. Directory callers must retain their read guard through
    /// admission to prove that the entry is current.
    #[must_use]
    pub(crate) fn begin(&self) -> Option<RoomLifecycleLease> {
        let mut state = lock_unpoisoned(&self.state);
        if state.closing || state.remove_when_idle {
            return None;
        }
        state.active_mutations = state.active_mutations.checked_add(1)?;
        let lease = RoomLifecycleLease {
            state: Arc::clone(&self.state),
            finished: false,
        };
        drop(state);
        Some(lease)
    }
}

/// cancellation-safe permit for work accepted against a directory entry
///
/// dropping the lease releases admission without requesting removal
/// manager
/// teardown paths call [`Self::finish`] after checking whether the room is empty
#[derive(Debug)]
pub(crate) struct RoomLifecycleLease {
    /// shared lease state for the directory entry that accepted this work
    state: Arc<Mutex<RoomLifecycleState>>,
    /// Prevents `Drop` from releasing a lease already completed by `finish`.
    finished: bool,
}

impl RoomLifecycleLease {
    /// Releases the lease and returns whether this caller claimed directory removal.
    #[must_use]
    pub(crate) fn finish(mut self, remove_if_empty: bool, room_can_be_removed: bool) -> bool {
        self.release(remove_if_empty, room_can_be_removed)
    }

    #[must_use]
    fn release(&mut self, remove_if_empty: bool, room_can_be_removed: bool) -> bool {
        if self.finished {
            return false;
        }
        self.finished = true;
        let mut state = lock_unpoisoned(&self.state);
        if state.active_mutations > 0 {
            state.active_mutations -= 1;
        }
        if remove_if_empty && room_can_be_removed {
            state.remove_when_idle = true;
        }
        let idle_pending_removal = state.active_mutations == 0 && state.remove_when_idle;
        let should_remove = idle_pending_removal && room_can_be_removed;
        if should_remove {
            state.closing = true;
        } else if idle_pending_removal {
            // The final lease either found the room non-empty or supplied no
            // emptiness proof, so an earlier removal request cannot close it.
            state.remove_when_idle = false;
        }
        drop(state);
        should_remove
    }

    pub(crate) fn clear_expiration(&self) {
        lock_unpoisoned(&self.state).expires_at = None;
    }
}

impl Drop for RoomLifecycleLease {
    fn drop(&mut self) {
        let _ = self.release(false, false);
    }
}

#[derive(Debug, Default)]
pub(crate) struct RoomDirectory {
    by_uuid: BTreeMap<String, RoomDirectoryEntry>,
    uuid_by_instance: BTreeMap<RoomInstanceId, String>,
    uuid_by_issuer: BTreeMap<String, String>,
}

impl RoomDirectory {
    #[must_use]
    pub(crate) fn get_by_uuid(&self, uuid: &str) -> Option<Arc<Room>> {
        self.by_uuid.get(uuid).map(|entry| Arc::clone(&entry.room))
    }

    #[must_use]
    pub(crate) fn entry(&self, uuid: &str) -> Option<RoomDirectoryEntry> {
        self.by_uuid.get(uuid).cloned()
    }

    #[must_use]
    pub(crate) fn entry_by_issuer(&self, issuer: &str) -> Option<&RoomDirectoryEntry> {
        let uuid = self.uuid_by_issuer.get(issuer)?;
        self.by_uuid.get(uuid)
    }

    #[must_use]
    pub(crate) fn get_by_instance_id(&self, room_instance_id: RoomInstanceId) -> Option<Arc<Room>> {
        let uuid = self.uuid_by_instance.get(&room_instance_id)?;
        self.get_by_uuid(uuid)
    }

    #[must_use]
    pub(crate) fn entries(&self) -> Vec<RoomDirectoryEntry> {
        self.by_uuid.values().cloned().collect()
    }

    #[must_use]
    pub(crate) fn rooms(&self) -> Vec<Arc<Room>> {
        self.by_uuid
            .values()
            .map(|entry| Arc::clone(&entry.room))
            .collect()
    }

    pub(crate) fn insert(
        &mut self,
        room: Arc<Room>,
        remote_address: Option<&str>,
        reservation_ttl: Duration,
    ) {
        let room_id = room.uuid().to_owned();
        self.uuid_by_issuer
            .insert(room.issuer().to_owned(), room_id.clone());
        self.uuid_by_instance
            .insert(room.instance_id(), room_id.clone());
        self.by_uuid.insert(
            room_id,
            RoomDirectoryEntry::new(room, remote_address, reservation_ttl),
        );
    }

    #[must_use]
    pub(crate) fn contains_current(&self, uuid: &str, room: &Arc<Room>) -> bool {
        self.by_uuid
            .get(uuid)
            .is_some_and(|entry| Arc::ptr_eq(&entry.room, room))
    }

    pub(crate) fn remove_if_current(&mut self, uuid: &str, room: &Arc<Room>) {
        if self.contains_current(uuid, room) {
            self.by_uuid.remove(uuid);
            self.uuid_by_issuer.remove(room.issuer());
            self.uuid_by_instance.remove(&room.instance_id());
        }
    }
}
