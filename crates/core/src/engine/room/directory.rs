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
    fn new(
        room: Arc<Room>,
        remote_address: Option<&str>,
        reservation_ttl: Duration,
        departure_grace: Duration,
    ) -> Self {
        Self {
            room,
            lifecycle: RoomLifecycle::new(reservation_ttl, departure_grace),
            create_date: rfc3339_now(),
            remote_address: remote_address.unwrap_or(UNKNOWN_REMOTE_ADDRESS).to_owned(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoomLifecyclePhase {
    Reservation { expires_at: Instant },
    Alive,
    Grace { expires_at: Instant },
    Closing,
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
    pending_removal: Option<RoomRemovalPolicy>,
    /// lease length this reservation was published with and is renewed by
    reservation_ttl: Duration,
    /// grace duration this reservation was published with and is renewed by
    departure_grace: Duration,
    phase: RoomLifecyclePhase,
}

impl RoomLifecycleState {
    fn new(reservation_ttl: Duration, departure_grace: Duration) -> Self {
        Self {
            active_mutations: 0,
            pending_removal: None,
            reservation_ttl,
            departure_grace,
            phase: RoomLifecyclePhase::Reservation {
                expires_at: Instant::now() + reservation_ttl,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExpiryReason {
    /// reservation lapsed before any join succeeded
    ReservationLapsed,
    /// room went empty and the departure grace ran out
    GraceElapsed,
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
    pub(crate) fn new(reservation_ttl: Duration, departure_grace: Duration) -> Self {
        Self {
            state: Arc::new(Mutex::new(RoomLifecycleState::new(
                reservation_ttl,
                departure_grace,
            ))),
        }
    }

    /// atomically claims cleanup responsibility for an expired grace/reservation period
    pub(crate) fn claim_expired_room(&self) -> Option<ExpiryReason> {
        let mut state = lock_unpoisoned(&self.state);
        let expiry_reason = match state.phase {
            RoomLifecyclePhase::Reservation { expires_at } if expires_at <= Instant::now() => {
                Some(ExpiryReason::ReservationLapsed)
            }
            RoomLifecyclePhase::Grace { expires_at } if expires_at <= Instant::now() => {
                Some(ExpiryReason::GraceElapsed)
            }
            _ => None,
        };
        if state.active_mutations == 0 && expiry_reason.is_some() {
            state.phase = RoomLifecyclePhase::Closing;
            drop(state);
            return expiry_reason;
        }
        None
    }

    /// extends a room reservation; rearms it if coming from a grace period
    pub(crate) fn renew_reservation(&self) {
        let mut state = lock_unpoisoned(&self.state);
        if matches!(
            state.phase,
            RoomLifecyclePhase::Reservation { .. } | RoomLifecyclePhase::Grace { .. }
        ) {
            state.phase = RoomLifecyclePhase::Reservation {
                expires_at: Instant::now() + state.reservation_ttl,
            }
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(crate) fn expire_reservation_now_for_test(&self) {
        lock_unpoisoned(&self.state).phase = RoomLifecyclePhase::Reservation {
            expires_at: Instant::now(),
        };
    }

    #[cfg(any(test, feature = "testing-transport"))]
    #[must_use]
    pub(crate) fn has_reservation_deadline_for_test(&self) -> bool {
        matches!(
            lock_unpoisoned(&self.state).phase,
            RoomLifecyclePhase::Reservation { .. }
        )
    }

    #[cfg(any(test, feature = "testing-transport"))]
    #[must_use]
    pub(crate) fn has_departure_grace_for_test(&self) -> bool {
        matches!(
            lock_unpoisoned(&self.state).phase,
            RoomLifecyclePhase::Grace { .. }
        )
    }

    /// expires an armed departure grace, and reports whether one was armed
    ///
    /// this never arms a grace, so a test cannot expire a phase the room never
    /// reached
    #[cfg(any(test, feature = "testing-transport"))]
    #[must_use]
    pub(crate) fn expire_departure_grace_now_for_test(&self) -> bool {
        let mut state = lock_unpoisoned(&self.state);
        if !matches!(state.phase, RoomLifecyclePhase::Grace { .. }) {
            return false;
        }
        state.phase = RoomLifecyclePhase::Grace {
            expires_at: Instant::now(),
        };
        true
    }

    /// Accepts a lease unless immediate removal is pending or already claimed.
    ///
    /// Returns `None` when removal is pending or claimed or the lease count
    /// cannot increase. Directory callers must retain their read guard through
    /// admission to prove that the entry is current.
    #[must_use]
    pub(crate) fn begin(&self) -> Option<RoomLifecycleLease> {
        let mut state = lock_unpoisoned(&self.state);
        if matches!(state.phase, RoomLifecyclePhase::Closing)
            || matches!(state.pending_removal, Some(RoomRemovalPolicy::Immediately))
        {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum RoomRemovalPolicy {
    AfterGrace,
    Immediately,
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
    pub(crate) fn finish(
        mut self,
        on_empty: Option<RoomRemovalPolicy>,
        room_can_be_removed: bool,
    ) -> bool {
        self.release(on_empty, room_can_be_removed)
    }

    #[must_use]
    fn release(&mut self, on_empty: Option<RoomRemovalPolicy>, room_can_be_removed: bool) -> bool {
        if self.finished {
            return false;
        }
        self.finished = true;
        let mut state = lock_unpoisoned(&self.state);
        // Keep the strongest policy; promote 0 seconds grace to immediate removal.
        let removal_policy = match state.pending_removal.max(on_empty) {
            Some(RoomRemovalPolicy::AfterGrace) if state.departure_grace.is_zero() => {
                Some(RoomRemovalPolicy::Immediately)
            }
            policy => policy,
        };

        state.active_mutations = state.active_mutations.saturating_sub(1);
        if state.active_mutations > 0 {
            if on_empty.is_some() && room_can_be_removed {
                state.pending_removal = removal_policy;
            }
            return false;
        }
        // The last lease consumes the policy below or voids it here, and a
        // `None` policy means nothing was pending.
        state.pending_removal = None;
        if !room_can_be_removed {
            return false;
        }

        match removal_policy {
            Some(RoomRemovalPolicy::Immediately) => {
                state.phase = RoomLifecyclePhase::Closing;
                true
            }
            Some(RoomRemovalPolicy::AfterGrace) => {
                if matches!(state.phase, RoomLifecyclePhase::Alive) {
                    state.phase = RoomLifecyclePhase::Grace {
                        expires_at: Instant::now() + state.departure_grace,
                    };
                }
                false
            }
            None => false,
        }
    }

    pub(crate) fn clear_expiration(&self) {
        lock_unpoisoned(&self.state).phase = RoomLifecyclePhase::Alive;
    }
}

impl Drop for RoomLifecycleLease {
    fn drop(&mut self) {
        let _ = self.release(None, false);
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
    pub(crate) fn entry(&self, uuid: &str) -> Option<&RoomDirectoryEntry> {
        self.by_uuid.get(uuid)
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
        departure_grace: Duration,
    ) {
        let room_id = room.uuid().to_owned();
        self.uuid_by_issuer
            .insert(room.issuer().to_owned(), room_id.clone());
        self.uuid_by_instance
            .insert(room.instance_id(), room_id.clone());
        self.by_uuid.insert(
            room_id,
            RoomDirectoryEntry::new(room, remote_address, reservation_ttl, departure_grace),
        );
    }

    #[must_use]
    pub(crate) fn contains_current(&self, uuid: &str, room: &Arc<Room>) -> bool {
        self.by_uuid
            .get(uuid)
            .is_some_and(|entry| Arc::ptr_eq(&entry.room, room))
    }

    pub(crate) fn remove_if_current(&mut self, uuid: &str, room: &Arc<Room>) -> bool {
        if self.contains_current(uuid, room) {
            self.by_uuid.remove(uuid);
            self.uuid_by_issuer.remove(room.issuer());
            self.uuid_by_instance.remove(&room.instance_id());
            return true;
        }
        false
    }
}
