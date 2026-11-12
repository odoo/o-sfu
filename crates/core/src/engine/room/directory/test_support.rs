use tokio::time::Instant;

use super::{RoomLifecycle, RoomLifecyclePhase};
use crate::engine::sync::lock_unpoisoned;

impl RoomLifecycle {
    pub(crate) fn expire_reservation_now_for_test(&self) {
        lock_unpoisoned(&self.state).phase = RoomLifecyclePhase::Reservation {
            expires_at: Instant::now(),
        };
    }

    #[must_use]
    pub(crate) fn has_reservation_deadline_for_test(&self) -> bool {
        matches!(
            lock_unpoisoned(&self.state).phase,
            RoomLifecyclePhase::Reservation { .. }
        )
    }

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
}
