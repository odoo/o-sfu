use std::{sync::Arc, time::Duration};

use o_sfu_router::test_support::rtp_samples::sample_client_rtp_capabilities;

#[cfg(any(test, feature = "testing-transport"))]
use super::JoinPlacementTestGate;
use super::{
    super::{RoomAdmissionPolicy, RoomRuntimePolicy},
    RoomManager,
};
#[cfg(any(test, feature = "testing-transport"))]
use crate::engine::sync::lock_unpoisoned;
use crate::{
    RoomMediaLimits, RuntimeFeatureFlags, VideoAdaptationTuning, engine::metrics::RuntimeMetrics,
};

const DEFAULT_TEST_MAX_SESSIONS: usize = 100;
const DEFAULT_TEST_RESERVATION_TTL: Duration = Duration::from_mins(1);
const DEFAULT_TEST_DEPARTURE_GRACE: Duration = Duration::from_mins(1);

impl RoomManager {
    #[must_use]
    pub fn for_test() -> Self {
        Self::for_test_with_runtime_policy(test_runtime_policy(RoomAdmissionPolicy::new(
            DEFAULT_TEST_MAX_SESSIONS,
        )))
    }

    #[must_use]
    pub fn for_test_with_admission_policy(admission_policy: RoomAdmissionPolicy) -> Self {
        Self::for_test_with_runtime_policy(test_runtime_policy(admission_policy))
    }

    #[must_use]
    pub fn for_test_with_media_limits(media_limits: RoomMediaLimits) -> Self {
        Self::for_test_with_runtime_policy(
            test_runtime_policy(RoomAdmissionPolicy::new(DEFAULT_TEST_MAX_SESSIONS))
                .with_media_limits(media_limits),
        )
    }

    #[must_use]
    pub fn for_test_with_video_adaptation_tuning(tuning: VideoAdaptationTuning) -> Self {
        Self::for_test_with_runtime_policy(
            test_runtime_policy(RoomAdmissionPolicy::new(DEFAULT_TEST_MAX_SESSIONS))
                .with_video_adaptation_tuning(tuning),
        )
    }

    #[must_use]
    pub fn for_test_with_runtime_policy(runtime_policy: RoomRuntimePolicy) -> Self {
        Self::for_test_with_runtime_policy_and_reservation_ttl(
            runtime_policy,
            DEFAULT_TEST_RESERVATION_TTL,
        )
    }

    /// builds a manager whose new rooms expire after `reservation_ttl`
    ///
    /// [`Duration::ZERO`] publishes rooms that are already past their deadline,
    /// which lets reservation tests exercise expiry without touching a clock
    #[must_use]
    pub fn for_test_with_reservation_ttl(reservation_ttl: Duration) -> Self {
        Self::for_test_with_runtime_policy_and_reservation_ttl(
            test_runtime_policy(RoomAdmissionPolicy::new(DEFAULT_TEST_MAX_SESSIONS)),
            reservation_ttl,
        )
    }

    #[must_use]
    pub fn for_test_with_runtime_policy_and_reservation_ttl(
        runtime_policy: RoomRuntimePolicy,
        reservation_ttl: Duration,
    ) -> Self {
        Self::new(
            runtime_policy,
            Arc::new(RuntimeMetrics::default()),
            reservation_ttl,
            DEFAULT_TEST_DEPARTURE_GRACE,
        )
    }

    /// builds a manager whose emptied rooms wait `departure_grace` for a rejoin
    ///
    /// [`Duration::ZERO`] restores removal by the departure that empties the
    /// room, which lets tests exercise that path without touching a clock
    #[must_use]
    pub fn for_test_with_departure_grace(departure_grace: Duration) -> Self {
        Self::new(
            test_runtime_policy(RoomAdmissionPolicy::new(DEFAULT_TEST_MAX_SESSIONS)),
            Arc::new(RuntimeMetrics::default()),
            DEFAULT_TEST_RESERVATION_TTL,
            departure_grace,
        )
    }

    pub async fn expire_room_reservation_now_for_test(&self, room_id: &str) -> bool {
        let Some(entry) = self.entry(room_id).await else {
            return false;
        };
        entry.lifecycle.expire_reservation_now_for_test();
        true
    }

    pub async fn has_room_reservation_deadline_for_test(&self, room_id: &str) -> bool {
        self.entry(room_id)
            .await
            .is_some_and(|entry| entry.lifecycle.has_reservation_deadline_for_test())
    }

    pub async fn has_room_departure_grace_for_test(&self, room_id: &str) -> bool {
        self.entry(room_id)
            .await
            .is_some_and(|entry| entry.lifecycle.has_departure_grace_for_test())
    }

    pub async fn expire_room_departure_grace_now_for_test(&self, room_id: &str) -> bool {
        let Some(entry) = self.entry(room_id).await else {
            return false;
        };
        entry.lifecycle.expire_departure_grace_now_for_test()
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub fn set_join_placement_gate_for_test(&self, gate: Arc<JoinPlacementTestGate>) {
        *lock_unpoisoned(&self.join_placement_gate) = Some(gate);
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) fn join_placement_gate_for_test(&self) -> Option<Arc<JoinPlacementTestGate>> {
        lock_unpoisoned(&self.join_placement_gate).clone()
    }
}

fn test_runtime_policy(admission_policy: RoomAdmissionPolicy) -> RoomRuntimePolicy {
    RoomRuntimePolicy::new(
        admission_policy,
        RuntimeFeatureFlags::default(),
        sample_client_rtp_capabilities(),
    )
}
