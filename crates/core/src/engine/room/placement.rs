//! Router and packet-worker placement for room admission.
//!
//! [`RoomRuntimeContext`] seeds a room with assigned placements or an
//! unassigned primary router. Admission selects a packet worker from current
//! delay samples and may add a router within [`RoomWorkerPolicy`].

use o_sfu_router::RouterId;
pub use o_sfu_router::topology::{
    PlacementSnapshot, RouterPlacement, RouterPlacements, RouterPlacementsError,
};
#[cfg(any(test, feature = "testing-transport"))]
use {std::sync::Arc, tokio::sync::Barrier};

use super::{
    Room, RoomJoinError,
    factory::RoomFactory,
    membership::JoinUserRequest,
    state::{JoinCommit, UserJoinedFanout},
};
use crate::{
    RoomWorkerPolicy,
    engine::{MediaWorkerId, RoomInstanceId, media_transport::MediaTransport},
};

/// Initial router placement context for one room instance.
///
/// [`Self::new_unassigned`] defers packet-worker selection until admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomRuntimeContext {
    instance: RoomInstanceId,
    primary_router: RouterId,
    initial_router_placements: Option<RouterPlacements>,
}

impl RoomRuntimeContext {
    #[must_use]
    pub fn new(
        instance: RoomInstanceId,
        primary: RouterPlacement,
        spillover: Vec<RouterPlacement>,
    ) -> Self {
        Self {
            instance,
            primary_router: primary.router,
            initial_router_placements: Some(RouterPlacements::new(primary, spillover)),
        }
    }

    #[must_use]
    pub const fn new_unassigned(instance: RoomInstanceId, primary_router: RouterId) -> Self {
        Self {
            instance,
            primary_router,
            initial_router_placements: None,
        }
    }

    /// # Errors
    ///
    /// returns [`RouterPlacementsError::Empty`] when `placements` is empty
    pub fn try_from_placements(
        instance: RoomInstanceId,
        placements: Vec<RouterPlacement>,
    ) -> Result<Self, RouterPlacementsError> {
        let routers = RouterPlacements::try_from_vec(placements)?;
        Ok(Self {
            instance,
            primary_router: routers.primary().router,
            initial_router_placements: Some(routers),
        })
    }

    #[must_use]
    pub const fn instance(&self) -> RoomInstanceId {
        self.instance
    }

    #[must_use]
    pub const fn primary_router(&self) -> RouterId {
        self.primary_router
    }

    #[must_use]
    pub fn initial_router_placements(&self) -> Option<&RouterPlacements> {
        self.initial_router_placements.as_ref()
    }
}

#[cfg(any(test, feature = "testing-transport"))]
impl Room {
    pub(super) async fn placement_usage_snapshot(&self) -> PlacementSnapshot {
        self.state.read().await.placement_usage_snapshot()
    }
}

enum PacketLoopDelaySource<'a> {
    Transport(&'a MediaTransport),
    #[cfg(any(test, feature = "testing-transport"))]
    Fixed(Vec<Option<u64>>),
}

impl PacketLoopDelaySource<'_> {
    fn snapshot(self) -> Vec<Option<u64>> {
        match self {
            Self::Transport(transport) => transport.packet_loop_delays_ms(),
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fixed(delays_ms) => delays_ms,
        }
    }
}

pub(super) struct JoinAdmissionTurn<'a, A = fn() -> RouterId> {
    request: JoinUserRequest,
    packet_loop_delays: PacketLoopDelaySource<'a>,
    allocate_spillover_router: A,
    #[cfg(any(test, feature = "testing-transport"))]
    gate: Option<Arc<JoinPlacementTestGate>>,
}

impl JoinAdmissionTurn<'_> {
    pub(super) fn from_factory<'a>(
        request: JoinUserRequest,
        media_transport: &'a MediaTransport,
        factory: &'a RoomFactory,
    ) -> JoinAdmissionTurn<'a, impl FnOnce() -> RouterId + 'a> {
        JoinAdmissionTurn {
            request,
            packet_loop_delays: PacketLoopDelaySource::Transport(media_transport),
            allocate_spillover_router: move || factory.allocate_spillover_router(),
            #[cfg(any(test, feature = "testing-transport"))]
            gate: None,
        }
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) fn for_test(
        request: JoinUserRequest,
        delays_ms: Vec<Option<u64>>,
        spillover_router_id: RouterId,
    ) -> JoinAdmissionTurn<'static, impl FnOnce() -> RouterId> {
        JoinAdmissionTurn {
            request,
            packet_loop_delays: PacketLoopDelaySource::Fixed(delays_ms),
            allocate_spillover_router: move || spillover_router_id,
            gate: None,
        }
    }
}

impl<A: FnOnce() -> RouterId> JoinAdmissionTurn<'_, A> {
    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) fn with_gate(mut self, gate: Option<Arc<JoinPlacementTestGate>>) -> Self {
        self.gate = gate;
        self
    }

    pub(super) async fn commit(
        self,
        room: &Room,
        joined_fanout: UserJoinedFanout,
    ) -> Result<JoinCommit, RoomJoinError> {
        #[cfg(any(test, feature = "testing-transport"))]
        if let Some(gate) = &self.gate {
            gate.wait_before_commit().await;
        }
        let mut state = room.state.write().await;
        // Sample worker delay after reaching the serialized commit turn. The
        // state guard makes each join select from placements committed earlier.
        let delays_ms = self.packet_loop_delays.snapshot();
        let worker_count = delays_ms.len().max(1);
        let start_worker = room_worker_start(room.instance_id(), worker_count);
        let placement = choose_placement(
            &state.placement_usage_snapshot(),
            room.room_worker_policy(),
            &delays_ms,
            start_worker,
            self.allocate_spillover_router,
        );
        state.apply_join_on_placement(
            &self.request.user_id,
            self.request.sender,
            joined_fanout,
            placement,
        )
    }
}

fn choose_placement(
    room: &PlacementSnapshot,
    policy: RoomWorkerPolicy,
    delays_ms: &[Option<u64>],
    start_worker: usize,
    allocate_spillover_router: impl FnOnce() -> RouterId,
) -> RouterPlacement {
    let worker_count = delays_ms.len().max(1);
    let threshold_ms = policy.packet_loop_delay_threshold_ms();
    let assigned = room.assigned_placements();
    let Some(primary) = assigned.first().copied() else {
        return RouterPlacement {
            router: room.primary(),
            media_worker: choose_primary_worker(
                delays_ms,
                threshold_ms,
                start_worker % worker_count,
            ),
        };
    };
    if policy.max_local_routers() == 1 {
        return primary;
    }
    if let Some(placement) = assigned
        .iter()
        .filter(|placement| worker_is_healthy(delays_ms, placement.media_worker, threshold_ms))
        .min_by_key(|placement| worker_delay(delays_ms, placement.media_worker))
    {
        return *placement;
    }
    let placement_cap = policy.max_local_routers().min(worker_count);
    if assigned.len() < placement_cap
        && let Some(media_worker) = cyclic_workers(start_worker, worker_count).find(|worker| {
            worker_is_healthy(delays_ms, *worker, threshold_ms)
                && assigned
                    .iter()
                    .all(|placement| placement.media_worker != *worker)
        })
    {
        return RouterPlacement {
            router: allocate_spillover_router(),
            media_worker,
        };
    }
    assigned
        .iter()
        .copied()
        .min_by_key(|placement| worker_delay(delays_ms, placement.media_worker))
        .unwrap_or(primary)
}

fn choose_primary_worker(
    delays_ms: &[Option<u64>],
    threshold_ms: u64,
    start_worker: usize,
) -> MediaWorkerId {
    let worker_count = delays_ms.len().max(1);
    cyclic_workers(start_worker, worker_count)
        .find(|worker| worker_is_healthy(delays_ms, *worker, threshold_ms))
        .or_else(|| {
            cyclic_workers(start_worker, worker_count)
                .min_by_key(|worker| worker_delay(delays_ms, *worker))
        })
        .unwrap_or_else(|| MediaWorkerId::from_raw(0))
}

fn cyclic_workers(start_worker: usize, worker_count: usize) -> impl Iterator<Item = MediaWorkerId> {
    (0..worker_count).map(move |offset| {
        MediaWorkerId::from_raw(start_worker.wrapping_add(offset) % worker_count)
    })
}

fn worker_is_healthy(delays_ms: &[Option<u64>], worker: MediaWorkerId, threshold_ms: u64) -> bool {
    worker_delay(delays_ms, worker) < threshold_ms
}

fn worker_delay(delays_ms: &[Option<u64>], worker: MediaWorkerId) -> u64 {
    // Missing samples cannot qualify a worker as healthy. They retain the worst
    // rank for the all-unhealthy fallback.
    delays_ms
        .get(worker.as_usize())
        .copied()
        .flatten()
        .unwrap_or(u64::MAX)
}

fn room_worker_start(room_instance_id: RoomInstanceId, worker_count: usize) -> usize {
    let worker_count = u64::try_from(worker_count.max(1)).unwrap_or(u64::MAX);
    usize::try_from(room_instance_id.as_u64() % worker_count).unwrap_or_default()
}

#[cfg(any(test, feature = "testing-transport"))]
#[derive(Debug)]
pub struct JoinPlacementTestGate {
    ready: Barrier,
    release: Barrier,
}

#[cfg(any(test, feature = "testing-transport"))]
impl JoinPlacementTestGate {
    #[must_use]
    pub fn new(expected: usize) -> Self {
        Self {
            ready: Barrier::new(expected + 1),
            release: Barrier::new(expected + 1),
        }
    }

    async fn wait_before_commit(&self) {
        self.ready.wait().await;
        self.release.wait().await;
    }

    pub async fn hold_all_ready(&self) {
        self.ready.wait().await;
    }

    pub async fn release_all(&self) {
        self.release.wait().await;
    }
}

#[cfg(test)]
#[path = "TESTS/placement.rs"]
mod tests;
