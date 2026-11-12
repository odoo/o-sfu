//! Router and packet-worker placement for room admission.
//!
//! [`RoomRuntimeContext`] seeds a room with assigned placements or an
//! unassigned primary router. Admission selects a packet worker from current
//! delay samples and may add a router within [`RoomWorkerPolicy`].

#[cfg(any(test, feature = "testing-transport"))]
use std::sync::Arc;

use o_sfu_router::RouterId;
pub use o_sfu_router::topology::{PlacementSnapshot, RouterPlacement, RouterPlacements};

use super::{
    Room, RoomJoinError,
    factory::RoomFactory,
    membership::JoinUserRequest,
    state::{JoinCommit, UserJoinedFanout},
};
use crate::{
    RoomWorkerPolicy,
    engine::{
        MediaWorkerId, RoomInstanceId,
        media_transport::{MediaTransport, WorkerPlacementState},
    },
};

#[cfg(any(test, feature = "testing-transport"))]
pub(super) mod test_support;

#[cfg(any(test, feature = "testing-transport"))]
use test_support::JoinPlacementTestGate;

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

enum WorkerPlacementSource<'a> {
    Transport(&'a MediaTransport),
    #[cfg(any(test, feature = "testing-transport"))]
    Fixed(Vec<Option<u64>>),
}

impl WorkerPlacementSource<'_> {
    fn snapshot(self) -> Vec<WorkerPlacementState> {
        match self {
            Self::Transport(transport) => transport.worker_placement_states(),
            #[cfg(any(test, feature = "testing-transport"))]
            Self::Fixed(delays_ms) => delays_ms
                .into_iter()
                .map(WorkerPlacementState::Running)
                .collect(),
        }
    }
}

pub(super) struct JoinAdmissionTurn<'a, A = fn() -> RouterId> {
    request: JoinUserRequest,
    worker_states: WorkerPlacementSource<'a>,
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
            worker_states: WorkerPlacementSource::Transport(media_transport),
            allocate_spillover_router: move || factory.allocate_spillover_router(),
            #[cfg(any(test, feature = "testing-transport"))]
            gate: None,
        }
    }
}

impl<A: FnOnce() -> RouterId> JoinAdmissionTurn<'_, A> {
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
        // Sample worker availability and delay at the serialized commit turn.
        // The state guard includes placements committed by earlier joins.
        let worker_states = self.worker_states.snapshot();
        let worker_count = worker_states.len().max(1);
        let start_worker = room_worker_start(room.instance_id(), worker_count);
        let placement = choose_placement(
            &state.placement_usage_snapshot(),
            room.room_worker_policy(),
            &worker_states,
            start_worker,
            self.allocate_spillover_router,
        )?;
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
    workers: &[WorkerPlacementState],
    start_worker: usize,
    allocate_spillover_router: impl FnOnce() -> RouterId,
) -> Result<RouterPlacement, RoomJoinError> {
    if !workers
        .iter()
        .any(|worker| matches!(worker, WorkerPlacementState::Running(_)))
    {
        return Err(RoomJoinError::NoUsableWorker);
    }
    let worker_count = workers.len();
    let threshold_ms = policy.packet_loop_delay_threshold_ms();
    let assigned = room.assigned_placements();
    if assigned.is_empty() {
        return Ok(RouterPlacement {
            router: room.primary(),
            media_worker: choose_primary_worker(
                workers,
                threshold_ms,
                start_worker % worker_count,
            )?,
        });
    }
    if let Some(placement) = assigned
        .iter()
        .filter(|placement| worker_is_healthy(workers, placement.media_worker, threshold_ms))
        .min_by_key(|placement| worker_delay(workers, placement.media_worker))
    {
        return Ok(*placement);
    }
    let placement_cap = policy.max_local_routers().min(worker_count);
    let usable_placements = assigned
        .iter()
        .filter(|placement| worker_is_usable(workers, placement.media_worker))
        .count();
    if usable_placements < placement_cap
        && let Some(media_worker) = cyclic_workers(start_worker, worker_count).find(|worker| {
            worker_is_healthy(workers, *worker, threshold_ms)
                && assigned
                    .iter()
                    .all(|placement| placement.media_worker != *worker)
        })
    {
        return Ok(RouterPlacement {
            router: allocate_spillover_router(),
            media_worker,
        });
    }
    if let Some(placement) = assigned
        .iter()
        .copied()
        .filter(|placement| worker_is_usable(workers, placement.media_worker))
        .min_by_key(|placement| worker_delay(workers, placement.media_worker))
    {
        return Ok(placement);
    }
    if usable_placements < placement_cap
        && let Some(media_worker) = cyclic_workers(start_worker, worker_count).find(|worker| {
            worker_is_usable(workers, *worker)
                && assigned
                    .iter()
                    .all(|placement| placement.media_worker != *worker)
        })
    {
        return Ok(RouterPlacement {
            router: allocate_spillover_router(),
            media_worker,
        });
    }
    Err(RoomJoinError::RouterState)
}

fn choose_primary_worker(
    workers: &[WorkerPlacementState],
    threshold_ms: u64,
    start_worker: usize,
) -> Result<MediaWorkerId, RoomJoinError> {
    let worker_count = workers.len().max(1);
    cyclic_workers(start_worker, worker_count)
        .find(|worker| worker_is_healthy(workers, *worker, threshold_ms))
        .or_else(|| {
            cyclic_workers(start_worker, worker_count)
                .filter(|worker| worker_is_usable(workers, *worker))
                .min_by_key(|worker| worker_delay(workers, *worker))
        })
        .ok_or(RoomJoinError::NoUsableWorker)
}

fn cyclic_workers(start_worker: usize, worker_count: usize) -> impl Iterator<Item = MediaWorkerId> {
    (0..worker_count).map(move |offset| {
        MediaWorkerId::from_raw(start_worker.wrapping_add(offset) % worker_count)
    })
}

fn worker_is_healthy(
    workers: &[WorkerPlacementState],
    worker: MediaWorkerId,
    threshold_ms: u64,
) -> bool {
    matches!(
        workers.get(worker.as_usize()),
        Some(WorkerPlacementState::Running(Some(delay_ms))) if *delay_ms < threshold_ms
    )
}

fn worker_is_usable(workers: &[WorkerPlacementState], worker: MediaWorkerId) -> bool {
    matches!(
        workers.get(worker.as_usize()),
        Some(WorkerPlacementState::Running(_))
    )
}

fn worker_delay(workers: &[WorkerPlacementState], worker: MediaWorkerId) -> u64 {
    match workers.get(worker.as_usize()) {
        Some(WorkerPlacementState::Running(Some(delay_ms))) => *delay_ms,
        _ => u64::MAX,
    }
}

fn room_worker_start(room_instance_id: RoomInstanceId, worker_count: usize) -> usize {
    let worker_count = u64::try_from(worker_count.max(1)).unwrap_or(u64::MAX);
    usize::try_from(room_instance_id.as_u64() % worker_count).unwrap_or_default()
}

#[cfg(test)]
#[path = "TESTS/placement.rs"]
mod tests;
