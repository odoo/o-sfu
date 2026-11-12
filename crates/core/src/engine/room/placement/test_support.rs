use std::sync::Arc;

use o_sfu_router::RouterId;
use tokio::sync::Barrier;

use super::{JoinAdmissionTurn, PlacementSnapshot, Room, WorkerPlacementSource};
use crate::engine::room::membership::JoinUserRequest;

impl Room {
    pub(in crate::engine::room) async fn placement_usage_snapshot(&self) -> PlacementSnapshot {
        self.state.read().await.placement_usage_snapshot()
    }
}

impl JoinAdmissionTurn<'_> {
    pub(in crate::engine::room) fn for_test(
        request: JoinUserRequest,
        delays_ms: Vec<Option<u64>>,
        spillover_router_id: RouterId,
    ) -> JoinAdmissionTurn<'static, impl FnOnce() -> RouterId> {
        JoinAdmissionTurn {
            request,
            worker_states: WorkerPlacementSource::Fixed(delays_ms),
            allocate_spillover_router: move || spillover_router_id,
            gate: None,
        }
    }
}

impl<A: FnOnce() -> RouterId> JoinAdmissionTurn<'_, A> {
    pub(in crate::engine::room) fn with_gate(
        mut self,
        gate: Option<Arc<JoinPlacementTestGate>>,
    ) -> Self {
        self.gate = gate;
        self
    }
}

#[derive(Debug)]
pub struct JoinPlacementTestGate {
    ready: Barrier,
    release: Barrier,
}

impl JoinPlacementTestGate {
    #[must_use]
    pub fn new(expected: usize) -> Self {
        Self {
            ready: Barrier::new(expected + 1),
            release: Barrier::new(expected + 1),
        }
    }

    pub(super) async fn wait_before_commit(&self) {
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
