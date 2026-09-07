use std::{
    cell::Cell,
    num::{NonZeroU64, NonZeroUsize},
};

use o_sfu_router::{Router, RouterId, rtp::MediaCapabilities};

use super::*;

fn worker(raw: usize) -> MediaWorkerId {
    MediaWorkerId::from_raw(raw)
}

fn placement(router: u64, media_worker: usize) -> RouterPlacement {
    RouterPlacement {
        router: RouterId(router),
        media_worker: worker(media_worker),
    }
}

fn room_with(primary: RouterPlacement, spillover: Vec<RouterPlacement>) -> PlacementSnapshot {
    Router::with_placements(
        RouterPlacements::new(primary, spillover),
        MediaCapabilities::new(Vec::new(), Vec::new()),
    )
    .placement_snapshot()
}

fn unassigned_room() -> PlacementSnapshot {
    Router::new(RouterId(7), MediaCapabilities::new(Vec::new(), Vec::new())).placement_snapshot()
}

fn policy(max_local_routers: usize, threshold_ms: u64) -> RoomWorkerPolicy {
    assert!(max_local_routers > 0);
    assert!(threshold_ms > 0);
    RoomWorkerPolicy::new(
        NonZeroUsize::new(max_local_routers).unwrap_or(NonZeroUsize::MIN),
        NonZeroU64::new(threshold_ms).unwrap_or(NonZeroU64::MIN),
    )
}

fn choose(
    room: &PlacementSnapshot,
    policy: RoomWorkerPolicy,
    delays_ms: &[Option<u64>],
    start_worker: usize,
) -> RouterPlacement {
    choose_placement(room, policy, delays_ms, start_worker, || RouterId(8))
}

#[test]
fn first_join_uses_the_first_healthy_worker_in_room_order() {
    assert_eq!(
        choose(
            &unassigned_room(),
            policy(2, 20),
            &[Some(20), Some(3), Some(1)],
            0,
        ),
        placement(7, 1)
    );
    assert_eq!(
        choose(
            &unassigned_room(),
            policy(2, 20),
            &[Some(1), Some(1), Some(1)],
            2,
        ),
        placement(7, 2)
    );
}

#[test]
fn first_join_falls_back_to_the_least_delayed_worker() {
    assert_eq!(
        choose(
            &unassigned_room(),
            policy(2, 20),
            &[Some(40), None, Some(25)],
            0,
        ),
        placement(7, 2)
    );
}

#[test]
fn unknown_unused_worker_does_not_trigger_spillover() {
    let primary = placement(7, 0);
    assert_eq!(
        choose(
            &room_with(primary, Vec::new()),
            policy(2, 20),
            &[Some(30), None],
            0,
        ),
        primary
    );
}

#[test]
fn missed_heartbeat_spills_above_the_delay_threshold() {
    assert_eq!(
        choose(
            &room_with(placement(7, 0), Vec::new()),
            policy(2, 1_000),
            &[None, Some(0)],
            0,
        ),
        placement(8, 1)
    );
}

#[test]
fn least_delayed_healthy_assignment_absorbs_later_joins() {
    let spillover = placement(8, 1);
    assert_eq!(
        choose(
            &room_with(placement(7, 0), vec![spillover]),
            policy(3, 20),
            &[Some(15), Some(2), Some(0)],
            0,
        ),
        spillover
    );
}

#[test]
fn all_overloaded_workers_reuse_the_least_delayed_assignment() {
    let spillover = placement(8, 1);
    assert_eq!(
        choose(
            &room_with(placement(7, 0), vec![spillover]),
            policy(3, 20),
            &[Some(40), Some(25), Some(30)],
            0,
        ),
        spillover
    );
}

#[test]
fn single_router_policy_never_spills() {
    let primary = placement(7, 0);
    assert_eq!(
        choose(
            &room_with(primary, Vec::new()),
            RoomWorkerPolicy::strict_single_router(),
            &[Some(40), Some(0)],
            0,
        ),
        primary
    );
}

#[test]
fn spillover_router_allocation_is_lazy() {
    let allocations = Cell::new(0);
    let allocate = || {
        allocations.set(allocations.get() + 1);
        RouterId(8)
    };
    let primary = placement(7, 0);

    assert_eq!(
        choose_placement(
            &room_with(primary, Vec::new()),
            policy(2, 20),
            &[Some(0), Some(0)],
            0,
            allocate,
        ),
        primary
    );
    assert_eq!(
        choose_placement(
            &room_with(primary, Vec::new()),
            RoomWorkerPolicy::strict_single_router(),
            &[Some(20), Some(0)],
            0,
            allocate,
        ),
        primary
    );
    assert_eq!(
        choose_placement(
            &room_with(primary, vec![placement(9, 1)]),
            policy(2, 20),
            &[Some(20), Some(20)],
            0,
            allocate,
        ),
        primary
    );
    assert_eq!(allocations.get(), 0);

    assert_eq!(
        choose_placement(
            &room_with(primary, Vec::new()),
            policy(2, 20),
            &[Some(20), Some(0)],
            0,
            allocate,
        ),
        placement(8, 1)
    );
    assert_eq!(allocations.get(), 1);
}
