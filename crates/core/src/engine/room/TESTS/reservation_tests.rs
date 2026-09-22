//! room reservation and departure grace expiry coordination
//!
//! a room published by `/v1/channel` carries a reservation deadline until a
//! user successfully joins it, and an emptied room carries a departure grace
//! until it is collected. these tests pin the orderings that decide whether
//! expiry or room work wins, and the directory indexes an expiry is allowed to
//! touch
//!
//! the deadline is a [`tokio::time::Instant`], so these tests move the clock
//! explicitly instead of sleeping. they spawn no tasks, which keeps paused-time
//! auto-advance from reaching any other timer

use std::{sync::Arc, time::Duration};

use tokio::time::advance;

use super::{
    super::{
        RoomAdmissionPolicy, RoomConfig, RoomRuntimePolicy,
        directory::{ExpiryReason, RoomDirectory, RoomLifecycle, RoomRemovalPolicy},
        factory::RoomFactory,
    },
    fixtures::{TEST_ROOM_KEY, test_client_rtp_capabilities},
};
use crate::{RuntimeFeatureFlags, engine::metrics::RuntimeMetrics};

const TEST_RESERVATION_TTL: Duration = Duration::from_mins(1);
const TEST_DEPARTURE_GRACE: Duration = Duration::from_mins(1);

/// moves the clock just past a freshly published reservation deadline
///
/// the extra millisecond keeps the assertion off the boundary, because
/// [`advance`] may leave the clock exactly on the deadline
async fn advance_past_reservation_deadline() {
    advance(TEST_RESERVATION_TTL + Duration::from_millis(1)).await;
}

fn test_factory() -> RoomFactory {
    RoomFactory::new(
        RoomRuntimePolicy::new(
            RoomAdmissionPolicy::new(2),
            RuntimeFeatureFlags::default(),
            test_client_rtp_capabilities(),
        ),
        Arc::new(RuntimeMetrics::default()),
    )
}

#[tokio::test(start_paused = true)]
async fn an_expired_reservation_is_claimed_once_and_is_terminal() {
    let lifecycle = RoomLifecycle::new(TEST_RESERVATION_TTL, TEST_DEPARTURE_GRACE);

    drop(
        lifecycle
            .begin()
            .expect("work should be accepted before the deadline"),
    );
    assert_eq!(
        lifecycle.claim_expired_room(),
        None,
        "an unexpired reservation must not be claimed"
    );

    advance_past_reservation_deadline().await;

    // a room stays available until a reaper pass claims it, so a passed
    // deadline on its own must not refuse work
    drop(
        lifecycle
            .begin()
            .expect("work arriving before the reaper should still be accepted"),
    );
    assert_eq!(
        lifecycle.claim_expired_room(),
        Some(ExpiryReason::ReservationLapsed),
        "the first claim after the deadline should win directory removal"
    );
    assert_eq!(
        lifecycle.claim_expired_room(),
        None,
        "later reaper passes must not win again, so the room gauge drops once"
    );
    assert!(
        lifecycle.begin().is_none(),
        "a claimed reservation is terminal"
    );
}

#[tokio::test(start_paused = true)]
async fn work_that_leaves_the_room_empty_keeps_the_reservation() {
    let lifecycle = RoomLifecycle::new(TEST_RESERVATION_TTL, TEST_DEPARTURE_GRACE);
    let lease = lifecycle
        .begin()
        .expect("a join that will fail is still accepted");

    advance_past_reservation_deadline().await;
    assert!(
        !lease.finish(None, true),
        "an empty room is not removed without a removal request"
    );
    assert_eq!(
        lifecycle.claim_expired_room(),
        Some(ExpiryReason::ReservationLapsed),
        "only a successful join may retire the reservation"
    );
}

#[tokio::test(start_paused = true)]
async fn empty_room_removal_wins_over_a_later_expiry_claim() {
    let lifecycle = RoomLifecycle::new(TEST_RESERVATION_TTL, TEST_DEPARTURE_GRACE);
    let lease = lifecycle
        .begin()
        .expect("an administrative disconnect should be accepted");

    assert!(
        lease.finish(Some(RoomRemovalPolicy::Immediately), true),
        "the finisher that empties the room should win directory removal"
    );

    advance_past_reservation_deadline().await;
    assert_eq!(
        lifecycle.claim_expired_room(),
        None,
        "expiry must not remove a room the last-user path already removed"
    );
}

#[tokio::test(start_paused = true)]
async fn stale_expiry_never_removes_a_newer_room_for_the_same_issuer() {
    const ISSUER: &str = "issuer-stale-expiry";

    let factory = test_factory();
    let mut directory = RoomDirectory::default();
    let stale = factory.create(ISSUER, TEST_ROOM_KEY.into(), &RoomConfig::default());
    directory.insert(
        Arc::clone(&stale),
        None,
        TEST_RESERVATION_TTL,
        TEST_DEPARTURE_GRACE,
    );

    advance_past_reservation_deadline().await;
    let stale_entry = directory
        .entry(stale.uuid())
        .expect("the first room should be a current row");
    assert!(stale_entry.lifecycle.claim_expired_room().is_some());
    directory.remove_if_current(stale.uuid(), &stale);

    // a reaper pass keeps its cloned entries while `/v1/channel` republishes the
    // issuer, so removal must be re-validated against the current row
    let current = factory.create(ISSUER, TEST_ROOM_KEY.into(), &RoomConfig::default());
    directory.insert(
        Arc::clone(&current),
        None,
        TEST_RESERVATION_TTL,
        TEST_DEPARTURE_GRACE,
    );
    assert_ne!(
        stale.uuid(),
        current.uuid(),
        "a republished issuer should get a fresh uuid"
    );

    directory.remove_if_current(stale.uuid(), &stale);
    directory.remove_if_current(current.uuid(), &stale);

    assert_eq!(
        directory
            .entry_by_issuer(ISSUER)
            .map(|entry| entry.room.uuid().to_owned())
            .as_deref(),
        Some(current.uuid()),
        "a stale expiry must leave the issuer alias pointing at the current room"
    );
}

#[tokio::test(start_paused = true)]
async fn an_elapsed_departure_grace_is_claimed_once_and_repeated_cleanup_cannot_extend_it() {
    let lifecycle = RoomLifecycle::new(TEST_RESERVATION_TTL, TEST_DEPARTURE_GRACE);
    let departure = lifecycle
        .begin()
        .expect("last-user teardown should be accepted");
    // the room was joined, so only the departure below can arm a deadline
    departure.clear_expiration();

    assert!(
        !departure.finish(Some(RoomRemovalPolicy::AfterGrace), true),
        "a departure must not remove the room its grace still holds"
    );
    assert_eq!(
        lifecycle.claim_expired_room(),
        None,
        "an unelapsed grace must not be claimed"
    );

    // a stale close arrives mid-grace and asks for the same removal again
    advance(TEST_DEPARTURE_GRACE / 2).await;
    let repeated_cleanup = lifecycle
        .begin()
        .expect("repeated cleanup should be accepted");
    assert!(!repeated_cleanup.finish(Some(RoomRemovalPolicy::AfterGrace), true));

    advance(TEST_DEPARTURE_GRACE / 2 + Duration::from_millis(1)).await;
    assert_eq!(
        lifecycle.claim_expired_room(),
        Some(ExpiryReason::GraceElapsed),
        "repeated cleanup must not push the original deadline back"
    );
    assert_eq!(
        lifecycle.claim_expired_room(),
        None,
        "later reaper passes must not win again, so `room.destroyed` is logged once"
    );
    assert!(
        lifecycle.begin().is_none(),
        "a claimed departure grace is terminal"
    );
}

#[tokio::test(start_paused = true)]
async fn a_dropped_lease_neither_closes_an_occupied_room_nor_arms_a_grace() {
    let lifecycle = RoomLifecycle::new(TEST_RESERVATION_TTL, TEST_DEPARTURE_GRACE);
    let first_join = lifecycle.begin().expect("a first join should be accepted");
    first_join.clear_expiration();
    drop(first_join);

    let departure = lifecycle
        .begin()
        .expect("last-user teardown should be accepted");
    let rejoin = lifecycle.begin().expect("a rejoin should be accepted");
    assert!(
        !departure.finish(Some(RoomRemovalPolicy::AfterGrace), true),
        "the rejoin still holds a lease, so removal waits for it"
    );

    // the rejoin is cancelled, so it proves nothing about the room it leaves
    // behind and cannot consume the pending removal
    drop(rejoin);

    assert!(
        !lifecycle.has_departure_grace_for_test(),
        "a cancelled rejoin must not arm the grace the departure requested"
    );
    advance(TEST_DEPARTURE_GRACE + Duration::from_millis(1)).await;
    assert_eq!(
        lifecycle.claim_expired_room(),
        None,
        "a room the join retired a deadline for must survive the reaper"
    );
    assert!(
        lifecycle.begin().is_some(),
        "the room should still accept work"
    );
}
