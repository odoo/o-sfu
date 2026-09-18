//! room reservation expiry coordination
//!
//! a room published by `/v1/channel` carries a reservation deadline until a
//! user successfully joins it. these tests pin the orderings that decide
//! whether expiry or room work wins, and the directory indexes an expiry is
//! allowed to touch
//!
//! the deadline is a [`tokio::time::Instant`], so these tests move the clock
//! explicitly instead of sleeping. they spawn no tasks, which keeps paused-time
//! auto-advance from reaching any other timer

use std::{sync::Arc, time::Duration};

use tokio::time::advance;

use super::{
    super::{
        RoomAdmissionPolicy, RoomConfig, RoomRuntimePolicy,
        directory::{RoomDirectory, RoomLifecycle},
        factory::RoomFactory,
    },
    fixtures::{TEST_ROOM_KEY, test_client_rtp_capabilities},
};
use crate::{RuntimeFeatureFlags, engine::metrics::RuntimeMetrics};

const TEST_RESERVATION_TTL: Duration = Duration::from_mins(1);

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
    let lifecycle = RoomLifecycle::new(TEST_RESERVATION_TTL);

    drop(
        lifecycle
            .begin()
            .expect("work should be accepted before the deadline"),
    );
    assert!(
        !lifecycle.claim_expired_reservation(),
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
    assert!(
        lifecycle.claim_expired_reservation(),
        "the first claim after the deadline should win directory removal"
    );
    assert!(
        !lifecycle.claim_expired_reservation(),
        "later reaper passes must not win again, so the room gauge drops once"
    );
    assert!(
        lifecycle.begin().is_none(),
        "a claimed reservation is terminal"
    );
}

#[tokio::test(start_paused = true)]
async fn work_that_leaves_the_room_empty_keeps_the_reservation() {
    let lifecycle = RoomLifecycle::new(TEST_RESERVATION_TTL);
    let lease = lifecycle
        .begin()
        .expect("a join that will fail is still accepted");

    advance_past_reservation_deadline().await;
    assert!(
        !lease.finish(false, true),
        "an empty room is not removed without a removal request"
    );
    assert!(
        lifecycle.claim_expired_reservation(),
        "only a successful join may retire the reservation"
    );
}

#[tokio::test(start_paused = true)]
async fn empty_room_removal_wins_over_a_later_expiry_claim() {
    let lifecycle = RoomLifecycle::new(TEST_RESERVATION_TTL);
    let lease = lifecycle
        .begin()
        .expect("last-user teardown should be accepted");

    assert!(
        lease.finish(true, true),
        "the last-user finisher should win directory removal"
    );

    advance_past_reservation_deadline().await;
    assert!(
        !lifecycle.claim_expired_reservation(),
        "expiry must not remove a room the last-user path already removed"
    );
}

#[tokio::test(start_paused = true)]
async fn stale_expiry_never_removes_a_newer_room_for_the_same_issuer() {
    const ISSUER: &str = "issuer-stale-expiry";

    let factory = test_factory();
    let mut directory = RoomDirectory::default();
    let stale = factory.create(ISSUER, TEST_ROOM_KEY.into(), &RoomConfig::default());
    directory.insert(Arc::clone(&stale), None, TEST_RESERVATION_TTL);

    advance_past_reservation_deadline().await;
    let stale_entry = directory
        .entry(stale.uuid())
        .expect("the first room should be a current row");
    assert!(stale_entry.lifecycle.claim_expired_reservation());
    directory.remove_if_current(stale.uuid(), &stale);

    // a reaper pass keeps its cloned entries while `/v1/channel` republishes the
    // issuer, so removal must be re-validated against the current row
    let current = factory.create(ISSUER, TEST_ROOM_KEY.into(), &RoomConfig::default());
    directory.insert(Arc::clone(&current), None, TEST_RESERVATION_TTL);
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
