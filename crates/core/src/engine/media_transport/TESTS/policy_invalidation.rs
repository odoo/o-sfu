use std::{
    collections::BTreeSet,
    future::{Future, poll_fn},
    pin::Pin,
    task::Poll,
    time::Duration,
};

use tokio::time::{Instant, advance};

use super::SourcePolicySignal;
use crate::RoomInstanceId;

async fn assert_pending(future: Pin<&mut impl Future>) {
    let mut future = future;
    poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn dirty_marks_coalesce_without_consuming_the_deadline() {
    let signal = SourcePolicySignal::default();
    let subscription = signal.subscribe();
    let room = RoomInstanceId::from_raw(7);
    let deadline = Instant::now() + Duration::from_millis(750);
    signal.set_deadline(room, Some(deadline.into_std()));
    signal.mark_dirty_rooms([room, room]);
    assert_eq!(subscription.wait_for_update().await, BTreeSet::from([room]));
    let wait = subscription.wait_for_update();
    tokio::pin!(wait);
    assert_pending(wait.as_mut()).await;
    advance(Duration::from_millis(749)).await;
    assert_pending(wait.as_mut()).await;
    // Repeating the deadline cannot restart its dwell.
    signal.set_deadline(room, Some(deadline.into_std()));
    advance(Duration::from_millis(1)).await;
    assert_eq!(wait.await, BTreeSet::from([room]));
    assert!(subscription.take_pending_updates().is_empty());
}

#[tokio::test(start_paused = true)]
async fn sleeping_consumer_observes_earlier_and_later_replacements() {
    let signal = SourcePolicySignal::default();
    let subscription = signal.subscribe();
    let room = RoomInstanceId::from_raw(12);
    let now = Instant::now();
    signal.set_deadline(room, Some((now + Duration::from_millis(500)).into_std()));
    let wait = subscription.wait_for_update();
    tokio::pin!(wait);
    assert_pending(wait.as_mut()).await;
    signal.set_deadline(room, Some((now + Duration::from_millis(900)).into_std()));
    assert_pending(wait.as_mut()).await;
    advance(Duration::from_millis(500)).await;
    assert_pending(wait.as_mut()).await;
    signal.set_deadline(room, Some((now + Duration::from_millis(750)).into_std()));
    assert_pending(wait.as_mut()).await;
    advance(Duration::from_millis(249)).await;
    assert_pending(wait.as_mut()).await;
    advance(Duration::from_millis(1)).await;
    assert_eq!(wait.await, BTreeSet::from([room]));
    advance(Duration::from_secs(1)).await;
    assert!(subscription.take_pending_updates().is_empty());
}

#[tokio::test(start_paused = true)]
async fn cancellation_removes_sleep_and_wait_cancellation_preserves_work() {
    let signal = SourcePolicySignal::default();
    let subscription = signal.subscribe();
    let room = RoomInstanceId::from_raw(14);
    let now = Instant::now();
    signal.set_deadline(room, Some((now + Duration::from_millis(750)).into_std()));
    {
        let wait = subscription.wait_for_update();
        tokio::pin!(wait);
        assert_pending(wait.as_mut()).await;
        signal.set_deadline(room, None);
        assert_pending(wait.as_mut()).await;
        advance(Duration::from_secs(1)).await;
        assert_pending(wait.as_mut()).await;
        signal.mark_dirty(room);
    }
    assert_eq!(subscription.wait_for_update().await, BTreeSet::from([room]));
}

#[tokio::test(start_paused = true)]
async fn equal_room_deadlines_and_same_poll_dirty_notification_are_drained() {
    let signal = SourcePolicySignal::default();
    let subscription = signal.subscribe();
    let rooms = [
        RoomInstanceId::from_raw(1),
        RoomInstanceId::from_raw(2),
        RoomInstanceId::from_raw(3),
    ];
    let deadline = Instant::now() + Duration::from_millis(750);
    for room in &rooms[..2] {
        signal.set_deadline(*room, Some(deadline.into_std()));
    }
    let wait = subscription.wait_for_update();
    tokio::pin!(wait);
    assert_pending(wait.as_mut()).await;
    advance(Duration::from_millis(750)).await;
    signal.mark_dirty(rooms[2]);
    let mut woke = wait.await;
    woke.extend(subscription.take_pending_updates());
    assert_eq!(woke, BTreeSet::from(rooms));
}

#[tokio::test(start_paused = true)]
async fn cancelling_earliest_room_preserves_other_rooms() {
    let signal = SourcePolicySignal::default();
    let subscription = signal.subscribe();
    let first = RoomInstanceId::from_raw(1);
    let second = RoomInstanceId::from_raw(2);
    let now = Instant::now();
    signal.set_deadline(first, Some((now + Duration::from_millis(500)).into_std()));
    signal.set_deadline(second, Some((now + Duration::from_millis(750)).into_std()));
    signal.set_deadline(first, None);
    let wait = subscription.wait_for_update();
    tokio::pin!(wait);
    assert_pending(wait.as_mut()).await;
    advance(Duration::from_millis(749)).await;
    assert_pending(wait.as_mut()).await;
    advance(Duration::from_millis(1)).await;
    assert_eq!(wait.await, BTreeSet::from([second]));
}
