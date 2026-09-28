use KeyframeRequestKind::{Fir, Pli};
use PublisherRequestDemand::{Local, Relay};

use super::*;

#[test]
fn idle_sources_do_not_fill_the_publisher_deadline_queue() {
    let mut limiter = PublisherKeyframeLimiter::default();
    let start = Instant::now();
    for source_id in 1..=1_024 {
        let source = TransportMediaId::new(source_id);
        assert!(
            limiter
                .request(source, None, Pli, Local(None), start, |_| true)
                .is_some()
        );
        limiter.sent(source, None, start);
    }
    assert_eq!(limiter.targets.len(), 1_024);
    assert!(limiter.deadlines.is_empty());
    for _ in 0..1_024 {
        assert!(
            limiter
                .request(
                    TransportMediaId::new(1),
                    None,
                    Pli,
                    Local(None),
                    start + Duration::from_millis(100),
                    |_| true
                )
                .is_none()
        );
        assert_eq!(limiter.deadlines.len(), 1);
        limiter.observe_refresh(TransportMediaId::new(1), None);
        assert!(limiter.deadlines.is_empty());
    }
    assert_eq!(limiter.next_deadline(), None);
}

#[test]
fn refreshes_cannot_reset_the_publisher_dispatch_interval() {
    let mut limiter = PublisherKeyframeLimiter::default();
    let source = TransportMediaId::new(201);
    let rid = Some(Rid::from("hi"));
    let start = Instant::now();
    let mut dispatched = 0;
    for step in 0..10 {
        let now = start + Duration::from_millis(step * 100);
        if limiter
            .request(source, rid, Pli, Local(rid), now, |_| true)
            .is_some()
        {
            limiter.sent(source, rid, now);
            dispatched += 1;
        }
        limiter.observe_refresh(source, rid);
    }
    assert_eq!(dispatched, 4);
    assert_eq!(limiter.next_deadline(), None);
}

#[test]
fn cooldown_merges_fir_and_cancels_retired_relay_demand() {
    let mut limiter = PublisherKeyframeLimiter::default();
    let source = TransportMediaId::new(202);
    let first_relay = RelayTargetId::new(1);
    let second_relay = RelayTargetId::new(2);
    let start = Instant::now();
    assert!(
        limiter
            .request(source, None, Pli, Relay(first_relay), start, |_| true)
            .is_some()
    );
    limiter.sent(source, None, start);
    assert!(
        limiter
            .request(
                source,
                None,
                Pli,
                Relay(first_relay),
                start + Duration::from_millis(100),
                |_| true
            )
            .is_none()
    );
    assert!(
        limiter
            .request(
                source,
                None,
                Fir,
                Relay(second_relay),
                start + Duration::from_millis(200),
                |_| true
            )
            .is_none()
    );
    limiter.retire_relay(source, first_relay);
    let Some(due) = limiter.take_due(start + Duration::from_millis(300)) else {
        panic!("merged publisher request should become due");
    };
    assert_eq!(
        due.active_kind(|demand| demand == Relay(second_relay)),
        Some(Fir)
    );
    limiter.sent(source, None, start + Duration::from_millis(300));
    assert!(
        limiter
            .request(
                source,
                None,
                Pli,
                Relay(second_relay),
                start + Duration::from_millis(400),
                |_| true
            )
            .is_none()
    );
    limiter.retire_relay(source, second_relay);
    assert_eq!(limiter.next_deadline(), None);
}

#[test]
fn retired_local_fir_does_not_strengthen_a_new_consumer_pli() {
    let mut limiter = PublisherKeyframeLimiter::default();
    let source = TransportMediaId::new(204);
    let start = Instant::now();
    assert_eq!(
        limiter.request(source, None, Pli, Local(None), start, |_| true,),
        Some(Pli)
    );
    limiter.sent(source, None, start);
    assert_eq!(
        limiter.request(
            source,
            None,
            Fir,
            Local(None),
            start + Duration::from_millis(100),
            |_| true,
        ),
        None
    );
    limiter.retire_local(source);
    assert_eq!(limiter.next_deadline(), None);
    assert_eq!(
        limiter.request(
            source,
            None,
            Pli,
            Local(None),
            start + Duration::from_millis(300),
            |_| true,
        ),
        Some(Pli)
    );
}

#[test]
fn refresh_cancels_deferred_request_but_preserves_last_dispatch() {
    let mut limiter = PublisherKeyframeLimiter::default();
    let source = TransportMediaId::new(203);
    let rid = Some(Rid::from("lo"));
    let start = Instant::now();
    assert!(
        limiter
            .request(source, rid, Pli, Local(rid), start, |_| true)
            .is_some()
    );
    limiter.sent(source, rid, start);
    assert!(
        limiter
            .request(
                source,
                rid,
                Pli,
                Local(rid),
                start + Duration::from_millis(100),
                |_| true
            )
            .is_none()
    );
    limiter.observe_refresh(source, rid);
    limiter.cancel_source(source);
    assert_eq!(limiter.next_deadline(), None);
    assert!(
        limiter
            .request(
                source,
                rid,
                Pli,
                Local(rid),
                start + Duration::from_millis(200),
                |_| true
            )
            .is_none()
    );
    assert!(
        limiter
            .request(
                source,
                rid,
                Pli,
                Local(rid),
                start + Duration::from_millis(500),
                |_| true
            )
            .is_some()
    );
    limiter.sent(source, rid, start + Duration::from_millis(500));
    assert!(
        limiter
            .request(
                source,
                rid,
                Pli,
                Local(rid),
                start + Duration::from_millis(600),
                |_| true
            )
            .is_none()
    );
    assert!(limiter.next_deadline().is_some());
    limiter.forget_source(source);
    assert_eq!(limiter.next_deadline(), None);
}
