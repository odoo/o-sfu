use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering as AtomicOrdering},
    },
    thread,
};

use super::*;
use crate::engine::{UserId, media_transport::rtc::test_support::test_transport_session_key};

#[test]
fn incoming_media_bitrate_reports_a_completed_window() {
    let now = Instant::now();
    let bitrate = MediaBitrateCounter::new(now);

    assert_eq!(
        bitrate.record(now, 120),
        IncomingBitrateObservation::IngressStarted
    );
    assert_eq!(bitrate.snapshot(now), Bitrate::zero());
    bitrate.record(now + Duration::from_millis(500), 120);
    assert_eq!(
        bitrate.record(now + Duration::from_secs(1), 30),
        IncomingBitrateObservation::SampleUpdated
    );

    assert_eq!(
        bitrate.snapshot(now + Duration::from_secs(1)),
        Bitrate::from_bps(1_920)
    );
}

#[test]
fn incoming_media_bitrate_normalizes_partial_second_windows() {
    let now = Instant::now();
    let bitrate = MediaBitrateCounter::new(now);

    bitrate.record(now, 120);
    bitrate.record(now + Duration::from_millis(600), 30);
    assert_eq!(
        bitrate.record(now + Duration::from_millis(1_200), 10),
        IncomingBitrateObservation::SampleUpdated
    );

    assert_eq!(
        bitrate.snapshot(now + Duration::from_millis(1_200)),
        Bitrate::from_bps(1_000)
    );
}

#[test]
fn incoming_media_bitrate_resets_after_inactivity() {
    let now = Instant::now();
    let bitrate = MediaBitrateCounter::new(now);

    bitrate.record(now, 64);
    let observation = bitrate.record(now + Duration::from_secs(1), 32);

    assert_eq!(observation, IncomingBitrateObservation::IngressStarted);
    assert_eq!(
        bitrate.snapshot(now + Duration::from_secs(1)),
        Bitrate::zero()
    );
}

#[test]
fn incoming_media_bitrate_expires_after_the_window() {
    let now = Instant::now();
    let bitrate = MediaBitrateCounter::new(now);

    bitrate.record(now, 64);
    bitrate.record(now + Duration::from_millis(500), 64);
    bitrate.record(now + Duration::from_secs(1), 64);

    assert_eq!(
        bitrate.snapshot(now + Duration::from_secs(1)),
        Bitrate::from_bps(1_024)
    );
    assert_eq!(
        bitrate.snapshot(now + Duration::from_secs(2)),
        Bitrate::zero()
    );
}

#[test]
fn egress_bitrate_snapshot_reports_recent_session_bits() {
    let now = Instant::now();
    let session_key = test_transport_session_key(0, 0, 0, UserId::Integer(7));
    let mut state = BitrateRegistry::default();
    let counter = Arc::new(MediaBitrateCounter::new(now));
    state.register_session_egress(&session_key, Arc::clone(&counter));

    counter.record(now, 125);
    counter.record(now + Duration::from_millis(500), 125);
    counter.record(now + Duration::from_secs(1), 1);

    assert_eq!(
        state.egress_bitrate_snapshot_at(&[session_key], now + Duration::from_secs(1)),
        Bitrate::from_bps(2_000)
    );
}

#[test]
fn incoming_media_bitrate_first_observation_fires_once() {
    let now = Instant::now();
    let bitrate = MediaBitrateCounter::new(now);

    assert!(bitrate.record(now, 1).ingress_started());
    assert!(!bitrate.record(now, 1).ingress_started());
}

#[test]
fn bitrate_snapshot_observes_packet_loop_thread_writes() {
    let now = Instant::now();
    let bitrate = Arc::new(MediaBitrateCounter::new(now));
    let writer = Arc::clone(&bitrate);
    let started = Arc::new(AtomicBool::new(false));
    let writer_started = Arc::clone(&started);

    let handle = thread::spawn(move || {
        writer_started.store(true, AtomicOrdering::Release);
        writer.record(now, 10);
        for _ in 0..1024 {
            writer.record(now + Duration::from_millis(500), 10);
        }
        writer.record(now + Duration::from_secs(1), 10);
    });

    while !started.load(AtomicOrdering::Acquire) {
        thread::yield_now();
    }

    let mut observed_bitrate = Bitrate::zero();
    for _ in 0..1024 {
        observed_bitrate = bitrate.snapshot(now + Duration::from_secs(1));
        if observed_bitrate > Bitrate::zero() {
            break;
        }
        thread::yield_now();
    }

    assert!(handle.join().is_ok());
    assert!(
        observed_bitrate > Bitrate::zero()
            || bitrate.snapshot(now + Duration::from_secs(1)) > Bitrate::zero()
    );
}

#[test]
fn packet_loop_counter_write_does_not_need_the_snapshot_lock() {
    let mut shared_registry = BitrateRegistry::default();
    let mut packet_loop_state = PacketLoopState::default();
    let now = Instant::now();
    let session_key = test_transport_session_key(1, 0, 2, UserId::Integer(3));
    let media_id = TransportMediaId::new(4);
    let counter = shared_registry.register_incoming_media(&session_key, media_id, now);
    packet_loop_state.register_incoming_bitrate_counter(media_id, counter);
    let shared_registry = Mutex::new(shared_registry);
    let Ok(_snapshot_guard) = shared_registry.lock() else {
        return;
    };

    assert_eq!(
        packet_loop_state.record_incoming_bitrate(media_id, now, 32),
        Some(IncomingBitrateObservation::IngressStarted)
    );
}
