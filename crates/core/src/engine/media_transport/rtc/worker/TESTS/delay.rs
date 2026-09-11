use super::*;

#[test]
fn delay_requires_two_consecutive_late_heartbeats() {
    let started_at = Instant::now();
    let snapshot = PacketLoopDelaySnapshot::new(started_at);
    let mut publisher = PacketLoopDelayPublisher::new(started_at);

    publisher.observe(&snapshot, started_at + Duration::from_millis(125));
    assert_eq!(
        snapshot.packet_loop_delay_ms_at(started_at + Duration::from_millis(125)),
        Some(0)
    );

    publisher.observe(&snapshot, started_at + Duration::from_millis(250));
    assert_eq!(
        snapshot.packet_loop_delay_ms_at(started_at + Duration::from_millis(250)),
        Some(25)
    );

    publisher.observe(&snapshot, started_at + Duration::from_millis(355));
    assert_eq!(
        snapshot.packet_loop_delay_ms_at(started_at + Duration::from_millis(355)),
        Some(5)
    );
}

#[test]
fn absent_heartbeat_is_unhealthy_after_one_full_missed_interval() {
    let started_at = Instant::now();
    let snapshot = PacketLoopDelaySnapshot::new(started_at);
    let mut publisher = PacketLoopDelayPublisher::new(started_at);

    publisher.observe(&snapshot, started_at + Duration::from_millis(100));

    assert_eq!(
        snapshot.packet_loop_delay_ms_at(started_at + Duration::from_millis(299)),
        Some(0)
    );
    assert_eq!(
        snapshot.packet_loop_delay_ms_at(started_at + Duration::from_millis(300)),
        None
    );
}

#[test]
fn heartbeat_observed_one_interval_late_is_unhealthy() {
    let started_at = Instant::now();
    let snapshot = PacketLoopDelaySnapshot::new(started_at);
    let mut publisher = PacketLoopDelayPublisher::new(started_at);

    publisher.observe(&snapshot, started_at + Duration::from_millis(250));

    assert_eq!(
        snapshot.packet_loop_delay_ms_at(started_at + Duration::from_millis(250)),
        None
    );
}

#[test]
fn timely_heartbeat_recovers_after_a_missed_interval() {
    let started_at = Instant::now();
    let snapshot = PacketLoopDelaySnapshot::new(started_at);
    let mut publisher = PacketLoopDelayPublisher::new(started_at);

    publisher.observe(&snapshot, started_at + Duration::from_millis(250));
    publisher.observe(&snapshot, started_at + Duration::from_millis(350));

    assert_eq!(
        snapshot.packet_loop_delay_ms_at(started_at + Duration::from_millis(350)),
        Some(0)
    );
}

#[test]
fn startup_grace_expires_without_a_first_heartbeat() {
    let started_at = Instant::now();
    let snapshot = PacketLoopDelaySnapshot::new(started_at);

    assert_eq!(
        snapshot.packet_loop_delay_ms_at(started_at + Duration::from_millis(300)),
        Some(0)
    );
    assert_eq!(
        snapshot.packet_loop_delay_ms_at(started_at + Duration::from_millis(301)),
        None
    );
}
