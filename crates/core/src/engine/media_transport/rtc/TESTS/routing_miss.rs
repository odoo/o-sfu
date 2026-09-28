use std::{
    net::{Ipv4Addr, SocketAddr},
    time::{Duration, Instant},
};

use super::{
    DemuxRecoveryState, PacketLoopRoutingMissKey, UNKNOWN_SOURCE_MISS_BURST_LIMIT,
    UnknownSourceRateLimiter,
};

impl DemuxRecoveryState {
    pub(crate) fn tracked_source_count(&self) -> usize {
        self.source_rate_limiter.entries.len()
    }
}

#[test]
fn unknown_source_rate_limiter_blocks_after_burst_and_recovers_after_cooldown() {
    let source_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 44_000));
    let mut limiter = UnknownSourceRateLimiter::default();
    let start = Instant::now();

    let mut now = start;
    for offset in 0..UNKNOWN_SOURCE_MISS_BURST_LIMIT {
        assert!(!limiter.is_blocked(source_addr, now));
        assert_eq!(
            limiter.record_miss(source_addr, now),
            offset == UNKNOWN_SOURCE_MISS_BURST_LIMIT - 1
        );
        now += Duration::from_millis(1);
    }

    assert!(limiter.is_blocked(source_addr, start + Duration::from_millis(4)));
    assert!(limiter.is_blocked(source_addr, start + Duration::from_millis(199)));
    assert!(!limiter.is_blocked(source_addr, start + Duration::from_millis(203)));
}

#[test]
fn route_success_clears_source_rate_limit_state() {
    let source_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 44_010));
    let candidate_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 44_011));
    let mut demux = DemuxRecoveryState::new();
    let start = Instant::now();
    let packet = [0x80, 0x60, 0x00, 0x01];
    let miss_key = PacketLoopRoutingMissKey::new(source_addr, candidate_addr, &packet);

    let mut now = start;
    for offset in 0..UNKNOWN_SOURCE_MISS_BURST_LIMIT {
        assert_eq!(
            demux.record_miss(miss_key, &packet, source_addr, now),
            offset == UNKNOWN_SOURCE_MISS_BURST_LIMIT - 1
        );
        now += Duration::from_millis(1);
    }

    assert!(demux.is_source_blocked(source_addr, start + Duration::from_millis(4),));

    demux.record_fallback_route_success(source_addr);

    assert!(!demux.is_source_blocked(source_addr, start + Duration::from_millis(5),));
}

#[test]
fn accepted_source_invalidates_only_its_exact_misses() {
    let accepted_source = SocketAddr::from((Ipv4Addr::LOCALHOST, 44_020));
    let unrelated_source = SocketAddr::from((Ipv4Addr::LOCALHOST, 44_021));
    let candidate = SocketAddr::from((Ipv4Addr::LOCALHOST, 44_022));
    let mut demux = DemuxRecoveryState::new();
    let packet = [0x80, 0x60, 0x00, 0x01];
    let now = Instant::now();
    let accepted_miss = PacketLoopRoutingMissKey::new(accepted_source, candidate, &packet);
    let unrelated_miss = PacketLoopRoutingMissKey::new(unrelated_source, candidate, &packet);
    demux.record_miss(accepted_miss, &packet, accepted_source, now);
    demux.record_miss(unrelated_miss, &packet, unrelated_source, now);
    demux.record_fallback_route_success(accepted_source);
    assert!(!demux.should_skip_scan(accepted_miss, &packet));
    assert!(demux.should_skip_scan(unrelated_miss, &packet));
}
