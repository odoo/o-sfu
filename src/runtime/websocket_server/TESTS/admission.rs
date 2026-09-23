use std::{
    net::{IpAddr, Ipv6Addr},
    time::Instant,
};

use super::{
    PreAuthWebSocketAdmission, PreAuthWebSocketAdmissionRejection, REJECTION_LOG_BURST,
    REJECTION_LOG_INTERVAL, RejectionLogBudget,
};
use crate::config::{
    DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS, DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN,
};

#[test]
fn seventeenth_address_in_one_ipv6_subnet_is_rejected() -> anyhow::Result<()> {
    let admission = PreAuthWebSocketAdmission::new(
        DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS,
        DEFAULT_MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN,
    );
    let prefix = "2001:db8:1::".parse::<Ipv6Addr>()?.to_bits();
    let permits = (0..16)
        .map(|suffix| admission.try_acquire(Some(IpAddr::V6(Ipv6Addr::from_bits(prefix | suffix)))))
        .collect::<Result<Vec<_>, _>>();
    assert!(permits.is_ok());
    assert!(matches!(
        admission.try_acquire(Some("2001:db8:1::ffff".parse()?)),
        Err(PreAuthWebSocketAdmissionRejection::Origin)
    ));
    assert!(
        admission
            .try_acquire(Some("2001:db8:2::1".parse()?))
            .is_ok()
    );
    drop(permits);
    assert!(
        admission
            .try_acquire(Some("2001:db8:1::ffff".parse()?))
            .is_ok()
    );
    Ok(())
}

#[test]
fn ipv4_mapped_address_cannot_bypass_origin_capacity() -> anyhow::Result<()> {
    let admission = PreAuthWebSocketAdmission::new(10, 1);
    let first = admission.try_acquire(Some("198.51.100.7".parse()?));
    assert!(first.is_ok());
    assert!(matches!(
        admission.try_acquire(Some("::ffff:198.51.100.7".parse()?)),
        Err(PreAuthWebSocketAdmissionRejection::Origin)
    ));
    drop(first);
    assert!(super::lock_origins(&admission.origins).is_empty());
    assert!(
        admission
            .try_acquire(Some("::ffff:198.51.100.7".parse()?))
            .is_ok()
    );
    Ok(())
}

#[test]
fn rejection_logs_have_one_fixed_budget_and_report_suppressed_count() {
    let now = Instant::now();
    let mut budget = RejectionLogBudget::new(now);
    for _ in 0..REJECTION_LOG_BURST {
        assert_eq!(budget.admit(now), Some(0));
    }
    for _ in 0..1000 {
        assert_eq!(budget.admit(now), None);
    }
    let next_window = now + REJECTION_LOG_INTERVAL;
    assert_eq!(budget.admit(next_window), Some(1000));
    assert_eq!(budget.admit(next_window), Some(0));
}
