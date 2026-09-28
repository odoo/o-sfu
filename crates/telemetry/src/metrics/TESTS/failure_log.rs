use super::*;

#[test]
fn repeated_failures_are_counted_and_reported_on_next_sample() {
    let start = Instant::now();
    let mut slot = FailureLogSlot::default();
    assert_eq!(slot.observe(start), Some(0));
    assert_eq!(slot.observe(start + Duration::from_secs(1)), None);
    assert_eq!(slot.observe(start + Duration::from_secs(2)), None);
    assert_eq!(slot.observe(start + FAILURE_LOG_INTERVAL), Some(2));
    assert_eq!(slot.observe(start + FAILURE_LOG_INTERVAL * 2), Some(0));
}
