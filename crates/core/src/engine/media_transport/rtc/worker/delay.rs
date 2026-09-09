#[cfg(any(test, feature = "testing-transport"))]
use std::sync::atomic::AtomicU8;
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

const NO_HEARTBEAT: u64 = u64::MAX;
const PACKET_LOOP_STARTUP_GRACE: Duration = Duration::from_millis(300);
const PACKET_LOOP_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);
#[cfg(any(test, feature = "testing-transport"))]
const TEST_DELAY_DISABLED: u8 = 0;
#[cfg(any(test, feature = "testing-transport"))]
const TEST_DELAY_NONE: u8 = 1;
#[cfg(any(test, feature = "testing-transport"))]
const TEST_DELAY_SOME: u8 = 2;

/// Recent sustained packet-loop service delay.
///
/// Startup reports zero for `300 ms`. A missed full heartbeat interval returns
/// `None` so placement cannot mistake an unresponsive worker for an idle one.
#[derive(Debug)]
pub struct PacketLoopDelaySnapshot {
    started_at: Instant,
    delay_ms: AtomicU64,
    next_deadline_elapsed_ms: AtomicU64,
    #[cfg(any(test, feature = "testing-transport"))]
    test_delay_ms: AtomicU64,
    #[cfg(any(test, feature = "testing-transport"))]
    test_delay_state: AtomicU8,
}

impl PacketLoopDelaySnapshot {
    pub fn new(started_at: Instant) -> Self {
        Self {
            started_at,
            delay_ms: AtomicU64::new(0),
            next_deadline_elapsed_ms: AtomicU64::new(NO_HEARTBEAT),
            #[cfg(any(test, feature = "testing-transport"))]
            test_delay_ms: AtomicU64::new(0),
            #[cfg(any(test, feature = "testing-transport"))]
            test_delay_state: AtomicU8::new(TEST_DELAY_DISABLED),
        }
    }

    fn publish(&self, delay_ms: Option<u64>, next_deadline: Instant) {
        // Publish the delay before the Release deadline so an Acquire reader cannot
        // observe a new deadline with an older delay.
        self.delay_ms
            .store(delay_ms.unwrap_or(NO_HEARTBEAT), Ordering::Relaxed);
        self.next_deadline_elapsed_ms
            .store(self.elapsed_ms(next_deadline), Ordering::Release);
    }

    pub fn packet_loop_delay_ms_at(&self, now: Instant) -> Option<u64> {
        #[cfg(any(test, feature = "testing-transport"))]
        match self.test_delay_state.load(Ordering::Acquire) {
            TEST_DELAY_NONE => return None,
            TEST_DELAY_SOME => return Some(self.test_delay_ms.load(Ordering::Relaxed)),
            _ => {}
        }
        let next_deadline_elapsed_ms = self.next_deadline_elapsed_ms.load(Ordering::Acquire);
        let now_elapsed_ms = self.elapsed_ms(now);
        if next_deadline_elapsed_ms == NO_HEARTBEAT {
            return (now_elapsed_ms <= millis_u64(PACKET_LOOP_STARTUP_GRACE)).then_some(0);
        }
        if now_elapsed_ms
            >= next_deadline_elapsed_ms.saturating_add(millis_u64(PACKET_LOOP_HEARTBEAT_INTERVAL))
        {
            return None;
        }
        match self.delay_ms.load(Ordering::Relaxed) {
            NO_HEARTBEAT => None,
            delay_ms => Some(delay_ms),
        }
    }

    fn elapsed_ms(&self, instant: Instant) -> u64 {
        millis_u64(instant.saturating_duration_since(self.started_at))
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(in crate::engine::media_transport) fn set_for_test(&self, delay_ms: Option<u64>) {
        let state = delay_ms.map_or(TEST_DELAY_NONE, |delay_ms| {
            self.test_delay_ms.store(delay_ms, Ordering::Relaxed);
            TEST_DELAY_SOME
        });
        self.test_delay_state.store(state, Ordering::Release);
    }
}

pub(super) struct PacketLoopDelayPublisher {
    deadline: Instant,
    previous_delay_ms: u64,
}

impl PacketLoopDelayPublisher {
    pub(super) fn new(started_at: Instant) -> Self {
        Self {
            deadline: started_at + PACKET_LOOP_HEARTBEAT_INTERVAL,
            previous_delay_ms: 0,
        }
    }

    pub(super) const fn deadline(&self) -> Instant {
        self.deadline
    }

    pub(super) fn observe(&mut self, snapshot: &PacketLoopDelaySnapshot, observed_at: Instant) {
        if observed_at < self.deadline {
            return;
        }
        let delay_ms = millis_u64(observed_at.saturating_duration_since(self.deadline));
        let sustained_delay_ms = if delay_ms >= millis_u64(PACKET_LOOP_HEARTBEAT_INTERVAL) {
            None
        } else {
            Some(self.previous_delay_ms.min(delay_ms))
        };
        self.previous_delay_ms = delay_ms;
        self.deadline = observed_at + PACKET_LOOP_HEARTBEAT_INTERVAL;
        snapshot.publish(sustained_delay_ms, self.deadline);
    }
}

fn millis_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
#[path = "TESTS/delay.rs"]
mod tests;
