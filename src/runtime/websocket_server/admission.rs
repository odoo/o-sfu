use std::{
    collections::HashMap,
    mem,
    net::{IpAddr, Ipv6Addr},
    sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError},
    time::{Duration, Instant},
};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Debug, Clone)]
pub(crate) struct PreAuthWebSocketAdmission {
    global: Arc<Semaphore>,
    per_origin_capacity: usize,
    origins: Arc<Mutex<HashMap<Option<IpAddr>, Arc<Semaphore>>>>,
}

/// holds global and origin pre-auth capacity until authentication releases it
/// or the upgraded socket is dropped
///
/// dropping the permit removes idle origin buckets after the last origin permit
/// returns
#[derive(Debug)]
pub(super) struct PreAuthWebSocketPermit {
    _global_permit: OwnedSemaphorePermit,
    origin_permit: Option<OwnedSemaphorePermit>,
    origin: Option<IpAddr>,
    origins: Arc<Mutex<HashMap<Option<IpAddr>, Arc<Semaphore>>>>,
    per_origin_capacity: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PreAuthWebSocketAdmissionRejection {
    Global,
    Origin,
}

impl PreAuthWebSocketAdmission {
    #[must_use]
    pub(crate) fn new(global_capacity: usize, per_origin_capacity: usize) -> Self {
        debug_assert!(global_capacity > 0);
        debug_assert!(per_origin_capacity > 0);
        Self {
            global: Arc::new(Semaphore::new(global_capacity)),
            per_origin_capacity,
            origins: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(super) fn try_acquire(
        &self,
        origin: Option<IpAddr>,
    ) -> Result<PreAuthWebSocketPermit, PreAuthWebSocketAdmissionRejection> {
        let origin = origin.map(origin_bucket);
        let global_permit = Arc::clone(&self.global)
            .try_acquire_owned()
            .map_err(|_error| PreAuthWebSocketAdmissionRejection::Global)?;
        let mut origins = lock_origins(&self.origins);
        let origin_semaphore = origins
            .entry(origin)
            .or_insert_with(|| Arc::new(Semaphore::new(self.per_origin_capacity)));
        let origin_permit = Arc::clone(origin_semaphore)
            .try_acquire_owned()
            .map_err(|_error| PreAuthWebSocketAdmissionRejection::Origin)?;
        drop(origins);
        Ok(PreAuthWebSocketPermit {
            _global_permit: global_permit,
            origin_permit: Some(origin_permit),
            origin,
            origins: Arc::clone(&self.origins),
            per_origin_capacity: self.per_origin_capacity,
        })
    }
}

impl Drop for PreAuthWebSocketPermit {
    fn drop(&mut self) {
        drop(self.origin_permit.take());
        let mut origins = lock_origins(&self.origins);
        let should_remove = origins
            .get(&self.origin)
            .is_some_and(|semaphore| semaphore.available_permits() == self.per_origin_capacity);
        if should_remove {
            origins.remove(&self.origin);
        }
    }
}

fn lock_origins(
    origins: &Mutex<HashMap<Option<IpAddr>, Arc<Semaphore>>>,
) -> MutexGuard<'_, HashMap<Option<IpAddr>, Arc<Semaphore>>> {
    origins.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One IPv6 /64 shares a bucket so rotating interface addresses cannot evade
/// admission. IPv4-mapped IPv6 shares the IPv4 bucket (RFC 4291 section 2.5.5.2).
fn origin_bucket(address: IpAddr) -> IpAddr {
    match address.to_canonical() {
        address @ IpAddr::V4(_) => address,
        IpAddr::V6(address) => {
            IpAddr::V6(Ipv6Addr::from_bits(address.to_bits() & (u128::MAX << 64)))
        }
    }
}

// All origins share one budget. An attacker cannot allocate logging state by
// rotating addresses or make a flood expensive merely by getting rejected.
static REJECTION_LOG_BUDGET: LazyLock<Mutex<RejectionLogBudget>> =
    LazyLock::new(|| Mutex::new(RejectionLogBudget::new(Instant::now())));
const REJECTION_LOG_INTERVAL: Duration = Duration::from_secs(1);
const REJECTION_LOG_BURST: u8 = 5;

struct RejectionLogBudget {
    window_started: Instant,
    remaining: u8,
    suppressed: u64,
}

impl RejectionLogBudget {
    fn new(now: Instant) -> Self {
        Self {
            window_started: now,
            remaining: REJECTION_LOG_BURST,
            suppressed: 0,
        }
    }

    /// Reports suppressed rejections on the next admitted log after refill.
    fn admit(&mut self, now: Instant) -> Option<u64> {
        if now.saturating_duration_since(self.window_started) >= REJECTION_LOG_INTERVAL {
            self.window_started = now;
            self.remaining = REJECTION_LOG_BURST;
        }
        if self.remaining == 0 {
            self.suppressed = self.suppressed.saturating_add(1);
            return None;
        }
        self.remaining -= 1;
        Some(mem::take(&mut self.suppressed))
    }
}

/// Reserves one rejection log and returns the count suppressed since the last log.
///
/// The budget covers both rejected upgrades and authentication failures.
pub(super) fn admit_rejection_log() -> Option<u64> {
    REJECTION_LOG_BUDGET
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .admit(Instant::now())
}

#[cfg(test)]
#[path = "TESTS/admission.rs"]
mod tests;
