use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Debug, Clone)]
pub(crate) struct PreAuthWebSocketAdmission {
    global: Arc<Semaphore>,
    per_origin_capacity: usize,
    origins: Arc<Mutex<HashMap<Arc<str>, Arc<Semaphore>>>>,
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
    origin: Arc<str>,
    origins: Arc<Mutex<HashMap<Arc<str>, Arc<Semaphore>>>>,
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
        origin: Arc<str>,
    ) -> Result<PreAuthWebSocketPermit, PreAuthWebSocketAdmissionRejection> {
        let global_permit = Arc::clone(&self.global)
            .try_acquire_owned()
            .map_err(|_error| PreAuthWebSocketAdmissionRejection::Global)?;
        let mut origins = lock_origins(&self.origins);
        let origin_semaphore = origins
            .entry(Arc::clone(&origin))
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
    origins: &Mutex<HashMap<Arc<str>, Arc<Semaphore>>>,
) -> MutexGuard<'_, HashMap<Arc<str>, Arc<Semaphore>>> {
    origins.lock().unwrap_or_else(PoisonError::into_inner)
}
