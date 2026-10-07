use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use o_sfu_router::MediaWorkerId;

use crate::engine::sync::lock_unpoisoned;

/// Mapping from ufrag to the worker id that handles the corresponding session. Thread
/// safe. This is a hint, and [`str0m::Rtc::accepts`] stays the authority on whether that
/// worker session owns a given datagram.
#[derive(Clone, Default)]
pub(in crate::engine::media_transport) struct UfragWorkerMap {
    worker_id_by_ufrag: Arc<Mutex<HashMap<String, MediaWorkerId>>>,
}
impl UfragWorkerMap {
    /// Assign the given ufrag to the given worker id.
    pub(in super::super) fn insert(&self, ufrag: &str, worker_id: MediaWorkerId) {
        let owned_ufrag = ufrag.to_owned();
        lock_unpoisoned(&self.worker_id_by_ufrag).insert(owned_ufrag, worker_id);
    }
    /// Remove the given ufrag from the map.
    pub(in super::super) fn remove(&self, ufrag: &str) {
        lock_unpoisoned(&self.worker_id_by_ufrag).remove(ufrag);
    }
    /// Get the worker id handling the given ufrag, if any.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "will be read by the TCP acceptor")
    )]
    pub(in super::super) fn get(&self, ufrag: &str) -> Option<MediaWorkerId> {
        lock_unpoisoned(&self.worker_id_by_ufrag)
            .get(ufrag)
            .copied()
    }
}
