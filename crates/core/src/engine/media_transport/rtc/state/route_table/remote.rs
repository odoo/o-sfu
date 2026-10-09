//! Remote identities and their latest-gate outbox share one lifecycle.
//!
//! Gate changes publish immediately. Retry passes run between packet-loop turns
//! and visit each queued registration at most once.

use std::collections::{BTreeMap, VecDeque};

use super::super::{
    super::commands::{RemoteControlSendOutcome, RemoteSourceControl},
    route_control::PacketLayerGate,
};
use crate::engine::media_transport::{TransportAdapterError, TransportMediaId, TransportSourceKey};

#[derive(Debug, Default)]
pub(super) struct RemoteSources {
    registrations: BTreeMap<TransportMediaId, RemoteSourceRegistration>,
    queue: VecDeque<TransportMediaId>,
}

/// Previous registry state for rolling back a registration or replacement.
///
/// Dropping the transaction keeps the new registration. Rollback requires the
/// same registry before another registration change for this source.
pub struct RemoteSourceTransaction {
    pub(super) source_id: TransportMediaId,
    pub(super) previous: Option<RemoteSourceRegistration>,
}

#[derive(Debug)]
pub struct RemoteSourceRegistration {
    source: TransportSourceKey,
    control: RemoteSourceControl,
    pending_gate: Option<PacketLayerGate>,
    // Queue membership can outlast pending_gate until the next flush.
    queued: bool,
}

impl RemoteSources {
    /// # Errors
    ///
    /// Returns [`TransportAdapterError::InvalidInput`] for conflicting source identity.
    pub(super) fn register(
        &mut self,
        source: &TransportSourceKey,
        control: RemoteSourceControl,
    ) -> Result<RemoteSourceTransaction, TransportAdapterError> {
        let source_id = source.transport_media_id();
        if self
            .get(source_id)
            .is_some_and(|current| current.source != *source)
        {
            return Err(TransportAdapterError::InvalidInput);
        }
        // Queue entries belong to a registration, not a reusable media id.
        // Replacement and removal retire them before the id can be reused.
        let previous = self.remove(source_id);
        self.registrations.insert(
            source_id,
            RemoteSourceRegistration {
                source: source.clone(),
                control,
                pending_gate: None,
                queued: false,
            },
        );
        Ok(RemoteSourceTransaction {
            source_id,
            previous,
        })
    }

    pub(super) fn rollback(&mut self, transaction: RemoteSourceTransaction) {
        let RemoteSourceTransaction {
            source_id,
            previous,
        } = transaction;
        self.remove(source_id);
        if let Some(mut registration) = previous {
            registration.queued = registration.pending_gate.is_some();
            if registration.queued {
                self.queue.push_back(source_id);
            }
            self.registrations.insert(source_id, registration);
        }
    }

    pub(super) fn get(&self, source_id: TransportMediaId) -> Option<&RemoteSourceRegistration> {
        self.registrations.get(&source_id)
    }

    pub(super) fn remove(
        &mut self,
        source_id: TransportMediaId,
    ) -> Option<RemoteSourceRegistration> {
        let registration = self.registrations.remove(&source_id)?;
        if registration.queued {
            self.queue.retain(|queued| *queued != source_id);
        }
        Some(registration)
    }

    pub(super) fn publish_gate(&mut self, source_id: TransportMediaId, gate: PacketLayerGate) {
        let Some(registration) = self.registrations.get_mut(&source_id) else {
            return;
        };
        registration.pending_gate = match registration
            .control
            .set_pkt_gate(&registration.source, gate)
        {
            RemoteControlSendOutcome::Forwarded | RemoteControlSendOutcome::Closed => None,
            RemoteControlSendOutcome::Full => Some(gate),
        };
        if registration.pending_gate.is_some() && !registration.queued {
            registration.queued = true;
            self.queue.push_back(source_id);
        }
    }

    pub(super) fn flush(&mut self) {
        if self.queue.is_empty() {
            return;
        }
        // Visit each queued source once in order without cycling saturated
        // entries through pop/push on every pass.
        self.queue.retain(|source_id| {
            self.registrations
                .get_mut(source_id)
                .is_some_and(RemoteSourceRegistration::retry_pending_gate)
        });
    }
}

impl RemoteSourceRegistration {
    fn retry_pending_gate(&mut self) -> bool {
        let Some(gate) = self.pending_gate else {
            self.queued = false;
            return false;
        };
        self.control.record_pkt_gate_retry();
        match self.control.set_pkt_gate(&self.source, gate) {
            RemoteControlSendOutcome::Forwarded => {
                self.control.record_pkt_gate_flushed();
            }
            // Sustained pressure must not clear and restore retry state each turn.
            RemoteControlSendOutcome::Full => return true,
            // A closed worker cannot accept another retry. Retiring its gate
            // prevents repeated retry and drop accounting on later turns.
            RemoteControlSendOutcome::Closed => {}
        }
        self.pending_gate = None;
        self.queued = false;
        false
    }

    pub fn source(&self) -> &TransportSourceKey {
        &self.source
    }

    pub fn cloned_control_path(&self) -> (TransportSourceKey, RemoteSourceControl) {
        (self.source.clone(), self.control.clone())
    }

    #[cfg(test)]
    pub const fn pending_gate(&self) -> Option<PacketLayerGate> {
        self.pending_gate
    }

    #[cfg(any(test, feature = "internal-benchmarks"))]
    pub const fn has_pending_gate(&self) -> bool {
        self.pending_gate.is_some()
    }
}
