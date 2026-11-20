//! Remote identities and their latest-control outbox share one lifecycle.
//!
//! A registration announces this worker as a relay target of its source, then
//! publishes gate changes. Sends never wait. A full source queue keeps the
//! latest add or gate for retry passes between packet-loop turns and a closed
//! one ends delivery. A removal outlives its registration until the source
//! worker accepts it, unless a new registration of the source replaces it.

use std::collections::{BTreeMap, BTreeSet, btree_map::Entry};

use super::super::{
    super::commands::{RemoteControlSendOutcome, RemoteSourceControl},
    route_control::PacketLayerGate,
};
use crate::engine::media_transport::{TransportAdapterError, TransportMediaId, TransportSourceKey};

#[derive(Debug, Default)]
pub(super) struct RemoteSources {
    registrations: BTreeMap<TransportMediaId, RemoteSourceRegistration>,
    /// Removed registrations whose withdrawal the source queue has not
    /// accepted yet.
    releases: BTreeMap<TransportMediaId, RemoteSourceRegistration>,
    /// Sources with an undelivered add, gate or removal. Membership must clear
    /// on immediate completion too, since it schedules idle retry wakes.
    pending: BTreeSet<TransportMediaId>,
}

/// Registration created by one consumer setup.
///
/// Rollback requires the same registry before another registration change for
/// this source. Dropping the transaction keeps the registration.
pub struct RemoteSourceTransaction {
    pub(super) source_id: TransportMediaId,
}

#[derive(Debug)]
pub struct RemoteSourceRegistration {
    source: TransportSourceKey,
    control: RemoteSourceControl,
    gate: PacketLayerGate,
    delivery: Delivery,
}

/// Control the source worker has not accepted yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Delivery {
    Add { replacing: bool },
    Gate,
    Done,
}

impl RemoteSources {
    /// Registers `source` and announces its relay target, or keeps a current
    /// registration of the same source.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError::InvalidInput`] for conflicting source
    /// identity and [`TransportAdapterError::TransportUnavailable`] when the
    /// source worker queue is closed.
    pub(super) fn register(
        &mut self,
        source: &TransportSourceKey,
        control: RemoteSourceControl,
    ) -> Result<Option<RemoteSourceTransaction>, TransportAdapterError> {
        let source_id = source.transport_media_id();
        if let Some(current) = self.get(source_id) {
            if current.source != *source {
                return Err(TransportAdapterError::InvalidInput);
            }
            return Ok(None);
        }
        // The source worker still holds the target of an undelivered removal.
        // The new add keeps it and asks again for the current activity.
        let replacing = self.releases.remove(&source_id).is_some();
        self.pending.remove(&source_id);
        // Routes need a registration, so a new one has none and blocks.
        let mut registration = RemoteSourceRegistration {
            source: source.clone(),
            control,
            gate: PacketLayerGate::Block,
            delivery: Delivery::Add { replacing },
        };
        match registration.deliver() {
            RemoteControlSendOutcome::Forwarded => {}
            RemoteControlSendOutcome::Full => {
                self.pending.insert(source_id);
            }
            RemoteControlSendOutcome::Closed => {
                return Err(TransportAdapterError::TransportUnavailable);
            }
        }
        self.registrations.insert(source_id, registration);
        Ok(Some(RemoteSourceTransaction { source_id }))
    }

    pub(super) fn get(&self, source_id: TransportMediaId) -> Option<&RemoteSourceRegistration> {
        self.registrations.get(&source_id)
    }

    /// Removes a registration and withdraws its relay target if announced.
    pub(super) fn remove(&mut self, source_id: TransportMediaId) {
        let Some(registration) = self.registrations.remove(&source_id) else {
            return;
        };
        self.pending.remove(&source_id);
        // A replacement add still owes withdrawal of the earlier target.
        // An initial add that never arrived has nothing to release.
        if matches!(registration.delivery, Delivery::Add { replacing: false }) {
            return;
        }
        if registration.withdraw() == RemoteControlSendOutcome::Full {
            self.releases.insert(source_id, registration);
            self.pending.insert(source_id);
        }
    }

    pub(super) fn publish_gate(&mut self, source_id: TransportMediaId, gate: PacketLayerGate) {
        let Some(registration) = self.registrations.get_mut(&source_id) else {
            return;
        };
        if registration.delivery == Delivery::Done {
            if registration.gate == gate {
                return;
            }
            registration.delivery = Delivery::Gate;
        }
        registration.gate = gate;
        if registration.deliver() == RemoteControlSendOutcome::Full {
            self.pending.insert(source_id);
        } else {
            self.pending.remove(&source_id);
        }
    }

    pub(super) fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Retries each pending source once.
    pub(super) fn flush(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        let (registrations, releases) = (&mut self.registrations, &mut self.releases);
        self.pending.retain(|source_id| {
            if let Some(registration) = registrations.get_mut(source_id) {
                return registration.retry();
            }
            match releases.entry(*source_id) {
                Entry::Occupied(release)
                    if release.get().withdraw() == RemoteControlSendOutcome::Full =>
                {
                    true
                }
                Entry::Occupied(release) => {
                    release.remove();
                    false
                }
                Entry::Vacant(_) => false,
            }
        });
    }
}

impl RemoteSourceRegistration {
    /// Sends undelivered control. Full queues keep it pending.
    fn deliver(&mut self) -> RemoteControlSendOutcome {
        let outcome = match self.delivery {
            Delivery::Add { .. } => self.control.add_target(&self.source, self.gate),
            Delivery::Gate => self.control.set_pkt_gate(&self.source, self.gate),
            Delivery::Done => return RemoteControlSendOutcome::Forwarded,
        };
        // A closed worker cannot accept another retry.
        if outcome != RemoteControlSendOutcome::Full {
            self.delivery = Delivery::Done;
        }
        outcome
    }

    fn withdraw(&self) -> RemoteControlSendOutcome {
        self.control.remove_target(&self.source)
    }

    /// Returns whether delivery still waits for queue capacity.
    fn retry(&mut self) -> bool {
        if self.delivery == Delivery::Done {
            return false;
        }
        self.control.record_pkt_gate_retry();
        match self.deliver() {
            RemoteControlSendOutcome::Forwarded => self.control.record_pkt_gate_flushed(),
            RemoteControlSendOutcome::Full => return true,
            RemoteControlSendOutcome::Closed => {}
        }
        false
    }

    pub fn source(&self) -> &TransportSourceKey {
        &self.source
    }

    pub fn cloned_control_path(&self) -> (TransportSourceKey, RemoteSourceControl) {
        (self.source.clone(), self.control.clone())
    }

    /// Returns the gate the source worker has not accepted yet.
    #[cfg(test)]
    pub const fn pending_gate(&self) -> Option<PacketLayerGate> {
        match self.delivery {
            Delivery::Add { .. } | Delivery::Gate => Some(self.gate),
            Delivery::Done => None,
        }
    }

    #[cfg(feature = "internal-benchmarks")]
    pub const fn has_pending_gate(&self) -> bool {
        !matches!(self.delivery, Delivery::Done)
    }
}
