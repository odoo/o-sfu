//! packet-loop source route facts
//!
//! source routes bind one producer media id to local consumer destinations
//! plus the remote-source and decoder-refresh facts attached to that source

use str0m::media::{Mid, Pt};

mod decoder_delivery;

pub(in super::super) use decoder_delivery::{
    DecoderDelivery, DestinationKeyframeTarget, SelectedRefresh,
};

use super::{
    super::commands::{RemoteControlSendOutcome, RemoteSourceControl},
    route_control::PacketLayerGate,
    slots::ConsumerStreamHandle,
};
use crate::engine::media_transport::{TransportMediaId, TransportSessionKey, TransportSourceKey};

/// forwarding destination selected for one source media route
///
/// one producer media id can fan out to many consumer transports
/// each destination keeps the consumer-negotiated RTP identity and the
/// currently effective packet gate so the packet loop can forward without
/// consulting room policy on the hot path
#[derive(Debug, Clone)]
pub(in super::super) struct MediaRouteDestination {
    /// consumer session that owns the destination `Rtc`
    pub(in super::super) dest_session: TransportSessionKey,
    /// consumer media id used by the destination session state
    pub(in super::super) dest_transport_media_id: TransportMediaId,
    /// destination-owned RTP rewrite handle for this consumer route
    ///
    /// the counters live in the destination session
    /// this handle lets a route destination reach them without keying local
    /// RTP projection by sparse transport media ids on the hot path
    pub(in super::super) dest_stream: ConsumerStreamHandle,
    /// consumer MID used when rewriting the packet for local egress
    pub(in super::super) dest_mid: Mid,
    /// payload type negotiated for this consumer stream
    ///
    /// source payload types can differ from consumer payload types after router
    /// negotiation, so forwarding must not reuse the publisher value blindly
    pub(in super::super) dest_payload_type: Option<Pt>,
    /// whether this destination negotiated Generic NACK with matching RTX
    pub(in super::super) repair_enabled: bool,
    /// destination-level activity gate controlled by consumer state
    pub(in super::super) active: bool,
    /// requested selection, decoder readiness and receiver delivery generation
    pub(in super::super) delivery: DecoderDelivery,
}

/// local packet-loop fanout for one producer media id
///
/// callers must mutate `destinations` through this type's helpers
/// `active_destination_count` is a cached admission invariant used by the hot
/// planner before it walks the destination vector
#[derive(Debug, Clone)]
pub(in super::super) struct MediaRouteEntry {
    /// active local destinations cached for source-route admission checks
    pub(in super::super) active_destination_count: usize,
    /// local consumer destinations reached from this source media id
    pub(in super::super) destinations: Vec<MediaRouteDestination>,
}

impl MediaRouteEntry {
    /// creates an empty route entry with no local destinations
    pub(in super::super) fn new() -> Self {
        Self {
            active_destination_count: 0,
            destinations: Vec::new(),
        }
    }

    /// returns whether local fanout has any active destination work
    ///
    /// this is the O(1) route-admission check used before source packet gates
    /// are evaluated
    pub(in super::super) const fn has_active_destinations(&self) -> bool {
        self.active_destination_count > 0
    }

    /// appends one destination while preserving the active-count invariant
    ///
    /// route registration is the only caller that should add destinations
    /// directly because it owns source validation and consumer media ownership
    pub(in super::super) fn push_destination(&mut self, destination: MediaRouteDestination) {
        self.active_destination_count += usize::from(destination.active);
        self.destinations.push(destination);
    }

    /// removes one destination while preserving the active-count invariant
    ///
    /// callers pass the index they found under the same mutable route borrow
    /// destination order is not semantically observable, so removal keeps the
    /// vector dense by moving the final destination into the cleared slot
    /// callers that cache destination indexes must repair the moved destination
    /// before feedback can use the cache again
    pub(in super::super) fn remove_destination(&mut self, index: usize) -> MediaRouteDestination {
        let destination = self.destinations.swap_remove(index);
        self.active_destination_count -= usize::from(destination.active);
        destination
    }

    pub(in super::super) fn advance_delivery(&mut self) {
        for destination in &mut self.destinations {
            destination.delivery.advance_generation();
        }
    }

    pub(in super::super) fn pause_delivery(&mut self) {
        for destination in &mut self.destinations {
            destination.delivery.pause();
        }
    }

    /// updates destination activity and reports whether the route changed
    ///
    /// a missing index is treated as unchanged because stale worker commands
    /// are rejected by the caller's ownership checks before reaching the route
    /// mutation
    pub(in super::super) fn set_destination_active(&mut self, index: usize, active: bool) -> bool {
        let Some(destination) = self.destinations.get_mut(index) else {
            return false;
        };
        if destination.active == active {
            return false;
        }
        if active {
            self.active_destination_count += 1;
        } else {
            self.active_destination_count -= 1;
        }
        destination.delivery.pause();
        destination.active = active;
        true
    }
}

/// remote source control path with latest-gate retry state
///
/// full mailboxes keep only the newest packet gate
/// `RouteTable::flush_remote_pkt_gates` retries it until the source worker
/// accepts it or its control receiver closes
#[derive(Debug, Clone)]
pub(in super::super) struct RemoteSourceRegistration {
    source: TransportSourceKey,
    source_control: RemoteSourceControl,
    pending_gate: Option<PacketLayerGate>,
}

impl RemoteSourceRegistration {
    pub(in super::super) fn new(
        source: TransportSourceKey,
        source_control: RemoteSourceControl,
    ) -> Self {
        Self {
            source,
            source_control,
            pending_gate: None,
        }
    }

    pub(in super::super) fn source(&self) -> &TransportSourceKey {
        &self.source
    }

    pub(in super::super) fn cloned_control_path(
        &self,
    ) -> (TransportSourceKey, RemoteSourceControl) {
        (self.source.clone(), self.source_control.clone())
    }

    #[cfg(test)]
    pub(in super::super) const fn pending_gate(&self) -> Option<PacketLayerGate> {
        self.pending_gate
    }

    pub(in super::super) const fn has_pending_gate(&self) -> bool {
        self.pending_gate.is_some()
    }

    pub(in super::super) fn publish_packet_gate_needs_retry(
        &mut self,
        packet_gate: PacketLayerGate,
    ) -> bool {
        self.pending_gate = match self.source_control.set_pkt_gate(&self.source, packet_gate) {
            RemoteControlSendOutcome::Forwarded | RemoteControlSendOutcome::Closed => None,
            RemoteControlSendOutcome::Full => Some(packet_gate),
        };
        self.pending_gate.is_some()
    }

    pub(in super::super) fn flush_pending_gate(&mut self) -> bool {
        let Some(packet_gate) = self.pending_gate else {
            return false;
        };
        self.source_control.record_pkt_gate_retry();
        match self.source_control.set_pkt_gate(&self.source, packet_gate) {
            RemoteControlSendOutcome::Forwarded => {
                self.pending_gate = None;
                self.source_control.record_pkt_gate_flushed();
                false
            }
            RemoteControlSendOutcome::Full => true,
            RemoteControlSendOutcome::Closed => {
                // A closed worker cannot accept a later retry. Keeping this gate
                // pending would retry and count the same terminal failure every turn.
                self.pending_gate = None;
                false
            }
        }
    }
}
