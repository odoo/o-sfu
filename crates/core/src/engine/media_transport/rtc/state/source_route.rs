//! packet-loop source route facts
//!
//! source routes bind one producer media id to local consumer destinations
//! plus the decoder-refresh facts attached to that source

use str0m::media::{Mid, Pt};

mod decoder_delivery;

pub(in super::super) use decoder_delivery::{
    DecoderDelivery, DestinationKeyframeTarget, SelectedRefresh,
};

use super::{
    route_control::PacketLayerGate,
    slots::{ConsumerStreamHandle, RouteDestinationHandle, RouteDestinationSlot, SlotStore},
};
use crate::engine::media_transport::{TransportMediaId, TransportSessionKey};

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
/// the entry issues a [`RouteDestinationHandle`] for each destination and keeps
/// it valid while other destinations are removed
/// a handle can outlive its entry when the route is dropped and recreated, so
/// lookups also match the consumer media id, which a worker never reuses
/// dense positions change on removal and are valid only under one route
/// borrow, such as one packet-loop turn
/// `active_destination_count` is a cached admission invariant used by the hot
/// planner before it walks the destination vector
#[derive(Debug)]
pub(in super::super) struct MediaRouteEntry {
    /// active local destinations cached for source-route admission checks
    active_destination_count: usize,
    /// local consumer destinations reached from this source media id
    destinations: Vec<MediaRouteDestination>,
    /// handle bookkeeping read by lookups and removals, never by fanout
    ///
    /// boxed because the route table stores every source by value, so inline
    /// bookkeeping would enlarge each source entry and its map nodes
    slots: Box<DestinationSlots>,
}

#[derive(Debug, Default)]
struct DestinationSlots {
    /// handle of the destination at the same position
    ///
    /// kept outside `MediaRouteDestination` so the fanout walk does not load it
    handles: Vec<RouteDestinationHandle>,
    /// current dense position of each live handle's destination
    positions: SlotStore<usize, RouteDestinationSlot>,
}

impl MediaRouteEntry {
    /// creates an empty route entry with no local destinations
    pub(in super::super) fn new() -> Self {
        Self {
            active_destination_count: 0,
            destinations: Vec::new(),
            slots: Box::default(),
        }
    }

    /// returns whether local fanout has any active destination work
    ///
    /// this is the O(1) route-admission check used before source packet gates
    /// are evaluated
    pub(in super::super) const fn has_active_destinations(&self) -> bool {
        self.active_destination_count > 0
    }

    pub(in super::super) const fn active_destination_count(&self) -> usize {
        self.active_destination_count
    }

    /// destinations in fanout order
    pub(in super::super) fn destinations(&self) -> &[MediaRouteDestination] {
        &self.destinations
    }

    /// destinations in fanout order for delivery updates
    ///
    /// activity changes must use [`Self::set_destination_active`] so the
    /// cached active count stays exact
    pub(in super::super) fn destinations_mut(&mut self) -> &mut [MediaRouteDestination] {
        &mut self.destinations
    }

    /// returns the destination this entry issued `handle` for
    ///
    /// `None` means the handle is stale or names another consumer media
    pub(in super::super) fn destination(
        &self,
        handle: RouteDestinationHandle,
        dest_transport_media_id: TransportMediaId,
    ) -> Option<&MediaRouteDestination> {
        self.destinations
            .get(self.resolve(handle, dest_transport_media_id)?)
    }

    /// returns the destination this entry issued `handle` for
    ///
    /// activity changes must use [`Self::set_destination_active`] so the
    /// cached active count stays exact
    pub(in super::super) fn destination_mut(
        &mut self,
        handle: RouteDestinationHandle,
        dest_transport_media_id: TransportMediaId,
    ) -> Option<&mut MediaRouteDestination> {
        let position = self.resolve(handle, dest_transport_media_id)?;
        self.destinations.get_mut(position)
    }

    /// appends one destination and returns its handle
    ///
    /// route registration is the only caller that should add destinations
    /// directly because it owns source validation and consumer media ownership
    pub(in super::super) fn push_destination(
        &mut self,
        destination: MediaRouteDestination,
    ) -> RouteDestinationHandle {
        let handle = self.slots.positions.insert(self.destinations.len());
        self.active_destination_count += usize::from(destination.active);
        self.destinations.push(destination);
        self.slots.handles.push(handle);
        handle
    }

    /// removes one consumer media handle's destination and invalidates its handle
    ///
    /// destination order is not semantically observable, so removal keeps the
    /// vector dense by moving the final destination into the cleared position
    pub(in super::super) fn remove_destination(
        &mut self,
        dest_session: &TransportSessionKey,
        dest_transport_media_id: TransportMediaId,
    ) -> Option<MediaRouteDestination> {
        let position = self.destinations.iter().position(|destination| {
            destination.dest_transport_media_id == dest_transport_media_id
                && destination.dest_session == *dest_session
        })?;
        let destination = self.destinations.swap_remove(position);
        let DestinationSlots { handles, positions } = &mut *self.slots;
        positions.remove(handles.swap_remove(position));
        if let Some(moved) = handles.get(position)
            && let Some(moved_position) = positions.get_mut(*moved)
        {
            *moved_position = position;
        }
        self.active_destination_count -= usize::from(destination.active);
        Some(destination)
    }

    /// removes every destination of one consumer session in fanout order
    ///
    /// returns whether any destination was removed
    pub(in super::super) fn remove_session_destinations(
        &mut self,
        dest_session: &TransportSessionKey,
    ) -> bool {
        let Self {
            active_destination_count,
            destinations,
            slots,
        } = self;
        let DestinationSlots { handles, positions } = &mut **slots;
        let destination_count = destinations.len();
        handles.retain(|handle| {
            let retained = positions
                .get(*handle)
                .and_then(|position| destinations.get(*position))
                .is_some_and(|destination| destination.dest_session != *dest_session);
            if !retained {
                positions.remove(*handle);
            }
            retained
        });
        destinations.retain(|destination| destination.dest_session != *dest_session);
        if destinations.len() == destination_count {
            return false;
        }
        for (position, handle) in handles.iter().enumerate() {
            if let Some(handle_position) = positions.get_mut(*handle) {
                *handle_position = position;
            }
        }
        *active_destination_count = destinations
            .iter()
            .filter(|destination| destination.active)
            .count();
        true
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

    /// updates the activity of the destination this entry issued `handle` for
    ///
    /// returns whether the activity changed, or `None` when the handle is stale
    /// or names another consumer media
    pub(in super::super) fn set_destination_active(
        &mut self,
        handle: RouteDestinationHandle,
        dest_transport_media_id: TransportMediaId,
        active: bool,
    ) -> Option<bool> {
        let position = self.resolve(handle, dest_transport_media_id)?;
        let destination = self.destinations.get_mut(position)?;
        if destination.active == active {
            return Some(false);
        }
        if active {
            self.active_destination_count += 1;
        } else {
            self.active_destination_count -= 1;
        }
        destination.delivery.pause();
        destination.active = active;
        Some(true)
    }

    fn resolve(
        &self,
        handle: RouteDestinationHandle,
        dest_transport_media_id: TransportMediaId,
    ) -> Option<usize> {
        let position = *self.slots.positions.get(handle)?;
        (self.destinations.get(position)?.dest_transport_media_id == dest_transport_media_id)
            .then_some(position)
    }
}
