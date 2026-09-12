//! Packet forwarding planner for the RTC engine hot path.
//!
//! The packet loop receives media as `ForwardedPacket` values, but the flush
//! step needs concrete destinations that know how to write to local RTC state,
//! packet sinks such as recording or relay mailboxes. This module is the narrow
//! planning boundary between those two shapes.
//!
//! The planner owns mechanical fanout only. Room policy, receiver layout,
//! bandwidth budgeting and source selection must already have been projected
//! into packet-facing route-control gates before a packet reaches this file.
//!
//! # Hot-path contract
//!
//! Planning runs inside the worker packet loop while the packet-loop state is
//! borrowed. It must avoid async work, broad scans and steady-state allocation.
//! `PacketLoopBuffers` keeps the destination list across iterations, so this
//! module may reserve the known fanout bound but must not create detached
//! per-packet collections.
//!
//! Destination order is part of the flush contract. Packet sinks are planned
//! first so origin-side side effects see publisher packets before relay or
//! local egress. Relay destinations are planned before local RTC destinations.

use str0m::media::Rid;

use super::{
    super::state::{
        relay_registry::{ActiveRelayTarget, RelayTargetId},
        route_table::{ForwardView, RouteTable},
        source_route::MediaRouteEntry,
    },
    forwarded_packet::PacketFacts,
    forwarding_destination::ForwardingDestination,
};
use crate::engine::{
    media_transport::TransportMediaId,
    packet_sink_registry::{PacketSinkRouteCache, RegisteredPacketSink},
};

/// Source-wide gate result when a packet has routed destinations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in super::super) enum PacketGateDecision {
    Allowed,
    Dropped,
}

/// Appends origin sinks, relays and local destinations from observed packet facts.
///
/// Observation must finish before planning. Relayed packets skip origin sinks
/// and second-hop relays. Origin sinks receive packets even when the source gate
/// drops routed fanout.
///
/// Returns `None` when no routed destination needs a gate decision. Sink-only
/// packets do not contribute allowed or dropped route-control observations.
pub(in super::super) fn plan_forwards(
    facts: &PacketFacts,
    visits_origin: bool,
    routes: &RouteTable,
    packet_sinks: &PacketSinkRouteCache,
    forwards: &mut Vec<ForwardingDestination>,
) -> Option<PacketGateDecision> {
    let src_media = facts.src_media;
    let origin_sink = if visits_origin {
        packet_sinks.sink_for_room(facts.room_instance_id)
    } else {
        None
    };
    match origin_sink {
        Some(origin_sink) if !routes.has_forwarding_sources() => {
            // Sink-only packets need neither route lookup nor fanout accounting.
            forwards.push(ForwardingDestination::from_packet_sink(
                src_media,
                origin_sink,
            ));
            return None;
        }
        _ => {}
    }
    let ForwardView {
        route,
        relays,
        source_gate,
    } = routes.forward_view(src_media, visits_origin);
    reserve_forward_capacity(origin_sink.as_ref(), relays, route, forwards);
    if let Some(origin_sink) = origin_sink {
        forwards.push(ForwardingDestination::from_packet_sink(
            src_media,
            origin_sink,
        ));
    }
    if !has_routed_forward(relays, route) {
        return None;
    }
    let packet_rid = facts.rid;
    if source_gate.is_some_and(|gate| !gate.permits(packet_rid)) {
        return Some(PacketGateDecision::Dropped);
    }
    if let Some(relays) = relays {
        populate_relay_forwards(routes, relays, src_media, packet_rid, forwards);
    }
    if let Some(route) = route {
        populate_local_forwards(route, src_media, packet_rid, forwards);
    }
    Some(PacketGateDecision::Allowed)
}

/// Reserves the fanout list for the largest destination count this packet can
/// produce.
///
/// The bound counts configured destinations before packet gates
/// are applied. That may reserve a few unused slots when routes are inactive or
/// layer gates drop the packet, but it avoids allocator churn when a dense room
/// crosses a previous high-water mark.
fn reserve_forward_capacity(
    origin_sink: Option<&RegisteredPacketSink>,
    relay_targets: Option<&[ActiveRelayTarget]>,
    route_entry: Option<&MediaRouteEntry>,
    forwards: &mut Vec<ForwardingDestination>,
) {
    let planned_forwards = usize::from(origin_sink.is_some())
        + relay_targets.map_or(0, <[ActiveRelayTarget]>::len)
        + route_entry.map_or(0, |entry| entry.destinations.len());
    if forwards.capacity().saturating_sub(forwards.len()) < planned_forwards {
        forwards.reserve(planned_forwards);
    }
}

/// Adds relay destinations whose target-specific gates permit this packet.
///
/// Relay targets represent worker or node boundaries, not room policy. The
/// registry tells this planner which targets currently need this source and
/// route control decides whether the current packet layer is allowed for each
/// target.
fn populate_relay_forwards(
    routes: &RouteTable,
    relay_targets: &[ActiveRelayTarget],
    src_media: TransportMediaId,
    packet_rid: Option<Rid>,
    forwards: &mut Vec<ForwardingDestination>,
) {
    forwards.extend(
        relay_targets
            .iter()
            .filter(|target| {
                relay_target_gate_permits(routes, src_media, target.target_id, packet_rid)
            })
            .map(|target| {
                ForwardingDestination::from_relay_target(src_media, target.target.clone())
            }),
    );
}

/// Adds local RTC destinations for active routes whose consumer gate permits
/// the current packet layer.
///
/// Local fanout remains proportional to the number of writable receiver
/// sessions. This planner can avoid avoidable allocation work, but it cannot
/// collapse receiver-specific WebRTC egress into one broadcast operation.
///
/// planned local destinations are compact route handles
/// the flush step resolves each handle against `RouteTable` before
/// touching the destination session, so planning does not clone route-stable
/// consumer identity
fn populate_local_forwards(
    route_entry: &MediaRouteEntry,
    src_media: TransportMediaId,
    packet_rid: Option<Rid>,
    forwards: &mut Vec<ForwardingDestination>,
) {
    let all_active = route_entry.active_destination_count == route_entry.destinations.len();
    // `extend` avoids per-destination gate-result setup in Rust 1.95 x86 codegen.
    forwards.extend(
        route_entry
            .destinations
            .iter()
            .enumerate()
            .filter(|(_, dst)| {
                (all_active || dst.active) && dst.delivery.effective_gate().permits(packet_rid)
            })
            .map(|(dst_idx, _)| {
                ForwardingDestination::from_local_route_destination(src_media, dst_idx)
            }),
    );
}

/// Checks relay-target packet policy without treating a missing gate as a drop.
///
/// Missing relay gates mean the target has no extra layer restriction beyond
/// the source-wide gate. This keeps newly activated relay targets open until
/// room or transport policy installs a narrower packet gate.
fn relay_target_gate_permits(
    routes: &RouteTable,
    src_media: TransportMediaId,
    target_id: RelayTargetId,
    packet_rid: Option<Rid>,
) -> bool {
    routes
        .relay_packet_gate(src_media, target_id)
        .is_none_or(|packet_gate| packet_gate.permits(packet_rid))
}

/// Reports whether route planning has any destination work after origin sinks.
///
/// Origin sinks are excluded because recording or similar side
/// effects must still run for source packets even when the source has no live
/// relay or local RTC consumers.
fn has_routed_forward(
    relay_targets: Option<&[ActiveRelayTarget]>,
    route_entry: Option<&MediaRouteEntry>,
) -> bool {
    route_entry.is_some_and(MediaRouteEntry::has_active_destinations)
        || relay_targets.is_some_and(|targets| !targets.is_empty())
}
