//! packet-facing layer gates for routed rtp
//!
//! this module defines the small gate language used after the packet loop has
//! resolved the route-control RID from an rtp packet
//! it does not parse codecs, choose room policy or schedule keyframes
//! it only answers whether the current packet layer can pass a gate
//!
//! source-wide route control uses this language as set algebra
//! downstream gates are unioned first so packets needed by any relay target or
//! local destination stay alive at the source
//! then the aggregate source gate is intersected with source-level policy such
//! as transport audio activity, so a stricter source policy can still block all
//! fanout for the packet
//!
//! `None` is kept by the surrounding route-control state as "no gate installed"
//! once a [`PacketLayerGate`] exists, [`PacketLayerGate::Open`] is the explicit
//! allow-all value and [`PacketLayerGate::Block`] is the explicit deny-all value

use str0m::media::Rid;

/// packet-layer predicate installed on a source, relay target or destination
///
/// gates are transport-native predicates over the resolved packet RID
/// they do not carry room policy meaning
/// by the time a gate reaches this file, "selected thumbnail quality" or
/// "active speaker audio policy" has already been projected into layer facts
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PacketLayerGate {
    /// allow every packet layer for this route
    #[default]
    Open,
    /// drop every packet layer for this route
    Block,
    /// allow only packets whose resolved rid matches the selected rid
    Rid(Rid),
}

impl PacketLayerGate {
    /// returns the concrete RID selected by this gate when one exists
    ///
    /// open and blocked routes do not name a simulcast layer
    /// feedback routing uses this to map destination policy back to the
    /// producer layer that can satisfy a keyframe request
    #[inline]
    pub const fn selected_rid(&self) -> Option<Rid> {
        match *self {
            Self::Rid(rid) => Some(rid),
            Self::Open | Self::Block => None,
        }
    }

    /// checks whether the packet RID passes this gate without allocating
    ///
    /// the packet loop calls this on the forwarding hot path
    /// it must remain a pure RID predicate with no source-state mutation
    pub fn permits(&self, packet_rid: Option<Rid>) -> bool {
        match self {
            Self::Open => true,
            Self::Block => false,
            Self::Rid(selected_rid) => packet_rid == Some(*selected_rid),
        }
    }
}

/// computes the permissive source gate needed to preserve all downstream gates
///
/// the route-control state uses this before source-level packet filtering
/// if two downstream gates require different rids, the aggregate becomes open
/// because no narrower source-level predicate can keep both routes decodable
/// destination-level gates still apply later and perform the final narrowing
///
/// returns `None` when no gate was installed by any caller
pub fn aggregate_packet_gates<'a>(
    packet_gates: impl IntoIterator<Item = &'a PacketLayerGate>,
) -> Option<PacketLayerGate> {
    let mut aggregate = None;
    for packet_gate in packet_gates {
        aggregate = Some(aggregate.map_or_else(
            || *packet_gate,
            |current| union_packet_gates(current, *packet_gate),
        ));
        if matches!(aggregate.as_ref(), Some(PacketLayerGate::Open)) {
            return aggregate;
        }
    }
    aggregate
}

/// composes two gates so only packets accepted by both gates can pass
///
/// `None` means "no gate installed" and acts as the identity value
/// this is used to apply source-level restrictions such as transport audio
/// policy after downstream gates have already been widened into one source gate
pub fn intersect_packet_gates(
    first: Option<PacketLayerGate>,
    second: Option<PacketLayerGate>,
) -> Option<PacketLayerGate> {
    match (first, second) {
        (None, None) => None,
        (Some(gate), None) | (None, Some(gate)) => Some(gate),
        (Some(PacketLayerGate::Block), _) | (_, Some(PacketLayerGate::Block)) => {
            Some(PacketLayerGate::Block)
        }
        (Some(PacketLayerGate::Open), Some(gate)) | (Some(gate), Some(PacketLayerGate::Open)) => {
            Some(gate)
        }
        (Some(PacketLayerGate::Rid(first_rid)), Some(PacketLayerGate::Rid(second_rid))) => {
            if first_rid == second_rid {
                Some(PacketLayerGate::Rid(first_rid))
            } else {
                Some(PacketLayerGate::Block)
            }
        }
    }
}

/// widens two gates so any packet allowed by either gate can survive source filtering
///
/// this produces the narrowest gate expressible by [`PacketLayerGate`] for the
/// union
/// when the grammar cannot express the union precisely, it returns open and
/// relies on destination gates to narrow fanout later
fn union_packet_gates(first: PacketLayerGate, second: PacketLayerGate) -> PacketLayerGate {
    match (first, second) {
        (PacketLayerGate::Open, _) | (_, PacketLayerGate::Open) => PacketLayerGate::Open,
        (PacketLayerGate::Block, gate) | (gate, PacketLayerGate::Block) => gate,
        (PacketLayerGate::Rid(first_rid), PacketLayerGate::Rid(second_rid)) => {
            if first_rid == second_rid {
                PacketLayerGate::Rid(first_rid)
            } else {
                PacketLayerGate::Open
            }
        }
    }
}

#[cfg(test)]
#[path = "TESTS/packet_gate.rs"]
mod tests;
