//! Packet gates and audio activity used by [`RouteTable`](super::route_table::RouteTable).
//!
//! [`PacketLayerGate`] admits resolved packet RIDs. Downstream gates combine to
//! preserve packets needed by any destination, then source restrictions narrow
//! that aggregate. Destination gates still determine each consumer's delivery.
//!
//! [`SourceAudioPolicyState`] derives speaker activity and an audio gate from
//! packet observations. These are transport inputs to room policy and forwarding,
//! while room code chooses subscriptions and receiver budgets.

mod active_speaker;
mod packet_gate;

pub(in super::super) use self::packet_gate::{aggregate_packet_gates, intersect_packet_gates};
pub use self::{active_speaker::SourceAudioPolicyState, packet_gate::PacketLayerGate};
