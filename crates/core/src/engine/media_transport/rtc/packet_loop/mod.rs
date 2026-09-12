//! Datagram admission, packet observation and forwarding execution.
//!
//! [`ingress_routing`] maps a UDP datagram to a session using
//! [`super::state::demux`] indexes and [`str0m::Rtc::accepts`]. [`routing_miss`]
//! bounds repeated unsuccessful lookup. [`udp`] owns socket I/O and ingress.
//!
//! The worker stages RTP from session output and relay input. Each packet
//! completes this sequence before the next packet can change route state:
//!
//! ```text
//! ForwardedPacket
//!   -> record_incoming_packet: resolve facts and update observations
//!   -> plan_forwards: read facts and routes into a destination plan
//!   -> flush_packet_forwards: origin sink -> relays -> local RTC
//! ```
//!
//! Relayed packets skip origin sinks and additional relay hops.
//! [`forwarded_packet`] retains shared payloads and cached facts.
//! [`PacketForwarder`] owns observation, planning, execution and batch completion.
//! [`forwarding_planner`] fills its reusable destination vector.
//! Local [`forwarding_destination`] writes mark sessions for the next drain.
//!
//! [`event_observation`] translates str0m events into transport observations.
//! [`super::worker::loop_driver`] governs intake, session drains and retry dispatch.

pub(super) mod event_observation;
mod forward_flush;
pub(super) mod forwarded_packet;
mod forwarding_destination;
mod forwarding_planner;
pub(super) mod ingress_routing;
pub(super) mod routing_miss;
pub(super) mod udp;

#[cfg(test)]
pub use event_observation::{transport_health_from_event, transport_ice_state};
#[cfg(feature = "internal-benchmarks")]
pub(super) use forward_flush::test_support::record_incoming_stats as record_incoming_stats_for_benchmark;
pub(super) use forward_flush::{ForwardingEffects, PacketForwarder, drain_relay_packets};
#[cfg(test)]
pub(super) use forward_flush::{
    finish_incoming_stats, record_incoming_packet, test_support::record_incoming_stats,
};
#[cfg(any(test, feature = "internal-benchmarks", fuzzing))]
pub use ingress_routing::{PacketRouteDatagram, route_pkt_to_session_at};
pub use udp::{RtcUdpSocket, UdpIngress};
#[cfg(any(test, feature = "internal-benchmarks"))]
pub(super) use {
    forward_flush::flush_packet_forwards,
    forwarding_destination::{ForwardingDestination, LocalRtcPacketDestination},
    forwarding_planner::{PacketGateDecision, plan_forwards},
};

#[cfg(feature = "internal-benchmarks")]
pub use self::{event_observation::observe_rtc_event_for_benchmark, udp::UdpIngressBenchHarness};
