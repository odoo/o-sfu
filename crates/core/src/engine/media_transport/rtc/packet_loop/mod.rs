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
//! [`forward_flush`] owns observation and execution. [`forwarding_planner`]
//! reads domain state and fills the worker's reusable destination vector.
//! Local [`forwarding_destination`] writes mark sessions for the next drain.
//!
//! [`event_observation`] translates str0m events into transport observations.
//! [`super::worker::loop_driver`] governs turn ordering and batch completion.

pub(super) mod event_observation;
pub(super) mod forward_flush;
pub(super) mod forwarded_packet;
pub(super) mod forwarding_destination;
pub(super) mod forwarding_planner;
pub(super) mod ingress_routing;
pub(super) mod routing_miss;
pub(super) mod udp;

#[cfg(test)]
pub use event_observation::{transport_health_from_event, transport_ice_state};
#[cfg(feature = "internal-benchmarks")]
pub(in crate::engine::media_transport::rtc) use forward_flush::flush_packet_forwards;
#[cfg(any(test, feature = "internal-benchmarks", fuzzing))]
pub use ingress_routing::{PacketRouteDatagram, route_pkt_to_session_at};
pub use udp::{RtcUdpSocket, UdpIngress};

#[cfg(feature = "internal-benchmarks")]
pub use self::{
    event_observation::observe_rtc_event_for_benchmark,
    forward_flush::{drain_relay_packets, record_incoming_stats_for_benchmark},
    udp::UdpIngressBenchHarness,
};
