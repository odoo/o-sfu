//! Worker-local sessions, routing and their lifecycle transitions.
//!
//! Control commands, packet processing and recovery mutate one [`PacketLoopState`].
//! Its sessions, media identity and source routes remain separately borrowable.
//! [`RtcSessionState`] combines str0m with receiver delivery state so stream
//! retirement and RTX cache changes stay within the session boundary.
//! [`RouteTable`] owns source forwarding decisions and keyframe retry state.
//!
//! Topology changes that cross these owners belong to [`PacketLoopState`].
//! Registration and SSRC binding maintain paired media lookups. Consumer removal
//! repairs destination indexes before retiring its stream. Session teardown also
//! clears scheduling and demux state while preserving surviving consumers' indexes.
//! Callers use these complete operations instead of coordinating cleanup steps.
//!
//! [`schedule`] validates queued handles and deadlines against current sessions.
//! Datagram lookup in [`demux`] is independent of source-to-consumer routing.
//! [`RtcSnapshotState`] shares transport observations with readers outside the
//! worker without exposing mutable session or route state.

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap},
    net::SocketAddr,
    sync::Arc,
    time::Instant,
};

mod consumer_egress;
mod consumer_routes;
mod media_lifecycle;
mod ownership;
mod schedule;
mod session;
mod session_lifecycle;
mod snapshot;

pub(super) mod bitrate;
pub(super) mod demux;
pub(super) mod keyframe_tracker;
pub(super) mod media_registry;
pub(super) mod relay_registry;
pub(super) mod route_control;
pub(super) mod route_table;
pub(super) mod slots;
pub(super) mod source_route;

pub(super) use consumer_routes::ConsumerRouteRegistration;
pub use ownership::RouteSourceKind;
pub(super) use session::{
    PendingRecvStream, PendingSessionOffer, RtcNackTotals, RtcSessionState, RtcpIngressBudget,
    SessionSdpNegotiationState, muxed_rtp_ssrc,
};
#[cfg(test)]
pub(super) use session::{
    RTCP_INGRESS_BUDGET_CAPACITY_BYTES, RTCP_INGRESS_BUDGET_REFILL_BYTES_PER_SECOND,
};
pub use snapshot::RtcSnapshotState;

use self::{
    bitrate::MediaBitrateCounter,
    demux::RemoteAddrDemux,
    media_registry::{MediaStore, SessionMediaRegistry},
    route_table::{RidReadinessScratch, RouteTable},
    slots::{SessionHandle, SessionStore},
};
use super::packet_loop::{RtcUdpSocket, UdpIngress};
use crate::engine::media_transport::TransportMediaId;
pub use crate::engine::media_transport::TransportSessionHealth;

/// shared UDP socket owned by one RTC worker
///
/// every live session on the worker advertises the same candidate address and
/// uses `Rtc::accepts()` to decide whether an inbound datagram belongs to that
/// session
pub(super) struct SharedRtcSocket {
    /// worker socket used by packet-loop UDP sends
    pub(super) socket: RtcUdpSocket,
    /// completed datagrams received by the worker-local ingress pump
    pub(super) ingress: UdpIngress,
    /// public candidate tuple inserted into local SDP for sessions on this worker
    pub(super) candidate_addr: SocketAddr,
}

/// authoritative mutable state for one RTC packet-loop worker
///
/// the packet loop owns this value without a mutex
/// control commands, UDP ingress, `str0m` polling and relay fanout all pass
/// through one mutable borrow so the media indexes can be updated together
///
/// command-facing APIs keep stable transport ids while hot queues use
/// generation-checked handles
#[derive(Default)]
pub(super) struct PacketLoopState {
    /// live worker-local RTC sessions
    pub(super) users: SessionStore,
    /// source-scoped packet routing, relay and recovery state
    pub(super) routes: RouteTable,
    /// session-scoped media lookup vectors for packet source resolution
    pub(super) session_media: SessionMediaRegistry,
    /// reusable selected-RID readiness scratch vectors
    pub(super) rid_readiness_scratch: RidReadinessScratch,
    /// packet-loop write handles for incoming media bitrate accounting
    pub(super) incoming_bitrate_counters: BTreeMap<TransportMediaId, Arc<MediaBitrateCounter>>,
    /// worker-local UDP ingress demux hints
    pub(super) remote_addr_demux: RemoteAddrDemux,
    /// primary media handle table keyed by stable transport media id
    pub(super) mid_registry: MediaStore,
    /// sessions that must be polled before the worker waits again
    pub(super) dirty_sessions: Vec<SessionHandle>,
    /// Immutable deadline snapshots validated against the current session generation
    /// and [`RtcSessionState::next_timeout`].
    pub(super) timeout_queue: BinaryHeap<Reverse<(Instant, SessionHandle)>>,
    /// next worker-local media id from the disjoint range assigned at boot
    pub(super) next_media_id: u64,
}
