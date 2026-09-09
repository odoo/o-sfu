//! Worker-local demultiplexing for the UDP socket shared by RTC sessions.
//!
//! [`str0m::Rtc::accepts()`] owns the datagram-to-session decision. Learned
//! source-address pins and ICE indexes only reduce how many sessions reach that
//! check. Each pin is revalidated because ICE can stop attributing the source
//! address to the session that first accepted it.
//!
//! Negative results belong to the worker topology that produced them. The
//! packet-loop driver clears them after control input. The exact-packet cache
//! avoids repeating a proven miss while the per-source cooldown limits varied
//! unknown traffic that could otherwise consume packet-loop CPU.
//!
//! [`str0m::Rtc::handle_input()`] may fail after `Rtc::accepts()` selects a
//! session. That processing failure does not revoke the learned source pin.

use core::hint::cold_path;
use std::{fmt, net::SocketAddr, slice::Iter, time::Instant};

use o_sfu_rfc::{
    rtp::{self, RtpRtcpMuxPacketKind},
    webrtc,
};
use str0m::{
    Input,
    ice::StunMessage,
    net::{Protocol, Receive},
};
use tracing::{debug, trace, warn};

use super::{
    super::state::{PacketLoopState, RtcSessionState, demux::RemoteAddrDemux, slots::SessionStore},
    routing_miss::{DemuxRecoveryState, PacketLoopRoutingMissKey},
};
use crate::engine::{
    media_transport::TransportSessionKey,
    metrics::{RtcDatagramDropReason, RtcDatagramRoutePath, RtcMetricsRecorder},
};

enum CachedRouteOutcome {
    Routed,
    NotMatched,
    Malformed,
}

enum IndexedSessionRecoveryOutcome {
    Matched {
        session_key: TransportSessionKey,
        examined_sessions: usize,
    },
    NoMatch {
        examined_sessions: usize,
    },
    Malformed,
}

enum PacketIndexProbe<'a> {
    LocalIceUfrag(&'a str),
    RemoteCandidateAddr(SocketAddr),
}

impl fmt::Display for PacketIndexProbe<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LocalIceUfrag(local_ice_ufrag) => {
                write!(formatter, "local-ice-ufrag:{local_ice_ufrag}")
            }
            Self::RemoteCandidateAddr(remote_candidate_addr) => {
                write!(formatter, "remote-candidate-addr:{remote_candidate_addr}")
            }
        }
    }
}

/// Preserves the socket receive time through queued ingress.
///
/// str0m uses `now` to advance jitter and bandwidth clocks. Sampling it during
/// routing would include packet-loop queue delay in those measurements.
#[derive(Clone, Copy)]
pub struct PacketRouteDatagram<'a> {
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &'a [u8],
    now: Instant,
}

impl<'a> PacketRouteDatagram<'a> {
    pub const fn new(
        source_addr: SocketAddr,
        candidate_addr: SocketAddr,
        packet: &'a [u8],
        now: Instant,
    ) -> Self {
        Self {
            source_addr,
            candidate_addr,
            packet,
            now,
        }
    }
}

enum CandidateSessionKeys<'a> {
    Single(Option<&'a TransportSessionKey>),
    Slice(Iter<'a, TransportSessionKey>),
}

impl<'a> Iterator for CandidateSessionKeys<'a> {
    type Item = &'a TransportSessionKey;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Single(session_key) => session_key.take(),
            Self::Slice(iter) => iter.next(),
        }
    }
}

/// Routes one datagram against the current worker topology.
///
/// Malformed, unowned and throttled datagrams are recorded then dropped.
///
/// ```text
/// incoming UDP datagram (source_addr, packet)
///                     |
///                     v
///      +------------------------------+
///      | learned pin still accepts?   | -- yes --> [ fast-path: indexed route ]
///      +------------------------------+
///                     | no
///                     v
///      +------------------------------+
///      | source cooldown rate-limit?  | -- yes --> [ drop: SourceRateLimited ]
///      +------------------------------+
///                     | no
///                     v
///      +------------------------------+
///      | in recent negative cache?    | -- yes --> [ drop: RecentMissCache ]
///      +------------------------------+
///                     | no
///                     v
///      +------------------------------+
///      | header probing (N sessions)  |
///      | - STUN local ICE ufrag       |
///      | - other mux remote candidate |
///      +------------------------------+
///                     |
///                     v
///      +------------------------------+
///      | candidate str0m::accepts()   |
///      | - 1 session: direct test     |
///      | - N sessions: probe-narrowed |
///      +------------------------------+
///            |                  |
///         matched            no match
///            |                  |
///            v                  v
///     [ pin source addr ]    [ record miss, maybe cooldown ]
///     [ route to Rtc ]       [ drop: NoUser ]
/// ```
///
/// The one-session branch precedes recovery indexing. Input construction failure
/// is `Malformed`. RTCP budget rejection skips `handle_input` but retains or
/// learns the accepted pin. A handling error clears ingress context but retains
/// the pin. Successful fallback clears its miss and source cooldown. `Indexed`
/// and `Scan` are [`RtcDatagramRoutePath`] metric values.
pub fn route_pkt_to_session_at(
    state: &mut PacketLoopState,
    demux: &mut DemuxRecoveryState,
    metrics: &RtcMetricsRecorder,
    datagram: PacketRouteDatagram<'_>,
) {
    match route_cached_pkt(
        state,
        metrics,
        datagram.source_addr,
        datagram.candidate_addr,
        datagram.packet,
        datagram.now,
    ) {
        CachedRouteOutcome::Routed => {
            metrics.record_rtc_datagram_route(RtcDatagramRoutePath::Indexed);
            return;
        }
        CachedRouteOutcome::Malformed => {
            metrics.record_rtc_datagram_drop(RtcDatagramDropReason::Malformed);
            return;
        }
        CachedRouteOutcome::NotMatched => {}
    }
    // Accepted source pins bypass abuse throttling. For unknown sources, check
    // the source-keyed cooldown before packet fingerprinting so blocked traffic
    // avoids fingerprint work and changing packet bytes cannot evade the limit.
    if demux.is_source_blocked(datagram.source_addr, datagram.now) {
        metrics.record_rtc_datagram_drop(RtcDatagramDropReason::SourceRateLimited);
        return;
    }
    let miss_key = PacketLoopRoutingMissKey::new(
        datagram.source_addr,
        datagram.candidate_addr,
        datagram.packet,
    );
    // A negative answer is valid only for the topology that produced it.
    if demux.should_skip_scan(miss_key, datagram.packet) {
        metrics.record_rtc_datagram_drop(RtcDatagramDropReason::RecentMissCache);
        trace!(
            source = %datagram.source_addr,
            "dropping UDP datagram because a recent cache miss already proved no rtc user accepted it"
        );
        return;
    }
    let route = PacketRouteContext {
        metrics,
        source_addr: datagram.source_addr,
        candidate_addr: datagram.candidate_addr,
        packet: datagram.packet,
        now: datagram.now,
    };
    // With one session, packet shape cannot narrow the candidate set. Calling
    // `Rtc::accepts()` directly avoids duplicate classification.
    if state.users.len() == 1 {
        route_pkt_by_session(state, demux, miss_key, &route);
        return;
    }
    route_pkt_by_recovery(state, demux, miss_key, &route);
}

/// Attempts routing through a learned source-address pin.
fn route_cached_pkt(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &[u8],
    now: Instant,
) -> CachedRouteOutcome {
    let Some(session_key) = state
        .remote_addr_demux
        .session_key_for_remote_addr(source_addr)
    else {
        return CachedRouteOutcome::NotMatched;
    };
    let Some(session_state) = state.users.get_mut(session_key) else {
        state.remote_addr_demux.forget_remote_addr(source_addr);
        return CachedRouteOutcome::NotMatched;
    };
    let Ok(receive) = Receive::new(Protocol::Udp, source_addr, candidate_addr, packet) else {
        log_malformed_datagram(source_addr);
        return CachedRouteOutcome::Malformed;
    };
    let input = Input::Receive(now, receive);
    // A learned pin is only a hint because ICE state can change. Revalidate it
    // before every use.
    let accepts_input = session_state.rtc.accepts(&input);
    if !accepts_input {
        debug!(
            source_addr = %source_addr,
            candidate_addr = %candidate_addr,
            user_id = ?session_key.user_id(),
            media_worker_id = session_key.media_worker_id().as_usize(),
            "indexed rtc source address no longer matched the cached user; clearing source-address pin"
        );
        state.remote_addr_demux.forget_remote_addr(source_addr);
        return CachedRouteOutcome::NotMatched;
    }
    if !admit_rtcp_datagram(session_state, packet, now) {
        metrics.record_rtc_rtcp_ingress_budget_drop();
        return CachedRouteOutcome::Routed;
    }
    session_state.prepare_rtp_input(packet);
    // `Rtc::accepts()` owns the demux decision. A later processing error does
    // not invalidate the learned pin.
    if session_state.rtc.handle_input(input).is_err() {
        session_state.clear_ingress_context();
        cold_path();
        warn!(
            user_id = ?session_key.user_id(),
            media_worker_id = session_key.media_worker_id().as_usize(),
            "failed to feed indexed UDP datagram into rtc user state"
        );
    } else {
        let dirty_session_key = if session_state.packet_loop_dirty {
            None
        } else {
            Some(session_key.clone())
        };
        if let Some(dirty_session_key) = dirty_session_key {
            state.mark_session_dirty(&dirty_session_key);
        }
    }
    CachedRouteOutcome::Routed
}

/// Searches the recovery index selected by the datagram's multiplex class.
fn indexed_session_for_pkt(
    sessions: &SessionStore,
    remote_addr_demux: &mut RemoteAddrDemux,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &[u8],
    input: &Input<'_>,
) -> IndexedSessionRecoveryOutcome {
    let Some(packet_index_probe) = packet_index_probe(source_addr, packet) else {
        return IndexedSessionRecoveryOutcome::Malformed;
    };
    let candidate_session_keys = match &packet_index_probe {
        PacketIndexProbe::LocalIceUfrag(local_ice_ufrag) => {
            CandidateSessionKeys::Single(remote_addr_demux.session_for_local_ufrag(local_ice_ufrag))
        }
        PacketIndexProbe::RemoteCandidateAddr(remote_candidate_addr) => remote_addr_demux
            .candidates_for_src_addr(*remote_candidate_addr)
            .map_or(
                CandidateSessionKeys::Single(None),
                |candidate_session_keys| CandidateSessionKeys::Slice(candidate_session_keys.iter()),
            ),
    };
    let mut examined_sessions: usize = 0;
    // `candidate_session_keys` borrows `remote_addr_demux`. Collect stale sessions
    // then repair their candidate and ufrag indexes even if a later candidate
    // matches.
    let mut stale_session_keys = Vec::new();
    let matched_session_key = {
        let mut matched_session_key = None;
        for session_key in candidate_session_keys {
            let Some(session_state) = sessions.get(session_key) else {
                stale_session_keys.push(session_key.clone());
                continue;
            };
            examined_sessions = examined_sessions.saturating_add(1);
            // Index membership is only a hint. Candidate addresses can be shared
            // and ICE indexes can lag, so revalidate ownership here.
            if session_state.rtc.accepts(input) {
                matched_session_key = Some(session_key.clone());
                break;
            }
        }
        matched_session_key
    };
    for stale_session_key in &stale_session_keys {
        remote_addr_demux.forget_user_remote_candidates(stale_session_key);
        remote_addr_demux.forget_user_local_ice_ufrag(stale_session_key);
    }
    if let Some(matched_session_key) = matched_session_key {
        debug!(
            source_addr = %source_addr,
            candidate_addr = %candidate_addr,
            probe = %packet_index_probe,
            user_id = ?matched_session_key.user_id(),
            media_worker_id = matched_session_key.media_worker_id().as_usize(),
            examined_sessions,
            "recovered rtc user routing from packet probe"
        );
        return IndexedSessionRecoveryOutcome::Matched {
            session_key: matched_session_key,
            examined_sessions,
        };
    }
    debug!(
        source_addr = %source_addr,
        candidate_addr = %candidate_addr,
        probe = %packet_index_probe,
        examined_sessions,
        "packet probe did not match any rtc user"
    );
    IndexedSessionRecoveryOutcome::NoMatch { examined_sessions }
}

/// Selects the recovery index for a str0m-classifiable datagram.
fn packet_index_probe(source_addr: SocketAddr, packet: &[u8]) -> Option<PacketIndexProbe<'_>> {
    let byte0 = packet.first().copied()?;
    let packet_len = packet.len();
    // Keep these gates aligned with str0m 0.21's `Receive::new` classifier:
    // https://github.com/algesten/str0m/blob/0.21.0/src/io/mod.rs#L114-L150
    // It keeps RFC 5764 Section 5.1.2's `0..=1` STUN range rather than RFC 7983
    // Section 7's `0..=3`. A narrower probe would drop supported unknown-source
    // traffic before `Rtc::accepts()` can decide ownership.
    // https://www.rfc-editor.org/rfc/rfc5764.html#section-5.1.2
    // https://www.rfc-editor.org/rfc/rfc7983.html#section-7
    if byte0 < 2 && packet_len >= 20 {
        let message = StunMessage::parse(packet).ok()?;
        if let Some(local_ice_ufrag) = message
            .username()
            .and_then(|username| username.split_once(':'))
            .map(|(local_ice_ufrag, _remote_ice_ufrag)| local_ice_ufrag)
        {
            // RFC 8445 puts the receiving agent's local ufrag before the colon.
            // https://www.rfc-editor.org/rfc/rfc8445.html#section-7.2.2
            return Some(PacketIndexProbe::LocalIceUfrag(local_ice_ufrag));
        }
        // This layer does not index STUN transaction IDs. Source address narrows
        // the candidate set before `Rtc::accepts()` applies STUN-specific checks.
        return Some(PacketIndexProbe::RemoteCandidateAddr(source_addr));
    }
    // DTLS exposes no ICE ufrag. RTP and RTCP share source-candidate lookup
    // after the RFC 5761 second-octet split.
    // https://www.rfc-editor.org/rfc/rfc5761.html#section-4
    // DTLS records occupy their RFC 7983 first-octet range.
    // https://www.rfc-editor.org/rfc/rfc7983.html#section-5
    if webrtc::is_dtls_mux_packet(byte0) {
        return Some(PacketIndexProbe::RemoteCandidateAddr(source_addr));
    }
    if rtp::classify_rtp_rtcp_mux(packet).is_some() {
        return Some(PacketIndexProbe::RemoteCandidateAddr(source_addr));
    }
    None
}

fn receive_input(
    now: Instant,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &[u8],
) -> Option<Input<'_>> {
    let receive = Receive::new(Protocol::Udp, source_addr, candidate_addr, packet).ok()?;
    Some(Input::Receive(now, receive))
}

fn admit_rtcp_datagram(session_state: &mut RtcSessionState, packet: &[u8], now: Instant) -> bool {
    // RFC 5761 section 4 reserves the muxed second-octet range used to
    // recognize RTCP before admitting feedback to str0m.
    // https://www.rfc-editor.org/rfc/rfc5761.html#section-4
    if !matches!(
        rtp::classify_rtp_rtcp_mux(packet),
        Some(RtpRtcpMuxPacketKind::Rtcp)
    ) {
        return true;
    }
    if !session_state
        .rtcp_ingress_budget
        .try_charge(u64::try_from(packet.len()).unwrap_or(u64::MAX), now)
    {
        return false;
    }
    // Rotate expired caches before str0m consumes RTCP so a NACK cannot
    // recover media older than the configured RTX cache lifetime.
    session_state.expire_rtx_streams(now);
    // Carry this receive-time decision into the immediately following drain.
    session_state.defer_rtx_expiry = true;
    true
}

fn log_malformed_datagram(source_addr: SocketAddr) {
    trace!(
        source = %source_addr,
        "ignoring malformed UDP datagram in rtc packet loop"
    );
}

fn record_unknown_src_miss(
    demux: &mut DemuxRecoveryState,
    miss_key: PacketLoopRoutingMissKey,
    route: &PacketRouteContext<'_>,
) {
    if demux.record_miss(miss_key, route.packet, route.source_addr, route.now) {
        trace!(
            source = %route.source_addr,
            "entering rtc unknown-source recovery cooldown after sustained routing misses"
        );
    }
}

struct PacketRouteContext<'a> {
    metrics: &'a RtcMetricsRecorder,
    source_addr: SocketAddr,
    candidate_addr: SocketAddr,
    packet: &'a [u8],
    now: Instant,
}

/// Feeds a previously accepted datagram into its session and learns its source
/// pin.
///
/// Returns `false` if the session disappeared before delivery.
fn route_packet_to_session(
    state: &mut PacketLoopState,
    session_key: &TransportSessionKey,
    route: &PacketRouteContext<'_>,
    input: Input<'_>,
    route_resolution: &'static str,
) -> bool {
    let Some(session_state) = state.users.get_mut(session_key) else {
        return false;
    };
    if admit_rtcp_datagram(session_state, route.packet, route.now) {
        session_state.prepare_rtp_input(route.packet);
        if session_state.rtc.handle_input(input).is_err() {
            session_state.clear_ingress_context();
            warn!(
                user_id = ?session_key.user_id(),
                media_worker_id = session_key.media_worker_id().as_usize(),
                "failed to feed incoming UDP datagram into rtc user state"
            );
        } else {
            state.mark_session_dirty(session_key);
        }
    } else {
        route.metrics.record_rtc_rtcp_ingress_budget_drop();
    }
    // `Rtc::accepts()` already decided ownership. Later input processing does
    // not revise that decision, so retain the source pin on either result.
    let previous_session_key = state
        .remote_addr_demux
        .session_key_for_remote_addr(route.source_addr)
        .cloned();
    let source_pin_changed = state
        .remote_addr_demux
        .remember_remote_addr(route.source_addr, session_key);
    match (source_pin_changed, previous_session_key) {
        (true, Some(previous_session_key)) => {
            debug!(
                source_addr = %route.source_addr,
                candidate_addr = %route.candidate_addr,
                route_resolution,
                previous_session_id = ?previous_session_key.user_id(),
                previous_media_worker_id = previous_session_key.media_worker_id().as_usize(),
                user_id = ?session_key.user_id(),
                media_worker_id = session_key.media_worker_id().as_usize(),
                "remapped rtc source address to a different user"
            );
        }
        (true, None) => {
            debug!(
                source_addr = %route.source_addr,
                candidate_addr = %route.candidate_addr,
                route_resolution,
                user_id = ?session_key.user_id(),
                media_worker_id = session_key.media_worker_id().as_usize(),
                "pinned rtc source address to user"
            );
        }
        (false, _) => {}
    }
    route
        .metrics
        .record_rtc_datagram_route(RtcDatagramRoutePath::Scan);
    true
}

fn route_pkt_by_session(
    state: &mut PacketLoopState,
    demux: &mut DemuxRecoveryState,
    miss_key: PacketLoopRoutingMissKey,
    route: &PacketRouteContext<'_>,
) {
    let Some(session_key) = state.users.keys().next().cloned() else {
        return;
    };
    let Some(input) = receive_input(
        route.now,
        route.source_addr,
        route.candidate_addr,
        route.packet,
    ) else {
        drop_malformed_fallback(route);
        return;
    };
    let accepts_input = state
        .users
        .get(&session_key)
        .is_some_and(|session_state| session_state.rtc.accepts(&input));
    route.metrics.record_rtc_datagram_fallback_scan(1);
    if !accepts_input {
        record_no_user_miss(demux, miss_key, route);
        return;
    }
    if route_packet_to_session(state, &session_key, route, input, "single-user-scan") {
        demux.record_fallback_route_success(miss_key, route.packet, route.source_addr);
    }
}

fn route_pkt_by_recovery(
    state: &mut PacketLoopState,
    demux: &mut DemuxRecoveryState,
    miss_key: PacketLoopRoutingMissKey,
    route: &PacketRouteContext<'_>,
) {
    let Some(input) = receive_input(
        route.now,
        route.source_addr,
        route.candidate_addr,
        route.packet,
    ) else {
        drop_malformed_fallback(route);
        return;
    };
    let session_key = match indexed_session_for_pkt(
        &state.users,
        &mut state.remote_addr_demux,
        route.source_addr,
        route.candidate_addr,
        route.packet,
        &input,
    ) {
        IndexedSessionRecoveryOutcome::Matched {
            session_key,
            examined_sessions,
        } => {
            route
                .metrics
                .record_rtc_datagram_fallback_scan(examined_sessions);
            session_key
        }
        IndexedSessionRecoveryOutcome::NoMatch { examined_sessions } => {
            route
                .metrics
                .record_rtc_datagram_fallback_scan(examined_sessions);
            record_no_user_miss(demux, miss_key, route);
            return;
        }
        IndexedSessionRecoveryOutcome::Malformed => {
            drop_malformed_fallback(route);
            return;
        }
    };
    if route_packet_to_session(state, &session_key, route, input, "recovery-index") {
        demux.record_fallback_route_success(miss_key, route.packet, route.source_addr);
    }
}

fn drop_malformed_fallback(route: &PacketRouteContext<'_>) {
    route
        .metrics
        .record_rtc_datagram_drop(RtcDatagramDropReason::Malformed);
    log_malformed_datagram(route.source_addr);
}

fn record_no_user_miss(
    demux: &mut DemuxRecoveryState,
    miss_key: PacketLoopRoutingMissKey,
    route: &PacketRouteContext<'_>,
) {
    route
        .metrics
        .record_rtc_datagram_drop(RtcDatagramDropReason::NoUser);
    record_unknown_src_miss(demux, miss_key, route);
    trace!(
        source = %route.source_addr,
        "dropping UDP datagram because no rtc user accepted it"
    );
}

#[cfg(test)]
#[path = "TESTS/ingress_routing.rs"]
mod tests;
