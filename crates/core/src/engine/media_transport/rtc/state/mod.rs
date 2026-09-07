//! worker-local state for the RTC engine transport layer
//!
//! this module holds the mutable facts that must stay local to one packet-loop
//! worker:
//!
//! ```text
//! room and router intent
//!   |
//!   v
//! worker commands
//!   |
//!   v
//! PacketLoopState
//!   |
//!   +--> str0m sessions
//!   +--> media route indexes
//!   +--> demux hints
//!   +--> packet-loop schedules
//! ```
//!
//! `PacketLoopState` is the authoritative hot-path state
//! callers outside the worker observe selected facts through
//! `RtcSnapshotState`, bitrate counters and diagnostics instead of mutating
//! this state directly
//!
use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap},
    net::SocketAddr,
    sync::Arc,
    time::Instant,
};

use o_sfu_rfc::rtp;
use o_sfu_router::rtp::MediaStream as RouterRtpParameters;
#[cfg(test)]
use str0m::rtp::SeqNo;
use str0m::{
    Rtc,
    change::{SdpOffer, SdpPendingOffer},
    media::{Mid, Rid},
    rtp::Ssrc,
};

use super::{
    bitrate::MediaBitrateCounter,
    codec::RepairSummary,
    demux::RemoteAddrDemux,
    local_send_rewrite::ConsumerStreamStore,
    media_registry::{MediaStore, SessionMediaRegistry},
    packet_loop::{RtcUdpSocket, UdpIngress},
    route_table::{RidReadinessScratch, RouteTable},
    slots::{SessionHandle, SessionStore},
};
pub use crate::engine::media_transport::TransportSessionHealth;
use crate::{
    Bitrate,
    engine::media_transport::{
        ReceiverBandwidthSnapshot, SessionUploadSlot, TransportHealthSnapshot, TransportMediaId,
        TransportQualitySample, TransportQualitySnapshot, TransportSessionKey,
    },
};

// Across str0m's three-second resend-cache window the local rate permits
// 20_000 bytes. A maximally dense Generic NACK can therefore reference
// 85_000 sequence numbers.
// https://www.rfc-editor.org/rfc/rfc4585.html#section-6.2.1
pub(super) const RTCP_INGRESS_BUDGET_CAPACITY_BYTES: u64 = 8_000;
pub(super) const RTCP_INGRESS_BUDGET_REFILL_BYTES_PER_SECOND: u64 = 4_000;
const NANOS_PER_SECOND: u64 = 1_000_000_000;

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

/// worker-local [`str0m::Rtc`] state for one transport session
///
/// this state is single-threaded under [`PacketLoopState`]
/// control commands mutate negotiation or routing facts before the packet loop
/// polls `rtc`, which keeps `str0m` access ordered without a per-session lock
pub(super) struct RtcSessionState {
    /// public room UUID used to correlate structured transport events
    pub(super) room_id: Arc<str>,
    /// sans-I/O WebRTC engine driven only by the packet-loop worker
    pub(super) rtc: Rtc,
    /// creation time used for transport lifetime metrics during session teardown
    pub(super) started_at: Instant,
    /// candidate RTCP admission state at the pre-authentication boundary
    pub(super) rtcp_ingress_budget: RtcpIngressBudget,
    /// whether admitted RTCP already checked RTX age at receive time and its
    /// next output drain must poll before applying a newer clock
    pub(super) defer_rtx_expiry: bool,
    /// Wire SSRC retained across input and output polling so RTX remains
    /// distinguishable after str0m normalizes the event to its primary SSRC.
    /// <https://www.rfc-editor.org/rfc/rfc4588.html#section-4>
    pub(super) pending_rtp_input: Option<Ssrc>,
    /// Per-MID/RID baselines that convert cumulative str0m NACK counts to
    /// deltas.
    pub(super) nack_totals: RtcNackTotals,
    /// shared writer and cold-reader handle for sent media bitrate
    pub(super) egress_bitrate: Arc<MediaBitrateCounter>,
    /// local ICE fragment registered in demux recovery hints
    pub(super) local_ice_ufrag: String,
    #[cfg(test)]
    /// last inbound bitrate cap applied by tests that inspect negotiation refreshes
    pub(super) max_bitrate_in: Option<Bitrate>,
    #[cfg(test)]
    /// outbound bitrate cap used to build the session in deterministic tests
    pub(super) max_bitrate_out: Option<Bitrate>,
    /// last desired receiver-side send bitrate for str0m BWE, retained to skip
    /// duplicate updates
    pub(super) receiver_bwe_target: Option<Bitrate>,
    #[cfg(test)]
    /// number of non-deduped desired-bitrate writes issued to str0m BWE
    pub(super) receiver_bwe_str0m_update_count: u64,
    pub(super) dtls_started: bool,
    /// scheduler bit that prevents duplicate dirty-session wakeups
    pub(super) packet_loop_dirty: bool,
    /// Current str0m deadline used to reject stale timeout heap entries.
    pub(super) next_timeout: Option<Instant>,
    /// staged SDP state owned by the worker-local offer and answer paths
    pub(super) sdp_negotiation: SessionSdpNegotiationState,
    /// monotonic RTP identity state keyed by consumer transport media
    ///
    /// this belongs to the destination session because the browser sees one
    /// local RTP stream per consumer route, independent from whichever
    /// publisher SSRC or RID currently feeds that route
    pub(super) consumer_streams: ConsumerStreamStore,
    #[cfg(test)]
    /// Accepted projected sequence and payload at the `StreamTx::write_rtp`
    /// queue boundary.
    pub(super) last_local_write: Option<(SeqNo, Arc<[u8]>)>,
}

impl RtcSessionState {
    pub(super) fn prepare_rtp_input(&mut self, packet: &[u8]) {
        self.pending_rtp_input = muxed_rtp_ssrc(packet);
    }

    pub(super) fn take_rtp_repair(&mut self, primary_ssrc: Ssrc) -> bool {
        self.pending_rtp_input
            .take()
            .is_some_and(|outer_ssrc| outer_ssrc != primary_ssrc)
    }

    pub(super) fn clear_ingress_context(&mut self) {
        self.pending_rtp_input = None;
        self.defer_rtx_expiry = false;
    }
}

pub(super) fn muxed_rtp_ssrc(packet: &[u8]) -> Option<Ssrc> {
    // The SSRC is part of the fixed RTP header. RTCP packets sharing the port
    // must be rejected before reading that field.
    // https://www.rfc-editor.org/rfc/rfc3550.html#section-5.1
    // https://www.rfc-editor.org/rfc/rfc5761.html#section-4
    let ssrc = rtp::parse_muxed_rtp_fixed_header(packet)?.ssrc();
    Some(Ssrc::from(ssrc.value()))
}

pub(super) struct RtcpIngressBudget {
    available_bytes: u64,
    refill_remainder: u64,
    last_refill_at: Instant,
}

impl RtcpIngressBudget {
    pub(super) const fn new(now: Instant) -> Self {
        Self {
            available_bytes: RTCP_INGRESS_BUDGET_CAPACITY_BYTES,
            refill_remainder: 0,
            last_refill_at: now,
        }
    }

    pub(super) fn try_charge(&mut self, bytes: u64, now: Instant) -> bool {
        self.refill(now);
        if bytes > self.available_bytes {
            return false;
        }
        self.available_bytes -= bytes;
        true
    }

    fn refill(&mut self, now: Instant) {
        let Some(elapsed) = now.checked_duration_since(self.last_refill_at) else {
            return;
        };
        self.last_refill_at = now;
        if self.available_bytes == RTCP_INGRESS_BUDGET_CAPACITY_BYTES {
            self.refill_remainder = 0;
            return;
        }
        let fractional_credit = u64::from(elapsed.subsec_nanos())
            * RTCP_INGRESS_BUDGET_REFILL_BYTES_PER_SECOND
            + self.refill_remainder;
        let refill_bytes = elapsed
            .as_secs()
            .saturating_mul(RTCP_INGRESS_BUDGET_REFILL_BYTES_PER_SECOND)
            .saturating_add(fractional_credit / NANOS_PER_SECOND);
        let missing_bytes = RTCP_INGRESS_BUDGET_CAPACITY_BYTES - self.available_bytes;
        if refill_bytes >= missing_bytes {
            self.available_bytes = RTCP_INGRESS_BUDGET_CAPACITY_BYTES;
            self.refill_remainder = 0;
        } else {
            self.available_bytes += refill_bytes;
            self.refill_remainder = fractional_credit % NANOS_PER_SECOND;
        }
    }
}

/// offer and answer staging state for one worker-local session
///
/// this preserves `str0m`'s one-outstanding-offer rule while media lifecycle
/// code can stage additions or removals through serialized worker commands
#[derive(Default)]
pub(super) struct SessionSdpNegotiationState {
    /// initial audio and video MIDs reused across repeated bootstrap offer attempts
    pub(super) bootstrap_mids: Vec<Mid>,
    /// Pending str0m changes paired with the repair mappings offered for them.
    pub(super) pending_offer: Option<PendingSessionOffer>,
    /// follow-up local offer prepared by media lifecycle and not yet delivered
    pub(super) staged_offer: Option<Box<SdpOffer>>,
    pub(super) staged_offer_upload_slots: Vec<SessionUploadSlot>,
    /// whether the remote side has answered the initial transport offer
    pub(super) initial_offer_applied: bool,
    /// producer recv identities that must be rebound after answer application
    ///
    /// answer-time `str0m` updates can recreate `StreamRx` bindings, so the
    /// worker keeps the intended SSRC and RID pairs until the matching answer
    /// has been applied
    pub(super) pending_recv_streams: BTreeMap<Mid, Vec<PendingRecvStream>>,
    /// negotiated producer RTP parameters keyed by producer MID
    ///
    /// answer application refreshes this table so transport handles can report
    /// the exact upload encodings that are now committed
    pub(super) negotiated_producer_parameters: BTreeMap<Mid, RouterRtpParameters>,
    /// negotiated MIDs whose inactive offer must wait for the current answer
    pub(super) queued_removal_mids: BTreeSet<Mid>,
}

impl SessionSdpNegotiationState {
    /// Replaces the undelivered offer and the upload metadata retained through its answer.
    pub(super) fn stage_offer(
        &mut self,
        offer: SdpOffer,
        pending_offer: SdpPendingOffer,
        upload_slots: Vec<SessionUploadSlot>,
    ) {
        self.pending_offer = Some(PendingSessionOffer::new(&offer, pending_offer));
        self.staged_offer = Some(Box::new(offer));
        self.staged_offer_upload_slots = upload_slots;
    }
}

/// A str0m answer token and the repair mappings from its corresponding local offer.
pub(super) struct PendingSessionOffer {
    pub(super) token: SdpPendingOffer,
    pub(super) repair: RepairSummary,
}

impl PendingSessionOffer {
    pub(super) fn new(offer: &SdpOffer, token: SdpPendingOffer) -> Self {
        Self {
            token,
            repair: RepairSummary::from_offer(offer),
        }
    }
}

/// receive stream identity staged before a producer media addition is answered
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PendingRecvStream {
    /// producer SSRC expected by the receiving `str0m` media line
    pub(super) ssrc: Ssrc,
    /// repair SSRC paired with the producer primary SSRC when RTX is negotiated
    pub(super) repair_ssrc: Option<Ssrc>,
    /// simulcast RID for the producer encoding when the source uses RID identity
    pub(super) rid: Option<Rid>,
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

impl PacketLoopState {
    /// schedule a live session for the next packet-loop poll
    ///
    /// missing sessions are ignored because teardown may race with already
    /// queued wakeups
    /// each live session can appear at most once until
    /// [`Self::collect_ready_sessions`] clears its dirty bit
    pub(super) fn mark_session_dirty(&mut self, session_key: &TransportSessionKey) {
        let Some(session_handle) = self.users.handle_for_key(session_key) else {
            return;
        };
        let Some(session_state) = self.users.get_mut_by_handle(session_handle) else {
            return;
        };
        if session_state.packet_loop_dirty {
            return;
        }
        session_state.packet_loop_dirty = true;
        self.dirty_sessions.push(session_handle);
    }

    /// report whether the worker has session work that is due immediately
    pub(super) fn has_dirty_sessions(&self) -> bool {
        !self.dirty_sessions.is_empty()
    }

    /// drain dirty sessions and due `str0m` timeouts into caller-owned scratch
    ///
    /// this method is the session scheduler for the packet loop
    /// it clears dirty bits for live sessions, skips removed sessions and lazily
    /// discards timeout heap entries whose deadline no longer matches
    /// [`RtcSessionState::next_timeout`]
    ///
    /// stale handles are skipped before replacement sessions can be polled
    ///
    /// the output is sorted and deduplicated so a session that is both dirty
    /// and timed out is polled once in the current turn
    pub(super) fn collect_ready_sessions(
        &mut self,
        now: Instant,
        ready_sessions: &mut Vec<SessionHandle>,
    ) {
        for session_handle in self.dirty_sessions.drain(..) {
            if let Some(session_state) = self.users.get_mut_by_handle(session_handle) {
                // Clear before polling. A later mutation in this turn must be able
                // to enqueue the session again instead of being hidden by this mark.
                session_state.packet_loop_dirty = false;
                ready_sessions.push(session_handle);
            }
        }
        while let Some(&Reverse((deadline, session_handle))) = self.timeout_queue.peek() {
            if deadline > now {
                break;
            }
            self.timeout_queue.pop();
            let Some(session_state) = self.users.get_mut_by_handle(session_handle) else {
                continue;
            };
            if session_state.next_timeout == Some(deadline) {
                session_state.next_timeout = None;
                ready_sessions.push(session_handle);
            }
        }
        ready_sessions.sort_unstable();
        ready_sessions.dedup();
    }

    /// replace the next `str0m` timeout deadline by worker-local handle
    ///
    /// stale handles are ignored because the session has already left this
    /// worker or the slot now belongs to a later generation
    pub(super) fn update_session_timeout_by_handle(
        &mut self,
        session_handle: SessionHandle,
        next_timeout: Option<Instant>,
    ) {
        let Some(session_state) = self.users.get_mut_by_handle(session_handle) else {
            return;
        };
        // Packet-driven polls can return the same future str0m deadline.
        if session_state.next_timeout == next_timeout {
            return;
        }
        session_state.next_timeout = next_timeout;
        if let Some(next_timeout) = next_timeout {
            self.timeout_queue
                .push(Reverse((next_timeout, session_handle)));
        }
    }

    #[cfg(test)]
    pub(super) fn update_session_timeout(
        &mut self,
        session_key: &TransportSessionKey,
        next_timeout: Option<Instant>,
    ) {
        let Some(session_handle) = self.users.handle_for_key(session_key) else {
            return;
        };
        self.update_session_timeout_by_handle(session_handle, next_timeout);
    }

    /// return the earliest live `str0m` timeout deadline
    ///
    /// stale heap entries are removed while searching
    /// this includes entries for handles whose slot generation no longer names
    /// a live session
    /// callers may invoke this before awaiting because it does not borrow any
    /// session state after returning
    pub(super) fn next_timeout_deadline(&mut self) -> Option<Instant> {
        loop {
            let &Reverse((deadline, session_handle)) = self.timeout_queue.peek()?;
            if self
                .users
                .get_by_handle(session_handle)
                .is_some_and(|session| session.next_timeout == Some(deadline))
            {
                return Some(deadline);
            }
            self.timeout_queue.pop();
        }
    }

    /// remove all explicit scheduler state for a session being torn down
    ///
    /// stale timeout heap entries can remain because the session no longer
    /// validates their deadline
    /// stale handles are also rejected by generation checks before polling
    pub(super) fn clear_session_schedule(&mut self, session_key: &TransportSessionKey) {
        let Some(session_handle) = self.users.handle_for_key(session_key) else {
            return;
        };
        self.dirty_sessions.retain(|dirty| *dirty != session_handle);
        if let Some(session_state) = self.users.get_mut_by_handle(session_handle) {
            session_state.packet_loop_dirty = false;
            session_state.next_timeout = None;
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct RtcNackTotals {
    sent_to_publisher: HashMap<(Mid, Option<Rid>), u64>,
    received_from_subscriber: HashMap<(Mid, Option<Rid>), u64>,
}

impl RtcNackTotals {
    pub(super) fn sent_to_publisher(&mut self, mid: Mid, rid: Option<Rid>, total: u64) -> u64 {
        Self::delta(&mut self.sent_to_publisher, mid, rid, total)
    }

    pub(super) fn received_from_subscriber(
        &mut self,
        mid: Mid,
        rid: Option<Rid>,
        total: u64,
    ) -> u64 {
        Self::delta(&mut self.received_from_subscriber, mid, rid, total)
    }

    pub(super) fn remove_mid(&mut self, mid: Mid) {
        self.sent_to_publisher
            .retain(|(entry_mid, _), _| *entry_mid != mid);
        self.received_from_subscriber
            .retain(|(entry_mid, _), _| *entry_mid != mid);
    }

    /// Returns the cumulative increase or the new total after a stream counter reset.
    fn delta(
        totals: &mut HashMap<(Mid, Option<Rid>), u64>,
        mid: Mid,
        rid: Option<Rid>,
        total: u64,
    ) -> u64 {
        totals.insert((mid, rid), total).map_or(total, |previous| {
            total.checked_sub(previous).unwrap_or(total)
        })
    }
}

/// read-side RTC transport snapshot shared outside the packet loop
///
/// this state mirrors facts that diagnostics, placement and transport policy
/// need without exposing mutable [`PacketLoopState`]
/// it is protected by a cold-path mutex while packet-path state remains
/// worker-local and single-threaded
#[derive(Debug, Default)]
pub struct RtcSnapshotState {
    /// latest observed transport health by session
    transport_health: BTreeMap<TransportSessionKey, TransportSessionHealth>,
    /// latest receiver bandwidth estimate by session
    receiver_bandwidth: BTreeMap<TransportSessionKey, Bitrate>,
    /// latest sampled media quality by session
    transport_quality: BTreeMap<TransportSessionKey, TransportQualitySample>,
}

impl RtcSnapshotState {
    /// remove every read-side fact owned by one session
    ///
    /// returns the previous transport health so teardown metrics can record the
    /// final transition without doing a second lookup
    pub(super) fn remove_session(
        &mut self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        self.receiver_bandwidth.remove(session_key);
        self.transport_quality.remove(session_key);
        self.transport_health.remove(session_key)
    }

    /// replace the latest transport health observation for one session
    ///
    /// returns the previous value so callers can record health transitions
    pub(super) fn set_transport_health(
        &mut self,
        session_key: &TransportSessionKey,
        health: TransportSessionHealth,
    ) -> Option<TransportSessionHealth> {
        self.transport_health.insert(session_key.clone(), health)
    }

    /// return the latest health observation for a session
    ///
    /// missing health means the packet loop has not observed a transport event
    /// for that session or the session was removed
    pub fn transport_health(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        self.transport_health.get(session_key).copied()
    }

    /// Build a transport-health snapshot for the requested sessions.
    pub fn transport_health_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportHealthSnapshot {
        session_keys
            .iter()
            .filter_map(|key| {
                self.transport_health
                    .get(key)
                    .copied()
                    .map(|health| (key.clone(), health))
            })
            .collect()
    }

    /// replace the latest receiver bandwidth estimate for one session
    pub(super) fn set_receiver_bandwidth(
        &mut self,
        session_key: &TransportSessionKey,
        estimate: Bitrate,
    ) -> Option<Bitrate> {
        self.receiver_bandwidth
            .insert(session_key.clone(), estimate)
    }

    /// build a receiver bandwidth snapshot for the requested sessions
    ///
    /// sessions without an estimate are omitted so callers can distinguish
    /// missing observations from a zero bitrate estimate
    pub fn receiver_bandwidth_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> ReceiverBandwidthSnapshot {
        ReceiverBandwidthSnapshot {
            per_session: session_keys
                .iter()
                .filter_map(|session_key| {
                    self.receiver_bandwidth
                        .get(session_key)
                        .copied()
                        .map(|estimate| (session_key.clone(), estimate))
                })
                .collect(),
        }
    }

    /// update sampled transport-quality observations for one session
    pub(super) fn update_transport_quality(
        &mut self,
        session_key: &TransportSessionKey,
        update: impl FnOnce(&mut TransportQualitySample),
    ) {
        let sample = self
            .transport_quality
            .entry(session_key.clone())
            .or_default();
        sample.sample_count = sample.sample_count.saturating_add(1);
        update(sample);
    }

    /// build a transport-quality snapshot for the requested sessions
    pub fn transport_quality_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportQualitySnapshot {
        session_keys
            .iter()
            .filter_map(|session_key| {
                self.transport_quality
                    .get(session_key)
                    .copied()
                    .map(|sample| (session_key.clone(), sample))
            })
            .collect()
    }
}
