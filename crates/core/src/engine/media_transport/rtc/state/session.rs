//! Session-local str0m state, staged negotiation and ingress accounting.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
    time::Instant,
};

use o_sfu_rfc::rtp;
use o_sfu_router::rtp::MediaStream as RouterRtpParameters;
use str0m::{
    Rtc,
    change::{SdpOffer, SdpPendingOffer},
    media::{Mid, Rid},
    rtp::Ssrc,
};

use super::{
    super::{codec::RepairSummary, consumer_egress::ConsumerStreamStore},
    bitrate::MediaBitrateCounter,
};
use crate::{Bitrate, engine::media_transport::SessionUploadSlot};

// Across str0m's three-second resend-cache window the local rate permits
// 20_000 bytes. A maximally dense Generic NACK can therefore reference
// 85_000 sequence numbers.
// https://www.rfc-editor.org/rfc/rfc4585.html#section-6.2.1
pub(in super::super) const RTCP_INGRESS_BUDGET_CAPACITY_BYTES: u64 = 8_000;
pub(in super::super) const RTCP_INGRESS_BUDGET_REFILL_BYTES_PER_SECOND: u64 = 4_000;
const NANOS_PER_SECOND: u64 = 1_000_000_000;

/// worker-local [`str0m::Rtc`] state for one transport session
///
/// this state is single-threaded under [`super::PacketLoopState`]
/// control commands mutate negotiation or routing facts before the packet loop
/// polls `rtc`, which keeps `str0m` access ordered without a per-session lock
pub(in super::super) struct RtcSessionState {
    /// public room UUID used to correlate structured transport events
    pub(in super::super) room_id: Arc<str>,
    /// sans-I/O WebRTC engine driven only by the packet-loop worker
    pub(in super::super) rtc: Rtc,
    /// creation time used for transport lifetime metrics during session teardown
    pub(in super::super) started_at: Instant,
    /// candidate RTCP admission state at the pre-authentication boundary
    pub(in super::super) rtcp_ingress_budget: RtcpIngressBudget,
    /// whether admitted RTCP already checked RTX age at receive time and its
    /// next output drain must poll before applying a newer clock
    pub(in super::super) defer_rtx_expiry: bool,
    /// Wire SSRC retained across input and output polling so RTX remains
    /// distinguishable after str0m normalizes the event to its primary SSRC.
    /// <https://www.rfc-editor.org/rfc/rfc4588.html#section-4>
    pub(in super::super) pending_rtp_input: Option<Ssrc>,
    /// Per-MID/RID baselines that convert cumulative str0m NACK counts to
    /// deltas.
    pub(in super::super) nack_totals: RtcNackTotals,
    /// shared writer and cold-reader handle for sent media bitrate
    pub(in super::super) egress_bitrate: Arc<MediaBitrateCounter>,
    /// local ICE fragment registered in demux recovery hints
    pub(in super::super) local_ice_ufrag: String,
    #[cfg(test)]
    /// last inbound bitrate cap applied by tests that inspect negotiation refreshes
    pub(in super::super) max_bitrate_in: Option<Bitrate>,
    #[cfg(test)]
    /// outbound bitrate cap used to build the session in deterministic tests
    pub(in super::super) max_bitrate_out: Option<Bitrate>,
    /// last desired receiver-side send bitrate for str0m BWE, retained to skip
    /// duplicate updates
    pub(in super::super) receiver_bwe_target: Option<Bitrate>,
    #[cfg(test)]
    /// number of non-deduped desired-bitrate writes issued to str0m BWE
    pub(in super::super) receiver_bwe_str0m_update_count: u64,
    pub(in super::super) dtls_started: bool,
    /// scheduler bit that prevents duplicate dirty-session wakeups
    pub(in super::super) packet_loop_dirty: bool,
    /// Current str0m deadline used to reject stale timeout heap entries.
    pub(in super::super) next_timeout: Option<Instant>,
    /// staged SDP state owned by the worker-local offer and answer paths
    pub(in super::super) sdp_negotiation: SessionSdpNegotiationState,
    /// monotonic RTP identity state keyed by consumer transport media
    ///
    /// this belongs to the destination session because the browser sees one
    /// local RTP stream per consumer route, independent from whichever
    /// publisher SSRC or RID currently feeds that route
    pub(in super::super) consumer_streams: ConsumerStreamStore,
}

impl RtcSessionState {
    pub(in super::super) fn prepare_rtp_input(&mut self, packet: &[u8]) {
        self.pending_rtp_input = muxed_rtp_ssrc(packet);
    }

    pub(in super::super) fn take_rtp_repair(&mut self, primary_ssrc: Ssrc) -> bool {
        self.pending_rtp_input
            .take()
            .is_some_and(|outer_ssrc| outer_ssrc != primary_ssrc)
    }

    pub(in super::super) fn clear_ingress_context(&mut self) {
        self.pending_rtp_input = None;
        self.defer_rtx_expiry = false;
    }
}

pub(in super::super) fn muxed_rtp_ssrc(packet: &[u8]) -> Option<Ssrc> {
    // The SSRC is part of the fixed RTP header. RTCP packets sharing the port
    // must be rejected before reading that field.
    // https://www.rfc-editor.org/rfc/rfc3550.html#section-5.1
    // https://www.rfc-editor.org/rfc/rfc5761.html#section-4
    let ssrc = rtp::parse_muxed_rtp_fixed_header(packet)?.ssrc();
    Some(Ssrc::from(ssrc.value()))
}

pub(in super::super) struct RtcpIngressBudget {
    available_bytes: u64,
    refill_remainder: u64,
    last_refill_at: Instant,
}

impl RtcpIngressBudget {
    pub(in super::super) const fn new(now: Instant) -> Self {
        Self {
            available_bytes: RTCP_INGRESS_BUDGET_CAPACITY_BYTES,
            refill_remainder: 0,
            last_refill_at: now,
        }
    }

    pub(in super::super) fn try_charge(&mut self, bytes: u64, now: Instant) -> bool {
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
pub(in super::super) struct SessionSdpNegotiationState {
    /// initial audio and video MIDs reused across repeated bootstrap offer attempts
    pub(in super::super) bootstrap_mids: Vec<Mid>,
    /// Pending str0m changes paired with the repair mappings offered for them.
    pub(in super::super) pending_offer: Option<PendingSessionOffer>,
    /// follow-up local offer prepared by media lifecycle and not yet delivered
    pub(in super::super) staged_offer: Option<Box<SdpOffer>>,
    pub(in super::super) staged_offer_upload_slots: Vec<SessionUploadSlot>,
    /// whether the remote side has answered the initial transport offer
    pub(in super::super) initial_offer_applied: bool,
    /// producer recv identities that must be rebound after answer application
    ///
    /// answer-time `str0m` updates can recreate `StreamRx` bindings, so the
    /// worker keeps the intended SSRC and RID pairs until the matching answer
    /// has been applied
    pub(in super::super) pending_recv_streams: BTreeMap<Mid, Vec<PendingRecvStream>>,
    /// negotiated producer RTP parameters keyed by producer MID
    ///
    /// answer application refreshes this table so transport handles can report
    /// the exact upload encodings that are now committed
    pub(in super::super) negotiated_producer_parameters: BTreeMap<Mid, RouterRtpParameters>,
    /// negotiated MIDs whose inactive offer must wait for the current answer
    pub(in super::super) queued_removal_mids: BTreeSet<Mid>,
}

impl SessionSdpNegotiationState {
    /// Replaces the undelivered offer and the upload metadata retained through its answer.
    pub(in super::super) fn stage_offer(
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
pub(in super::super) struct PendingSessionOffer {
    pub(in super::super) token: SdpPendingOffer,
    pub(in super::super) repair: RepairSummary,
}

impl PendingSessionOffer {
    pub(in super::super) fn new(offer: &SdpOffer, token: SdpPendingOffer) -> Self {
        Self {
            token,
            repair: RepairSummary::from_offer(offer),
        }
    }
}

/// receive stream identity staged before a producer media addition is answered
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in super::super) struct PendingRecvStream {
    /// producer SSRC expected by the receiving `str0m` media line
    pub(in super::super) ssrc: Ssrc,
    /// repair SSRC paired with the producer primary SSRC when RTX is negotiated
    pub(in super::super) repair_ssrc: Option<Ssrc>,
    /// simulcast RID for the producer encoding when the source uses RID identity
    pub(in super::super) rid: Option<Rid>,
}

#[derive(Debug, Default)]
pub(in super::super) struct RtcNackTotals {
    sent_to_publisher: HashMap<(Mid, Option<Rid>), u64>,
    received_from_subscriber: HashMap<(Mid, Option<Rid>), u64>,
}

impl RtcNackTotals {
    pub(in super::super) fn sent_to_publisher(
        &mut self,
        mid: Mid,
        rid: Option<Rid>,
        total: u64,
    ) -> u64 {
        Self::delta(&mut self.sent_to_publisher, mid, rid, total)
    }

    pub(in super::super) fn received_from_subscriber(
        &mut self,
        mid: Mid,
        rid: Option<Rid>,
        total: u64,
    ) -> u64 {
        Self::delta(&mut self.received_from_subscriber, mid, rid, total)
    }

    pub(in super::super) fn remove_mid(&mut self, mid: Mid) {
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
