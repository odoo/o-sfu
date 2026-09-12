//! Shared packet representation for observation, planning and fanout.
//!
//! Local RTP starts with a generation-checked session handle.
//! [`ForwardedPacket::resolve_facts`] binds it to source media and caches
//! [`PacketFacts`] for planning and receiver codec rewriting. Unresolved packets
//! are omitted from the current turn's observations and fanout.
//!
//! Relay copies retain the resolved facts and share payload bytes through [`Arc`].
//! Later registry changes do not replace that packet's source view. Local sends
//! borrow cached codec inspection through [`ForwardedPacket::local_codec_packet`]
//! so each destination uses the same source interpretation.

use std::{sync::Arc, time::Instant};

#[cfg(test)]
use str0m::media::Pt;
use str0m::{
    media::{Mid, Rid},
    rtp::{RtpHeader, RtpPacket, SeqNo, Ssrc},
};

use super::super::{
    codec,
    consumer_egress::LocalForwardedRtp,
    state::{PacketLoopState, slots::SessionHandle},
};
use crate::engine::{
    RoomInstanceId,
    media_transport::{TransportMediaId, TransportSessionKey},
};

#[cfg(any(test, feature = "internal-benchmarks"))]
#[path = "TESTS/support.rs"]
pub mod test_support;

/// one RTP packet staged for packet-loop observation and forwarding
///
/// a value may come directly from a local `str0m` session or from a relay
/// mailbox
/// both origins expose the same source identity, header metadata and
/// payload ownership contract to the rest of the packet loop
#[derive(Debug)]
pub struct ForwardedPacket {
    /// session that published the packet
    ///
    /// this also anchors the room instance used by source-policy wakeups and
    /// room-scoped packet sinks
    source: ForwardedPacketSource,
    /// producer identity resolved from MID, SSRC or relay metadata
    ///
    /// once this is known it is cached so later media-registry updates or relay
    /// delivery cannot bind the packet to a different source
    src_media: Option<TransportMediaId>,
    /// packet-scoped source observations shared by stats, route planning and egress
    facts: Option<PacketFacts>,
    /// whether source-worker side effects still belong to this packet
    ///
    /// relayed packets already passed through their origin worker, so target
    /// workers must not send them back into origin sinks or second-hop relays
    visits_origin_sinks: bool,
    /// whether str0m authenticated and normalized this RFC 4588 repair packet
    /// <https://www.rfc-editor.org/rfc/rfc4588.html#section-4>
    was_repair: bool,
    /// packet timestamp used for bitrate, activity and egress metrics
    received_at: Instant,
    /// source payload bytes shared by relay and local fanout
    payload: Arc<[u8]>,
    /// source RTP header used by observation and destination rewriting
    header: RtpHeader,
    /// extended source sequence preserved across rollover and reordering
    sequence_number: SeqNo,
}

/// source identity for one staged forwarded packet
///
/// local packets carry only a worker-local generation-checked handle while
/// relayed packets carry the stable public key needed to cross worker mailboxes
#[derive(Debug, Clone)]
pub(in super::super) enum ForwardedPacketSource {
    Local(SessionHandle),
    Relayed(TransportSessionKey),
}

impl ForwardedPacketSource {
    pub(in super::super) fn session_key<'a>(
        &'a self,
        state: &'a PacketLoopState,
    ) -> Option<&'a TransportSessionKey> {
        match self {
            Self::Local(session_handle) => state.users.key_for_handle(*session_handle),
            Self::Relayed(session_key) => Some(session_key),
        }
    }
}

/// Source observations resolved once and preserved across relay fanout.
///
/// Facts do not own packet bytes. Keeping them on the packet avoids resolving
/// source identity or inspecting codec payloads again after relay delivery.
#[derive(Debug, Clone, Copy)]
pub(in super::super) struct PacketFacts {
    /// source producer selected by MID, SSRC or relay metadata
    pub(in super::super) src_media: TransportMediaId,
    /// resolved RID used by packet-layer gates
    pub(in super::super) rid: Option<Rid>,
    /// room that owns source-policy wakeups for this packet
    pub(in super::super) room_instance_id: RoomInstanceId,
    /// audio activity flag projected from RTP header extensions
    pub(in super::super) voice_activity: Option<bool>,
    /// audio level projected from RTP header extensions
    pub(in super::super) audio_level: Option<i8>,
    /// source codec observations and private local rewrite state
    pub(in super::super) codec: codec::Packet,
}

impl ForwardedPacket {
    /// Stages one packet emitted by a local `str0m` session.
    ///
    /// Local and relay packets retain the same source header, extended sequence
    /// and arrival time. Moving the payload preserves shared storage for fanout.
    pub(in super::super) fn from_rtp_packet(
        source_session_handle: SessionHandle,
        rtp_packet: RtpPacket,
        was_repair: bool,
    ) -> Self {
        Self {
            source: ForwardedPacketSource::Local(source_session_handle),
            src_media: None,
            facts: None,
            visits_origin_sinks: true,
            was_repair,
            received_at: rtp_packet.timestamp,
            payload: rtp_packet.payload,
            header: rtp_packet.header,
            sequence_number: rtp_packet.seq_no,
        }
    }

    #[must_use]
    pub(in super::super) const fn source(&self) -> &ForwardedPacketSource {
        &self.source
    }

    #[must_use]
    pub(in super::super) fn src_key<'a>(
        &'a self,
        state: &'a PacketLoopState,
    ) -> Option<&'a TransportSessionKey> {
        self.source.session_key(state)
    }

    #[cfg(any(test, feature = "testing-transport"))]
    #[must_use]
    pub fn stable_src_key(&self) -> Option<&TransportSessionKey> {
        match &self.source {
            ForwardedPacketSource::Local(_) => None,
            ForwardedPacketSource::Relayed(session_key) => Some(session_key),
        }
    }

    #[must_use]
    pub const fn received_at(&self) -> Instant {
        self.received_at
    }

    #[must_use]
    pub fn payload(&self) -> &[u8] {
        self.payload.as_ref()
    }

    #[cfg(test)]
    #[must_use]
    pub(in super::super) fn repair_identity(&self) -> Option<(Pt, Ssrc, SeqNo)> {
        self.was_repair.then(|| {
            let header = &self.header;
            (header.payload_type, header.ssrc, self.sequence_number)
        })
    }

    /// Caches one source view for local and relay fanout.
    ///
    /// `None` means the packet cannot currently be attached to a source
    /// transport media id
    /// callers should treat that as a best-effort ingress miss and drop the
    /// packet from stats or fanout for this turn
    ///
    /// Cached facts travel with relay clones and remain the packet's source view
    /// even if registries change before delivery. Borrowing that view avoids
    /// copying codec rewrite state between packet-loop phases.
    pub(in super::super) fn resolve_facts(
        &mut self,
        state: &PacketLoopState,
    ) -> Option<&PacketFacts> {
        if self.facts.is_some() {
            return self.facts.as_ref();
        }
        let src_media = self.resolve_src_media(state)?;
        let rid = self.compute_route_control_rid(state);
        let codec = state.routes.inspect_packet(
            src_media,
            self.header.payload_type,
            self.payload.as_ref(),
            rid.is_some(),
        );
        let extensions = &self.header.ext_vals;
        let facts = PacketFacts {
            src_media,
            rid,
            room_instance_id: self.src_key(state)?.room_instance_id(),
            voice_activity: extensions.voice_activity,
            audio_level: extensions.audio_level,
            codec,
        };
        self.facts = Some(facts);
        self.facts.as_ref()
    }

    /// Borrows the resolved source view without changing packet state.
    ///
    /// Returns `None` until source facts have been resolved.
    pub(in super::super) fn cached_facts(&self) -> Option<&PacketFacts> {
        self.facts.as_ref()
    }

    /// Reuses source codec inspection across every local destination.
    ///
    /// incoming observation fills `PacketFacts` before route planning and local
    /// forwarding run, so codec inspection remains packet-scoped instead of
    /// destination-scoped
    pub(in super::super) fn local_codec_packet(&self) -> Option<&codec::Packet> {
        self.facts.as_ref().map(|facts| &facts.codec)
    }

    fn compute_route_control_rid(&self, state: &PacketLoopState) -> Option<Rid> {
        let extensions = &self.header.ext_vals;
        extensions
            .rid
            .or(extensions.rid_repair)
            .or_else(|| self.route_control_rid_from_ssrc(state))
    }

    pub(in super::super) fn route_control_ssrc(&self) -> Ssrc {
        self.header.ssrc
    }

    pub(in super::super) fn route_control_mid(&self) -> Option<Mid> {
        self.header.ext_vals.mid
    }

    pub(in super::super) fn route_control_rid_extension(&self) -> Option<Rid> {
        let extensions = &self.header.ext_vals;
        extensions.rid.or(extensions.rid_repair)
    }

    /// creates a relay-owned view that shares this packet payload
    ///
    /// the caller supplies the resolved source media id because relay targets
    /// must not depend on the target worker being able to rediscover source
    /// identity from local producer registries
    /// cached facts preserve the resolved RID so the receiving worker can reuse
    /// source observations
    pub(in super::super) fn share_for_relay(
        &self,
        state: &PacketLoopState,
        src_media: TransportMediaId,
    ) -> Option<Self> {
        Some(Self {
            source: ForwardedPacketSource::Relayed(self.source.session_key(state)?.clone()),
            src_media: Some(src_media),
            facts: self.facts,
            visits_origin_sinks: false,
            was_repair: self.was_repair,
            received_at: self.received_at,
            payload: Arc::clone(&self.payload),
            header: self.header.clone(),
            sequence_number: self.sequence_number,
        })
    }

    pub(in super::super) const fn visits_origin_sinks(&self) -> bool {
        self.visits_origin_sinks
    }

    /// resolves the source producer that owns this packet
    ///
    /// the lookup prefers cached facts, then an explicit relay media id, then
    /// source-worker MID and SSRC registries
    /// a successful lookup is cached on the packet so a later registry update
    /// cannot split one packet across different source identities
    pub(in super::super) fn resolve_src_media(
        &mut self,
        state: &PacketLoopState,
    ) -> Option<TransportMediaId> {
        if let Some(facts) = self.facts {
            return Some(facts.src_media);
        }
        if let Some(src_media) = self.src_media {
            return Some(src_media);
        }
        let src_key = self.source.session_key(state)?;
        let header = &self.header;
        let resolved = if let Some(source_mid) = header.ext_vals.mid
            && let Some(src_media) = state.src_media_for_mid(src_key, source_mid)
        {
            Some(src_media)
        } else {
            state.src_media_for_ssrc(src_key, header.ssrc)
        };
        if let Some(src_media) = resolved {
            self.src_media = Some(src_media);
        }
        resolved
    }

    /// Avoids payload-byte copies during local fanout by borrowing source headers
    /// and the shared payload.
    pub(in super::super) fn local_send_packet(&self) -> LocalForwardedRtp<'_> {
        LocalForwardedRtp::new(
            &self.header,
            self.sequence_number,
            self.received_at,
            &self.payload,
            self.was_repair,
        )
    }

    fn route_control_rid_from_ssrc(&self, state: &PacketLoopState) -> Option<Rid> {
        let src_key = self.source.session_key(state)?;
        state.source_rid_for_ssrc(src_key, self.header.ssrc)
    }
}

#[cfg(test)]
#[expect(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;
