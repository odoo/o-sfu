//! the packet loop needs one packet shape that can cross four local concerns:
//! observation, fanout planning, local browser egress and relay egress
//! `ForwardedPacket` is that shape
//! it keeps the source session, RTP header view and `Arc` payload together
//! while letting each concern borrow only the state it owns
//!
//! relay and multi-destination local sends share the source bytes through `Arc`
//! callers that need cached source observations must read them before entering
//! the local-send path
//!
//! source-derived observations live in `PacketFacts`
//! they are cached after source resolution so incoming stats, fanout planning
//! and local codec rewriting reuse the same media id, RID and codec inspection
//! across local and relay fanout

use std::{mem::take, sync::Arc, time::Instant};

#[cfg(test)]
use str0m::media::Pt;
use str0m::{
    media::{ExtensionValues, Mid, Rid},
    rtp::{RtpHeader, RtpPacket, SeqNo, Ssrc},
};

use super::{
    codec, local_forwarding::LocalForwardedRtp, slots::SessionHandle, state::PacketLoopState,
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
    /// origin-specific RTP header storage
    data: ForwardedPacketData,
}

/// source identity for one staged forwarded packet
///
/// local packets carry only a worker-local generation-checked handle while
/// relayed packets carry the stable public key needed to cross worker mailboxes
#[derive(Debug, Clone)]
pub(super) enum ForwardedPacketSource {
    Local(SessionHandle),
    Relayed(TransportSessionKey),
}

impl ForwardedPacketSource {
    pub(super) fn session_key<'a>(
        &'a self,
        state: &'a PacketLoopState,
    ) -> Option<&'a TransportSessionKey> {
        match self {
            Self::Local(session_handle) => state.users.key_for_handle(*session_handle),
            Self::Relayed(session_key) => Some(session_key),
        }
    }
}

#[derive(Debug)]
enum ForwardedPacketData {
    Str0mRtp(ForwardedRtpData),
    RelayRtp(ForwardedRelayRtpData),
}

/// local `str0m` packet metadata kept after the payload was moved out
#[derive(Debug)]
struct ForwardedRtpData {
    rtp_packet: RtpPacket,
}

/// relay packet metadata after payload ownership has moved to `ForwardedPacket`
#[derive(Debug)]
pub(super) struct ForwardedRelayRtpData {
    pub(super) header: RtpHeader,
    /// extended source sequence preserved across rollover and reordering
    pub(super) sequence_number: SeqNo,
}

/// Source observations resolved once and preserved across relay fanout.
///
/// Facts do not own packet bytes. Keeping them on the packet avoids resolving
/// source identity or inspecting codec payloads again after relay delivery.
#[derive(Debug, Clone, Copy)]
pub(super) struct PacketFacts {
    /// source producer selected by MID, SSRC or relay metadata
    pub(super) src_media: TransportMediaId,
    /// resolved RID used by packet-layer gates
    pub(super) rid: Option<Rid>,
    /// room that owns source-policy wakeups for this packet
    pub(super) room_instance_id: RoomInstanceId,
    /// audio activity flag projected from RTP header extensions
    pub(super) voice_activity: Option<bool>,
    /// audio level projected from RTP header extensions
    pub(super) audio_level: Option<i8>,
    /// source codec observations and private local rewrite state
    pub(super) codec: codec::Packet,
}

impl ForwardedPacket {
    /// stages one packet emitted by a local `str0m` session
    ///
    /// the payload is moved out of the str0m packet immediately so relay fanout
    /// and local fanout can share the same ownership model
    /// the remaining `str0m` packet is kept only for header metadata and the
    /// original receive time
    pub(super) fn from_rtp_packet(
        source_session_handle: SessionHandle,
        mut rtp_packet: RtpPacket,
        was_repair: bool,
    ) -> Self {
        Self {
            source: ForwardedPacketSource::Local(source_session_handle),
            src_media: None,
            facts: None,
            visits_origin_sinks: true,
            was_repair,
            received_at: rtp_packet.timestamp,
            payload: take(&mut rtp_packet.payload),
            data: ForwardedPacketData::Str0mRtp(ForwardedRtpData { rtp_packet }),
        }
    }

    #[must_use]
    pub(super) const fn source(&self) -> &ForwardedPacketSource {
        &self.source
    }

    #[must_use]
    pub(super) fn src_key<'a>(
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
    pub(super) fn repair_identity(&self) -> Option<(Pt, Ssrc, SeqNo)> {
        let sequence_number = match &self.data {
            ForwardedPacketData::Str0mRtp(data) => data.rtp_packet.seq_no,
            ForwardedPacketData::RelayRtp(data) => data.sequence_number,
        };
        self.was_repair.then(|| {
            let header = self.rtp_header();
            (header.payload_type, header.ssrc, sequence_number)
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
    /// even if registries change before delivery.
    pub(super) fn resolve_facts(&mut self, state: &PacketLoopState) -> Option<PacketFacts> {
        if let Some(facts) = self.facts {
            return Some(facts);
        }
        let src_media = self.resolve_src_media(state)?;
        let rid = self.compute_route_control_rid(state);
        let codec = state.routes.inspect_packet(
            src_media,
            self.rtp_header().payload_type,
            self.payload.as_ref(),
            rid.is_some(),
        );
        let extensions = self.route_control_extension_values();
        let facts = PacketFacts {
            src_media,
            rid,
            room_instance_id: self.src_key(state)?.room_instance_id(),
            voice_activity: extensions.voice_activity,
            audio_level: extensions.audio_level,
            codec,
        };
        self.facts = Some(facts);
        Some(facts)
    }

    /// Reuses source codec inspection across every local destination.
    ///
    /// incoming observation fills `PacketFacts` before route planning and local
    /// forwarding run, so codec inspection remains packet-scoped instead of
    /// destination-scoped
    pub(super) fn local_codec_packet(&self) -> Option<&codec::Packet> {
        self.facts.as_ref().map(|facts| &facts.codec)
    }

    fn compute_route_control_rid(&self, state: &PacketLoopState) -> Option<Rid> {
        let extensions = self.route_control_extension_values();
        extensions
            .rid
            .or(extensions.rid_repair)
            .or_else(|| self.route_control_rid_from_ssrc(state))
    }

    pub(super) fn route_control_ssrc(&self) -> Ssrc {
        self.rtp_header().ssrc
    }

    pub(super) fn route_control_mid(&self) -> Option<Mid> {
        self.route_control_extension_values().mid
    }

    pub(super) fn route_control_rid_extension(&self) -> Option<Rid> {
        let extensions = self.route_control_extension_values();
        extensions.rid.or(extensions.rid_repair)
    }

    /// creates a relay-owned view that shares this packet payload
    ///
    /// the caller supplies the resolved source media id because relay targets
    /// must not depend on the target worker being able to rediscover source
    /// identity from local producer registries
    /// cached facts preserve the resolved RID so the receiving worker can reuse
    /// source observations
    pub(super) fn share_for_relay(
        &self,
        state: &PacketLoopState,
        src_media: TransportMediaId,
    ) -> Option<Self> {
        let (header, sequence_number) = match &self.data {
            ForwardedPacketData::Str0mRtp(data) => {
                (data.rtp_packet.header.clone(), data.rtp_packet.seq_no)
            }
            ForwardedPacketData::RelayRtp(data) => (data.header.clone(), data.sequence_number),
        };
        Some(Self {
            source: ForwardedPacketSource::Relayed(self.source.session_key(state)?.clone()),
            src_media: Some(src_media),
            facts: self.facts,
            visits_origin_sinks: false,
            was_repair: self.was_repair,
            received_at: self.received_at,
            payload: Arc::clone(&self.payload),
            data: ForwardedPacketData::RelayRtp(ForwardedRelayRtpData {
                header,
                sequence_number,
            }),
        })
    }

    pub(super) const fn visits_origin_sinks(&self) -> bool {
        self.visits_origin_sinks
    }

    /// resolves the source producer that owns this packet
    ///
    /// the lookup prefers cached facts, then an explicit relay media id, then
    /// source-worker MID and SSRC registries
    /// a successful lookup is cached on the packet so a later registry update
    /// cannot split one packet across different source identities
    pub(super) fn resolve_src_media(
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
        let header = self.rtp_header();
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
    pub(super) fn local_send_packet(&self) -> LocalForwardedRtp<'_> {
        let payload = &self.payload;
        match &self.data {
            ForwardedPacketData::Str0mRtp(rtp_data) => {
                LocalForwardedRtp::new(&rtp_data.rtp_packet, payload, self.was_repair)
            }
            ForwardedPacketData::RelayRtp(rtp_data) => {
                let header = &rtp_data.header;
                LocalForwardedRtp::from_relay(
                    header,
                    rtp_data.sequence_number,
                    self.received_at,
                    payload,
                    self.was_repair,
                )
            }
        }
    }

    fn route_control_extension_values(&self) -> &ExtensionValues {
        &self.rtp_header().ext_vals
    }

    fn rtp_header(&self) -> &RtpHeader {
        match &self.data {
            ForwardedPacketData::Str0mRtp(rtp_data) => &rtp_data.rtp_packet.header,
            ForwardedPacketData::RelayRtp(rtp_data) => &rtp_data.header,
        }
    }

    fn route_control_rid_from_ssrc(&self, state: &PacketLoopState) -> Option<Rid> {
        let src_key = self.source.session_key(state)?;
        state.source_rid_for_ssrc(src_key, self.rtp_header().ssrc)
    }
}

#[cfg(test)]
#[expect(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;
