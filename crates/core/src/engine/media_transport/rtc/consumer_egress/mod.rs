//! Receiver RTP projection and repair bookkeeping for local RTC destinations.
//!
//! [`RtcSessionState::send_consumer_packet`](super::state::RtcSessionState::send_consumer_packet)
//! pairs the session's [`Rtc`] with its [`ConsumerStreamStore`]. Each consumer
//! keeps its receiver RTP and codec projection across source switches and
//! delivery generations.
//!
//! ```text
//! resolved route + source RTP
//!   -> receiver projection -> StreamTx queue
//!                                  |
//!                            str0m Transmit
//!                                  |
//!                         RTX write accounting
//! ```
//!
//! Queuing a repairable write does not arm its cache deadline. Transmit output
//! accounts for queued writes and arms expiry for current delivery. Invalidation
//! and route release retain outstanding-write accounting so late output clears
//! stale cache entries. Session operations pair these transitions with `Rtc`
//! cache rotation and stream retirement.

use std::{sync::Arc, time::Instant};

use str0m::{
    Rtc,
    media::{ExtensionValues, Mid, Pt},
    rtp::{RtpHeader, RtpPacket, RtpWrite, SeqNo, StreamTx},
};
use tracing::debug;

mod streams;
#[cfg(test)]
#[path = "TESTS/support.rs"]
pub(super) mod test_support;
#[cfg(feature = "internal-benchmarks")]
pub use self::streams::LocalRewriteBenchFixture;
pub(super) use self::streams::{ConsumerStreamStore, RTX_CACHE_MAX_PACKETS, rotate_rtx_cache};
use self::streams::{ProjectedIdentity, SourceRtpIdentity, SourceTransition};
use super::{codec, state::slots::ConsumerStreamHandle};
use crate::engine::media_transport::TransportMediaId;

/// receiver route snapshot used for one local RTP write
///
/// values come from `MediaRouteDestination` after route lookup in the flush turn
#[derive(Debug)]
pub(super) struct LocalPacketDestination {
    transport_media_id: TransportMediaId,
    stream_handle: ConsumerStreamHandle,
    delivery_generation: u64,
    mid: Mid,
    payload_type: Option<Pt>,
    repair_enabled: bool,
}

/// borrowed RTP metadata plus shared payload bytes for local egress
pub(super) struct LocalForwardedRtp<'a> {
    data: LocalForwardedRtpData<'a>,
    payload: &'a Arc<[u8]>,
    was_repair: bool,
}

enum LocalForwardedRtpData<'a> {
    Str0m(&'a RtpPacket),
    Relay {
        header: &'a RtpHeader,
        sequence_number: SeqNo,
        timestamp: Instant,
    },
}

impl<'a> LocalForwardedRtp<'a> {
    pub(super) fn new(rtp_packet: &'a RtpPacket, payload: &'a Arc<[u8]>, was_repair: bool) -> Self {
        Self {
            data: LocalForwardedRtpData::Str0m(rtp_packet),
            payload,
            was_repair,
        }
    }

    pub(super) fn from_relay(
        header: &'a RtpHeader,
        sequence_number: SeqNo,
        timestamp: Instant,
        payload: &'a Arc<[u8]>,
        was_repair: bool,
    ) -> Self {
        Self {
            data: LocalForwardedRtpData::Relay {
                header,
                sequence_number,
                timestamp,
            },
            payload,
            was_repair,
        }
    }

    fn header(&self) -> &RtpHeader {
        match self.data {
            LocalForwardedRtpData::Str0m(packet) => &packet.header,
            LocalForwardedRtpData::Relay { header, .. } => header,
        }
    }

    fn timestamp(&self) -> Instant {
        match self.data {
            LocalForwardedRtpData::Str0m(packet) => packet.timestamp,
            LocalForwardedRtpData::Relay { timestamp, .. } => timestamp,
        }
    }

    fn sequence_number(&self) -> SeqNo {
        match self.data {
            LocalForwardedRtpData::Str0m(packet) => packet.seq_no,
            LocalForwardedRtpData::Relay {
                sequence_number, ..
            } => sequence_number,
        }
    }
}

impl LocalPacketDestination {
    pub(super) fn new(
        transport_media_id: TransportMediaId,
        stream_handle: ConsumerStreamHandle,
        delivery_generation: u64,
        mid: Mid,
        payload_type: Option<Pt>,
        repair_enabled: bool,
    ) -> Self {
        Self {
            transport_media_id,
            stream_handle,
            delivery_generation,
            mid,
            payload_type,
            repair_enabled,
        }
    }

    /// Queues one packet in the destination `Rtc`.
    ///
    /// Returns `None` when no destination [`StreamTx`] exists or RTP identity
    /// projection rejects a stale handle, older delivery generation or source
    /// sequence outside its projection range.
    pub(super) fn send(
        &self,
        consumer_streams: &mut ConsumerStreamStore,
        rtc: &mut Rtc,
        rtp: &LocalForwardedRtp<'_>,
        codec_packet: Option<&codec::Packet>,
    ) -> Option<usize> {
        let codec_identity =
            codec_packet.map_or_else(codec::PacketIdentity::default, codec::Packet::identity);
        let header = rtp.header();
        let (identity, repairable_primary_ssrc) = {
            let mut direct_api = rtc.direct_api();
            let stream_tx = direct_api.stream_tx_by_mid(self.mid, None)?;
            let identity = consumer_streams.project_identity(
                self.stream_handle,
                SourceRtpIdentity {
                    delivery_generation: self.delivery_generation,
                    ssrc: header.ssrc,
                    seq_no: rtp.sequence_number(),
                    timestamp: header.timestamp,
                    was_repair: rtp.was_repair,
                },
                codec_identity,
            )?;
            let repairable_primary_ssrc =
                (self.repair_enabled && stream_tx.rtx().is_some()).then(|| stream_tx.ssrc());
            if let SourceTransition::Switched { previous_ssrc } = identity.transition {
                debug!(
                    ?self.transport_media_id,
                    mid = ?self.mid,
                    previous_source_ssrc = ?previous_ssrc,
                    source_ssrc = ?header.ssrc,
                    source_rtp_timestamp = header.timestamp,
                    outbound_rtp_timestamp = identity.rtp_timestamp,
                    source_codec_identity = ?codec_identity,
                    outbound_codec_identity = ?identity.codec,
                    "projected consumer RTP identity after producer SSRC switch"
                );
            }
            self.write_rtp(
                stream_tx,
                rtp,
                identity,
                codec_packet,
                repairable_primary_ssrc.is_some(),
            );
            (identity, repairable_primary_ssrc)
        };
        #[cfg(test)]
        {
            consumer_streams.last_local_write = Some((identity.seq_no, Arc::clone(rtp.payload)));
        }
        let Some(primary_ssrc) = repairable_primary_ssrc else {
            return Some(rtp.payload.len());
        };
        if identity.resets_rtx_cache {
            // str0m caches this queued write only when it is emitted. Rotate the
            // old cache before tracking the write in the new delivery epoch.
            if let Some(primary_ssrc) = consumer_streams.invalidate_rtx_stream(self.stream_handle) {
                streams::rotate_rtx_cache(rtc, primary_ssrc);
            }
        }
        consumer_streams.queue_repairable_write(self.stream_handle, primary_ssrc);
        Some(rtp.payload.len())
    }

    fn write_rtp(
        &self,
        stream_tx: &mut StreamTx,
        rtp: &LocalForwardedRtp<'_>,
        identity: ProjectedIdentity,
        codec_packet: Option<&codec::Packet>,
        nackable: bool,
    ) {
        let header = rtp.header();
        let payload_type = outbound_payload_type(header, self.payload_type);
        let ext_vals = outbound_extension_values(header, self.mid);
        // Share payload storage because copying bytes would multiply packet work
        // by destination fanout.
        let payload = Arc::clone(rtp.payload);
        let mut write = RtpWrite::new(
            payload_type,
            identity.seq_no,
            identity.rtp_timestamp,
            rtp.timestamp(),
            payload,
        )
        .marker(header.marker)
        .ext_vals(ext_vals);
        if let Some(csrc) = header.csrc.get(..header.csrc_count) {
            write = write.csrc(csrc);
        }
        if let Some(rewrite) = codec_packet.and_then(|packet| packet.rewrite(identity.codec)) {
            write = rewrite.apply(write);
        }
        if nackable {
            write = write.nackable(true);
        }
        stream_tx.write_rtp(write);
    }
}

fn outbound_payload_type(header: &RtpHeader, payload_type: Option<Pt>) -> Pt {
    payload_type.unwrap_or(header.payload_type)
}

fn outbound_extension_values(header: &RtpHeader, mid: Mid) -> ExtensionValues {
    let mut ext_vals = header.ext_vals.clone();
    // RFC 9143 section 15 binds MID to the consumer media section. RFC 8852
    // section 3 scopes RID and repaired RID by source and MID, so publisher
    // encoding identifiers must not leak onto the consumer stream.
    // https://www.rfc-editor.org/rfc/rfc9143.html#section-15
    // https://www.rfc-editor.org/rfc/rfc8852.html#section-3
    ext_vals.mid = Some(mid);
    ext_vals.rid = None;
    ext_vals.rid_repair = None;
    ext_vals
}

#[cfg(test)]
#[path = "TESTS/sending.rs"]
mod tests;
