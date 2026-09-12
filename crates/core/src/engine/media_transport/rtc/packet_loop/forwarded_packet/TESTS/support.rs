use std::{sync::Arc, time::Instant};

use str0m::{
    media::{ExtensionValues, Mid, Pt, Rid},
    rtp::{RtpHeader, Ssrc},
};

use super::{ForwardedPacket, ForwardedPacketSource};
#[cfg(test)]
use crate::engine::media_transport::TransportMediaId;
use crate::engine::media_transport::TransportSessionKey;
#[cfg(any(test, feature = "internal-benchmarks"))]
use crate::engine::media_transport::rtc::state::slots::SessionHandle;

#[must_use]
pub fn sample_forwarded_packet(
    source_session_key: TransportSessionKey,
    mid: &str,
    payload: &[u8],
) -> ForwardedPacket {
    sample_forwarded_packet_with_extensions(
        source_session_key,
        mid,
        None,
        ExtensionValues::default(),
        payload,
    )
}

#[cfg(test)]
#[must_use]
pub fn sample_already_relayed_packet(
    src_key: TransportSessionKey,
    src_media: TransportMediaId,
    mid: &str,
    payload: &[u8],
) -> ForwardedPacket {
    let mut packet = sample_forwarded_packet(src_key, mid, payload);
    packet.src_media = Some(src_media);
    packet.visits_origin_sinks = false;
    packet
}

#[cfg(test)]
#[must_use]
pub fn sample_already_relayed_audio_packet_at(
    src_key: TransportSessionKey,
    src_media: TransportMediaId,
    received_at: Instant,
) -> ForwardedPacket {
    let mut packet =
        sample_forwarded_packet_with_audio_activity(src_key, "aud-up", Some(true), Some(-32), b"");
    packet.src_media = Some(src_media);
    packet.visits_origin_sinks = false;
    packet.received_at = received_at;
    packet
}

#[cfg(test)]
#[must_use]
pub fn sample_local_forwarded_packet(
    source_session_handle: SessionHandle,
    mid: &str,
    payload: &[u8],
) -> ForwardedPacket {
    sample_forwarded_packet_with_source(
        ForwardedPacketSource::Local(source_session_handle),
        mid,
        None,
        ExtensionValues::default(),
        payload,
    )
}

#[cfg(test)]
#[must_use]
pub fn sample_local_repaired_packet(
    source_session_handle: SessionHandle,
    mid: &str,
    sequence_number: u64,
    payload: &[u8],
) -> ForwardedPacket {
    let mut packet = sample_local_forwarded_packet(source_session_handle, mid, payload);
    packet.was_repair = true;
    packet.sequence_number = sequence_number.into();
    packet.header.sequence_number = packet.sequence_number.as_u16();
    packet
}

#[cfg(test)]
#[must_use]
pub fn sample_forwarded_packet_with_rid(
    src_key: TransportSessionKey,
    mid: &str,
    rid: Option<&str>,
    payload: &[u8],
) -> ForwardedPacket {
    sample_forwarded_packet_with_extensions(src_key, mid, rid, ExtensionValues::default(), payload)
}

#[cfg(test)]
#[must_use]
pub fn sample_forwarded_packet_with_audio_activity(
    src_key: TransportSessionKey,
    mid: &str,
    voice_activity: Option<bool>,
    audio_level: Option<i8>,
    payload: &[u8],
) -> ForwardedPacket {
    sample_forwarded_packet_with_extensions(
        src_key,
        mid,
        None,
        ExtensionValues {
            audio_level,
            voice_activity,
            ..ExtensionValues::default()
        },
        payload,
    )
}

#[cfg(feature = "internal-benchmarks")]
#[must_use]
pub fn sample_forwarded_packet_with_rid_and_audio_activity(
    src_key: TransportSessionKey,
    mid: &str,
    rid: Option<&str>,
    voice_activity: Option<bool>,
    audio_level: Option<i8>,
    payload: &[u8],
) -> ForwardedPacket {
    sample_forwarded_packet_with_extensions(
        src_key,
        mid,
        rid,
        ExtensionValues {
            audio_level,
            voice_activity,
            ..ExtensionValues::default()
        },
        payload,
    )
}

/// the RTP identity one benchmark producer stream keeps for its whole run
///
/// a simulcast layer is its own stream on the wire, so each layer carries its own
/// SSRC rather than sharing one with the rest of the camera
#[cfg(feature = "internal-benchmarks")]
#[derive(Debug, Clone, Copy)]
pub struct BenchmarkStreamIdentity {
    pub ssrc: u32,
    pub payload_type: u8,
}

/// the per-packet RTP state a benchmark tick stages onto a reusable packet
#[cfg(feature = "internal-benchmarks")]
#[derive(Debug, Clone, Copy, Default)]
pub struct BenchmarkPacketStaging {
    /// extended source sequence, which must advance by one per staged packet for
    /// the consumer projection to take its in-order path
    pub sequence_number: u64,
    pub rtp_timestamp: u32,
    /// set on the last packet of a frame
    pub marker: bool,
    pub voice_activity: Option<bool>,
    pub audio_level: Option<i8>,
}

#[cfg(feature = "internal-benchmarks")]
#[must_use]
pub fn sample_local_forwarded_packet_for_benchmark(
    source_session_handle: SessionHandle,
    mid: &str,
    rid: Option<&str>,
    identity: BenchmarkStreamIdentity,
    payload: Arc<[u8]>,
) -> ForwardedPacket {
    let received_at = Instant::now();
    ForwardedPacket {
        source: ForwardedPacketSource::Local(source_session_handle),
        src_media: None,
        facts: None,
        visits_origin_sinks: true,
        was_repair: false,
        received_at,
        payload,
        sequence_number: 0_u64.into(),
        header: RtpHeader {
            version: 2,
            has_padding: false,
            has_extension: false,
            csrc_count: 0,
            marker: false,
            payload_type: Pt::from(identity.payload_type),
            sequence_number: 0,
            timestamp: 0,
            ssrc: Ssrc::from(identity.ssrc),
            csrc: [0; 15],
            ext_vals: ExtensionValues {
                rid: rid.map(Rid::from),
                mid: Some(Mid::from(mid)),
                ..ExtensionValues::default()
            },
            header_len: 12,
        },
    }
}

fn sample_forwarded_packet_with_extensions(
    src_key: TransportSessionKey,
    mid: &str,
    rid: Option<&str>,
    ext_vals: ExtensionValues,
    payload: &[u8],
) -> ForwardedPacket {
    sample_forwarded_packet_with_source(
        ForwardedPacketSource::Relayed(src_key),
        mid,
        rid,
        ext_vals,
        payload,
    )
}

fn sample_forwarded_packet_with_source(
    source: ForwardedPacketSource,
    mid: &str,
    rid: Option<&str>,
    ext_vals: ExtensionValues,
    payload: &[u8],
) -> ForwardedPacket {
    let received_at = Instant::now();
    ForwardedPacket {
        source,
        src_media: None,
        facts: None,
        visits_origin_sinks: true,
        was_repair: false,
        received_at,
        payload: Arc::from(payload),
        sequence_number: 1_u64.into(),
        header: RtpHeader {
            version: 2,
            has_padding: false,
            has_extension: false,
            csrc_count: 0,
            marker: false,
            payload_type: Pt::from(111),
            sequence_number: 1,
            timestamp: 1234,
            ssrc: Ssrc::from(4321),
            csrc: [0; 15],
            ext_vals: ExtensionValues {
                rid: rid.map(Rid::from),
                mid: Some(Mid::from(mid)),
                ..ext_vals
            },
            header_len: 12,
        },
    }
}

#[cfg(any(test, feature = "internal-benchmarks"))]
#[must_use]
pub fn sample_forwarded_packet_without_mid(
    src_key: TransportSessionKey,
    ssrc: u32,
    payload: &[u8],
) -> ForwardedPacket {
    let received_at = Instant::now();
    ForwardedPacket {
        source: ForwardedPacketSource::Relayed(src_key),
        src_media: None,
        facts: None,
        visits_origin_sinks: true,
        was_repair: false,
        received_at,
        payload: Arc::from(payload),
        sequence_number: 1_u64.into(),
        header: RtpHeader {
            version: 2,
            has_padding: false,
            has_extension: false,
            csrc_count: 0,
            marker: false,
            payload_type: Pt::from(111),
            sequence_number: 1,
            timestamp: 1234,
            ssrc: Ssrc::from(ssrc),
            csrc: [0; 15],
            ext_vals: ExtensionValues::default(),
            header_len: 12,
        },
    }
}

#[cfg(feature = "internal-benchmarks")]
pub fn reset_packet_resolution(packet: &mut ForwardedPacket) {
    packet.facts = None;
    packet.src_media = None;
}

/// restages one reusable packet as the next packet of its producer stream
///
/// `payload` replaces the codec payload, which is how a video stream walks its
/// prebuilt frames. `None` keeps the payload the packet already carries
#[cfg(feature = "internal-benchmarks")]
pub fn restage_packet_for_benchmark(
    packet: &mut ForwardedPacket,
    staging: BenchmarkPacketStaging,
    payload: Option<&Arc<[u8]>>,
    received_at: Instant,
) {
    reset_packet_resolution(packet);
    packet.received_at = received_at;
    if let Some(payload) = payload {
        packet.payload = Arc::clone(payload);
    }
    packet.sequence_number = staging.sequence_number.into();
    packet.header.sequence_number =
        u16::try_from(staging.sequence_number & u64::from(u16::MAX)).unwrap_or(0);
    packet.header.timestamp = staging.rtp_timestamp;
    packet.header.marker = staging.marker;
    packet.header.ext_vals.audio_level = staging.audio_level;
    packet.header.ext_vals.voice_activity = staging.voice_activity;
}
