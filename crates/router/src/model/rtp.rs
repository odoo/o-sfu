//! Router-native RTP model for the transport edge.
//!
//! This module defines the typed domain model used to describe media streams,
//! codecs, and their negotiated properties. It sits between the signaling/SDP
//! layer and the raw packet loop, allowing the router to reason about media
//! without parsing raw bytes or string-heavy protocol bags.
//!
//! ### RTP Packet Context
//!
//! Most of the types defined here map directly to fields in the "RFC 3550"
//! RTP header or its extensions:
//!
//! ```text
//!  0                   1                   2                   3
//!  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! |V=2|P|X|  CC   |M|     PT      |       Sequence Number         |
//! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! |                           Timestamp                           |
//! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! |           Synchronization Source (SSRC) identifier            |
//! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! ```
//!
//! - `V`: version (always 2)
//! - `P`: padding bit
//! - `X`: extension bit (if set, an extension header follows the SSRC)
//! - `CC`: "CSRC count", the number of contributing source identifiers (0-15) that follow the SSRC
//! - `M`: "Marker" bit, used by profiles to mark significant events like the end of a video frame
//! - `PT`: "Payload Type" (identifies the codec)
//! - `SSRC`: unique stream identifier
//!
//! RFC references for this module:
//! - RTP base protocol: <https://www.rfc-editor.org/rfc/rfc3550>
//! - RTP A/V profile payload assignments: <https://www.rfc-editor.org/rfc/rfc3551>
//! - RTP header extension framework: <https://www.rfc-editor.org/rfc/rfc8285>

use std::borrow::Cow;

use o_sfu_rfc::{rtp as rfc_rtp, webrtc as rfc_webrtc};
pub use rfc_rtp::{HeaderExtensionId, Mid, PayloadType, Rid, Ssrc};

use super::MediaKind;

/// Canonical name for a media codec (e.g. "opus", "vp8", "h264")
pub type MediaCodec = rfc_rtp::CodecName;

/// Uniform resource identifier for a header extension (e.g. "urn:ietf:params:rtp-hdrext:ssrc-audio-level")
pub type HeaderExtensionUri = rfc_webrtc::RtpHeaderExtensionUri;

/// Categories of feedback messages sent over RTCP to control stream behavior.
///
/// These define how the receiver reports issues (like packet loss) or requests
/// changes (like a new keyframe) to the sender.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RtcpFeedbackKind {
    /// Generic NACK from
    /// [RFC 4585 section 6.2.1](https://www.rfc-editor.org/rfc/rfc4585.html#section-6.2.1).
    Nack,
    /// Picture Loss Indication from
    /// [RFC 4585 section 6.3.1](https://www.rfc-editor.org/rfc/rfc4585.html#section-6.3.1).
    NackPli,
    /// Full Intra Request from
    /// [RFC 5104 section 4.3.1](https://www.rfc-editor.org/rfc/rfc5104.html#section-4.3.1).
    CcmFir,
    /// Google-specific receiver estimated maximum bitrate
    GoogRemb,
    /// Transport-wide congestion control (draft-holmer-rmcat-transport-wide-cc-extensions)
    TransportCc,
    /// Any other vendor-specific or experimental feedback type
    Other(String),
}

/// One negotiated RTCP feedback mechanism for a codec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RtcpFeedback {
    kind: RtcpFeedbackKind,
    parameter: Option<String>,
}

impl RtcpFeedback {
    #[must_use]
    pub fn new(kind: RtcpFeedbackKind, parameter: Option<String>) -> Self {
        Self { kind, parameter }
    }

    #[must_use]
    pub fn kind(&self) -> &RtcpFeedbackKind {
        &self.kind
    }

    #[must_use]
    pub fn parameter(&self) -> Option<&str> {
        self.parameter.as_deref()
    }
}

/// Typed codec parameter that affects interoperability.
///
/// These correspond to "a=fmtp" parameters in SDP. Mismatched settings
/// here usually mean the receiver cannot decode the sender's bitstream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecSetting {
    /// RTX associated payload type from
    /// [RFC 4588 section 8.1](https://www.rfc-editor.org/rfc/rfc4588.html#section-8.1).
    RtxAssociation(PayloadType),
    /// h264-specific packetization mode
    H264PacketizationMode(rfc_rtp::h264::PacketizationMode),
    /// H264 profile and level (e.g. "42e01f" for Constrained Baseline Level 3.1)
    H264ProfileLevelId(String),
    /// VP9-specific profile identifier
    Vp9ProfileId(rfc_rtp::Vp9ProfileId),
    /// OPUS-specific flag for in-band forward error correction
    UseInBandFec(bool),
    /// Generic catch-all for unknown or vendor parameters
    Other { key: String, value: String },
}

impl CodecSetting {
    #[must_use]
    pub fn key(&self) -> &str {
        match self {
            Self::RtxAssociation(_) => rfc_rtp::fmtp::RTX_ASSOCIATION,
            Self::H264PacketizationMode(_) => rfc_rtp::fmtp::H264_PACKETIZATION_MODE,
            Self::H264ProfileLevelId(_) => rfc_rtp::fmtp::H264_PROFILE_LEVEL_ID,
            Self::Vp9ProfileId(_) => rfc_rtp::fmtp::VP9_PROFILE_ID,
            Self::UseInBandFec(_) => rfc_rtp::fmtp::OPUS_USE_IN_BAND_FEC,
            Self::Other { key, .. } => key.as_str(),
        }
    }

    #[must_use]
    pub fn wire_value(&self) -> Cow<'_, str> {
        match self {
            Self::RtxAssociation(payload_type) => Cow::Owned(payload_type.value().to_string()),
            Self::H264PacketizationMode(mode) => Cow::Owned(mode.fmtp_value().to_string()),
            Self::H264ProfileLevelId(profile_level_id) => Cow::Borrowed(profile_level_id.as_str()),
            Self::Vp9ProfileId(profile_id) => Cow::Owned(profile_id.value().to_string()),
            Self::UseInBandFec(enabled) => Cow::Borrowed(if *enabled {
                rfc_rtp::fmtp::VALUE_ENABLED
            } else {
                rfc_rtp::fmtp::VALUE_DISABLED
            }),
            Self::Other { value, .. } => Cow::Borrowed(value.as_str()),
        }
    }
}

/// RTP header-extension configuration (RFC 8285).
///
/// Allows carrying extra metadata (like bandwidth estimation hints or
/// audio levels) in a standard way within the RTP packet header.
///
/// ```text
///  0                   1                   2                   3
///  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// |      defined by profile       |           length              |
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// |  ID   |  len  |     data...                                   |
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderExtension {
    uri: HeaderExtensionUri,
    id: HeaderExtensionId,
}

impl HeaderExtension {
    #[must_use]
    pub fn new(uri: impl Into<HeaderExtensionUri>, id: impl Into<HeaderExtensionId>) -> Self {
        Self {
            uri: uri.into(),
            id: id.into(),
        }
    }

    #[must_use]
    pub fn uri_kind(&self) -> &HeaderExtensionUri {
        &self.uri
    }

    #[must_use]
    pub fn id(&self) -> HeaderExtensionId {
        self.id
    }

    #[must_use]
    pub fn uri(&self) -> &str {
        self.uri.as_str()
    }
}

/// Codec capability advertised by a router or endpoint.
///
/// Represents one possible way an endpoint can encode or decode media.
/// The `payload_type` is optional here because capabilities are often
/// just templates (e.g. "i support VP8") before a concrete session pins
/// a specific PT number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaCodecCapability {
    media_kind: MediaKind,
    codec: MediaCodec,
    clock_rate: u32,
    payload_type: Option<PayloadType>,
    channels: Option<u16>,
    settings: Vec<CodecSetting>,
    rtcp_feedback: Vec<RtcpFeedback>,
}

impl MediaCodecCapability {
    #[must_use]
    pub fn new(media_kind: MediaKind, codec: impl Into<MediaCodec>, clock_rate: u32) -> Self {
        Self {
            media_kind,
            codec: codec.into(),
            clock_rate,
            payload_type: None,
            channels: None,
            settings: Vec::new(),
            rtcp_feedback: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_payload_type(mut self, payload_type: PayloadType) -> Self {
        self.payload_type = Some(payload_type);
        self
    }

    #[must_use]
    pub fn with_channels(mut self, channels: u16) -> Self {
        self.channels = Some(channels);
        self
    }

    #[must_use]
    pub fn with_setting(mut self, setting: CodecSetting) -> Self {
        self.settings.push(setting);
        self
    }

    /// An invalid RTX `apt` remains in [`Self::parameters`] while
    /// [`Self::rtx_associated_payload_type`] returns `None`.
    #[must_use]
    pub fn with_parameter(self, name: impl Into<String>, value: impl Into<String>) -> Self {
        let setting = codec_setting_from_wire(&self.codec, name.into(), value.into());
        self.with_setting(setting)
    }

    #[must_use]
    pub fn with_rtcp_feedback(mut self, feedback: RtcpFeedback) -> Self {
        self.rtcp_feedback.push(feedback);
        self
    }

    #[must_use]
    pub fn media_kind(&self) -> MediaKind {
        self.media_kind
    }

    #[must_use]
    pub fn codec(&self) -> &MediaCodec {
        &self.codec
    }

    #[must_use]
    pub fn codec_name(&self) -> &str {
        self.codec.as_str()
    }

    #[must_use]
    pub fn clock_rate(&self) -> u32 {
        self.clock_rate
    }

    #[must_use]
    pub fn payload_type_id(&self) -> Option<PayloadType> {
        self.payload_type
    }

    #[must_use]
    pub fn payload_type(&self) -> Option<u8> {
        self.payload_type.map(PayloadType::value)
    }

    #[must_use]
    pub fn channels(&self) -> Option<u16> {
        self.channels
    }

    pub fn settings(&self) -> impl Iterator<Item = &CodecSetting> {
        self.settings.iter()
    }

    pub fn parameters(&self) -> impl Iterator<Item = (String, String)> + '_ {
        self.settings
            .iter()
            .map(|setting| (setting.key().to_owned(), setting.wire_value().into_owned()))
    }

    pub fn rtcp_feedback(&self) -> impl Iterator<Item = &RtcpFeedback> {
        self.rtcp_feedback.iter()
    }

    #[must_use]
    pub fn rtx_associated_payload_type_id(&self) -> Option<PayloadType> {
        self.settings.iter().find_map(|setting| match setting {
            CodecSetting::RtxAssociation(payload_type) => Some(*payload_type),
            _ => None,
        })
    }

    #[must_use]
    pub fn rtx_associated_payload_type(&self) -> Option<u8> {
        self.rtx_associated_payload_type_id()
            .map(PayloadType::value)
    }
}

/// Full set of codec and extension capabilities for an RTP endpoint.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MediaCapabilities {
    codecs: Vec<MediaCodecCapability>,
    header_extensions: Vec<HeaderExtension>,
}

impl MediaCapabilities {
    #[must_use]
    pub fn new(codecs: Vec<MediaCodecCapability>, header_extensions: Vec<HeaderExtension>) -> Self {
        Self {
            codecs,
            header_extensions,
        }
    }

    pub fn codecs(&self) -> impl Iterator<Item = &MediaCodecCapability> {
        self.codecs.iter()
    }

    pub fn header_extensions(&self) -> impl Iterator<Item = &HeaderExtension> {
        self.header_extensions.iter()
    }
}

/// Negotiated codec format for a concrete media stream.
///
/// Unlike a capability, a format has a fixed `payload_type` that matches the
/// actual value expected in the RTP packets on the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaFormat {
    media_kind: MediaKind,
    codec: MediaCodec,
    payload_type: PayloadType,
    clock_rate: u32,
    channels: Option<u16>,
    settings: Vec<CodecSetting>,
    rtcp_feedback: Vec<RtcpFeedback>,
}

impl MediaFormat {
    #[must_use]
    pub fn new(
        media_kind: MediaKind,
        codec: impl Into<MediaCodec>,
        payload_type: PayloadType,
        clock_rate: u32,
    ) -> Self {
        Self {
            media_kind,
            codec: codec.into(),
            payload_type,
            clock_rate,
            channels: None,
            settings: Vec::new(),
            rtcp_feedback: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_channels(mut self, channels: u16) -> Self {
        self.channels = Some(channels);
        self
    }

    #[must_use]
    pub fn with_setting(mut self, setting: CodecSetting) -> Self {
        self.settings.push(setting);
        self
    }

    #[must_use]
    pub fn with_parameter(self, name: impl Into<String>, value: impl Into<String>) -> Self {
        let setting = codec_setting_from_wire(&self.codec, name.into(), value.into());
        self.with_setting(setting)
    }

    #[must_use]
    pub fn with_rtcp_feedback(mut self, feedback: RtcpFeedback) -> Self {
        self.rtcp_feedback.push(feedback);
        self
    }

    #[must_use]
    pub fn media_kind(&self) -> MediaKind {
        self.media_kind
    }

    #[must_use]
    pub fn codec(&self) -> &MediaCodec {
        &self.codec
    }

    #[must_use]
    pub fn codec_name(&self) -> &str {
        self.codec.as_str()
    }

    #[must_use]
    pub fn payload_type_id(&self) -> PayloadType {
        self.payload_type
    }

    #[must_use]
    pub fn payload_type(&self) -> u8 {
        self.payload_type.value()
    }

    #[must_use]
    pub fn clock_rate(&self) -> u32 {
        self.clock_rate
    }

    #[must_use]
    pub fn channels(&self) -> Option<u16> {
        self.channels
    }

    pub fn settings(&self) -> impl Iterator<Item = &CodecSetting> {
        self.settings.iter()
    }

    pub fn parameters(&self) -> impl Iterator<Item = (String, String)> + '_ {
        self.settings
            .iter()
            .map(|setting| (setting.key().to_owned(), setting.wire_value().into_owned()))
    }

    pub fn rtcp_feedback(&self) -> impl Iterator<Item = &RtcpFeedback> {
        self.rtcp_feedback.iter()
    }

    #[must_use]
    pub fn rtx_associated_payload_type_id(&self) -> Option<PayloadType> {
        self.settings.iter().find_map(|setting| match setting {
            CodecSetting::RtxAssociation(payload_type) => Some(*payload_type),
            _ => None,
        })
    }

    #[must_use]
    pub fn rtx_associated_payload_type(&self) -> Option<u8> {
        self.rtx_associated_payload_type_id()
            .map(PayloadType::value)
    }
}

/// Routing bridge between a negotiated media format and a physical stream.
///
/// Ties logical codec formats to the physical multiplexing identifiers (primary SSRC,
/// repair SSRC, or simulcast RID) resolved from RTP packet headers and SDP.
///
/// ```text
/// Wire RTP Packet Multiplexing:
///   Primary Stream:  [ RTP Header: SSRC 1000 ] ------------+
///                                                          |
///   RTX Repair:      [ RTP Header: SSRC 1001 ] (FID/RRID)  +--> StreamBinding
///                                                          |    - SSRC: 1000
///   Simulcast Layer: [ Header Ext: RID "h"   ] ------------+    - Repair SSRC: 1001
///                                                               - RID: "h"
///                                                               - PayloadType: 96
/// ```
///
/// FID supplies `repair_ssrc`. RID and repaired RID associate packets through `rid`.
///
/// References:
/// - RFC 4588 section 5 (RTX source association): <https://www.rfc-editor.org/rfc/rfc4588.html#section-5>
/// - RFC 5576 section 4.2 (SDP FID grouping): <https://www.rfc-editor.org/rfc/rfc5576.html#section-4.2>
/// - RFC 8852 section 3.3 (RID-based repair): <https://www.rfc-editor.org/rfc/rfc8852.html#section-3.3>
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamBinding {
    ssrc: Option<Ssrc>,
    repair_ssrc: Option<Ssrc>,
    rid: Option<Rid>,
    payload_type: Option<PayloadType>,
    max_bitrate: Option<u64>,
}

impl StreamBinding {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_ssrc(mut self, ssrc: impl Into<Ssrc>) -> Self {
        self.ssrc = Some(ssrc.into());
        self
    }

    /// Associates an RTX source with its primary per
    /// <https://www.rfc-editor.org/rfc/rfc4588.html#section-5> and the FID form
    /// in <https://www.rfc-editor.org/rfc/rfc5576.html#section-4.2>.
    #[must_use]
    pub fn with_repair_ssrc(mut self, repair_ssrc: impl Into<Ssrc>) -> Self {
        self.repair_ssrc = Some(repair_ssrc.into());
        self
    }

    #[must_use]
    pub fn with_rid(mut self, rid: impl Into<Rid>) -> Self {
        self.rid = Some(rid.into());
        self
    }

    #[must_use]
    pub fn with_payload_type(mut self, payload_type: PayloadType) -> Self {
        self.payload_type = Some(payload_type);
        self
    }

    #[must_use]
    pub fn with_max_bitrate(mut self, max_bitrate: u64) -> Self {
        self.max_bitrate = Some(max_bitrate);
        self
    }

    #[must_use]
    pub fn ssrc(&self) -> Option<u32> {
        self.ssrc.map(Ssrc::value)
    }

    #[must_use]
    pub fn repair_ssrc(&self) -> Option<u32> {
        self.repair_ssrc.map(Ssrc::value)
    }

    #[must_use]
    pub fn rid(&self) -> Option<&str> {
        self.rid.as_ref().map(Rid::as_str)
    }

    #[must_use]
    pub(super) fn with_payload_type_mapping(
        mut self,
        payload_types: &[(PayloadType, PayloadType)],
    ) -> Self {
        if let Some(payload_type) = self.payload_type {
            let mapped_payload_type = payload_types
                .iter()
                .find_map(|(original, mapped)| (*original == payload_type).then_some(*mapped))
                .unwrap_or(payload_type);
            self.payload_type = Some(mapped_payload_type);
        }
        self
    }

    #[must_use]
    pub fn payload_type_id(&self) -> Option<PayloadType> {
        self.payload_type
    }

    #[must_use]
    pub fn payload_type(&self) -> Option<u8> {
        self.payload_type.map(PayloadType::value)
    }

    #[must_use]
    pub fn max_bitrate(&self) -> Option<u64> {
        self.max_bitrate
    }
}

/// Description of one logical media stream (e.g. "camera") at the router boundary.
///
/// Combines negotiated formats (codecs), extensions, and the stream bindings
/// that tell the router how to identify the packets on the wire.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MediaStream {
    formats: Vec<MediaFormat>,
    header_extensions: Vec<HeaderExtension>,
    bindings: Vec<StreamBinding>,
    mid: Option<Mid>,
}

impl MediaStream {
    #[must_use]
    pub fn new(
        formats: Vec<MediaFormat>,
        header_extensions: Vec<HeaderExtension>,
        bindings: Vec<StreamBinding>,
    ) -> Self {
        Self {
            formats,
            header_extensions,
            bindings,
            mid: None,
        }
    }

    #[must_use]
    pub fn with_mid(mut self, mid: impl Into<Mid>) -> Self {
        self.mid = Some(mid.into());
        self
    }

    pub fn formats(&self) -> impl Iterator<Item = &MediaFormat> {
        self.formats.iter()
    }

    pub fn header_extensions(&self) -> impl Iterator<Item = &HeaderExtension> {
        self.header_extensions.iter()
    }

    pub fn bindings(&self) -> impl Iterator<Item = &StreamBinding> {
        self.bindings.iter()
    }

    #[must_use]
    pub fn mid(&self) -> Option<&str> {
        self.mid.as_ref().map(Mid::as_str)
    }
}

fn codec_setting_from_wire(codec: &MediaCodec, key: String, value: String) -> CodecSetting {
    match key.as_str() {
        rfc_rtp::fmtp::RTX_ASSOCIATION => value
            .parse::<u8>()
            .ok()
            .and_then(PayloadType::try_new)
            .map_or(
                CodecSetting::Other { key, value },
                CodecSetting::RtxAssociation,
            ),
        rfc_rtp::fmtp::H264_PACKETIZATION_MODE => value
            .parse::<u8>()
            .ok()
            .and_then(rfc_rtp::h264::PacketizationMode::from_fmtp_value)
            .map_or_else(
                || CodecSetting::Other { key, value },
                CodecSetting::H264PacketizationMode,
            ),
        rfc_rtp::fmtp::H264_PROFILE_LEVEL_ID => CodecSetting::H264ProfileLevelId(value),
        rfc_rtp::fmtp::VP9_PROFILE_ID if codec == &MediaCodec::Vp9 => value
            .parse::<u8>()
            .ok()
            .and_then(rfc_rtp::Vp9ProfileId::try_new)
            .map_or(
                CodecSetting::Other { key, value },
                CodecSetting::Vp9ProfileId,
            ),
        rfc_rtp::fmtp::OPUS_USE_IN_BAND_FEC => match value.as_str() {
            rfc_rtp::fmtp::VALUE_ENABLED | rfc_rtp::fmtp::VALUE_TRUE => {
                CodecSetting::UseInBandFec(true)
            }
            rfc_rtp::fmtp::VALUE_DISABLED | rfc_rtp::fmtp::VALUE_FALSE => {
                CodecSetting::UseInBandFec(false)
            }
            _ => CodecSetting::Other { key, value },
        },
        _ => CodecSetting::Other { key, value },
    }
}
