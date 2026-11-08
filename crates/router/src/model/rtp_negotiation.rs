//! RTP capability matching between producers, routers, and consumers.
//!
//! Separates negotiation into two distinct stages to keep producer codecs
//! decoupled from consumer capabilities:
//!
//! 1. **Ingress Normalization**: Producer formats are matched against router capabilities
//!    to produce a standardized `Consumable` stream.
//! 2. **Egress Selection**: Consumer capabilities are intersected with the `Consumable` stream
//!    to determine supported formats, RTCP feedback, and congestion control modes.
//!
//! ```text
//!                   Producer Stream                     Router Capabilities
//!                          \                                   /
//!                           v                                 v
//!                +-------------------------------------------------------+
//!                | Stage 1: Ingress Normalization                        |
//!                | - Match primary codecs & assign router payload types  |
//!                | - Remap RTX `apt` to negotiated primary payload types |
//!                | - Intersect supported header extensions               |
//!                +-------------------------------------------------------+
//!                                            |
//!                                            v
//!                                Consumable MediaStream
//!                                            |
//!                                            +--------------------+
//!                                            |                    |
//!                                            v                    v
//!                                  Consumer 1 Capabilities    Consumer 2 Capabilities
//!                                            |                    |
//!                                            v                    v
//!                              +-----------------------+ +-----------------------+
//!                              | Stage 2: Egress Match | | Stage 2: Egress Match |
//!                              | - Codec intersection  | | - Codec intersection  |
//!                              | - RTCP feedback match | | - RTCP feedback match |
//!                              | - BWE policy (TWCC)   | | - BWE policy (REMB)   |
//!                              +-----------------------+ +-----------------------+
//!                                            |                    |
//!                                            v                    v
//!                                Consumer 1 Egress Stream    Consumer 2 Egress Stream
//! ```
//!
//! This module operates purely on the typed domain model (`MediaStream`, `MediaFormat`,
//! `HeaderExtension`, `StreamBinding`). Rules that depend on raw SDP session structure
//! (m-line ordering, rejected m-sections, BUNDLE extmap consistency) are outside this module
//! and belong at the signaling edge.

use std::collections::HashSet;

use o_sfu_rfc::rtp as rfc_rtp;

#[cfg(any(test, feature = "test-support"))]
use super::diagnostic::{ParseDiagnostic, ParseDiagnosticKind, ParseDiagnosticSpec, RfcReference};
use super::{
    CodecSetting, HeaderExtension, HeaderExtensionUri, MediaCapabilities, MediaCodec,
    MediaCodecCapability, MediaFormat, MediaKind, MediaStream, PayloadType, RtcpFeedback,
    RtcpFeedbackKind,
};

/// Failure raised while deriving or negotiating RTP parameters.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RtpNegotiationError {
    #[error("unsupported producer codec {codec_name} payload {payload_type}")]
    UnsupportedProducerCodec {
        codec_name: String,
        payload_type: u8,
    },
    #[error("invalid apt parameter for codec {codec_name} payload {payload_type}")]
    InvalidAptParameter {
        codec_name: String,
        payload_type: u8,
    },
    #[error("missing media codec for rtx payload {payload_type} apt {associated_payload_type}")]
    MissingAssociatedMediaCodecForRtx {
        payload_type: u8,
        associated_payload_type: u8,
    },
    #[error("no compatible consumer codec")]
    NoCompatibleConsumerCodec,
}

#[cfg(any(test, feature = "test-support"))]
const RFC_3264_SECTION_6: RfcReference = RfcReference::new(
    "RFC 3264",
    "section 6",
    "https://www.rfc-editor.org/rfc/rfc3264.html#section-6",
);
#[cfg(any(test, feature = "test-support"))]
const RFC_4588_SECTION_8_1: RfcReference = RfcReference::new(
    "RFC 4588",
    "section 8.1",
    "https://www.rfc-editor.org/rfc/rfc4588.html#section-8.1",
);

#[cfg(any(test, feature = "test-support"))]
impl ParseDiagnostic for RtpNegotiationError {
    fn diagnostic(&self) -> ParseDiagnosticSpec {
        match self {
            Self::UnsupportedProducerCodec { .. } => ParseDiagnosticSpec::new(
                ParseDiagnosticKind::UnsupportedFeature,
                "producer codec is valid but not supported by router capabilities",
                RFC_3264_SECTION_6,
                "capture producer media stream and router media capabilities and replay derive_consumable_rtp_parameters",
            ),
            Self::InvalidAptParameter { .. } => ParseDiagnosticSpec::new(
                ParseDiagnosticKind::InvalidInput,
                "RTX codec has an invalid or missing apt parameter",
                RFC_4588_SECTION_8_1,
                "capture producer media stream and replay derive_consumable_rtp_parameters to inspect RTX linkage",
            ),
            Self::MissingAssociatedMediaCodecForRtx { .. } => ParseDiagnosticSpec::new(
                ParseDiagnosticKind::InvalidInput,
                "RTX codec references an associated payload type that is not negotiated",
                RFC_4588_SECTION_8_1,
                "capture producer media stream and replay derive_consumable_rtp_parameters to inspect RTX linkage",
            ),
            Self::NoCompatibleConsumerCodec => ParseDiagnosticSpec::new(
                ParseDiagnosticKind::UnsupportedFeature,
                "consumer capabilities have no compatible media codec with the consumable set",
                RFC_3264_SECTION_6,
                "capture consumable media stream and consumer capabilities and replay negotiate_consumer_rtp_parameters",
            ),
        }
    }
}

/// Derives the router-consumable media stream from producer parameters.
///
/// # Two-Pass Codec Resolution
///
/// Retransmission (RTX) formats depend on an associated primary codec via RFC 4588 `apt`.
/// Negotiation therefore executes in two passes:
/// 1. **Pass 1 (Primary Codecs)**: Match primary media formats against router capabilities,
///    assigning router-normalized payload types and building a translation map.
/// 2. **Pass 2 (RTX Codecs)**: Match RTX repair formats, resolve their `apt` references against
///    the translation map, and omit valid formats with no matching router RTX capability.
///
/// ```text
/// Incoming Producer Formats:
///   [ Format 0: RTX (PT 97, apt=96) ] <---+ (references PT 96)
///   [ Format 1: VP8 (PT 96)         ] ----+
///
/// Pass 1: Primary Codec Matching (Skip RTX)
///   Format 1 (VP8, PT 96) matches Router Capability (VP8, PT 100)
///     ==> Consumable Primary: VP8 (PT 100)
///     ==> Translation Map: [ Producer PT 96 -> Router PT 100 ]
///
/// Pass 2: RTX Matching & `apt` Rewriting
///   Format 0 (RTX, PT 97, apt=96):
///     - Lookup `apt=96` in Translation Map ==> matches Router PT 100
///     - Match Router Capability for RTX with apt=100 ==> Router PT 101
///     ==> Consumable Repair: RTX (PT 101, apt=100)
///
/// Final Consumable Stream:
///   [ Primary: VP8 (PT 100) ] + [ Repair: RTX (PT 101, apt=100) ]
/// ```
///
/// The final repair pair is retained only when the primary format also retains Generic NACK.
///
/// # Errors
///
/// Returns [`RtpNegotiationError::UnsupportedProducerCodec`] when a producer media codec does not
/// match any router media codec capability, [`RtpNegotiationError::InvalidAptParameter`] when a
/// RTX codec carries an invalid `apt` parameter, or
/// [`RtpNegotiationError::MissingAssociatedMediaCodecForRtx`] when a RTX codec references a media
/// payload that is not part of the negotiated media codec set.
pub fn derive_consumable_rtp_parameters(
    producer_parameters: &MediaStream,
    capabilities: &MediaCapabilities,
) -> Result<MediaStream, RtpNegotiationError> {
    // Maps the producer's original primary payload type to the router-visible
    // payload type chosen for the consumable stream.
    //
    // This exists for two reasons:
    // - RTX `apt` must be rewritten to point at the negotiated primary PT.
    // - payload-type-bound stream bindings must keep pointing at the negotiated PT,
    //   not the producer's original PT.
    let mut producer_to_router_payload_types = Vec::<(PayloadType, PayloadType)>::new();
    let mut consumable_formats = Vec::new();

    // Primary media codecs are the actual media contract.
    // If a producer format has no router capability match, the router would not be
    // able to describe or forward that media in its consumable model, so we reject
    // the whole producer stream instead of silently dropping the codec.
    for format in producer_parameters.formats() {
        // RFC 4588 section 8.1 binds RTX to an already-negotiated primary PT
        // through `apt`, so media codecs must be matched first.
        // https://www.rfc-editor.org/rfc/rfc4588.html#section-8.1
        if format.codec().is_rtx() {
            continue;
        }
        let Some(capability_format) = find_matching_media_capability(format, capabilities) else {
            // RFC 3264 section 6 only allows formats that both sides support to survive
            // negotiation, so a producer codec with no router capability match is rejected.
            // https://www.rfc-editor.org/rfc/rfc3264.html#section-6
            return Err(RtpNegotiationError::UnsupportedProducerCodec {
                codec_name: format.codec().as_str().to_owned(),
                payload_type: format.payload_type(),
            });
        };
        let negotiated_payload_type = negotiated_payload_type(capability_format, format);
        producer_to_router_payload_types.push((format.payload_type_id(), negotiated_payload_type));
        let feedback =
            intersect_feedback(format.rtcp_feedback(), capability_format.rtcp_feedback());
        consumable_formats.push(format_with_overrides(
            format,
            negotiated_payload_type,
            None,
            &feedback,
        ));
    }

    for format in producer_parameters.formats() {
        if !format.codec().is_rtx() {
            continue;
        }
        let associated_payload_type = parse_rtx_associated_payload(format)?;
        let Some(negotiated_associated_payload_type) = producer_to_router_payload_types
            .iter()
            .find_map(|(original, mapped)| {
                (*original == associated_payload_type).then_some(*mapped)
            })
        else {
            // RFC 4588 section 8.1 makes RTX invalid without a negotiated
            // associated payload type.
            // https://www.rfc-editor.org/rfc/rfc4588.html#section-8.1
            return Err(RtpNegotiationError::MissingAssociatedMediaCodecForRtx {
                payload_type: format.payload_type(),
                associated_payload_type: associated_payload_type.value(),
            });
        };
        let Some(capability_format) =
            find_matching_rtx_capability(format, negotiated_associated_payload_type, capabilities)
        else {
            // Unlike a primary media codec, RTX is an auxiliary retransmission format.
            // If the router does not support this RTX pairing, the media stream can still
            // remain valid without retransmission support, so we drop RTX rather than fail
            // the whole negotiation.
            continue;
        };
        let negotiated_payload_type = negotiated_payload_type(capability_format, format);
        let feedback =
            intersect_feedback(format.rtcp_feedback(), capability_format.rtcp_feedback());
        consumable_formats.push(format_with_overrides(
            format,
            negotiated_payload_type,
            Some(negotiated_associated_payload_type),
            &feedback,
        ));
    }
    normalize_nack_rtx(&mut consumable_formats);

    let header_extensions = capabilities
        .header_extensions()
        .filter(|extension| {
            // RFC 8285 negotiates header extensions by common support. Keep only producer-backed
            // URIs here because the runtime only forwards observed extension values
            // https://www.rfc-editor.org/rfc/rfc8285.html#section-5
            producer_parameters
                .header_extensions()
                .any(|producer_extension| producer_extension.uri_kind() == extension.uri_kind())
        })
        .cloned()
        .collect::<Vec<_>>();
    let bindings = producer_parameters
        .bindings()
        .cloned()
        .map(|binding| binding.with_payload_type_mapping(&producer_to_router_payload_types))
        .collect::<Vec<_>>();

    let mut consumable = MediaStream::new(consumable_formats, header_extensions, bindings);
    if let Some(mid) = producer_parameters.mid() {
        consumable = consumable.with_mid(mid);
    }
    Ok(consumable)
}

/// Negotiates the consumer-facing stream from a consumable stream.
///
/// Algorithm:
/// 1. Intersect header extensions.
/// 2. Derive the BWE feedback policy from surviving extensions.
/// 3. negotiate primary media codecs first
/// 4. Admit RTX only when its primary codec survived
/// 5. Filter bindings so they only reference negotiated payload types.
///
/// The output keeps consumable payload types rather than adopting arbitrary
/// consumer capability PT numbers, because the consumable stream is already the
/// router's negotiated forwarding model
///
/// # Errors
///
/// Returns [`RtpNegotiationError::NoCompatibleConsumerCodec`] when no compatible media codec can
/// be negotiated.
pub fn negotiate_consumer_rtp_parameters(
    consumable_parameters: &MediaStream,
    consumer_capabilities: &MediaCapabilities,
) -> Result<MediaStream, RtpNegotiationError> {
    let negotiated_header_extensions =
        negotiate_header_extensions(consumable_parameters, consumer_capabilities);
    let feedback_policy = bwe_feedback_policy(&negotiated_header_extensions);

    let mut negotiated_formats = consumable_parameters
        .formats()
        .filter_map(|format| {
            // RFC 4588 section 8.1 requires a surviving primary codec before
            // RTX can be admitted, so the first pass negotiates primary formats.
            // https://www.rfc-editor.org/rfc/rfc4588.html#section-8.1
            if format.codec().is_rtx() {
                return None;
            }
            let capability_format = find_matching_media_capability(format, consumer_capabilities)?;
            let feedback =
                intersect_feedback(format.rtcp_feedback(), capability_format.rtcp_feedback());
            let feedback = apply_bwe_feedback_policy(feedback, feedback_policy);
            Some(format_with_overrides(
                format,
                format.payload_type_id(),
                None,
                &feedback,
            ))
        })
        .collect::<Vec<_>>();

    if negotiated_formats.is_empty() {
        // RFC 3264 section 6 requires at least one mutually acceptable media format for an
        // accepted stream, so a consumer with no surviving media codec is incompatible.
        // https://www.rfc-editor.org/rfc/rfc3264.html#section-6
        return Err(RtpNegotiationError::NoCompatibleConsumerCodec);
    }

    for format in consumable_parameters.formats() {
        if !format.codec().is_rtx() {
            continue;
        }
        let Some(capability_format) = find_matching_consumer_rtx_capability(
            format,
            &negotiated_formats,
            consumer_capabilities,
        ) else {
            continue;
        };
        let feedback =
            intersect_feedback(format.rtcp_feedback(), capability_format.rtcp_feedback());
        let feedback = apply_bwe_feedback_policy(feedback, feedback_policy);
        negotiated_formats.push(format_with_overrides(
            format,
            format.payload_type_id(),
            None,
            &feedback,
        ));
    }
    normalize_nack_rtx(&mut negotiated_formats);

    let bindings = consumable_parameters
        .bindings()
        .filter(|binding| {
            binding.payload_type_id().is_none_or(|payload_type| {
                formats_contain_payload_type(&negotiated_formats, payload_type)
            })
        })
        .cloned()
        .collect::<Vec<_>>();

    let mut negotiated =
        MediaStream::new(negotiated_formats, negotiated_header_extensions, bindings);
    if let Some(mid) = consumable_parameters.mid() {
        negotiated = negotiated.with_mid(mid);
    }
    Ok(negotiated)
}

/// Payload-type policy:
///
/// - if the capability pins an explicit PT, that PT becomes authoritative in the
///   negotiated stream
/// - otherwise we preserve the source PT
///
/// This keeps PT assignment under capability control without forcing every
/// capability entry to hardcode payload types.
fn negotiated_payload_type(
    capability_format: &MediaCodecCapability,
    format: &MediaFormat,
) -> PayloadType {
    capability_format
        .payload_type_id()
        .unwrap_or(format.payload_type_id())
}

/// Media capability matching ignores payload type.
/// PT is negotiated output state, not an identity key for codec compatibility.
///
/// Compatibility is instead based on the codec's media kind, codec name,
/// clock rate, normalized channel count, and codec-specific critical fmtp
/// parameters.
fn find_matching_media_capability<'a>(
    format: &MediaFormat,
    capabilities: &'a MediaCapabilities,
) -> Option<&'a MediaCodecCapability> {
    capabilities.codecs().find(|capability_format| {
        !capability_format.codec().is_rtx()
            && codec_match_ignoring_payload_type(format, capability_format)
    })
}

/// RTX matching has one extra constraint beyond ordinary codec matching:
///
/// the router RTX capability must be associated with the already negotiated
/// primary payload type, because RFC 4588 binds RTX to a specific primary PT
/// through `apt`.
/// <https://www.rfc-editor.org/rfc/rfc4588.html#section-8.1>
fn find_matching_rtx_capability<'a>(
    format: &MediaFormat,
    negotiated_associated_payload_type: PayloadType,
    capabilities: &'a MediaCapabilities,
) -> Option<&'a MediaCodecCapability> {
    capabilities.codecs().find(|capability_format| {
        capability_format.codec().is_rtx()
            && codec_match_ignoring_payload_type(format, capability_format)
            && capability_format.rtx_associated_payload_type_id()
                == Some(negotiated_associated_payload_type)
    })
}

/// Consumer-side RTX matching works against the already-consumable stream.
/// At this point the stream's `apt` must refer to a primary PT that survived
/// consumer negotiation, otherwise forwarding RTX would create an orphan repair
/// stream with no valid primary target
fn find_matching_consumer_rtx_capability<'a>(
    format: &MediaFormat,
    negotiated_formats: &[MediaFormat],
    capabilities: &'a MediaCapabilities,
) -> Option<&'a MediaCodecCapability> {
    let associated_payload_type = parse_rtx_associated_payload(format).ok()?;
    // RFC 4588 section 8.1 ties each RTX format to one negotiated primary PT.
    // https://www.rfc-editor.org/rfc/rfc4588.html#section-8.1
    if !formats_contain_primary_payload_type(negotiated_formats, associated_payload_type) {
        return None;
    }
    capabilities.codecs().find(|capability_format| {
        capability_format.codec().is_rtx()
            && codec_match_ignoring_payload_type(format, capability_format)
            && capability_format.rtx_associated_payload_type_id() == Some(associated_payload_type)
    })
}

/// Returns whether two formats describe the same codec configuration, ignoring PT
///
/// In RTP/SDP, payload type is only a local number bound to a codec description;
/// it is not itself the codec identity. Compatibility therefore comes from the
/// semantic codec fields and any fmtp parameters that change the wire format
fn codec_match_ignoring_payload_type(
    format: &MediaFormat,
    capability_format: &MediaCodecCapability,
) -> bool {
    if format.media_kind() != capability_format.media_kind()
        || format.codec() != capability_format.codec()
        || format.clock_rate() != capability_format.clock_rate()
    {
        return false;
    }
    if normalized_channels(format.media_kind(), format.channels())
        != normalized_channels(capability_format.media_kind(), capability_format.channels())
    {
        return false;
    }
    critical_codec_settings_match(format, capability_format)
}

/// Only settings that actually affect wire compatibility are treated as hard
/// negotiation keys here.
///
/// Receiver preferences or advisory parameters are not all compatibility
/// blockers because this module is trying to answer "can the
/// formats interoperate?" rather than "are all local preferences identical?"
fn critical_codec_settings_match(
    format: &MediaFormat,
    capability_format: &MediaCodecCapability,
) -> bool {
    match format.codec() {
        MediaCodec::H264 => h264_critical_settings_match(format, capability_format),
        MediaCodec::Vp9 => vp9_critical_settings_match(format, capability_format),
        _ => true,
    }
}

/// `packetization-mode` is a hard compatibility key.
/// Different packetization modes describe different RTP packetization behaviour,
/// so mismatches are not just preferences
fn h264_critical_settings_match(
    format: &MediaFormat,
    capability_format: &MediaCodecCapability,
) -> bool {
    let Some(format_packetization_mode) = h264_packetization_mode(format.settings()) else {
        return false;
    };
    let Some(capability_packetization_mode) = h264_packetization_mode(capability_format.settings())
    else {
        return false;
    };
    // RFC 6184 section 8.2.2 requires packetization-mode compatibility, mismatched packetization
    // modes describe different wire behaviors and are therefore rejected.
    // https://www.rfc-editor.org/rfc/rfc6184.html#section-8.2.2
    if format_packetization_mode != capability_packetization_mode {
        return false;
    }
    let format_profile_level_id = format
        .settings()
        .find_map(|setting| match setting {
            CodecSetting::H264ProfileLevelId(profile_level_id) => Some(profile_level_id.as_str()),
            _ => None,
        })
        .unwrap_or(rfc_rtp::fmtp::H264_DEFAULT_PROFILE_LEVEL_ID);
    let capability_profile_level_id = capability_format
        .settings()
        .find_map(|setting| match setting {
            CodecSetting::H264ProfileLevelId(profile_level_id) => Some(profile_level_id.as_str()),
            _ => None,
        })
        .unwrap_or(rfc_rtp::fmtp::H264_DEFAULT_PROFILE_LEVEL_ID);
    let Some(parsed_format_profile_level_id) =
        rfc_rtp::h264::ProfileLevelId::parse(format_profile_level_id)
    else {
        return false;
    };
    let Some(parsed_capability_profile_level_id) =
        rfc_rtp::h264::ProfileLevelId::parse(capability_profile_level_id)
    else {
        return false;
    };
    parsed_format_profile_level_id.profile() == parsed_capability_profile_level_id.profile()
        && parsed_format_profile_level_id.level() <= parsed_capability_profile_level_id.level()
}

fn h264_packetization_mode<'a>(
    settings: impl Iterator<Item = &'a CodecSetting>,
) -> Option<rfc_rtp::h264::PacketizationMode> {
    for setting in settings {
        match setting {
            CodecSetting::H264PacketizationMode(mode) => return Some(*mode),
            CodecSetting::Other { key, .. } if key == rfc_rtp::fmtp::H264_PACKETIZATION_MODE => {
                return None;
            }
            _ => {}
        }
    }
    rfc_rtp::h264::PacketizationMode::from_fmtp_value(
        rfc_rtp::fmtp::H264_DEFAULT_PACKETIZATION_MODE,
    )
}

fn vp9_critical_settings_match(
    format: &MediaFormat,
    capability_format: &MediaCodecCapability,
) -> bool {
    effective_vp9_profile_id(format.settings())
        .zip(effective_vp9_profile_id(capability_format.settings()))
        .is_some_and(|(format_profile, capability_profile)| format_profile == capability_profile)
}

fn effective_vp9_profile_id<'a>(
    settings: impl Iterator<Item = &'a CodecSetting>,
) -> Option<rfc_rtp::Vp9ProfileId> {
    let mut profile_id = None;
    for setting in settings {
        match setting {
            CodecSetting::Vp9ProfileId(value) => profile_id = Some(*value),
            CodecSetting::Other { key, .. } if key == rfc_rtp::fmtp::VP9_PROFILE_ID => return None,
            _ => {}
        }
    }
    Some(profile_id.unwrap_or(rfc_rtp::fmtp::VP9_DEFAULT_PROFILE_ID))
}

/// `apt` is not optional metadata for RTX.
/// It is the linkage that says which primary payload type this repair stream
/// protects. Without it, the RTX format is structurally invalid for negotiation.
fn parse_rtx_associated_payload(format: &MediaFormat) -> Result<PayloadType, RtpNegotiationError> {
    format.rtx_associated_payload_type_id().ok_or_else(|| {
        RtpNegotiationError::InvalidAptParameter {
            codec_name: format.codec().as_str().to_owned(),
            payload_type: format.payload_type(),
        }
    })
}

/// Rebuild the format from the source while applying negotiated overrides.
///
/// We copy all original codec settings except RTX `apt`, because `apt` may need
/// to be rewritten after payload-type remapping. RTCP feedback is also rebuilt
/// from the negotiated intersection rather than just copied, so the result
/// only advertises mutually supported feedback mechanisms.
fn format_with_overrides(
    source: &MediaFormat,
    payload_type: PayloadType,
    apt_override: Option<PayloadType>,
    feedback: &[RtcpFeedback],
) -> MediaFormat {
    let mut format = MediaFormat::new(
        source.media_kind(),
        source.codec().clone(),
        payload_type,
        source.clock_rate(),
    );
    if let Some(channels) = source.channels() {
        format = format.with_channels(channels);
    }
    for setting in source
        .settings()
        .filter(|setting| !matches!(setting, CodecSetting::RtxAssociation(_)))
        .cloned()
    {
        format = format.with_setting(setting);
    }
    if let Some(apt) = apt_override {
        format = format.with_setting(CodecSetting::RtxAssociation(apt));
    } else if let Some(apt) = source.rtx_associated_payload_type_id() {
        format = format.with_setting(CodecSetting::RtxAssociation(apt));
    }
    for entry in feedback {
        format = format.with_rtcp_feedback(entry.clone());
    }
    format
}

/// RTCP feedback is negotiated by common support, not by union.
/// Advertising feedback that only one side supports would let later code assume
/// a control signal is usable when the peer never negotiated it.
/// <https://www.rfc-editor.org/rfc/rfc4585.html#section-4.2>
fn intersect_feedback<'a>(
    format_feedback: impl Iterator<Item = &'a RtcpFeedback>,
    capability_feedback: impl Iterator<Item = &'a RtcpFeedback>,
) -> Vec<RtcpFeedback> {
    let capability_feedback = capability_feedback.cloned().collect::<Vec<_>>();
    format_feedback
        .filter(|feedback| capability_feedback.contains(feedback))
        .cloned()
        .collect()
}

fn normalize_nack_rtx(formats: &mut Vec<MediaFormat>) {
    // RFC 4585 negotiates Generic NACK per format and RFC 4588 links RTX through
    // `apt`. O-SFU policy exposes repair only as a complete pair. It keeps the
    // first matching RTX, drops duplicates and orphans then removes Generic
    // NACK when no repair remains.
    // https://www.rfc-editor.org/rfc/rfc4585.html#section-4.2
    // https://www.rfc-editor.org/rfc/rfc4588.html#section-8.1
    let nack_payload_types = formats
        .iter()
        .filter(|format| !format.codec().is_rtx() && has_generic_nack(format))
        .map(MediaFormat::payload_type_id)
        .collect::<HashSet<_>>();
    let mut paired_payload_types = HashSet::new();
    formats.retain(|format| {
        !format.codec().is_rtx()
            || format.rtx_associated_payload_type_id().is_some_and(|apt| {
                nack_payload_types.contains(&apt) && paired_payload_types.insert(apt)
            })
    });

    for format in formats.iter_mut() {
        if format.codec().is_rtx()
            || !has_generic_nack(format)
            || paired_payload_types.contains(&format.payload_type_id())
        {
            continue;
        }
        let feedback = format
            .rtcp_feedback()
            .filter(|feedback| !matches!(feedback.kind(), RtcpFeedbackKind::Nack))
            .cloned()
            .collect::<Vec<_>>();
        *format = format_with_overrides(format, format.payload_type_id(), None, &feedback);
    }
}

fn has_generic_nack(format: &MediaFormat) -> bool {
    format
        .rtcp_feedback()
        .any(|feedback| matches!(feedback.kind(), RtcpFeedbackKind::Nack))
}

/// Channel count is only part of codec identity for audio.
/// Video formats do not use RTP channel count semantics in this model, so we
/// normalize all video channel counts away to avoid spurious mismatches.
fn normalized_channels(media_kind: MediaKind, channels: Option<u16>) -> Option<u16> {
    if media_kind == MediaKind::Audio {
        Some(channels.unwrap_or(1))
    } else {
        None
    }
}

/// This is a URI-level capability intersection only.
///
/// It does NOT perform full SDP extmap negotiation:
/// - no direction filtering
/// - no id collision checks
/// - no BUNDLE-wide extmap consistency checks
///
/// Those rules belong to the SDP/signaling edge. This helper only answers
/// whether the typed RTP model should keep an extension URI at all.
fn negotiate_header_extensions(
    consumable_parameters: &MediaStream,
    consumer_capabilities: &MediaCapabilities,
) -> Vec<HeaderExtension> {
    consumable_parameters
        .header_extensions()
        // RFC 8285 extmaps are negotiated by common support. This helper keeps the
        // router model at URI-intersection scope and leaves direction/id validation to the SDP edge.
        // https://www.rfc-editor.org/rfc/rfc8285.html#section-5
        .filter(|extension| {
            consumer_capabilities
                .header_extensions()
                .any(|supported| supported.uri_kind() == extension.uri_kind())
        })
        .cloned()
        .collect()
}

fn formats_contain_payload_type(formats: &[MediaFormat], payload_type: PayloadType) -> bool {
    formats
        .iter()
        .any(|format| format.payload_type_id() == payload_type)
}

fn formats_contain_primary_payload_type(
    formats: &[MediaFormat],
    payload_type: PayloadType,
) -> bool {
    formats
        .iter()
        .any(|format| !format.codec().is_rtx() && format.payload_type_id() == payload_type)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BweFeedbackPolicy {
    PreferTransportCc,
    PreferGoogRemb,
    DisableBoth,
}

/// Selects a local forwarding policy for mutually exclusive bandwidth-estimation
/// feedback families.
///
/// Resolves conflicting congestion control feedback modes by removing feedback only:
/// - if transport-wide CC is available, prefer `transport-cc` and strip `goog-remb`
/// - otherwise, if abs-send-time is available, prefer `goog-remb` and strip `transport-cc`
/// - otherwise strip both feedback families
///
/// ```text
///                Negotiated Header Extensions
///                             |
///            +----------------+----------------+
///            |                                 |
///   Contains TWCC extension?          Otherwise abs-send-time?
///            |                                 |
///            v (yes)                           v (yes)
///  [ PreferTransportCc ]              [ PreferGoogRemb ]
///            |                                 |
///            v                                 v
///  - Keep `transport-cc` feedback     - Keep `goog-remb` feedback
///  - Strip conflicting `goog-remb`    - Strip `transport-cc`
/// ```
///
/// If neither extension is present, [`BweFeedbackPolicy::DisableBoth`] strips both families.
fn bwe_feedback_policy(header_extensions: &[HeaderExtension]) -> BweFeedbackPolicy {
    if header_extensions.iter().any(|extension| {
        matches!(
            extension.uri_kind(),
            HeaderExtensionUri::TransportWideCcDraft01
        )
    }) {
        return BweFeedbackPolicy::PreferTransportCc;
    }
    if header_extensions
        .iter()
        .any(|extension| matches!(extension.uri_kind(), HeaderExtensionUri::AbsSendTime))
    {
        return BweFeedbackPolicy::PreferGoogRemb;
    }
    BweFeedbackPolicy::DisableBoth
}

/// Negotiation may leave both transport-cc and goog-remb present in the raw
/// RTCP feedback intersection. We filter here so downstream sender logic sees one
/// coherent bandwidth-estimation mode instead of multiple competing ones.
fn apply_bwe_feedback_policy(
    feedback: Vec<RtcpFeedback>,
    policy: BweFeedbackPolicy,
) -> Vec<RtcpFeedback> {
    feedback
        .into_iter()
        .filter(|entry| match policy {
            BweFeedbackPolicy::PreferTransportCc => {
                !matches!(entry.kind(), RtcpFeedbackKind::GoogRemb)
            }
            BweFeedbackPolicy::PreferGoogRemb => {
                !matches!(entry.kind(), RtcpFeedbackKind::TransportCc)
            }
            BweFeedbackPolicy::DisableBoth => !matches!(
                entry.kind(),
                RtcpFeedbackKind::TransportCc | RtcpFeedbackKind::GoogRemb
            ),
        })
        .collect()
}
