use o_sfu_rfc::{
    rtp::{
        self as rfc_rtp, CodecName, RTP_RTCP_MUX_FORBIDDEN_PAYLOAD_TYPE_START,
        RTP_VIDEO_CLOCK_RATE_HZ,
    },
    webrtc,
};

use crate::{
    MediaKind,
    negotiation::{
        self, ParseDiagnostic, ParseDiagnosticKind, RtpNegotiationError,
        derive_consumable_rtp_parameters, negotiate_consumer_rtp_parameters,
    },
    rtp::{
        HeaderExtension, HeaderExtensionId, MediaCapabilities, MediaCodecCapability, MediaFormat,
        MediaStream, PayloadType, Rid, RtcpFeedback, RtcpFeedbackKind, StreamBinding,
    },
};

fn negotiates(stream: &MediaStream, capabilities: &MediaCapabilities) -> bool {
    negotiation::negotiate_consumer_rtp_parameters(stream, capabilities).is_ok()
}

fn pt(value: u8) -> PayloadType {
    PayloadType::new(value)
}

fn video_capability(codec: CodecName) -> MediaCodecCapability {
    MediaCodecCapability::new(MediaKind::Video, codec, RTP_VIDEO_CLOCK_RATE_HZ)
}

fn video_format(codec: CodecName, payload_type: u8) -> MediaFormat {
    MediaFormat::new(
        MediaKind::Video,
        codec,
        pt(payload_type),
        RTP_VIDEO_CLOCK_RATE_HZ,
    )
}

fn assert_repair_disabled(stream: &MediaStream) {
    let formats = stream.formats().collect::<Vec<_>>();
    assert_eq!(formats.len(), 1);
    let Some(format) = formats.first() else {
        return;
    };
    let feedback = format
        .rtcp_feedback()
        .map(RtcpFeedback::kind)
        .collect::<Vec<_>>();
    assert!(!feedback.contains(&&RtcpFeedbackKind::Nack));
    assert!(feedback.contains(&&RtcpFeedbackKind::NackPli));
    assert!(feedback.contains(&&RtcpFeedbackKind::CcmFir));
}

#[test]
fn rtp_identifiers_validate_rfc_ranges() {
    assert_eq!(
        HeaderExtensionId::try_new(1).map(HeaderExtensionId::value),
        Some(1)
    );
    assert_eq!(
        HeaderExtensionId::try_new(14).map(HeaderExtensionId::value),
        Some(14)
    );
    assert_eq!(HeaderExtensionId::try_new(0), None);
    assert_eq!(HeaderExtensionId::try_new(15), None);

    assert_eq!(Rid::try_new("hi").as_ref().map(Rid::as_str), Some("hi"));
    assert_eq!(Rid::try_new(""), None);
    assert_eq!(Rid::try_new("bad-rid"), None);
}

#[test]
fn invalid_rtx_apt_does_not_become_a_payload_type() {
    let capability = video_capability(CodecName::Rtx).with_parameter(
        rfc_rtp::fmtp::RTX_ASSOCIATION,
        RTP_RTCP_MUX_FORBIDDEN_PAYLOAD_TYPE_START.to_string(),
    );

    assert_eq!(capability.rtx_associated_payload_type(), None);
    assert_eq!(
        capability.parameters().collect::<Vec<_>>(),
        vec![(
            rfc_rtp::fmtp::RTX_ASSOCIATION.to_owned(),
            RTP_RTCP_MUX_FORBIDDEN_PAYLOAD_TYPE_START.to_string(),
        )]
    );
}

#[test]
fn derive_consumable_parameters_maps_rtx_and_preserves_repair_ssrc() {
    let capabilities = MediaCapabilities::new(
        vec![
            video_capability(CodecName::H264)
                .with_payload_type(pt(101))
                .with_parameter(rfc_rtp::fmtp::H264_PACKETIZATION_MODE, "1")
                .with_parameter(rfc_rtp::fmtp::H264_PROFILE_LEVEL_ID, "4d0032")
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::Nack, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::TransportCc, None)),
            video_capability(CodecName::Rtx)
                .with_payload_type(pt(102))
                .with_parameter(rfc_rtp::fmtp::RTX_ASSOCIATION, "101"),
        ],
        vec![
            HeaderExtension::new(webrtc::rtp_header_extension_uri::MID, 1),
            HeaderExtension::new(
                webrtc::rtp_header_extension_uri::TRANSPORT_WIDE_CC_DRAFT_01,
                5,
            ),
        ],
    );
    let producer_parameters = MediaStream::new(
        vec![
            video_format(CodecName::H264, 111)
                .with_parameter(rfc_rtp::fmtp::H264_PACKETIZATION_MODE, "1")
                .with_parameter(rfc_rtp::fmtp::H264_PROFILE_LEVEL_ID, "4d0032")
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::Nack, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::GoogRemb, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::TransportCc, None)),
            video_format(CodecName::Rtx, 112).with_parameter(rfc_rtp::fmtp::RTX_ASSOCIATION, "111"),
        ],
        vec![
            HeaderExtension::new(webrtc::rtp_header_extension_uri::MID, 1),
            HeaderExtension::new(webrtc::rtp_header_extension_uri::ABS_SEND_TIME, 4),
        ],
        vec![
            StreamBinding::new()
                .with_rid("f")
                .with_ssrc(1234)
                .with_repair_ssrc(1235)
                .with_payload_type(pt(111)),
        ],
    )
    .with_mid("video-0");

    let consumable_result = derive_consumable_rtp_parameters(&producer_parameters, &capabilities);
    assert!(consumable_result.is_ok());
    let Ok(consumable) = consumable_result else {
        return;
    };
    let formats = consumable.formats().collect::<Vec<_>>();
    assert_eq!(formats.len(), 2);
    let Some(media_codec) = formats.first() else {
        return;
    };
    assert_eq!(media_codec.payload_type(), 101);
    assert!(
        media_codec
            .rtcp_feedback()
            .any(|feedback| matches!(feedback.kind(), RtcpFeedbackKind::Nack))
    );
    let Some(rtx_codec) = formats.get(1) else {
        return;
    };
    assert_eq!(rtx_codec.payload_type(), 102);
    assert_eq!(
        rtx_codec.parameters().find_map(|(name, value)| {
            (name == rfc_rtp::fmtp::RTX_ASSOCIATION).then_some(value)
        }),
        Some("101".to_owned())
    );
    assert_eq!(consumable.mid(), Some("video-0"));
    let header_extension_uris = consumable
        .header_extensions()
        .map(HeaderExtension::uri)
        .collect::<Vec<_>>();
    assert_eq!(
        header_extension_uris,
        vec![webrtc::rtp_header_extension_uri::MID,]
    );
    let first_binding = consumable.bindings().next();
    assert!(first_binding.is_some());
    let Some(first_binding) = first_binding else {
        return;
    };
    assert_eq!(first_binding.ssrc(), Some(1234));
    assert_eq!(first_binding.repair_ssrc(), Some(1235));
    assert_eq!(first_binding.payload_type(), Some(101));
}

#[test]
fn derive_consumable_parameters_removes_partial_nack_rtx_pairs() {
    let capabilities = MediaCapabilities::new(
        vec![
            video_capability(CodecName::Vp8)
                .with_payload_type(pt(100))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::Nack, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::CcmFir, None)),
            video_capability(CodecName::Rtx)
                .with_payload_type(pt(101))
                .with_parameter(rfc_rtp::fmtp::RTX_ASSOCIATION, "100"),
        ],
        vec![],
    );

    for (has_nack, has_rtx) in [(true, false), (false, true)] {
        let primary = video_format(CodecName::Vp8, 96)
            .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None))
            .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::CcmFir, None));
        let primary = if has_nack {
            primary.with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::Nack, None))
        } else {
            primary
        };
        let mut formats = vec![primary];
        if has_rtx {
            formats.push(
                video_format(CodecName::Rtx, 97)
                    .with_parameter(rfc_rtp::fmtp::RTX_ASSOCIATION, "96"),
            );
        }
        let producer_parameters = MediaStream::new(formats, vec![], vec![]);

        let consumable = derive_consumable_rtp_parameters(&producer_parameters, &capabilities);
        assert!(consumable.is_ok());
        let Ok(consumable) = consumable else {
            return;
        };
        assert_repair_disabled(&consumable);
    }
}

#[test]
fn consumer_negotiation_removes_partial_nack_rtx_pairs() {
    let consumable_parameters = MediaStream::new(
        vec![
            video_format(CodecName::Vp8, 96)
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::Nack, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::CcmFir, None)),
            video_format(CodecName::Rtx, 97).with_parameter(rfc_rtp::fmtp::RTX_ASSOCIATION, "96"),
        ],
        vec![],
        vec![],
    );

    for (has_nack, has_rtx) in [(true, false), (false, true)] {
        let primary = video_capability(CodecName::Vp8)
            .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None))
            .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::CcmFir, None));
        let primary = if has_nack {
            primary.with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::Nack, None))
        } else {
            primary
        };
        let mut codecs = vec![primary];
        if has_rtx {
            codecs.push(
                video_capability(CodecName::Rtx)
                    .with_parameter(rfc_rtp::fmtp::RTX_ASSOCIATION, "96"),
            );
        }
        let consumer_capabilities = MediaCapabilities::new(codecs, vec![]);

        let negotiated =
            negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
        assert!(negotiated.is_ok());
        let Ok(negotiated) = negotiated else {
            return;
        };
        assert_repair_disabled(&negotiated);
    }
}

#[test]
fn consumer_negotiation_keeps_abs_send_time_and_filters_transport_cc_feedback() {
    let consumable_parameters = MediaStream::new(
        vec![
            MediaFormat::new(MediaKind::Video, "VP8", pt(96), 90_000)
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::Nack, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::TransportCc, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::GoogRemb, None)),
            MediaFormat::new(MediaKind::Video, "rtx", pt(97), 90_000).with_parameter("apt", "96"),
        ],
        vec![HeaderExtension::new(
            webrtc::rtp_header_extension_uri::ABS_SEND_TIME,
            4,
        )],
        vec![StreamBinding::new().with_ssrc(5678)],
    );
    let consumer_capabilities = MediaCapabilities::new(
        vec![
            MediaCodecCapability::new(MediaKind::Video, "VP8", 90_000)
                .with_payload_type(pt(100))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::Nack, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::TransportCc, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::GoogRemb, None)),
            MediaCodecCapability::new(MediaKind::Video, "rtx", 90_000)
                .with_payload_type(pt(101))
                .with_parameter("apt", "96"),
        ],
        vec![HeaderExtension::new(
            webrtc::rtp_header_extension_uri::ABS_SEND_TIME,
            4,
        )],
    );

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert!(negotiated_result.is_ok());
    let Ok(negotiated) = negotiated_result else {
        return;
    };
    let formats = negotiated.formats().collect::<Vec<_>>();
    assert_eq!(formats.len(), 2);
    let first_codec_feedback = formats
        .first()
        .map(|codec| {
            codec
                .rtcp_feedback()
                .map(RtcpFeedback::kind)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    assert!(first_codec_feedback.contains(&&RtcpFeedbackKind::NackPli));
    assert!(first_codec_feedback.contains(&&RtcpFeedbackKind::GoogRemb));
    assert!(!first_codec_feedback.contains(&&RtcpFeedbackKind::TransportCc));
    let header_extension_uris = negotiated
        .header_extensions()
        .map(HeaderExtension::uri)
        .collect::<Vec<_>>();
    assert_eq!(
        header_extension_uris,
        vec![webrtc::rtp_header_extension_uri::ABS_SEND_TIME]
    );
}

#[test]
fn consumer_negotiation_keeps_transport_cc_and_filters_goog_remb_feedback() {
    let consumable_parameters = MediaStream::new(
        vec![
            MediaFormat::new(MediaKind::Video, "VP8", pt(96), 90_000)
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::Nack, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::TransportCc, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::GoogRemb, None)),
            MediaFormat::new(MediaKind::Video, "rtx", pt(97), 90_000).with_parameter("apt", "96"),
        ],
        vec![HeaderExtension::new(
            webrtc::rtp_header_extension_uri::TRANSPORT_WIDE_CC_DRAFT_01,
            5,
        )],
        vec![StreamBinding::new().with_ssrc(5678)],
    );
    let consumer_capabilities = MediaCapabilities::new(
        vec![
            MediaCodecCapability::new(MediaKind::Video, "VP8", 90_000)
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::Nack, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::TransportCc, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::GoogRemb, None)),
            MediaCodecCapability::new(MediaKind::Video, "rtx", 90_000).with_parameter("apt", "96"),
        ],
        vec![HeaderExtension::new(
            webrtc::rtp_header_extension_uri::TRANSPORT_WIDE_CC_DRAFT_01,
            5,
        )],
    );

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert!(negotiated_result.is_ok());
    let Ok(negotiated) = negotiated_result else {
        return;
    };
    let formats = negotiated.formats().collect::<Vec<_>>();
    assert_eq!(formats.len(), 2);
    let [media_codec, _rtx_codec] = formats.as_slice() else {
        return;
    };
    let media_codec_feedback = media_codec
        .rtcp_feedback()
        .map(RtcpFeedback::kind)
        .collect::<Vec<_>>();
    assert!(media_codec_feedback.contains(&&RtcpFeedbackKind::NackPli));
    assert!(media_codec_feedback.contains(&&RtcpFeedbackKind::TransportCc));
    assert!(!media_codec_feedback.contains(&&RtcpFeedbackKind::GoogRemb));
    let header_extension_uris = negotiated
        .header_extensions()
        .map(HeaderExtension::uri)
        .collect::<Vec<_>>();
    assert_eq!(
        header_extension_uris,
        vec![webrtc::rtp_header_extension_uri::TRANSPORT_WIDE_CC_DRAFT_01]
    );
}

#[test]
fn consumer_negotiation_fails_when_no_media_codec_matches() {
    let consumable_parameters = MediaStream::new(
        vec![MediaFormat::new(MediaKind::Audio, "opus", pt(111), 48_000)],
        vec![],
        vec![],
    );
    let consumer_capabilities = MediaCapabilities::new(
        vec![MediaCodecCapability::new(MediaKind::Video, "VP8", 90_000).with_payload_type(pt(96))],
        vec![],
    );

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert_eq!(
        negotiated_result,
        Err(RtpNegotiationError::NoCompatibleConsumerCodec)
    );
    assert!(!negotiates(&consumable_parameters, &consumer_capabilities));
}

#[test]
fn invalid_rtx_apt_is_reported_as_invalid_diagnostic() {
    let capabilities = MediaCapabilities::new(
        vec![
            MediaCodecCapability::new(MediaKind::Video, "VP8", 90_000).with_payload_type(pt(100)),
            MediaCodecCapability::new(MediaKind::Video, "rtx", 90_000)
                .with_payload_type(pt(101))
                .with_parameter("apt", "100"),
        ],
        vec![],
    );
    let producer_parameters = MediaStream::new(
        vec![
            MediaFormat::new(MediaKind::Video, "VP8", pt(96), 90_000),
            MediaFormat::new(MediaKind::Video, "rtx", pt(97), 90_000).with_parameter("apt", "bad"),
        ],
        vec![],
        vec![],
    );

    let negotiation = derive_consumable_rtp_parameters(&producer_parameters, &capabilities);
    assert_eq!(
        negotiation,
        Err(RtpNegotiationError::InvalidAptParameter {
            codec_name: "rtx".to_owned(),
            payload_type: 97,
        })
    );
    let Err(error) = negotiation else {
        return;
    };
    let diagnostic = error.diagnostic();
    assert_eq!(diagnostic.kind(), ParseDiagnosticKind::InvalidInput);
    assert_eq!(diagnostic.rfc_reference().document(), "RFC 4588",);
    assert_eq!(diagnostic.rfc_reference().section(), "section 8.1");
}

#[test]
fn missing_rtx_associated_media_codec_is_reported_as_invalid_diagnostic() {
    let capabilities = MediaCapabilities::new(
        vec![
            MediaCodecCapability::new(MediaKind::Video, "VP8", 90_000).with_payload_type(pt(100)),
            MediaCodecCapability::new(MediaKind::Video, "rtx", 90_000)
                .with_payload_type(pt(101))
                .with_parameter("apt", "100"),
        ],
        vec![],
    );
    let producer_parameters = MediaStream::new(
        vec![
            MediaFormat::new(MediaKind::Video, "VP8", pt(96), 90_000),
            MediaFormat::new(MediaKind::Video, "rtx", pt(97), 90_000).with_parameter("apt", "98"),
        ],
        vec![],
        vec![],
    );

    let negotiation = derive_consumable_rtp_parameters(&producer_parameters, &capabilities);
    assert_eq!(
        negotiation,
        Err(RtpNegotiationError::MissingAssociatedMediaCodecForRtx {
            payload_type: 97,
            associated_payload_type: 98,
        })
    );
    let Err(error) = negotiation else {
        return;
    };
    let diagnostic = error.diagnostic();
    assert_eq!(diagnostic.kind(), ParseDiagnosticKind::InvalidInput);
    assert_eq!(diagnostic.rfc_reference().document(), "RFC 4588");
    assert_eq!(diagnostic.rfc_reference().section(), "section 8.1");
}

#[test]
fn incompatible_consumer_codec_is_reported_as_unsupported_diagnostic() {
    let consumable_parameters = MediaStream::new(
        vec![MediaFormat::new(MediaKind::Audio, "opus", pt(111), 48_000)],
        vec![],
        vec![],
    );
    let consumer_capabilities = MediaCapabilities::new(
        vec![MediaCodecCapability::new(MediaKind::Video, "VP8", 90_000)],
        vec![],
    );

    let negotiation =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert_eq!(
        negotiation,
        Err(RtpNegotiationError::NoCompatibleConsumerCodec)
    );
    let Err(error) = negotiation else {
        return;
    };
    let diagnostic = error.diagnostic();
    assert_eq!(diagnostic.kind(), ParseDiagnosticKind::UnsupportedFeature);
    assert_eq!(diagnostic.rfc_reference().document(), "RFC 3264");
    assert_eq!(diagnostic.rfc_reference().section(), "section 6");
}

#[test]
fn consumer_negotiation_rejects_vp9_profile_mismatch() {
    let consumable_parameters = MediaStream::new(
        vec![
            MediaFormat::new(MediaKind::Video, "VP9", pt(98), 90_000)
                .with_parameter("profile-id", "2")
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None)),
            MediaFormat::new(MediaKind::Video, "rtx", pt(99), 90_000).with_parameter("apt", "98"),
        ],
        vec![],
        vec![StreamBinding::new().with_ssrc(5678)],
    );
    let consumer_capabilities = MediaCapabilities::new(
        vec![
            MediaCodecCapability::new(MediaKind::Video, "VP9", 90_000)
                .with_payload_type(pt(100))
                .with_parameter("profile-id", "0")
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None)),
            MediaCodecCapability::new(MediaKind::Video, "rtx", 90_000)
                .with_payload_type(pt(101))
                .with_parameter("apt", "100"),
        ],
        vec![],
    );

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert_eq!(
        negotiated_result,
        Err(RtpNegotiationError::NoCompatibleConsumerCodec)
    );
    assert!(!negotiates(&consumable_parameters, &consumer_capabilities));
}

#[test]
fn consumer_negotiation_rejects_vp9_profiles_incompatible_with_omission() {
    for profile_id in ["2", "4"] {
        let consumable_parameters = MediaStream::new(
            vec![
                MediaFormat::new(MediaKind::Video, "VP9", pt(98), 90_000)
                    .with_parameter("profile-id", profile_id)
                    .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None)),
            ],
            vec![],
            vec![StreamBinding::new().with_ssrc(5678)],
        );
        let consumer_capabilities = MediaCapabilities::new(
            vec![
                MediaCodecCapability::new(MediaKind::Video, "VP9", 90_000)
                    .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None)),
            ],
            vec![],
        );

        let negotiated_result =
            negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
        assert_eq!(
            negotiated_result,
            Err(RtpNegotiationError::NoCompatibleConsumerCodec)
        );
        assert!(!negotiates(&consumable_parameters, &consumer_capabilities));
    }
}

#[test]
fn consumer_negotiation_accepts_missing_vp9_profile_id_for_profile_zero() {
    let consumable_parameters = MediaStream::new(
        vec![
            MediaFormat::new(MediaKind::Video, "VP9", pt(98), 90_000)
                .with_parameter("profile-id", "0")
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None)),
        ],
        vec![],
        vec![StreamBinding::new().with_ssrc(5678)],
    );
    let consumer_capabilities = MediaCapabilities::new(
        vec![
            MediaCodecCapability::new(MediaKind::Video, "VP9", 90_000)
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None)),
        ],
        vec![],
    );

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert!(negotiated_result.is_ok());
    assert!(negotiates(&consumable_parameters, &consumer_capabilities));
}

#[test]
fn consumer_negotiation_accepts_h264_when_capability_level_is_higher() {
    let consumable_parameters = h264_consumable_parameters("42e01f");
    let consumer_capabilities = h264_consumer_capabilities("42e032");

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert!(negotiated_result.is_ok());
    assert!(negotiates(&consumable_parameters, &consumer_capabilities));
}

#[test]
fn consumer_negotiation_rejects_h264_when_capability_level_is_lower() {
    let consumable_parameters = h264_consumable_parameters("42e032");
    let consumer_capabilities = h264_consumer_capabilities("42e01f");

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert_eq!(
        negotiated_result,
        Err(RtpNegotiationError::NoCompatibleConsumerCodec)
    );
    assert!(!negotiates(&consumable_parameters, &consumer_capabilities));
}

#[test]
fn consumer_negotiation_rejects_h264_level_1_1_for_level_1_capability() {
    let consumable_parameters = h264_consumable_parameters("42e00b");
    let consumer_capabilities = h264_consumer_capabilities("42e00a");

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert_eq!(
        negotiated_result,
        Err(RtpNegotiationError::NoCompatibleConsumerCodec)
    );
    assert!(!negotiates(&consumable_parameters, &consumer_capabilities));
}

#[test]
fn consumer_negotiation_accepts_h264_level_1b_for_level_1_1_capability() {
    let consumable_parameters = h264_consumable_parameters("42500b");
    let consumer_capabilities = h264_consumer_capabilities("42e00b");

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert!(negotiated_result.is_ok());
    assert!(negotiates(&consumable_parameters, &consumer_capabilities));
}

#[test]
fn consumer_negotiation_rejects_h264_high_profile_when_capability_omits_profile_level_id() {
    let consumable_parameters = h264_consumable_parameters("64001f");
    let consumer_capabilities = h264_consumer_capabilities_without_profile_level_id();

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert_eq!(
        negotiated_result,
        Err(RtpNegotiationError::NoCompatibleConsumerCodec)
    );
    assert!(!negotiates(&consumable_parameters, &consumer_capabilities));
}

#[test]
fn consumer_negotiation_rejects_h264_level_1_1_when_capability_omits_profile_level_id() {
    let consumable_parameters = h264_consumable_parameters("42a00b");
    let consumer_capabilities = h264_consumer_capabilities_without_profile_level_id();

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert_eq!(
        negotiated_result,
        Err(RtpNegotiationError::NoCompatibleConsumerCodec)
    );
    assert!(!negotiates(&consumable_parameters, &consumer_capabilities));
}

#[test]
fn consumer_negotiation_accepts_h264_when_both_sides_omit_profile_level_id() {
    let consumable_parameters = h264_consumable_parameters_without_profile_level_id();
    let consumer_capabilities = h264_consumer_capabilities_without_profile_level_id();

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert!(negotiated_result.is_ok());
    assert!(negotiates(&consumable_parameters, &consumer_capabilities));
}

#[test]
fn consumer_negotiation_rejects_h264_malformed_profile_level_id_when_capability_omits_it() {
    let consumable_parameters = h264_consumable_parameters("42e000");
    let consumer_capabilities = h264_consumer_capabilities_without_profile_level_id();

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert_eq!(
        negotiated_result,
        Err(RtpNegotiationError::NoCompatibleConsumerCodec)
    );
    assert!(!negotiates(&consumable_parameters, &consumer_capabilities));
}

#[test]
fn consumer_negotiation_rejects_h264_zero_level_idc() {
    let consumable_parameters = h264_consumable_parameters("42e000");
    let consumer_capabilities = h264_consumer_capabilities("42e00b");

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert_eq!(
        negotiated_result,
        Err(RtpNegotiationError::NoCompatibleConsumerCodec)
    );
    assert!(!negotiates(&consumable_parameters, &consumer_capabilities));
}

#[test]
fn consumer_negotiation_rejects_matching_malformed_h264_profile_level_id() {
    let consumable_parameters = h264_consumable_parameters("42e000");
    let consumer_capabilities = h264_consumer_capabilities("42e000");

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert_eq!(
        negotiated_result,
        Err(RtpNegotiationError::NoCompatibleConsumerCodec)
    );
    assert!(!negotiates(&consumable_parameters, &consumer_capabilities));
}

#[test]
fn consumer_negotiation_rejects_malformed_h264_packetization_mode() {
    let consumable_parameters = MediaStream::new(
        vec![
            MediaFormat::new(MediaKind::Video, "H264", pt(98), 90_000)
                .with_parameter("packetization-mode", "3")
                .with_parameter("profile-level-id", "42e01f"),
        ],
        vec![],
        vec![StreamBinding::new().with_ssrc(5678)],
    );
    let consumer_capabilities = MediaCapabilities::new(
        vec![
            MediaCodecCapability::new(MediaKind::Video, "H264", 90_000)
                .with_parameter("packetization-mode", "3")
                .with_parameter("profile-level-id", "42e01f"),
        ],
        vec![],
    );

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert_eq!(
        negotiated_result,
        Err(RtpNegotiationError::NoCompatibleConsumerCodec)
    );
    assert!(!negotiates(&consumable_parameters, &consumer_capabilities));
}

fn h264_consumable_parameters(profile_level_id: &str) -> MediaStream {
    h264_consumable_parameters_with_profile_level_id(Some(profile_level_id))
}

fn h264_consumable_parameters_without_profile_level_id() -> MediaStream {
    h264_consumable_parameters_with_profile_level_id(None)
}

fn h264_consumable_parameters_with_profile_level_id(profile_level_id: Option<&str>) -> MediaStream {
    let mut format = MediaFormat::new(MediaKind::Video, "H264", pt(98), 90_000)
        .with_parameter("packetization-mode", "1")
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None));
    if let Some(profile_level_id) = profile_level_id {
        format = format.with_parameter("profile-level-id", profile_level_id);
    }
    MediaStream::new(
        vec![format],
        vec![],
        vec![StreamBinding::new().with_ssrc(5678)],
    )
}

fn h264_consumer_capabilities(profile_level_id: &str) -> MediaCapabilities {
    h264_consumer_capabilities_with_profile_level_id(Some(profile_level_id))
}

fn h264_consumer_capabilities_without_profile_level_id() -> MediaCapabilities {
    h264_consumer_capabilities_with_profile_level_id(None)
}

fn h264_consumer_capabilities_with_profile_level_id(
    profile_level_id: Option<&str>,
) -> MediaCapabilities {
    let mut capability = MediaCodecCapability::new(MediaKind::Video, "H264", 90_000)
        .with_parameter("packetization-mode", "1")
        .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None));
    if let Some(profile_level_id) = profile_level_id {
        capability = capability.with_parameter("profile-level-id", profile_level_id);
    }
    MediaCapabilities::new(vec![capability], vec![])
}

#[test]
fn consumer_negotiation_filters_rtx_bindings_when_consumer_apt_does_not_match() {
    let consumable_parameters = MediaStream::new(
        vec![
            MediaFormat::new(MediaKind::Video, "VP8", pt(96), 90_000)
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None)),
            MediaFormat::new(MediaKind::Video, "rtx", pt(97), 90_000).with_parameter("apt", "96"),
        ],
        vec![],
        vec![
            StreamBinding::new()
                .with_ssrc(5678)
                .with_payload_type(pt(96)),
            StreamBinding::new()
                .with_ssrc(5679)
                .with_payload_type(pt(97)),
            StreamBinding::new().with_rid("fallback"),
        ],
    );
    let consumer_capabilities = MediaCapabilities::new(
        vec![
            MediaCodecCapability::new(MediaKind::Video, "VP8", 90_000)
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None)),
            MediaCodecCapability::new(MediaKind::Video, "rtx", 90_000).with_parameter("apt", "120"),
        ],
        vec![],
    );

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert!(negotiated_result.is_ok());
    let Ok(negotiated) = negotiated_result else {
        return;
    };

    let formats = negotiated.formats().collect::<Vec<_>>();
    assert_eq!(formats.len(), 1);
    let Some(format) = formats.first() else {
        return;
    };
    assert_eq!(format.codec_name(), "VP8");
    let bindings = negotiated.bindings().collect::<Vec<_>>();
    assert_eq!(bindings.len(), 2);
    let Some(first_binding) = bindings.first() else {
        return;
    };
    assert_eq!(first_binding.payload_type(), Some(96));
    let Some(second_binding) = bindings.get(1) else {
        return;
    };
    assert_eq!(second_binding.rid(), Some("fallback"));
}

#[test]
fn consumer_negotiation_preserves_repair_ssrc_when_rtx_is_listed_first() {
    let consumable_parameters = MediaStream::new(
        vec![
            video_format(CodecName::Rtx, 97).with_parameter(rfc_rtp::fmtp::RTX_ASSOCIATION, "96"),
            video_format(CodecName::Vp8, 96)
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::Nack, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None)),
        ],
        vec![],
        vec![
            StreamBinding::new()
                .with_ssrc(5678)
                .with_repair_ssrc(5679)
                .with_payload_type(pt(96)),
        ],
    );
    let consumer_capabilities = MediaCapabilities::new(
        vec![
            video_capability(CodecName::Vp8)
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::Nack, None))
                .with_rtcp_feedback(RtcpFeedback::new(RtcpFeedbackKind::NackPli, None)),
            video_capability(CodecName::Rtx).with_parameter(rfc_rtp::fmtp::RTX_ASSOCIATION, "96"),
        ],
        vec![],
    );

    let negotiated_result =
        negotiate_consumer_rtp_parameters(&consumable_parameters, &consumer_capabilities);
    assert!(negotiated_result.is_ok());
    let Ok(negotiated) = negotiated_result else {
        return;
    };

    let formats = negotiated.formats().collect::<Vec<_>>();
    assert_eq!(formats.len(), 2);
    let Some(media_codec) = formats.first() else {
        return;
    };
    assert_eq!(media_codec.codec_name(), rfc_rtp::codec_name::VP8);
    assert!(
        media_codec
            .rtcp_feedback()
            .any(|feedback| matches!(feedback.kind(), RtcpFeedbackKind::Nack))
    );
    let Some(rtx_codec) = formats.get(1) else {
        return;
    };
    assert_eq!(rtx_codec.codec_name(), rfc_rtp::codec_name::RTX);
    assert_eq!(
        negotiated
            .bindings()
            .map(|binding| (binding.ssrc(), binding.repair_ssrc()))
            .collect::<Vec<_>>(),
        vec![(Some(5678), Some(5679))]
    );
    assert!(negotiates(&consumable_parameters, &consumer_capabilities));
}
