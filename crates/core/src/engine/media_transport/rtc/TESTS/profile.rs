use std::collections::BTreeSet;

use o_sfu_rfc::{rtp as rfc_rtp, webrtc};
use o_sfu_router::{MediaKind as RouterMediaKind, rtp::RtcpFeedbackKind};
use str0m::{format::Codec, media::MediaKind};

use super::{super::validate_answer_sdp, *};

fn codec_flags(mask: u8) -> MediaCodecFlags {
    MediaCodecFlags::default()
        .with_opus(mask & 1 != 0)
        .with_pcmu(mask & 2 != 0)
        .with_pcma(mask & 4 != 0)
        .with_vp8(mask & 8 != 0)
        .with_h264(mask & 16 != 0)
        .with_h265(mask & 32 != 0)
        .with_vp9(mask & 64 != 0)
        .with_av1(mask & 128 != 0)
}

fn unique_names(config: &mut RtcConfig, audio: bool) -> Vec<String> {
    let mut names = config
        .codec_config()
        .params()
        .iter()
        .filter(|payload| payload.spec().codec.is_audio() == audio)
        .map(|payload| payload.spec().codec.to_string())
        .collect::<Vec<_>>();
    names.dedup();
    names
}

#[test]
fn all_codec_sets_keep_profile_views_aligned() {
    for mask in 0..=u8::MAX {
        let profile = RtpProfile::compile(codec_flags(mask), CodecPreferences::default())
            .expect("code-controlled RTP profile should project");
        let mut config = profile.session_config();
        let capabilities = profile.router_capabilities();
        let payloads = config.codec_config().params();
        assert!(
            capabilities
                .codecs()
                .map(|codec| (codec.codec_name().to_owned(), codec.payload_type()))
                .eq(payloads.iter().flat_map(|payload| {
                    [
                        (payload.spec().codec.to_string(), Some(*payload.pt())),
                        (
                            rfc_rtp::codec_name::RTX.to_owned(),
                            payload.resend().map(|pt| *pt),
                        ),
                    ]
                    .into_iter()
                    .filter(|(_name, payload_type)| payload_type.is_some())
                })),
            "capability mismatch for codec mask {mask}"
        );
        let payload_types = payloads
            .iter()
            .flat_map(|payload| [Some(*payload.pt()), payload.resend().map(|pt| *pt)].into_iter())
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(
            payload_types.len(),
            payload_types.iter().collect::<BTreeSet<_>>().len(),
            "payload collision for codec mask {mask}"
        );
        assert!(payloads.iter().all(|payload| {
            let video = payload.spec().codec.is_video();
            payload.fb_nack() == video && payload.resend().is_some() == video
        }));
        for (kind, audio) in [(MediaKind::Audio, true), (MediaKind::Video, false)] {
            assert_eq!(profile.codec_names(kind), unique_names(&mut config, audio));
        }
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the complete wire contract is clearer as one exact profile regression"
)]
fn all_enabled_profile_keeps_the_browser_wire_contract() {
    let profile = RtpProfile::compile(codec_flags(u8::MAX), CodecPreferences::default())
        .expect("code-controlled RTP profile should project");
    let mut config = profile.session_config();
    let payloads = config.codec_config().params();
    assert_eq!(
        payloads
            .iter()
            .map(|payload| (
                payload.spec().codec,
                *payload.pt(),
                payload.resend().map(|pt| *pt)
            ))
            .collect::<Vec<_>>(),
        [
            (Codec::Opus, 111_u8, None),
            (Codec::PCMU, 0, None),
            (Codec::PCMA, 8, None),
            (Codec::Vp8, 96, Some(97)),
            (Codec::H264, 127, Some(121)),
            (Codec::H264, 125, Some(107)),
            (Codec::H264, 108, Some(109)),
            (Codec::H264, 124, Some(120)),
            (Codec::H264, 123, Some(119)),
            (Codec::H264, 35, Some(36)),
            (Codec::H264, 114, Some(115)),
            (Codec::H265, 102, Some(103)),
            (Codec::Vp9, 98, Some(99)),
            (Codec::Vp9, 100, Some(101)),
            (Codec::Av1, 45, Some(46)),
        ]
    );
    let fmtp = payloads
        .iter()
        .filter_map(|payload| {
            let fmtp = payload.spec().format.to_string();
            (!fmtp.is_empty()).then(|| format!("{} {fmtp}", *payload.pt()))
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(
        fmtp,
        concat!(
            "111 minptime=10;useinbandfec=1\n",
            "127 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42001f\n",
            "125 level-asymmetry-allowed=1;packetization-mode=0;profile-level-id=42001f\n",
            "108 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f\n",
            "124 level-asymmetry-allowed=1;packetization-mode=0;profile-level-id=42e01f\n",
            "123 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=4d001f\n",
            "35 level-asymmetry-allowed=1;packetization-mode=0;profile-level-id=4d001f\n",
            "114 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=64001f\n",
            "102 profile-id=1;tier-flag=0;level-id=180\n",
            "98 profile-id=0\n",
            "100 profile-id=2"
        )
    );
    assert!(payloads.iter().all(|payload| {
        let video = payload.spec().codec.is_video();
        payload.fb_transport_cc()
            && payload.fb_nack() == video
            && payload.resend().is_some() == video
            && payload.fb_pli() == video
            && payload.fb_fir() == video
            && payload.fb_remb() == video
    }));
    let capabilities = profile.router_capabilities();
    let mut codecs = capabilities.codecs();
    for payload in payloads {
        let codec = codecs.next().expect("primary router codec should project");
        let fmtp = payload.spec().format.to_string();
        let video = payload.spec().codec.is_video();
        let kind = if video {
            RouterMediaKind::Video
        } else {
            RouterMediaKind::Audio
        };
        assert_eq!(codec.media_kind(), kind);
        assert_eq!(codec.clock_rate(), payload.spec().clock_rate.get());
        assert_eq!(codec.channels(), payload.spec().channels.map(u16::from));
        let params = fmtp
            .split(rfc_rtp::fmtp::PARAMETER_SEPARATOR)
            .filter_map(|entry| entry.split_once(rfc_rtp::fmtp::NAME_VALUE_SEPARATOR))
            .map(|(key, value)| (key.trim().to_owned(), value.trim().to_owned()));
        assert!(codec.parameters().eq(params));
        assert!(
            codec
                .rtcp_feedback()
                .map(|feedback| feedback.kind().clone())
                .eq([
                    Some(RtcpFeedbackKind::TransportCc),
                    video.then_some(RtcpFeedbackKind::Nack),
                    video.then_some(RtcpFeedbackKind::NackPli),
                    video.then_some(RtcpFeedbackKind::CcmFir),
                    video.then_some(RtcpFeedbackKind::GoogRemb),
                ]
                .into_iter()
                .flatten())
        );
        if let Some(resend) = payload.resend() {
            let rtx = codecs.next().expect("RTX router codec should project");
            assert_eq!(rtx.media_kind(), RouterMediaKind::Video);
            assert_eq!(rtx.codec_name(), rfc_rtp::codec_name::RTX);
            assert_eq!(rtx.payload_type(), Some(*resend));
            assert_eq!(rtx.clock_rate(), payload.spec().clock_rate.get());
            assert_eq!(rtx.channels(), None);
            assert_eq!(rtx.rtx_associated_payload_type(), Some(*payload.pt()));
            assert!(rtx.rtcp_feedback().next().is_none());
        }
    }
    assert!(codecs.next().is_none());
    assert_eq!(
        config
            .extension_map()
            .iter()
            .map(|(id, extension)| (id, extension.as_uri()))
            .collect::<Vec<_>>(),
        [
            (1_u8, webrtc::rtp_header_extension_uri::SSRC_AUDIO_LEVEL),
            (2, webrtc::rtp_header_extension_uri::ABS_SEND_TIME),
            (
                3,
                webrtc::rtp_header_extension_uri::TRANSPORT_WIDE_CC_DRAFT_01
            ),
            (4, webrtc::rtp_header_extension_uri::MID),
            (10, webrtc::rtp_header_extension_uri::RTP_STREAM_ID),
            (11, webrtc::rtp_header_extension_uri::REPAIRED_RTP_STREAM_ID),
            (13, "urn:3gpp:video-orientation"),
        ]
    );
    let router_exts = capabilities
        .header_extensions()
        .map(|extension| (extension.id().value(), extension.uri()));
    let rtc_exts = config
        .extension_map()
        .iter()
        .map(|(id, extension)| (id, extension.as_uri()));
    assert!(router_exts.eq(rtc_exts));
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the malformed topology table shares one valid repair fixture"
)]
fn answer_validation_requires_complete_video_repair_topology() {
    fn media(kind: &str, payload_types: &str, attributes: &[&str]) -> String {
        format!(
            "m={kind} 9 UDP/TLS/RTP/SAVPF {payload_types}\r\n{}\r\n",
            attributes.join("\r\n")
        )
    }

    let valid_pair = media(
        webrtc::media_kind::VIDEO,
        "96 97",
        &[
            "a=rtpmap:96 VP8/90000",
            "a=rtcp-fb:96 nack",
            "a=rtcp-fb:96 nack pli",
            "a=rtpmap:97 rtx/90000",
            "a=fmtp:97 apt=96",
            "a=extmap:10 urn:ietf:params:rtp-hdrext:sdes:rtp-stream-id",
            "a=extmap:11 urn:ietf:params:rtp-hdrext:sdes:repaired-rtp-stream-id",
            "a=ssrc-group:FID 1234 5678",
            "a=ssrc:1234 cname:test",
            "a=ssrc:5678 cname:test",
        ],
    );
    assert!(validate_answer_sdp(&valid_pair).is_ok());
    assert!(
        validate_answer_sdp(include_str!("testdata/chromium_optional_codecs_answer.sdp")).is_ok()
    );
    let valid_without_repair = media(
        webrtc::media_kind::VIDEO,
        "96",
        &["a=rtpmap:96 VP8/90000", "a=rtcp-fb:96 nack pli"],
    );
    assert!(validate_answer_sdp(&valid_without_repair).is_ok());
    let wildcard_feedback = format!("{valid_without_repair}a=rtcp-fb:* transport-cc\r\n");
    assert!(validate_answer_sdp(&wildcard_feedback).is_ok());
    let rid_declaration = valid_pair
        .replace("a=ssrc-group:FID 1234 5678\r\n", "")
        .replace("a=ssrc:1234 cname:test\r\n", "")
        .replace("a=ssrc:5678 cname:test\r\n", "")
        + "a=rid:hi send\r\n";
    assert!(validate_answer_sdp(&rid_declaration).is_ok());
    let rid_with_signaled_primaries = valid_pair.replace("a=ssrc-group:FID 1234 5678\r\n", "")
        + "a=rid:hi send\r\na=simulcast:send hi\r\n";
    assert!(validate_answer_sdp(&rid_with_signaled_primaries).is_ok());
    let inactive_with_signaled_primary = valid_pair
        .replace("a=ssrc-group:FID 1234 5678\r\n", "")
        .replace("a=ssrc:5678 cname:test\r\n", "")
        + "a=inactive\r\n";
    assert!(validate_answer_sdp(&inactive_with_signaled_primary).is_ok());

    let valid_video_formats = "m=video 9 UDP/TLS/RTP/SAVPF 96 97\r\n";
    let static_rtx_payload_type = rfc_rtp::AvpStaticPayloadType::Pcma.as_u8();
    let static_rtx = valid_pair
        .replace(" 97\r\n", &format!(" {static_rtx_payload_type}\r\n"))
        .replace(
            "a=rtpmap:97 ",
            &format!("a=rtpmap:{static_rtx_payload_type} "),
        )
        .replace("a=fmtp:97 ", &format!("a=fmtp:{static_rtx_payload_type} "));
    let invalid = [
        (
            "bare Generic NACK",
            valid_without_repair.replace("a=rtcp-fb:96 nack pli\r\n", "a=rtcp-fb:96 nack\r\n"),
        ),
        (
            "RTX without Generic NACK",
            valid_pair.replace("a=rtcp-fb:96 nack\r\n", ""),
        ),
        (
            "case-variant Generic NACK",
            valid_pair.replace("a=rtcp-fb:96 nack\r\n", "a=rtcp-fb:96 NACK\r\n"),
        ),
        ("static RTX payload type", static_rtx),
        ("orphan RTX", valid_pair.replace("a=fmtp:97 apt=96\r\n", "")),
        (
            "missing apt target",
            valid_pair.replace("a=fmtp:97 apt=96\r\n", "a=fmtp:97 apt=98\r\n"),
        ),
        (
            "duplicate repair mapping",
            valid_pair.replace(
                valid_video_formats,
                "m=video 9 UDP/TLS/RTP/SAVPF 96 97 98\r\n",
            ) + "a=rtpmap:98 rtx/90000\r\na=fmtp:98 apt=96\r\n",
        ),
        (
            "payload collision",
            valid_pair.replace(
                valid_video_formats,
                "m=video 9 UDP/TLS/RTP/SAVPF 96 97 97\r\n",
            ),
        ),
        (
            "extra rtpmap field",
            valid_pair.replace(
                "a=rtpmap:96 VP8/90000\r\n",
                "a=rtpmap:96 VP8/90000 extra\r\n",
            ),
        ),
        (
            "RTX absent from the media formats",
            valid_pair.replace(valid_video_formats, "m=video 9 UDP/TLS/RTP/SAVPF 96\r\n"),
        ),
        (
            "invalid FID arity",
            format!("{valid_pair}a=ssrc-group:FID 9000\r\n"),
        ),
        (
            "extra FID member",
            valid_pair.replace(
                "a=ssrc-group:FID 1234 5678\r\n",
                "a=ssrc-group:FID 1234 5678 9000\r\n",
            ),
        ),
        (
            "ambiguous FID",
            format!("{valid_pair}a=ssrc:9000 cname:test\r\na=ssrc-group:FID 1234 9000\r\n"),
        ),
        (
            "multiple RID-less FID pairs",
            format!(
                "{valid_pair}a=ssrc:9000 cname:test\r\na=ssrc:9001 cname:test\r\na=ssrc-group:FID 9000 9001\r\n"
            ),
        ),
        (
            "receive-only simulcast does not identify multiple FID pairs",
            format!(
                "{valid_pair}a=ssrc:9000 cname:test\r\na=ssrc:9001 cname:test\r\na=ssrc-group:FID 9000 9001\r\na=rid:unused send\r\na=rid:view recv\r\na=simulcast:recv view\r\n"
            ),
        ),
        (
            "one send RID does not identify multiple FID pairs",
            format!(
                "{valid_pair}a=ssrc:9000 cname:test\r\na=ssrc:9001 cname:test\r\na=ssrc-group:FID 9000 9001\r\na=rid:hi send\r\na=simulcast:send hi\r\n"
            ),
        ),
        (
            "orphan FID SSRC",
            valid_pair.replace("a=ssrc:5678 cname:test\r\n", ""),
        ),
        (
            "cross-media FID SSRC reuse",
            format!("{valid_pair}{valid_pair}"),
        ),
        (
            "cross-media audio SSRC reuse",
            format!(
                "{valid_pair}{}",
                media(
                    webrtc::media_kind::AUDIO,
                    "111",
                    &["a=rtpmap:111 opus/48000/2", "a=ssrc:1234 cname:audio"],
                )
            ),
        ),
        (
            "signaled repair without FID",
            valid_pair.replace("a=ssrc-group:FID 1234 5678\r\n", ""),
        ),
        (
            "repaired RID without primary RID",
            valid_pair.replace(
                "a=extmap:10 urn:ietf:params:rtp-hdrext:sdes:rtp-stream-id\r\n",
                "",
            ),
        ),
        (
            "RID repair without repaired RID",
            valid_pair.replace(
                "a=extmap:11 urn:ietf:params:rtp-hdrext:sdes:repaired-rtp-stream-id\r\n",
                "",
            ) + "a=rid:hi send\r\n",
        ),
        (
            "audio repair",
            media(
                webrtc::media_kind::AUDIO,
                "96 97",
                &[
                    "a=rtpmap:96 opus/48000/2",
                    "a=rtcp-fb:96 nack",
                    "a=rtpmap:97 rtx/48000",
                    "a=fmtp:97 apt=96",
                ],
            ),
        ),
    ];
    for (case, answer) in invalid {
        assert!(
            validate_answer_sdp(&answer).is_err(),
            "{case} must be rejected"
        );
    }
}
