use std::{sync::Arc, time::Instant};

use o_sfu_rfc::{
    rtp as rfc_rtp,
    webrtc::{self, sdp},
};
use o_sfu_router::{
    MediaKind as RouterMediaKind, rtp::RtcpFeedbackKind,
    test_support::rtp_samples::sample_simulcast_video_rtp_parameters,
};
use str0m::{
    Candidate, Rtc,
    change::SdpOffer,
    format::{Codec, FormatParams},
    media::Frequency,
};

use super::{
    super::{RtpProfile, state::PacketLoopState, worker::WorkerCommandContext},
    fixtures::*,
};
use crate::{
    AudioCodecPreference, VideoCodecPreference,
    engine::media_transport::{SessionUploadSlot, TransportMediaId},
};

const CHROME_OFFER_AUDIO_ONLY: &str = include_str!("testdata/chrome_offer_audio_only.sdp");
const FIREFOX_OFFER_AUDIO_ONLY: &str = include_str!("testdata/firefox_offer_audio_only.sdp");
const SAFARI_DATA_CHANNEL_OFFER: &str = include_str!("testdata/safari_datachannel_offer.sdp");

#[tokio::test]
async fn rtc_initial_session_offer_accepts_answer_without_candidates() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(1, 34, UserId::Integer(34));

    let offer = expect_initial_offer(&adapter, &session_key).await;
    let offer_sdp = offer.into_parts().0;
    assert!(offer_sdp.contains("m=audio"));
    assert!(offer_sdp.contains("m=video"));
    assert!(offer_sdp.contains("a=recvonly"));

    let mut remote = Rtc::new(Instant::now());
    let answer_sdp = remote
        .sdp_api()
        .accept_offer(
            SdpOffer::from_sdp_string(&offer_sdp)
                .expect("adapter should return parseable SDP offer"),
        )
        .expect("remote RTC should accept the adapter offer")
        .to_sdp_string();
    assert!(!answer_sdp.contains("a=candidate:"));

    assert!(
        adapter
            .apply_session_answer(&session_key, &answer_sdp)
            .await
            .is_ok(),
        "{answer_sdp}"
    );
    assert_eq!(
        adapter
            .create_initial_session_offer("test-room", &session_key)
            .await,
        Err(TransportAdapterError::UnsupportedFeature)
    );
}

#[test]
fn captured_browser_offer_fixtures_stay_str0m_parse_compatible() {
    for (name, offer_sdp, expected_media_line) in [
        (
            "chrome audio offer",
            CHROME_OFFER_AUDIO_ONLY,
            "m=audio 9 UDP/TLS/RTP/SAVPF",
        ),
        (
            "firefox audio offer",
            FIREFOX_OFFER_AUDIO_ONLY,
            "m=audio 9 UDP/TLS/RTP/SAVPF",
        ),
        (
            "safari datachannel offer",
            SAFARI_DATA_CHANNEL_OFFER,
            "m=application 9 UDP/DTLS/SCTP webrtc-datachannel",
        ),
    ] {
        let offer = SdpOffer::from_sdp_string(offer_sdp)
            .unwrap_or_else(|error| panic!("{name} should parse through str0m: {error:?}"));
        assert_eq!(
            offer.media_lines.len(),
            1,
            "{name} should expose one captured media line"
        );
        assert!(
            offer.to_sdp_string().contains(expected_media_line),
            "{name} should preserve the expected media line"
        );
    }
}

#[tokio::test]
async fn rtc_session_answer_rejects_invalid_sdp() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(1, 40, UserId::Integer(40));
    let _offer = expect_initial_offer(&adapter, &session_key).await;

    assert_eq!(
        adapter
            .apply_session_answer(&session_key, "not an SDP answer")
            .await,
        Err(TransportAdapterError::InvalidInput)
    );
}

#[tokio::test]
async fn rtc_session_answer_rejects_partial_repair_before_mutation() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(1, 140, UserId::Integer(140));
    let offer_sdp = expect_initial_offer(&adapter, &session_key)
        .await
        .into_parts()
        .0;
    let mut remote = build_remote_rtc(55_140);
    let valid_answer = remote
        .sdp_api()
        .accept_offer(SdpOffer::from_sdp_string(&offer_sdp).expect("initial offer should parse"))
        .expect("remote answer should build")
        .to_sdp_string();
    let invalid_answer = valid_answer.replacen("a=fmtp:97 apt=96\r\n", "", 1);
    assert_ne!(invalid_answer, valid_answer);
    let remapped_answer = valid_answer
        .replacen(" 96 97", " 96 118", 1)
        .replace("a=rtpmap:97 rtx/90000", "a=rtpmap:118 rtx/90000")
        .replace("a=fmtp:97 apt=96", "a=fmtp:118 apt=96");
    assert_ne!(remapped_answer, valid_answer);

    assert_eq!(
        adapter
            .apply_session_answer(&session_key, &invalid_answer)
            .await,
        Err(TransportAdapterError::InvalidInput)
    );
    assert_eq!(
        adapter
            .apply_session_answer(&session_key, &remapped_answer)
            .await,
        Err(TransportAdapterError::InvalidInput)
    );
    adapter
        .apply_session_answer(&session_key, &valid_answer)
        .await
        .expect("rejected repair topology should preserve the pending offer");
}

#[tokio::test]
async fn rtc_initial_session_offer_advertises_vp8_simulcast_receive_surface() {
    let adapter = rtc_with_codec_flags(MediaCodecFlags::default().with_h264(true));
    let session_key = transport_key(1, 134, UserId::Integer(134));

    let (offer_sdp, upload_slots) = expect_initial_offer(&adapter, &session_key)
        .await
        .into_parts();

    assert!(
        offer_sdp.contains(&sdp_rid_line("lo", sdp::rid::DIRECTION_RECV, Some(150_000))),
        "video offers should claim the low receive RID on the production VP8 path"
    );
    assert!(
        offer_sdp.contains(&sdp_rid_line(
            "hi",
            sdp::rid::DIRECTION_RECV,
            Some(4_000_000)
        )),
        "video offers should claim the high receive RID on the production VP8 path"
    );
    assert!(
        offer_sdp.contains(&sdp_simulcast_line(
            sdp::simulcast::DIRECTION_RECV,
            &["lo", "mid", "hi"]
        )),
        "video offers should advertise VP8 RID simulcast receive metadata"
    );
    assert!(
        offer_sdp.contains(&format!(
            "a={}:96 {} {}",
            sdp::attribute::RTCP_FB,
            webrtc::rtcp_feedback::kind::NACK,
            webrtc::rtcp_feedback::parameter::PLI
        )),
        "video offers should retain the keyframe feedback surface used after layer switches"
    );
    assert!(offer_sdp.contains("a=rtcp-fb:96 nack\r\n"));
    assert!(offer_sdp.contains("a=rtpmap:97 rtx/90000\r\n"));
    assert!(offer_sdp.contains("a=fmtp:97 apt=96\r\n"));
    assert!(offer_sdp.contains(webrtc::rtp_header_extension_uri::REPAIRED_RTP_STREAM_ID));
    let video_slot = upload_slots
        .iter()
        .find(|slot| slot.kind == RouterMediaKind::Video)
        .expect("initial offer should include a video upload slot");
    assert_eq!(
        video_slot.codecs,
        vec![
            String::from(rfc_rtp::codec_name::VP8),
            String::from(rfc_rtp::codec_name::H264)
        ]
    );
    assert_default_vp8_upload_slot(&upload_slots);
}

#[tokio::test]
async fn rtc_initial_session_offer_advertises_h264_simulcast_when_h264_leads() {
    let adapter = rtc_with_codec_policy(
        MediaCodecFlags::default().with_h264(true),
        CodecPreferences::default().with_video_order(&[VideoCodecPreference::H264]),
    );
    let session_key = transport_key(1, 136, UserId::Integer(136));

    let (offer_sdp, upload_slots) = expect_initial_offer(&adapter, &session_key)
        .await
        .into_parts();

    assert!(
        offer_sdp.contains(&sdp_rid_line("lo", sdp::rid::DIRECTION_RECV, Some(150_000))),
        "H264-first video offers should claim the low RID on the promoted matrix"
    );
    assert!(
        offer_sdp.contains(&sdp_simulcast_line(
            sdp::simulcast::DIRECTION_RECV,
            &["lo", "mid", "hi"]
        )),
        "H264-first video offers should claim the promoted RID simulcast matrix"
    );
    assert!(offer_sdp.contains("a=rtcp-fb:127 nack\r\n"));
    assert!(offer_sdp.contains("a=rtpmap:121 rtx/90000\r\n"));
    assert!(offer_sdp.contains("a=fmtp:121 apt=127\r\n"));
    let video_slot = upload_slots
        .iter()
        .find(|slot| slot.kind == RouterMediaKind::Video)
        .expect("initial offer should include a video upload slot");
    assert_eq!(
        video_slot.codecs,
        vec![
            String::from(rfc_rtp::codec_name::H264),
            String::from(rfc_rtp::codec_name::VP8)
        ]
    );
    assert_eq!(
        video_slot
            .simulcast_encodings
            .iter()
            .map(|encoding| (
                encoding.rid.as_str(),
                encoding.max_bitrate,
                encoding.resolution_scale,
            ))
            .collect::<Vec<_>>(),
        vec![
            ("lo", Some(Bitrate::from_kbps(150)), None),
            ("mid", Some(Bitrate::from_kbps(800)), None),
            ("hi", Some(Bitrate::from_mbps(4)), None),
        ]
    );
}

#[expect(
    clippy::redundant_closure_for_method_calls,
    reason = "str0m keeps the media-line type private so its method cannot be named here"
)]
#[tokio::test]
async fn rtc_initial_session_offer_reports_configured_codec_preferences() {
    let codec_flags = MediaCodecFlags::default()
        .with_pcmu(true)
        .with_pcma(true)
        .with_h264(true)
        .with_h265(true)
        .with_vp9(true)
        .with_av1(true);
    let codec_preferences = CodecPreferences::default()
        .with_audio_order(&[
            AudioCodecPreference::Pcma,
            AudioCodecPreference::Pcmu,
            AudioCodecPreference::Opus,
        ])
        .with_video_order(&[
            VideoCodecPreference::Av1,
            VideoCodecPreference::Vp9,
            VideoCodecPreference::H265,
            VideoCodecPreference::H264,
            VideoCodecPreference::Vp8,
        ]);
    let adapter = rtc_with_codec_policy(codec_flags, codec_preferences);
    let session_key = transport_key(1, 137, UserId::Integer(137));

    let (offer_sdp, upload_slots) = expect_initial_offer(&adapter, &session_key)
        .await
        .into_parts();

    let offer = SdpOffer::from_sdp_string(&offer_sdp).expect("initial offer should parse");
    let profile = RtpProfile::compile(codec_flags, codec_preferences)
        .expect("test RTP profile should compile");
    let router_capabilities = profile.router_capabilities();
    for (kind, expected) in [
        (
            RouterMediaKind::Audio,
            [
                rfc_rtp::codec_name::PCMA,
                rfc_rtp::codec_name::PCMU,
                rfc_rtp::codec_name::OPUS,
            ]
            .join(","),
        ),
        (
            RouterMediaKind::Video,
            [
                rfc_rtp::codec_name::AV1,
                rfc_rtp::codec_name::VP9,
                rfc_rtp::codec_name::H265,
                rfc_rtp::codec_name::H264,
                rfc_rtp::codec_name::VP8,
            ]
            .join(","),
        ),
    ] {
        let video = kind == RouterMediaKind::Video;
        let mut sdp_names = offer
            .media_lines
            .iter()
            .flat_map(|media| media.rtp_params())
            .filter(|payload| {
                payload.spec().codec != Codec::Rtx && payload.spec().codec.is_video() == video
            })
            .map(|payload| payload.spec().codec.to_string())
            .collect::<Vec<_>>();
        let mut router_names = router_capabilities
            .codecs()
            .filter(|codec| {
                codec.media_kind() == kind && codec.codec_name() != rfc_rtp::codec_name::RTX
            })
            .map(|codec| codec.codec_name().to_owned())
            .collect::<Vec<_>>();
        sdp_names.dedup();
        router_names.dedup();
        let slot = upload_slots
            .iter()
            .find(|slot| slot.kind == kind)
            .expect("initial offer should include the media upload slot");
        assert_eq!(sdp_names.join(","), expected);
        assert_eq!(router_names, sdp_names);
        assert_eq!(slot.codecs, sdp_names);
        if video {
            assert!(slot.simulcast_encodings.is_empty());
        }
    }
    assert!(!offer_sdp.contains(&sdp_simulcast_line(
        sdp::simulcast::DIRECTION_RECV,
        &["lo", "mid", "hi"]
    )));
}

#[tokio::test]
async fn rtc_initial_session_offer_projects_client_capabilities_from_answer() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(1, 38, UserId::Integer(38));

    let offer_sdp = expect_initial_offer(&adapter, &session_key)
        .await
        .into_parts()
        .0;
    let mut remote = reduced_capability_probe_rtc();
    remote
        .add_local_candidate(
            Candidate::host(
                SocketAddr::from(([127, 0, 0, 1], 55_038)),
                webrtc::ice::transport::UDP,
            )
            .expect("test host candidate should build"),
        )
        .expect("remote candidate should register");
    let answer = remote
        .sdp_api()
        .accept_offer(
            SdpOffer::from_sdp_string(&without_vp8_repair(&offer_sdp))
                .expect("adapter should return parseable SDP offer"),
        )
        .expect("remote answer should build")
        .to_sdp_string();

    let applied_answer = adapter
        .apply_session_answer(&session_key, &answer)
        .await
        .expect("real RTC answer should apply");
    let projected = applied_answer
        .client_capabilities()
        .expect("real RTC answer should expose client RTP capabilities");
    let codec_names = projected
        .codecs()
        .map(|codec| (codec.media_kind(), codec.codec_name().to_owned()))
        .collect::<Vec<_>>();
    assert_eq!(
        codec_names,
        vec![
            (
                RouterMediaKind::Audio,
                String::from(rfc_rtp::codec_name::OPUS),
            ),
            (
                RouterMediaKind::Video,
                String::from(rfc_rtp::codec_name::VP8),
            ),
        ]
    );
    assert!(projected.codecs().all(|codec| {
        !codec.codec().is_rtx()
            && codec
                .rtcp_feedback()
                .all(|feedback| feedback.kind() != &RtcpFeedbackKind::Nack)
    }));
}

#[tokio::test]
async fn rtc_simulcast_publish_intent_preserves_negotiated_encoding_facts() {
    let mut config = test_media_transport_config(1, test_rtc_port_range());
    config.bitrate_limits =
        SessionBitrateLimits::new(Bitrate::from_bps(2_222_222), Bitrate::from_bps(3_333_333));
    config.codec_flags = MediaCodecFlags::default().with_vp8(false).with_h264(true);
    let adapter = RtcWorker::for_test(config);
    let session_key = transport_key(1, 135, UserId::Integer(135));

    let mut remote = complete_initial_offer_answer(&adapter, &session_key, 55_135).await;

    let transport_media_id = adapter
        .add_recv_media(
            &session_key,
            Str0mMediaKind::Video,
            &simulcast_video_rtp_parameters_with_repair(),
        )
        .await
        .expect("simulcast publish intent should stage a renegotiation offer");
    let negotiated_mid = adapter
        .debug_resolve_mid(transport_media_id)
        .await
        .expect("simulcast publish should expose the staged mid");

    let (renegotiation_offer, upload_slots) = adapter
        .create_session_renegotiation_offer(&session_key)
        .await
        .expect("staged simulcast renegotiation offer should be available")
        .into_parts();
    let video_slot = upload_slots
        .iter()
        .find(|slot| slot.kind == RouterMediaKind::Video)
        .expect("simulcast publish should expose a video slot");
    assert_eq!(
        video_slot.codecs,
        vec![String::from(rfc_rtp::codec_name::VP8)]
    );
    assert!(
        renegotiation_offer.contains(&sdp_rid_line("lo", sdp::rid::DIRECTION_RECV, Some(150_000))),
        "simulcast publish offers should expose low RID receive metadata"
    );
    assert!(
        renegotiation_offer.contains(&sdp_rid_line("hi", sdp::rid::DIRECTION_RECV, Some(900_000))),
        "simulcast publish offers should expose high RID receive metadata"
    );
    assert!(
        renegotiation_offer.contains(&sdp_simulcast_line(
            sdp::simulcast::DIRECTION_RECV,
            &["lo", "hi"]
        )),
        "simulcast publish offers should advertise the selected-layer receive ladder"
    );

    let answer_sdp = remote
        .sdp_api()
        .accept_offer(
            SdpOffer::from_sdp_string(&renegotiation_offer).expect("simulcast offer should parse"),
        )
        .expect("remote simulcast answer should build")
        .to_sdp_string();
    let answer_sdp = answer_with_extmap_id(
        &answer_sdp,
        &negotiated_mid,
        webrtc::rtp_header_extension_uri::RTP_STREAM_ID,
        5,
    );
    let answer_sdp = answer_with_extmap_id(
        &answer_sdp,
        &negotiated_mid,
        webrtc::rtp_header_extension_uri::REPAIRED_RTP_STREAM_ID,
        6,
    );
    let answer_sdp = answer_with_simulcast_send_rids(
        &answer_sdp,
        &negotiated_mid,
        &[("lo", Some(150_000)), ("hi", Some(900_000))],
    );
    let answer_sdp = answer_with_leading_fid_pair(&answer_sdp, &negotiated_mid);
    let applied_answer = adapter
        .apply_session_answer(&session_key, &answer_sdp)
        .await
        .expect("simulcast answer should apply");

    let negotiated_parameters = adapter
        .negotiated_producer_parameters(&session_key, transport_media_id)
        .await
        .expect("answered simulcast publish should project router RTP parameters");
    assert_renumbered_simulcast_repair(&negotiated_parameters);
    let upload_encodings = applied_answer.negotiated_producer_upload_encodings(transport_media_id);
    assert_eq!(
        upload_encodings
            .iter()
            .map(|encoding| (encoding.rid.as_str(), encoding.max_bitrate))
            .collect::<Vec<_>>(),
        vec![
            ("lo", Some(Bitrate::from_bps(150_000))),
            ("hi", Some(Bitrate::from_bps(900_000))),
        ]
    );
}

fn simulcast_video_rtp_parameters_with_repair() -> RouterRtpParameters {
    let parameters = sample_simulcast_video_rtp_parameters(Some("simulcast-up"));
    RouterRtpParameters::new(
        parameters.formats().cloned().collect(),
        parameters.header_extensions().cloned().collect(),
        parameters
            .bindings()
            .zip([31_101, 31_102])
            .map(|(binding, repair_ssrc)| binding.clone().with_repair_ssrc(repair_ssrc))
            .collect(),
    )
    .with_mid("simulcast-up")
}

fn assert_renumbered_simulcast_repair(parameters: &RouterRtpParameters) {
    assert_eq!(
        parameters
            .bindings()
            .map(|encoding| {
                (
                    encoding.rid(),
                    encoding.ssrc(),
                    encoding.repair_ssrc(),
                    encoding.max_bitrate(),
                )
            })
            .collect::<Vec<_>>(),
        vec![
            (Some("lo"), Some(31_001), Some(31_101), Some(150_000),),
            (Some("hi"), Some(31_002), Some(31_102), Some(900_000),),
        ]
    );
    let formats = parameters.formats().collect::<Vec<_>>();
    let primary = formats
        .iter()
        .find(|format| !format.codec().is_rtx())
        .expect("simulcast answer should retain its primary format");
    assert!(formats.iter().any(|format| {
        format.codec().is_rtx()
            && format.rtx_associated_payload_type() == Some(primary.payload_type())
    }));
    let extensions = parameters
        .header_extensions()
        .map(|extension| (extension.uri_kind().as_str(), extension.id().value()))
        .collect::<Vec<_>>();
    assert!(extensions.contains(&(webrtc::rtp_header_extension_uri::RTP_STREAM_ID, 5)));
    assert!(extensions.contains(&(webrtc::rtp_header_extension_uri::REPAIRED_RTP_STREAM_ID, 6)));
}

#[tokio::test]
async fn rtc_simulcast_answer_rejects_unoffered_rid_alternatives() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(1, 335, UserId::Integer(335));
    let mut remote = complete_initial_offer_answer(&adapter, &session_key, 55_035).await;

    let transport_media_id = adapter
        .add_recv_media(
            &session_key,
            Str0mMediaKind::Video,
            &sample_simulcast_video_rtp_parameters(Some("simulcast-up")),
        )
        .await
        .expect("simulcast publish intent should stage a renegotiation offer");
    let negotiated_mid = adapter
        .debug_resolve_mid(transport_media_id)
        .await
        .expect("simulcast publish should expose the staged mid");
    let renegotiation_offer = adapter
        .create_session_renegotiation_offer(&session_key)
        .await
        .expect("staged simulcast renegotiation offer should be available")
        .into_parts()
        .0;
    let answer_sdp = remote
        .sdp_api()
        .accept_offer(
            SdpOffer::from_sdp_string(&renegotiation_offer).expect("simulcast offer should parse"),
        )
        .expect("remote simulcast answer should build")
        .to_sdp_string();
    let valid_answer_sdp = answer_with_simulcast_send_rids(
        &answer_sdp,
        &negotiated_mid,
        &[("lo", Some(150_000)), ("hi", Some(900_000))],
    );
    let lo_declaration = sdp_rid_line("lo", sdp::rid::DIRECTION_SEND, Some(150_000));
    let lo_declaration = format!("{lo_declaration}{}", sdp::CRLF);
    let duplicate_rid_answer = valid_answer_sdp.replacen(
        &lo_declaration,
        &format!("{lo_declaration}{lo_declaration}"),
        1,
    );
    assert_eq!(
        adapter
            .apply_session_answer(&session_key, &duplicate_rid_answer)
            .await,
        Err(TransportAdapterError::InvalidInput)
    );

    let alternative_rid_answer = valid_answer_sdp.replacen(
        &sdp_simulcast_line(sdp::simulcast::DIRECTION_SEND, &["lo", "hi"]),
        &format!(
            "{}{}{}{}{}lo{}backup{}hi",
            sdp::ATTR,
            sdp::attribute::SIMULCAST,
            sdp::ATTR_SEP,
            sdp::simulcast::DIRECTION_SEND,
            sdp::SP,
            sdp::simulcast::ALTERNATIVE_SEPARATOR,
            sdp::simulcast::STREAM_SEPARATOR
        ),
        1,
    );

    assert_eq!(
        adapter
            .apply_session_answer(&session_key, &alternative_rid_answer)
            .await,
        Err(TransportAdapterError::InvalidInput)
    );
    adapter
        .apply_session_answer(&session_key, &valid_answer_sdp)
        .await
        .expect("ambiguous RID answers should preserve the pending offer");
}

#[tokio::test]
async fn rtc_simulcast_answer_rejects_unoffered_plain_rid() {
    let adapter =
        rtc_with_bitrate_limits(Bitrate::from_bps(2_222_222), Bitrate::from_bps(3_333_333));
    let session_key = transport_key(1, 336, UserId::Integer(336));
    let mut remote = complete_initial_offer_answer(&adapter, &session_key, 55_036).await;

    let transport_media_id = adapter
        .add_recv_media(
            &session_key,
            Str0mMediaKind::Video,
            &sample_simulcast_video_rtp_parameters(Some("simulcast-up")),
        )
        .await
        .expect("simulcast publish intent should stage a renegotiation offer");
    let negotiated_mid = adapter
        .debug_resolve_mid(transport_media_id)
        .await
        .expect("simulcast publish should expose the staged mid");
    let renegotiation_offer = adapter
        .create_session_renegotiation_offer(&session_key)
        .await
        .expect("staged simulcast renegotiation offer should be available")
        .into_parts()
        .0;
    let answer_sdp = remote
        .sdp_api()
        .accept_offer(
            SdpOffer::from_sdp_string(&renegotiation_offer).expect("simulcast offer should parse"),
        )
        .expect("remote simulcast answer should build")
        .to_sdp_string();
    let answer_sdp = answer_with_simulcast_send_rids(
        &answer_sdp,
        &negotiated_mid,
        &[("lo", Some(150_000)), ("backup", Some(450_000))],
    );

    assert_eq!(
        adapter
            .apply_session_answer(&session_key, &answer_sdp)
            .await,
        Err(TransportAdapterError::InvalidInput)
    );
}

#[tokio::test]
async fn rtc_simulcast_answer_rejects_larger_max_br_than_offer() {
    let adapter =
        rtc_with_bitrate_limits(Bitrate::from_bps(2_222_222), Bitrate::from_bps(3_333_333));
    let session_key = transport_key(1, 337, UserId::Integer(337));
    let mut remote = complete_initial_offer_answer(&adapter, &session_key, 55_037).await;

    let transport_media_id = adapter
        .add_recv_media(
            &session_key,
            Str0mMediaKind::Video,
            &sample_simulcast_video_rtp_parameters(Some("simulcast-up")),
        )
        .await
        .expect("simulcast publish intent should stage a renegotiation offer");
    let negotiated_mid = adapter
        .debug_resolve_mid(transport_media_id)
        .await
        .expect("simulcast publish should expose the staged mid");
    let renegotiation_offer = adapter
        .create_session_renegotiation_offer(&session_key)
        .await
        .expect("staged simulcast renegotiation offer should be available")
        .into_parts()
        .0;
    let valid_answer_sdp = remote
        .sdp_api()
        .accept_offer(
            SdpOffer::from_sdp_string(&renegotiation_offer).expect("simulcast offer should parse"),
        )
        .expect("remote simulcast answer should build")
        .to_sdp_string();
    let invalid_answer_sdp = answer_with_simulcast_send_rids(
        &valid_answer_sdp,
        &negotiated_mid,
        &[("lo", Some(150_001)), ("hi", Some(900_000))],
    );

    assert_eq!(
        adapter
            .apply_session_answer(&session_key, &invalid_answer_sdp)
            .await,
        Err(TransportAdapterError::InvalidInput)
    );
    adapter
        .apply_session_answer(&session_key, &valid_answer_sdp)
        .await
        .expect("rejected answer should preserve the pending offer");
}

#[tokio::test]
async fn rtc_producer_answer_rejects_non_one_byte_extmap_id() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(1, 338, UserId::Integer(338));
    let mut remote = complete_initial_offer_answer(&adapter, &session_key, 55_038).await;

    let transport_media_id = adapter
        .add_recv_media(
            &session_key,
            Str0mMediaKind::Video,
            &sample_router_rtp_parameters("compat-producer-extmap", 93_000),
        )
        .await
        .expect("protocol producer media should stage a renegotiation offer");
    let negotiated_mid = adapter
        .debug_resolve_mid(transport_media_id)
        .await
        .expect("producer media should expose its staged mid");
    let renegotiation_offer = adapter
        .create_session_renegotiation_offer(&session_key)
        .await
        .expect("staged renegotiation offer should be available")
        .into_parts()
        .0;
    let answer_sdp = remote
        .sdp_api()
        .accept_offer(
            SdpOffer::from_sdp_string(&renegotiation_offer).expect("producer offer should parse"),
        )
        .expect("remote producer answer should build")
        .to_sdp_string();
    let answer_sdp = answer_with_extmap_id(
        &answer_sdp,
        &negotiated_mid,
        webrtc::rtp_header_extension_uri::RTP_STREAM_ID,
        rfc_rtp::header_extension::ONE_BYTE_ID_RESERVED,
    );

    assert_eq!(
        adapter
            .apply_session_answer(&session_key, &answer_sdp)
            .await,
        Err(TransportAdapterError::InvalidInput)
    );
}

#[tokio::test]
async fn rtc_initial_session_offer_rejects_overlapping_pending_offer() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(1, 35, UserId::Integer(35));

    let _offer = expect_initial_offer(&adapter, &session_key).await;
    let handle = adapter.test_handle();
    let first_counter = handle
        .bitrate_registry
        .lock()
        .ok()
        .and_then(|registry| {
            registry
                .egress_bitrates_by_session
                .get(&session_key)
                .cloned()
        })
        .expect("initial offer should register its egress counter");
    assert_eq!(
        adapter
            .create_initial_session_offer("test-room", &session_key)
            .await,
        Err(TransportAdapterError::InvalidInput)
    );
    let probe_key = session_key.clone();
    let (repeated_counter, registered_counter) = handle
        .debug_handle
        .probe(
            move |state: &PacketLoopState, context: &WorkerCommandContext<'_>| {
                let session_counter = state
                    .users
                    .get(&probe_key)
                    .map(|session| Arc::clone(&session.egress_bitrate))?;
                let registered_counter = context
                    .bitrate_registry
                    .lock()
                    .ok()?
                    .egress_bitrates_by_session
                    .get(&probe_key)
                    .cloned()?;
                Some((session_counter, registered_counter))
            },
        )
        .await
        .flatten()
        .expect("repeated offer should retain its egress counter");
    assert!(Arc::ptr_eq(&first_counter, &repeated_counter));
    assert!(Arc::ptr_eq(&first_counter, &registered_counter));
}

#[tokio::test]
async fn rtc_session_renegotiation_offer_stages_protocol_producer_additions() {
    let adapter =
        rtc_with_bitrate_limits(Bitrate::from_bps(2_222_222), Bitrate::from_bps(3_333_333));
    let session_key = transport_key(1, 45, UserId::Integer(45));

    let mut remote = complete_initial_offer_answer(&adapter, &session_key, 55_006).await;

    let transport_media_id = adapter
        .add_recv_media(
            &session_key,
            Str0mMediaKind::Video,
            &sample_router_rtp_parameters("compat-producer-mid", 89_000),
        )
        .await
        .expect("protocol producer media should stage a renegotiation offer");

    let renegotiation_offer = adapter
        .create_session_renegotiation_offer(&session_key)
        .await
        .expect("staged renegotiation offer should be available");
    let renegotiation_sdp = renegotiation_offer.into_parts().0;
    assert!(renegotiation_sdp.contains("m=video"));

    let negotiated_mid = adapter
        .debug_resolve_mid(transport_media_id)
        .await
        .expect("transport media should resolve to the server-assigned mid");
    assert!(renegotiation_sdp.contains(&format!("a=mid:{negotiated_mid}")));

    let answer_sdp = remote
        .sdp_api()
        .accept_offer(
            SdpOffer::from_sdp_string(&renegotiation_sdp).expect("producer offer should parse"),
        )
        .expect("remote producer answer should build")
        .to_sdp_string();
    let ambiguous_answer = answer_with_leading_fid_pair(&answer_sdp, &negotiated_mid);
    assert_eq!(
        adapter
            .apply_session_answer(&session_key, &ambiguous_answer)
            .await,
        Err(TransportAdapterError::InvalidInput)
    );
    let fid_pair = fid_pair_for_mid(&answer_sdp, &negotiated_mid)
        .expect("RID-less producer answer should signal a FID pair");
    adapter
        .apply_session_answer(&session_key, &answer_sdp)
        .await
        .expect("RID-less FID answer should apply");

    assert_eq!(
        adapter
            .debug_session_stream_rx_ssrc(&session_key, negotiated_mid)
            .await,
        Some(fid_pair.0),
        "RID-less recv media should use the primary SSRC accepted from the answer"
    );
    assert_eq!(
        adapter.debug_session_max_bitrate_in(&session_key).await,
        Some(Bitrate::from_bps(2_222_222)),
        "renegotiated recv media should reapply the incoming bitrate cap after the answer lands"
    );

    let negotiated_parameters = adapter
        .negotiated_producer_parameters(&session_key, transport_media_id)
        .await
        .expect("answered producer negotiation should project router RTP parameters");
    assert_eq!(negotiated_parameters.mid(), Some(&*negotiated_mid));
    let formats = negotiated_parameters.formats().collect::<Vec<_>>();
    assert!(!formats.is_empty());
    for primary in formats.iter().filter(|format| !format.codec().is_rtx()) {
        assert!(
            primary
                .rtcp_feedback()
                .any(|feedback| { feedback.kind() == &RtcpFeedbackKind::Nack })
        );
        assert!(
            primary
                .rtcp_feedback()
                .any(|feedback| { feedback.kind() == &RtcpFeedbackKind::NackPli })
        );
        assert_eq!(
            formats
                .iter()
                .filter(|format| {
                    format.codec().is_rtx()
                        && format.rtx_associated_payload_type() == Some(primary.payload_type())
                })
                .count(),
            1
        );
    }
    let binding = negotiated_parameters
        .bindings()
        .next()
        .expect("RID-less producer should project its signaled binding");
    assert_eq!(binding.rid(), None);
    assert_eq!(
        (binding.ssrc(), binding.repair_ssrc()),
        (Some(fid_pair.0), Some(fid_pair.1))
    );
}

#[tokio::test]
async fn rtc_protocol_publish_projects_rid_bindings_when_publish_intent_is_empty() {
    let offered = [
        ("lo", Some(150_000), Some(4)),
        ("mid", Some(800_000), Some(2)),
        ("hi", Some(4_000_000), Some(1)),
    ];
    for accepted_rids in [
        &["lo", "mid", "hi"][..],
        &["lo", "hi"],
        &["mid", "hi"],
        &["mid"],
    ] {
        let adapter = RtcWorker::default();
        let session_key = transport_key(1, 46, UserId::Integer(46));
        let mut remote = complete_initial_offer_answer(&adapter, &session_key, 55_007).await;
        let transport_media_id = adapter
            .add_recv_media(
                &session_key,
                Str0mMediaKind::Video,
                &RouterRtpParameters::new(vec![], vec![], vec![]),
            )
            .await
            .expect("protocol publish intent should stage a recv-only media line");
        let (renegotiation_sdp, upload_slots) = adapter
            .create_session_renegotiation_offer(&session_key)
            .await
            .expect("protocol publish should stage a follow-up offer")
            .into_parts();
        assert_default_vp8_upload_slot(&upload_slots);
        assert!(
            renegotiation_sdp.contains(&sdp_simulcast_line(
                sdp::simulcast::DIRECTION_RECV,
                &["lo", "mid", "hi"]
            )),
            "empty protocol publish intents should emit the server-defined RID ladder"
        );
        let negotiated_mid = adapter
            .debug_resolve_mid(transport_media_id)
            .await
            .expect("transport media should expose its negotiated mid");
        let answer_sdp = remote
            .sdp_api()
            .accept_offer(
                SdpOffer::from_sdp_string(&renegotiation_sdp)
                    .expect("default simulcast offer should parse"),
            )
            .expect("remote default simulcast answer should build")
            .to_sdp_string();
        let accepted = offered
            .iter()
            .filter(|(rid, _, _)| accepted_rids.contains(rid))
            .copied()
            .collect::<Vec<_>>();
        let answer_sdp = answer_with_simulcast_send_rids(
            &answer_sdp,
            &negotiated_mid,
            &accepted
                .iter()
                .map(|(rid, bitrate, _scale)| (*rid, *bitrate))
                .collect::<Vec<_>>(),
        );
        let applied = adapter
            .apply_session_answer(&session_key, &answer_sdp)
            .await
            .expect("default simulcast answer should apply");
        assert_eq!(
            applied
                .negotiated_producer_upload_encodings(transport_media_id)
                .iter()
                .map(|encoding| (
                    encoding.rid.as_str(),
                    encoding.max_bitrate.map(Bitrate::as_bps),
                    encoding.resolution_scale,
                ))
                .collect::<Vec<_>>(),
            accepted,
            "answer pruning must preserve the offered RID bitrate and resolution"
        );
        let negotiated_parameters = adapter
            .negotiated_producer_parameters(&session_key, transport_media_id)
            .await
            .expect("protocol publish should project negotiated RTP parameters");
        assert_eq!(
            negotiated_parameters
                .bindings()
                .map(|binding| binding.rid())
                .collect::<Vec<_>>(),
            accepted_rids.iter().copied().map(Some).collect::<Vec<_>>()
        );
    }
}

#[tokio::test]
async fn rtc_session_renegotiation_projects_multiple_protocol_producers_from_one_answer() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(1, 48, UserId::Integer(48));

    let mut remote = complete_initial_offer_answer(&adapter, &session_key, 55_048).await;

    let audio_media_id = adapter
        .add_recv_media(
            &session_key,
            Str0mMediaKind::Audio,
            &RouterRtpParameters::new(vec![], vec![], vec![]),
        )
        .await
        .expect("audio publish intent should stage a renegotiation offer");
    let video_media_id = adapter
        .add_recv_media(
            &session_key,
            Str0mMediaKind::Video,
            &RouterRtpParameters::new(vec![], vec![], vec![]),
        )
        .await
        .expect("video publish intent should merge into the same renegotiation offer");

    let renegotiation_offer = adapter
        .create_session_renegotiation_offer(&session_key)
        .await
        .expect("staged renegotiation offer should be available");
    apply_offer_answer(
        &adapter,
        &session_key,
        &mut remote,
        renegotiation_offer.into_parts().0,
    )
    .await;

    let audio_parameters = adapter
        .negotiated_producer_parameters(&session_key, audio_media_id)
        .await;
    assert!(
        audio_parameters.is_ok(),
        "audio publish should project negotiated RTP parameters after a shared answer, got {audio_parameters:?}"
    );
    let video_parameters = adapter
        .negotiated_producer_parameters(&session_key, video_media_id)
        .await;
    assert!(
        video_parameters.is_ok(),
        "video publish should project negotiated RTP parameters after a shared answer, got {video_parameters:?}"
    );
}

#[tokio::test]
async fn rtc_session_renegotiation_offer_stages_protocol_consumer_additions() {
    let adapter = RtcWorker::default();
    let src_key = transport_key(1, 36, UserId::Integer(36));
    let consumer_key = transport_key(1, 37, UserId::Integer(37));

    let _offer = expect_initial_offer(&adapter, &src_key).await;
    let source_media_id = adapter
        .add_recv_media(
            &src_key,
            Str0mMediaKind::Video,
            &sample_router_rtp_parameters("source-up", 81_000),
        )
        .await
        .expect("source media should register");

    let mut remote = complete_initial_offer_answer(&adapter, &consumer_key, 55_002).await;

    let consumer_media_id = adapter
        .add_send_media(
            &consumer_key,
            Str0mMediaKind::Video,
            TransportSourceKey::new(src_key.clone(), source_media_id),
            &sample_router_rtp_parameters("compat-mid", 82_000),
            true,
        )
        .await
        .expect("protocol consumer media should stage a renegotiation offer");

    let renegotiation_offer = adapter
        .create_session_renegotiation_offer(&consumer_key)
        .await
        .expect("staged renegotiation offer should be available");
    let renegotiation_sdp = renegotiation_offer.into_parts().0;
    assert!(renegotiation_sdp.contains("m=video"));

    let renegotiated_mid = adapter
        .debug_resolve_mid(consumer_media_id)
        .await
        .expect("transport media should resolve to the server-assigned mid");
    assert!(renegotiation_sdp.contains(&format!("a=mid:{renegotiated_mid}")));
    let consumer_section = media_section_for_mid(&renegotiation_sdp, &renegotiated_mid)
        .expect("consumer offer should contain its send-only media section");
    assert!(consumer_section.contains("a=sendonly"));
    assert!(consumer_section.contains("a=rtcp-fb:96 nack\r\n"));
    assert!(consumer_section.contains("a=rtpmap:97 rtx/90000\r\n"));
    assert!(consumer_section.contains("a=fmtp:97 apt=96\r\n"));
    let fid_pair = fid_pair_for_mid(&renegotiation_sdp, &renegotiated_mid)
        .expect("consumer offer should signal the str0m transmit pair");
    assert_ne!(fid_pair.0, 82_000);

    apply_offer_answer(&adapter, &consumer_key, &mut remote, renegotiation_sdp).await;

    assert_eq!(
        adapter
            .debug_session_stream_tx_pair(&consumer_key, renegotiated_mid)
            .await,
        Some((fid_pair.0, Some(fid_pair.1))),
        "consumer FID offer should match the str0m transmit stream"
    );
}

#[tokio::test]
async fn rtc_session_answer_releases_declined_consumer_without_follow_up_offer() {
    let adapter = RtcWorker::default();
    let src_key = transport_key(1, 49, UserId::Integer(49));
    let consumer_key = transport_key(1, 50, UserId::Integer(50));

    let _offer = expect_initial_offer(&adapter, &src_key).await;
    let source_media_id = adapter
        .add_recv_media(
            &src_key,
            Str0mMediaKind::Video,
            &sample_router_rtp_parameters("declined-source", 93_000),
        )
        .await
        .expect("source media should register");
    let mut remote = complete_initial_offer_answer(&adapter, &consumer_key, 55_010).await;
    let consumer_media_id = adapter
        .add_send_media(
            &consumer_key,
            Str0mMediaKind::Video,
            TransportSourceKey::new(src_key, source_media_id),
            &sample_router_rtp_parameters("declined-consumer", 94_000),
            true,
        )
        .await
        .expect("consumer media should stage an addition offer");
    let consumer_mid = adapter
        .debug_resolve_mid(consumer_media_id)
        .await
        .expect("consumer media should expose its staged mid");
    let addition_offer = adapter
        .create_session_renegotiation_offer(&consumer_key)
        .await
        .expect("addition offer should be available");
    let answer = remote
        .sdp_api()
        .accept_offer(
            SdpOffer::from_sdp_string(&addition_offer.into_parts().0)
                .expect("addition offer should parse"),
        )
        .expect("remote answer should build")
        .to_sdp_string();
    let declined_answer =
        answer_with_mid_direction(&answer, &consumer_mid, sdp::direction::INACTIVE);

    let applied = adapter
        .apply_session_answer(&consumer_key, &declined_answer)
        .await
        .expect("declined consumer answer should apply");
    assert_eq!(applied.declined_consumers(), &[consumer_media_id]);

    assert_eq!(
        adapter.debug_route_entry_by_media_id(source_media_id).await,
        None
    );
    assert_eq!(
        adapter
            .debug_session_stream_tx_pair(&consumer_key, consumer_mid)
            .await,
        None
    );
    assert_eq!(
        adapter.remove_media(&consumer_key, consumer_media_id).await,
        Ok(())
    );
    assert_eq!(adapter.debug_resolve_mid(consumer_media_id).await, None);
    assert_eq!(
        adapter
            .create_session_renegotiation_offer(&consumer_key)
            .await,
        Err(TransportAdapterError::UnsupportedFeature)
    );
}

#[tokio::test]
async fn rtc_session_renegotiation_offer_stages_negotiated_consumer_removal() {
    let adapter = RtcWorker::default();
    let src_key = transport_key(1, 39, UserId::Integer(39));
    let consumer_key = transport_key(1, 40, UserId::Integer(40));

    let _offer = expect_initial_offer(&adapter, &src_key).await;
    let source_media_id = adapter
        .add_recv_media(
            &src_key,
            Str0mMediaKind::Video,
            &sample_router_rtp_parameters("source-up-remove", 83_000),
        )
        .await
        .expect("source media should register");

    let mut remote = complete_initial_offer_answer(&adapter, &consumer_key, 55_004).await;

    let consumer_media_id = adapter
        .add_send_media(
            &consumer_key,
            Str0mMediaKind::Video,
            TransportSourceKey::new(src_key.clone(), source_media_id),
            &sample_router_rtp_parameters("compat-mid-remove", 84_000),
            true,
        )
        .await
        .expect("protocol consumer media should stage a renegotiation offer");
    let consumer_mid = adapter
        .debug_resolve_mid(consumer_media_id)
        .await
        .expect("consumer media should expose its staged mid");

    let addition_offer = adapter
        .create_session_renegotiation_offer(&consumer_key)
        .await
        .expect("staged addition offer should be available");
    apply_offer_answer(
        &adapter,
        &consumer_key,
        &mut remote,
        addition_offer.into_parts().0,
    )
    .await;

    assert!(
        adapter
            .remove_media(&consumer_key, consumer_media_id)
            .await
            .is_ok()
    );
    assert_eq!(
        adapter.debug_route_entry_by_media_id(source_media_id).await,
        None
    );

    let removal_offer = adapter
        .create_session_renegotiation_offer(&consumer_key)
        .await
        .expect("removal should stage a renegotiation offer");
    let removal_sdp = removal_offer.into_parts().0;
    let removal_section = media_section_for_mid(&removal_sdp, &consumer_mid)
        .expect("removed consumer mid should remain in the renegotiation offer");
    assert!(removal_section.contains("a=inactive"));

    apply_offer_answer(&adapter, &consumer_key, &mut remote, removal_sdp).await;
    assert_eq!(
        adapter
            .create_session_renegotiation_offer(&consumer_key)
            .await,
        Err(TransportAdapterError::UnsupportedFeature)
    );
}

#[tokio::test]
async fn rtc_session_renegotiation_offer_stages_negotiated_producer_removal() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(1, 46, UserId::Integer(46));

    let mut remote = complete_initial_offer_answer(&adapter, &session_key, 55_007).await;

    let (producer_media_id, producer_mid) = add_negotiated_producer_media(
        &adapter,
        &session_key,
        "compat-producer-mid-remove",
        rfc_rtp::RTP_VIDEO_CLOCK_RATE_HZ,
        &mut remote,
    )
    .await;

    assert!(
        adapter
            .remove_media(&session_key, producer_media_id)
            .await
            .is_ok()
    );

    let removal_offer = adapter
        .create_session_renegotiation_offer(&session_key)
        .await
        .expect("removal should stage a renegotiation offer");
    let removal_sdp = removal_offer.into_parts().0;
    let removal_section = media_section_for_mid(&removal_sdp, &producer_mid)
        .expect("removed producer mid should remain in the renegotiation offer");
    assert!(removal_section.contains("a=inactive"));

    apply_offer_answer(&adapter, &session_key, &mut remote, removal_sdp).await;
    assert_eq!(
        adapter
            .negotiated_producer_parameters(&session_key, producer_media_id)
            .await,
        Err(TransportAdapterError::TransportUnavailable)
    );
    assert_eq!(
        adapter
            .create_session_renegotiation_offer(&session_key)
            .await,
        Err(TransportAdapterError::UnsupportedFeature)
    );
}

#[tokio::test]
async fn rtc_session_renegotiation_stages_follow_up_removal_for_cancelled_pending_producer() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(1, 47, UserId::Integer(47));

    let mut remote = complete_initial_offer_answer(&adapter, &session_key, 55_008).await;

    let producer_media_id = adapter
        .add_recv_media(
            &session_key,
            Str0mMediaKind::Video,
            &sample_router_rtp_parameters("compat-producer-mid-cancel", 91_000),
        )
        .await
        .expect("protocol producer media should stage an addition offer");
    let producer_mid = adapter
        .debug_resolve_mid(producer_media_id)
        .await
        .expect("producer media should expose its staged mid");
    let addition_offer = adapter
        .create_session_renegotiation_offer(&session_key)
        .await
        .expect("addition offer should be available");
    let addition_sdp = addition_offer.into_parts().0;

    assert!(
        adapter
            .remove_media(&session_key, producer_media_id)
            .await
            .is_ok()
    );
    assert_eq!(
        adapter
            .create_session_renegotiation_offer(&session_key)
            .await,
        Err(TransportAdapterError::InvalidInput)
    );

    apply_offer_answer(&adapter, &session_key, &mut remote, addition_sdp).await;

    let removal_offer = adapter
        .create_session_renegotiation_offer(&session_key)
        .await
        .expect("cancelled pending producer should stage a follow-up removal offer");
    let removal_sdp = removal_offer.into_parts().0;
    let removal_section = media_section_for_mid(&removal_sdp, &producer_mid)
        .expect("cancelled producer mid should remain in the follow-up offer");
    assert!(removal_section.contains("a=inactive"));
    assert_eq!(
        adapter
            .negotiated_producer_parameters(&session_key, producer_media_id)
            .await,
        Err(TransportAdapterError::TransportUnavailable)
    );

    apply_offer_answer(&adapter, &session_key, &mut remote, removal_sdp).await;
    assert_eq!(
        adapter
            .create_session_renegotiation_offer(&session_key)
            .await,
        Err(TransportAdapterError::UnsupportedFeature)
    );
}

#[tokio::test]
async fn rtc_session_cleanup_releases_declined_staged_producer_without_follow_up_offer() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(1, 48, UserId::Integer(48));

    let mut remote = complete_initial_offer_answer(&adapter, &session_key, 55_009).await;

    let producer_media_id = adapter
        .add_recv_media(
            &session_key,
            Str0mMediaKind::Video,
            &sample_router_rtp_parameters("compat-producer-mid-declined", 92_000),
        )
        .await
        .expect("protocol producer media should stage an addition offer");
    let producer_mid = adapter
        .debug_resolve_mid(producer_media_id)
        .await
        .expect("producer media should expose its staged mid");
    let addition_offer = adapter
        .create_session_renegotiation_offer(&session_key)
        .await
        .expect("addition offer should be available");
    let answer = remote
        .sdp_api()
        .accept_offer(
            SdpOffer::from_sdp_string(&addition_offer.into_parts().0)
                .expect("addition offer should parse"),
        )
        .expect("remote answer should build")
        .to_sdp_string();
    let declined_answer = answer_with_mid_direction(&answer, &producer_mid, "inactive");

    let applied_answer = adapter
        .apply_session_answer(&session_key, &declined_answer)
        .await
        .expect("declined producer answer should apply");

    assert!(
        applied_answer
            .negotiated_producer_parameters(producer_media_id)
            .is_none()
    );
    assert_eq!(
        adapter.remove_media(&session_key, producer_media_id).await,
        Ok(())
    );
    assert_eq!(
        adapter
            .negotiated_producer_parameters(&session_key, producer_media_id)
            .await,
        Err(TransportAdapterError::TransportUnavailable)
    );
    assert_eq!(
        adapter
            .create_session_renegotiation_offer(&session_key)
            .await,
        Err(TransportAdapterError::UnsupportedFeature)
    );
}

#[tokio::test]
async fn rtc_session_renegotiation_queues_consumer_removal_while_answer_is_pending() {
    let adapter = RtcWorker::default();
    let src_key = transport_key(1, 42, UserId::Integer(42));
    let consumer_key = transport_key(1, 43, UserId::Integer(43));

    let (first_source_media_id, second_source_media_id) =
        setup_queued_removal_sources(&adapter, &src_key).await;

    let mut remote = complete_initial_offer_answer(&adapter, &consumer_key, 55_005).await;

    let (first_consumer_media_id, first_consumer_mid) = add_negotiated_consumer_media(
        &adapter,
        &consumer_key,
        &src_key,
        first_source_media_id,
        "compat-mid-queued-remove-a",
        87_000,
        &mut remote,
    )
    .await;

    let _second_consumer_media_id = adapter
        .add_send_media(
            &consumer_key,
            Str0mMediaKind::Video,
            TransportSourceKey::new(src_key.clone(), second_source_media_id),
            &sample_router_rtp_parameters("compat-mid-queued-remove-b", 88_000),
            true,
        )
        .await
        .expect("second protocol consumer media should stage an addition offer");
    let second_addition_offer = adapter
        .create_session_renegotiation_offer(&consumer_key)
        .await
        .expect("second addition offer should be available");
    let second_addition_sdp = second_addition_offer.into_parts().0;

    assert!(
        adapter
            .remove_media(&consumer_key, first_consumer_media_id)
            .await
            .is_ok()
    );
    assert_eq!(
        adapter
            .debug_route_entry_by_media_id(first_source_media_id)
            .await,
        None
    );
    assert_eq!(
        adapter
            .create_session_renegotiation_offer(&consumer_key)
            .await,
        Err(TransportAdapterError::InvalidInput)
    );

    apply_offer_answer(&adapter, &consumer_key, &mut remote, second_addition_sdp).await;

    let queued_removal_offer = adapter
        .create_session_renegotiation_offer(&consumer_key)
        .await
        .expect("queued removal should stage after the in-flight answer lands");
    let queued_removal_sdp = queued_removal_offer.into_parts().0;
    let removal_section = media_section_for_mid(&queued_removal_sdp, &first_consumer_mid)
        .expect("queued removal mid should remain in the follow-up offer");
    assert!(removal_section.contains("a=inactive"));

    apply_offer_answer(&adapter, &consumer_key, &mut remote, queued_removal_sdp).await;
    assert_eq!(
        adapter
            .create_session_renegotiation_offer(&consumer_key)
            .await,
        Err(TransportAdapterError::UnsupportedFeature)
    );
}

#[tokio::test]
async fn rtc_session_renegotiation_offer_stays_blocked_after_initial_answer() {
    let adapter = RtcWorker::default();
    let session_key = transport_key(1, 41, UserId::Integer(41));

    complete_initial_offer_answer(&adapter, &session_key, 55_003).await;
    assert_eq!(
        adapter
            .create_session_renegotiation_offer(&session_key)
            .await,
        Err(TransportAdapterError::UnsupportedFeature)
    );
}

fn media_section_for_mid<'a>(sdp: &'a str, mid: &str) -> Option<&'a str> {
    let marker = format!("{}{}{}{mid}", sdp::ATTR, sdp::attribute::MID, sdp::ATTR_SEP);
    let media_boundary = format!("{}{}", sdp::CRLF, sdp::MEDIA);
    let marker_start = sdp.find(&marker)?;
    let section_start = sdp[..marker_start]
        .rfind(&media_boundary)
        .map_or(0, |index| index + sdp::CRLF.len());
    let section_end = sdp[marker_start..]
        .find(&media_boundary)
        .map_or(sdp.len(), |offset| marker_start + offset + sdp::CRLF.len());
    Some(&sdp[section_start..section_end])
}

fn answer_with_mid_direction(answer_sdp: &str, mid: &str, direction: &str) -> String {
    let Some(section) = media_section_for_mid(answer_sdp, mid) else {
        return answer_sdp.to_owned();
    };
    let next_direction = format!("{}{direction}", sdp::ATTR);
    let mut updated_section = section.to_owned();
    for direction in [
        sdp::direction::SEND_RECV,
        sdp::direction::SEND_ONLY,
        sdp::direction::RECV_ONLY,
        sdp::direction::INACTIVE,
    ] {
        let current_direction = format!("{}{direction}", sdp::ATTR);
        if updated_section.contains(&current_direction) {
            updated_section = updated_section.replacen(&current_direction, &next_direction, 1);
            break;
        }
    }
    answer_sdp.replacen(section, &updated_section, 1)
}

fn answer_with_extmap_id(answer_sdp: &str, mid: &str, uri: &str, id: u8) -> String {
    let section = media_section_for_mid(answer_sdp, mid)
        .expect("test answer should contain the target MID section");
    let extmap_prefix = format!("{}{}{}", sdp::ATTR, sdp::attribute::EXTMAP, sdp::ATTR_SEP);
    let extmap = section
        .lines()
        .find(|line| {
            line.strip_prefix(&extmap_prefix)
                .and_then(|value| value.split_ascii_whitespace().nth(1))
                == Some(uri)
        })
        .expect("test answer section should contain the target extmap URI");
    let (_, value) = extmap
        .split_once(sdp::SP)
        .expect("test extmap line should separate id and URI");
    let replacement = format!("{extmap_prefix}{id}{}{value}", sdp::SP);
    let updated_section = section.replacen(extmap, &replacement, 1);
    answer_sdp.replacen(section, &updated_section, 1)
}

fn fid_pair_for_mid(answer_sdp: &str, mid: &str) -> Option<(u32, u32)> {
    let section = media_section_for_mid(answer_sdp, mid)?;
    let fid_prefix = format!(
        "{}{}{}{}{}",
        sdp::ATTR,
        sdp::attribute::SSRC_GROUP,
        sdp::ATTR_SEP,
        sdp::ssrc_group_semantics::FID,
        sdp::SP,
    );
    section.lines().find_map(|line| {
        let mut ssrcs = line.strip_prefix(&fid_prefix)?.split_ascii_whitespace();
        Some((ssrcs.next()?.parse().ok()?, ssrcs.next()?.parse().ok()?))
    })
}

fn answer_with_leading_fid_pair(answer_sdp: &str, mid: &str) -> String {
    let section = media_section_for_mid(answer_sdp, mid)
        .expect("test answer should contain the target MID section");
    let ssrc_prefix = format!("{}{}{}", sdp::ATTR, sdp::attribute::SSRC, sdp::ATTR_SEP);
    let ssrc_start = section
        .find(&ssrc_prefix)
        .expect("test answer section should signal an SSRC pair");
    let mut updated_section = section.to_owned();
    updated_section.insert_str(
        ssrc_start,
        concat!(
            "a=ssrc:4200001 cname:reordered\r\n",
            "a=ssrc:4200002 cname:reordered\r\n",
            "a=ssrc-group:FID 4200001 4200002\r\n",
        ),
    );
    answer_sdp.replacen(section, &updated_section, 1)
}

fn assert_default_vp8_upload_slot(upload_slots: &[SessionUploadSlot]) {
    let Some(video_upload_slot) = upload_slots
        .iter()
        .find(|slot| slot.kind == RouterMediaKind::Video)
    else {
        panic!("protocol publish should advertise a video upload slot");
    };
    assert!(
        video_upload_slot
            .codecs
            .iter()
            .any(|codec| codec.as_str() == rfc_rtp::codec_name::VP8),
        "empty protocol publish intent should use the server-defined VP8 upload profile"
    );
    assert_eq!(
        video_upload_slot
            .simulcast_encodings
            .iter()
            .map(|encoding| (
                encoding.rid.as_str(),
                encoding.max_bitrate,
                encoding.resolution_scale,
            ))
            .collect::<Vec<_>>(),
        vec![
            ("lo", Some(Bitrate::from_kbps(150)), Some(4)),
            ("mid", Some(Bitrate::from_kbps(800)), Some(2)),
            ("hi", Some(Bitrate::from_mbps(4)), Some(1))
        ]
    );
}

fn sdp_rid_line(rid: &str, direction: &str, max_bitrate: Option<u64>) -> String {
    let mut line = format!(
        "{}{}{}{}{}{}",
        sdp::ATTR,
        sdp::attribute::RID,
        sdp::ATTR_SEP,
        rid,
        sdp::SP,
        direction,
    );
    if let Some(max_bitrate) = max_bitrate {
        line.push(sdp::SP);
        line.push_str(sdp::rid_restriction::MAX_BITRATE);
        line.push(sdp::rid_restriction::NAME_VALUE_SEPARATOR);
        line.push_str(&max_bitrate.to_string());
    }
    line
}

fn sdp_simulcast_line(direction: &str, rids: &[&str]) -> String {
    format!(
        "{}{}{}{}{}{}",
        sdp::ATTR,
        sdp::attribute::SIMULCAST,
        sdp::ATTR_SEP,
        direction,
        sdp::SP,
        rids.join(&sdp::simulcast::STREAM_SEPARATOR.to_string())
    )
}

fn without_vp8_repair(sdp: &str) -> String {
    sdp.replace(" 96 97", " 96")
        .replace("a=rtcp-fb:96 nack\r\n", "")
        .replace("a=rtpmap:97 rtx/90000\r\n", "")
        .replace("a=fmtp:97 apt=96\r\n", "")
}

fn answer_with_simulcast_send_rids(
    answer_sdp: &str,
    mid: &str,
    rids: &[(&str, Option<u64>)],
) -> String {
    let marker = format!(
        "{}{}{}{mid}{}",
        sdp::ATTR,
        sdp::attribute::MID,
        sdp::ATTR_SEP,
        sdp::CRLF,
    );
    let mut replacement = marker.clone();
    for (rid, max_bitrate) in rids {
        replacement.push_str(&sdp_rid_line(rid, sdp::rid::DIRECTION_SEND, *max_bitrate));
        replacement.push_str(sdp::CRLF);
    }
    let rid_values = rids
        .iter()
        .map(|(rid, _max_bitrate)| *rid)
        .collect::<Vec<_>>();
    replacement.push_str(&sdp_simulcast_line(
        sdp::simulcast::DIRECTION_SEND,
        &rid_values,
    ));
    replacement.push_str(sdp::CRLF);
    answer_sdp.replacen(&marker, &replacement, 1)
}

fn build_remote_rtc(port: u16) -> Rtc {
    let mut remote = Rtc::new(Instant::now());
    remote
        .add_local_candidate(
            Candidate::host(
                SocketAddr::from(([127, 0, 0, 1], port)),
                webrtc::ice::transport::UDP,
            )
            .expect("test host candidate should build"),
        )
        .expect("remote candidate should register");
    remote
}

async fn complete_initial_offer_answer(
    adapter: &RtcWorker,
    session_key: &TransportSessionKey,
    port: u16,
) -> Rtc {
    let initial_offer = expect_initial_offer(adapter, session_key).await;
    let mut remote = build_remote_rtc(port);
    apply_offer_answer(
        adapter,
        session_key,
        &mut remote,
        initial_offer.into_parts().0,
    )
    .await;
    remote
}

fn reduced_capability_probe_rtc() -> Rtc {
    let mut config = Rtc::builder().clear_codecs();
    config.codec_config().add_config(
        111.into(),
        None,
        Codec::Opus,
        Frequency::FORTY_EIGHT_KHZ,
        Some(2),
        FormatParams {
            use_inband_fec: Some(true),
            ..Default::default()
        },
    );
    config.codec_config().add_config(
        96.into(),
        None,
        Codec::Vp8,
        Frequency::NINETY_KHZ,
        None,
        FormatParams::default(),
    );
    config
        .codec_config()
        .last_mut()
        .expect("reduced video capability should exist")
        .set_fb_nack(false);
    config.build(Instant::now())
}

async fn apply_offer_answer(
    adapter: &RtcWorker,
    session_key: &TransportSessionKey,
    remote: &mut Rtc,
    offer_sdp: String,
) {
    let offer =
        SdpOffer::from_sdp_string(&offer_sdp).expect("adapter should return parseable SDP offer");
    assert!(offer.session.end_of_candidates());
    let answer = remote
        .sdp_api()
        .accept_offer(offer)
        .expect("remote answer should build");
    let answer_sdp = answer.to_sdp_string();
    assert!(
        adapter
            .apply_session_answer(session_key, &answer_sdp)
            .await
            .is_ok(),
        "{answer_sdp}"
    );
}

async fn setup_queued_removal_sources(
    adapter: &RtcWorker,
    src_key: &TransportSessionKey,
) -> (TransportMediaId, TransportMediaId) {
    assert!(
        adapter
            .create_initial_session_offer("test-room", src_key)
            .await
            .is_ok()
    );
    let first_source_media_id = adapter
        .add_recv_media(
            src_key,
            Str0mMediaKind::Video,
            &sample_router_rtp_parameters("source-up-queued-remove-a", 85_000),
        )
        .await
        .expect("first source media should register");
    let second_source_media_id = adapter
        .add_recv_media(
            src_key,
            Str0mMediaKind::Video,
            &sample_router_rtp_parameters("source-up-queued-remove-b", 86_000),
        )
        .await
        .expect("second source media should register");
    (first_source_media_id, second_source_media_id)
}

async fn add_negotiated_consumer_media(
    adapter: &RtcWorker,
    consumer_key: &TransportSessionKey,
    src_key: &TransportSessionKey,
    source_media_id: TransportMediaId,
    mid: &str,
    ssrc: u32,
    remote: &mut Rtc,
) -> (TransportMediaId, Mid) {
    let consumer_media_id = adapter
        .add_send_media(
            consumer_key,
            Str0mMediaKind::Video,
            TransportSourceKey::new(src_key.clone(), source_media_id),
            &sample_router_rtp_parameters(mid, ssrc),
            true,
        )
        .await
        .expect("protocol consumer media should stage an addition offer");
    let consumer_mid = adapter
        .debug_resolve_mid(consumer_media_id)
        .await
        .expect("consumer media should expose its staged mid");
    let addition_offer = adapter
        .create_session_renegotiation_offer(consumer_key)
        .await
        .expect("addition offer should be available");
    apply_offer_answer(adapter, consumer_key, remote, addition_offer.into_parts().0).await;
    (consumer_media_id, consumer_mid)
}

async fn add_negotiated_producer_media(
    adapter: &RtcWorker,
    session_key: &TransportSessionKey,
    mid: &str,
    ssrc: u32,
    remote: &mut Rtc,
) -> (TransportMediaId, Mid) {
    let producer_media_id = adapter
        .add_recv_media(
            session_key,
            Str0mMediaKind::Video,
            &sample_router_rtp_parameters(mid, ssrc),
        )
        .await
        .expect("protocol producer media should stage an addition offer");
    let producer_mid = adapter
        .debug_resolve_mid(producer_media_id)
        .await
        .expect("producer media should expose its staged mid");
    let addition_offer = adapter
        .create_session_renegotiation_offer(session_key)
        .await
        .expect("addition offer should be available");
    apply_offer_answer(adapter, session_key, remote, addition_offer.into_parts().0).await;
    (producer_media_id, producer_mid)
}
