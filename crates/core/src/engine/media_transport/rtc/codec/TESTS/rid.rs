use std::fmt::Write as _;

use o_sfu_rfc::webrtc::sdp;
use o_sfu_router::rtp::{MediaStream, StreamBinding};
use str0m::media::Mid;

use super::*;
use crate::Bitrate;

const HIGH_MAX_BITRATE: Bitrate = Bitrate::from_kbps(900);

#[test]
fn default_upload_ladder_respects_video_cap() {
    for (cap, expected) in [
        (0, [0, 0, 0]),
        (100_000, [100_000, 100_000, 100_000]),
        (150_000, [150_000, 150_000, 150_000]),
        (500_000, [150_000, 150_000, 500_000]),
        (1_000_000, [150_000, 200_000, 1_000_000]),
        (4_000_000, [150_000, 800_000, 4_000_000]),
    ] {
        let layers = default_layers(VideoBitrateLimits::new(Bitrate::from_bps(cap)));
        assert_eq!(layers.map(|layer| layer.rid), ["lo", "mid", "hi"]);
        assert_eq!(
            layers.map(|layer| layer.max_bitrate),
            expected.map(|rate| Some(Bitrate::from_bps(rate))),
        );
    }
}

fn send_rids(
    answer_sdp: &str,
    offered_encodings: &[SessionUploadEncoding],
) -> Result<Vec<SessionUploadEncoding>, SimulcastAnswerError> {
    accepted_encodings(&parse_section_rids(answer_sdp)?, offered_encodings)
}

#[test]
fn answer_keeps_offered_encodings_in_simulcast_order() {
    let answer = answer("video_0", &["hi", "lo"], Some("lo;hi"));

    assert_eq!(
        send_rids(&answer, &default_upload_encodings()),
        Ok(default_upload_encodings())
    );
}

#[test]
fn answer_keeps_only_accepted_simulcast_rids() {
    let answer = answer("video_0", &["lo", "hi"], Some("lo"));

    assert_eq!(
        send_rids(&answer, &default_upload_encodings()),
        Ok(default_upload_encodings()[..1].to_vec())
    );
}

#[test]
fn answer_rejects_rid_restrictions() {
    for restriction in [
        "max-br=150000",
        "max-br=149999",
        "max-br",
        "pt=96",
        "max-br=150000;max-width=640",
    ] {
        // The restricted RID is rejected whether or not the answer selects it.
        for simulcast in [Some("lo;hi"), Some("hi"), None] {
            let unrestricted = answer("video_0", &["lo", "hi"], simulcast);
            let restricted = unrestricted.replacen(
                "a=rid:lo send\r\n",
                &format!("a=rid:lo send {restriction}\r\n"),
                1,
            );

            assert!(
                parse_answer(&unrestricted).is_ok(),
                "simulcast {simulcast:?} without restriction"
            );
            assert_eq!(
                parse_answer(&restricted).err(),
                Some(SimulcastAnswerError),
                "{restriction} with simulcast {simulcast:?}"
            );
        }
    }
}

#[test]
fn answer_rejects_duplicate_rid_declaration() {
    let answer = answer("video_0", &["lo", "lo"], Some("lo"));

    assert_eq!(
        send_rids(&answer, &custom_upload_encodings("lo", Some(150_000))),
        Err(SimulcastAnswerError)
    );
}

#[test]
fn answer_rejects_rid_prefix_alias() {
    let answer = answer("video_0", &["abcdefghX"], Some("abcdefghX"));

    assert_eq!(
        send_rids(&answer, &custom_upload_encodings("abcdefgh", None)),
        Err(SimulcastAnswerError)
    );
}

#[test]
fn answer_requires_accepted_simulcast_send_list() {
    let answer = answer("video_0", &["lo", "hi"], None);

    assert!(matches!(
        send_rids(&answer, &default_upload_encodings()),
        Ok(rids) if rids.is_empty()
    ));
}

#[test]
fn answer_rejects_multiple_simulcast_attributes() {
    let mut answer = answer("video_0", &["lo", "hi"], Some("lo"));
    answer.push_str("a=simulcast:send hi\r\n");

    assert_eq!(
        send_rids(&answer, &default_upload_encodings()),
        Err(SimulcastAnswerError)
    );
}

#[test]
fn answer_rejects_simulcast_alternatives() {
    let send = format!(
        "{pause}lo{alternative}backup{separator}hi",
        pause = sdp::simulcast::INITIAL_PAUSE_PREFIX,
        alternative = sdp::simulcast::ALTERNATIVE_SEPARATOR,
        separator = sdp::simulcast::STREAM_SEPARATOR,
    );
    let answer = answer("video_0", &["lo", "backup", "hi"], Some(&send));

    assert_eq!(
        send_rids(&answer, &default_upload_encodings()),
        Err(SimulcastAnswerError)
    );
}

#[test]
fn answer_matches_exact_mid_section() -> Result<(), SimulcastAnswerError> {
    let answer_sdp = format!(
        concat!(
            "v=0\r\n",
            "o=- 1 1 IN IP4 0.0.0.0\r\n",
            "s=-\r\n",
            "t=0 0\r\n",
            "a=group:BUNDLE camera cam\r\n",
            "a=mid:cam\r\n",
            "m=video 9 UDP/TLS/RTP/SAVPF 96\r\n",
            "a=mid:camera\r\n",
            "a=sendonly\r\n",
            "a=rtpmap:96 VP8/90000\r\n",
            "a={rid}:wrong {send}\r\n",
            "a={simulcast}:{send} wrong\r\n",
            "m=video 9 UDP/TLS/RTP/SAVPF 96\r\n",
            "a=mid:cam\r\n",
            "a=sendonly\r\n",
            "a=rtpmap:96 VP8/90000\r\n",
            "a={rid}:right {send}\r\n",
            "a={simulcast}:{send} right\r\n"
        ),
        rid = sdp::attribute::RID,
        simulcast = sdp::attribute::SIMULCAST,
        send = sdp::rid::DIRECTION_SEND,
    );
    let rids = parse_answer(&answer_sdp)?;
    let right = custom_upload_encodings("right", Some(222_000));
    let wrong = custom_upload_encodings("wrong", Some(111_000));

    assert_eq!(rids.negotiate(Mid::from("cam"), &right), Ok(right.clone()));
    assert_eq!(
        rids.negotiate(Mid::from("camera"), &wrong),
        Ok(wrong.clone())
    );
    assert_eq!(
        rids.negotiate(Mid::from("cam"), &wrong),
        Err(SimulcastAnswerError)
    );
    Ok(())
}

#[test]
fn answer_rejects_invalid_rfc8852_ids() {
    let send = format!(
        "low-1{separator}hi_2{separator}hi2",
        separator = sdp::simulcast::STREAM_SEPARATOR,
    );
    let answer = answer("video_0", &["low-1", "hi_2", "hi2"], Some(&send));

    assert_eq!(
        send_rids(&answer, &default_upload_encodings()),
        Err(SimulcastAnswerError)
    );
}

#[test]
fn three_rid_answer_accepts_offered_streams_and_subsets() {
    let mut offered_encodings = default_upload_encodings();
    offered_encodings.insert(
        1,
        SessionUploadEncoding {
            rid: "mid".to_owned(),
            max_bitrate: Some(Bitrate::from_kbps(450)),
            resolution_scale: Some(2),
            max_framerate: None,
        },
    );
    for accepted in [
        &["lo", "mid", "hi"][..],
        &["lo", "hi"],
        &["mid", "hi"],
        &["mid"],
    ] {
        let answer = answer("video_0", &["lo", "mid", "hi"], Some(&accepted.join(";")));
        let expected = offered_encodings
            .iter()
            .filter(|encoding| accepted.contains(&encoding.rid.as_str()))
            .cloned()
            .collect();
        assert_eq!(send_rids(&answer, &offered_encodings), Ok(expected));
    }
}

#[test]
fn answer_rejects_unoffered_simulcast_stream() {
    let send = format!(
        "lo{separator}mid{separator}hi",
        separator = sdp::simulcast::STREAM_SEPARATOR,
    );
    let answer = answer("video_0", &["lo", "mid", "hi"], Some(&send));

    assert_eq!(
        send_rids(&answer, &default_upload_encodings()),
        Err(SimulcastAnswerError)
    );
}

#[test]
fn answer_rejects_more_than_three_simulcast_streams() {
    assert_eq!(
        send_simulcast_stream_count("send lo;mid;hi;extra"),
        Err(SimulcastAnswerError),
    );
}

#[test]
fn initial_gate_selects_lowest_bitrate_rid() {
    let parameters = MediaStream::new(
        Vec::new(),
        Vec::new(),
        vec![
            StreamBinding::new()
                .with_rid("hi")
                .with_max_bitrate(4_000_000),
            StreamBinding::new()
                .with_rid("lo")
                .with_max_bitrate(150_000),
        ],
    );

    assert_eq!(
        initial_packet_gate(&parameters),
        PacketLayerGate::Rid("lo".into())
    );
}

#[test]
fn initial_gate_uses_declared_order_without_bitrates() {
    let parameters = MediaStream::new(
        Vec::new(),
        Vec::new(),
        vec![
            StreamBinding::new().with_rid("lo"),
            StreamBinding::new().with_rid("hi"),
        ],
    );

    assert_eq!(
        initial_packet_gate(&parameters),
        PacketLayerGate::Rid("lo".into())
    );
}

#[test]
fn initial_gate_keeps_ridless_or_mixed_routes_open() {
    let ridless = MediaStream::new(Vec::new(), Vec::new(), vec![StreamBinding::new()]);
    let mixed = MediaStream::new(
        Vec::new(),
        Vec::new(),
        vec![
            StreamBinding::new().with_rid("lo"),
            StreamBinding::new().with_ssrc(72_002),
        ],
    );

    assert_eq!(initial_packet_gate(&ridless), PacketLayerGate::Open);
    assert_eq!(initial_packet_gate(&mixed), PacketLayerGate::Open);
}

fn parse_answer(answer_sdp: &str) -> Result<ParsedAnswerRids, SimulcastAnswerError> {
    let answer = SdpAnswer::from_sdp_string(answer_sdp).expect("test answer should parse");
    ParsedAnswerRids::parse(answer_sdp, &answer)
}

fn answer(mid: &str, send_rids: &[&str], simulcast: Option<&str>) -> String {
    let mut answer = format!(
        concat!(
            "v=0\r\n",
            "o=- 1 1 IN IP4 0.0.0.0\r\n",
            "s=-\r\n",
            "t=0 0\r\n",
            "m=video 9 UDP/TLS/RTP/SAVPF 96\r\n",
            "a=mid:{mid}\r\n",
            "a=sendonly\r\n",
            "a=rtpmap:96 VP8/90000\r\n",
        ),
        mid = mid,
    );
    for rid in send_rids {
        let _ = write!(
            answer,
            "a={attribute}:{rid} {send}\r\n",
            attribute = sdp::attribute::RID,
            send = sdp::rid::DIRECTION_SEND,
        );
    }
    if let Some(simulcast) = simulcast {
        let _ = write!(
            answer,
            "a={attribute}:{send} {simulcast}\r\n",
            attribute = sdp::attribute::SIMULCAST,
            send = sdp::rid::DIRECTION_SEND,
        );
    }
    answer
}

fn default_upload_encodings() -> Vec<SessionUploadEncoding> {
    vec![
        SessionUploadEncoding {
            rid: DEFAULT_LOW_RID.to_owned(),
            max_bitrate: Some(DEFAULT_LOW_MAX_BITRATE),
            resolution_scale: Some(4),
            max_framerate: None,
        },
        SessionUploadEncoding {
            rid: DEFAULT_HIGH_RID.to_owned(),
            max_bitrate: Some(HIGH_MAX_BITRATE),
            resolution_scale: Some(1),
            max_framerate: None,
        },
    ]
}

fn custom_upload_encodings(rid: &str, max_bitrate: Option<u64>) -> Vec<SessionUploadEncoding> {
    vec![SessionUploadEncoding {
        rid: rid.to_owned(),
        max_bitrate: max_bitrate.map(Bitrate::from_bps),
        resolution_scale: None,
        max_framerate: None,
    }]
}
