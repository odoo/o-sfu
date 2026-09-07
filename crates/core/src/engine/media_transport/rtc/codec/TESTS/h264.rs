use o_sfu_rfc::rtp::{self as rfc_rtp, h264::PacketizationMode};
use o_sfu_router::{
    MediaKind,
    rtp::{CodecSetting, MediaFormat, MediaStream, PayloadType, StreamBinding},
};

use super::super::{SimulcastProfile, rid};
use crate::{Bitrate, VideoBitrateLimits, engine::media_transport::SessionUploadEncoding};

#[test]
fn profile_accepts_only_the_promoted_chromium_format() {
    let profile = SimulcastProfile::H264(VideoBitrateLimits::default());
    let parameters = h264_parameters(PacketizationMode::NonInterleaved, "42E01F");
    assert!(profile.recv_simulcast(Some(&parameters)).is_some());

    for parameters in [
        h264_parameters(PacketizationMode::SingleNalUnit, "42e01f"),
        h264_parameters(PacketizationMode::NonInterleaved, "42001f"),
        h264_parameters(PacketizationMode::NonInterleaved, "4d001f"),
        h264_parameters(PacketizationMode::NonInterleaved, "4de01f"),
    ] {
        assert!(profile.recv_simulcast(Some(&parameters)).is_none());
    }
}

#[test]
fn default_policy_advertises_three_rids_without_resolution_hints() {
    let profile = SimulcastProfile::H264(VideoBitrateLimits::default());
    let simulcast = profile.recv_simulcast(None);

    assert_eq!(
        profile.upload_encodings(None),
        vec![
            SessionUploadEncoding {
                rid: rid::DEFAULT_LOW_RID.to_owned(),
                max_bitrate: Some(rid::DEFAULT_LOW_MAX_BITRATE),
                resolution_scale: None,
                max_framerate: None,
            },
            SessionUploadEncoding {
                rid: rid::DEFAULT_MIDDLE_RID.to_owned(),
                max_bitrate: Some(Bitrate::from_kbps(800)),
                resolution_scale: None,
                max_framerate: None,
            },
            SessionUploadEncoding {
                rid: rid::DEFAULT_HIGH_RID.to_owned(),
                max_bitrate: Some(VideoBitrateLimits::default().max_video_bitrate()),
                resolution_scale: None,
                max_framerate: None,
            },
        ]
    );
    assert!(matches!(simulcast, Some(simulcast) if simulcast.recv.len() == 3));
}

#[test]
fn publication_policy_preserves_rid_bitrates_without_resolution_hints() {
    let parameters = MediaStream::new(
        h264_parameters(PacketizationMode::NonInterleaved, "42e01f")
            .formats()
            .cloned()
            .collect(),
        Vec::new(),
        vec![
            StreamBinding::new()
                .with_rid(rid::DEFAULT_LOW_RID)
                .with_max_bitrate(120_000),
            StreamBinding::new()
                .with_rid(rid::DEFAULT_MIDDLE_RID)
                .with_max_bitrate(400_000),
            StreamBinding::new()
                .with_rid(rid::DEFAULT_HIGH_RID)
                .with_max_bitrate(800_000),
        ],
    );
    let profile = SimulcastProfile::H264(VideoBitrateLimits::default());

    assert_eq!(
        profile.upload_encodings(Some(&parameters)),
        vec![
            SessionUploadEncoding {
                rid: rid::DEFAULT_LOW_RID.to_owned(),
                max_bitrate: Some(Bitrate::from_kbps(120)),
                resolution_scale: None,
                max_framerate: None,
            },
            SessionUploadEncoding {
                rid: rid::DEFAULT_MIDDLE_RID.to_owned(),
                max_bitrate: Some(Bitrate::from_kbps(400)),
                resolution_scale: None,
                max_framerate: None,
            },
            SessionUploadEncoding {
                rid: rid::DEFAULT_HIGH_RID.to_owned(),
                max_bitrate: Some(Bitrate::from_kbps(800)),
                resolution_scale: None,
                max_framerate: None,
            },
        ]
    );
    assert!(profile.recv_simulcast(Some(&parameters)).is_some());
}

fn h264_parameters(packetization_mode: PacketizationMode, profile_level_id: &str) -> MediaStream {
    MediaStream::new(
        vec![
            MediaFormat::new(
                MediaKind::Video,
                rfc_rtp::CodecName::H264,
                PayloadType::new(102),
                90_000,
            )
            .with_setting(CodecSetting::H264PacketizationMode(packetization_mode))
            .with_setting(CodecSetting::H264ProfileLevelId(
                profile_level_id.to_owned(),
            )),
        ],
        Vec::new(),
        vec![
            StreamBinding::new().with_rid(rid::DEFAULT_LOW_RID),
            StreamBinding::new().with_rid(rid::DEFAULT_HIGH_RID),
        ],
    )
}
