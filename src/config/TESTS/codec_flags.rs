use super::{Env, MediaCodecFlags, load_media_codec_flags};

#[test]
fn load_media_codec_flags_applies_per_codec_overrides() {
    let flags = load_media_codec_flags(&Env::new(|key| match key {
        "CODEC_OPUS" => Some("false".to_owned()),
        "CODEC_H264" | "CODEC_AV1" => Some("true".to_owned()),
        _ => None,
    }));
    assert_eq!(
        flags.ok(),
        Some(
            MediaCodecFlags::default()
                .with_opus(false)
                .with_h264(true)
                .with_av1(true),
        )
    );
}

#[test]
fn load_media_codec_flags_rejects_invalid_bool() {
    let error = load_media_codec_flags(&Env::new(|key| match key {
        "CODEC_VP8" => Some("enabled".to_owned()),
        _ => None,
    }))
    .err()
    .map(|error| error.to_string());

    assert_eq!(
        error.as_deref(),
        Some("CODEC_VP8 must be either `true` or `false`")
    );
}
