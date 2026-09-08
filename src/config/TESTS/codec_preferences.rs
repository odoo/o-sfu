use std::io;

use o_sfu_core::prelude::{AudioCodecPreference, CodecPreferences, VideoCodecPreference};

use super::{Env, load_codec_preferences};

#[test]
fn load_codec_preferences_accepts_partial_orders() {
    let preferences = load_codec_preferences(&Env::new(
        |key| match key {
            "CODEC_AUDIO_PREFERENCE" => Some("PCMU,opus".to_owned()),
            "CODEC_VIDEO_PREFERENCE" => Some("H264,VP9".to_owned()),
            _ => None,
        },
        |_| Err(io::Error::new(io::ErrorKind::NotFound, "file not found")),
    ));
    assert_eq!(
        preferences.ok(),
        Some(
            CodecPreferences::default()
                .with_audio_order(&[AudioCodecPreference::Pcmu, AudioCodecPreference::Opus])
                .with_video_order(&[VideoCodecPreference::H264, VideoCodecPreference::Vp9]),
        )
    );
}

#[test]
fn load_codec_preferences_rejects_unknown_codecs() {
    let error = load_codec_preferences(&Env::new(
        |key| match key {
            "CODEC_VIDEO_PREFERENCE" => Some("VP8,THEORA".to_owned()),
            _ => None,
        },
        |_| Err(io::Error::new(io::ErrorKind::NotFound, "file not found")),
    ))
    .err()
    .map(|error| error.to_string());

    assert_eq!(
        error.as_deref(),
        Some("CODEC_VIDEO_PREFERENCE contains unsupported codec `THEORA`")
    );
}

#[test]
fn load_codec_preferences_rejects_duplicates() {
    let error = load_codec_preferences(&Env::new(
        |key| match key {
            "CODEC_AUDIO_PREFERENCE" => Some("opus,OPUS".to_owned()),
            _ => None,
        },
        |_| Err(io::Error::new(io::ErrorKind::NotFound, "file not found")),
    ))
    .err()
    .map(|error| error.to_string());

    assert_eq!(
        error.as_deref(),
        Some("CODEC_AUDIO_PREFERENCE cannot contain duplicate codec `OPUS`")
    );
}
