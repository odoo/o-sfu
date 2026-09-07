use super::{Env, RuntimeFeatureFlags, load_runtime_feature_flags};

#[test]
fn load_runtime_feature_flags_accepts_explicit_flags() {
    let config = load_runtime_feature_flags(&Env::new(|key| match key {
        "FEATURE_TRANSCRIPTION" | "FEATURE_AUDIO_RECORDING" | "FEATURE_VIDEO_RECORDING" => {
            Some("true".to_owned())
        }
        _ => None,
    }));
    assert_eq!(
        config.ok(),
        Some(RuntimeFeatureFlags {
            transcription: true,
            audio_recording: true,
            video_recording: true,
        })
    );
}

#[test]
fn load_runtime_feature_flags_rejects_invalid_bool() {
    let error = load_runtime_feature_flags(&Env::new(|key| match key {
        "FEATURE_TRANSCRIPTION" => Some("enabled".to_owned()),
        _ => None,
    }))
    .err()
    .map(|error| error.to_string());

    assert_eq!(
        error.as_deref(),
        Some("FEATURE_TRANSCRIPTION must be either `true` or `false`")
    );
}
