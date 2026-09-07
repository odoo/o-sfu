use anyhow::{Result, anyhow, ensure};
use o_sfu_core::prelude::{AudioCodecPreference, CodecPreferences, VideoCodecPreference};

use super::env::Env;

pub(super) fn load_codec_preferences(env: &Env<'_>) -> Result<CodecPreferences> {
    let audio = match env.var::<String>("CODEC_AUDIO_PREFERENCE").optional()? {
        Some(value) => parse_codec_list(&value, "CODEC_AUDIO_PREFERENCE", audio_codec_preference)?,
        None => Vec::new(),
    };
    let video = match env.var::<String>("CODEC_VIDEO_PREFERENCE").optional()? {
        Some(value) => parse_codec_list(&value, "CODEC_VIDEO_PREFERENCE", video_codec_preference)?,
        None => Vec::new(),
    };
    Ok(CodecPreferences::default()
        .with_audio_order(&audio)
        .with_video_order(&video))
}

fn parse_codec_list<T>(
    value: &str,
    env_key: &str,
    parse_codec: impl Fn(&str) -> Option<T>,
) -> Result<Vec<T>>
where
    T: PartialEq,
{
    let mut codecs = Vec::new();
    for raw_codec in value.split(',') {
        let codec_name = raw_codec.trim();
        ensure!(
            !codec_name.is_empty(),
            "{env_key} cannot contain empty entries"
        );
        let codec = parse_codec(codec_name)
            .ok_or_else(|| anyhow!("{env_key} contains unsupported codec `{codec_name}`"))?;
        ensure!(
            !codecs.contains(&codec),
            "{env_key} cannot contain duplicate codec `{codec_name}`"
        );
        codecs.push(codec);
    }
    Ok(codecs)
}

fn audio_codec_preference(codec_name: &str) -> Option<AudioCodecPreference> {
    CodecPreferences::DEFAULT_AUDIO
        .into_iter()
        .find(|codec| codec_name.eq_ignore_ascii_case(codec.wire_name()))
}

fn video_codec_preference(codec_name: &str) -> Option<VideoCodecPreference> {
    CodecPreferences::DEFAULT_VIDEO
        .into_iter()
        .find(|codec| codec_name.eq_ignore_ascii_case(codec.wire_name()))
}

#[cfg(test)]
#[path = "TESTS/codec_preferences.rs"]
mod tests;
