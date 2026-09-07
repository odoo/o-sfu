use anyhow::Result;
use o_sfu_core::prelude::MediaCodecFlags;

use super::env::Env;

pub(super) fn load_media_codec_flags(env: &Env<'_>) -> Result<MediaCodecFlags> {
    Ok(MediaCodecFlags::empty()
        .with_opus(env.var("CODEC_OPUS").default(true)?)
        .with_pcmu(env.var("CODEC_PCMU").default(false)?)
        .with_pcma(env.var("CODEC_PCMA").default(false)?)
        .with_vp8(env.var("CODEC_VP8").default(true)?)
        .with_h264(env.var("CODEC_H264").default(false)?)
        .with_h265(env.var("CODEC_H265").default(false)?)
        .with_vp9(env.var("CODEC_VP9").default(false)?)
        .with_av1(env.var("CODEC_AV1").default(false)?))
}

#[cfg(test)]
#[path = "TESTS/codec_flags.rs"]
mod tests;
