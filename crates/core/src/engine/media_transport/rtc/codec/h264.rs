//! H.264 policy for o-sfu's promoted simulcast matrix.
//!
//! Publication parameters promote RID simulcast only for packetization mode 1
//! with `profile-level-id=42e01f`. Encoded H.264 payloads remain opaque to
//! [`super::packet`].

use o_sfu_rfc::rtp::{self as rfc_rtp, h264::PacketizationMode};
use o_sfu_router::rtp::{CodecSetting, MediaFormat};

const CHROMIUM_PACKETIZATION_MODE: PacketizationMode = PacketizationMode::NonInterleaved;
const CHROMIUM_CONSTRAINED_BASELINE_PROFILE_LEVEL_ID: &str = "42e01f";

pub(super) fn is_promoted_format(format: &MediaFormat) -> bool {
    if format.codec() != &rfc_rtp::CodecName::H264 {
        return false;
    }
    let mut packetization_mode = None;
    let mut profile_level_id = None;
    for setting in format.settings() {
        match setting {
            CodecSetting::H264PacketizationMode(mode) => packetization_mode = Some(*mode),
            CodecSetting::H264ProfileLevelId(value) => profile_level_id = Some(value.as_str()),
            _ => {}
        }
    }
    packetization_mode == Some(CHROMIUM_PACKETIZATION_MODE)
        && profile_level_id.is_some_and(|value| {
            value.eq_ignore_ascii_case(CHROMIUM_CONSTRAINED_BASELINE_PROFILE_LEVEL_ID)
        })
}

#[cfg(test)]
#[path = "TESTS/h264.rs"]
mod tests;
