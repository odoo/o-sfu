use o_sfu_router::{MediaKind as RouterMediaKind, rtp::MediaCapabilities};
use str0m::{Rtc, RtcConfig, format::Codec, media::MediaKind, rtp::Extension};

use super::{capabilities, retransmission};
use crate::{
    AudioCodecPreference, CodecPreferences, MediaCodecFlags, VideoCodecPreference,
    engine::media_transport::TransportAdapterError,
};

#[derive(Debug)]
pub(in crate::engine::media_transport) struct RtpProfile {
    config: RtcConfig,
    router_capabilities: MediaCapabilities,
    audio_names: Vec<String>,
    video_names: Vec<String>,
    simulcast_codec: Option<Codec>,
}

impl RtpProfile {
    pub(in crate::engine::media_transport) fn compile(
        flags: MediaCodecFlags,
        preferences: CodecPreferences,
    ) -> Result<Self, TransportAdapterError> {
        let mut config = Rtc::builder()
            .clear_codecs()
            .clear_extension_map()
            .set_extension(1, Extension::AudioLevel)
            .set_extension(2, Extension::AbsoluteSendTime)
            .set_extension(3, Extension::TransportSequenceNumber)
            .set_extension(4, Extension::RtpMid)
            .set_extension(10, Extension::RtpStreamId)
            // RID-based redundancy must use RepairedRtpStreamId where applicable.
            // https://www.rfc-editor.org/rfc/rfc8851.html#section-4
            // https://www.rfc-editor.org/rfc/rfc8852.html#section-3.3
            .set_extension(11, Extension::RepairedRtpStreamId)
            .set_extension(13, Extension::VideoOrientation)
            .set_rtp_mode(true);
        let codecs = config.codec_config();
        for codec in preferences.audio_order() {
            if !codec.enabled_by(flags) {
                continue;
            }
            // Keep RED disabled to preserve the negotiated audio payload types.
            match codec {
                AudioCodecPreference::Opus => codecs.enable_opus(true, false),
                AudioCodecPreference::Pcmu => codecs.enable_pcmu(true, false),
                AudioCodecPreference::Pcma => codecs.enable_pcma(true, false),
            }
        }
        for codec in preferences.video_order() {
            if !codec.enabled_by(flags) {
                continue;
            }
            match codec {
                VideoCodecPreference::Vp8 => codecs.enable_vp8(true),
                VideoCodecPreference::H264 => codecs.enable_h264(true),
                VideoCodecPreference::H265 => codecs.enable_h265(true),
                VideoCodecPreference::Vp9 => codecs.enable_vp9(true),
                VideoCodecPreference::Av1 => codecs.enable_av1(true),
            }
        }
        retransmission::validate_profile_payload_types(codecs.params())?;
        let simulcast_codec = preferences
            .video_order()
            .into_iter()
            .find(|codec| codec.enabled_by(flags))
            .and_then(|codec| match codec {
                VideoCodecPreference::Vp8 => Some(Codec::Vp8),
                VideoCodecPreference::H264 => Some(Codec::H264),
                VideoCodecPreference::H265
                | VideoCodecPreference::Vp9
                | VideoCodecPreference::Av1 => None,
            });

        let mut router_codecs = Vec::new();
        let mut audio_names = Vec::new();
        let mut video_names = Vec::new();
        for payload in config.codec_config().params() {
            let kind = capabilities::media_kind(payload);
            router_codecs.push(capabilities::media_capability(kind, payload)?);
            if let Some(rtx) = capabilities::rtx_capability(kind, payload)? {
                router_codecs.push(rtx);
            }
            let names = if kind == RouterMediaKind::Audio {
                &mut audio_names
            } else {
                &mut video_names
            };
            let name = payload.spec().codec.to_string();
            if !names.contains(&name) {
                names.push(name);
            }
        }
        let header_extensions = config
            .extension_map()
            .iter()
            .map(capabilities::header_extension)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            config,
            router_capabilities: MediaCapabilities::new(router_codecs, header_extensions),
            audio_names,
            video_names,
            simulcast_codec,
        })
    }

    pub(in crate::engine::media_transport::rtc) fn session_config(&self) -> RtcConfig {
        self.config.clone()
    }

    pub(in crate::engine::media_transport) fn router_capabilities(&self) -> MediaCapabilities {
        self.router_capabilities.clone()
    }

    pub(in crate::engine::media_transport::rtc) fn codec_names(
        &self,
        kind: MediaKind,
    ) -> &[String] {
        if kind.is_video() {
            &self.video_names
        } else {
            &self.audio_names
        }
    }

    pub(super) fn simulcast_codec(&self) -> Option<Codec> {
        self.simulcast_codec
    }
}

#[cfg(test)]
#[path = "../TESTS/profile.rs"]
mod tests;
