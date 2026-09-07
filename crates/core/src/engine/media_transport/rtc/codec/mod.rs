//! RTC codec policy, negotiation and packet behavior.
//!
//! [`profile`] and [`capabilities`] compile configured codecs into `str0m` and
//! router RTP forms. VP8 and promoted H.264 profiles derive RID simulcast
//! signaling and upload encodings through [`rid`]. [`rid`] also validates
//! answer-side send RIDs and selects the initial consumer packet gate.
//!
//! [`packet`] keeps codec-specific packet inspection and receiver identity
//! projection below the room source graph and video policy.

mod capabilities;
mod h264;
mod packet;
mod profile;
mod retransmission;
mod rid;
mod vp8;

#[cfg(any(test, fuzzing))]
pub use capabilities::client_rtp_capabilities_from_answer;
pub(super) use capabilities::{
    answer_payload_params, client_rtp_capabilities_from_sdp_answer, header_extension, media_format,
    router_payload_type, rtx_format,
};
use o_sfu_router::rtp::{MediaCodec, MediaStream};
pub(super) use packet::{
    Packet, PacketIdentity, PacketInspector, ProjectedPacket, Projection, requires_decoder_refresh,
};
pub(in crate::engine::media_transport) use profile::RtpProfile;
pub(super) use retransmission::{
    RepairSummary, primary_payload_type, repair_enabled, validate_answer_sdp,
};
pub(super) use rid::{
    NegotiatedRid, ParsedAnswerRids, initial_packet_gate as initial_consumer_packet_gate,
};
use str0m::{
    format::Codec,
    media::{MediaKind, Simulcast},
};

use crate::{VideoBitrateLimits, engine::media_transport::SessionUploadEncoding};

const LOW_LAYER_RESOLUTION_SCALE: u16 = 4;
const MIDDLE_LAYER_RESOLUTION_SCALE: u16 = 2;
const HIGH_LAYER_RESOLUTION_SCALE: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SimulcastProfile {
    Vp8(VideoBitrateLimits),
    H264(VideoBitrateLimits),
}

impl SimulcastProfile {
    fn bootstrap(
        media_kind: MediaKind,
        rtp_profile: &RtpProfile,
        video_bitrate_limits: VideoBitrateLimits,
    ) -> Option<Self> {
        if !media_kind.is_video() {
            return None;
        }
        match rtp_profile.simulcast_codec()? {
            Codec::Vp8 => Some(Self::Vp8(video_bitrate_limits)),
            Codec::H264 => Some(Self::H264(video_bitrate_limits)),
            _ => None,
        }
    }

    fn publish(
        media_kind: MediaKind,
        parameters: &MediaStream,
        video_bitrate_limits: VideoBitrateLimits,
    ) -> Option<Self> {
        if !media_kind.is_video() {
            return None;
        }
        match capabilities::primary_codec(parameters)? {
            MediaCodec::Vp8 => Some(Self::Vp8(video_bitrate_limits)),
            MediaCodec::H264 => Some(Self::H264(video_bitrate_limits)),
            _ => None,
        }
    }

    fn layers(self, parameters: Option<&MediaStream>) -> Option<Vec<rid::LayerSpec<'_>>> {
        let limits = match self {
            Self::Vp8(limits) | Self::H264(limits) => limits,
        };
        let Some(parameters) = parameters else {
            return Some(rid::default_layers(limits).into());
        };
        if matches!(self, Self::H264(_)) && !parameters.formats().any(h264::is_promoted_format) {
            return None;
        }
        rid::layers_from_bindings(parameters)
    }

    fn recv_simulcast(self, parameters: Option<&MediaStream>) -> Option<Simulcast> {
        self.layers(parameters)
            .map(|layers| rid::recv_simulcast(&layers))
    }

    fn upload_encodings(self, parameters: Option<&MediaStream>) -> Vec<SessionUploadEncoding> {
        self.layers(parameters).map_or_else(Vec::new, |layers| {
            let layer_count = layers.len();
            layers
                .into_iter()
                .enumerate()
                .map(|(index, layer)| SessionUploadEncoding {
                    rid: layer.rid.to_owned(),
                    max_bitrate: layer.max_bitrate,
                    // Two-RID VP8 publishers retain their existing 4:1 spatial ladder.
                    resolution_scale: matches!(self, Self::Vp8(_)).then_some(if index == 0 {
                        LOW_LAYER_RESOLUTION_SCALE
                    } else if index + 1 == layer_count {
                        HIGH_LAYER_RESOLUTION_SCALE
                    } else {
                        MIDDLE_LAYER_RESOLUTION_SCALE
                    }),
                    max_framerate: None,
                })
                .collect()
        })
    }
}

pub(super) fn bootstrap_recv_simulcast(
    media_kind: MediaKind,
    rtp_profile: &RtpProfile,
    video_bitrate_limits: VideoBitrateLimits,
) -> Option<Simulcast> {
    SimulcastProfile::bootstrap(media_kind, rtp_profile, video_bitrate_limits)
        .and_then(|profile| profile.recv_simulcast(None))
}

pub(super) fn bootstrap_upload_encodings(
    media_kind: MediaKind,
    rtp_profile: &RtpProfile,
    video_bitrate_limits: VideoBitrateLimits,
) -> Vec<SessionUploadEncoding> {
    SimulcastProfile::bootstrap(media_kind, rtp_profile, video_bitrate_limits)
        .map_or_else(Vec::new, |profile| profile.upload_encodings(None))
}

pub(super) fn publish_recv_simulcast(
    media_kind: MediaKind,
    parameters: &MediaStream,
) -> Option<Simulcast> {
    SimulcastProfile::publish(media_kind, parameters, VideoBitrateLimits::default())
        .and_then(|profile| profile.recv_simulcast(Some(parameters)))
}

pub(super) fn publish_recv_simulcast_or_default(
    media_kind: MediaKind,
    parameters: &MediaStream,
    rtp_profile: &RtpProfile,
    video_bitrate_limits: VideoBitrateLimits,
) -> Option<Simulcast> {
    publish_recv_simulcast(media_kind, parameters).or_else(|| {
        publish_uses_default_profile(parameters)
            .then(|| bootstrap_recv_simulcast(media_kind, rtp_profile, video_bitrate_limits))
            .flatten()
    })
}

pub(super) fn publish_upload_encodings(
    media_kind: MediaKind,
    parameters: &MediaStream,
) -> Vec<SessionUploadEncoding> {
    SimulcastProfile::publish(media_kind, parameters, VideoBitrateLimits::default())
        .map_or_else(Vec::new, |profile| {
            profile.upload_encodings(Some(parameters))
        })
}

pub(super) fn publish_upload_encodings_or_default(
    media_kind: MediaKind,
    parameters: &MediaStream,
    rtp_profile: &RtpProfile,
    video_bitrate_limits: VideoBitrateLimits,
) -> Vec<SessionUploadEncoding> {
    let encodings = publish_upload_encodings(media_kind, parameters);
    if !encodings.is_empty() || !publish_uses_default_profile(parameters) {
        return encodings;
    }
    bootstrap_upload_encodings(media_kind, rtp_profile, video_bitrate_limits)
}

fn publish_uses_default_profile(parameters: &MediaStream) -> bool {
    parameters.formats().next().is_none() && parameters.bindings().next().is_none()
}

#[cfg(test)]
#[path = "TESTS/mod.rs"]
mod tests;
