//! SDP projections of the media a session queues for its next offer.
use o_sfu_router::rtp::MediaStream as RouterRtpParameters;
use str0m::{
    media::{MediaKind, Mid},
    rtp::Ssrc,
};

use super::{super::RtpProfile, super::codec, PendingRecvStream, upload_kind};
use crate::{
    VideoBitrateLimits,
    engine::{
        media_transport::{SessionUploadSlot, TransportSessionKey},
        source_model::SourceSyncPolicy,
    },
};

/// Returns the SDP stream id that groups a consumer with its publisher's
/// synchronized pair.
pub(in crate::engine::media_transport::rtc) fn consumer_stream_id(
    source: &TransportSessionKey,
    sync: SourceSyncPolicy,
) -> Option<String> {
    // Browsers synchronize only one audio/video pair per SDP stream. Sources
    // outside that pair need independent IDs so they cannot displace its video.
    // https://webrtc.googlesource.com/src/+/refs/heads/main/call/call.cc
    match sync {
        SourceSyncPolicy::Independent => None,
        SourceSyncPolicy::Publisher => Some(format!(
            "sfu-{}-{}",
            source.room_instance_id(),
            source.connection_id()
        )),
    }
}

pub(in crate::engine::media_transport::rtc) fn recv_encoding_identities(
    rtp_parameters: &RouterRtpParameters,
) -> impl Iterator<Item = PendingRecvStream> + '_ {
    rtp_parameters.bindings().filter_map(|encoding| {
        encoding.ssrc().map(|ssrc| PendingRecvStream {
            ssrc: Ssrc::from(ssrc),
            repair_ssrc: encoding.repair_ssrc().map(Ssrc::from),
            rid: encoding.rid().map(Into::into),
        })
    })
}

pub(super) fn upload_slot(
    mid: Mid,
    media_kind: MediaKind,
    rtp_parameters: &RouterRtpParameters,
    profile: &RtpProfile,
    video_bitrate_limits: VideoBitrateLimits,
) -> SessionUploadSlot {
    SessionUploadSlot {
        mid: mid.to_string(),
        kind: upload_kind(media_kind),
        codecs: upload_codecs(media_kind, rtp_parameters, profile),
        simulcast_encodings: codec::publish_upload_encodings_or_default(
            media_kind,
            rtp_parameters,
            profile,
            video_bitrate_limits,
        ),
    }
}

fn upload_codecs(
    media_kind: MediaKind,
    rtp_parameters: &RouterRtpParameters,
    profile: &RtpProfile,
) -> Vec<String> {
    let mut codecs = Vec::new();
    for format in rtp_parameters.formats() {
        let codec = format.codec_name();
        if !codecs
            .iter()
            .any(|existing: &String| existing.as_str() == codec)
        {
            codecs.push(codec.to_owned());
        }
    }
    if codecs.is_empty() {
        profile.codec_names(media_kind).to_vec()
    } else {
        codecs
    }
}
