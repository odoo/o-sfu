//! SDP projections of the media a session queues for its next offer.
use o_sfu_router::rtp::MediaStream as RouterRtpParameters;
use str0m::{
    change::DirectApi,
    media::{MediaKind, Mid},
    rtp::Ssrc,
};

use super::{
    super::{RtpProfile, codec},
    PendingRecvStream, upload_kind,
};
use crate::{VideoBitrateLimits, engine::media_transport::SessionUploadSlot};

/// Declares the RID-less transmit stream of the send section `mid`, with an
/// RTX stream when `repair`.
pub(in crate::engine::media_transport::rtc) fn declare_send_stream(
    api: &mut DirectApi<'_>,
    mid: Mid,
    repair: bool,
) -> (Ssrc, Option<Ssrc>) {
    let ssrc = api.new_ssrc();
    // SSRC-multiplexed RTX uses a different SSRC from the primary stream.
    // https://www.rfc-editor.org/rfc/rfc4588.html#section-4
    let repair_ssrc = repair.then(|| {
        loop {
            let repair_ssrc = api.new_ssrc();
            if repair_ssrc != ssrc {
                break repair_ssrc;
            }
        }
    });
    api.declare_stream_tx(ssrc, repair_ssrc, mid, None);
    (ssrc, repair_ssrc)
}

/// Removes the RID-less transmit streams of the send section `mid`.
///
/// Besides the stream its consumer declared, the section may hold one str0m
/// created: every answer gives a sending section without a stream a new one.
pub(in crate::engine::media_transport::rtc) fn remove_send_streams(
    api: &mut DirectApi<'_>,
    mid: Mid,
) {
    while let Some(ssrc) = api.stream_tx_by_mid(mid, None).map(|stream| stream.ssrc()) {
        api.remove_stream_tx(ssrc);
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
