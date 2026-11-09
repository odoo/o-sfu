//! SDP changes requested by media lifecycle.
use o_sfu_router::rtp::MediaStream as RouterRtpParameters;
use str0m::{
    media::{Direction, MediaKind, Mid},
    rtp::Ssrc,
};
use tracing::warn;

use super::{
    super::{RtpProfile, codec, state::RtcSessionState},
    PendingRecvStream, upload_kind,
};
use crate::{
    VideoBitrateLimits,
    engine::{
        media_transport::{
            SessionUploadSlot, TransportAdapterError, TransportResult, TransportSessionKey,
        },
        source_model::SourceSyncPolicy,
    },
};

/// "Removal" for negotiated media means preserving the existing MID and
/// disabling the m-section with `inactive`, not rejecting it out of the SDP.
///
/// If an earlier offer is already in flight, removal is deferred into the next
/// follow-up offer so the worker keeps the one-outstanding-offer contract.
pub(in crate::engine::media_transport::rtc) fn queue_removal(
    session_state: &mut RtcSessionState,
    mid: Mid,
) -> Result<(), TransportAdapterError> {
    if session_state.sdp_negotiation.is_awaiting_answer() {
        session_state
            .sdp_negotiation
            .queued_removal_mids
            .insert(mid);
        return Ok(());
    }
    let Some(media) = session_state.rtc.media(mid) else {
        return Err(TransportAdapterError::InvalidInput);
    };
    if media.direction() == Direction::Inactive {
        session_state.reset_rtx_streams(mid);
        return Ok(());
    }

    let result = session_state
        .sdp_negotiation
        .stage_change(&mut session_state.rtc, |api| {
            api.set_direction(mid, Direction::Inactive);
            ((), Vec::new())
        });
    session_state.reset_rtx_streams(mid);
    result?.ok_or(TransportAdapterError::InvalidInput)?;
    let negotiation = &mut session_state.sdp_negotiation;
    negotiation.queued_removal_mids.remove(&mid);
    Ok(())
}

/// Stages a producer-side recv-only media section in the next local offer.
pub(in crate::engine::media_transport::rtc) fn stage_recv_media(
    session_state: &mut RtcSessionState,
    media_kind: MediaKind,
    rtp_parameters: &RouterRtpParameters,
    profile: &RtpProfile,
    video_bitrate_limits: VideoBitrateLimits,
) -> TransportResult<Mid> {
    let mid = session_state
        .sdp_negotiation
        .stage_change(&mut session_state.rtc, |api| {
            let mid = api.add_media(
                media_kind,
                Direction::RecvOnly,
                None,
                None,
                codec::publish_recv_simulcast_or_default(
                    media_kind,
                    rtp_parameters,
                    profile,
                    video_bitrate_limits,
                ),
            );
            (
                mid,
                vec![upload_slot(
                    mid,
                    media_kind,
                    rtp_parameters,
                    profile,
                    video_bitrate_limits,
                )],
            )
        })?
        .ok_or(TransportAdapterError::TransportUnavailable)?;
    let negotiation = &mut session_state.sdp_negotiation;
    // `accept_answer` can recreate `StreamRx` bindings. Retain SSRC and RID so
    // answer application can restore publisher identity and the bitrate cap.
    let pending_streams = recv_encoding_identities(rtp_parameters).collect::<Vec<_>>();
    if pending_streams.is_empty() {
        negotiation.pending_recv_streams.remove(&mid);
    } else {
        negotiation
            .pending_recv_streams
            .insert(mid, pending_streams);
    }
    Ok(mid)
}

/// Stages one send-only consumer media section in the next local offer.
///
/// # Errors
///
/// Returns [`TransportAdapterError::InvalidInput`] after an offer has been
/// handed out. A new MID cannot be merged into that offer and a second
/// [`str0m::change::SdpApi::apply`] would invalidate the
/// [`str0m::change::SdpPendingOffer`] needed by its answer.
/// Returns [`TransportAdapterError::TransportUnavailable`] when str0m cannot
/// apply the staged change.
pub(in crate::engine::media_transport::rtc) fn stage_send_media(
    session_state: &mut RtcSessionState,
    source: &TransportSessionKey,
    media_kind: MediaKind,
    sync: SourceSyncPolicy,
) -> TransportResult<Mid> {
    // Browsers synchronize only one audio/video pair per SDP stream. Sources
    // outside that pair need independent IDs so they cannot displace its video.
    // https://webrtc.googlesource.com/src/+/refs/heads/main/call/call.cc
    let stream_id = match sync {
        SourceSyncPolicy::Independent => None,
        SourceSyncPolicy::Publisher => Some(format!(
            "sfu-{}-{}",
            source.room_instance_id(),
            source.connection_id()
        )),
    };
    session_state
        .sdp_negotiation
        .stage_change(&mut session_state.rtc, |api| {
            (
                api.add_media(media_kind, Direction::SendOnly, stream_id, None, None),
                Vec::new(),
            )
        })
        .inspect_err(|_error| {
            warn!(
                ?media_kind,
                initial_offer_applied = session_state.sdp_negotiation.initial_offer_applied(),
                "cannot stage consumer media while a previous offer is still awaiting answer"
            );
        })?
        .ok_or(TransportAdapterError::TransportUnavailable)
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

fn upload_slot(
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
