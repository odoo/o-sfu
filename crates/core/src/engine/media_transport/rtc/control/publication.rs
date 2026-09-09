//! Answer-side producer parameter projection.
//!
//! Producers are declared inside the worker before the remote answer confirms
//! the effective RTP bindings. This module projects the accepted answer back
//! into router-native RTP parameters and keeps the producer SSRC indexes aligned
//! with those negotiated bindings.

use std::collections::{BTreeMap, BTreeSet};

use o_sfu_router::{
    MediaKind as RouterMediaKind,
    rtp::{
        HeaderExtension as RouterHeaderExtension, MediaFormat as RouterMediaFormat,
        MediaStream as RouterRtpParameters, PayloadType, StreamBinding,
    },
};
use str0m::{
    change::SdpAnswer,
    format::PayloadParams,
    media::{Direction, MediaKind as Str0mMediaKind, Mid, Rid},
};
#[cfg(test)]
use {
    super::super::state::media_registry::RegisteredMediaHandle,
    crate::engine::media_transport::TransportMediaId, tracing::warn,
};

use super::{
    super::{
        codec,
        state::{PacketLoopState, RtcSessionState},
    },
    recv_stream::{RecvStreamRepair, StaleSsrcPolicy, apply_recv_stream},
};
use crate::{
    Bitrate,
    engine::media_transport::{TransportAdapterError, TransportSessionKey},
};

pub(super) struct AnswerProducerProjection {
    mid: Mid,
    direction: Direction,
    payload_params: Vec<PayloadParams>,
    header_extensions: Vec<RouterHeaderExtension>,
    signaled_ssrcs: Vec<(u32, Option<u32>)>,
}

pub(super) fn answer_producer_projection(
    answer: &SdpAnswer,
    producer_mids: &[Mid],
) -> Result<Vec<AnswerProducerProjection>, TransportAdapterError> {
    answer
        .media_lines
        .iter()
        .filter(|media_line| producer_mids.contains(&media_line.mid()))
        .map(|media_line| {
            let ssrc_info = media_line.ssrc_info();
            // Each FID group associates an RTX SSRC with its primary SSRC.
            // https://www.rfc-editor.org/rfc/rfc5576.html#section-7
            let signaled_ssrcs = ssrc_info
                .iter()
                .filter(|info| info.repairs.is_none())
                .map(|info| {
                    let primary = *info.ssrc;
                    let repair = ssrc_info.iter().find_map(|candidate| {
                        (candidate.repairs == Some(info.ssrc)).then_some(*candidate.ssrc)
                    });
                    (primary, repair)
                })
                .collect::<Vec<_>>();
            Ok(AnswerProducerProjection {
                mid: media_line.mid(),
                direction: media_line.direction(),
                payload_params: codec::answer_payload_params(media_line.rtp_params()),
                header_extensions: media_line
                    .extmaps()
                    .into_iter()
                    .map(codec::header_extension)
                    .collect::<Result<Vec<_>, _>>()?,
                signaled_ssrcs,
            })
        })
        .collect()
}

pub(super) fn refresh_negotiated_producer_parameters(
    state: &mut PacketLoopState,
    session_key: &TransportSessionKey,
    producer_mids: &[Mid],
    answer_projection: Vec<AnswerProducerProjection>,
    rids_by_mid: &BTreeMap<Mid, Vec<codec::NegotiatedRid>>,
    max_bitrate_in: Bitrate,
) -> Result<Vec<(Mid, RouterRtpParameters)>, TransportAdapterError> {
    let mut refreshed_parameters = Vec::with_capacity(producer_mids.len());
    let producer_mid_set = producer_mids.iter().copied().collect::<BTreeSet<_>>();
    {
        let Some(session_state) = state.users.get_mut(session_key) else {
            return Ok(refreshed_parameters);
        };
        session_state
            .sdp_negotiation
            .negotiated_producer_parameters
            .retain(|mid, _parameters| !producer_mid_set.contains(mid));
        for media_line in answer_projection {
            // Only answer directions where the remote sends can establish a producer.
            // https://www.rfc-editor.org/rfc/rfc3264.html#section-6.1
            if !matches!(
                media_line.direction,
                Direction::SendOnly | Direction::SendRecv
            ) {
                continue;
            }
            let mid = media_line.mid;
            let Some(media_kind) = session_state
                .rtc
                .media(mid)
                .map(|media| to_router_media_kind(media.kind()))
            else {
                continue;
            };
            let Some(primary_payload) = media_line.payload_params.first() else {
                continue;
            };
            let primary_payload_type = codec::router_payload_type(*primary_payload.pt())?;
            let formats = project_media_formats(media_kind, &media_line.payload_params)?;
            let rids = rids_by_mid.get(&mid).map(Vec::as_slice).unwrap_or_default();
            let bindings = project_bindings(
                session_state,
                mid,
                primary_payload_type,
                rids,
                &media_line.signaled_ssrcs,
            );
            if bindings.is_empty() {
                continue;
            }
            let parameters =
                RouterRtpParameters::new(formats, media_line.header_extensions, bindings)
                    .with_mid(mid.to_string());
            apply_projected_recv_streams(session_state, mid, &parameters, max_bitrate_in);
            session_state
                .sdp_negotiation
                .negotiated_producer_parameters
                .insert(mid, parameters.clone());
            refreshed_parameters.push((mid, parameters));
        }
    }
    for producer_mid in producer_mids {
        state.clear_producer_ssrcs_for_mid(session_key, *producer_mid);
    }
    for (mid, parameters) in &refreshed_parameters {
        state.refresh_producer_ssrcs(session_key, *mid, parameters);
    }
    Ok(refreshed_parameters)
}

fn apply_projected_recv_streams(
    session_state: &mut RtcSessionState,
    mid: Mid,
    parameters: &RouterRtpParameters,
    max_bitrate_in: Bitrate,
) {
    let nack_enabled = codec::repair_enabled(parameters);
    {
        let mut api = session_state.rtc.direct_api();
        for binding in parameters.bindings() {
            let Some(ssrc) = binding.ssrc() else {
                continue;
            };
            apply_recv_stream(
                &mut api,
                mid,
                binding.rid().map(Rid::from),
                ssrc.into(),
                RecvStreamRepair {
                    ssrc: binding.repair_ssrc().map(Into::into),
                    nack_enabled,
                },
                max_bitrate_in,
                StaleSsrcPolicy::KeepExisting,
            );
        }
    }
    #[cfg(test)]
    {
        session_state.max_bitrate_in = Some(max_bitrate_in);
    }
}

/// Resolve the router-native RTP parameters for one producer after answer-side
/// projection has populated them for the owning user.
#[cfg(test)]
pub(super) fn worker_resolve_negotiated_producer_parameters(
    state: &PacketLoopState,
    session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
) -> Result<RouterRtpParameters, TransportAdapterError> {
    let Some(handle) = state.media_handle(transport_media_id) else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    let mid = match handle {
        RegisteredMediaHandle::Producer {
            session_key: owner_session_key,
            mid,
        } if owner_session_key == session_key => *mid,
        RegisteredMediaHandle::Producer { .. } | RegisteredMediaHandle::Consumer { .. } => {
            return Err(TransportAdapterError::InvalidInput);
        }
    };
    let Some(session_state) = state.users.get(session_key) else {
        return Err(TransportAdapterError::TransportUnavailable);
    };
    let result = session_state
        .sdp_negotiation
        .negotiated_producer_parameters
        .get(&mid)
        .cloned()
        .ok_or(TransportAdapterError::UnsupportedFeature);
    if let Err(TransportAdapterError::UnsupportedFeature) = &result {
        warn!(
            user_id = ?session_key.user_id(),
            media_worker_id = session_key.media_worker_id().as_usize(),
            ?transport_media_id,
            ?mid,
            initial_offer_applied = session_state.sdp_negotiation.initial_offer_applied,
            pending_offer = session_state.sdp_negotiation.pending_offer.is_some(),
            staged_offer = session_state.sdp_negotiation.staged_offer.is_some(),
            negotiated_mids = ?session_state
                .sdp_negotiation
                .negotiated_producer_parameters
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            "negotiated producer parameters were not projected for the staged producer media"
        );
    }
    result
}

fn project_media_formats(
    media_kind: RouterMediaKind,
    payload_params: &[PayloadParams],
) -> Result<Vec<RouterMediaFormat>, TransportAdapterError> {
    let mut formats = Vec::with_capacity(payload_params.len() * 2);
    for payload in payload_params {
        formats.push(codec::media_format(media_kind, payload)?);
        if let Some(rtx) = codec::rtx_format(media_kind, payload)? {
            formats.push(rtx);
        }
    }
    Ok(formats)
}

fn project_bindings(
    session_state: &mut RtcSessionState,
    mid: Mid,
    primary_payload_type: PayloadType,
    rids: &[codec::NegotiatedRid],
    signaled_ssrcs: &[(u32, Option<u32>)],
) -> Vec<StreamBinding> {
    if !rids.is_empty() {
        // RepairedRtpStreamId associates each repair stream with its source RID.
        // https://www.rfc-editor.org/rfc/rfc8852.html#section-3
        return rids
            .iter()
            .map(|rid| {
                let mut binding = StreamBinding::new()
                    .with_rid(rid.rid.to_string())
                    .with_payload_type(primary_payload_type);
                if let Some(max_bitrate) = rid.max_bitrate {
                    binding = binding.with_max_bitrate(max_bitrate.as_bps());
                }
                if let Some((ssrc, repair_ssrc)) =
                    stream_rx_ssrcs(session_state, mid, Some(rid.rid))
                {
                    binding = binding.with_ssrc(ssrc);
                    if let Some(repair_ssrc) = repair_ssrc {
                        binding = binding.with_repair_ssrc(repair_ssrc);
                    }
                }
                binding
            })
            .collect();
    }

    // Multiple RID-less primaries are ambiguous without another stream identity.
    // Requiring exactly one signaled pair here is O-SFU policy.
    signaled_ssrcs
        .first()
        .copied()
        .filter(|_| signaled_ssrcs.len() == 1)
        .or_else(|| stream_rx_ssrcs(session_state, mid, None))
        .map(|(ssrc, repair_ssrc)| {
            let mut binding = StreamBinding::new()
                .with_ssrc(ssrc)
                .with_payload_type(primary_payload_type);
            if let Some(repair_ssrc) = repair_ssrc {
                binding = binding.with_repair_ssrc(repair_ssrc);
            }
            vec![binding]
        })
        .unwrap_or_default()
}

fn stream_rx_ssrcs(
    session_state: &mut RtcSessionState,
    mid: Mid,
    rid: Option<Rid>,
) -> Option<(u32, Option<u32>)> {
    let mut api = session_state.rtc.direct_api();
    let stream_rx = api.stream_rx_by_mid(mid, rid)?;
    Some((*stream_rx.ssrc(), stream_rx.rtx().map(|ssrc| *ssrc)))
}

fn to_router_media_kind(media_kind: Str0mMediaKind) -> RouterMediaKind {
    match media_kind {
        Str0mMediaKind::Audio => RouterMediaKind::Audio,
        Str0mMediaKind::Video => RouterMediaKind::Video,
    }
}
