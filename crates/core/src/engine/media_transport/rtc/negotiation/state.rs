//! One outstanding offer and its answer-time projections.
use std::{
    collections::{BTreeMap, BTreeSet},
    mem,
};

use o_sfu_router::rtp::MediaStream as RouterRtpParameters;
use str0m::{
    Rtc,
    change::{SdpAnswer, SdpApi, SdpOffer, SdpPendingOffer},
    media::{Direction, MediaKind, Mid, Rid},
    rtp::Ssrc,
};
use tracing::warn;

use super::{
    super::{
        RtpProfile,
        codec::{self, RepairSummary},
        commands::RtcSessionOffer,
    },
    upload_kind,
};
use crate::{
    VideoBitrateLimits,
    engine::media_transport::{SessionUploadSlot, TransportAdapterError, TransportResult},
};

#[cfg(test)]
#[path = "state/TESTS.rs"]
mod tests;

const INITIAL_NEGOTIATION_DIRECTION: Direction = Direction::RecvOnly;
const INITIAL_NEGOTIATION_MEDIA_KINDS: [MediaKind; 2] = [MediaKind::Audio, MediaKind::Video];

#[derive(Default)]
enum OfferPhase {
    #[default]
    Idle,
    Staged {
        offer: Box<SdpOffer>,
        pending: PendingOffer,
    },
    Delivered(PendingOffer),
}

struct PendingOffer {
    token: SdpPendingOffer,
    repair: RepairSummary,
    // Keep the projection after delivery: answer RIDs are checked against this offer.
    upload_slots: Vec<SessionUploadSlot>,
}

impl PendingOffer {
    fn new(offer: &SdpOffer, token: SdpPendingOffer, upload_slots: Vec<SessionUploadSlot>) -> Self {
        Self {
            token,
            repair: RepairSummary::from_offer(offer),
            upload_slots,
        }
    }
}

#[derive(Default)]
pub(in crate::engine::media_transport::rtc) struct SessionSdpNegotiationState {
    phase: OfferPhase,
    bootstrap_mids: Vec<Mid>,
    initial_offer_applied: bool,
    pub(super) pending_recv_streams: BTreeMap<Mid, Vec<PendingRecvStream>>,
    pub(in crate::engine::media_transport::rtc) negotiated_producer_parameters:
        BTreeMap<Mid, RouterRtpParameters>,
    pub(super) queued_removal_mids: BTreeSet<Mid>,
}

impl SessionSdpNegotiationState {
    pub(super) fn create_initial_offer(
        &mut self,
        rtc: &mut Rtc,
        profile: &RtpProfile,
        video_bitrate_limits: VideoBitrateLimits,
    ) -> TransportResult<RtcSessionOffer> {
        if !matches!(self.phase, OfferPhase::Idle) {
            return Err(TransportAdapterError::InvalidInput);
        }
        if self.initial_offer_applied {
            return Err(TransportAdapterError::UnsupportedFeature);
        }
        let mut api = rtc.sdp_api();
        ensure_initial_negotiation_media(
            &mut self.bootstrap_mids,
            &mut api,
            profile,
            video_bitrate_limits,
        );
        let (offer, token) = api
            .apply()
            .ok_or(TransportAdapterError::TransportUnavailable)?;
        self.phase = OfferPhase::Delivered(PendingOffer::new(&offer, token, Vec::new()));
        Ok(RtcSessionOffer::new(
            offer,
            initial_upload_slots(&self.bootstrap_mids, profile, video_bitrate_limits),
        ))
    }

    pub(super) fn offered_repair(&self) -> Option<&RepairSummary> {
        self.pending_offer().map(|pending| &pending.repair)
    }

    pub(in crate::engine::media_transport::rtc) fn initial_offer_applied(&self) -> bool {
        self.initial_offer_applied
    }

    pub(in crate::engine::media_transport::rtc) fn is_awaiting_answer(&self) -> bool {
        matches!(self.phase, OfferPhase::Delivered(_))
    }

    pub(in crate::engine::media_transport::rtc) fn pending_recv_streams(
        &self,
        mid: Mid,
    ) -> Option<&[PendingRecvStream]> {
        self.pending_recv_streams.get(&mid).map(Vec::as_slice)
    }

    fn pending_offer(&self) -> Option<&PendingOffer> {
        match &self.phase {
            OfferPhase::Idle => None,
            OfferPhase::Staged { pending, .. } | OfferPhase::Delivered(pending) => Some(pending),
        }
    }

    pub(super) fn upload_slots(&self) -> &[SessionUploadSlot] {
        self.pending_offer()
            .map_or(&[], |pending| pending.upload_slots.as_slice())
    }

    pub(super) fn stage_change<T>(
        &mut self,
        rtc: &mut Rtc,
        change: impl FnOnce(&mut SdpApi<'_>) -> (T, Vec<SessionUploadSlot>),
    ) -> TransportResult<Option<T>> {
        let mut api = rtc.sdp_api();
        match mem::take(&mut self.phase) {
            OfferPhase::Staged { pending, .. } => {
                api.merge(pending.token);
            }
            OfferPhase::Idle => {}
            OfferPhase::Delivered(pending) => {
                self.phase = OfferPhase::Delivered(pending);
                return Err(TransportAdapterError::InvalidInput);
            }
        };
        let (result, upload_slots) = change(&mut api);
        let Some((offer, token)) = api.apply() else {
            return Ok(None);
        };
        self.phase = OfferPhase::Staged {
            pending: PendingOffer::new(&offer, token, upload_slots),
            offer: Box::new(offer),
        };
        Ok(Some(result))
    }

    pub(in crate::engine::media_transport::rtc) fn take_offer(
        &mut self,
    ) -> TransportResult<RtcSessionOffer> {
        if !self.initial_offer_applied || self.is_awaiting_answer() {
            return Err(TransportAdapterError::InvalidInput);
        }
        let OfferPhase::Staged { offer, pending } = mem::take(&mut self.phase) else {
            return Err(TransportAdapterError::UnsupportedFeature);
        };
        let upload_slots = pending.upload_slots.clone();
        self.phase = OfferPhase::Delivered(pending);
        Ok(RtcSessionOffer::new(*offer, upload_slots))
    }

    /// Only a delivered offer can be answered. Rejection preserves an undelivered draft.
    pub(super) fn accept_answer(
        &mut self,
        rtc: &mut Rtc,
        answer: SdpAnswer,
    ) -> TransportResult<Vec<SessionUploadSlot>> {
        let pending = match mem::take(&mut self.phase) {
            OfferPhase::Idle => return Err(TransportAdapterError::InvalidInput),
            staged @ OfferPhase::Staged { .. } => {
                self.phase = staged;
                return Err(TransportAdapterError::InvalidInput);
            }
            OfferPhase::Delivered(pending) => pending,
        };
        // str0m consumes the token and may mutate ICE/DTLS before failing. The room
        // must close this session because retrying cannot restore the transaction.
        rtc.sdp_api()
            .accept_answer(pending.token, answer)
            .inspect_err(|error| warn!(?error, "str0m failed to accept the session answer"))
            .map_err(|_error| TransportAdapterError::AnswerApplicationFailed)?;
        self.initial_offer_applied = true;
        Ok(pending.upload_slots)
    }
}

/// Receive identity to restore if answer application recreates str0m bindings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::engine::media_transport::rtc) struct PendingRecvStream {
    pub(in crate::engine::media_transport::rtc) ssrc: Ssrc,
    pub(in crate::engine::media_transport::rtc) repair_ssrc: Option<Ssrc>,
    pub(in crate::engine::media_transport::rtc) rid: Option<Rid>,
}

fn ensure_initial_negotiation_media(
    bootstrap_mids: &mut Vec<Mid>,
    sdp_api: &mut SdpApi<'_>,
    profile: &RtpProfile,
    video_bitrate_limits: VideoBitrateLimits,
) {
    if !bootstrap_mids.is_empty() {
        return;
    }
    *bootstrap_mids = INITIAL_NEGOTIATION_MEDIA_KINDS
        .into_iter()
        .map(|media_kind| {
            sdp_api.add_media(
                media_kind,
                INITIAL_NEGOTIATION_DIRECTION,
                None,
                None,
                codec::bootstrap_recv_simulcast(media_kind, profile, video_bitrate_limits),
            )
        })
        .collect();
}

fn initial_upload_slots(
    bootstrap_mids: &[Mid],
    profile: &RtpProfile,
    video_bitrate_limits: VideoBitrateLimits,
) -> Vec<SessionUploadSlot> {
    INITIAL_NEGOTIATION_MEDIA_KINDS
        .iter()
        .zip(bootstrap_mids.iter())
        .map(|(media_kind, mid)| SessionUploadSlot {
            mid: mid.to_string(),
            kind: upload_kind(*media_kind),
            codecs: profile.codec_names(*media_kind).to_vec(),
            simulcast_encodings: codec::bootstrap_upload_encodings(
                *media_kind,
                profile,
                video_bitrate_limits,
            ),
        })
        .collect()
}
