//! Negotiation queue, outstanding offer and answer projections of one session.
//!
//! ```text
//! add recv, add send, remove --> queue --take_offer--> offer --answer--> idle
//!                                  ^                     |
//!                                  +-- changes made while the offer is out
//! ```
//!
//! Queue: a producer's recv addition, a consumer's send addition and the
//! removal of a negotiated MID wait here in arrival order. The
//! [`TransportMediaId`] returned to the caller names an addition until its MID
//! exists.
//!
//! MID binding: str0m draws a MID inside [`SdpApi::add_media`] and only the
//! [`SdpApi`] that applies the change keeps it. `take_offer` replays the queue
//! into one [`SdpApi`] and applies it once, so an addition gets its MID there
//! and no earlier.
//!
//! Committed consumers: `take_offer` leaves a send addition queued until its
//! caller lists the consumer as committed. The caller names each consumer MID
//! to the client before the offer, which it can only do for a consumer it has
//! committed.
//!
//! Free MIDs: a removal offers its section as `inactive` and str0m keeps the
//! MID. A send addition takes the free MID that [`super::send_streams`] allows
//! before it adds a section. The MID then sends with new SSRCs, so the
//! receiver decodes nothing of the consumer it had before.
//!
//! One outstanding offer: another [`SdpApi::apply`] would invalidate the
//! [`SdpPendingOffer`] that the awaited answer needs. Changes made meanwhile
//! stay queued for the next offer.
use std::collections::BTreeMap;

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
    media,
    send_streams::{Placement, SendStreams, SyncGroup},
    upload_kind,
};
use crate::{
    VideoBitrateLimits,
    engine::media_transport::{
        SessionUploadSlot, TransportAdapterError, TransportMediaId, TransportResult,
    },
};

#[cfg(test)]
#[path = "state/TESTS.rs"]
mod tests;

const INITIAL_NEGOTIATION_DIRECTION: Direction = Direction::RecvOnly;
const INITIAL_NEGOTIATION_MEDIA_KINDS: [MediaKind; 2] = [MediaKind::Audio, MediaKind::Video];

pub(in crate::engine::media_transport::rtc) enum QueuedChange {
    AddRecv {
        media: TransportMediaId,
        kind: MediaKind,
        rtp_parameters: RouterRtpParameters,
    },
    AddSend {
        media: TransportMediaId,
        kind: MediaKind,
        sync_group: Option<SyncGroup>,
    },
    /// Keeps the MID and offers its m-section as `inactive` instead of
    /// rejecting it out of the SDP. A consumer's MID is then free.
    Remove(Mid),
}

/// A queued change with the section of a send addition chosen.
enum OfferStep {
    AddRecv {
        media: TransportMediaId,
        kind: MediaKind,
        rtp_parameters: RouterRtpParameters,
    },
    /// Adds a section to the `a=msid` stream `stream`.
    AddSend {
        media: TransportMediaId,
        kind: MediaKind,
        stream: String,
    },
    /// Gives the free MID to the consumer `media`.
    ReuseSend {
        media: TransportMediaId,
        mid: Mid,
    },
    Remove(Mid),
}

struct PendingOffer {
    token: SdpPendingOffer,
    repair: RepairSummary,
    // Answer RIDs are checked against the slots of this offer.
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

/// A renegotiation offer and the MIDs it bound to queued additions.
pub(super) struct TakenOffer {
    pub(super) offer: SdpOffer,
    pub(super) upload_slots: Vec<SessionUploadSlot>,
    pub(super) bound_mids: Vec<(TransportMediaId, Mid)>,
}

#[derive(Default)]
pub(in crate::engine::media_transport::rtc) struct SessionSdpNegotiationState {
    pub(in crate::engine::media_transport::rtc) queue: Vec<QueuedChange>,
    send_streams: SendStreams,
    /// Whether a removed consumer leaves its MID to a later one. The initial
    /// offer sets it, before the session has a consumer.
    mid_reuse: bool,
    pending: Option<PendingOffer>,
    bootstrap_mids: Vec<Mid>,
    initial_offer_applied: bool,
    pub(super) pending_recv_streams: BTreeMap<Mid, Vec<PendingRecvStream>>,
    pub(in crate::engine::media_transport::rtc) negotiated_producer_parameters:
        BTreeMap<Mid, RouterRtpParameters>,
}

impl SessionSdpNegotiationState {
    pub(super) fn create_initial_offer(
        &mut self,
        rtc: &mut Rtc,
        profile: &RtpProfile,
        video_bitrate_limits: VideoBitrateLimits,
        mid_reuse: bool,
    ) -> TransportResult<RtcSessionOffer> {
        if self.pending.is_some() {
            return Err(TransportAdapterError::InvalidInput);
        }
        if self.initial_offer_applied {
            return Err(TransportAdapterError::UnsupportedFeature);
        }
        self.mid_reuse = mid_reuse;
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
        self.pending = Some(PendingOffer::new(&offer, token, Vec::new()));
        Ok(RtcSessionOffer::new(
            offer,
            initial_upload_slots(&self.bootstrap_mids, profile, video_bitrate_limits),
            Vec::new(),
        ))
    }

    pub(super) fn offered_repair(&self) -> Option<&RepairSummary> {
        self.pending.as_ref().map(|pending| &pending.repair)
    }

    pub(in crate::engine::media_transport::rtc) fn initial_offer_applied(&self) -> bool {
        self.initial_offer_applied
    }

    pub(in crate::engine::media_transport::rtc) fn pending_recv_streams(
        &self,
        mid: Mid,
    ) -> Option<&[PendingRecvStream]> {
        self.pending_recv_streams.get(&mid).map(Vec::as_slice)
    }

    pub(super) fn upload_slots(&self) -> &[SessionUploadSlot] {
        self.pending
            .as_ref()
            .map_or(&[], |pending| pending.upload_slots.as_slice())
    }

    /// Drops the addition of `media` unless an offer already bound it.
    pub(in crate::engine::media_transport::rtc) fn cancel_addition(
        &mut self,
        media: TransportMediaId,
    ) {
        self.queue.retain(|change| match change {
            QueuedChange::AddRecv { media: queued, .. }
            | QueuedChange::AddSend { media: queued, .. } => *queued != media,
            QueuedChange::Remove(_) => true,
        });
    }

    /// Producers whose recv addition still waits for an offer.
    pub(super) fn queued_producers(&self) -> impl Iterator<Item = TransportMediaId> + '_ {
        self.queue.iter().filter_map(|change| match change {
            QueuedChange::AddRecv { media, .. } => Some(*media),
            QueuedChange::AddSend { .. } | QueuedChange::Remove(_) => None,
        })
    }

    /// Takes the changes of the next offer off the queue. A removal frees its
    /// MID and a send addition gets its section, in queue order.
    ///
    /// A send addition whose media is not in `committed_consumers` stays queued.
    fn take_steps(
        &mut self,
        rtc: &mut Rtc,
        committed_consumers: &[TransportMediaId],
    ) -> Vec<OfferStep> {
        let Self {
            queue,
            send_streams,
            mid_reuse,
            ..
        } = self;
        queue
            .extract_if(.., |change| match change {
                QueuedChange::AddSend { media, .. } => committed_consumers.contains(media),
                QueuedChange::AddRecv { .. } | QueuedChange::Remove(_) => true,
            })
            .map(|change| match change {
                QueuedChange::AddRecv {
                    media,
                    kind,
                    rtp_parameters,
                } => OfferStep::AddRecv {
                    media,
                    kind,
                    rtp_parameters,
                },
                QueuedChange::AddSend {
                    media,
                    kind,
                    sync_group,
                } => {
                    let Placement { stream, free_mid } =
                        send_streams.place(media, kind, sync_group);
                    let Some(mid) = free_mid else {
                        return OfferStep::AddSend {
                            media,
                            kind,
                            stream,
                        };
                    };
                    // The offer names the SSRCs of the transmit streams that
                    // exist when str0m writes it. Without this stream the
                    // receiver would learn them from the first packet.
                    // `add_media` pairs every video stream with RTX.
                    media::declare_send_stream(&mut rtc.direct_api(), mid, kind.is_video());
                    OfferStep::ReuseSend { media, mid }
                }
                QueuedChange::Remove(mid) => {
                    send_streams.release(rtc, mid, *mid_reuse);
                    // The section sent until this offer, so an answer applied
                    // since its consumer left gave it a transmit stream again.
                    // A free MID has none: the consumer that takes it declares
                    // the only one.
                    media::remove_send_streams(&mut rtc.direct_api(), mid);
                    OfferStep::Remove(mid)
                }
            })
            .collect()
    }

    /// Replays the queue into the one offer the session may have outstanding.
    ///
    /// A send addition whose media is not in `committed_consumers` stays queued.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError::InvalidInput`] before the initial answer
    /// or while an offer awaits its answer. The queue is kept. Returns
    /// [`TransportAdapterError::UnsupportedFeature`] when no replayed change
    /// needs an offer.
    pub(super) fn take_offer(
        &mut self,
        rtc: &mut Rtc,
        profile: &RtpProfile,
        video_bitrate_limits: VideoBitrateLimits,
        committed_consumers: &[TransportMediaId],
    ) -> TransportResult<TakenOffer> {
        if !self.initial_offer_applied || self.pending.is_some() {
            return Err(TransportAdapterError::InvalidInput);
        }
        // `SdpApi` borrows the `Rtc` until it writes the offer, so a reused MID
        // gets its transmit stream first.
        let steps = self.take_steps(rtc, committed_consumers);
        let mut api = rtc.sdp_api();
        let mut upload_slots = Vec::new();
        let mut bound_mids = Vec::new();
        for step in steps {
            match step {
                OfferStep::AddRecv {
                    media: transport_media_id,
                    kind,
                    rtp_parameters,
                } => {
                    let mid = api.add_media(
                        kind,
                        Direction::RecvOnly,
                        None,
                        None,
                        codec::publish_recv_simulcast_or_default(
                            kind,
                            &rtp_parameters,
                            profile,
                            video_bitrate_limits,
                        ),
                    );
                    // `accept_answer` can recreate `StreamRx` bindings. Retain SSRC and RID so
                    // answer application can restore publisher identity and the bitrate cap.
                    let streams =
                        media::recv_encoding_identities(&rtp_parameters).collect::<Vec<_>>();
                    if !streams.is_empty() {
                        self.pending_recv_streams.insert(mid, streams);
                    }
                    upload_slots.push(media::upload_slot(
                        mid,
                        kind,
                        &rtp_parameters,
                        profile,
                        video_bitrate_limits,
                    ));
                    bound_mids.push((transport_media_id, mid));
                }
                OfferStep::AddSend {
                    media,
                    kind,
                    stream,
                } => {
                    let mid = api.add_media(kind, Direction::SendOnly, Some(stream), None, None);
                    bound_mids.push((media, mid));
                }
                OfferStep::ReuseSend { media, mid } => {
                    api.set_direction(mid, Direction::SendOnly);
                    bound_mids.push((media, mid));
                }
                // An already inactive MID records no change.
                OfferStep::Remove(mid) => api.set_direction(mid, Direction::Inactive),
            }
        }
        let (offer, token) = api
            .apply()
            .ok_or(TransportAdapterError::UnsupportedFeature)?;
        self.pending = Some(PendingOffer::new(&offer, token, upload_slots.clone()));
        Ok(TakenOffer {
            offer,
            upload_slots,
            bound_mids,
        })
    }

    /// Rejection before str0m sees the answer keeps the offer outstanding.
    pub(super) fn accept_answer(
        &mut self,
        rtc: &mut Rtc,
        answer: SdpAnswer,
    ) -> TransportResult<()> {
        let pending = self
            .pending
            .take()
            .ok_or(TransportAdapterError::InvalidInput)?;
        // str0m consumes the token and may mutate ICE/DTLS before failing. The room
        // must close this session because retrying cannot restore the transaction.
        rtc.sdp_api()
            .accept_answer(pending.token, answer)
            .inspect_err(|error| warn!(?error, "str0m failed to accept the session answer"))
            .map_err(|_error| TransportAdapterError::AnswerApplicationFailed)?;
        self.initial_offer_applied = true;
        Ok(())
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
