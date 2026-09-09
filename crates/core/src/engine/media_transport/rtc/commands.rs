//! mailbox command contract for the RTC worker engine
//!
//! `MediaTransport` production paths and cfg-gated worker harnesses translate
//! transport intent into these values before the packet-loop task dispatches
//! them while it owns mutable rtc state
//! request commands carry a oneshot response
//! fire-and-forget route controls are best-effort because they may target a
//! worker that has already torn down the corresponding relay or session

use std::sync::Arc;

use o_sfu_rfc::webrtc::sdp;
use o_sfu_router::rtp::MediaStream as RouterRtpParameters;
use str0m::{
    change::{SdpAnswer, SdpOffer},
    media::{KeyframeRequestKind, MediaKind, Rid},
};
use tokio::sync::{mpsc, oneshot};

use super::{
    codec::{ParsedAnswerRids, RepairSummary, validate_answer_sdp},
    state::{
        relay_registry::{RelayPacketMailbox, RelayTargetId},
        route_control::PacketLayerGate,
    },
};
use crate::engine::{
    media_transport::{
        ActiveSpeakerSource, AppliedSessionAnswer, ConsumerRouteControl,
        ConsumerRouteControlOutcome, ProducerRouteControl, ReceiverBweTargetUpdate, SessionOffer,
        SessionUploadSlot, SourceActivityUpdate, TransportAdapterError, TransportConsumerRoute,
        TransportMediaId, TransportResult, TransportSessionKey, TransportSourceDiagnosticsSnapshot,
        TransportSourceKey,
    },
    metrics::{RtcMetricsRecorder, RtcRemoteControlDropKind, RtcRemotePacketGateConvergence},
};

/// command handle used by remote consumers to push control back to a source worker
///
/// a route that consumes media from another worker keeps this handle beside the
/// remote-source registration
/// later keyframe or layer-gate requests can then reach the worker that owns
/// the producer without exposing the source worker internals
///
/// sends are deliberately best-effort
/// stale remote routes, closed workers and full mailboxes are normal during
/// teardown or topology churn
#[derive(Debug, Clone)]
pub struct RemoteSourceControl {
    tx: mpsc::Sender<RtcWorkerCommand>,
    target_id: RelayTargetId,
    rtc_metrics: Arc<RtcMetricsRecorder>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RemoteControlSendOutcome {
    Forwarded,
    Full,
    Closed,
}

impl RemoteSourceControl {
    /// creates a source-control handle for one relay target on a worker mailbox
    pub(super) fn new(
        tx: mpsc::Sender<RtcWorkerCommand>,
        target_id: RelayTargetId,
        rtc_metrics: Arc<RtcMetricsRecorder>,
    ) -> Self {
        Self {
            tx,
            target_id,
            rtc_metrics,
        }
    }

    /// asks the source worker to request a keyframe for a remote consumer
    ///
    /// this never waits for the source worker
    /// `Full` retains caller-side retry authority while `Closed` ends it
    pub(super) fn request_kf(
        &self,
        source: &TransportSourceKey,
        rid: Option<Rid>,
        kind: KeyframeRequestKind,
    ) -> RemoteControlSendOutcome {
        self.send_command(
            RouteControlRequest::RequestRemoteKeyframe {
                source: source.clone(),
                target_id: self.target_id,
                rid,
                kind,
            },
            RtcRemoteControlDropKind::Keyframe,
        )
    }

    /// publishes the effective remote-source packet gate to the source worker
    pub(super) fn set_pkt_gate(
        &self,
        source: &TransportSourceKey,
        packet_gate: PacketLayerGate,
    ) -> bool {
        self.send_command(
            RouteControlRequest::SetRemoteSourcePacketGate {
                source: source.clone(),
                target_id: self.target_id,
                packet_gate,
            },
            RtcRemoteControlDropKind::PacketGate,
        ) == RemoteControlSendOutcome::Forwarded
    }

    pub(super) fn record_pkt_gate_retry(&self) {
        self.rtc_metrics
            .record_rtc_remote_packet_gate_convergence(RtcRemotePacketGateConvergence::Retry);
    }

    pub(super) fn record_pkt_gate_flushed(&self) {
        self.rtc_metrics
            .record_rtc_remote_packet_gate_convergence(RtcRemotePacketGateConvergence::Flushed);
    }

    #[inline]
    fn send_command(
        &self,
        request: RouteControlRequest,
        drop_kind: RtcRemoteControlDropKind,
    ) -> RemoteControlSendOutcome {
        match self.tx.try_send(RtcWorkerCommand::RouteControl {
            request,
            response: None,
        }) {
            Ok(()) => RemoteControlSendOutcome::Forwarded,
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.rtc_metrics.record_rtc_remote_control_drop(drop_kind);
                RemoteControlSendOutcome::Full
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.rtc_metrics.record_rtc_remote_control_drop(drop_kind);
                RemoteControlSendOutcome::Closed
            }
        }
    }
}

/// Response channel for a command executed by the packet loop.
///
/// Dropping the receiver cancels only the API wait and does not retract an
/// enqueued command. Terminal worker cancellation may still win before dispatch.
pub type RtcWorkerResponse<T> = oneshot::Sender<TransportResult<T>>;

/// SDP answer parsed before packet-loop command delivery.
pub struct ParsedSessionAnswer {
    pub(super) answer: SdpAnswer,
    pub(super) rids: ParsedAnswerRids,
    pub(super) repair: RepairSummary,
}

impl ParsedSessionAnswer {
    /// Validates and parses a remote SDP answer.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError::InvalidInput`] for malformed repair
    /// topology or SDP that str0m cannot parse.
    pub(in crate::engine::media_transport) fn parse(answer_sdp: &str) -> TransportResult<Self> {
        let repair = validate_answer_sdp(answer_sdp)?;
        let answer = SdpAnswer::from_sdp_string(answer_sdp)
            .map_err(|_error| TransportAdapterError::InvalidInput)?;
        let rids = ParsedAnswerRids::parse(answer_sdp, &answer);
        Ok(Self {
            answer,
            rids,
            repair,
        })
    }
}

pub struct RtcSessionOffer {
    offer: SdpOffer,
    upload_slots: Vec<SessionUploadSlot>,
}

impl RtcSessionOffer {
    pub(super) fn new(offer: SdpOffer, upload_slots: Vec<SessionUploadSlot>) -> Self {
        Self {
            offer,
            upload_slots,
        }
    }

    pub(in crate::engine::media_transport) fn into_session_offer(self) -> SessionOffer {
        let mut sdp = self.offer.to_sdp_string();
        let media_line_start = sdp.find("\r\nm=").map_or(sdp.len(), |index| index + 2);
        sdp.insert_str(media_line_start, sdp::EOC_LINE);
        SessionOffer::new(sdp).with_upload_slots(self.upload_slots)
    }
}

#[derive(Debug)]
pub enum WorkerMediaControlBatch {
    ReceiverBwe(Vec<(usize, ReceiverBweTargetUpdate)>),
    ProducerActivity(Vec<(usize, ProducerRouteControl)>),
    ConsumerGates {
        source: TransportSourceKey,
        updates: Vec<(usize, TransportConsumerRoute, PacketLayerGate)>,
    },
    ConsumerFollowUp(Vec<(usize, ConsumerRouteControl)>),
}

#[derive(Debug)]
pub enum WorkerMediaControlBatchOutcome {
    Applied(Vec<TransportResult<()>>),
    Consumers(Vec<ConsumerRouteControlOutcome>),
}

pub enum RouteControlRequest {
    AddRelayTarget {
        source: TransportSourceKey,
        target_id: RelayTargetId,
        target: RelayPacketMailbox,
    },
    RemoveRelayTarget {
        source: TransportSourceKey,
        target_id: RelayTargetId,
    },
    SetRelayTargetActive {
        source: TransportSourceKey,
        target_id: RelayTargetId,
        active: bool,
    },
    SetRemoteSourceActivity {
        source: TransportSourceKey,
        update: SourceActivityUpdate,
    },
    RequestRemoteKeyframe {
        source: TransportSourceKey,
        target_id: RelayTargetId,
        rid: Option<Rid>,
        kind: KeyframeRequestKind,
    },
    SetRemoteSourcePacketGate {
        source: TransportSourceKey,
        target_id: RelayTargetId,
        packet_gate: PacketLayerGate,
    },
}

/// production command handled by the rtc packet-loop task
///
/// variants are grouped by ownership boundary: negotiation mutates str0m SDP
/// state, media commands mutate producer or consumer registrations, relay
/// commands mutate cross-worker fanout and observability commands read
/// worker-local snapshots
pub enum RtcWorkerCommand {
    /// create the initial capability-probe offer before media registration
    ///
    /// worker startup binds the shared UDP socket, then this command lazily creates
    /// session RTC state from its candidate address
    /// it rejects a pending offer, a committed initial answer or registered media
    CreateInitialSessionOffer {
        room_id: Arc<str>,
        session_key: TransportSessionKey,
        response: RtcWorkerResponse<RtcSessionOffer>,
    },
    /// drain a staged follow-up offer after media topology changed
    ///
    /// media add and remove commands stage the SDP work before this command
    /// runs
    /// this command hands the staged offer to the worker API and preserves the
    /// one-outstanding-offer rule owned by the worker
    CreateSessionRenegotiationOffer {
        session_key: TransportSessionKey,
        response: RtcWorkerResponse<RtcSessionOffer>,
    },
    /// read active-speaker sources from worker-local route-control state
    ///
    /// the result is a cold-path observation for room policy
    /// it does not mutate route state or packet-loop scheduling
    ActiveSpeakerSourceSnapshot {
        response: RtcWorkerResponse<Vec<ActiveSpeakerSource>>,
    },
    /// Read packet activity and active-speaker facts for selected sources.
    SourceDiagnosticsSnapshot {
        transport_media_ids: Vec<TransportMediaId>,
        response: RtcWorkerResponse<TransportSourceDiagnosticsSnapshot>,
    },
    /// accept the answer for the current pending local offer
    ///
    /// this commits str0m SDP state, marks the session dirty, refreshes
    /// negotiated producer parameters, registers remote candidate recovery hints
    /// and returns the producer details that became usable after the answer
    ApplySessionAnswer {
        session_key: TransportSessionKey,
        answer: ParsedSessionAnswer,
        response: RtcWorkerResponse<AppliedSessionAnswer>,
    },
    /// remove a session from worker state
    ///
    /// teardown removes rtc state, media handles, route destinations, demux
    /// indexes, bitrate counters and snapshot entries owned by the session
    CloseSession {
        session_key: TransportSessionKey,
        response: RtcWorkerResponse<()>,
    },
    /// remove one producer or consumer media registration owned by a session
    ///
    /// producer removal drops incoming bitrate tracking and the source route
    /// consumer removal drops local rewrite state and the destination route
    /// negotiated media removal may stage the next SDP offer before the handle
    /// leaves the public registry
    RemoveMedia {
        session_key: TransportSessionKey,
        transport_media_id: TransportMediaId,
        response: RtcWorkerResponse<()>,
    },
    /// resolve negotiated producer parameters for adapter tests
    ///
    /// this test-only command reads the answer-derived producer state after
    /// negotiation so adapter tests can assert the transport boundary without
    /// reaching into private registries
    #[cfg(test)]
    ResolveNegotiatedProducerParameters {
        session_key: TransportSessionKey,
        transport_media_id: TransportMediaId,
        response: RtcWorkerResponse<RouterRtpParameters>,
    },
    /// resolve the MID stored by a registered media handle
    ///
    /// returns `None` when this worker has no current handle for the id, including
    /// after media removal
    ResolveMediaMid {
        transport_media_id: TransportMediaId,
        response: RtcWorkerResponse<Option<String>>,
    },
    /// register one browser upload as worker-local producer media
    ///
    /// before the initial answer this can declare receive state directly in
    /// str0m
    /// after negotiation it stages a recv-only m-section plus pending
    /// receive identities, then registers bitrate counters and the producer
    /// media handle
    AddRecvMedia {
        session_key: TransportSessionKey,
        media_kind: MediaKind,
        rtp_parameters: RouterRtpParameters,
        response: RtcWorkerResponse<TransportMediaId>,
    },
    /// register one browser download as consumer media for a source
    ///
    /// the worker validates local or remote source ownership, stages or declares
    /// send-only media, registers the consumer handle and creates the packet-loop
    /// route destination
    /// remote sources install rollback-protected control so failed consumer
    /// setup does not leave stale relay state behind
    AddSendMedia {
        consumer_key: TransportSessionKey,
        media_kind: MediaKind,
        source: TransportSourceKey,
        remote_source_control: Option<RemoteSourceControl>,
        consumer_rtp_parameters: RouterRtpParameters,
        active: bool,
        response: RtcWorkerResponse<TransportMediaId>,
    },
    ApplyMediaControlBatch {
        batch: WorkerMediaControlBatch,
        response: RtcWorkerResponse<WorkerMediaControlBatchOutcome>,
    },
    RouteControl {
        request: RouteControlRequest,
        response: Option<RtcWorkerResponse<()>>,
    },
}
