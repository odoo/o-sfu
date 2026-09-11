//! Mailbox command projection into worker-local RTC handlers.
//!
//! [`handle_worker_command`] owns the exhaustive command projection while
//! focused modules own each state transition. [`WorkerCommandContext`] carries
//! immutable services without broadening access to worker state.

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Instant,
};

use tokio::sync::oneshot;

#[cfg(test)]
use super::publication;
use super::{
    super::{
        RtcWorkerConfig,
        commands::RtcWorkerCommand,
        state::{PacketLoopState, RtcSnapshotState, bitrate::BitrateRegistry},
    },
    media,
    negotiation::{self, OfferBootstrapConfig},
    session,
};
use crate::engine::{
    media_transport::{TransportMediaId, TransportResult, TransportSourceDiagnosticsSnapshot},
    metrics::{RtcMetricsRecorder, RuntimeMetrics},
};

pub struct WorkerCommandContext<'a> {
    pub bitrate_registry: &'a Arc<Mutex<BitrateRegistry>>,
    pub snapshot_state: &'a Arc<Mutex<RtcSnapshotState>>,
    pub candidate_addr: SocketAddr,
    pub now: Instant,
    pub config: &'a RtcWorkerConfig,
    pub runtime_metrics: &'a RuntimeMetrics,
    pub rtc_metrics: &'a RtcMetricsRecorder,
}

impl WorkerCommandContext<'_> {
    fn offer_bootstrap_config(&self) -> OfferBootstrapConfig<'_> {
        OfferBootstrapConfig {
            candidate_addr: self.candidate_addr,
            max_bitrate_out: self.config.bitrate_limits.max_bitrate_out(),
            video_bitrate_limits: self.config.video_bitrate_limits,
            profile: self.config.profile.as_ref(),
            media_quality_interval: self.config.media_quality_interval,
            metrics: self.runtime_metrics,
        }
    }

    fn recv_media_policy(&self) -> media::RecvMediaPolicy<'_> {
        media::RecvMediaPolicy {
            max_bitrate_in: self.config.bitrate_limits.max_bitrate_in(),
            video_bitrate_limits: self.config.video_bitrate_limits,
            profile: self.config.profile.as_ref(),
        }
    }
}

/// Applies one production command to worker-local RTC state.
#[expect(
    clippy::too_many_lines,
    reason = "one exhaustive command match keeps dispatch compiler checked and auditable"
)]
pub fn handle_worker_command(
    state: &mut PacketLoopState,
    context: &WorkerCommandContext<'_>,
    command: RtcWorkerCommand,
) {
    match command {
        RtcWorkerCommand::CreateInitialSessionOffer {
            room_id,
            session_key,
            response,
        } => respond(
            response,
            negotiation::worker_create_initial_session_offer(
                state,
                context.bitrate_registry,
                context.offer_bootstrap_config(),
                room_id,
                &session_key,
            ),
        ),
        RtcWorkerCommand::ActiveSpeakerSourceSnapshot { response } => respond(
            response,
            Ok(state.routes.active_speaker_sources(context.now)),
        ),
        RtcWorkerCommand::SourceDiagnosticsSnapshot {
            transport_media_ids,
            response,
        } => respond_source_diagnostics(state, &transport_media_ids, context.now, response),
        RtcWorkerCommand::CreateSessionRenegotiationOffer {
            session_key,
            response,
        } => respond(
            response,
            negotiation::worker_create_session_renegotiation_offer(state, &session_key),
        ),
        RtcWorkerCommand::ApplySessionAnswer {
            session_key,
            answer,
            response,
        } => respond(
            response,
            negotiation::worker_apply_session_answer(
                state,
                context.config.bitrate_limits.max_bitrate_in(),
                &session_key,
                answer,
            ),
        ),
        #[cfg(test)]
        RtcWorkerCommand::ResolveNegotiatedProducerParameters {
            session_key,
            transport_media_id,
            response,
        } => respond(
            response,
            publication::worker_resolve_negotiated_producer_parameters(
                state,
                &session_key,
                transport_media_id,
            ),
        ),
        RtcWorkerCommand::ResolveMediaMid {
            transport_media_id,
            response,
        } => respond(
            response,
            Ok(state
                .resolve_mid(transport_media_id)
                .map(|mid| mid.to_string())),
        ),
        RtcWorkerCommand::CloseSession {
            session_key,
            response,
        } => {
            session::worker_close_session(
                state,
                context.bitrate_registry,
                context.snapshot_state,
                &session_key,
                session::SessionCloseDisposition::OwnerClose,
                context.runtime_metrics,
            );
            respond(response, Ok(()));
        }
        RtcWorkerCommand::RemoveMedia {
            session_key,
            transport_media_id,
            response,
        } => respond(
            response,
            media::worker_remove_media(
                state,
                context.bitrate_registry,
                &session_key,
                transport_media_id,
            ),
        ),
        RtcWorkerCommand::AddRecvMedia {
            session_key,
            media_kind,
            rtp_parameters,
            response,
        } => respond(
            response,
            media::worker_add_recv_media(
                state,
                context.bitrate_registry,
                context.recv_media_policy(),
                &session_key,
                media_kind,
                &rtp_parameters,
            ),
        ),
        RtcWorkerCommand::AddSendMedia {
            consumer_key,
            media_kind,
            source,
            remote_source_control,
            consumer_rtp_parameters,
            active,
            response,
        } => respond(
            response,
            media::worker_add_send_media(
                state,
                media::AddSendMediaRequest {
                    consumer_key: &consumer_key,
                    media_kind,
                    source: &source,
                    remote_source_control,
                    consumer_rtp_parameters: &consumer_rtp_parameters,
                    active,
                },
            ),
        ),
        RtcWorkerCommand::ApplyMediaControlBatch { batch, response } => respond(
            response,
            Ok(media::apply_media_control_batch(
                state,
                context.rtc_metrics,
                context.config.bitrate_limits.max_bitrate_out(),
                context.now,
                batch,
            )),
        ),
        RtcWorkerCommand::RouteControl { request, response } => {
            media::apply_route_control_request(state, context.rtc_metrics, request, response);
        }
    }
}

fn respond<T>(response: oneshot::Sender<TransportResult<T>>, result: TransportResult<T>) {
    // Handler completion is independent of result delivery. A dropped request
    // future only makes this send fail.
    let _ = response.send(result);
}

fn respond_source_diagnostics(
    state: &PacketLoopState,
    transport_media_ids: &[TransportMediaId],
    now: Instant,
    response: oneshot::Sender<TransportResult<TransportSourceDiagnosticsSnapshot>>,
) {
    let activity = state.routes.source_activity_snapshot(
        transport_media_ids,
        now,
        &state.incoming_bitrate_counters,
    );
    respond(
        response,
        Ok(TransportSourceDiagnosticsSnapshot {
            activity,
            active_speaker_diagnostics: state
                .routes
                .active_speaker_diagnostics(transport_media_ids, now),
        }),
    );
}
