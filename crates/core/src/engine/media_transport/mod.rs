//! Runtime media transport boundary used by core.
//!
//! This module contains the service surface that room, server and
//! [`crate::prelude::SfuCore`] code use when media work must happen outside the
//! pure router. Callers ask the media transport to create SDP offers, apply
//! answers, publish or consume media, close sessions, read diagnostics
//! snapshots and subscribe to source-policy wakeups.
//!
//! The boundary exposes the opaque [`MediaTransport`] handle, narrow
//! construction inputs for the server runtime and transport operations that let
//! higher layers express intent without knowing about RTC workers, Str0m state,
//! UDP sockets or worker-local relay routing
//!
//! Code above this module should depend on [`MediaTransport`]. Code below this
//! module, especially the RTC engine, may deal with worker-local state machines
//! and packet-loop details. Keeping that split explicit prevents room and
//! signaling code from growing knowledge of the concrete WebRTC
//! implementation.

mod build;
mod config;
mod policy_invalidation;
mod route_control;
mod rtc;
mod teardown;
#[cfg(any(test, feature = "testing-transport", feature = "internal-benchmarks"))]
#[path = "TESTS/test_support/mod.rs"]
pub mod test_support;
mod types;
mod workers;

#[cfg(any(test, feature = "testing-transport"))]
use std::sync::atomic::AtomicUsize;
use std::{ptr, sync::Arc};

pub use build::MediaTransportBuildError;
pub use config::{MediaTransportConfig, MediaTransportDeps};
use o_sfu_router::{
    MediaKind,
    rtp::{MediaCapabilities, MediaStream as RouterRtpParameters},
};
pub use policy_invalidation::{SourcePolicySignal, SourcePolicyUpdateSubscription};
pub(crate) use route_control::{
    ConsumerRouteControl, ConsumerRouteControlOutcome, MediaControlPlan,
    TransportSourceActivityEffect,
};
pub(in crate::engine::media_transport) use route_control::{
    ConsumerRouteControlFailure, ProducerRouteControl,
};
pub(crate) use teardown::TransportTeardown;
#[cfg(feature = "internal-benchmarks")]
pub mod benchmark_support {
    pub use super::rtc::benchmark_support::*;
}
#[cfg(any(test, fuzzing))]
pub mod fuzz_support {
    pub use super::rtc::{
        client_rtp_capabilities_from_answer, fuzz_support::route_packet_loop_ingress_demux,
    };
}
use rtc::{
    ParsedSessionAnswer, RtcSessionOffer, RtcWorker, RtcWorkerCommand, RtcWorkerResponse,
    RtpProfile,
};
use tracing::warn;
pub use types::{
    ActiveSpeakerActivityReason, ActiveSpeakerActivityState, ActiveSpeakerSource,
    ActiveSpeakerSourceDiagnostic, AppliedProducer, AppliedSessionAnswer, ConsumerActivity,
    ProducerActivity, ReceiverBandwidthSnapshot, ReceiverBweTargetUpdate, RelayRouteActivity,
    SessionOffer, SessionUploadEncoding, SessionUploadSlot, SourcePacketGate,
    TransportAdapterError, TransportBitrateSnapshot, TransportConsumerRoute,
    TransportHealthSnapshot, TransportMediaId, TransportQualitySample, TransportQualitySnapshot,
    TransportRelayRouteAction, TransportRelayRouteEffect, TransportResult, TransportRidActivity,
    TransportSessionHealth, TransportSessionKey, TransportSourceActivity,
    TransportSourceDiagnosticsSnapshot, TransportSourceKey, TransportWorkerPressureSnapshot,
};
pub(crate) use types::{SourceActivityRevision, SourceActivityUpdate};

use self::workers::signaling_to_str0m_media_kind;
use crate::engine::metrics::RuntimeMetrics;

/// Opaque runtime media transport handle.
///
/// [`MediaTransport`] is the handle server code and [`crate::prelude::SfuCore`]
/// should hold.
/// It hides the production RTC worker topology. Callers express intent through
/// inherent methods and must not branch on concrete worker internals.
///
/// The handle also centralizes warning logs for failed transport effects. Inner
/// backends return typed errors, while this boundary adds stable diagnostic
/// context such as session keys, media ids and SDP lengths.
#[derive(Debug, Clone)]
pub struct MediaTransport {
    workers: Arc<[RtcWorker]>,
    profile: Arc<RtpProfile>,
    metrics: Arc<RuntimeMetrics>,
    #[cfg(test)]
    media_control_batches: test_support::MediaControlBatchLog,
    #[cfg(any(test, feature = "testing-transport"))]
    source_diagnostics_requests: Arc<AtomicUsize>,
    /// Coalesces transport observations for room policy.
    source_policy_signal: SourcePolicySignal,
}

#[derive(Debug, Clone, Copy)]
enum TransportCommandOp {
    CreateInitialSessionOffer,
    CreateSessionRenegotiationOffer,
    ApplySessionAnswer,
    PublishMedia,
    ConsumeMedia,
}

fn warn_session_command_failed(
    session_key: &TransportSessionKey,
    op: TransportCommandOp,
    error: TransportAdapterError,
) {
    warn!(
        ?session_key,
        ?op,
        ?error,
        "media transport worker command failed"
    );
}

impl MediaTransport {
    /// Cancels every RTC worker without waiting for termination.
    pub fn cancel(&self) {
        for worker in self.workers.iter() {
            worker.cancel();
        }
    }

    /// Cancels every RTC worker and waits for its thread to terminate.
    pub async fn shutdown(&self) {
        self.cancel();
        for worker in self.workers.iter() {
            worker.wait_for_shutdown().await;
        }
    }

    /// returns the router capability snapshot compiled from the RTC wire profile
    #[must_use]
    pub fn router_rtp_capabilities(&self) -> MediaCapabilities {
        self.profile.router_capabilities()
    }

    async fn request_session_command<T>(
        &self,
        session_key: &TransportSessionKey,
        build: impl FnOnce(RtcWorkerResponse<T>) -> RtcWorkerCommand,
        log_error: impl FnOnce(TransportAdapterError),
    ) -> Result<T, TransportAdapterError> {
        let result = match self.require_worker_for_user(session_key) {
            Ok(worker) => worker.request_worker(build).await,
            Err(error) => Err(error),
        };
        if let Err(error) = &result {
            log_error(*error);
        }
        result
    }

    /// Creates the first SDP offer for a transport session.
    ///
    /// The session must already be assigned to a transport worker by its
    /// [`TransportSessionKey`]. The returned offer is transport state that
    /// callers should send to the browser unchanged.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError`] when the session cannot be addressed or
    /// the active backend cannot create the offer.
    pub async fn create_initial_session_offer(
        &self,
        room_id: &str,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        let room_id: Arc<str> = Arc::from(room_id);
        self.request_session_command(
            session_key,
            move |response| RtcWorkerCommand::CreateInitialSessionOffer {
                room_id,
                session_key: session_key.clone(),
                response,
            },
            |error| {
                warn_session_command_failed(
                    session_key,
                    TransportCommandOp::CreateInitialSessionOffer,
                    error,
                );
            },
        )
        .await
        .map(RtcSessionOffer::into_session_offer)
    }

    /// Creates a new SDP offer after transport media state changed.
    ///
    /// Backends reject sessions that cannot renegotiate in their current state.
    /// Callers should treat the returned offer as replacing any older pending
    /// transport offer for the same session.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError`] when the session cannot be addressed or
    /// the active backend cannot create a renegotiation offer.
    pub async fn create_session_renegotiation_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        self.request_session_command(
            session_key,
            |response| RtcWorkerCommand::CreateSessionRenegotiationOffer {
                session_key: session_key.clone(),
                response,
            },
            |error| {
                warn_session_command_failed(
                    session_key,
                    TransportCommandOp::CreateSessionRenegotiationOffer,
                    error,
                );
            },
        )
        .await
        .map(RtcSessionOffer::into_session_offer)
    }

    /// Applies a browser SDP answer to a pending transport offer.
    ///
    /// The answer can reveal transport-derived producer facts such as mapped
    /// RTP parameters. Those facts are returned so room code can commit staged
    /// media using values observed by the transport.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError`] when the session cannot be addressed,
    /// the answer is invalid or the active backend cannot apply it.
    pub async fn apply_session_answer(
        &self,
        session_key: &TransportSessionKey,
        answer_sdp: &str,
    ) -> Result<AppliedSessionAnswer, TransportAdapterError> {
        async {
            let worker = self.require_worker_for_user(session_key)?;
            let answer = ParsedSessionAnswer::parse(answer_sdp)?;
            worker
                .request_worker(|response| RtcWorkerCommand::ApplySessionAnswer {
                    session_key: session_key.clone(),
                    answer,
                    response,
                })
                .await
        }
        .await
        .inspect_err(|error| {
            warn!(
                ?session_key,
                op = ?TransportCommandOp::ApplySessionAnswer,
                answer_len = answer_sdp.len(),
                ?error,
                "media transport failed to apply session answer"
            );
        })
    }

    /// Declares a new producer on a transport session.
    ///
    /// `rtp_parameters` must come from router media state accepted by the core.
    /// The returned [`TransportMediaId`] addresses the backend-local producer
    /// for later route, activity and cleanup operations.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError`] when the session cannot be addressed,
    /// the RTP parameters are invalid or the active backend cannot create the
    /// producer.
    pub async fn publish_media(
        &self,
        session_key: &TransportSessionKey,
        media_kind: MediaKind,
        rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.request_session_command(
            session_key,
            |response| RtcWorkerCommand::AddRecvMedia {
                session_key: session_key.clone(),
                media_kind: signaling_to_str0m_media_kind(media_kind),
                rtp_parameters: rtp_parameters.clone(),
                response,
            },
            |error| {
                warn!(
                    ?session_key,
                    op = ?TransportCommandOp::PublishMedia,
                    ?media_kind,
                    mid = rtp_parameters.mid(),
                    ?error,
                    "media transport worker command failed"
                );
            },
        )
        .await
    }

    /// Declares a new consumer route from a source session to a consumer session.
    ///
    /// The source and consumer sessions must belong to the same room instance.
    /// Cross-worker routing is an implementation detail hidden behind this
    /// method. The initial activity controls whether the packet loop can
    /// forward packets before later room policy updates arrive.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError`] when either session cannot be
    /// addressed, the source media id is unknown or the active backend cannot
    /// create the consumer.
    pub async fn consume_media(
        &self,
        consumer_session_key: &TransportSessionKey,
        media_kind: MediaKind,
        source_session_key: &TransportSessionKey,
        source_media_id: TransportMediaId,
        consumer_rtp_parameters: &RouterRtpParameters,
        initial_activity: ConsumerActivity,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        let result = async {
            Self::ensure_same_room(consumer_session_key, source_session_key)?;
            let consumer_worker = self.require_worker_for_user(consumer_session_key)?;
            let source_worker = self.require_worker_for_user(source_session_key)?;
            let remote_source_control = (!ptr::eq(consumer_worker, source_worker))
                .then(|| source_worker.remote_source_control(consumer_worker));
            consumer_worker
                .request_worker(|response| RtcWorkerCommand::AddSendMedia {
                    consumer_key: consumer_session_key.clone(),
                    media_kind: signaling_to_str0m_media_kind(media_kind),
                    source: TransportSourceKey::new(source_session_key.clone(), source_media_id),
                    remote_source_control,
                    consumer_rtp_parameters: consumer_rtp_parameters.clone(),
                    active: initial_activity.is_active(),
                    response,
                })
                .await
        }
        .await;
        if let Err(error) = &result {
            warn!(
                ?consumer_session_key,
                ?source_session_key,
                ?source_media_id,
                op = ?TransportCommandOp::ConsumeMedia,
                ?media_kind,
                mid = consumer_rtp_parameters.mid(),
                initial_active = initial_activity.is_active(),
                ?error,
                "media transport worker command failed"
            );
        }
        result
    }

    /// Applies a room relay route mutation.
    ///
    /// The transport may install, release or gate the packet-loop relay target.
    /// Room state remains the lifecycle owner.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError`] when the referenced route cannot be
    /// addressed or the active backend cannot apply the relay mutation.
    pub(crate) async fn apply_relay_route_effect(
        &self,
        effect: &TransportRelayRouteEffect,
    ) -> Result<(), TransportAdapterError> {
        self.execute_relay_route_effect(effect)
            .await
            .inspect_err(|error| {
                warn!(
                source = ?effect.source,
                target_media_worker_id = effect.target_media_worker_id.as_usize(),
                action = ?effect.action,
                ?error,
                "media transport failed to apply relay route effect"
                );
            })
    }

    /// Applies one activity revision to a remote source.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError`] when the target worker is unavailable
    /// or rejects the update.
    pub(crate) async fn apply_remote_source_activity_effect(
        &self,
        effect: &TransportSourceActivityEffect,
    ) -> Result<(), TransportAdapterError> {
        self.execute_remote_source_activity_effect(effect)
            .await
            .inspect_err(|error| {
                warn!(
                    source = ?effect.source,
                    target_media_worker_id = effect.target_media_worker_id.as_usize(),
                    update = ?effect.update,
                    ?error,
                    "media transport failed to apply remote source activity"
                );
            })
    }

    /// Subscribes to source-policy invalidation signals emitted by transport
    /// workers.
    #[must_use]
    pub fn source_policy_subscription(&self) -> SourcePolicyUpdateSubscription {
        self.source_policy_signal.subscribe()
    }
}

#[cfg(test)]
#[expect(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;
