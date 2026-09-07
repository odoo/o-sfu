//! Each [`RtcWorker`] creates a private current-thread executor inside a dedicated
//! OS thread. UDP ingress, relay packets and command APIs therefore mutate RTC
//! state in the same packet-loop task instead of entering the process-wide
//! work-stealing runtime.
//!
//! Read-side snapshots remain observational. They may race packet processing or
//! teardown and cannot authorize room state.

mod handlers;
mod lifecycle;

#[cfg(any(test, feature = "testing-transport"))]
#[path = "TESTS/support.rs"]
mod test_support;

use std::{
    fmt,
    sync::{Arc, Mutex, atomic::AtomicU64},
    thread,
};

#[cfg(feature = "internal-benchmarks")]
pub use handlers::apply_media_control_batch;
#[cfg(feature = "internal-benchmarks")]
pub(in crate::engine::media_transport::rtc) use handlers::guarded_pkt_gate;
pub(super) use handlers::{
    KeyframeRequestMode, KeyframeRequestTarget, SessionCloseDisposition, WorkerCommandContext,
    apply_src_decoder_ready, handle_worker_command, request_kf_for_target, worker_close_session,
};
#[cfg(any(test, feature = "internal-benchmarks"))]
use o_sfu_router::rtp::MediaStream as RouterRtpParameters;
#[cfg(any(test, feature = "internal-benchmarks"))]
use str0m::media::MediaKind;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[cfg(test)]
use super::commands::ParsedSessionAnswer;
#[cfg(any(test, feature = "internal-benchmarks"))]
use super::commands::RtcSessionOffer;
use super::{
    bitrate::BitrateRegistry,
    commands::{RemoteSourceControl, RouteControlRequest, RtcWorkerCommand},
    packet_loop::PacketLoopDelaySnapshot,
    relay_registry::{RelayPacketMailbox, RelayTargetId},
    state::RtcSnapshotState,
};
#[cfg(any(test, feature = "internal-benchmarks"))]
use crate::engine::media_transport::TransportAdapterError;
#[cfg(test)]
use crate::engine::media_transport::{AppliedSessionAnswer, SourcePolicySignal};
#[cfg(any(test, feature = "internal-benchmarks"))]
use crate::engine::media_transport::{SessionOffer, TransportMediaId, TransportSessionKey};
#[cfg(any(test, feature = "testing-transport"))]
use crate::engine::metrics::RuntimeMetrics;
use crate::engine::{
    media_transport::{SourceActivityUpdate, TransportRelayRouteAction, TransportSourceKey},
    metrics::RtcMetricsRecorder,
};

static NEXT_RELAY_TARGET_ID: AtomicU64 = AtomicU64::new(1);

pub(super) struct RtcWorkerHandle {
    pub(super) command_tx: mpsc::Sender<RtcWorkerCommand>,
    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) debug_handle: super::test_support::RtcWorkerDebugHandle,
    pub(super) relay_mailbox: RelayPacketMailbox,
    pub(super) bitrate_registry: Arc<Mutex<BitrateRegistry>>,
    pub(super) snapshot_state: Arc<Mutex<RtcSnapshotState>>,
    pub(super) packet_loop_delay: Arc<PacketLoopDelaySnapshot>,
}

impl fmt::Debug for RtcWorkerHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("RtcWorkerHandle");
        debug.field("command_tx", &self.command_tx);
        #[cfg(any(test, feature = "testing-transport"))]
        debug.field("debug_handle", &self.debug_handle);
        debug.field("relay_mailbox", &self.relay_mailbox);
        debug.finish_non_exhaustive()
    }
}

/// Worker-local RTC transport API.
pub struct RtcWorker {
    relay_target_id: RelayTargetId,
    handle: RtcWorkerHandle,
    shutdown: CancellationToken,
    thread: Option<thread::JoinHandle<()>>,
    #[cfg(any(test, feature = "testing-transport"))]
    pub metrics: Arc<RuntimeMetrics>,
    rtc_metrics: Arc<RtcMetricsRecorder>,
    #[cfg(test)]
    pub(super) source_policy_signal: SourcePolicySignal,
}

impl RtcWorker {
    #[cfg(any(test, feature = "internal-benchmarks"))]
    pub async fn create_initial_session_offer(
        &self,
        room_id: &str,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        let room_id: Arc<str> = Arc::from(room_id);
        self.request_worker(
            move |response| RtcWorkerCommand::CreateInitialSessionOffer {
                room_id,
                session_key: session_key.clone(),
                response,
            },
        )
        .await
        .map(RtcSessionOffer::into_session_offer)
    }

    #[cfg(test)]
    pub async fn create_session_renegotiation_offer(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<SessionOffer, TransportAdapterError> {
        self.request_worker(
            |response| RtcWorkerCommand::CreateSessionRenegotiationOffer {
                session_key: session_key.clone(),
                response,
            },
        )
        .await
        .map(RtcSessionOffer::into_session_offer)
    }

    #[cfg(test)]
    pub async fn apply_session_answer(
        &self,
        session_key: &TransportSessionKey,
        answer_sdp: &str,
    ) -> Result<AppliedSessionAnswer, TransportAdapterError> {
        let answer = ParsedSessionAnswer::parse(answer_sdp)?;
        self.request_worker(|response| RtcWorkerCommand::ApplySessionAnswer {
            session_key: session_key.clone(),
            answer,
            response,
        })
        .await
    }
    #[cfg(any(test, feature = "internal-benchmarks"))]
    pub async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        self.request_worker(|response| RtcWorkerCommand::CloseSession {
            session_key: session_key.clone(),
            response,
        })
        .await
    }
    #[cfg(any(test, feature = "internal-benchmarks"))]
    pub async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        self.request_worker(|response| RtcWorkerCommand::RemoveMedia {
            session_key: session_key.clone(),
            transport_media_id,
            response,
        })
        .await
    }

    #[cfg(test)]
    pub async fn negotiated_producer_parameters(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<RouterRtpParameters, TransportAdapterError> {
        self.request_worker(
            |response| RtcWorkerCommand::ResolveNegotiatedProducerParameters {
                session_key: session_key.clone(),
                transport_media_id,
                response,
            },
        )
        .await
    }

    #[cfg(any(test, feature = "internal-benchmarks"))]
    pub async fn add_recv_media(
        &self,
        session_key: &TransportSessionKey,
        media_kind: MediaKind,
        rtp_parameters: &RouterRtpParameters,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.request_worker(|response| RtcWorkerCommand::AddRecvMedia {
            session_key: session_key.clone(),
            media_kind,
            rtp_parameters: rtp_parameters.clone(),
            response,
        })
        .await
    }

    #[cfg(test)]
    pub async fn add_send_media(
        &self,
        consumer_key: &TransportSessionKey,
        media_kind: MediaKind,
        source: TransportSourceKey,
        consumer_rtp_parameters: &RouterRtpParameters,
        active: bool,
    ) -> Result<TransportMediaId, TransportAdapterError> {
        self.request_worker(|response| RtcWorkerCommand::AddSendMedia {
            consumer_key: consumer_key.clone(),
            media_kind,
            source,
            remote_source_control: None,
            consumer_rtp_parameters: consumer_rtp_parameters.clone(),
            active,
            response,
        })
        .await
    }

    /// Builds the handle `consumer` stores for a producer owned by `self`.
    ///
    /// The returned handle lets the consumer worker send best-effort keyframe
    /// and packet-gate updates back to the worker that owns the producer.
    pub fn remote_source_control(&self, consumer: &Self) -> RemoteSourceControl {
        RemoteSourceControl::new(
            self.handle.command_tx.clone(),
            consumer.relay_target_id,
            Arc::clone(&consumer.rtc_metrics),
        )
    }

    /// Builds source-worker route control for `self` as the relay target.
    ///
    /// Dispatch the request to the worker that owns `source`. Install requests
    /// carry `self`'s relay mailbox and every action carries `self`'s target ID.
    pub fn relay_route_request(
        &self,
        source: TransportSourceKey,
        action: TransportRelayRouteAction,
    ) -> RouteControlRequest {
        match action {
            TransportRelayRouteAction::Install => RouteControlRequest::AddRelayTarget {
                source,
                target_id: self.relay_target_id,
                target: self.handle.relay_mailbox.clone(),
            },
            TransportRelayRouteAction::Release => RouteControlRequest::RemoveRelayTarget {
                source,
                target_id: self.relay_target_id,
            },
            TransportRelayRouteAction::SetActivity(activity) => {
                RouteControlRequest::SetRelayTargetActive {
                    source,
                    target_id: self.relay_target_id,
                    active: activity.is_active(),
                }
            }
        }
    }

    pub(crate) fn remote_source_activity_request(
        source: TransportSourceKey,
        update: SourceActivityUpdate,
    ) -> RouteControlRequest {
        RouteControlRequest::SetRemoteSourceActivity { source, update }
    }
}

impl fmt::Debug for RtcWorker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RtcWorker")
            .field("relay_target_id", &self.relay_target_id)
            .finish_non_exhaustive()
    }
}
