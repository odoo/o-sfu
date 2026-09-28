use std::ptr;

use tracing::warn;

use super::{
    MediaTransport, TransportAdapterError, TransportMediaId, TransportRelayRouteAction,
    TransportSessionKey, TransportSourceKey, rtc::RtcWorkerCommand,
};
use crate::engine::MediaWorkerId;

#[derive(Debug)]
pub(crate) enum TransportTeardown {
    CloseSession {
        session_key: TransportSessionKey,
    },
    RemoveMedia {
        session_key: TransportSessionKey,
        transport_media_id: TransportMediaId,
    },
    ReleaseRelayRoute {
        source: TransportSourceKey,
        target_media_worker_id: MediaWorkerId,
    },
}

impl TransportTeardown {
    pub(crate) fn session_key(&self) -> &TransportSessionKey {
        match self {
            Self::CloseSession { session_key } | Self::RemoveMedia { session_key, .. } => {
                session_key
            }
            Self::ReleaseRelayRoute { source, .. } => source.session_key(),
        }
    }
}

impl MediaTransport {
    /// Runs teardown idempotently and continues after terminal failures.
    ///
    /// Each failed media or relay item escalates once to its session because
    /// partial worker state may remain. Errors are consumed so one failure cannot
    /// suppress later cleanup.
    pub(crate) async fn teardown(&self, teardowns: impl IntoIterator<Item = TransportTeardown>) {
        for teardown in teardowns {
            let (session_key, result, transport_media_id, target_media_worker_id) = match &teardown
            {
                TransportTeardown::CloseSession { session_key } => (
                    session_key,
                    self.close_session(session_key).await,
                    None,
                    None,
                ),
                TransportTeardown::RemoveMedia {
                    session_key,
                    transport_media_id,
                } => (
                    session_key,
                    self.remove_media(session_key, *transport_media_id).await,
                    Some(*transport_media_id),
                    None,
                ),
                TransportTeardown::ReleaseRelayRoute {
                    source,
                    target_media_worker_id,
                } => (
                    source.session_key(),
                    self.release_relay_route(source, *target_media_worker_id)
                        .await,
                    Some(source.transport_media_id()),
                    Some(*target_media_worker_id),
                ),
            };
            let Err(error) = result else {
                continue;
            };
            self.metrics.record_transport_cleanup_failure();
            warn!(
                ?session_key,
                ?transport_media_id,
                ?target_media_worker_id,
                ?error,
                "media transport teardown reached terminal failure"
            );
            // `RemoveMedia` or `ReleaseRelayRoute` failure may leave worker state behind.
            // Escalate once to `CloseSession`, whose own failure is already terminal.
            if transport_media_id.is_some()
                && let Err(fallback_error) = self.close_session(session_key).await
            {
                warn!(
                    ?session_key,
                    ?transport_media_id,
                    ?target_media_worker_id,
                    ?fallback_error,
                    "media transport teardown session fallback failed"
                );
            }
        }
    }

    async fn close_session(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        let worker = self.require_worker_for_user(session_key)?;
        worker
            .request_worker(|response| RtcWorkerCommand::CloseSession {
                session_key: session_key.clone(),
                response,
            })
            .await
    }

    pub(super) async fn remove_media(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        let worker = self.require_worker_for_user(session_key)?;
        worker
            .request_worker(|response| RtcWorkerCommand::RemoveMedia {
                session_key: session_key.clone(),
                transport_media_id,
                response,
            })
            .await
    }

    async fn release_relay_route(
        &self,
        source: &TransportSourceKey,
        target_media_worker_id: MediaWorkerId,
    ) -> Result<(), TransportAdapterError> {
        let source_worker = self.require_worker_for_user(source.session_key())?;
        // The target handle remains in the pool after its thread exits. Its
        // relay ID still identifies the route that the source worker must remove.
        let Some(target_worker) = self.workers.get(target_media_worker_id.as_usize()) else {
            return Ok(());
        };
        if ptr::eq(source_worker, target_worker) {
            return Ok(());
        }
        let request =
            target_worker.relay_route_request(source.clone(), TransportRelayRouteAction::Release);
        source_worker
            .request_worker(|response| RtcWorkerCommand::RouteControl {
                request,
                response: Some(response),
            })
            .await
    }
}
