//! websocket controller for one upgraded socket
//!
//! this module bounds upgrade admission before handing the socket to
//! [`super::session::run`]

use std::{net::IpAddr, sync::Arc};

use axum::{
    extract::{FromRef, State, ws::WebSocketUpgrade},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::warn;

use super::{
    admission::{PreAuthWebSocketAdmissionRejection, admit_rejection_log},
    io::MAX_CLIENT_FRAME_BYTES,
    session,
};
use crate::{
    config::UserConfig,
    core::prelude::SfuCore,
    runtime::{
        RuntimeMetrics, RuntimeState,
        request_origin::RequestOrigin,
        room::RoomManager,
        telemetry::{metrics::WsPreAuthRejection, schema::event as telemetry_event},
    },
};

pub(crate) struct WebSocketServices {
    pub(super) authentication_timeout_ms: u64,
    max_pre_auth_websocket_sessions: usize,
    max_pre_auth_websocket_sessions_per_origin: usize,
    pub(super) user: UserConfig,
    pub(super) room_manager: Arc<RoomManager>,
    pub(super) sfu_core: SfuCore,
    pub(super) metrics: Arc<RuntimeMetrics>,
    pub(super) shutdown: CancellationToken,
    sessions: TaskTracker,
    pre_auth_websocket_admission: super::PreAuthWebSocketAdmission,
}

impl FromRef<RuntimeState> for WebSocketServices {
    fn from_ref(state: &RuntimeState) -> Self {
        Self {
            authentication_timeout_ms: state.config.auth.authentication_timeout_ms,
            max_pre_auth_websocket_sessions: state.config.auth.max_pre_auth_websocket_sessions,
            max_pre_auth_websocket_sessions_per_origin: state
                .config
                .auth
                .max_pre_auth_websocket_sessions_per_origin,
            user: state.config.user,
            room_manager: Arc::clone(&state.room_manager),
            sfu_core: state.sfu_core.clone(),
            metrics: Arc::clone(&state.metrics),
            shutdown: state.session_shutdown.clone(),
            sessions: state.session_tasks.clone(),
            pre_auth_websocket_admission: state.pre_auth_websocket_admission.clone(),
        }
    }
}

pub(crate) async fn upgrade(
    State(services): State<WebSocketServices>,
    origin: RequestOrigin,
    websocket: WebSocketUpgrade,
) -> Response {
    let pre_auth_permit = match services
        .pre_auth_websocket_admission
        .try_acquire(origin.remote_address)
    {
        Ok(permit) => permit,
        Err(rejection) => {
            reject_pre_auth_admission(&services, origin.remote_address, rejection);
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
    };
    let remote_address = Arc::<str>::from(format_remote_address(origin.remote_address));
    let session_task = services.sessions.token();
    websocket
        .max_message_size(MAX_CLIENT_FRAME_BYTES)
        .max_frame_size(MAX_CLIENT_FRAME_BYTES)
        .on_upgrade(move |socket| async move {
            session::run(socket, services, remote_address, pre_auth_permit).await;
            drop(session_task);
        })
}

fn reject_pre_auth_admission(
    services: &WebSocketServices,
    remote_address: Option<IpAddr>,
    rejection: PreAuthWebSocketAdmissionRejection,
) {
    services
        .metrics
        .record_ws_pre_auth_rejection(match rejection {
            PreAuthWebSocketAdmissionRejection::Global => WsPreAuthRejection::Global,
            PreAuthWebSocketAdmissionRejection::Origin => WsPreAuthRejection::Origin,
        });
    let Some(suppressed_rejections) = admit_rejection_log() else {
        return;
    };
    let remote_address = format_remote_address(remote_address);
    match rejection {
        PreAuthWebSocketAdmissionRejection::Global => {
            warn!(
                event = telemetry_event::WS_HANDSHAKE_REJECTED,
                remote_address,
                suppressed_rejections,
                max_pre_auth_websocket_sessions = services.max_pre_auth_websocket_sessions,
                "rejecting websocket upgrade because global pre-auth admission is full"
            );
        }
        PreAuthWebSocketAdmissionRejection::Origin => {
            warn!(
                event = telemetry_event::WS_HANDSHAKE_REJECTED,
                remote_address,
                suppressed_rejections,
                max_pre_auth_websocket_sessions_per_origin =
                    services.max_pre_auth_websocket_sessions_per_origin,
                "rejecting websocket upgrade because origin pre-auth admission is full"
            );
        }
    }
}

fn format_remote_address(address: Option<IpAddr>) -> String {
    address.map_or_else(|| "unknown".to_owned(), |address| address.to_string())
}
