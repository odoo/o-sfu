//! websocket controller for one upgraded socket
//!
//! this module bounds upgrade admission before handing the socket to
//! [`super::session::run`]

use std::sync::Arc;

use axum::{
    extract::{FromRef, State, ws::WebSocketUpgrade},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::warn;

use super::{admission::PreAuthWebSocketAdmissionRejection, io::MAX_CLIENT_FRAME_BYTES, session};
use crate::{
    config::UserConfig,
    core::prelude::SfuCore,
    runtime::{
        RuntimeMetrics, RuntimeState, options::RuntimeAuthConfig, request_origin::RequestOrigin,
        room::RoomManager, telemetry::schema::event as telemetry_event,
    },
};

pub(crate) struct WebSocketServices {
    pub(super) auth: RuntimeAuthConfig,
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
            auth: state.config.auth.clone(),
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
    let remote_address = Arc::<str>::from(origin.remote_address);
    let pre_auth_permit = match services
        .pre_auth_websocket_admission
        .try_acquire(Arc::clone(&remote_address))
    {
        Ok(permit) => permit,
        Err(rejection) => {
            reject_pre_auth_admission(&services, remote_address.as_ref(), rejection);
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
    };
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
    remote_address: &str,
    rejection: PreAuthWebSocketAdmissionRejection,
) {
    match rejection {
        PreAuthWebSocketAdmissionRejection::Global => {
            warn!(
                event = telemetry_event::WS_HANDSHAKE_REJECTED,
                remote_address,
                max_pre_auth_websocket_sessions = services.auth.max_pre_auth_websocket_sessions,
                "rejecting websocket upgrade because global pre-auth admission is full"
            );
        }
        PreAuthWebSocketAdmissionRejection::Origin => {
            warn!(
                event = telemetry_event::WS_HANDSHAKE_REJECTED,
                remote_address,
                max_pre_auth_websocket_sessions_per_origin =
                    services.auth.max_pre_auth_websocket_sessions_per_origin,
                "rejecting websocket upgrade because origin pre-auth admission is full"
            );
        }
    }
}
