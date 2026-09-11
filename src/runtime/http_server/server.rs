//! HTTP listener lifecycle and graceful shutdown.

use std::{io, net::SocketAddr};

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::info;

use super::app;
use crate::runtime::{RuntimeState, telemetry::schema::event as telemetry_event};

/// Binds the configured HTTP address and serves until shutdown completes.
///
/// # Errors
///
/// Returns [`io::Error`] if binding or reading the listener address fails.
pub(crate) async fn serve_http(
    state: RuntimeState,
    shutdown_token: CancellationToken,
) -> io::Result<()> {
    let listener = TcpListener::bind(state.config.http.bind_address).await?;
    serve_http_on(listener, state, shutdown_token).await
}

/// Serves an existing listener with authorization bound to its actual address.
///
/// Shutdown waits for Axum's active connections to complete.
///
/// # Errors
///
/// Returns [`io::Error`] if reading the listener address fails.
pub(crate) async fn serve_http_on(
    listener: TcpListener,
    state: RuntimeState,
    shutdown_token: CancellationToken,
) -> io::Result<()> {
    let local_address = listener.local_addr()?;
    info!(
        event = telemetry_event::HTTP_LISTENER_READY,
        bind_address = %state.config.http.bind_address,
        local_address = %local_address,
        trust_proxy_headers = state.config.http.trust_proxy_headers,
        "booted HTTP and WebSocket listener"
    );
    axum::serve(
        listener,
        app(state, local_address).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_token.cancelled_owned())
    .await
}
