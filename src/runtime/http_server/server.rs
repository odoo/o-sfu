//! HTTP listener admission, header deadlines and graceful shutdown.

use std::{
    io,
    net::SocketAddr,
    pin::{Pin, pin},
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use axum::{Router, extract::ConnectInfo, serve::Listener};
use hyper::{body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
    time::{Instant, sleep_until},
};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
use tracing::{debug, info, warn};

use super::app;
use crate::{
    config::HttpConfig,
    runtime::{RuntimeMetrics, RuntimeState, telemetry::schema::event as telemetry_event},
};

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
/// Shutdown stops acceptance and drains HTTP requests. Upgraded WebSocket connections
/// retain their connection permits and follow the runtime's session shutdown.
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
    let config = state.config.http.clone();
    let metrics = Arc::clone(&state.metrics);
    serve_connections(
        listener,
        app(state, local_address),
        config,
        metrics,
        shutdown_token,
    )
    .await;
    Ok(())
}

/// Owns HTTP connection tasks so cancelling the listener also closes their sockets.
async fn serve_connections(
    mut listener: TcpListener,
    router: Router,
    config: HttpConfig,
    metrics: Arc<RuntimeMetrics>,
    shutdown: CancellationToken,
) {
    let permits = Arc::new(Semaphore::new(config.max_http_connections));
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => break,
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = result {
                    warn!(?error, "HTTP connection task failed");
                }
            },
            (stream, remote_address) = Listener::accept(&mut listener) => {
                let accepted_at = Instant::now();
                let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
                    metrics.record_http_connection_rejection();
                    continue;
                };
                connections.spawn(serve_connection(
                    AdmittedSocket { stream, _permit: permit },
                    router.clone(),
                    remote_address,
                    accepted_at + config.header_read_timeout,
                    config.header_read_timeout,
                    shutdown.clone(),
                ));
            }
        }
    }
    drop(listener);
    while let Some(result) = connections.join_next().await {
        if let Err(error) = result {
            warn!(?error, "HTTP connection task failed during shutdown");
        }
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Hyper must retain the socket permit while polling the connection and transfer it to upgraded IO"
)]
async fn serve_connection(
    socket: AdmittedSocket,
    router: Router,
    remote_address: SocketAddr,
    first_header_deadline: Instant,
    header_timeout: Duration,
    shutdown: CancellationToken,
) {
    if Instant::now() >= first_header_deadline {
        return;
    }
    let first_headers = CancellationToken::new();
    let received_headers = first_headers.clone();
    let service = service_fn(move |mut request: hyper::Request<Incoming>| {
        // Ready socket IO can outrun timer notifications after a scheduling
        // stall. First headers must meet the deadline before any router effects.
        let expired = !received_headers.is_cancelled() && Instant::now() >= first_header_deadline;
        if !expired {
            received_headers.cancel();
        }
        let router = router.clone();
        async move {
            if expired {
                return Err(io::Error::from(io::ErrorKind::TimedOut));
            }
            request.extensions_mut().insert(ConnectInfo(remote_address));
            router
                .oneshot(request)
                .await
                .map_err(|never| match never {})
        }
    });
    let mut builder = http1::Builder::new();
    builder
        .timer(TokioTimer::new())
        .header_read_timeout(header_timeout);
    let mut connection = pin!(
        builder
            .serve_connection(TokioIo::new(socket), service)
            .with_upgrades()
    );
    let mut deadline = pin!(sleep_until(first_header_deadline));
    let mut awaiting_headers = true;
    let mut draining = false;
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled(), if !draining => {
                if !first_headers.is_cancelled() {
                    return;
                }
                draining = true;
                connection.as_mut().graceful_shutdown();
            }
            () = first_headers.cancelled(), if awaiting_headers => {
                awaiting_headers = false;
            }
            // Hyper starts its header timer when polled. This deadline includes
            // the time between acceptance and the connection task's first poll.
            () = &mut deadline, if awaiting_headers => return,
            result = connection.as_mut() => {
                if let Err(error) = result {
                    debug!(?error, "HTTP connection closed");
                }
                return;
            }
        }
    }
}

/// Couples admission to the socket because Hyper transfers the IO into an
/// upgraded WebSocket before its HTTP connection future completes.
struct AdmittedSocket {
    stream: TcpStream,
    _permit: OwnedSemaphorePermit,
}

impl AsyncRead for AdmittedSocket {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_read(context, buffer)
    }
}

impl AsyncWrite for AdmittedSocket {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write(context, buffer)
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(context)
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(context)
    }

    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write_vectored(context, buffers)
    }
}

#[cfg(test)]
#[path = "TESTS/server.rs"]
mod tests;
