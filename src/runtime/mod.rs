//! Wires process services and drains them during shutdown.
//!
//! Request handlers receive [`RuntimeState`] without boot or teardown control.

use std::{future::Future, io, process, sync::Arc, time::Duration};

use anyhow::Result as AnyResult;
use thiserror::Error;
#[cfg(unix)]
use tokio::signal::unix::{SignalKind, signal};
use tokio::{
    net::TcpListener,
    runtime::Builder,
    signal::ctrl_c,
    task::JoinHandle,
    time::{MissedTickBehavior, interval, sleep},
};
use tokio_util::{
    sync::CancellationToken,
    task::{AbortOnDropHandle, TaskTracker},
};
use tracing::{info, warn};

use crate::{config::Config, core::prelude::SfuCore};

pub(crate) mod auth;
pub(crate) mod diagnostics;
pub(crate) mod http_server;
pub(crate) mod options;
pub(crate) mod request_origin;
#[cfg(test)]
#[path = "TESTS/support.rs"]
pub(super) mod test_support;
pub(crate) mod websocket_server;

use http_server::{serve_http, serve_http_on};
pub(crate) use o_sfu_core::{
    prelude::{ConnectionId, SessionBitrateLimits},
    server::{metrics, packet_sinks, room, transport as media_transport},
};
pub(crate) use o_sfu_telemetry::{self as telemetry, prometheus};
use options::{RuntimeConfig, effective_feature_flags};
use room::{RoomAdmissionPolicy, RoomManager, RoomRuntimePolicy};
use telemetry::{init_tracing, schema::event as telemetry_event};

pub(crate) use self::{
    media_transport::{MediaTransport, MediaTransportConfig, MediaTransportDeps},
    metrics::RuntimeMetrics,
    packet_sinks::RoomPacketSinkRegistry,
};

/// Failure to serve or fully drain a [`Runtime`].
#[derive(Debug, Error)]
pub enum ServeError {
    /// The listener or process shutdown signal failed.
    #[error(transparent)]
    Io(#[from] io::Error),
    /// The deadline elapsed before runtime drainage finished.
    #[error(
        "runtime shutdown exceeded its deadline with {remaining_sessions} WebSocket sessions remaining"
    )]
    ShutdownIncomplete {
        /// Tracked WebSocket sessions whose finalizers had not returned.
        remaining_sessions: usize,
    },
}

/// Process services and lifecycle configuration.
#[derive(Debug)]
pub struct Runtime {
    config: RuntimeConfig,
    room_manager: Arc<RoomManager>,
    metrics: Arc<RuntimeMetrics>,
    media_transport: MediaTransport,
}

/// Cloneable request dependencies without process lifecycle control.
#[derive(Debug, Clone)]
pub(super) struct RuntimeState {
    config: RuntimeConfig,
    room_manager: Arc<RoomManager>,
    media_transport: MediaTransport,
    sfu_core: SfuCore,
    metrics: Arc<RuntimeMetrics>,
    pre_auth_websocket_admission: websocket_server::PreAuthWebSocketAdmission,
    session_shutdown: CancellationToken,
    session_tasks: TaskTracker,
}

#[derive(Default)]
pub(super) struct RuntimeServices {
    metrics: Arc<RuntimeMetrics>,
    packet_sink_registry: Arc<RoomPacketSinkRegistry>,
}

impl Runtime {
    /// Builds the room manager and media workers from loaded configuration.
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] when the authentication key is malformed or
    /// shorter than the HS256 minimum, the diagnostics token is invalid, proxy
    /// mode lacks trusted proxies, listener limits are outside their supported
    /// ranges or the media transport cannot be built.
    pub fn new(config: &Config) -> AnyResult<Self> {
        Self::from_services(config, RuntimeServices::default())
    }

    #[cfg(feature = "testing-transport")]
    #[doc(hidden)]
    #[must_use]
    pub fn media_transport_for_test(&self) -> MediaTransport {
        self.media_transport.clone()
    }

    fn from_services(config: &Config, services: RuntimeServices) -> AnyResult<Self> {
        config.http.validate()?;
        config.diagnostics.validate()?;
        let runtime_config = RuntimeConfig::from_config(config)?;
        let media_transport = build_media_transport(config, &services)?;
        let room_runtime_policy = build_room_runtime_policy(config, &media_transport);
        info!(
            event = telemetry_event::RUNTIME_BOOT,
            rtc_udp_io_backend = config.transport.rtc_udp_io_backend.wire_name(),
            "runtime configuration loaded"
        );
        let room_manager = build_room_manager(
            room_runtime_policy,
            &services,
            config.user.room_reservation_ttl,
            config.user.departure_grace,
        );
        Ok(Self {
            config: runtime_config,
            room_manager,
            metrics: services.metrics,
            media_transport,
        })
    }

    /// Serves a caller-provided listener until `shutdown` resolves.
    ///
    /// Tokenless operator access follows the listener's actual local address.
    ///
    /// # Errors
    ///
    /// Returns [`ServeError::Io`] for serving failures or
    /// [`ServeError::ShutdownIncomplete`] when the drainage deadline expires.
    pub async fn serve_listener(
        self,
        listener: TcpListener,
        shutdown: impl Future<Output = ()>,
    ) -> Result<(), ServeError> {
        self.serve(
            |state, token| serve_http_on(listener, state, token),
            async move {
                shutdown.await;
                Ok(())
            },
        )
        .await
    }

    async fn serve<F, HttpServer, Shutdown>(
        self,
        http_server: F,
        shutdown: Shutdown,
    ) -> Result<(), ServeError>
    where
        F: FnOnce(RuntimeState, CancellationToken) -> HttpServer,
        HttpServer: Future<Output = io::Result<()>>,
        Shutdown: Future<Output = io::Result<()>>,
    {
        let timeout = Duration::from_millis(self.config.http.shutdown_timeout_ms);
        let tasks = RuntimeTasks::spawn(Arc::clone(&self.room_manager), self.media_transport);
        let state = RuntimeState::from_parts(
            self.config,
            self.room_manager,
            self.metrics,
            tasks.media_transport.clone(),
            tasks.session_shutdown.clone(),
            tasks.session_tasks.clone(),
        );
        let listener_shutdown = tasks.shutdown_token.child_token();
        let server = http_server(state, listener_shutdown.clone());
        tokio::pin!(server);
        tokio::pin!(shutdown);
        let (server_done, mut failure) = tokio::select! {
            result = &mut server => (true, result.err()),
            result = &mut shutdown => {
                listener_shutdown.cancel();
                (false, result.err())
            }
        };
        let deadline = sleep(timeout);
        tokio::pin!(deadline);
        if let Some(error) = &failure {
            warn!(?error, "runtime serving stopped with an error");
        }
        let session_tasks = tasks.session_tasks.clone();
        let teardown = async move {
            if !server_done && let Err(error) = server.await {
                warn!(?error, "HTTP server stopped with an error during shutdown");
                failure.get_or_insert(error);
            }
            tasks.shutdown().await;
            failure.map_or(Ok(()), |error| Err(ServeError::Io(error)))
        };
        tokio::select! {
            biased;
            () = &mut deadline => Err(ServeError::ShutdownIncomplete {
                remaining_sessions: session_tasks.len(),
            }),
            result = teardown => result,
        }
    }
}

/// Cancels process work when the server future is dropped.
struct RuntimeTasks {
    shutdown_token: CancellationToken,
    session_shutdown: CancellationToken,
    session_tasks: TaskTracker,
    source_packet_policy_sync: AbortOnDropHandle<()>,
    rooms_reservations_reaper: AbortOnDropHandle<()>,
    media_transport: MediaTransport,
}

impl RuntimeTasks {
    fn spawn(room_manager: Arc<RoomManager>, media_transport: MediaTransport) -> Self {
        let shutdown_token = CancellationToken::new();
        let source_packet_policy_sync = spawn_source_packet_policy_update_task(
            Arc::clone(&room_manager),
            media_transport.clone(),
            shutdown_token.child_token(),
        );
        let rooms_reservations_reaper =
            spawn_room_reservation_expiration_reaper(room_manager, shutdown_token.child_token());
        Self {
            session_shutdown: shutdown_token.child_token(),
            shutdown_token,
            session_tasks: TaskTracker::new(),
            source_packet_policy_sync: AbortOnDropHandle::new(source_packet_policy_sync),
            rooms_reservations_reaper: AbortOnDropHandle::new(rooms_reservations_reaper),
            media_transport,
        }
    }

    async fn shutdown(mut self) {
        self.session_tasks.close();
        self.session_shutdown.cancel();
        self.session_tasks.wait().await;
        self.shutdown_token.cancel();
        if let Err(error) = (&mut self.source_packet_policy_sync).await
            && !error.is_cancelled()
        {
            warn!(
                ?error,
                task = "source packet policy update",
                "runtime background task stopped unexpectedly"
            );
        }
        if let Err(error) = (&mut self.rooms_reservations_reaper).await
            && !error.is_cancelled()
        {
            warn!(
                ?error,
                task = "rooms reservation reaper",
                "runtime background task stopped unexpectedly"
            );
        }
        self.media_transport.shutdown().await;
    }
}

impl Drop for RuntimeTasks {
    fn drop(&mut self) {
        self.shutdown_token.cancel();
        self.media_transport.cancel();
    }
}

impl RuntimeState {
    fn from_parts(
        config: RuntimeConfig,
        rooms: Arc<RoomManager>,
        metrics: Arc<RuntimeMetrics>,
        media_transport: MediaTransport,
        session_shutdown: CancellationToken,
        session_tasks: TaskTracker,
    ) -> Self {
        let sfu_core = SfuCore::new(media_transport.clone(), Arc::clone(&rooms));
        let pre_auth_websocket_admission = websocket_server::PreAuthWebSocketAdmission::new(
            config.auth.max_pre_auth_websocket_sessions,
            config.auth.max_pre_auth_websocket_sessions_per_origin,
        );
        Self {
            config,
            room_manager: rooms,
            media_transport,
            sfu_core,
            metrics,
            pre_auth_websocket_admission,
            session_shutdown,
            session_tasks,
        }
    }
}

async fn shutdown_signal() -> io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate = signal(SignalKind::terminate())?;
        tokio::select! {
            result = ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    ctrl_c().await
}

/// Recomputes room packet policy after transport observations change.
fn spawn_source_packet_policy_update_task(
    rooms: Arc<RoomManager>,
    media_transport: MediaTransport,
    shutdown_token: CancellationToken,
) -> JoinHandle<()> {
    info!("booted source packet policy update task");
    let updates = media_transport.source_policy_subscription();
    tokio::spawn(async move {
        loop {
            let mut dirty_rooms = tokio::select! {
                biased;
                () = shutdown_token.cancelled() => return,
                dirty_rooms = updates.wait_for_update() => dirty_rooms,
            };
            dirty_rooms.extend(updates.take_pending_updates());
            rooms
                .sync_source_packet_selection_policies_for_runtime_ids(
                    &dirty_rooms,
                    &media_transport,
                )
                .await;
        }
    })
}

fn spawn_room_reservation_expiration_reaper(
    rooms: Arc<RoomManager>,
    shutdown_token: CancellationToken,
) -> JoinHandle<()> {
    info!("booted room reservation expiration reaper");
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(10));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                () = shutdown_token.cancelled() => return,
                _ = interval.tick() => {},
            };
            rooms.check_expired_room_reservations().await;
        }
    })
}

fn build_media_transport(config: &Config, services: &RuntimeServices) -> AnyResult<MediaTransport> {
    Ok(MediaTransport::build(
        MediaTransportConfig {
            worker_count: config.transport.rtc_media_worker_count,
            announced_ip: config.transport.announced_ip,
            bitrate_limits: SessionBitrateLimits::new(
                config.transport.max_bitrate_in,
                config.transport.max_bitrate_out,
            ),
            video_bitrate_limits: config.transport.video_bitrate_limits,
            rtc_port_range: config.transport.rtc_port_range,
            rtc_udp_io_backend: config.transport.rtc_udp_io_backend,
            codec_flags: config.codecs.flags,
            codec_preferences: config.codecs.preferences,
            media_quality_interval: config.telemetry.media_quality_interval,
        },
        MediaTransportDeps {
            packet_sink_registry: Arc::clone(&services.packet_sink_registry),
            metrics: Arc::clone(&services.metrics),
        },
    )?)
}

fn build_room_runtime_policy(
    config: &Config,
    media_transport: &MediaTransport,
) -> RoomRuntimePolicy {
    RoomRuntimePolicy::new(
        RoomAdmissionPolicy::new(config.user.room_size),
        effective_feature_flags(config.features),
        media_transport.router_rtp_capabilities(),
    )
    .with_room_worker_policy(config.transport.room_worker_policy)
    .with_media_limits(config.transport.room_media_limits)
    .with_video_adaptation_tuning(config.transport.video_adaptation_tuning)
}

fn build_room_manager(
    runtime_policy: RoomRuntimePolicy,
    services: &RuntimeServices,
    reservation_ttl: Duration,
    departure_grace: Duration,
) -> Arc<RoomManager> {
    Arc::new(RoomManager::new(
        runtime_policy,
        Arc::clone(&services.metrics),
        reservation_ttl,
        departure_grace,
    ))
}

/// # Errors
///
/// Returns an error when configuration, tracing, Tokio startup, serving or
/// shutdown fails.
pub fn run() -> AnyResult<()> {
    let config = Config::from_env()?;
    let _telemetry = init_tracing(&config.telemetry, process::id())?;
    let runtime = Runtime::new(&config)?;
    Ok(Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(runtime.serve(serve_http, shutdown_signal()))?)
}

#[cfg(test)]
#[path = "TESTS/runtime.rs"]
mod tests;
