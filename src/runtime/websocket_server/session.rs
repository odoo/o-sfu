use std::{str, sync::Arc, time::Duration};

use axum::{
    Error as AxumError,
    extract::ws::{Message, WebSocket},
};
use futures_util::StreamExt;
use o_sfu_protocol::wire::{ClientEnvelope, WebSocketCloseCode as CloseCode};
use tokio::time::{Instant, sleep_until};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, debug, field, info, info_span, instrument, warn};

use super::{
    WsReader, WsWriter,
    admission::PreAuthWebSocketPermit,
    controller::WebSocketServices,
    handshake::{self, AuthenticatedJoin, HandshakeError, WebSocketAuth},
    io::{close_writer_bounded, send_message_bounded, send_user_output_bounded},
};
use crate::{
    application::user_session::{User, UserError, UserOutput},
    config::UserConfig,
    core::server::room::{
        JoinUserRequest, RoomManagerJoinError, UserOutbound, UserOutboundEvent,
        UserOutboundQueueLimits, UserOutboundReceiver, UserOutboundSender,
    },
    runtime::{
        metrics::{RuntimeMetrics, WsSessionLoopExitReason as LoopExit},
        telemetry::{
            self,
            schema::{event as telemetry_event, field as telemetry_field},
        },
        websocket_server::{ClientBatchDecodeFailureKind, decode_client_batch},
    },
};

struct AuthenticatedSession {
    _proof: WebSocketAuth,
    writer: WsWriter,
    reader: WsReader,
    outbound: UserOutboundReceiver,
    user: User,
    user_config: UserConfig,
    metrics: Arc<RuntimeMetrics>,
    shutdown: CancellationToken,
}

enum SessionExit {
    BeforeLoop(Option<CloseCode>),
    Loop(LoopExit, Option<CloseCode>),
}

impl SessionExit {
    const fn closing(reason: LoopExit, code: CloseCode) -> Self {
        Self::Loop(reason, Some(code))
    }
}

pub(super) async fn run(
    socket: WebSocket,
    services: WebSocketServices,
    remote: Arc<str>,
    permit: PreAuthWebSocketPermit,
) {
    async move {
        Span::current().record(
            telemetry_field::REMOTE_ADDRESS,
            field::display(remote.as_ref()),
        );
        services.metrics.record_ws_connection_accepted();
        let handshake_span = telemetry::ws_handshake_span();
        handshake_span.record(
            telemetry_field::REMOTE_ADDRESS,
            field::display(remote.as_ref()),
        );
        if let Some(session) = establish(socket, services, remote, permit)
            .instrument(handshake_span)
            .await
        {
            session.serve().await;
        }
    }
    .instrument(telemetry::ws_upgrade_span())
    .await;
}

async fn establish(
    mut socket: WebSocket,
    services: WebSocketServices,
    remote: Arc<str>,
    permit: PreAuthWebSocketPermit,
) -> Option<AuthenticatedSession> {
    let _guard = services.metrics.track_ws_handshake();
    let auth = {
        let _guard = services.metrics.track_ws_authentication();
        handshake::authenticate(&services, &mut socket, remote.as_ref()).await
    };
    let (mut writer, reader) = socket.split();
    let join = match auth {
        Ok(join) => join,
        Err(HandshakeError::PeerClosed) => return None,
        Err(HandshakeError::Rejected(code)) => {
            handshake::reject(
                &services,
                &mut writer,
                code,
                remote.as_ref(),
                "rejecting websocket during authentication",
            )
            .await;
            return None;
        }
        Err(HandshakeError::Shutdown) => {
            close_writer_bounded(&mut writer, CloseCode::Leaving).await;
            return None;
        }
    };
    drop(permit);
    if services.shutdown.is_cancelled() {
        close_writer_bounded(&mut writer, CloseCode::Leaving).await;
        return None;
    }
    let (proof, user, outbound) = admit(&services, join, remote, &mut writer).await?;
    let mut session = AuthenticatedSession {
        _proof: proof,
        writer,
        reader,
        outbound,
        user,
        user_config: services.user,
        metrics: Arc::clone(&services.metrics),
        shutdown: services.shutdown,
    };
    session.metrics.record_ws_user_joined();
    session.record_current_span();
    if session.shutdown.is_cancelled() {
        session
            .finish(SessionExit::BeforeLoop(Some(CloseCode::Leaving)))
            .await;
        return None;
    }
    session.start().await
}

#[instrument(
    name = "room.join",
    skip_all,
    fields(room_id = %room.uuid(), user_id = %claims.user_id.path_segment())
)]
async fn admit(
    services: &WebSocketServices,
    AuthenticatedJoin {
        room,
        claims,
        proof,
    }: AuthenticatedJoin,
    remote: Arc<str>,
    writer: &mut WsWriter,
) -> Option<(WebSocketAuth, User, UserOutboundReceiver)> {
    let user_id = claims.user_id;
    let limits = UserOutboundQueueLimits::new(
        services.user.outbound_queue_capacity,
        services.user.outbound_queue_byte_capacity,
    );
    let (outbound_tx, outbound) =
        UserOutboundSender::channel_with_limits(limits, Arc::clone(&services.metrics));
    match services
        .sfu_core
        .admit_user(
            room.uuid(),
            JoinUserRequest {
                user_id: user_id.clone(),
                label: claims.label,
                permissions: claims.permissions.unwrap_or_default(),
                sender: outbound_tx,
            },
        )
        .await
    {
        Ok(session) => Some((proof, User::new(session, remote), outbound)),
        Err(_error) if services.shutdown.is_cancelled() => {
            close_writer_bounded(writer, CloseCode::Leaving).await;
            None
        }
        Err(error) => {
            let code = match error {
                RoomManagerJoinError::RoomFull => CloseCode::RoomFull,
                RoomManagerJoinError::MissingRoom | RoomManagerJoinError::RouterState => {
                    CloseCode::AuthFailed
                }
            };
            warn!(
                event = telemetry_event::WS_JOIN_FAILED,
                ?user_id,
                remote_address = remote.as_ref(),
                ?error,
                close_code = u16::from(code),
                "rejecting websocket because the authenticated user could not join the room"
            );
            handshake::reject(
                services,
                writer,
                code,
                remote.as_ref(),
                "rejecting websocket during user join",
            )
            .await;
            None
        }
    }
}

impl AuthenticatedSession {
    async fn serve(mut self) {
        self.record_current_span();
        self.metrics.record_ws_user_loop_started();
        let exit = self.run_loop().await;
        self.finish(exit).await;
    }

    fn record_current_span(&self) {
        let span = Span::current();
        span.record("room_id", field::display(self.user.room_id()));
        span.record(
            "user_id",
            field::display(self.user.user_id().path_segment()),
        );
        span.record("connection_id", self.user.connection_id().as_u64());
        span.record(
            telemetry_field::REMOTE_ADDRESS,
            field::display(self.user.remote_address()),
        );
    }

    async fn start(mut self) -> Option<Self> {
        let metrics = Arc::clone(&self.metrics);
        let _guard = metrics.track_ws_user_initialization();
        let span = telemetry::activated_span(info_span!(
            "user.initialize",
            room_id = %self.user.room_id(),
            user_id = %self.user.user_id().path_segment(),
            connection_id = self.user.connection_id().as_u64(),
            remote_address = %self.user.remote_address()
        ));
        async move {
            match self.start_inner().await {
                Ok(()) => Some(self),
                Err(exit) => {
                    self.finish(exit).await;
                    None
                }
            }
        }
        .instrument(span)
        .await
    }

    async fn start_inner(&mut self) -> Result<(), SessionExit> {
        let output = self.user.start().await;
        if self.shutdown.is_cancelled() {
            return Err(SessionExit::BeforeLoop(Some(CloseCode::Leaving)));
        }
        let output = match output {
            Ok(output) => output,
            Err(_error) => {
                warn!(
                    event = telemetry_event::WS_JOIN_FAILED,
                    user_id = ?self.user.user_id(),
                    connection_id = ?self.user.connection_id(),
                    remote_address = self.user.remote_address(),
                    outcome = "user_initialize_failed",
                    "failed to initialize websocket user"
                );
                self.metrics.record_ws_user_initialize_failure();
                return Err(SessionExit::BeforeLoop(None));
            }
        };
        let sent = send_user_output_bounded(&mut self.writer, output).await;
        if self.shutdown.is_cancelled() {
            return Err(SessionExit::BeforeLoop(Some(CloseCode::Leaving)));
        }
        if sent.is_ok() {
            return Ok(());
        }
        debug!(
            user_id = ?self.user.user_id(),
            connection_id = ?self.user.connection_id(),
            "failed to send user startup payload"
        );
        self.metrics.record_ws_startup_send_failure();
        warn!(
            event = telemetry_event::WS_JOIN_FAILED,
            user_id = ?self.user.user_id(),
            connection_id = ?self.user.connection_id(),
            remote_address = self.user.remote_address(),
            outcome = "startup_send_failed",
            "failed to send websocket user startup payload"
        );
        Err(SessionExit::BeforeLoop(None))
    }

    async fn finish(&mut self, exit: SessionExit) {
        let (reason, close) = match exit {
            SessionExit::BeforeLoop(close) => (None, close),
            SessionExit::Loop(reason, close) => (Some(reason), close),
        };
        if let Some(close) = close {
            close_writer_bounded(&mut self.writer, close).await;
        }
        if let Some(reason) = reason {
            self.metrics.record_ws_user_loop_exit(reason);
            info!(
                event = telemetry_event::WS_CONNECTION_CLOSED,
                connection_id = ?self.user.connection_id(),
                remote_address = self.user.remote_address(),
                ?reason,
                "closing websocket user"
            );
        }
        self.user.close().await;
    }

    fn shutdown_exit(&self) -> Option<SessionExit> {
        self.shutdown.is_cancelled().then_some(SessionExit::closing(
            LoopExit::RuntimeShutdown,
            CloseCode::Leaving,
        ))
    }

    /// Checks transport health before each ping so RTC teardown closes idle sessions.
    #[expect(
        clippy::cognitive_complexity,
        reason = "all session wake sources stay in one owner loop"
    )]
    async fn run_loop(&mut self) -> SessionExit {
        let ping_interval = Duration::from_millis(self.user_config.ping_interval_ms);
        let ping_timeout = Duration::from_millis(self.user_config.timeout_ms);
        let mut next_ping_at = Instant::now() + ping_interval;
        let mut next_health_at = next_ping_at;
        let mut pong = None;
        let shutdown = self.shutdown.clone();
        loop {
            let health_tick = sleep_until(next_health_at);
            tokio::pin!(health_tick);
            let ping_tick = sleep_until(next_ping_at);
            tokio::pin!(ping_tick);
            let pong_deadline = pong;
            tokio::select! {
                biased;
                () = shutdown.cancelled() => {
                    return SessionExit::closing(LoopExit::RuntimeShutdown, CloseCode::Leaving);
                }
                () = &mut health_tick => {
                    next_health_at = Instant::now() + ping_interval;
                    if let Some(exit) = self.check_transport() {
                        return exit;
                    }
                }
                () = &mut ping_tick, if pong.is_none() => {
                    if let Some(exit) = self.check_transport() {
                        return exit;
                    }
                    if send_message_bounded(&mut self.writer, Message::Ping(Vec::new().into()))
                        .await
                        .is_err()
                    {
                        debug!("failed to send websocket ping frame");
                        return SessionExit::Loop(LoopExit::OutboundMessageSendFailure, None);
                    }
                    let now = Instant::now();
                    next_ping_at = now + ping_interval;
                    pong = Some(now + ping_timeout);
                }
                () = async {
                    if let Some(deadline) = pong_deadline {
                        sleep_until(deadline).await;
                    }
                }, if pong_deadline.is_some() => {
                    debug!("timed out waiting for websocket pong");
                    return SessionExit::closing(LoopExit::PingTimeout, CloseCode::Error);
                }
                outbound = self.outbound.recv_event() => {
                    if let Some(exit) = self.handle_outbound_event(outbound).await {
                        return exit;
                    }
                }
                message = self.reader.next() => {
                    if let Some(exit) = self.handle_socket_event(message, &mut pong).await {
                        return exit;
                    }
                }
            }
        }
    }

    fn check_transport(&self) -> Option<SessionExit> {
        if !self.user.transport_disconnected() {
            return None;
        }
        debug!("closing websocket because the underlying RTC transport disconnected");
        Some(SessionExit::closing(
            LoopExit::TransportDisconnected,
            CloseCode::Error,
        ))
    }

    async fn handle_socket_event(
        &mut self,
        message: Option<Result<Message, AxumError>>,
        pong: &mut Option<Instant>,
    ) -> Option<SessionExit> {
        let message = match message {
            Some(Ok(message)) => message,
            Some(Err(_error)) => {
                debug!("websocket reader returned an error");
                return Some(SessionExit::Loop(LoopExit::ReaderError, None));
            }
            None => {
                debug!("websocket user closed the socket");
                return Some(SessionExit::Loop(LoopExit::UserClosed, None));
            }
        };
        self.handle_frame(message, pong).await
    }

    async fn handle_frame(
        &mut self,
        message: Message,
        pong: &mut Option<Instant>,
    ) -> Option<SessionExit> {
        match message {
            Message::Ping(payload) => {
                if send_message_bounded(&mut self.writer, Message::Pong(payload))
                    .await
                    .is_err()
                {
                    debug!("failed to send websocket pong frame");
                    return Some(SessionExit::Loop(
                        LoopExit::OutboundMessageSendFailure,
                        None,
                    ));
                }
                None
            }
            Message::Pong(_) => {
                *pong = None;
                None
            }
            Message::Close(frame) => {
                debug!(?frame, "websocket user sent close frame");
                Some(SessionExit::Loop(LoopExit::BusBreak, None))
            }
            Message::Text(payload) => self.handle_text(&payload).await,
            Message::Binary(payload) => self.handle_binary(&payload).await,
        }
    }

    async fn handle_binary(&mut self, payload: &[u8]) -> Option<SessionExit> {
        let Ok(payload) = str::from_utf8(payload) else {
            self.metrics.record_ws_bus_invalid_input_failure();
            warn!("received websocket binary frame with invalid UTF-8");
            return Some(self.client_error(CloseCode::ProtocolError).await);
        };
        self.handle_text(payload).await
    }

    async fn handle_text(&mut self, payload: &str) -> Option<SessionExit> {
        let batch = match decode_client_batch(payload) {
            Ok(batch) => batch,
            Err(error) => {
                let failure = error.kind();
                match failure {
                    ClientBatchDecodeFailureKind::InvalidInput => {
                        self.metrics.record_ws_bus_invalid_input_failure();
                    }
                    ClientBatchDecodeFailureKind::UnsupportedFeature => {
                        self.metrics.record_ws_bus_unsupported_feature_failure();
                    }
                }
                warn!(?failure, "failed to decode client websocket batch");
                return Some(self.client_error(CloseCode::ProtocolError).await);
            }
        };
        self.metrics.record_ws_bus_batch_received(batch.len());
        let mut output = UserOutput::new();
        for envelope in batch {
            if let Some(exit) = self.shutdown_exit() {
                return Some(exit);
            }
            match &envelope {
                ClientEnvelope::Request { .. } => self.metrics.record_ws_bus_client_request(),
                ClientEnvelope::Message(_) => self.metrics.record_ws_bus_client_message(),
                ClientEnvelope::Response { .. } => {}
            }
            let result = self.user.apply_client_envelope(envelope).await;
            if let Some(exit) = self.shutdown_exit() {
                return Some(exit);
            }
            match result {
                Ok(user_output) => output.extend(user_output),
                Err(error) => return Some(self.client_error(map_user_error(error)).await),
            }
        }
        let result = send_user_output_bounded(&mut self.writer, output).await;
        if let Some(exit) = self.shutdown_exit() {
            return Some(exit);
        }
        match result {
            Ok(_sent) => None,
            Err(code) => Some(SessionExit::closing(LoopExit::BusBreak, code)),
        }
    }

    async fn client_error(&self, fallback_code: CloseCode) -> SessionExit {
        let close_code = if self.user.is_current_connection().await {
            fallback_code
        } else {
            CloseCode::Kicked
        };
        self.shutdown_exit()
            .unwrap_or(SessionExit::closing(LoopExit::BusBreak, close_code))
    }

    async fn handle_outbound_event(&mut self, outbound: UserOutboundEvent) -> Option<SessionExit> {
        match outbound {
            UserOutboundEvent::Message(UserOutbound::Close(_)) => {
                Some(self.outbound_error(CloseCode::Kicked, false))
            }
            UserOutboundEvent::Message(outbound) => {
                let result = self.user.apply_room_outbound(outbound).await;
                if let Some(exit) = self.shutdown_exit() {
                    return Some(exit);
                }
                let output = match result {
                    Ok(output) => output,
                    Err(error) => {
                        return Some(self.outbound_error(map_user_error(error), false));
                    }
                };
                let envelope_count = output.len();
                let result = send_user_output_bounded(&mut self.writer, output).await;
                if let Some(exit) = self.shutdown_exit() {
                    return Some(exit);
                }
                match result {
                    Ok(batch_count) => {
                        self.metrics
                            .record_ws_bus_batches_sent(batch_count, envelope_count);
                        None
                    }
                    Err(code) => Some(self.outbound_error(code, true)),
                }
            }
            UserOutboundEvent::Overflow(overflow) => {
                warn!(
                    capacity = overflow.capacity(),
                    byte_capacity = overflow.byte_capacity(),
                    queued_bytes = overflow.queued_bytes(),
                    message_bytes = overflow.message_bytes(),
                    overflow_kind = ?overflow.kind(),
                    "closing websocket because the outbound queue overflowed"
                );
                Some(SessionExit::closing(
                    LoopExit::OutboundQueueOverflow,
                    CloseCode::Kicked,
                ))
            }
            UserOutboundEvent::Closed => {
                debug!("user outbound room closed");
                Some(SessionExit::Loop(LoopExit::OutboundChannelClosed, None))
            }
        }
    }

    fn outbound_error(&self, code: CloseCode, log_send_failure: bool) -> SessionExit {
        if code == CloseCode::Kicked {
            debug!(
                close_code = u16::from(code),
                "closing websocket from outbound signal"
            );
            return SessionExit::closing(LoopExit::OutboundCloseSignal, CloseCode::Kicked);
        }
        self.metrics.record_ws_bus_send_failure();
        if log_send_failure {
            debug!(
                close_code = u16::from(code),
                "failed to send outbound user event"
            );
        }
        let close = matches!(
            code,
            CloseCode::Clean
                | CloseCode::Leaving
                | CloseCode::RoomFull
                | CloseCode::AuthFailed
                | CloseCode::AuthTimeout
        )
        .then_some(code);
        SessionExit::Loop(LoopExit::OutboundMessageSendFailure, close)
    }
}

fn map_user_error(error: UserError) -> CloseCode {
    match error {
        UserError::ProtocolViolation => CloseCode::ProtocolError,
        UserError::Kicked => CloseCode::Kicked,
        UserError::InternalError => CloseCode::Error,
    }
}
