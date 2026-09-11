//! socket lifecycle transitions for [`ProtocolCore`]
//!
//! [`ProtocolCore::connect`] starts a user attempt and clears replayable intent
//! [`ProtocolCore::disconnect`] ends that attempt and suppresses recovery
//! [`ProtocolCore::on_ws_close`] maps terminal codes to [`ConnectionState::Closed`]
//! while transient closes preserve the connect context for [`handle_recovery_timer`]
//!
//! welcome messages enter through [`ProtocolCore::on_ws_message`]
//! transport readiness enters through [`ProtocolCore::on_transport_ready`]
//! each transition returns ordered [`Command`] values for the host

use super::{
    Command, Commands, ConnectContext, ConnectionState, INITIAL_RECOVERY_DELAY_MS, ProtocolCore,
    ProtocolPhase, RECOVERY_TIMER_ID, empty_features, next_recovery_delay,
};
use crate::{shared::RecordingState, signaling::WebSocketCloseCode};

impl ProtocolCore {
    /// Starts a fresh connection attempt when the current state permits one.
    ///
    /// Accepts [`ConnectionState::Disconnected`], [`ConnectionState::Closed`] and
    /// [`ConnectionState::Recovering`]. Calls from [`ConnectionState::Connecting`],
    /// [`ConnectionState::Authenticated`] and [`ConnectionState::Connected`] return
    /// no commands without replacing the saved admission context.
    ///
    /// Clears sticky replay and runtime state so a caller switching rooms or credentials cannot
    /// accidentally leak the previous user intent into the new connection.
    /// A call from [`ConnectionState::Recovering`] cancels its recovery timer
    /// before the new socket attempt starts.
    pub fn connect(
        &mut self,
        url: impl Into<String>,
        jwt: impl Into<String>,
        room: Option<String>,
    ) -> Vec<Command> {
        let url = url.into();
        let jwt = jwt.into();
        let mut commands = match self.state() {
            ConnectionState::Disconnected | ConnectionState::Closed => Vec::new(),
            ConnectionState::Recovering => vec![Command::CancelTimer {
                id: RECOVERY_TIMER_ID,
            }],
            _ => return Vec::new(),
        };
        let connect_url = url.clone();
        self.connect_context = Some(ConnectContext { url, jwt, room });
        self.recovery_delay_ms = INITIAL_RECOVERY_DELAY_MS;
        self.phase = ProtocolPhase::Connecting;
        self.clear_runtime_state();
        self.sticky_replay.clear();
        reset_public_state(&mut commands);
        commands.push(state_change(self.state(), None));
        commands.push(Command::Connect { url: connect_url });
        commands
    }

    /// ends the current user attempt on purpose
    ///
    /// unlike [`ProtocolCore::on_ws_close`], this is not a recovery path
    /// it clears the saved connect context, runtime state and sticky replay state,
    /// then closes the websocket and peer connection
    /// any later recovery-timer delivery becomes a no-op because the caller
    /// explicitly asked to stop
    pub fn disconnect(&mut self) -> Vec<Command> {
        if matches!(
            self.state(),
            ConnectionState::Disconnected | ConnectionState::Closed
        ) {
            return Vec::new();
        }
        self.phase = ProtocolPhase::Disconnected;
        self.connect_context = None;
        self.recovery_delay_ms = INITIAL_RECOVERY_DELAY_MS;
        let mut commands = vec![Command::CancelTimer {
            id: RECOVERY_TIMER_ID,
        }];
        commands.extend(self.teardown_runtime_state());
        self.sticky_replay.clear();
        commands.push(Command::CloseWebSocket {
            code: u16::from(WebSocketCloseCode::Clean),
        });
        commands.push(Command::ClosePeerConnection);
        reset_public_state(&mut commands);
        commands.push(state_change(self.state(), None));
        commands
    }

    /// handles websocket closure after a user was already in flight
    ///
    /// there are three different cases here and mixing them up is the main way to
    /// break reconnect behavior:
    ///
    /// - terminal close codes move to [`ConnectionState::Closed`], clear the saved connect context,
    ///   and suppress recovery
    /// - non-terminal closes with saved connect context move to [`ConnectionState::Recovering`] and
    ///   schedule the recovery timer
    /// - non-terminal closes without saved connect context fall back to
    ///   [`ConnectionState::Disconnected`], because there is nothing safe to reconnect to
    ///
    /// example:
    ///
    /// ```text
    /// Connected --on_ws_close(AuthFailed)--> Closed
    /// Connected --on_ws_close(1011)--> Recovering
    /// ```
    pub fn on_ws_close(&mut self, close_code: u16) -> Vec<Command> {
        if matches!(
            self.state(),
            ConnectionState::Disconnected | ConnectionState::Closed
        ) {
            return Vec::new();
        }
        if let Some(
            terminal_code @ (WebSocketCloseCode::ProtocolError
            | WebSocketCloseCode::AuthFailed
            | WebSocketCloseCode::Kicked
            | WebSocketCloseCode::RoomFull),
        ) = WebSocketCloseCode::from_u16(close_code)
        {
            self.phase = ProtocolPhase::Closed;
            self.connect_context = None;
            self.recovery_delay_ms = INITIAL_RECOVERY_DELAY_MS;
            let mut commands = self.teardown_runtime_state();
            commands.push(Command::CancelTimer {
                id: RECOVERY_TIMER_ID,
            });
            commands.push(Command::ClosePeerConnection);
            reset_public_state(&mut commands);
            commands.push(state_change(
                self.state(),
                terminal_close_cause(terminal_code),
            ));
            return commands;
        }
        if self.connect_context.is_none() {
            self.phase = ProtocolPhase::Disconnected;
            let mut commands = self.teardown_runtime_state();
            reset_public_state(&mut commands);
            commands.push(state_change(self.state(), None));
            return commands;
        }
        let scheduled_delay_ms = self.recovery_delay_ms;
        self.recovery_delay_ms = next_recovery_delay(scheduled_delay_ms);
        self.phase = ProtocolPhase::Recovering;
        let mut commands = self.teardown_runtime_state();
        commands.push(Command::ClosePeerConnection);
        commands.push(state_change(self.state(), None));
        commands.push(Command::ScheduleTimer {
            id: RECOVERY_TIMER_ID,
            ms: scheduled_delay_ms,
        });
        commands
    }
}

/// retries the saved websocket connection after a recovery delay
///
/// this is narrow
/// only [`ConnectionState::Recovering`] may consume the recovery timer
/// a stale timer firing after a successful reconnect or explicit
/// disconnect must do nothing, otherwise old scheduled work can restart an
/// inactive attempt
///
/// example:
///
/// ```text
/// Connected --on_ws_close(1011)--> Recovering
/// Recovering --handle_recovery_timer()--> Connecting
/// Connected --handle_recovery_timer()--> no-op
/// ```
pub(super) fn handle_recovery_timer(core: &mut ProtocolCore) -> Commands {
    if core.state() != ConnectionState::Recovering {
        return Vec::new();
    }
    let Some(connect_context) = core.connect_context.as_ref() else {
        return Vec::new();
    };
    let connect_url = connect_context.url.clone();
    core.phase = ProtocolPhase::Connecting;
    let mut commands = vec![state_change(core.state(), None)];
    commands.push(Command::Connect { url: connect_url });
    commands
}

fn reset_public_state(commands: &mut Commands) {
    commands.extend([
        Command::SetAvailableFeatures {
            features: empty_features(),
        },
        Command::SetRecordingState {
            state: RecordingState::default(),
        },
    ]);
}

fn state_change(state: ConnectionState, cause: Option<&'static str>) -> Command {
    Command::EmitStateChange {
        state,
        cause: cause.map(str::to_owned),
    }
}

/// Returns the compatibility cause label for a terminal WebSocket close code.
fn terminal_close_cause(close_code: WebSocketCloseCode) -> Option<&'static str> {
    match close_code {
        WebSocketCloseCode::AuthFailed => Some("auth_failed"),
        WebSocketCloseCode::Kicked => Some("kicked"),
        WebSocketCloseCode::RoomFull => Some("full"),
        _ => None,
    }
}
