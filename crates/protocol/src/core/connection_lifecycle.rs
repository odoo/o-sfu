//! socket lifecycle transitions for [`ProtocolCore`]
//!
//! [`connect`] starts a user attempt and clears replayable intent
//! [`disconnect`] ends that attempt and suppresses recovery
//! [`on_ws_close`] maps terminal codes to [`ConnectionState::Closed`]
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

/// host-visible reason attached to a terminal lifecycle state change
///
/// values are derived from terminal websocket close codes and rendered as
/// compatibility labels by production command translation
#[derive(Clone, Copy)]
enum LifecycleCloseCause {
    /// authentication was rejected by the server
    AuthFailed,
    /// the server removed the user from the room
    Kicked,
    /// the room refused the connection because capacity was exhausted
    RoomFull,
}

/// maps terminal websocket close codes to host-visible lifecycle causes
fn terminal_close_cause(close_code: WebSocketCloseCode) -> Option<LifecycleCloseCause> {
    match close_code {
        WebSocketCloseCode::AuthFailed => Some(LifecycleCloseCause::AuthFailed),
        WebSocketCloseCode::Kicked => Some(LifecycleCloseCause::Kicked),
        WebSocketCloseCode::RoomFull => Some(LifecycleCloseCause::RoomFull),
        _ => None,
    }
}

/// returns the compatibility label exposed through `EmitStateChange`
fn lifecycle_close_cause_label(cause: LifecycleCloseCause) -> &'static str {
    match cause {
        LifecycleCloseCause::AuthFailed => "auth_failed",
        LifecycleCloseCause::Kicked => "kicked",
        LifecycleCloseCause::RoomFull => "full",
    }
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

fn state_change(state: ConnectionState, cause: Option<LifecycleCloseCause>) -> Command {
    Command::EmitStateChange {
        state,
        cause: cause.map(lifecycle_close_cause_label).map(str::to_owned),
    }
}

/// starts a fresh connection attempt from an inactive or recovering state
///
/// this is the only lifecycle entry point that wipes both
/// runtime state and sticky replay state before reconnecting
/// a brand-new [`connect`] means "start over with this endpoint and auth context",
/// not "resume whatever the previous user was trying to do"
///
/// calls from live admission states are ignored so the host cannot accidentally
/// stack overlapping connection attempts on top of an already-live user
/// a call from [`ConnectionState::Recovering`] also cancels the stale recovery timer before the
/// new socket attempt starts
///
/// ```text
/// Disconnected --connect(url, jwt, room)--> Connecting
/// Closed       --connect(url, jwt, room)--> Connecting
/// Recovering  --connect(url, jwt, room)--> Connecting
/// ```
pub(super) fn connect(
    core: &mut ProtocolCore,
    url: String,
    jwt: String,
    room: Option<String>,
) -> Commands {
    let mut commands = match core.state() {
        ConnectionState::Disconnected | ConnectionState::Closed => Vec::new(),
        ConnectionState::Recovering => vec![Command::CancelTimer {
            id: RECOVERY_TIMER_ID,
        }],
        _ => return Vec::new(),
    };
    let connect_url = url.clone();
    core.connect_context = Some(ConnectContext { url, jwt, room });
    core.recovery_delay_ms = INITIAL_RECOVERY_DELAY_MS;
    core.phase = ProtocolPhase::Connecting;
    core.clear_runtime_state();
    core.clear_sticky_state();
    reset_public_state(&mut commands);
    commands.push(state_change(core.state(), None));
    commands.push(Command::Connect { url: connect_url });
    commands
}

/// ends the current user attempt on purpose
///
/// unlike [`on_ws_close`], this is not a recovery path
/// it clears the saved connect context, runtime state and sticky replay state,
/// then closes the websocket and peer connection
/// any later recovery-timer delivery becomes a no-op because the caller
/// explicitly asked to stop
pub(super) fn disconnect(core: &mut ProtocolCore) -> Commands {
    if matches!(
        core.state(),
        ConnectionState::Disconnected | ConnectionState::Closed
    ) {
        return Vec::new();
    }
    core.phase = ProtocolPhase::Disconnected;
    core.connect_context = None;
    core.recovery_delay_ms = INITIAL_RECOVERY_DELAY_MS;
    let mut commands = vec![Command::CancelTimer {
        id: RECOVERY_TIMER_ID,
    }];
    commands.extend(core.teardown_runtime_state());
    core.clear_sticky_state();
    commands.push(Command::CloseWebSocket {
        code: u16::from(WebSocketCloseCode::Clean),
    });
    commands.push(Command::ClosePeerConnection);
    reset_public_state(&mut commands);
    commands.push(state_change(core.state(), None));
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
pub(super) fn on_ws_close(core: &mut ProtocolCore, close_code: u16) -> Commands {
    if matches!(
        core.state(),
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
        core.phase = ProtocolPhase::Closed;
        core.connect_context = None;
        core.recovery_delay_ms = INITIAL_RECOVERY_DELAY_MS;
        let mut commands = core.teardown_runtime_state();
        commands.push(Command::CancelTimer {
            id: RECOVERY_TIMER_ID,
        });
        commands.push(Command::ClosePeerConnection);
        reset_public_state(&mut commands);
        commands.push(state_change(
            core.state(),
            terminal_close_cause(terminal_code),
        ));
        return commands;
    }

    if core.connect_context.is_none() {
        core.phase = ProtocolPhase::Disconnected;
        let mut commands = core.teardown_runtime_state();
        reset_public_state(&mut commands);
        commands.push(state_change(core.state(), None));
        return commands;
    }

    let scheduled_delay_ms = core.recovery_delay_ms;
    core.recovery_delay_ms = next_recovery_delay(scheduled_delay_ms);
    core.phase = ProtocolPhase::Recovering;
    let mut commands = core.teardown_runtime_state();
    commands.push(Command::ClosePeerConnection);
    commands.push(state_change(core.state(), None));
    commands.push(Command::ScheduleTimer {
        id: RECOVERY_TIMER_ID,
        ms: scheduled_delay_ms,
    });
    commands
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
