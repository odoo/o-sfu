use super::*;

const ATTEMPT_STAGES: [AttemptStage; 3] = [
    AttemptStage::Connecting,
    AttemptStage::Authenticated,
    AttemptStage::Connected,
];
const TERMINAL_CLOSE_CODES: [WebSocketCloseCode; 4] = [
    WebSocketCloseCode::ProtocolError,
    WebSocketCloseCode::AuthFailed,
    WebSocketCloseCode::Kicked,
    WebSocketCloseCode::RoomFull,
];

#[derive(Clone, Copy, Debug)]
enum AttemptStage {
    Connecting,
    Authenticated,
    Connected,
}

fn core_at(stage: AttemptStage) -> ProtocolCore {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    if matches!(stage, AttemptStage::Authenticated | AttemptStage::Connected) {
        let _ = core.accept_welcome(sample_welcome_payload());
    }
    if matches!(stage, AttemptStage::Connected) {
        let _ = core.on_transport_ready();
    }
    let expected = match stage {
        AttemptStage::Connecting => ConnectionState::Connecting,
        AttemptStage::Authenticated => ConnectionState::Authenticated,
        AttemptStage::Connected => ConnectionState::Connected,
    };
    assert_eq!(core.state(), expected);
    core
}

fn connect_count(commands: &[Command]) -> usize {
    commands
        .iter()
        .filter(|command| matches!(command, Command::Connect { .. }))
        .count()
}

fn recovery_timer_count(commands: &[Command]) -> usize {
    commands
        .iter()
        .filter(|command| {
            matches!(
                command,
                Command::ScheduleTimer {
                    id: RECOVERY_TIMER_ID,
                    ..
                }
            )
        })
        .count()
}

fn batch_flush_cancel_count(commands: &[Command]) -> usize {
    commands
        .iter()
        .filter(|command| {
            matches!(
                command,
                Command::CancelTimer {
                    id: BATCH_FLUSH_TIMER_ID,
                }
            )
        })
        .count()
}

fn peer_close_count(commands: &[Command]) -> usize {
    commands
        .iter()
        .filter(|command| matches!(command, Command::ClosePeerConnection))
        .count()
}

#[test]
fn protocol_core_disconnect_cleans_up_connected_session() -> Result<(), String> {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.accept_welcome(sample_welcome_payload());
    let (request_id, timeout_timer_id) = start_flushed_recording_request(&mut core)?;
    let _ = core.on_transport_ready();

    let commands = core.disconnect();

    assert_eq!(core.state(), ConnectionState::Disconnected);
    assert_eq!(
        commands.as_slice(),
        &[
            Command::CancelTimer {
                id: RECOVERY_TIMER_ID,
            },
            Command::CompletePendingRequest {
                request_id,
                timeout_timer_id,
                ok: false,
            },
            Command::CloseWebSocket { code: 1000 },
            Command::ClosePeerConnection,
            Command::SetAvailableFeatures {
                features: AvailableFeatures {
                    rtc: false,
                    transcription: false,
                    audio_recording: false,
                    video_recording: false,
                },
            },
            Command::SetRecordingState {
                state: RecordingState::default(),
            },
            Command::EmitStateChange {
                state: ConnectionState::Disconnected,
                cause: None,
            },
        ]
    );
    Ok(())
}

#[test]
fn protocol_core_disconnect_clears_each_attempt_stage() {
    for stage in ATTEMPT_STAGES {
        let mut core = core_at(stage);
        assert!(
            core.on_timer(RECOVERY_TIMER_ID).is_empty(),
            "stale recovery timer was accepted from {stage:?}"
        );
        let _ = core.publish(StreamType::Audio, true);
        let _ = core.publish(StreamType::Camera, true);
        let _ = core.publish(StreamType::Screen, true);
        let _ = core.subscribe(
            "peer-7".into(),
            DownloadStates {
                audio: Some(true),
                ..DownloadStates::default()
            },
        );
        let _ = core.update_info(UserInfo {
            is_camera_on: Some(true),
            ..UserInfo::default()
        });
        assert_ne!(core.sticky_replay, StickyReplayState::new(), "{stage:?}");

        let commands = core.disconnect();

        assert_eq!(core.state(), ConnectionState::Disconnected, "{stage:?}");
        assert_eq!(peer_close_count(&commands), 1, "{stage:?}");
        assert_eq!(
            batch_flush_cancel_count(&commands),
            usize::from(matches!(
                stage,
                AttemptStage::Authenticated | AttemptStage::Connected
            )),
            "{stage:?}"
        );
        assert!(core.connect_context.is_none(), "{stage:?}");
        assert_eq!(core.sticky_replay, StickyReplayState::new(), "{stage:?}");
        assert!(core.on_timer(RECOVERY_TIMER_ID).is_empty(), "{stage:?}");
        assert!(core.on_timer(BATCH_FLUSH_TIMER_ID).is_empty(), "{stage:?}");
        assert!(core.on_ws_close(1011).is_empty(), "{stage:?}");
    }
}

#[test]
fn protocol_core_terminal_close_resolves_request_before_recovery_cancel() -> Result<(), String> {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.accept_welcome(sample_welcome_payload());
    let (request_id, timeout_timer_id) = start_flushed_recording_request(&mut core)?;
    let _ = core.on_transport_ready();

    let commands = core.on_ws_close(u16::from(WebSocketCloseCode::ProtocolError));

    assert_eq!(core.state(), ConnectionState::Closed);
    assert_eq!(
        commands.as_slice(),
        &[
            Command::CompletePendingRequest {
                request_id,
                timeout_timer_id,
                ok: false,
            },
            Command::CancelTimer {
                id: RECOVERY_TIMER_ID,
            },
            Command::ClosePeerConnection,
            Command::SetAvailableFeatures {
                features: AvailableFeatures {
                    rtc: false,
                    transcription: false,
                    audio_recording: false,
                    video_recording: false,
                },
            },
            Command::SetRecordingState {
                state: RecordingState::default(),
            },
            Command::EmitStateChange {
                state: ConnectionState::Closed,
                cause: None,
            },
        ]
    );
    Ok(())
}

fn start_flushed_recording_request(core: &mut ProtocolCore) -> Result<(RequestId, u32), String> {
    let result = core.start_recording(RecordingOptions {
        audio: Some(true),
        video: None,
        transcription: None,
    });
    let [
        Command::BeginPendingRequest {
            request: pending_request,
        },
        Command::ScheduleTimer {
            id: flush_timer_id,
            ms: 100,
        },
    ] = result.as_slice()
    else {
        return Err(format!("expected recording request start, got {result:?}"));
    };
    assert_eq!(pending_request.timeout_ms, REQUEST_TIMEOUT_MS);
    let _ = core.on_timer(*flush_timer_id);
    Ok((
        pending_request.request_id.clone(),
        pending_request.timeout_timer_id,
    ))
}

#[test]
fn protocol_core_non_terminal_close_enters_recovering() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.accept_welcome(sample_welcome_payload());
    let _ = core.on_transport_ready();

    let commands = core.on_ws_close(1011);

    assert_eq!(core.state(), ConnectionState::Recovering);
    assert_eq!(
        commands.as_slice(),
        &[
            Command::ClosePeerConnection,
            Command::EmitStateChange {
                state: ConnectionState::Recovering,
                cause: None,
            },
            Command::ScheduleTimer {
                id: RECOVERY_TIMER_ID,
                ms: 1_000,
            },
        ]
    );
}

#[test]
fn protocol_core_close_policy_is_stage_independent() {
    for stage in ATTEMPT_STAGES {
        for close_code in TERMINAL_CLOSE_CODES {
            let mut core = core_at(stage);

            let commands = core.on_ws_close(u16::from(close_code));

            assert_eq!(core.state(), ConnectionState::Closed, "{stage:?}");
            assert_eq!(
                recovery_timer_count(&commands),
                0,
                "{stage:?} {close_code:?}"
            );
            assert!(core.connect_context.is_none(), "{stage:?} {close_code:?}");
            assert!(
                core.on_timer(RECOVERY_TIMER_ID).is_empty(),
                "{stage:?} {close_code:?}"
            );
            let reconnect = core.connect("wss://sfu.example.com/socket", "signed-token", None);
            assert_eq!(connect_count(&reconnect), 1, "{stage:?} {close_code:?}");
        }

        let mut core = core_at(stage);
        let commands = core.on_ws_close(1011);
        assert_eq!(core.state(), ConnectionState::Recovering, "{stage:?}");
        assert_eq!(recovery_timer_count(&commands), 1, "{stage:?}");
        assert_eq!(peer_close_count(&commands), 1, "{stage:?}");
        let reconnect = core.on_timer(RECOVERY_TIMER_ID);
        assert_eq!(core.state(), ConnectionState::Connecting, "{stage:?}");
        assert_eq!(connect_count(&reconnect), 1, "{stage:?}");
    }
}

#[test]
fn protocol_core_replays_sticky_session_intents_after_recovery_authentication() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.accept_welcome(sample_welcome_payload());
    let _ = core.on_transport_ready();
    let _ = core.publish(StreamType::Camera, true);
    let _ = core.subscribe(
        String::from("peer-7").into(),
        DownloadStates {
            audio: Some(true),
            camera: Some(false),
            screen: None,
            ..DownloadStates::default()
        },
    );
    let _ = core.update_info(UserInfo {
        is_camera_on: Some(true),
        is_raising_hand: Some(true),
        ..UserInfo::default()
    });
    let _ = core.on_ws_close(1011);
    let _ = core.on_timer(RECOVERY_TIMER_ID);
    let commands = core.accept_welcome(sample_welcome_payload());
    assert_eq!(core.state(), ConnectionState::Authenticated);
    assert_eq!(
        commands.get(2),
        Some(&Command::EmitStateChange {
            state: ConnectionState::Authenticated,
            cause: None,
        })
    );
    assert_sent_client_envelopes(
        &commands,
        vec![
            ClientEnvelope::Message(ClientMessage::Subscribe(SubscribePayload {
                user_id: String::from("peer-7").into(),
                states: DownloadStates {
                    audio: Some(true),
                    camera: Some(false),
                    screen: None,
                    ..DownloadStates::default()
                },
            })),
            ClientEnvelope::Message(ClientMessage::Info(UserInfo {
                is_camera_on: Some(true),
                is_raising_hand: Some(true),
                ..UserInfo::default()
            })),
        ],
    );
}

#[test]
fn protocol_core_replays_sticky_publish_when_recovery_transport_is_ready() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.accept_welcome(sample_welcome_payload());
    let _ = core.on_transport_ready();
    let _ = core.publish(StreamType::Camera, true);
    let _ = core.subscribe(
        String::from("peer-7").into(),
        DownloadStates {
            audio: Some(true),
            camera: Some(false),
            screen: None,
            ..DownloadStates::default()
        },
    );
    let _ = core.update_info(UserInfo {
        is_camera_on: Some(true),
        is_raising_hand: Some(true),
        ..UserInfo::default()
    });
    let _ = core.on_ws_close(1011);
    let _ = core.on_timer(RECOVERY_TIMER_ID);
    let _ = core.accept_welcome(sample_welcome_payload());
    let commands = core.on_transport_ready();
    assert_eq!(core.state(), ConnectionState::Connected);
    assert_eq!(
        commands.first(),
        Some(&Command::EmitStateChange {
            state: ConnectionState::Connected,
            cause: None,
        })
    );
    assert_sent_client_envelopes(
        &commands,
        vec![ClientEnvelope::Message(ClientMessage::Publish(
            StreamIntentPayload {
                stream_type: StreamType::Camera,
            },
        ))],
    );
}

#[test]
fn protocol_core_updates_sticky_intents_while_recovering_before_replay() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.accept_welcome(sample_welcome_payload());
    let _ = core.on_transport_ready();
    let _ = core.publish(StreamType::Camera, true);
    let _ = core.subscribe(
        String::from("peer-7").into(),
        DownloadStates {
            audio: Some(true),
            camera: None,
            screen: None,
            ..DownloadStates::default()
        },
    );
    let _ = core.on_ws_close(1011);
    let _ = core.publish(StreamType::Camera, false);
    let _ = core.subscribe(
        String::from("peer-7").into(),
        DownloadStates {
            audio: Some(false),
            camera: Some(true),
            camera_layout: Some(VideoLayoutIntent::Pinned),
            screen: None,
            ..DownloadStates::default()
        },
    );
    let _ = core.subscribe(
        String::from("peer-7").into(),
        DownloadStates {
            screen_layout: Some(VideoLayoutIntent::Hidden),
            ..DownloadStates::default()
        },
    );
    let _ = core.update_info(UserInfo {
        is_featured: Some(true),
        is_self_muted: Some(true),
        ..UserInfo::default()
    });
    let _ = core.on_timer(RECOVERY_TIMER_ID);
    let commands = core.accept_welcome(sample_welcome_payload());
    assert_sent_client_envelopes(
        &commands,
        vec![
            ClientEnvelope::Message(ClientMessage::Subscribe(SubscribePayload {
                user_id: String::from("peer-7").into(),
                states: DownloadStates {
                    audio: Some(false),
                    camera: Some(true),
                    camera_layout: Some(VideoLayoutIntent::Pinned),
                    screen_layout: Some(VideoLayoutIntent::Hidden),
                    screen: None,
                },
            })),
            ClientEnvelope::Message(ClientMessage::Info(UserInfo {
                is_featured: None,
                is_self_muted: Some(true),
                ..UserInfo::default()
            })),
        ],
    );
    let commands = core.on_transport_ready();
    assert!(
        !commands
            .iter()
            .any(|command| matches!(command, Command::SendWebSocket { .. }))
    );
}

#[test]
fn protocol_core_replays_latest_reactivated_publish_after_recovery() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.accept_welcome(sample_welcome_payload());
    let _ = core.on_transport_ready();
    let _ = core.publish(StreamType::Camera, true);
    let _ = core.on_ws_close(1011);
    let _ = core.publish(StreamType::Camera, false);
    let _ = core.publish(StreamType::Camera, true);
    let _ = core.on_timer(RECOVERY_TIMER_ID);
    let _ = core.accept_welcome(sample_welcome_payload());
    let commands = core.on_transport_ready();
    assert_sent_client_envelopes(
        &commands,
        vec![ClientEnvelope::Message(ClientMessage::Publish(
            StreamIntentPayload {
                stream_type: StreamType::Camera,
            },
        ))],
    );
}

#[test]
fn protocol_core_recovery_timer_retries_the_saved_url() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.accept_welcome(sample_welcome_payload());
    let _ = core.on_transport_ready();
    let _ = core.on_ws_close(1011);

    let commands = core.on_timer(RECOVERY_TIMER_ID);

    assert_eq!(core.state(), ConnectionState::Connecting);
    assert_eq!(
        commands.as_slice(),
        &[
            Command::EmitStateChange {
                state: ConnectionState::Connecting,
                cause: None,
            },
            Command::Connect {
                url: String::from("wss://sfu.example.com/socket"),
            },
        ]
    );
}

#[test]
fn protocol_core_fresh_connect_supersedes_pending_recovery() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.accept_welcome(sample_welcome_payload());
    let _ = core.on_transport_ready();
    let _ = core.on_ws_close(1011);
    let commands = core.connect(
        "wss://other.example.com/socket",
        "other-token",
        Some(String::from("other-room")),
    );
    assert_eq!(core.state(), ConnectionState::Connecting);
    assert_eq!(
        commands.as_slice(),
        &[
            Command::CancelTimer {
                id: RECOVERY_TIMER_ID,
            },
            Command::SetAvailableFeatures {
                features: AvailableFeatures {
                    rtc: false,
                    transcription: false,
                    audio_recording: false,
                    video_recording: false,
                },
            },
            Command::SetRecordingState {
                state: RecordingState::default(),
            },
            Command::EmitStateChange {
                state: ConnectionState::Connecting,
                cause: None,
            },
            Command::Connect {
                url: String::from("wss://other.example.com/socket"),
            },
        ]
    );
    assert!(core.on_timer(RECOVERY_TIMER_ID).is_empty());
    let auth_commands = core.on_ws_open();
    assert_sent_client_envelopes(
        &auth_commands,
        vec![ClientEnvelope::Message(ClientMessage::Auth(AuthPayload {
            jwt: String::from("other-token"),
            channel: Some(String::from("other-room")),
        }))],
    );
}

#[test]
fn protocol_core_successful_recovery_resets_backoff_delay() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.accept_welcome(sample_welcome_payload());
    let _ = core.on_transport_ready();
    let _ = core.on_ws_close(1011);
    let _ = core.on_timer(RECOVERY_TIMER_ID);
    let _ = core.accept_welcome(sample_welcome_payload());

    let commands = core.on_ws_close(1011);

    assert_eq!(
        commands.as_slice(),
        &[
            Command::ClosePeerConnection,
            Command::EmitStateChange {
                state: ConnectionState::Recovering,
                cause: None,
            },
            Command::ScheduleTimer {
                id: RECOVERY_TIMER_ID,
                ms: 1_000,
            },
        ]
    );
}

#[test]
fn protocol_core_terminal_close_enters_closed_with_cause() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.accept_welcome(sample_welcome_payload());

    let commands = core.on_ws_close(4109);

    assert_eq!(core.state(), ConnectionState::Closed);
    assert_eq!(
        commands.as_slice(),
        &[
            Command::CancelTimer {
                id: RECOVERY_TIMER_ID,
            },
            Command::ClosePeerConnection,
            Command::SetAvailableFeatures {
                features: AvailableFeatures {
                    rtc: false,
                    transcription: false,
                    audio_recording: false,
                    video_recording: false,
                },
            },
            Command::SetRecordingState {
                state: RecordingState::default(),
            },
            Command::EmitStateChange {
                state: ConnectionState::Closed,
                cause: Some(String::from("full")),
            },
        ]
    );
}

#[test]
fn protocol_core_protocol_error_close_is_terminal() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.accept_welcome(sample_welcome_payload());
    let _ = core.on_transport_ready();

    let commands = core.on_ws_close(u16::from(WebSocketCloseCode::ProtocolError));

    assert_eq!(core.state(), ConnectionState::Closed);
    assert_eq!(
        commands.as_slice(),
        &[
            Command::CancelTimer {
                id: RECOVERY_TIMER_ID,
            },
            Command::ClosePeerConnection,
            Command::SetAvailableFeatures {
                features: AvailableFeatures {
                    rtc: false,
                    transcription: false,
                    audio_recording: false,
                    video_recording: false,
                },
            },
            Command::SetRecordingState {
                state: RecordingState::default(),
            },
            Command::EmitStateChange {
                state: ConnectionState::Closed,
                cause: None,
            },
        ]
    );
    assert!(core.on_timer(RECOVERY_TIMER_ID).is_empty());
}
