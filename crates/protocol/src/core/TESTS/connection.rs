use super::*;

#[test]
fn protocol_core_connect_emits_connecting_state_and_socket_command() {
    let mut core = ProtocolCore::new();

    let commands = core.connect(
        "wss://sfu.example.com/socket",
        "signed-token",
        Some(String::from("channel-1")),
    );

    assert_eq!(core.state(), ConnectionState::Connecting);
    assert_eq!(
        commands.as_slice(),
        &[
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
                url: String::from("wss://sfu.example.com/socket"),
            },
        ]
    );
}

#[test]
fn protocol_core_ignores_connect_while_session_is_active() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);

    let commands = core.connect("wss://other.example.com/socket", "other-token", None);

    assert!(commands.is_empty());
    assert_eq!(core.state(), ConnectionState::Connecting);
}

#[test]
fn protocol_core_ws_open_sends_auth_frame_immediately() {
    let mut core = ProtocolCore::new();
    let _ = core.connect(
        "wss://sfu.example.com/socket",
        "signed-token",
        Some(String::from("channel-1")),
    );
    let commands = core.on_ws_open();
    assert!(matches!(
        commands.as_slice(),
        [Command::SendWebSocket { .. }]
    ));
    assert_sent_client_envelopes(
        &commands,
        vec![ClientEnvelope::Message(ClientMessage::Auth(AuthPayload {
            jwt: String::from("signed-token"),
            channel: Some(String::from("channel-1")),
        }))],
    );
}

#[test]
fn protocol_core_welcome_transitions_to_authenticated_and_emits_peer_snapshot() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);

    let commands = core.accept_welcome(sample_welcome_payload());

    assert_eq!(core.state(), ConnectionState::Authenticated);
    assert_eq!(
        commands.as_slice(),
        &[
            Command::SetAvailableFeatures {
                features: AvailableFeatures {
                    rtc: true,
                    transcription: false,
                    audio_recording: false,
                    video_recording: true,
                },
            },
            Command::SetRecordingState {
                state: RecordingState {
                    recording: Some(false),
                    audio: Some(false),
                    transcription: Some(false),
                    video: Some(false),
                },
            },
            Command::EmitStateChange {
                state: ConnectionState::Authenticated,
                cause: None,
            },
            Command::EmitEvent {
                event: ProtocolEvent::PeerSnapshot {
                    peers: vec![PeerSnapshot {
                        user_id: 7_i64.into(),
                        info: UserInfo {
                            is_talking: Some(true),
                            ..UserInfo::default()
                        },
                    }],
                },
            },
        ]
    );
}

#[test]
fn protocol_core_transport_ready_transitions_to_connected() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.accept_welcome(sample_welcome_payload());

    let commands = core.on_transport_ready();

    assert_eq!(core.state(), ConnectionState::Connected);
    assert_eq!(
        commands.as_slice(),
        &[Command::EmitStateChange {
            state: ConnectionState::Connected,
            cause: None,
        }]
    );
}

#[test]
fn protocol_core_rejects_illegal_authenticated_transition() {
    let mut core = ProtocolCore::new();

    let commands = core.accept_welcome(sample_welcome_payload());

    assert!(commands.is_empty());
    assert_eq!(core.state(), ConnectionState::Disconnected);
}

#[test]
fn protocol_core_closes_on_invalid_server_batch() {
    let mut core = ProtocolCore::new();

    let commands = core.on_ws_message("{not json");

    assert_eq!(
        commands.as_slice(),
        &[Command::CloseWebSocket {
            code: u16::from(WebSocketCloseCode::ProtocolError),
        }]
    );
}

#[test]
fn protocol_core_rejects_malformed_batch_without_partial_state() -> serde_json::Result<()> {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let welcome = ServerEnvelope::Message(ServerMessage::Welcome(sample_welcome_payload()))
        .into_envelope()?;
    let mut batch = vec![welcome];
    batch.push(Envelope::message("unknown-server-tag", None));
    let frame = serde_json::to_string(&batch)?;

    let commands = core.on_ws_message(&frame);

    assert_eq!(
        commands.as_slice(),
        &[Command::CloseWebSocket {
            code: u16::from(WebSocketCloseCode::ProtocolError),
        }]
    );
    assert_eq!(core.state(), ConnectionState::Connecting);
    Ok(())
}

#[test]
fn protocol_core_ignores_unknown_or_stale_timers() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);

    let commands = core.on_timer(99);

    assert!(commands.is_empty());
    assert_eq!(core.state(), ConnectionState::Connecting);
}
