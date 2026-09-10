use super::*;

#[test]
fn protocol_core_emits_replacement_track_snapshots() -> Result<(), String> {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.accept_welcome(sample_welcome_payload());
    let peer_1_audio = TrackBinding {
        mid: String::from("0"),
        user_id: String::from("peer-1").into(),
        stream_type: StreamType::Audio,
        active: true,
    };
    let peer_2_camera = TrackBinding {
        mid: String::from("1"),
        user_id: String::from("peer-2").into(),
        stream_type: StreamType::Camera,
        active: true,
    };
    let first_tracks = encode_server_batch(ServerEnvelope::Message(ServerMessage::Tracks(vec![
        peer_1_audio.clone(),
        peer_2_camera.clone(),
    ])))?;
    let second_tracks = encode_server_batch(ServerEnvelope::Message(ServerMessage::Tracks(vec![
        peer_2_camera.clone(),
    ])))?;
    assert_eq!(
        core.on_ws_message(&first_tracks).as_slice(),
        &[Command::EmitEvent {
            event: ProtocolEvent::TrackSnapshot {
                bindings: vec![peer_1_audio, peer_2_camera.clone()],
            },
        }]
    );
    assert_eq!(
        core.on_ws_message(&second_tracks).as_slice(),
        &[Command::EmitEvent {
            event: ProtocolEvent::TrackSnapshot {
                bindings: vec![peer_2_camera],
            },
        }]
    );
    let peer_left = encode_server_batch(ServerEnvelope::Message(ServerMessage::PeerLeft(
        PeerLeftPayload {
            user_id: String::from("peer-2").into(),
        },
    )))?;
    let _ = core.on_ws_message(&peer_left);
    assert!(!core.disconnect().iter().any(|command| matches!(
        command,
        Command::EmitEvent {
            event: ProtocolEvent::TrackSnapshot { .. },
        }
    )));
    Ok(())
}

#[test]
fn protocol_core_peer_left_preserves_other_track_cleanup() -> Result<(), String> {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.accept_welcome(sample_welcome_payload());
    let peer_1 = TrackBinding {
        mid: String::from("0"),
        user_id: String::from("peer-1").into(),
        stream_type: StreamType::Audio,
        active: true,
    };
    let peer_2 = TrackBinding {
        mid: String::from("1"),
        user_id: String::from("peer-2").into(),
        stream_type: StreamType::Camera,
        active: true,
    };
    let tracks = encode_server_batch(ServerEnvelope::Message(ServerMessage::Tracks(vec![
        peer_1.clone(),
        peer_2.clone(),
    ])))?;
    assert_eq!(
        core.on_ws_message(&tracks).as_slice(),
        &[Command::EmitEvent {
            event: ProtocolEvent::TrackSnapshot {
                bindings: vec![peer_1, peer_2],
            },
        }]
    );
    let peer_left = encode_server_batch(ServerEnvelope::Message(ServerMessage::PeerLeft(
        PeerLeftPayload {
            user_id: String::from("peer-1").into(),
        },
    )))?;
    assert_eq!(
        core.on_ws_message(&peer_left).as_slice(),
        &[Command::EmitEvent {
            event: ProtocolEvent::PeerLeft {
                user_id: String::from("peer-1").into(),
            },
        }]
    );
    let commands = core.disconnect();
    assert_eq!(core.state(), ConnectionState::Disconnected);
    assert!(commands.iter().any(|command| matches!(
        command,
        Command::EmitEvent {
            event: ProtocolEvent::TrackSnapshot { bindings },
        } if bindings.is_empty()
    )));
    Ok(())
}

#[test]
fn protocol_core_ignores_legacy_sources_with_or_without_tracks() -> serde_json::Result<()> {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.accept_welcome(sample_welcome_payload());

    let binding = TrackBinding {
        mid: String::from("cam-0"),
        user_id: String::from("peer-1").into(),
        stream_type: StreamType::Camera,
        active: true,
    };
    let legacy_sources_payload = json!([{
        "sourceId": "source-7",
        "sessionId": "peer-1",
        "type": "camera",
        "active": true,
        "mid": "cam-0",
        "encodings": [{
            "encodingId": "encoding-1",
            "rid": "hi",
            "maxBitrate": 900_000,
            "resolutionScale": 1,
            "policyRole": "featured",
        }],
    }]);
    let tracks_message =
        ServerEnvelope::Message(ServerMessage::Tracks(vec![binding.clone()])).into_envelope()?;
    let legacy_sources_message = Envelope::message("sources", Some(legacy_sources_payload));
    let batch = serde_json::to_string(&[&tracks_message, &legacy_sources_message])?;

    assert_eq!(
        core.on_ws_message(&batch).as_slice(),
        &[Command::EmitEvent {
            event: ProtocolEvent::TrackSnapshot {
                bindings: vec![binding],
            }
        }]
    );

    let sources_only_batch = serde_json::to_string(&[legacy_sources_message])?;
    assert!(core.on_ws_message(&sources_only_batch).is_empty());
    Ok(())
}

#[test]
fn protocol_core_emits_peer_and_recording_updates_from_server_messages() -> Result<(), String> {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.accept_welcome(sample_welcome_payload());
    let peer_info_frame = encode_server_batch(ServerEnvelope::Message(ServerMessage::PeerInfo(
        PeerInfoPayload {
            user_id: String::from("peer-1").into(),
            info: UserInfo {
                is_camera_on: Some(true),
                ..UserInfo::default()
            },
        },
    )))?;
    let peer_left_frame = encode_server_batch(ServerEnvelope::Message(ServerMessage::PeerLeft(
        PeerLeftPayload {
            user_id: String::from("peer-1").into(),
        },
    )))?;
    let recording_frame = encode_server_batch(ServerEnvelope::Message(
        ServerMessage::RecordingChange(RecordingStateUpdate {
            state: RecordingState {
                recording: Some(false),
                audio: Some(false),
                transcription: Some(false),
                video: Some(false),
            },
            stop_code: Some(StopCode::UserRequest),
        }),
    ))?;
    let broadcast_frame = encode_server_batch(ServerEnvelope::Message(ServerMessage::Broadcast(
        ServerBroadcastPayload {
            sender_id: String::from("peer-2").into(),
            message: serde_json::json!({ "body": "hello" }),
        },
    )))?;
    assert_eq!(
        core.on_ws_message(&peer_info_frame).as_slice(),
        &[Command::EmitEvent {
            event: ProtocolEvent::PeerInfo {
                user_id: String::from("peer-1").into(),
                info: UserInfo {
                    is_camera_on: Some(true),
                    ..UserInfo::default()
                },
            },
        }]
    );
    assert_eq!(
        core.on_ws_message(&peer_left_frame).as_slice(),
        &[Command::EmitEvent {
            event: ProtocolEvent::PeerLeft {
                user_id: String::from("peer-1").into(),
            },
        }]
    );
    assert_eq!(
        core.on_ws_message(&broadcast_frame).as_slice(),
        &[Command::EmitEvent {
            event: ProtocolEvent::Broadcast {
                sender_id: String::from("peer-2").into(),
                message: serde_json::json!({ "body": "hello" }),
            },
        }]
    );
    assert_eq!(
        core.on_ws_message(&recording_frame).as_slice(),
        &[
            Command::SetRecordingState {
                state: RecordingState {
                    recording: Some(false),
                    audio: Some(false),
                    transcription: Some(false),
                    video: Some(false),
                },
            },
            Command::EmitEvent {
                event: ProtocolEvent::RecordingStateChanged {
                    state: RecordingStateUpdate {
                        state: RecordingState {
                            recording: Some(false),
                            audio: Some(false),
                            transcription: Some(false),
                            video: Some(false),
                        },
                        stop_code: Some(StopCode::UserRequest),
                    },
                },
            },
        ]
    );
    Ok(())
}

#[test]
fn protocol_core_rejects_state_updates_before_welcome() -> serde_json::Result<()> {
    let messages = vec![
        ServerMessage::Tracks(vec![]),
        ServerMessage::Sources(vec![]),
        ServerMessage::PeerInfo(PeerInfoPayload {
            user_id: 1.into(),
            info: UserInfo::default(),
        }),
        ServerMessage::PeerJoined(PeerInfoPayload {
            user_id: 1.into(),
            info: UserInfo::default(),
        }),
        ServerMessage::PeerLeft(PeerLeftPayload { user_id: 1.into() }),
        ServerMessage::Broadcast(ServerBroadcastPayload {
            sender_id: 1.into(),
            message: serde_json::json!({}),
        }),
        ServerMessage::RecordingChange(RecordingStateUpdate {
            state: RecordingState::default(),
            stop_code: None,
        }),
    ];
    for message in messages {
        let mut core = ProtocolCore::new();
        let _ = core.connect("wss://sfu.example.com", "token", None);
        let envelope = ServerEnvelope::Message(message).into_envelope()?;
        let batch = serde_json::to_string(&[&envelope])?;
        let commands = core.on_ws_message(&batch);
        assert_eq!(
            commands.as_slice(),
            &[Command::CloseWebSocket { code: 1002 }]
        );
        assert_eq!(core.state(), ConnectionState::Connecting);
    }
    Ok(())
}

#[test]
fn protocol_core_accepts_state_updates_after_welcome_in_same_batch() -> serde_json::Result<()> {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let welcome_message = ServerEnvelope::Message(ServerMessage::Welcome(sample_welcome_payload()))
        .into_envelope()?;
    let track_message = ServerEnvelope::Message(ServerMessage::Tracks(vec![TrackBinding {
        mid: String::from("0"),
        user_id: String::from("peer-1").into(),
        stream_type: StreamType::Audio,
        active: true,
    }]))
    .into_envelope()?;
    // batch where Welcome arrives BEFORE Tracks (Valid)
    let batch = serde_json::to_string(&[&welcome_message, &track_message])?;
    let commands = core.on_ws_message(&batch);
    // verify it transitioned to Authenticated and didn't close
    assert_eq!(core.state(), ConnectionState::Authenticated);
    assert!(
        !commands
            .iter()
            .any(|c| matches!(c, Command::CloseWebSocket { .. }))
    );
    // verify the Tracks update emitted its event
    assert!(commands.iter().any(|c| matches!(
        c,
        Command::EmitEvent {
            event: ProtocolEvent::TrackSnapshot { .. }
        }
    )));
    Ok(())
}

#[test]
fn protocol_core_rejects_updates_during_recovery_phase() -> serde_json::Result<()> {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com", "token", None);
    let _ = core.accept_welcome(sample_welcome_payload());
    let _ = core.on_transport_ready();
    // simulate a socket drop to trigger Recovery phase
    let _ = core.on_ws_close(1006);
    assert_eq!(core.state(), ConnectionState::Recovering);
    // verify it rejects updates
    let tracks_message = ServerEnvelope::Message(ServerMessage::Tracks(vec![])).into_envelope()?;
    let batch = serde_json::to_string(&[&tracks_message])?;
    assert_eq!(
        core.on_ws_message(&batch).as_slice(),
        &[Command::CloseWebSocket { code: 1002 }]
    );
    Ok(())
}
