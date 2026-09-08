use o_sfu_protocol::{
    host::{
        Command, ConnectionState, NegotiationKind, PendingRequest, ProtocolCore, ProtocolEvent,
    },
    wire::{
        AuthPayload, AvailableFeatures, ClientEnvelope, ClientMessage, ClientResponse,
        DownloadStates, RecordingOptions, RecordingState, RequestId, ServerEnvelope, ServerMessage,
        ServerRequest, SessionDescriptionPayload, StreamIntentPayload, StreamType,
        SubscribePayload, TrackBinding, UserId, UserInfo, WebSocketCloseCode,
    },
};
use o_sfu_tests::miri_support::{
    decode_sent_client_envelopes, empty_welcome_payload, encode_server_batch,
};
use secrecy::SecretString;

fn extract_pending_request(commands: &[Command]) -> Option<&PendingRequest> {
    commands.iter().find_map(|command| match command {
        Command::BeginPendingRequest { request } => Some(request),
        _ => None,
    })
}

fn welcome(core: &mut ProtocolCore) -> Vec<Command> {
    core.on_ws_message(&encode_server_batch(ServerEnvelope::Message(
        ServerMessage::Welcome(empty_welcome_payload()),
    )))
}

fn neutral_features() -> AvailableFeatures {
    AvailableFeatures {
        rtc: false,
        transcription: false,
        audio_recording: false,
        video_recording: false,
    }
}

#[test]
fn recovery_replay_splits_session_and_publication_phases() {
    let mut core = ProtocolCore::new();

    assert_eq!(
        core.connect("wss://sfu.example.com/socket", "signed-token", None)
            .as_slice(),
        &[
            Command::SetAvailableFeatures {
                features: neutral_features(),
            },
            Command::SetRecordingState {
                state: RecordingState::default(),
            },
            Command::EmitStateChange {
                state: ConnectionState::Connecting,
                cause: None,
            },
            Command::Connect {
                url: "wss://sfu.example.com/socket".to_owned(),
            },
        ]
    );
    assert_eq!(
        decode_sent_client_envelopes(&core.on_ws_open()),
        vec![ClientEnvelope::Message(ClientMessage::Auth(AuthPayload {
            jwt: SecretString::from("signed-token"),
            channel: None,
        }))]
    );

    assert!(core.publish(StreamType::Camera, true).is_empty());
    assert!(
        core.subscribe(
            UserId::String("peer-7".to_owned()),
            DownloadStates {
                audio: Some(true),
                camera: Some(false),
                screen: None,
                ..DownloadStates::default()
            },
        )
        .is_empty()
    );
    assert!(
        core.update_info(UserInfo {
            is_camera_on: Some(true),
            is_raising_hand: Some(true),
            ..UserInfo::default()
        })
        .is_empty()
    );

    let welcome_commands = welcome(&mut core);
    assert_eq!(
        welcome_commands.get(2),
        Some(&Command::EmitStateChange {
            state: ConnectionState::Authenticated,
            cause: None,
        })
    );
    assert_eq!(
        decode_sent_client_envelopes(&welcome_commands),
        vec![
            ClientEnvelope::Message(ClientMessage::Subscribe(SubscribePayload {
                user_id: UserId::String("peer-7".to_owned()),
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
        ]
    );

    let transport_ready_commands = core.on_transport_ready();
    assert_eq!(
        transport_ready_commands.first(),
        Some(&Command::EmitStateChange {
            state: ConnectionState::Connected,
            cause: None,
        })
    );
    assert_eq!(
        decode_sent_client_envelopes(&transport_ready_commands),
        vec![ClientEnvelope::Message(ClientMessage::Publish(
            StreamIntentPayload {
                stream_type: StreamType::Camera,
            },
        ))]
    );
}

#[test]
fn request_timeouts_ignore_unrelated_timer_ids_and_resolve_only_matching_request() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = welcome(&mut core);

    let start_result = core.start_recording(RecordingOptions {
        audio: Some(true),
        video: Some(true),
        transcription: None,
    });
    let Some(start_request) = extract_pending_request(&start_result) else {
        return;
    };

    let stop_result = core.stop_recording();
    let Some(stop_request) = extract_pending_request(&stop_result) else {
        return;
    };

    assert!(core.on_timer(99_999).is_empty());
    assert_eq!(
        core.on_timer(stop_request.timeout_timer_id).as_slice(),
        &[Command::CompletePendingRequest {
            request_id: stop_request.request_id.clone(),
            timeout_timer_id: stop_request.timeout_timer_id,
            ok: false,
        }]
    );
    assert!(core.on_timer(stop_request.timeout_timer_id).is_empty());
    assert_eq!(
        core.on_timer(start_request.timeout_timer_id).as_slice(),
        &[Command::CompletePendingRequest {
            request_id: start_request.request_id.clone(),
            timeout_timer_id: start_request.timeout_timer_id,
            ok: false,
        }]
    );
}

#[test]
fn negotiation_answer_mismatches_do_not_resolve_pending_request() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = welcome(&mut core);

    let offer_frame = encode_server_batch(ServerEnvelope::Request {
        request_id: RequestId::new("offer-1"),
        request: ServerRequest::Offer(SessionDescriptionPayload {
            sdp: "v=0\r\ns=offer\r\n".to_owned(),
            upload_slots: Vec::new(),
        }),
    });
    assert_eq!(
        core.on_ws_message(&offer_frame).as_slice(),
        &[Command::ApplyNegotiation {
            request_id: RequestId::new("offer-1"),
            kind: NegotiationKind::Offer,
            sdp: "v=0\r\ns=offer\r\n".to_owned(),
            upload_slots: Vec::new(),
        }]
    );

    assert!(
        core.submit_negotiation_answer(
            &RequestId::new("offer-2"),
            NegotiationKind::Offer,
            "v=0\r\ns=wrong-id\r\n",
        )
        .is_empty()
    );
    assert!(
        core.submit_negotiation_answer(
            &RequestId::new("offer-1"),
            NegotiationKind::Renegotiate,
            "v=0\r\ns=wrong-kind\r\n",
        )
        .is_empty()
    );

    let answer_commands = core.submit_negotiation_answer(
        &RequestId::new("offer-1"),
        NegotiationKind::Offer,
        "v=0\r\ns=answer\r\n",
    );
    assert_eq!(
        decode_sent_client_envelopes(&answer_commands),
        vec![ClientEnvelope::Response {
            response_to: RequestId::new("offer-1"),
            response: ClientResponse::Offer(SessionDescriptionPayload {
                sdp: "v=0\r\ns=answer\r\n".to_owned(),
                upload_slots: Vec::new(),
            }),
        }]
    );
    assert!(
        core.submit_negotiation_answer(
            &RequestId::new("offer-1"),
            NegotiationKind::Offer,
            "v=0\r\ns=stale\r\n",
        )
        .is_empty()
    );
}

#[test]
fn malformed_server_batches_close_the_socket_with_protocol_error() {
    let mut core = ProtocolCore::new();

    assert_eq!(
        core.on_ws_message("{not json").as_slice(),
        &[Command::CloseWebSocket {
            code: u16::from(WebSocketCloseCode::ProtocolError),
        }]
    );

    let invalid_envelope = r#"[{"t":"offer","p":{"sdp":"v=0\r\n"},"q":"1","r":"2"}]"#;
    assert_eq!(
        core.on_ws_message(invalid_envelope).as_slice(),
        &[Command::CloseWebSocket {
            code: u16::from(WebSocketCloseCode::ProtocolError),
        }]
    );
}

#[test]
fn disconnect_clears_pending_requests_snapshots_and_runtime_obligations() {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = welcome(&mut core);
    let _ = core.on_transport_ready();

    let binding = TrackBinding {
        mid: "0".to_owned(),
        user_id: UserId::String("peer-1".to_owned()),
        stream_type: StreamType::Audio,
        active: true,
    };
    let track_frame = encode_server_batch(ServerEnvelope::Message(ServerMessage::Tracks(vec![
        binding.clone(),
    ])));
    assert_eq!(
        core.on_ws_message(&track_frame).as_slice(),
        &[Command::EmitEvent {
            event: ProtocolEvent::TrackSnapshot {
                bindings: vec![binding],
            },
        }]
    );

    let request_result = core.start_recording(RecordingOptions {
        audio: Some(true),
        video: None,
        transcription: None,
    });
    let Some(request) = extract_pending_request(&request_result) else {
        return;
    };

    assert_eq!(
        core.disconnect().as_slice(),
        &[
            Command::CancelTimer { id: 1 },
            Command::CancelTimer { id: 2 },
            Command::CompletePendingRequest {
                request_id: request.request_id.clone(),
                timeout_timer_id: request.timeout_timer_id,
                ok: false,
            },
            Command::EmitEvent {
                event: ProtocolEvent::TrackSnapshot {
                    bindings: Vec::new(),
                },
            },
            Command::CloseWebSocket { code: 1000 },
            Command::ClosePeerConnection,
            Command::SetAvailableFeatures {
                features: neutral_features(),
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
    assert_eq!(core.state(), ConnectionState::Disconnected);
}
