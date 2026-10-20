use super::*;
use crate::core::PendingRequest;

#[test]
fn protocol_core_tracks_recording_request_until_matching_response() -> Result<(), String> {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.accept_welcome(sample_welcome_payload());
    let commands = core.start_recording(RecordingOptions {
        audio: Some(false),
        video: Some(true),
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
    ] = commands.as_slice()
    else {
        return Err(format!("expected recording request, got {commands:?}"));
    };
    assert_eq!(pending_request.timeout_ms, REQUEST_TIMEOUT_MS);
    let request_id = pending_request.request_id.clone();
    let flush_commands = core.on_timer(*flush_timer_id);
    assert_sent_client_envelopes(
        &flush_commands,
        vec![ClientEnvelope::Request {
            request_id: request_id.clone(),
            request: ClientRequest::StartRecording(RecordingOptions {
                audio: Some(false),
                video: Some(true),
                transcription: None,
            }),
        }],
    );
    let response_frame = encode_server_batch(ServerEnvelope::Response {
        response_to: request_id.clone(),
        response: ServerResponse::StartRecording(RecordingActionResult { ok: true }),
    })?;
    let response_commands = core.on_ws_message(&response_frame);
    assert_eq!(
        response_commands.as_slice(),
        &[Command::CompletePendingRequest {
            request_id,
            timeout_timer_id: pending_request.timeout_timer_id,
            ok: true,
        }]
    );
    Ok(())
}

#[test]
fn protocol_core_request_timeout_resolves_pending_request_as_failed() -> Result<(), String> {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.accept_welcome(sample_welcome_payload());
    let commands = core.stop_recording();
    let [
        Command::BeginPendingRequest {
            request: pending_request,
        },
        ..,
    ] = commands.as_slice()
    else {
        return Err(format!("expected pending request, got {commands:?}"));
    };
    let request_id = pending_request.request_id.clone();
    let timeout_commands = core.on_timer(pending_request.timeout_timer_id);
    assert_eq!(
        timeout_commands.as_slice(),
        &[Command::RejectPendingRequest {
            request_id,
            timeout_timer_id: pending_request.timeout_timer_id,
            error: "bus request timed out".to_owned(),
        }]
    );
    Ok(())
}

#[test]
fn protocol_core_matches_overlapping_recording_requests_and_clears_in_begin_order()
-> Result<(), String> {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.accept_welcome(sample_welcome_payload());
    let start = pending_request(&core.start_recording(RecordingOptions::default()))?;
    let stop = pending_request(&core.stop_recording())?;
    let crossed_response = encode_server_batch(ServerEnvelope::Response {
        response_to: stop.request_id.clone(),
        response: ServerResponse::StartRecording(RecordingActionResult { ok: true }),
    })?;
    assert!(core.on_ws_message(&crossed_response).is_empty());
    let stop_response = encode_server_batch(ServerEnvelope::Response {
        response_to: stop.request_id.clone(),
        response: ServerResponse::StopRecording(RecordingActionResult { ok: true }),
    })?;
    assert_eq!(
        core.on_ws_message(&stop_response).as_slice(),
        &[Command::CompletePendingRequest {
            request_id: stop.request_id.clone(),
            timeout_timer_id: stop.timeout_timer_id,
            ok: true,
        }]
    );
    assert!(core.on_timer(stop.timeout_timer_id).is_empty());
    let start_response = encode_server_batch(ServerEnvelope::Response {
        response_to: start.request_id.clone(),
        response: ServerResponse::StartRecording(RecordingActionResult { ok: false }),
    })?;
    assert_eq!(
        core.on_ws_message(&start_response).as_slice(),
        &[Command::CompletePendingRequest {
            request_id: start.request_id,
            timeout_timer_id: start.timeout_timer_id,
            ok: false,
        }]
    );
    assert!(core.on_ws_message(&start_response).is_empty());
    let stop = pending_request(&core.stop_recording())?;
    let start = pending_request(&core.start_recording(RecordingOptions::default()))?;
    let completions = core
        .disconnect()
        .into_iter()
        .filter_map(|command| match command {
            Command::RejectPendingRequest {
                request_id,
                timeout_timer_id,
                error,
            } => Some((request_id, timeout_timer_id, error)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        completions,
        vec![
            (
                stop.request_id,
                stop.timeout_timer_id,
                "bus closed".to_owned()
            ),
            (
                start.request_id,
                start.timeout_timer_id,
                "bus closed".to_owned()
            ),
        ]
    );
    Ok(())
}

#[test]
fn protocol_core_correlates_concurrent_partial_recording_updates() -> Result<(), String> {
    let mut core = ProtocolCore::new();
    let _ = core.connect("wss://sfu.example.com/socket", "signed-token", None);
    let _ = core.accept_welcome(sample_welcome_payload());
    let first = pending_request(&core.start_recording(RecordingOptions {
        audio: Some(false),
        ..RecordingOptions::default()
    }))?;
    let second = pending_request(&core.start_recording(RecordingOptions::default()))?;
    assert_ne!(first.request_id, second.request_id);
    assert_sent_client_envelopes(
        &core.on_timer(BATCH_FLUSH_TIMER_ID),
        vec![
            ClientEnvelope::Request {
                request_id: first.request_id.clone(),
                request: ClientRequest::StartRecording(RecordingOptions {
                    audio: Some(false),
                    ..RecordingOptions::default()
                }),
            },
            ClientEnvelope::Request {
                request_id: second.request_id.clone(),
                request: ClientRequest::StartRecording(RecordingOptions::default()),
            },
        ],
    );
    let response = encode_server_batch(ServerEnvelope::Response {
        response_to: second.request_id.clone(),
        response: ServerResponse::StartRecording(RecordingActionResult { ok: false }),
    })?;
    assert_eq!(
        core.on_ws_message(&response),
        vec![Command::CompletePendingRequest {
            request_id: second.request_id,
            timeout_timer_id: second.timeout_timer_id,
            ok: false,
        }]
    );
    assert!(core.on_ws_message(&response).is_empty());
    assert!(core.on_timer(second.timeout_timer_id).is_empty());
    assert_eq!(
        core.on_timer(first.timeout_timer_id),
        vec![Command::RejectPendingRequest {
            request_id: first.request_id.clone(),
            timeout_timer_id: first.timeout_timer_id,
            error: "bus request timed out".to_owned(),
        }]
    );
    let late_response = encode_server_batch(ServerEnvelope::Response {
        response_to: first.request_id,
        response: ServerResponse::StartRecording(RecordingActionResult { ok: false }),
    })?;
    assert!(core.on_ws_message(&late_response).is_empty());
    assert!(core.on_timer(first.timeout_timer_id).is_empty());
    Ok(())
}

fn pending_request(commands: &[Command]) -> Result<PendingRequest, String> {
    commands
        .iter()
        .find_map(|command| match command {
            Command::BeginPendingRequest { request } => Some(request.clone()),
            _ => None,
        })
        .ok_or_else(|| format!("expected pending request, got {commands:?}"))
}
