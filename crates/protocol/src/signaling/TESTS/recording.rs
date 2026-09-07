use serde_json::json;

use super::{super::EnvelopeDecodeError, *};

#[test]
fn protocol_start_recording_request_decodes_with_request_id() {
    let decoded = ClientEnvelope::decode(Envelope::request(
        "startrecording",
        RequestId::new("3"),
        Some(json!({
            "audio": true,
            "video": false,
        })),
    ));

    assert_eq!(
        decoded,
        Ok(ClientEnvelope::Request {
            request_id: RequestId::new("3"),
            request: ClientRequest::StartRecording(RecordingOptions {
                audio: Some(true),
                video: Some(false),
                transcription: None,
            }),
        })
    );
}

#[test]
fn stop_recording_response_round_trips() -> serde_json::Result<()> {
    let envelope = ServerEnvelope::Response {
        response_to: RequestId::new("recording-1"),
        response: ServerResponse::StopRecording(RecordingActionResult { ok: true }),
    }
    .into_envelope()?;

    assert_eq!(
        ServerEnvelope::decode(envelope),
        Ok(ServerEnvelope::Response {
            response_to: RequestId::new("recording-1"),
            response: ServerResponse::StopRecording(RecordingActionResult { ok: true }),
        })
    );
    Ok(())
}

#[test]
fn protocol_server_recording_change_message_round_trips_to_wire_envelope() -> serde_json::Result<()>
{
    let recording_change = ServerMessage::RecordingChange(RecordingStateUpdate {
        state: RecordingState {
            recording: Some(true),
            audio: Some(true),
            transcription: Some(false),
            video: Some(true),
        },
        stop_code: Some(StopCode::UserRequest),
    })
    .into_envelope()?;

    assert_eq!(
        serde_json::to_value(&recording_change)?,
        json!({
            "t": "recordingchange",
            "p": {
                "state": {
                    "recording": true,
                    "audio": true,
                    "transcription": false,
                    "video": true,
                },
                "stopCode": "user_request",
            },
        })
    );
    Ok(())
}

#[test]
fn protocol_server_start_recording_response_serializes_to_wire_envelope() -> serde_json::Result<()>
{
    let start_recording = ServerResponse::StartRecording(RecordingActionResult { ok: true })
        .into_envelope(RequestId::new("3"))?;

    assert_eq!(
        serde_json::to_value(&start_recording)?,
        json!({
            "t": "startrecording",
            "r": "3",
            "p": {
                "ok": true,
            },
        })
    );
    Ok(())
}

#[test]
fn stop_recording_request_requires_an_absent_payload() {
    let request_id = RequestId::new("stop-recording");
    assert_eq!(
        ClientEnvelope::decode(Envelope::request("stoprecording", request_id.clone(), None)),
        Ok(ClientEnvelope::Request {
            request_id: request_id.clone(),
            request: ClientRequest::StopRecording,
        })
    );
    for payload in [json!(null), json!({})] {
        assert_eq!(
            ClientEnvelope::decode(Envelope::request(
                "stoprecording",
                request_id.clone(),
                Some(payload)
            )),
            Err(EnvelopeDecodeError::UnexpectedPayload(
                "stoprecording".to_owned()
            ))
        );
    }
}
