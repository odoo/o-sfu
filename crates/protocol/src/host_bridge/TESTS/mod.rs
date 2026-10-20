use serde_json::json;

use super::project_protocol_event;
use crate::{
    bundle_api::BundleUpdate,
    core::{Command, ProtocolCore, ProtocolEvent},
    shared::{RecordingState, RecordingStateUpdate, StopCode, StreamType, UserId, UserInfo},
    signaling::{PeerSnapshot, TrackBinding},
};

#[test]
fn peer_departure_serializes_as_disconnect_update() {
    let command = Command::EmitEvent {
        event: ProtocolEvent::PeerLeft {
            user_id: UserId::Integer(9),
        },
    };

    assert_eq!(
        serde_json::to_value(command).unwrap_or_default(),
        json!({
            "kind": "emitUpdate",
            "update": {
                "name": "disconnect",
                "payload": {
                    "sessionId": 9
                }
            }
        })
    );
}

#[test]
fn borrowed_event_projection_matches_owned_bundle_shape() -> serde_json::Result<()> {
    let peer_snapshot = ProtocolEvent::PeerSnapshot {
        peers: vec![
            PeerSnapshot {
                user_id: UserId::Integer(5),
                info: UserInfo {
                    is_talking: Some(true),
                    ..UserInfo::default()
                },
            },
            PeerSnapshot {
                user_id: UserId::String("5".to_owned()),
                info: UserInfo {
                    is_camera_on: Some(true),
                    ..UserInfo::default()
                },
            },
        ],
    };
    let events = [
        ProtocolEvent::TrackSnapshot {
            bindings: vec![TrackBinding {
                mid: "video-1".to_owned(),
                user_id: UserId::Integer(7),
                stream_type: StreamType::Camera,
                active: true,
            }],
        },
        peer_snapshot.clone(),
        ProtocolEvent::PeerInfo {
            user_id: UserId::String("guest-8".to_owned()),
            info: UserInfo {
                is_raising_hand: Some(true),
                ..UserInfo::default()
            },
        },
        ProtocolEvent::PeerLeft {
            user_id: UserId::Integer(9),
        },
        ProtocolEvent::Broadcast {
            sender_id: UserId::Integer(10),
            message: json!({ "body": ["hello", 3] }),
        },
        ProtocolEvent::RecordingStateChanged {
            state: RecordingStateUpdate {
                state: RecordingState {
                    recording: Some(false),
                    audio: Some(true),
                    transcription: Some(false),
                    video: Some(true),
                },
                stop_code: Some(StopCode::UserRequest),
            },
        },
    ];
    for event in events {
        let command = Command::EmitEvent {
            event: event.clone(),
        };
        let actual = serde_json::to_value(command)?;
        let projected = project_protocol_event(event);
        let expected = serde_json::to_value(&projected)?;
        assert_eq!(actual.get("update"), Some(&expected));
        assert_eq!(serde_json::from_value::<BundleUpdate>(expected)?, projected);
    }
    let command = Command::EmitEvent {
        event: peer_snapshot,
    };
    let actual = serde_json::to_value(command)?;
    let collision_value = actual
        .get("update")
        .and_then(|update| update.get("payload"))
        .and_then(|payload| payload.get("5"));
    let expected = json!({ "isCameraOn": true });
    assert_eq!(collision_value, Some(&expected));
    Ok(())
}

#[test]
fn legacy_welcome_projects_browser_capabilities_and_full_recording_flags() -> serde_json::Result<()>
{
    let mut core = ProtocolCore::new();
    let reset = core.connect("wss://sfu.test/ws", "token", None);
    assert_eq!(
        serde_json::to_value(&reset[0])?,
        json!({
            "kind": "setAvailableFeatures",
            "features": {"rtc": false, "recording": {"audio": false, "video": false, "transcription": false}}
        })
    );
    assert_eq!(
        serde_json::to_value(&reset[1])?,
        json!({
            "kind": "setRecordingState",
            "state": {"audio": false, "video": false, "transcription": false}
        })
    );
    let commands = core.on_ws_message(r#"[{"t":"welcome","p":{"features":{"rtc":true,"audioRecording":true,"videoRecording":false,"transcription":true},"recording":{"recording":true,"audio":true},"peers":[]}}]"#);
    assert_eq!(
        serde_json::to_value(&commands[0])?,
        json!({
            "kind": "setAvailableFeatures",
            "features": {"rtc": true, "recording": {"audio": true, "video": false, "transcription": true}}
        })
    );
    assert_eq!(
        serde_json::to_value(&commands[1])?,
        json!({
            "kind": "setRecordingState",
            "state": {"audio": true, "video": false, "transcription": false}
        })
    );
    let update = Command::EmitEvent {
        event: ProtocolEvent::RecordingStateChanged {
            state: RecordingStateUpdate {
                state: RecordingState {
                    recording: Some(true),
                    video: Some(true),
                    ..RecordingState::default()
                },
                stop_code: None,
            },
        },
    };
    assert_eq!(
        serde_json::to_value(update)?,
        json!({
            "kind": "emitUpdate",
            "update": {"name": "channel_info_change", "payload": {"state": {"audio": false, "video": true, "transcription": false}}}
        })
    );
    Ok(())
}
