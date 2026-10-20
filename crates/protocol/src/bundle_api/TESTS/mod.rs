use std::{collections::BTreeMap, fmt::Debug};

use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

use super::{
    BundleBroadcastUpdate, BundleConnectionState, BundleRecordingFlags, BundleRecordingStateUpdate,
    BundleStateChange, BundleUpdate, bundle_session_info_key,
};
use crate::shared::{StopCode, UserId, UserInfo};

fn assert_round_trip<T>(value: &T, expected_json: Value) -> serde_json::Result<()>
where
    T: Debug + DeserializeOwned + PartialEq + Serialize,
{
    assert_eq!(serde_json::to_value(value)?, expected_json);
    assert_eq!(serde_json::from_value::<T>(expected_json)?, *value);
    Ok(())
}

#[test]
fn bundle_connection_states_round_trip() -> serde_json::Result<()> {
    for (state, expected_json) in [
        (BundleConnectionState::Disconnected, json!("disconnected")),
        (BundleConnectionState::Connecting, json!("connecting")),
        (BundleConnectionState::Authenticated, json!("authenticated")),
        (BundleConnectionState::Connected, json!("connected")),
        (BundleConnectionState::Recovering, json!("recovering")),
        (BundleConnectionState::Closed, json!("closed")),
    ] {
        assert_round_trip(&state, expected_json)?;
    }

    assert_round_trip(
        &BundleStateChange {
            state: BundleConnectionState::Closed,
            cause: Some(String::from("full")),
        },
        json!({
            "state": "closed",
            "cause": "full"
        }),
    )
}

#[test]
fn bundle_updates_round_trip() -> serde_json::Result<()> {
    let broadcast = BundleUpdate::Broadcast(BundleBroadcastUpdate {
        sender_id: UserId::String("guest-7".to_owned()),
        message: json!("hello"),
    });
    assert_round_trip(
        &broadcast,
        json!({
            "name": "broadcast",
            "payload": {
                "senderId": "guest-7",
                "message": "hello"
            }
        }),
    )?;

    let session_info = BundleUpdate::SessionInfoChange(BTreeMap::from([(
        bundle_session_info_key(&UserId::Integer(5)),
        UserInfo {
            is_talking: Some(false),
            is_featured: None,
            is_camera_on: Some(true),
            is_screen_sharing_on: None,
            is_self_muted: None,
            is_deaf: None,
            is_raising_hand: None,
        },
    )]));
    assert_round_trip(
        &session_info,
        json!({
            "name": "info_change",
            "payload": {
                "5": {
                    "isTalking": false,
                    "isCameraOn": true
                }
            }
        }),
    )?;

    let channel_info = BundleUpdate::ChannelInfoChange(BundleRecordingStateUpdate {
        state: BundleRecordingFlags {
            audio: false,
            video: false,
            transcription: false,
        },
        stop_code: Some(StopCode::UserRequest),
    });
    assert_round_trip(
        &channel_info,
        json!({
            "name": "channel_info_change",
            "payload": {
                "state": {
                    "audio": false,
                    "transcription": false,
                    "video": false
                },
                "stopCode": "user_request"
            }
        }),
    )
}
