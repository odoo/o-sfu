use o_sfu_telemetry::diagnostics::DiagnosticsSourceSelector;

use super::*;

#[test]
fn featured_layout_does_not_imply_selected_rid() -> TestResult {
    let mut room: DiagnosticsRoomDetail = serde_json::from_value(serde_json::json!({
        "users": [{
            "userId": 2,
            "userInfo": {},
            "publications": [],
            "subscriptions": [{
                "producerUserId": 1,
                "sourceId": 7,
                "streamId": "camera",
                "state": "active",
                "layoutRole": "featured",
                "selection": {
                    "active": true,
                    "activeVideoRouteCount": 1,
                    "policyAllowsDelivery": true,
                    "selectionReason": "open",
                    "selector": "open",
                    "selectedVideoBitrateBps": 0
                }
            }],
            "transport": {
                "connectionId": 2,
                "mediaWorkerId": 0,
                "qualitySummary": {
                    "currentIncomingBitrate": {"totalBps": 0},
                    "sampledMetricsAvailable": false,
                    "sampleCount": 0
                }
            }
        }],
        "sources": [{
            "active": true,
            "currentIncomingBitrateBps": 0,
            "mediaKind": "video",
            "ownerUserId": 1,
            "sourceId": 7,
            "streamId": "camera",
            "encodings": [{"encodingId": 10, "policyRole": "featured", "rid": "hi"}]
        }],
        "summary": {
            "createDate": "2026-09-07",
            "mediaWorkerId": 0,
            "publicationCount": 1,
            "recordingState": {},
            "remoteAddress": "127.0.0.1",
            "sourceCount": 1,
            "userCount": 2,
            "subscriptionCount": 1,
            "transport": {"connectedUsers": 2, "disconnectedUsers": 0, "totalUsers": 2, "unknownUsers": 0},
            "uuid": "selected-rid-observation",
            "webRtcEnabled": true
        }
    }))?;
    let consumer = UserId::Integer(2);
    let producer = UserId::Integer(1);
    assert_eq!(
        video_subscription_selected_rid(&room, &consumer, &producer),
        None
    );

    let user = require_some(room.users.first_mut(), "consumer should exist")?;
    let subscription = require_some(user.subscriptions.first_mut(), "subscription should exist")?;
    subscription.selection.selector = DiagnosticsSourceSelector::Encoding;
    subscription.selection.selected_encoding_id = Some(10);
    subscription.selection.selected_rid = Some("hi".to_owned());
    assert_eq!(
        video_subscription_selected_rid(&room, &consumer, &producer),
        Some("hi")
    );
    Ok(())
}
