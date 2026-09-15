use o_sfu_protocol::wire::StreamType;
use o_sfu_router::{
    rtp::MediaStream,
    test_support::rtp_samples::{sample_client_rtp_capabilities, sample_video_rtp_parameters},
};

use super::fixtures::*;
use crate::{
    application::stream_catalog::source_publish_intent_for_stream_type,
    core::server::session::{UserId, UserPermissions},
    runtime::room::Room,
};

fn test_video_rtp_parameters(ssrc: u64) -> MediaStream {
    sample_video_rtp_parameters(None, u32::try_from(ssrc).unwrap_or(u32::MAX))
}

async fn publish_video_stream(
    room: &Room,
    user_id: &UserId,
    stream_type: StreamType,
    ssrc: u64,
    media_transport: &MediaTransport,
) {
    assert!(
        create_transport_session_offer(room, user_id, media_transport)
            .await
            .is_some()
    );
    assert!(
        room.test_api()
            .mark_session_ready(user_id, sample_client_rtp_capabilities(), media_transport)
            .await
    );
    assert!(
        room.test_api()
            .publish_intent(
                user_id,
                &source_publish_intent_for_stream_type(stream_type),
                test_video_rtp_parameters(ssrc),
                media_transport,
            )
            .await
            .is_some()
    );
}

#[tokio::test]
async fn noop_returns_ok_response() -> TestResult {
    let payload: NoopResponse = route_json(
        &test_state(),
        Request::get(route::v1::NOOP),
        Body::empty(),
        StatusCode::OK,
        "noop request should succeed",
    )
    .await?;
    assert_eq!(payload.result, "ok");
    Ok(())
}

#[tokio::test]
async fn stats_returns_live_room_data() -> TestResult {
    let test_state = test_state_with_handles();
    let query = CreateRoomQuery::default();
    let room = require_ok(
        test_state
            .room_manager
            .serve_room(
                "issuer-a",
                TEST_ROOM_KEY.into(),
                &RoomConfig {
                    web_rtc_enabled: query.web_rtc_enabled(),
                    recording_address: query.recording_address.clone(),
                },
                Some("203.0.113.10"),
            )
            .await,
        "test room should be served",
    )?;
    let (alice_tx, _alice_rx) = test_outbound_sender(&test_state.state);
    let (bob_tx, _bob_rx) = test_outbound_sender(&test_state.state);
    let alice_join = room
        .test_api()
        .join_user(
            UserId::Integer(1),
            None,
            UserPermissions::default(),
            alice_tx,
        )
        .await;
    let bob_join = room
        .test_api()
        .join_user(UserId::Integer(2), None, UserPermissions::default(), bob_tx)
        .await;
    assert!(alice_join.is_ok());
    assert!(bob_join.is_ok());
    require_ok(alice_join, "alice should join")?;
    require_ok(bob_join, "bob should join")?;
    publish_video_stream(
        &room,
        &UserId::Integer(1),
        StreamType::Camera,
        22_222,
        &test_state.media_transport,
    )
    .await;
    publish_video_stream(
        &room,
        &UserId::Integer(2),
        StreamType::Screen,
        33_333,
        &test_state.media_transport,
    )
    .await;

    let payload: StatsResponse = route_json(
        &test_state.state,
        Request::get(route::v1::STATS),
        Body::empty(),
        StatusCode::OK,
        "stats request should succeed",
    )
    .await?;
    assert_eq!(payload.len(), 1);
    let first = require_some(payload.first(), "stats payload should contain one room")?;
    assert_eq!(first.uuid, room.uuid());
    assert_eq!(first.remote_address, "203.0.113.10");
    assert_eq!(first.users_stats.count, 2);
    assert_eq!(first.users_stats.camera_count, 1);
    assert_eq!(first.users_stats.screen_count, 1);
    assert_eq!(first.users_stats.incoming_bit_rate.total, 0);
    assert_eq!(first.users_stats.incoming_bit_rate.audio, 0);
    assert_eq!(first.users_stats.incoming_bit_rate.camera, 0);
    assert_eq!(first.users_stats.incoming_bit_rate.screen, 0);
    assert!(first.web_rtc_enabled);
    assert!(first.create_date.contains('T'));
    assert!(first.create_date.ends_with('Z'));
    Ok(())
}
