use o_sfu_router::{
    rtp::MediaStream,
    test_support::rtp_samples::sample_video_rtp_parameters as router_sample_video_rtp_parameters,
};
use str0m::media::Mid;

use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RealRtcRouteActivity {
    pub(crate) source_active: bool,
    pub(crate) consumer_active: bool,
}

pub(crate) async fn real_rtc_route_activity(
    server: &TestServer,
    room: &Arc<Room>,
    source_user_id: UserId,
    consumer_user_id: UserId,
    mid: &str,
) -> Option<RealRtcRouteActivity> {
    let core_source_user_id = source_user_id.clone();
    let core_consumer_user_id = consumer_user_id.clone();
    let _source_connection_id = room
        .test_api()
        .user_connection_id(&core_source_user_id)
        .await?;
    let consumer_connection_id = room
        .test_api()
        .user_connection_id(&core_consumer_user_id)
        .await?;
    let consumer_session_key = room
        .transport_user_key(&core_consumer_user_id, consumer_connection_id)
        .await;
    let route_entry = server
        .media_transport
        .test_api()
        .route_entry_by_consumer_mid(&consumer_session_key, Mid::from(mid))
        .await?;
    Some(RealRtcRouteActivity {
        source_active: route_entry.source_active,
        consumer_active: route_entry.destinations.iter().any(|destination| {
            destination.dest_session == consumer_session_key && destination.active
        }),
    })
}

pub(crate) async fn assert_real_rtc_subscribe_activity(
    bob: &mut ProtocolHarnessPeer,
    server: &TestServer,
    room: &Arc<Room>,
    published_track: &TrackBinding,
    source_user_id: UserId,
    consumer_user_id: UserId,
    active: bool,
) -> Option<()> {
    bob.subscribe(
        source_user_id.clone(),
        ProtocolDownloadStates {
            camera: Some(active),
            ..ProtocolDownloadStates::default()
        },
    )
    .await?;
    if !no_server_frame(bob, Duration::from_millis(150)).await {
        return None;
    }
    let route_activity = real_rtc_route_activity(
        server,
        room,
        source_user_id,
        consumer_user_id,
        &published_track.mid,
    )
    .await?;
    if route_activity
        != (RealRtcRouteActivity {
            source_active: true,
            consumer_active: active,
        })
    {
        return None;
    }
    Some(())
}

pub(crate) fn sample_video_rtp_parameters(mid: &str) -> MediaStream {
    router_sample_video_rtp_parameters(Some(mid), 22_222)
}
