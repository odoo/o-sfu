use super::support::{self as s, flows as f, setup as st};

#[tokio::test]
async fn fake_rtc_peers_forward_media_and_stop_after_download_mute_without_browsers()
-> s::TestResult {
    let _guard = st::full_stack_test_guard().await;
    let st::ReadyRoomFakePeers {
        server,
        room,
        mut publisher,
        mut subscriber,
    } = st::ready_room_fake_integer_peers("issuer-e", 70, 71).await?;

    f::assert_audio_media_arrives_and_download_mute_stops_flow(
        &server,
        &room,
        &mut publisher,
        &mut subscriber,
    )
    .await;
    server.join().await
}

#[tokio::test]
async fn fake_rtc_peers_pause_and_resume_retained_upload_route() -> s::TestResult {
    let _guard = st::full_stack_test_guard().await;
    let st::ReadyRoomFakePeers {
        server,
        room,
        mut publisher,
        mut subscriber,
    } = st::ready_room_fake_integer_peers("issuer-f", 70, 71).await?;

    f::assert_audio_publication_pause_and_resume(&server, &room, &mut publisher, &mut subscriber)
        .await;
    server.join().await
}
