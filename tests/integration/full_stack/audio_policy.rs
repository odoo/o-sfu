use super::support::{self as s, media as m, setup as st, spillover as sp};

macro_rules! opus_vad_test {
    (
        $name:ident,
        $issuer:literal,
        $publisher_id:literal,
        $subscriber_id:literal,
        $config:expr,
        $level:literal,
        $voice_activity:literal,
        $state:expr,
        $reason:expr,
        $cross_worker:literal
    ) => {
        #[tokio::test]
        async fn $name() -> s::TestResult {
            let _guard = st::full_stack_test_guard().await;
            let publisher_user_id = s::UserId::Integer($publisher_id);
            let subscriber_user_id = s::UserId::Integer($subscriber_id);
            let mut peers = match $config {
                Some(config) => {
                    st::ready_room_fake_peers_with_config(
                        config,
                        $issuer,
                        publisher_user_id.clone(),
                        subscriber_user_id.clone(),
                    )
                    .await?
                }
                None => {
                    st::ready_room_fake_integer_peers($issuer, $publisher_id, $subscriber_id)
                        .await?
                }
            };
            if $cross_worker {
                sp::assert_cross_worker_placement(
                    &peers.server,
                    &peers.room,
                    &publisher_user_id,
                    &subscriber_user_id,
                )
                .await;
            }
            assert_opus_vad_flow(&mut peers, $level, $voice_activity, $state, $reason).await;
            Ok(())
        }
    };
}

opus_vad_test!(
    fake_rtc_opus_vad_true_drives_active_speaker_diagnostics,
    "issuer-opus-active-speaker",
    88,
    89,
    None,
    -32,
    true,
    s::DiagnosticsActiveSpeakerState::Active,
    s::DiagnosticsActiveSpeakerReason::Vad,
    false
);

opus_vad_test!(
    fake_rtc_cross_worker_opus_vad_true_forwards_and_drives_active_speaker,
    "issuer-cross-worker-opus-active-speaker",
    188,
    189,
    Some(st::cross_worker_test_config()),
    -32,
    true,
    s::DiagnosticsActiveSpeakerState::Active,
    s::DiagnosticsActiveSpeakerReason::Vad,
    true
);

// A nonzero audio level must preserve the clear VAD bit on the wire.
opus_vad_test!(
    fake_rtc_opus_vad_false_blocks_audio_forwarding,
    "issuer-opus-vad-false",
    86,
    87,
    None,
    -32,
    false,
    s::DiagnosticsActiveSpeakerState::Blocked,
    s::DiagnosticsActiveSpeakerReason::VadFalse,
    false
);

opus_vad_test!(
    fake_rtc_cross_worker_opus_vad_false_blocks_relay_fanout,
    "issuer-cross-worker-opus-vad-false",
    186,
    187,
    Some(st::cross_worker_test_config()),
    -32,
    false,
    s::DiagnosticsActiveSpeakerState::Blocked,
    s::DiagnosticsActiveSpeakerReason::VadFalse,
    true
);

async fn assert_opus_vad_flow(
    peers: &mut st::ReadyRoomFakePeers,
    audio_level: i8,
    voice_activity: bool,
    expected_state: s::DiagnosticsActiveSpeakerState,
    expected_reason: s::DiagnosticsActiveSpeakerReason,
) {
    let st::ReadyRoomFakePeers {
        server,
        room,
        publisher,
        subscriber,
    } = peers;
    let publisher_user_id = publisher.user_id().clone();
    let mut source = s::FakeMediaSource::new(s::SyntheticOpusStream::with_audio_activity(
        audio_level,
        voice_activity,
    ));
    m::publish_source_and_ready_route(
        server,
        room,
        publisher,
        subscriber,
        &publisher_user_id,
        &source,
    )
    .await;

    let mut clock = s::FakeClock::default();
    if voice_activity {
        m::assert_packet_forwarded(publisher, subscriber, &mut source, &mut clock).await;
    } else {
        m::assert_packet_dropped(publisher, subscriber, &mut source, &mut clock).await;
    }
    assert!(
        server
            .wait_for_audio_source_active_speaker(
                room,
                &publisher_user_id,
                expected_state,
                expected_reason,
                Some(audio_level),
            )
            .await
    );
}
