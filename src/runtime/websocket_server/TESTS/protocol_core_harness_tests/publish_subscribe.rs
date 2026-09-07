use super::support::*;
use crate::core::prelude::{SourcePolicy, SourcePublishIntent};

#[tokio::test]
async fn protocol_core_projects_camera_publish_without_presence_as_active() -> TestResult {
    let (server, room, _alice, mut bob) = Box::pin(setup_protocol_peers(
        "issuer-protocol-no-presence-camera",
        UserId::Integer(53),
        UserId::Integer(54),
    ))
    .await?;
    let intent = SourcePublishIntent::new(
        stream_id_for_stream_type(StreamType::Camera),
        MediaKind::Video,
        SourcePolicy::hidden(),
    );

    require_some(
        room.test_api()
            .publish_intent(
                &UserId::Integer(53),
                &intent,
                sample_video_rtp_parameters("cam-0"),
                &server.media_transport,
            )
            .await,
        "protocol publisher should be ready",
    )?;

    let ServerMessage::Tracks(track_bindings) = require_some(
        read_single_protocol_server_message(&mut bob).await,
        "subscriber should receive one translated track snapshot",
    )?
    else {
        return Err(anyhow!("subscriber should receive only a tracks message"));
    };
    let [track_binding] = track_bindings.as_slice() else {
        return Err(anyhow!("subscriber should keep one camera track binding"));
    };
    assert_eq!(track_binding.user_id, ProtocolSessionId::Integer(53));
    assert_eq!(track_binding.stream_type, ProtocolStreamType::Camera);
    assert!(track_binding.active);
    Ok(())
}

#[tokio::test]
async fn protocol_core_publish_round_trips_through_real_rtc_server_user_protocol() -> TestResult {
    let (_server, _room, mut alice, mut bob) = require_some(
        Box::pin(setup_real_rtc_protocol_peers(
            "issuer-protocol-rtc-publish",
            UserId::Integer(71),
            UserId::Integer(72),
            56_301,
            56_302,
        ))
        .await,
        "real RTC protocol peers should start",
    )?;

    require_some(
        alice.publish(ProtocolStreamType::Camera, true).await,
        "publisher should stage camera publish",
    )?;
    require_some(
        alice.read_server_frame().await,
        "publisher should consume the rtc-backed renegotiation request and answer it",
    )?;

    let track_bindings = require_some(
        read_track_snapshot(&mut bob).await,
        "subscriber should receive the rtc-backed translated track snapshot",
    )?;
    let [published_track] = track_bindings.as_slice() else {
        return Err(anyhow!("subscriber should keep one published track"));
    };
    assert_eq!(published_track.user_id, ProtocolSessionId::Integer(71));
    assert_eq!(published_track.stream_type, ProtocolStreamType::Camera);
    assert!(published_track.active);
    require_some(
        bob.read_server_frame().await,
        "subscriber should receive the rtc-backed follow-up renegotiation request",
    )?;
    Ok(())
}

#[tokio::test]
async fn protocol_handshake_uses_answer_derived_client_capabilities_for_user_state() -> TestResult {
    let server = TestServerBuilder::new().spawn_required().await?;
    let room = require_some(
        create_room(
            &server,
            "issuer-protocol-rtc-capabilities",
            CreateRoomQuery::default(),
        )
        .await,
        "test room should be served",
    )?;
    let mut alice = require_some(
        ProtocolHarnessPeer::with_custom_rtc_negotiation(56_305, reduced_capability_rtc),
        "custom RTC protocol peer should build",
    )?;
    let alice_token = require_some(
        signed_connect_claims(TEST_ROOM_KEY, room.uuid(), UserId::Integer(75)),
        "alice token should sign",
    )?;

    require_some(
        alice
            .connect_and_finish_handshake(&server.url(), &alice_token, None)
            .await,
        "alice should finish protocol handshake",
    )?;

    let client_rtp_codec_names = timeout(Duration::from_secs(1), async {
        loop {
            if let Some(codec_names) = room
                .test_api()
                .session_client_rtp_codec_names(&UserId::Integer(75))
                .await
            {
                return codec_names;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        client_rtp_codec_names.is_ok(),
        "protocol handshake should store parsed client RTP capabilities"
    );
    let codec_names = require_some(client_rtp_codec_names.ok(), "codec names should resolve")?;
    assert_eq!(codec_names, vec![String::from("opus"), String::from("VP8")]);
    assert!(
        codec_names.iter().all(|codec| codec != "rtx"),
        "the production RTP surface should exclude retransmission codecs"
    );
    assert!(
        codec_names.iter().all(|codec| codec != "H264"),
        "the stored client RTP capabilities must reflect the real RTC answer"
    );
    Ok(())
}

#[tokio::test]
async fn protocol_core_queues_one_follow_up_offer_until_first_answer() -> TestResult {
    let (_server, room, mut alice, mut bob) = require_some(
        Box::pin(setup_real_rtc_protocol_peers(
            "issuer-protocol-rtc-publish-queue",
            UserId::Integer(73),
            UserId::Integer(74),
            56_303,
            56_304,
        ))
        .await,
        "real RTC protocol peers should start",
    )?;
    alice.auto_answer_negotiation = false;

    require_some(
        alice.publish(ProtocolStreamType::Camera, true).await,
        "publisher should stage camera publish",
    )?;
    assert!(
        alice.read_server_frame().await.is_some(),
        "publisher should receive the first rtc-backed renegotiation request"
    );
    assert_eq!(alice.pending_negotiations.len(), 1);

    for stream_type in [
        ProtocolStreamType::Screen,
        ProtocolStreamType::Audio,
        ProtocolStreamType::Screen,
    ] {
        assert!(alice.publish(stream_type, true).await.is_some());
    }
    assert_eq!(alice.pending_negotiations.len(), 1);

    assert!(
        alice.answer_next_negotiation().await.is_some(),
        "publisher should answer the first queued negotiation"
    );
    assert!(
        consume_camera_publish_setup(
            &mut bob,
            &ProtocolSessionId::Integer(73),
            "subscriber should receive the initial camera track snapshot",
        )
        .await
        .is_some(),
        "subscriber should consume the committed camera publish setup"
    );

    assert!(
        alice.read_server_frame().await.is_some(),
        "publisher should receive the queued follow-up renegotiation only after the first answer lands"
    );
    assert_eq!(alice.pending_negotiations.len(), 1);

    assert!(
        alice.answer_next_negotiation().await.is_some(),
        "publisher should answer the queued follow-up negotiation"
    );
    // RTC staging may coalesce several track snapshots into one subscriber offer.
    // Snapshot count is therefore independent of negotiation request count.
    for expected in [
        &[ProtocolStreamType::Camera, ProtocolStreamType::Audio][..],
        &[
            ProtocolStreamType::Camera,
            ProtocolStreamType::Audio,
            ProtocolStreamType::Screen,
        ],
    ] {
        let Some(tracks) = read_track_snapshot(&mut bob).await else {
            panic!("subscriber should receive each queued publish snapshot");
        };
        assert_eq!(tracks.len(), expected.len());
        for stream_type in expected {
            assert_track_snapshot_contains(&tracks, &ProtocolSessionId::Integer(73), *stream_type);
        }
    }
    assert!(
        timeout(Duration::from_millis(150), read_track_snapshot(&mut bob),)
            .await
            .is_err(),
        "duplicate queued publish should not produce another track snapshot"
    );
    assert!(
        timeout(
            Duration::from_millis(150),
            read_until_server_request(&mut alice),
        )
        .await
        .is_err(),
        "queued publishes should produce exactly one publisher follow-up offer"
    );
    assert!(
        close_peer_and_wait_for_room_cleanup(&mut alice, &room, &UserId::Integer(73))
            .await
            .is_some(),
        "publisher websocket should close cleanly before the test server is dropped"
    );
    assert!(
        close_peer_and_wait_for_room_cleanup(&mut bob, &room, &UserId::Integer(74))
            .await
            .is_some(),
        "subscriber websocket should close cleanly before the test server is dropped"
    );
    Ok(())
}

#[tokio::test]
async fn protocol_core_unpublish_cancels_pending_publish_before_commit() {
    let Some((_server, _channel, mut alice, mut bob)) = Box::pin(setup_real_rtc_protocol_peers(
        "issuer-protocol-rtc-publish-cancel",
        UserId::Integer(75),
        UserId::Integer(76),
        56_305,
        56_306,
    ))
    .await
    else {
        return;
    };
    alice.auto_answer_negotiation = false;

    assert!(
        alice
            .publish(ProtocolStreamType::Camera, true)
            .await
            .is_some()
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "publisher should receive the staged publish renegotiation request"
    );
    assert_eq!(
        alice.pending_negotiations.len(),
        1,
        "the first publish should leave one pending negotiation answer in the harness"
    );

    assert!(
        alice
            .publish(ProtocolStreamType::Camera, false)
            .await
            .is_some()
    );
    assert_eq!(
        alice.pending_negotiations.len(),
        1,
        "canceling the staged publish should not create an overlapping negotiation"
    );

    assert!(
        alice.answer_next_negotiation().await.is_some(),
        "publisher should answer the staged publish negotiation"
    );
    assert!(
        alice.read_server_frame().await.is_some(),
        "publisher should receive the follow-up renegotiation that removes the canceled publish"
    );
    assert_eq!(
        alice.pending_negotiations.len(),
        1,
        "canceling the staged publish should queue exactly one follow-up removal negotiation"
    );
    assert!(
        no_server_frame(&mut bob, Duration::from_millis(150)).await,
        "subscriber should not observe a track snapshot before the canceled publish is removed"
    );

    assert!(
        alice.answer_next_negotiation().await.is_some(),
        "publisher should answer the follow-up removal negotiation"
    );
    assert!(
        no_server_frame(&mut bob, Duration::from_millis(150)).await,
        "subscriber should not observe track or renegotiation updates for a publish canceled before commit"
    );
}
