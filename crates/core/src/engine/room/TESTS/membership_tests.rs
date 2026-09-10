use super::fixtures::*;

#[tokio::test]
async fn join_user_enforces_capacity() {
    let manager = RoomManager::for_test_with_admission_policy(RoomAdmissionPolicy::new(1));
    let room = manager
        .serve_room(
            "issuer-a",
            TEST_ROOM_KEY.into(),
            &RoomConfig::default(),
            None,
        )
        .await
        .expect("test room should be served");
    let (tx1, _rx1) = test_sender();
    let result = room
        .test_api()
        .join_user(UserId::Integer(1), None, UserPermissions::default(), tx1)
        .await;
    assert!(result.is_ok());

    let (tx2, _rx2) = test_sender();
    let result = room
        .test_api()
        .join_user(UserId::Integer(2), None, UserPermissions::default(), tx2)
        .await;
    assert_eq!(result, Err(RoomJoinError::RoomFull));
}

#[tokio::test]
async fn reconnection_bypasses_capacity_and_replaces_existing_connection() {
    let manager = RoomManager::for_test_with_admission_policy(RoomAdmissionPolicy::new(1));
    let room = manager
        .serve_room(
            "issuer-a",
            TEST_ROOM_KEY.into(),
            &RoomConfig::default(),
            None,
        )
        .await
        .expect("test room should be served");
    let user_id = UserId::Integer(1);
    let (tx1, mut rx1) = test_sender();
    let first_connection = room
        .test_api()
        .join_user(user_id.clone(), None, UserPermissions::default(), tx1)
        .await
        .expect("first join should succeed");
    assert_eq!(
        room.state.write().await.set_user_negotiated(
            &user_id,
            first_connection,
            test_client_rtp_capabilities(),
        ),
        Some(true)
    );

    let (tx2, _rx2) = test_sender();
    let second_connection = room
        .test_api()
        .join_user(user_id.clone(), None, UserPermissions::default(), tx2)
        .await
        .expect("replacement join should succeed");

    assert_ne!(first_connection, second_connection);
    let mut state = room.state.write().await;
    assert!(state.session_client_rtp_codec_names(&user_id).is_none());
    assert_eq!(
        state.set_user_negotiated(&user_id, first_connection, test_client_rtp_capabilities()),
        None
    );
    assert!(state.session_client_rtp_codec_names(&user_id).is_none());
    let (user_count, _active_stream_counts) = state.user_stats_counts();
    drop(state);
    assert_eq!(user_count, 1);
    assert!(matches!(
        rx1.try_recv().ok(),
        Some(UserOutbound::Close(UserCloseReason::Replaced))
    ));
    assert!(
        !room
            .remove_user_with_teardown(
                &user_id,
                first_connection,
                RoomEffectContext::state_only(None),
            )
            .await
    );
    assert_eq!(
        room.test_api().user_connection_id(&user_id).await,
        Some(second_connection)
    );
}

#[tokio::test]
async fn leave_user_sends_departure_to_remaining_peers() {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room(
            "issuer-a",
            TEST_ROOM_KEY.into(),
            &RoomConfig::default(),
            None,
        )
        .await
        .expect("test room should be served");
    let (tx1, mut rx1) = test_sender();
    let (tx2, _rx2) = test_sender();
    join_user_with_sender(&room, UserId::Integer(1), tx1).await;
    let bob_connection = join_user_with_sender(&room, UserId::Integer(2), tx2).await;

    room.remove_user_with_teardown(
        &UserId::Integer(2),
        bob_connection,
        RoomEffectContext::state_only(None),
    )
    .await;

    let msg = rx1.try_recv();
    assert!(matches!(
        msg,
        Ok(UserOutbound::Message(RoomEventMessage::UserDeparted {
            user_id: UserId::Integer(2)
        }))
    ));
    assert_eq!(room.user_count().await, 1);
}

#[tokio::test]
async fn mismatched_stale_close_keeps_other_user_routing() {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room(
            "issuer-a",
            TEST_ROOM_KEY.into(),
            &RoomConfig::default(),
            None,
        )
        .await
        .expect("test room should be served");
    let alice_id = UserId::Integer(1);
    let bob_id = UserId::Integer(2);
    let (alice_tx, _alice_rx) = test_sender();
    let (bob_tx, _bob_rx) = test_sender();
    let alice_connection = join_user_with_sender(&room, alice_id.clone(), alice_tx).await;
    let bob_connection = join_user_with_sender(&room, bob_id.clone(), bob_tx).await;

    assert!(
        !room
            .remove_user_with_teardown(
                &alice_id,
                bob_connection,
                RoomEffectContext::state_only(None),
            )
            .await
    );

    let inspect = room.test_api();
    assert_eq!(
        inspect.user_connection_id(&alice_id).await,
        Some(alice_connection)
    );
    assert_eq!(
        inspect.user_connection_id(&bob_id).await,
        Some(bob_connection)
    );
    assert!(inspect.home_media_worker_id(&bob_id).await.is_some());
}

#[tokio::test]
async fn replacement_join_closes_displaced_transport_user() {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room(
            "issuer-a",
            TEST_ROOM_KEY.into(),
            &RoomConfig::default(),
            None,
        )
        .await
        .expect("test room should be served");
    let media_transport = real_adapter();
    let user_id = UserId::Integer(1);
    let (first_tx, first_rx) = test_sender();
    let first_admission = manager
        .join_user(
            room.uuid(),
            JoinUserRequest {
                user_id: user_id.clone(),
                label: None,
                permissions: UserPermissions::default(),
                sender: first_tx,
            },
            &media_transport,
        )
        .await
        .expect("first join should succeed");
    let first_connection = first_admission.connection_id;
    let first_key = first_admission.transport_session_key.clone();
    media_transport
        .create_initial_session_offer("test-room", &first_key)
        .await
        .expect("first transport user should create an offer");
    media_transport
        .test_api()
        .set_session_transport_health(&first_key, TransportSessionHealth::Disconnected);
    assert_eq!(
        media_transport.session_transport_health(&first_key),
        Some(TransportSessionHealth::Disconnected)
    );
    drop(first_rx);

    let (second_tx, _second_rx) = test_sender();
    let second_connection = manager
        .join_user(
            room.uuid(),
            JoinUserRequest {
                user_id: user_id.clone(),
                label: None,
                permissions: UserPermissions::default(),
                sender: second_tx,
            },
            &media_transport,
        )
        .await
        .expect("replacement join should succeed")
        .connection_id;

    assert_ne!(first_connection, second_connection);
    assert_eq!(media_transport.session_transport_health(&first_key), None);
    assert_eq!(
        room.test_api().user_connection_id(&user_id).await,
        Some(second_connection)
    );
    assert!(
        room.test_api()
            .home_media_worker_id(&user_id)
            .await
            .is_some()
    );
}

#[tokio::test]
async fn removing_publisher_clears_media_state_and_transport_routes() {
    let (room, media_transport, mut publisher_rx, mut subscriber_rx) =
        setup_two_ready_users().await;
    publish_simulcast_camera(&room, &UserId::Integer(1), &media_transport).await;
    drain_outbound(&mut publisher_rx);
    assert!(
        drain_outbound(&mut subscriber_rx)
            .iter()
            .any(|message| matches!(message, UserOutbound::RemoteTracks(_)))
    );
    let connection_id = user_connection_id(&room, &UserId::Integer(1)).await;
    let transport_media_id = room
        .test_api()
        .producer_transport_media_id(
            &UserId::Integer(1),
            connection_id,
            TestSourceKind::ScalableVideo,
        )
        .await
        .expect("published camera should expose transport media");
    assert!(
        room.test_api()
            .deactivate_publication(
                &UserId::Integer(1),
                &stream_id_for_source(TestSourceKind::ScalableVideo),
                &media_transport,
            )
            .await
    );

    assert!(
        room.remove_user_with_teardown(
            &UserId::Integer(1),
            connection_id,
            RoomEffectContext::runtime(&media_transport),
        )
        .await
    );
    let subscriber_messages = drain_outbound(&mut subscriber_rx);
    assert!(subscriber_messages.iter().any(|message| matches!(
        message,
        UserOutbound::RemoteTracks(snapshot)
            if snapshot.requires_negotiation && snapshot.tracks.is_empty()
    )));

    assert_eq!(room.test_api().producer_count().await, 0);
    assert_eq!(room.test_api().consumer_count().await, 0);
    assert!(
        room.test_api()
            .producer_stream_type_for_transport_media_id(transport_media_id)
            .await
            .is_none()
    );
    assert!(
        media_transport
            .test_api()
            .route_entry_by_media_id(transport_media_id)
            .await
            .is_none()
    );
}
