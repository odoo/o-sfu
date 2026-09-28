use super::support::*;

#[tokio::test]
async fn protocol_core_replays_real_server_welcome_peer_snapshot() -> TestResult {
    let server = TestServerBuilder::new().spawn_required().await?;
    let room = require_some(
        create_room(&server, "issuer-a", CreateRoomQuery::default()).await,
        "test room should be served",
    )?;
    let _existing = connect_until_welcome(&server, &room, UserId::Integer(31)).await?;
    let peer = connect_until_welcome(&server, &room, UserId::Integer(32)).await?;

    assert_eq!(peer.core.state(), BundleConnectionState::Authenticated);
    assert_eq!(
        &peer.available_features,
        &AvailableFeatures {
            rtc: true,
            transcription: false,
            audio_recording: false,
            video_recording: false,
        }
    );
    assert_eq!(
        &peer.recording_state,
        &RecordingState {
            recording: Some(false),
            audio: Some(false),
            transcription: Some(false),
            video: Some(false),
        }
    );
    assert_eq!(
        peer.state_changes,
        vec![
            BundleStateChange {
                state: BundleConnectionState::Connecting,
                cause: None,
            },
            BundleStateChange {
                state: BundleConnectionState::Authenticated,
                cause: None,
            },
        ]
    );
    assert_eq!(
        peer.updates,
        vec![BundleUpdate::SessionInfoChange(BTreeMap::from([(
            bundle_session_info_key(&ProtocolSessionId::Integer(31)),
            ProtocolSessionInfo::default().snapshot_complete(),
        )]))]
    );
    Ok(())
}

#[tokio::test]
async fn protocol_core_maps_real_server_auth_failure_to_closed_state() -> TestResult {
    let server = TestServerBuilder::new().spawn_required().await?;
    let room = require_some(
        create_room(&server, "issuer-a", CreateRoomQuery::default()).await,
        "test room should be served",
    )?;

    let mut peer = ProtocolHarnessPeer::default();
    require_some(
        peer.connect(
            &server.url(),
            "invalid.jwt.payload",
            Some(room.uuid().to_owned()),
        )
        .await,
        "protocol core should open websocket",
    )?;

    let close_code = {
        let websocket = require_some(
            peer.websocket.as_mut(),
            "protocol core should keep the websocket until close",
        )?;
        read_close_code(websocket).await
    };
    assert_eq!(close_code, Some(CloseCode::Library(4106)));

    require_some(
        peer.observe_close(4106).await,
        "protocol core should consume the auth failure close code",
    )?;

    assert_eq!(peer.core.state(), BundleConnectionState::Closed);
    assert_eq!(
        peer.state_changes,
        vec![
            BundleStateChange {
                state: BundleConnectionState::Connecting,
                cause: None,
            },
            BundleStateChange {
                state: BundleConnectionState::Closed,
                cause: Some(String::from("auth_failed")),
            },
        ]
    );
    assert!(peer.timers.is_empty());
    Ok(())
}

#[tokio::test]
async fn protocol_core_answers_real_server_offer_when_enabled() -> TestResult {
    let (_server, _room, peer) =
        setup_protocol_peer("issuer-protocol", UserId::Integer(33)).await?;

    assert_eq!(peer.core.state(), BundleConnectionState::Connected);
    assert!(
        peer.state_changes.iter().any(|change| {
            change.state == BundleConnectionState::Connected && change.cause.is_none()
        }),
        "protocol offer handling should drive the protocol core into the connected state"
    );
    Ok(())
}

#[tokio::test]
async fn protocol_core_receives_protocol_broadcast_and_peer_updates() -> TestResult {
    let (_server, _room, mut alice, mut bob) = Box::pin(setup_protocol_peers(
        "issuer-protocol-events",
        UserId::Integer(41),
        UserId::Integer(42),
    ))
    .await?;
    bob.updates.clear();

    require_some(
        alice.broadcast(json!({ "text": "hello" })).await,
        "alice should broadcast through the protocol core",
    )?;
    require_some(
        bob.read_server_frame().await,
        "bob should consume translated broadcast",
    )?;
    assert_eq!(
        bob.updates.last(),
        Some(&BundleUpdate::Broadcast(BundleBroadcastUpdate {
            sender_id: ProtocolSessionId::Integer(41),
            message: json!({ "text": "hello" }),
        }))
    );

    require_some(
        alice
            .update_info(ProtocolSessionInfo {
                is_talking: Some(true),
                ..ProtocolSessionInfo::default()
            })
            .await,
        "alice should emit protocol info update",
    )?;
    require_some(
        bob.read_server_frame().await,
        "bob should consume translated peer info",
    )?;
    assert_eq!(
        bob.updates.last(),
        Some(&BundleUpdate::SessionInfoChange(BTreeMap::from([(
            bundle_session_info_key(&ProtocolSessionId::Integer(41)),
            ProtocolSessionInfo {
                is_talking: Some(true),
                ..ProtocolSessionInfo::default().snapshot_complete()
            },
        )])))
    );
    require_some(
        alice.read_server_frame().await,
        "alice should consume its own translated peer info frame before disconnect assertions",
    )?;

    let close_result = require_some(
        bob.websocket.as_mut(),
        "bob websocket should stay connected before close",
    )?
    .close(None)
    .await;
    assert!(close_result.is_ok());
    bob.websocket = None;
    require_some(
        alice.read_server_frame().await,
        "alice should consume translated peer disconnect",
    )?;
    assert_eq!(
        alice.updates.last(),
        Some(&BundleUpdate::Disconnect(BundleDisconnectUpdate {
            user_id: ProtocolSessionId::Integer(42),
        }))
    );
    Ok(())
}

#[tokio::test]
async fn protocol_user_emits_peerjoined_message_for_existing_peers() -> TestResult {
    let (server, room, mut alice) =
        setup_protocol_peer("issuer-protocol-peerjoined", UserId::Integer(43)).await?;
    let _bob = connect_protocol_peer(&server, &room, UserId::Integer(44)).await?;

    let peer_joined = require_some(
        read_single_protocol_server_message(&mut alice).await,
        "existing peer should receive a peerjoined message",
    )?;
    assert!(
        matches!(peer_joined, ServerMessage::PeerJoined(_)),
        "existing peers should receive peerjoined rather than a generic peerinfo frame on join"
    );
    Ok(())
}

#[tokio::test]
async fn protocol_user_replacement_emits_peerleft_then_peerjoined_for_existing_peers() -> TestResult
{
    let (server, room, mut alice, mut bob) = Box::pin(setup_protocol_peers(
        "issuer-protocol-peer-replacement",
        UserId::Integer(45),
        UserId::Integer(46),
    ))
    .await?;
    alice.updates.clear();

    let mut replacement = connect_protocol_peer(&server, &room, UserId::Integer(46)).await?;

    let close_code = {
        let websocket = require_some(
            bob.websocket.as_mut(),
            "bob websocket should remain until replacement close",
        )?;
        read_close_code(websocket).await
    };
    assert_eq!(close_code, Some(CloseCode::Library(4108)));

    let peer_left = require_some(
        read_single_protocol_server_message(&mut alice).await,
        "replacement should send peerleft",
    )?;
    assert!(
        matches!(peer_left, ServerMessage::PeerLeft(_)),
        "replacement should emit peerleft before rejoin"
    );
    assert_eq!(
        alice.updates.last(),
        Some(&BundleUpdate::Disconnect(BundleDisconnectUpdate {
            user_id: ProtocolSessionId::Integer(46),
        }))
    );

    let peer_joined = require_some(
        read_single_protocol_server_message(&mut alice).await,
        "replacement should send peerjoined",
    )?;
    assert!(
        matches!(peer_joined, ServerMessage::PeerJoined(_)),
        "replacement should emit peerjoined after peerleft"
    );
    assert_eq!(
        alice.updates.last(),
        Some(&BundleUpdate::SessionInfoChange(BTreeMap::from([(
            bundle_session_info_key(&ProtocolSessionId::Integer(46)),
            ProtocolSessionInfo::default().snapshot_complete(),
        )]))),
    );
    require_some(
        close_peer_and_wait_for_room_cleanup(&mut alice, &room, &ProtocolSessionId::Integer(45))
            .await,
        "alice should clean up",
    )?;
    require_some(
        close_peer_and_wait_for_room_cleanup(
            &mut replacement,
            &room,
            &ProtocolSessionId::Integer(46),
        )
        .await,
        "replacement should clean up",
    )?;
    Ok(())
}
