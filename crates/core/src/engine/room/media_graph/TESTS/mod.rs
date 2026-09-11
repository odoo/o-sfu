#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test assertions use panic, unwrap, expect, and direct indexing for clear failure messages"
)]

use std::{mem, sync::Arc};

use o_sfu_router::{
    MediaKind as RouterMediaKind, RouterId,
    negotiation::derive_consumable_rtp_parameters,
    rtp::{MediaStream, Mid, Rid, Ssrc},
    test_support::rtp_samples::{
        sample_client_rtp_capabilities, sample_simulcast_video_rtp_parameters,
        sample_three_layer_simulcast_video_rtp_parameters, sample_video_rtp_parameters,
    },
};

use super::{
    ConsumerRouteState, ConsumerRouteTarget, ConsumerSetupOrigin, ConsumerSetupOutcome,
    DeclaredConsumerSetup, PendingConsumerSetup, SubscriptionKey, ValidatedPublish,
};
use crate::{
    Bitrate, RoomMediaLimits, VideoAdaptationTuning,
    engine::{
        ConnectionId, MediaWorkerId, RoomInstanceId, TestSourceKind, UserId,
        media_transport::{
            ProducerActivity, SessionUploadEncoding, SourceActivityRevision, SourceActivityUpdate,
            TransportConsumerRoute, TransportMediaId, TransportSessionKey,
        },
        metrics::RuntimeMetrics,
        room::{
            RoomAdmissionPolicy, RoomRuntimeContext, RouterPlacement, UserOutboundSender,
            state::RoomState,
        },
        source_model::{
            ConsumerSourceSelection, PolicyPauseReason, PublishedSourceId,
            SourceEncodingDescriptor, UploadLayerPolicyRole, UserStreamId,
            test_support::{
                TestSubscriptionStates, source_kind_for_stream_id,
                source_publish_intent_for_source, stream_id_for_source,
                subscription_intents_from_test_states,
            },
        },
    },
};

impl PendingConsumerSetup {
    pub(crate) fn declared(
        self,
        media: TransportMediaId,
        mid: Option<String>,
    ) -> DeclaredConsumerSetup {
        let route = self.target.transport_consumer_route(media);
        DeclaredConsumerSetup {
            pending: self,
            route,
            mid,
        }
    }
}

impl ConsumerRouteTarget {
    pub(in crate::engine::room) fn for_test(
        transport_route: TransportConsumerRoute,
        stream_id: UserStreamId,
        kind: RouterMediaKind,
    ) -> Self {
        Self::new(transport_route, stream_id, kind)
    }
}

fn test_state() -> RoomState {
    test_state_with_media_limits(RoomMediaLimits::default())
}

fn test_state_with_media_limits(media_limits: RoomMediaLimits) -> RoomState {
    let runtime_context = RoomRuntimeContext::new(
        RoomInstanceId::from_raw(0),
        RouterPlacement {
            router: RouterId(1),
            media_worker: MediaWorkerId::from_raw(0),
        },
        Vec::new(),
    );
    RoomState::new(
        &runtime_context,
        RoomAdmissionPolicy::new(4),
        media_limits,
        VideoAdaptationTuning::default(),
        sample_client_rtp_capabilities(),
    )
}

fn test_sender() -> UserOutboundSender {
    UserOutboundSender::channel(128, Arc::new(RuntimeMetrics::default())).0
}

fn join_test_user(state: &mut RoomState, user_id: &UserId) -> ConnectionId {
    state
        .apply_join(user_id, test_sender())
        .expect("test user should join")
        .receipt
        .transport_session_key
        .connection_id()
}

fn set_test_user_ready(state: &mut RoomState, user_id: &UserId) -> ConnectionId {
    let connection_id = state
        .user_connection_id(user_id)
        .expect("user should have a connection id");
    assert!(
        state
            .set_user_negotiated(user_id, connection_id, sample_client_rtp_capabilities())
            .is_some()
    );
    connection_id
}

fn scalable_video_states(active: bool) -> TestSubscriptionStates {
    TestSubscriptionStates {
        scalable_video: Some(active),
        audio_detector: None,
        readable_video: None,
        ..TestSubscriptionStates::default()
    }
}

fn test_upload_encodings() -> Vec<SessionUploadEncoding> {
    vec![
        SessionUploadEncoding {
            rid: "lo".to_owned(),
            max_bitrate: Some(Bitrate::from_kbps(180)),
            resolution_scale: Some(4),
            max_framerate: Some(15),
        },
        SessionUploadEncoding {
            rid: "hi".to_owned(),
            max_bitrate: Some(Bitrate::from_kbps(950)),
            resolution_scale: Some(1),
            max_framerate: None,
        },
    ]
}

fn assert_upload_profile_metadata(encodings: &[&SourceEncodingDescriptor]) {
    assert_eq!(encodings[0].resolution_scale(), Some(4));
    assert_eq!(encodings[0].max_framerate(), Some(15));
    assert_eq!(
        encodings[0].policy_role(),
        Some(UploadLayerPolicyRole::Thumbnail)
    );
    assert_eq!(encodings[1].resolution_scale(), Some(1));
    assert_eq!(encodings[1].max_framerate(), None);
    assert_eq!(
        encodings[1].policy_role(),
        Some(UploadLayerPolicyRole::Featured)
    );
}

fn install_test_consumer_route(
    state: &mut RoomState,
    producer_user_id: &UserId,
    consumer_user_id: &UserId,
) -> (SubscriptionKey, ConnectionId) {
    join_test_user(state, producer_user_id);
    join_test_user(state, consumer_user_id);
    let consumer_connection_id = set_test_user_ready(state, consumer_user_id);
    commit_test_publication(
        state,
        producer_user_id,
        TestSourceKind::ScalableVideo,
        TransportMediaId::new(1),
    );
    let route_key = SubscriptionKey::new(
        consumer_user_id,
        producer_user_id,
        &stream_id_for_source(TestSourceKind::ScalableVideo),
    );
    let mut setups = state
        .plan_missing_consumers(consumer_user_id, consumer_connection_id)
        .expect("consumer session should exist");
    assert_eq!(setups.len(), 1);
    let setup = setups.pop().expect("consumer setup should be planned");
    assert!(matches!(
        commit_setup_with_media(
            state,
            setup,
            TransportMediaId::new(2),
            Some(String::from("camera-down")),
        ),
        ConsumerSetupOutcome::Committed { .. }
    ));
    (route_key, consumer_connection_id)
}

fn commit_test_publication_with_rtp(
    state: &mut RoomState,
    user_id: &UserId,
    connection_id: ConnectionId,
    stream_type: TestSourceKind,
    consumable_rtp_parameters: MediaStream,
    transport_media_id: TransportMediaId,
) -> PublishedSourceId {
    let intent = source_publish_intent_for_source(stream_type);
    state
        .topology
        .commit_publication(
            ValidatedPublish {
                session_key: state.transport_user_key(user_id, connection_id),
                intent,
            },
            consumable_rtp_parameters,
            &[],
            transport_media_id,
        )
        .expect("test publication should commit")
}

fn commit_test_publication(
    state: &mut RoomState,
    user_id: &UserId,
    stream_type: TestSourceKind,
    transport_media_id: TransportMediaId,
) -> PublishedSourceId {
    let connection_id = state
        .user_connection_id(user_id)
        .expect("publisher user should have a connection id");
    commit_test_publication_with_rtp(
        state,
        user_id,
        connection_id,
        stream_type,
        sample_video_rtp_parameters(None, 77_777),
        transport_media_id,
    )
}

fn commit_test_video_publication(
    state: &mut RoomState,
    user_id: &UserId,
    stream_type: TestSourceKind,
    transport_media_id: TransportMediaId,
    ssrc: u32,
) -> PublishedSourceId {
    let connection_id = state
        .user_connection_id(user_id)
        .expect("publisher should have a connection id");
    let consumable_rtp_parameters = derive_consumable_rtp_parameters(
        &sample_video_rtp_parameters(None, ssrc),
        &state.router_rtp_capabilities(),
    )
    .expect("publisher RTP parameters should derive consumable router parameters");
    commit_test_publication_with_rtp(
        state,
        user_id,
        connection_id,
        stream_type,
        consumable_rtp_parameters,
        transport_media_id,
    )
}

fn pending_consumer_setup() -> (
    RoomState,
    UserId,
    UserId,
    ConnectionId,
    UserStreamId,
    PendingConsumerSetup,
) {
    let mut state = test_state();
    let publisher_user_id = UserId::Integer(1);
    let subscriber_user_id = UserId::Integer(2);
    let stream_id = stream_id_for_source(TestSourceKind::ScalableVideo);

    join_test_user(&mut state, &publisher_user_id);
    join_test_user(&mut state, &subscriber_user_id);

    let subscriber_connection_id = set_test_user_ready(&mut state, &subscriber_user_id);
    commit_test_video_publication(
        &mut state,
        &publisher_user_id,
        TestSourceKind::ScalableVideo,
        TransportMediaId::new(10),
        22_222,
    );
    let mut planned_setups = state
        .plan_missing_consumers(&subscriber_user_id, subscriber_connection_id)
        .expect("subscriber session should exist");
    assert_eq!(planned_setups.len(), 1);
    let setup = planned_setups.pop().expect("setup should be planned");

    (
        state,
        publisher_user_id,
        subscriber_user_id,
        subscriber_connection_id,
        stream_id,
        setup,
    )
}

fn commit_setup_with_media(
    state: &mut RoomState,
    setup: PendingConsumerSetup,
    media: TransportMediaId,
    mid: Option<String>,
) -> ConsumerSetupOutcome {
    let setup = setup.declared(media, mid);
    state.commit_declared_consumer_setup(setup, ConsumerSetupOrigin::Subscribe)
}

fn commit_setup(state: &mut RoomState, setup: PendingConsumerSetup) -> ConsumerSetupOutcome {
    commit_setup_with_media(
        state,
        setup,
        TransportMediaId::new(20),
        Some(String::from("m0")),
    )
}

#[test]
fn consumer_setup_mid_uses_transport_then_negotiated_then_id() {
    for (negotiated, transport, expected) in [
        (None, None, "consumer-1"),
        (Some("negotiated-mid"), None, "negotiated-mid"),
        (
            Some("negotiated-mid"),
            Some(String::from("transport-mid")),
            "transport-mid",
        ),
    ] {
        let (mut state, publisher, subscriber, _, stream, mut setup) = pending_consumer_setup();
        if let Some(mid) = negotiated {
            setup.rtp = mem::take(&mut setup.rtp).with_mid(mid);
        }
        assert!(matches!(
            commit_setup_with_media(&mut state, setup, TransportMediaId::new(20), transport),
            ConsumerSetupOutcome::Committed { .. }
        ));
        let key = SubscriptionKey::new(&subscriber, &publisher, &stream);

        assert_eq!(
            state
                .topology
                .committed_consumer_route_for_key(&key)
                .map(|route| route.mid),
            Some(expected)
        );
    }
}

#[test]
fn stale_replaced_connection_cannot_update_download_state() {
    let mut state = test_state();
    let producer_user_id = UserId::Integer(1);
    let consumer_user_id = UserId::Integer(2);

    let (route_key, stale_connection_id) =
        install_test_consumer_route(&mut state, &producer_user_id, &consumer_user_id);

    join_test_user(&mut state, &consumer_user_id);

    let intents = subscription_intents_from_test_states(&scalable_video_states(false));
    state.plan_receiver_route_work(
        &consumer_user_id,
        stale_connection_id,
        &producer_user_id,
        &intents,
    );
    assert!(
        state
            .topology
            .subscription_intent(&route_key)
            .active()
            .unwrap_or(true),
        "stale subscription updates must not overwrite the replacement user's stored preferences"
    );
    assert_eq!(
        state.media_counts().subscriptions,
        0,
        "replacement join should clear stale consumer routes"
    );
}

#[test]
fn stored_intent_attaches_before_readiness_then_reserves_setup() {
    let mut state = test_state();
    let publisher = UserId::Integer(1);
    let receiver = UserId::Integer(2);
    let publisher_connection = join_test_user(&mut state, &publisher);
    let receiver_connection = join_test_user(&mut state, &receiver);
    assert!(
        state
            .set_user_negotiated(
                &publisher,
                publisher_connection,
                sample_client_rtp_capabilities()
            )
            .is_some()
    );
    let intent = source_publish_intent_for_source(TestSourceKind::ScalableVideo);
    let intents = subscription_intents_from_test_states(&scalable_video_states(false));
    assert!(
        state
            .apply_receiver_intent(&receiver, receiver_connection, &publisher, &intents)
            .is_some()
    );
    let publish = state
        .validate_publish(&publisher, publisher_connection, &intent)
        .expect("ready publisher should validate");
    let rtp = derive_consumable_rtp_parameters(
        &sample_video_rtp_parameters(None, 22_222),
        &state.router_rtp_capabilities(),
    )
    .expect("publisher RTP parameters should be consumable");
    let commit = state
        .commit_publish_reservation(publish, rtp, &[], TransportMediaId::new(10))
        .expect("publication should commit");
    assert!(commit.receiver_route_work.setups.is_empty());
    let source_id = state
        .published_source_id(&publisher, publisher_connection, intent.stream_id())
        .expect("publication should be current");
    assert_eq!(
        state
            .topology
            .source_selection_for_test(&receiver, source_id),
        Some(ConsumerSourceSelection::open(false))
    );
    assert_eq!(
        state.set_user_negotiated(
            &receiver,
            receiver_connection,
            sample_client_rtp_capabilities()
        ),
        Some(true)
    );
    let readiness = state
        .refresh_consumer_readiness(&receiver, receiver_connection, &[])
        .expect("receiver should remain current");
    assert_eq!(readiness.work.setups.len(), 1);
}

#[test]
fn missing_consumer_setup_applies_video_cap_before_transport() {
    let mut state = test_state_with_media_limits(RoomMediaLimits::try_new(4, 1).unwrap());
    let publisher = UserId::Integer(1);
    let receiver = UserId::Integer(2);
    join_test_user(&mut state, &publisher);
    join_test_user(&mut state, &receiver);
    let receiver_connection = set_test_user_ready(&mut state, &receiver);
    let scalable = commit_test_video_publication(
        &mut state,
        &publisher,
        TestSourceKind::ScalableVideo,
        TransportMediaId::new(10),
        22_222,
    );
    let readable = commit_test_video_publication(
        &mut state,
        &publisher,
        TestSourceKind::ReadableVideo,
        TransportMediaId::new(11),
        33_333,
    );
    let setups = state
        .plan_missing_consumers(&receiver, receiver_connection)
        .expect("receiver should remain current");
    assert_eq!(setups.len(), 2);
    let declared_active = |source_id| {
        setups
            .iter()
            .find(|setup| setup.target.source_id == source_id)
            .expect("published source should have a setup")
            .reservation
            .declared_activity()
            .is_active()
    };
    assert!(declared_active(readable));
    assert!(!declared_active(scalable));
    assert_eq!(
        state
            .topology
            .source_selection_for_test(&receiver, scalable)
            .expect("pending route should retain its policy selection")
            .policy_pause_reason(),
        Some(PolicyPauseReason::VideoDownloadLimit)
    );
}

#[test]
fn consumer_setup_commit_uses_latest_room_state() {
    let (
        mut state,
        publisher_user_id,
        subscriber_user_id,
        subscriber_connection_id,
        stream_id,
        mut setup,
    ) = pending_consumer_setup();
    let target_session = &setup.target.session;
    let target_worker = MediaWorkerId::from_raw(1);
    setup.target.session = TransportSessionKey::new(
        target_session.room_instance_id(),
        target_worker,
        target_session.connection_id(),
        target_session.user_id().clone(),
    );

    let publisher_connection_id = state
        .user_connection_id(&publisher_user_id)
        .expect("publisher should have a connection id");
    let source_id = state
        .published_source_id(&publisher_user_id, publisher_connection_id, &stream_id)
        .expect("published source should exist");
    assert!(
        state
            .topology
            .set_published_source_activity(source_id, publisher_connection_id, false)
            .is_some()
    );

    let intents = subscription_intents_from_test_states(&scalable_video_states(false));
    let change = state.plan_receiver_route_work(
        &subscriber_user_id,
        subscriber_connection_id,
        &publisher_user_id,
        &intents,
    );
    assert!(change.setups.is_empty());

    let ConsumerSetupOutcome::Committed {
        track_snapshot: snapshot,
        remote_source_activity,
        transport_activity_update,
        ..
    } = commit_setup(&mut state, setup)
    else {
        panic!("current room state should still accept the setup");
    };
    assert!(
        snapshot
            .snapshot
            .tracks
            .iter()
            .any(|track| !track.producer_active)
    );
    let transport_activity_update =
        transport_activity_update.expect("transport declaration should be corrected");
    assert!(!transport_activity_update);
    let remote_source_activity =
        remote_source_activity.expect("remote setup should reconcile source activity");
    assert_eq!(remote_source_activity.target_media_worker_id, target_worker);
    assert_eq!(
        remote_source_activity.update,
        SourceActivityUpdate::new(
            ProducerActivity::Inactive,
            SourceActivityRevision::default().next(),
        )
    );
    assert_eq!(
        state.consumer_route_state(&subscriber_user_id, &publisher_user_id, &stream_id),
        Some(ConsumerRouteState::Inactive)
    );
}

#[test]
fn consumer_setup_commit_releases_stale_receiver_plan() {
    let (
        mut state,
        _publisher_user_id,
        subscriber_user_id,
        _subscriber_connection_id,
        _stream_id,
        setup,
    ) = pending_consumer_setup();
    assert_eq!(state.media_counts().subscriptions, 1);

    state
        .users
        .get_mut(&subscriber_user_id)
        .expect("subscriber should exist")
        .connection_id = ConnectionId::from_raw(900);
    let ConsumerSetupOutcome::Released(..) = commit_setup(&mut state, setup) else {
        panic!("stale receiver setup should be released");
    };

    assert_eq!(state.media_counts().subscriptions, 0);
}

#[test]
fn consumer_setup_commit_releases_stale_publication_plan() {
    let (
        mut state,
        publisher_user_id,
        _subscriber_user_id,
        _subscriber_connection_id,
        stream_id,
        setup,
    ) = pending_consumer_setup();
    assert_eq!(state.media_counts().subscriptions, 1);

    let source_id = state
        .topology
        .source_id_for_owner_stream(&publisher_user_id, &stream_id)
        .expect("publisher source should exist");
    assert!(state.topology.remove_source_for_test(source_id));
    let ConsumerSetupOutcome::Released(..) = commit_setup(&mut state, setup) else {
        panic!("stale producer setup should be released");
    };

    assert_eq!(state.media_counts().subscriptions, 0);
}

#[test]
fn commit_publish_reservation_registers_all_source_encodings() {
    let mut state = test_state();
    let user_id = UserId::Integer(1);

    join_test_user(&mut state, &user_id);
    let connection_id = set_test_user_ready(&mut state, &user_id);

    let consumable_rtp_parameters = derive_consumable_rtp_parameters(
        &sample_simulcast_video_rtp_parameters(Some("camera-0")),
        &state.router_rtp_capabilities(),
    )
    .expect("simulcast RTP parameters should derive consumable router parameters");
    let validated_publish = state
        .validate_publish(
            &user_id,
            connection_id,
            &source_publish_intent_for_source(TestSourceKind::ScalableVideo),
        )
        .expect("publish descriptor should validate once the user is publish-ready");
    let transport_media_id = TransportMediaId::new(101);
    let upload_encodings = test_upload_encodings();

    assert!(
        state
            .commit_publish_reservation(
                validated_publish,
                consumable_rtp_parameters,
                &upload_encodings,
                transport_media_id,
            )
            .is_some()
    );

    let source_id = state
        .inspect_source_id_for_transport_media_id(transport_media_id)
        .expect("transport media should resolve to the committed source");
    let source = state
        .topology
        .source_descriptor(source_id)
        .expect("source registry should own the committed source");
    assert_eq!(source.owner().user_id(), &user_id);
    assert_eq!(
        source_kind_for_stream_id(source.stream_id()),
        Some(TestSourceKind::ScalableVideo)
    );
    assert_eq!(source.mid().map(Mid::as_str), Some("camera-0"));
    let encodings = source.encodings().collect::<Vec<_>>();
    assert_eq!(encodings.len(), 2);
    assert_eq!(encodings[0].rid().map(Rid::as_str), Some("lo"));
    assert_eq!(encodings[1].rid().map(Rid::as_str), Some("hi"));
    assert_eq!(encodings[0].primary_ssrc(), Some(Ssrc::new(31_001)));
    assert_eq!(encodings[1].primary_ssrc(), Some(Ssrc::new(31_002)));
    assert_eq!(encodings[0].max_bitrate(), Some(Bitrate::from_kbps(150)));
    assert_eq!(encodings[1].max_bitrate(), Some(Bitrate::from_kbps(900)));
    assert_upload_profile_metadata(&encodings);
    assert_eq!(
        state
            .inspect_source_encoding_ids_for_transport_media_id(transport_media_id)
            .expect("transport media should resolve to source encoding ids"),
        encodings
            .iter()
            .map(|encoding| encoding.encoding_id())
            .collect::<Vec<_>>()
    );
}

#[test]
fn publication_preserves_negotiated_upload_roles() {
    let mut upload_encodings = test_upload_encodings();
    upload_encodings.insert(
        1,
        SessionUploadEncoding {
            rid: "mid".to_owned(),
            max_bitrate: Some(Bitrate::from_kbps(450)),
            resolution_scale: Some(2),
            max_framerate: None,
        },
    );
    for expected in [
        &[
            ("lo", 4, UploadLayerPolicyRole::DegradedThumbnail),
            ("mid", 2, UploadLayerPolicyRole::Thumbnail),
            ("hi", 1, UploadLayerPolicyRole::Featured),
        ][..],
        &[("mid", 2, UploadLayerPolicyRole::Thumbnail)],
    ] {
        let mut state = test_state();
        let user_id = UserId::Integer(1);
        join_test_user(&mut state, &user_id);
        let connection_id = set_test_user_ready(&mut state, &user_id);
        let offered = sample_three_layer_simulcast_video_rtp_parameters(Some("camera-0"));
        let accepted_rid = |rid: &str| expected.iter().any(|(accepted, _, _)| *accepted == rid);
        let parameters = MediaStream::new(
            offered.formats().cloned().collect(),
            offered.header_extensions().cloned().collect(),
            offered
                .bindings()
                .filter(|binding| binding.rid().is_some_and(accepted_rid))
                .cloned()
                .collect(),
        );
        let parameters =
            derive_consumable_rtp_parameters(&parameters, &state.router_rtp_capabilities())
                .expect("accepted RID publication should negotiate");
        let publish = state
            .validate_publish(
                &user_id,
                connection_id,
                &source_publish_intent_for_source(TestSourceKind::ScalableVideo),
            )
            .expect("ready publisher should validate");
        let accepted_upload_encodings = upload_encodings
            .iter()
            .filter(|encoding| accepted_rid(&encoding.rid))
            .cloned()
            .collect::<Vec<_>>();
        let media = TransportMediaId::new(101);
        assert!(
            state
                .commit_publish_reservation(publish, parameters, &accepted_upload_encodings, media)
                .is_some()
        );
        let source_id = state
            .inspect_source_id_for_transport_media_id(media)
            .unwrap();
        let source = state.topology.source_descriptor(source_id).unwrap();
        let metadata = source
            .selectable_encodings()
            .map(|encoding| {
                (
                    encoding.rid().map(Rid::as_str),
                    encoding.resolution_scale(),
                    encoding.policy_role(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            metadata,
            expected
                .iter()
                .map(|(rid, scale, role)| (Some(*rid), Some(*scale), Some(*role)))
                .collect::<Vec<_>>()
        );
    }
}
