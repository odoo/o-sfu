use std::num::{NonZeroU64, NonZeroUsize};

use o_sfu_rfc::webrtc::sdp;

use super::support::*;
use crate::{
    engine::{
        media_transport::TransportSourceKey,
        room::{RoomRuntimePolicy, media_graph::ConsumerRouteState},
    },
    prelude::{
        MediaSession, NegotiationOffer, RoomWorkerPolicy, RuntimeFeatureFlags, SessionError,
        SfuCore, SourceDeactivateIntent,
    },
};

#[tokio::test]
async fn media_session_initial_offer_is_one_shot() {
    let (mut session, mut remote) = build_publisher_session(55_202).await;
    let offer = session
        .establish()
        .await
        .expect("initial offer should be created")
        .expect("initial offer should be present");

    assert!(
        session
            .establish()
            .await
            .expect("second initial offer should not fail")
            .is_none()
    );

    let answer_sdp = remote_answer_sdp(&mut remote, &offer.sdp);
    let offer = session
        .answer(&answer_sdp)
        .await
        .expect("initial answer should apply");
    assert!(offer.is_none());
    assert!(matches!(
        session.answer(&answer_sdp).await,
        Err(SessionError::NoPendingRequest)
    ));
    assert!(
        session
            .establish()
            .await
            .expect("stable session should reject a new initial offer without failing")
            .is_none()
    );
}

#[tokio::test]
async fn media_session_keeps_pending_offer_after_invalid_answer() {
    let (mut session, mut remote) = build_publisher_session(55_205).await;
    let offer = session
        .establish()
        .await
        .expect("initial offer should be created")
        .expect("initial offer should be present");

    assert_invalid_answer_is_rejected(&mut session).await;

    let answer_sdp = remote_answer_sdp(&mut remote, &offer.sdp);
    let offer = session
        .answer(&answer_sdp)
        .await
        .expect("pending offer should accept a later valid answer");
    assert!(offer.is_none());
}

#[tokio::test]
async fn media_session_keeps_staged_publish_after_invalid_answer() {
    let mut fixture = build_publisher_fixture(55_225).await;
    establish_session(&mut fixture.session, &mut fixture.remote).await;
    let video = TestSourceKind::ScalableVideo;
    let offer = fixture
        .session
        .publish(source_publish_intent_for_source(video))
        .await
        .expect("publish should stage through the media session");
    let offer = expect_renegotiation_offer(offer);

    assert_invalid_answer_is_rejected(&mut fixture.session).await;
    fixture.assert_staged(video, true).await;

    let answer_sdp = remote_answer_sdp(&mut fixture.remote, &offer.sdp);
    let offer = fixture
        .session
        .answer(&answer_sdp)
        .await
        .expect("staged publish should accept a later valid answer");
    assert!(offer.is_none());
    fixture.assert_committed(video, 1).await;
}

#[tokio::test]
async fn queued_publish_after_initial_offer_creates_a_follow_up_offer() {
    let mut fixture = build_publisher_fixture(55_229).await;
    let initial_offer = fixture
        .session
        .establish()
        .await
        .expect("initial offer should be created")
        .expect("initial offer should be present");
    let video = TestSourceKind::ScalableVideo;
    assert!(
        fixture
            .session
            .publish(source_publish_intent_for_source(video))
            .await
            .expect("publish should queue while the initial offer is pending")
            .is_none()
    );

    let initial_answer = remote_answer_sdp(&mut fixture.remote, &initial_offer.sdp);
    let follow_up_offer = fixture
        .session
        .answer(&initial_answer)
        .await
        .expect("initial answer should apply")
        .expect("queued publish should create a follow-up offer");
    let follow_up_answer = remote_answer_sdp(&mut fixture.remote, &follow_up_offer.sdp);
    assert!(
        fixture
            .session
            .answer(&follow_up_answer)
            .await
            .expect("follow-up answer should commit the queued publish")
            .is_none()
    );
    fixture.assert_committed(video, 1).await;
}

#[tokio::test]
async fn inactive_consumer_answer_releases_room_route_for_later_retry() {
    let ConsumerAnswerFixture {
        room,
        media_transport,
        publisher_user_id,
        subscriber_user_id,
        subscriber_session_key,
        source,
        source_media_id,
        declined_media_id,
        declined_mid,
        mut subscriber,
        mut subscriber_remote,
        mut subscriber_rx,
    } = Box::pin(build_consumer_answer_fixture()).await;
    let inspect = room.test_api();
    let transport = media_transport.test_api();
    assert_eq!(transport.source_relay_target_count(&source).await, 1);
    assert_eq!(router_consumer_dependency_count(&room).await, 1);

    let offer = subscriber
        .renegotiate()
        .await
        .expect("subscriber renegotiation should succeed")
        .expect("consumer setup should stage an offer");
    let answer = remote_answer_sdp(&mut subscriber_remote, &offer.sdp);
    let inactive_answer = inactive_answer_for_mid(&answer, declined_mid);
    drain_remote_track_snapshots(&mut subscriber_rx);
    let offer = subscriber
        .answer(&inactive_answer)
        .await
        .expect("inactive consumer answer should apply");
    assert!(offer.is_none(), "declined consumer must not be restaged");
    let snapshots = drain_remote_track_snapshots(&mut subscriber_rx);
    let [snapshot] = snapshots.as_slice() else {
        panic!("decline should emit one authoritative track snapshot");
    };
    assert!(!snapshot.requires_negotiation);
    assert!(snapshot.tracks.is_empty());
    assert_eq!(inspect.consumer_count().await, 0);
    let route_state = inspect
        .consumer_route_state(
            &subscriber_user_id,
            &publisher_user_id,
            &stream_id_for_source(TestSourceKind::ScalableVideo),
        )
        .await;
    assert_eq!(route_state, Some(ConsumerRouteState::Absent));
    let media_mid = media_transport
        .transport_media_mid(&subscriber_session_key, declined_media_id)
        .await;
    assert_eq!(media_mid, None);
    let tx_pair = transport
        .session_stream_tx_pair(&subscriber_session_key, declined_mid)
        .await;
    assert_eq!(tx_pair, None);
    assert_eq!(transport.source_relay_target_count(&source).await, 0);
    assert_eq!(router_consumer_dependency_count(&room).await, 0);

    assert!(
        room.test_api()
            .refresh_session(&subscriber_user_id, &media_transport)
            .await
    );
    let (replacement_media_id, replacement_mid) =
        consumer_destination_identity(&media_transport, source_media_id, &subscriber_user_id).await;
    assert_ne!(replacement_media_id, declined_media_id);
    assert_eq!(transport.source_relay_target_count(&source).await, 1);
    assert_eq!(router_consumer_dependency_count(&room).await, 1);
    let offer = subscriber
        .renegotiate()
        .await
        .expect("replacement renegotiation should succeed")
        .expect("later readiness should stage a replacement offer");
    let answer = remote_answer_sdp(&mut subscriber_remote, &offer.sdp);
    let offer = subscriber
        .answer(&answer)
        .await
        .expect("replacement consumer answer should apply");
    assert!(offer.is_none());
    assert_eq!(inspect.consumer_count().await, 1);
    let tx_pair = transport
        .session_stream_tx_pair(&subscriber_session_key, replacement_mid)
        .await;
    assert!(tx_pair.is_some());
}

#[tokio::test]
async fn queued_publish_cancellation_does_not_create_a_follow_up_offer() {
    let mut fixture = build_publisher_fixture(55_226).await;
    establish_session(&mut fixture.session, &mut fixture.remote).await;
    let video = TestSourceKind::ScalableVideo;
    let audio = TestSourceKind::AudioDetector;
    let offer = fixture
        .session
        .publish(source_publish_intent_for_source(video))
        .await
        .expect("first publish should stage")
        .expect("first publish should create an offer");

    assert!(
        fixture
            .session
            .publish(source_publish_intent_for_source(audio))
            .await
            .expect("second publish should queue")
            .is_none()
    );
    assert!(
        fixture
            .session
            .publish(source_publish_intent_for_source(audio))
            .await
            .expect("queued publish should be a no-op")
            .is_none()
    );
    fixture
        .session
        .deactivate_publication(SourceDeactivateIntent::new(stream_id_for_source(audio)))
        .await;

    let answer_sdp = remote_answer_sdp(&mut fixture.remote, &offer.sdp);
    assert!(
        fixture
            .session
            .answer(&answer_sdp)
            .await
            .expect("first publish answer should apply")
            .is_none()
    );
    fixture.assert_committed(video, 1).await;
    fixture.assert_staged(audio, false).await;
}

#[tokio::test]
async fn staged_publish_cancellation_creates_one_cleanup_offer() {
    let mut fixture = build_publisher_fixture(55_227).await;
    establish_session(&mut fixture.session, &mut fixture.remote).await;
    let video = TestSourceKind::ScalableVideo;
    let offer = fixture
        .session
        .publish(source_publish_intent_for_source(video))
        .await
        .expect("publish should stage")
        .expect("staged publish should create an offer");
    assert!(
        fixture
            .session
            .publish(source_publish_intent_for_source(video))
            .await
            .expect("staged publish should be a no-op")
            .is_none()
    );

    fixture
        .session
        .deactivate_publication(SourceDeactivateIntent::new(stream_id_for_source(video)))
        .await;
    fixture.assert_staged(video, false).await;

    let answer_sdp = remote_answer_sdp(&mut fixture.remote, &offer.sdp);
    let cleanup = fixture
        .session
        .answer(&answer_sdp)
        .await
        .expect("obsolete publish offer should still accept its answer")
        .expect("staged cancellation should create one cleanup offer");
    let answer_sdp = remote_answer_sdp(&mut fixture.remote, &cleanup.sdp);
    assert!(
        fixture
            .session
            .answer(&answer_sdp)
            .await
            .expect("cleanup answer should apply")
            .is_none()
    );
    fixture.assert_staged(video, false).await;
    assert_eq!(fixture.room.test_api().producer_count().await, 0);
}

#[tokio::test]
async fn committed_publication_pauses_and_resumes_without_an_offer() {
    let mut fixture = build_publisher_fixture(55_228).await;
    establish_session(&mut fixture.session, &mut fixture.remote).await;
    let video = TestSourceKind::ScalableVideo;
    publish_source(&mut fixture, video).await;
    let inspect = fixture.room.test_api();
    let source_id = inspect
        .source_id_for_owner_stream(&fixture.user_id, video)
        .await
        .expect("committed publication should expose a source id");
    assert!(
        fixture
            .session
            .publish(source_publish_intent_for_source(video))
            .await
            .expect("active publication should be a no-op")
            .is_none()
    );

    fixture
        .session
        .deactivate_publication(SourceDeactivateIntent::new(stream_id_for_source(video)))
        .await;
    assert_eq!(
        inspect
            .source_id_for_owner_stream(&fixture.user_id, video)
            .await,
        Some(source_id)
    );

    assert!(
        fixture
            .session
            .publish(source_publish_intent_for_source(video))
            .await
            .expect("committed publication should resume")
            .is_none()
    );
    assert_eq!(
        inspect
            .source_id_for_owner_stream(&fixture.user_id, video)
            .await,
        Some(source_id)
    );
}

#[tokio::test]
async fn media_session_close_rolls_back_staged_publish_and_removes_room_session() {
    let mut fixture = build_publisher_fixture(55_206).await;
    establish_session(&mut fixture.session, &mut fixture.remote).await;
    stage_scalable_video(&mut fixture.session).await;
    let video = TestSourceKind::ScalableVideo;
    fixture.assert_staged(video, true).await;

    assert!(fixture.session.close().await);

    fixture.assert_staged(video, false).await;
    assert!(!fixture.room.test_api().has_session(&fixture.user_id).await);
}

#[tokio::test]
async fn media_session_close_is_idempotent_after_completed_cleanup() {
    let PublisherFixture {
        room,
        user_id,
        mut session,
        mut remote,
        ..
    } = build_publisher_fixture(55_207).await;
    establish_session(&mut session, &mut remote).await;

    assert!(session.close().await);
    assert!(!session.close().await);

    assert!(!room.test_api().has_session(&user_id).await);
}

#[tokio::test]
async fn replacement_drains_staged_publish_before_stale_close() {
    let mut fixture = build_publisher_fixture(55_208).await;
    establish_session(&mut fixture.session, &mut fixture.remote).await;
    stage_scalable_video(&mut fixture.session).await;
    let video = TestSourceKind::ScalableVideo;
    let staged_media_id = fixture
        .room
        .staged_media_id(&fixture.user_id, fixture.session.connection_id(), video)
        .await
        .expect("test publish should be staged");
    let replacement = fixture
        .core
        .admit_user(fixture.room.uuid(), join_request(fixture.user_id.clone()))
        .await
        .expect("replacement user should join through core facade");

    fixture.assert_staged(video, false).await;
    assert!(
        fixture
            .media_transport
            .test_api()
            .route_entry_by_media_id(staged_media_id)
            .await
            .is_none()
    );

    assert!(!fixture.session.close().await);

    assert_eq!(
        fixture
            .room
            .test_api()
            .user_connection_id(&fixture.user_id)
            .await,
        Some(replacement.connection_id())
    );
}

async fn build_publisher_session(port: u16) -> (MediaSession, Rtc) {
    let fixture = build_publisher_fixture(port).await;
    (fixture.session, fixture.remote)
}

struct PublisherFixture {
    core: SfuCore,
    room: Arc<Room>,
    media_transport: MediaTransport,
    user_id: UserId,
    session: MediaSession,
    remote: Rtc,
}

struct ConsumerAnswerFixture {
    room: Arc<Room>,
    media_transport: MediaTransport,
    publisher_user_id: UserId,
    subscriber_user_id: UserId,
    subscriber_session_key: TransportSessionKey,
    source: TransportSourceKey,
    source_media_id: TransportMediaId,
    declined_media_id: TransportMediaId,
    declined_mid: Mid,
    subscriber: MediaSession,
    subscriber_remote: Rtc,
    subscriber_rx: UserOutboundReceiver,
}

impl PublisherFixture {
    async fn assert_staged(&self, source: TestSourceKind, expected: bool) {
        assert_eq!(
            self.room
                .has_staged_publish(
                    &self.user_id,
                    self.session.connection_id(),
                    &stream_id_for_source(source),
                )
                .await,
            expected
        );
    }

    async fn assert_committed(&self, source: TestSourceKind, producer_count: usize) {
        let stream_id = stream_id_for_source(source);
        let inspect = self.room.test_api();
        assert!(inspect.is_stream_published(&self.user_id, &stream_id).await);
        assert_eq!(inspect.producer_count().await, producer_count);
        self.assert_staged(source, false).await;
    }
}

async fn build_publisher_fixture(port: u16) -> PublisherFixture {
    let manager = Arc::new(RoomManager::for_test());
    build_publisher_fixture_with(
        port,
        manager,
        build_real_rtc_media_transport(),
        "issuer-sfu-core",
    )
    .await
}

async fn build_publisher_fixture_with(
    port: u16,
    manager: Arc<RoomManager>,
    media_transport: MediaTransport,
    issuer: &str,
) -> PublisherFixture {
    let room = manager
        .serve_room(issuer, TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await
        .expect("test room should be served");
    let core = SfuCore::new(media_transport.clone(), Arc::clone(&manager));
    let publisher_user_id = UserId::Integer(1);
    let session = core
        .admit_user(room.uuid(), join_request(publisher_user_id.clone()))
        .await
        .expect("publisher should join through core facade");
    PublisherFixture {
        core,
        room,
        media_transport,
        user_id: publisher_user_id,
        session,
        remote: build_remote_rtc(port),
    }
}

async fn build_consumer_answer_fixture() -> ConsumerAnswerFixture {
    let manager = Arc::new(RoomManager::for_test_with_runtime_policy(
        RoomRuntimePolicy::new(
            RoomAdmissionPolicy::new(100),
            RuntimeFeatureFlags::default(),
            test_client_rtp_capabilities(),
        )
        .with_room_worker_policy(RoomWorkerPolicy::new(
            NonZeroUsize::new(2).expect("test router cap should be positive"),
            NonZeroU64::new(RoomWorkerPolicy::DEFAULT_PACKET_LOOP_DELAY_THRESHOLD_MS)
                .expect("default delay threshold should be positive"),
        )),
    ));
    let media_transport = real_adapter();
    media_transport
        .test_api()
        .set_packet_loop_delays_ms(vec![Some(0); 4]);
    let mut publisher = build_publisher_fixture_with(
        55_230,
        manager,
        media_transport,
        "issuer-sfu-core-consumer-answer",
    )
    .await;
    establish_session(&mut publisher.session, &mut publisher.remote).await;
    let publisher_session_key = publisher
        .room
        .transport_user_key(&publisher.user_id, publisher.session.connection_id())
        .await;
    let mut delays = vec![Some(0); 4];
    delays[publisher_session_key.media_worker_id().as_usize()] =
        Some(RoomWorkerPolicy::DEFAULT_PACKET_LOOP_DELAY_THRESHOLD_MS);
    publisher
        .media_transport
        .test_api()
        .set_packet_loop_delays_ms(delays);

    let subscriber_user_id = UserId::Integer(2);
    let (subscriber_sender, subscriber_rx) = test_sender();
    let mut subscriber = publisher
        .core
        .admit_user(
            publisher.room.uuid(),
            JoinUserRequest {
                user_id: subscriber_user_id.clone(),
                label: None,
                permissions: UserPermissions::default(),
                sender: subscriber_sender,
            },
        )
        .await
        .expect("subscriber should join through core facade");
    let mut subscriber_remote = build_remote_rtc(55_231);
    establish_session(&mut subscriber, &mut subscriber_remote).await;
    let subscriber_session_key = publisher
        .room
        .transport_user_key(&subscriber_user_id, subscriber.connection_id())
        .await;
    assert_ne!(
        publisher_session_key.media_worker_id(),
        subscriber_session_key.media_worker_id()
    );
    assert_eq!(router_consumer_dependency_count(&publisher.room).await, 0);
    publish_source(&mut publisher, TestSourceKind::ScalableVideo).await;
    let source_media_id = publisher
        .room
        .test_api()
        .first_published_transport_media_id()
        .await
        .expect("published source should expose its transport media id");
    let source = TransportSourceKey::new(publisher_session_key.clone(), source_media_id);
    let (declined_media_id, declined_mid) = consumer_destination_identity(
        &publisher.media_transport,
        source_media_id,
        &subscriber_user_id,
    )
    .await;
    let PublisherFixture {
        room,
        media_transport,
        user_id: publisher_user_id,
        ..
    } = publisher;

    ConsumerAnswerFixture {
        room,
        media_transport,
        publisher_user_id,
        subscriber_user_id,
        subscriber_session_key,
        source,
        source_media_id,
        declined_media_id,
        declined_mid,
        subscriber,
        subscriber_remote,
        subscriber_rx,
    }
}

fn join_request(user_id: UserId) -> JoinUserRequest {
    let (sender, _receiver) = test_sender();
    JoinUserRequest {
        user_id,
        label: None,
        permissions: UserPermissions::default(),
        sender,
    }
}

async fn establish_session(session: &mut MediaSession, remote: &mut Rtc) {
    let offer = session
        .establish()
        .await
        .expect("initial offer should be created")
        .expect("initial offer should be present");
    let answer_sdp = remote_answer_sdp(remote, &offer.sdp);
    let offer = session
        .answer(&answer_sdp)
        .await
        .expect("initial answer should apply");
    assert!(offer.is_none());
}

async fn stage_scalable_video(session: &mut MediaSession) {
    let offer = session
        .publish(source_publish_intent_for_source(
            TestSourceKind::ScalableVideo,
        ))
        .await
        .expect("publish should stage through the media session");
    expect_renegotiation_offer(offer);
}

async fn publish_source(fixture: &mut PublisherFixture, source: TestSourceKind) {
    let offer = fixture
        .session
        .publish(source_publish_intent_for_source(source))
        .await
        .expect("publish should stage")
        .expect("publish should create an offer");
    let answer_sdp = remote_answer_sdp(&mut fixture.remote, &offer.sdp);
    assert!(
        fixture
            .session
            .answer(&answer_sdp)
            .await
            .expect("publish answer should apply")
            .is_none()
    );
}

async fn router_consumer_dependency_count(room: &Room) -> usize {
    room.state
        .read()
        .await
        .topology
        .router()
        .consumer_dependency_count()
}

fn inactive_answer_for_mid(answer: &str, mid: Mid) -> String {
    let marker = format!("{}{}{}{mid}", sdp::ATTR, sdp::attribute::MID, sdp::ATTR_SEP);
    let media_boundary = format!("{}{}", sdp::CRLF, sdp::MEDIA);
    let marker_start = answer
        .find(&marker)
        .expect("consumer answer should contain its MID");
    let section_start = answer[..marker_start]
        .rfind(&media_boundary)
        .map_or(0, |index| index + sdp::CRLF.len());
    let section_end = answer[marker_start..]
        .find(&media_boundary)
        .map_or(answer.len(), |offset| {
            marker_start + offset + sdp::CRLF.len()
        });
    let section = &answer[section_start..section_end];
    let recv_only = format!("{}{}", sdp::ATTR, sdp::direction::RECV_ONLY);
    let inactive = format!("{}{}", sdp::ATTR, sdp::direction::INACTIVE);
    assert!(section.contains(&recv_only));
    answer.replacen(section, &section.replacen(&recv_only, &inactive, 1), 1)
}

async fn assert_invalid_answer_is_rejected(session: &mut MediaSession) {
    let error = session
        .answer("not an SDP answer")
        .await
        .expect_err("invalid answer should be rejected");
    assert!(error.is_client_error());
}

fn expect_renegotiation_offer(offer: Option<NegotiationOffer>) -> NegotiationOffer {
    let Some(offer) = offer else {
        panic!("media session should return one renegotiation offer");
    };
    offer
}
