use std::{cell::Cell, time::Duration};
pub(super) use std::{sync::Arc, time::Instant};

use o_sfu_router::test_support::rtp_samples::{
    sample_audio_rtp_parameters, sample_client_rtp_capabilities,
    sample_simulcast_video_rtp_parameters, sample_three_layer_simulcast_video_rtp_parameters,
    sample_video_rtp_parameters,
};
pub(super) use o_sfu_router::{
    MediaKind,
    rtp::{MediaCapabilities, MediaStream},
};

pub(super) use super::super::{
    JoinUserRequest, Room, RoomAdmissionPolicy, RoomConfig, RoomEffectContext, RoomEventMessage,
    RoomJoinError, RoomManager, UserCloseReason, UserOutbound, UserOutboundReceiver,
    UserOutboundSender, source_policy::SourcePolicyTurn, transition::PublishStageOutcome,
};
pub(super) use crate::{
    RoomMediaLimits, VideoAdaptationTuning,
    engine::{
        ConnectionId, TestSourceKind, UserId, UserInfo, UserPermissions, VideoLayoutIntent,
        media_transport::{
            AppliedSessionAnswer, MediaTransport, ReceiverBandwidthSnapshot,
            TransportBitrateSnapshot, TransportMediaId, TransportSessionHealth,
            test_support::{
                test_media_transport_config, test_media_transport_deps, test_rtc_port_range,
            },
        },
        metrics::{RuntimeMetrics, test_support::RuntimeMetricsSnapshotTestExt},
        source_model::{
            UserStreamId,
            test_support::{
                TestSubscriptionStates, source_publish_intent_for_source, stream_id_for_source,
                subscription_intents_from_test_states,
            },
        },
    },
};

pub(super) async fn refresh_source_policy(room: &Room, adapter: &MediaTransport) {
    SourcePolicyTurn::packet_selection()
        .execute(room, Some(adapter), None)
        .await;
}

pub(super) const TEST_ROOM_KEY: &str = "Y2hhbm5lbC1rZXk=";
const DEFAULT_ACTIVE_SPEAKER_AUDIO_LEVEL_DBOV: i8 = -20;

pub(super) fn test_client_rtp_capabilities() -> MediaCapabilities {
    sample_client_rtp_capabilities()
}

pub(super) fn test_audio_rtp_parameters() -> MediaStream {
    sample_audio_rtp_parameters(11_111)
}

pub(super) fn test_video_rtp_parameters() -> MediaStream {
    sample_video_rtp_parameters(None, 22_222)
}

pub(super) fn test_simulcast_video_rtp_parameters() -> MediaStream {
    sample_simulcast_video_rtp_parameters(None)
}

pub(super) fn test_sender() -> (UserOutboundSender, UserOutboundReceiver) {
    UserOutboundSender::channel(1024, Arc::new(RuntimeMetrics::default()))
}

pub(super) fn real_adapter() -> MediaTransport {
    real_adapter_with_metrics(Arc::new(RuntimeMetrics::default()))
}

fn real_adapter_with_metrics(metrics: Arc<RuntimeMetrics>) -> MediaTransport {
    let mut deps = test_media_transport_deps();
    deps.metrics = metrics;
    MediaTransport::build(test_media_transport_config(4, test_rtc_port_range()), deps)
        .unwrap_or_else(|error| {
            panic!("constant RTC room test transport config should be valid: {error}")
        })
}

pub(super) fn test_connection_id(raw: u64) -> ConnectionId {
    ConnectionId::from_raw(raw)
}

pub(super) async fn join_user_with_sender(
    room: &Arc<super::super::Room>,
    user_id: UserId,
    sender: UserOutboundSender,
) -> ConnectionId {
    room.test_api()
        .lifecycle()
        .join_user(user_id, None, UserPermissions::default(), sender)
        .await
        .expect("user should join")
}

pub(super) async fn join_user_without_transport_teardown(
    room: &Arc<super::super::Room>,
    adapter: &MediaTransport,
    user_id: UserId,
    sender: UserOutboundSender,
) -> ConnectionId {
    room.test_api()
        .lifecycle()
        .join_session_without_transport_teardown(
            user_id,
            None,
            UserPermissions::default(),
            sender,
            adapter,
        )
        .await
        .expect("user should join")
}

pub(super) async fn user_connection_id(
    room: &super::super::Room,
    user_id: &UserId,
) -> ConnectionId {
    room.test_api()
        .inspect()
        .user_connection_id(user_id)
        .await
        .expect("test fixture requires a live user connection")
}

pub(super) async fn make_session_ready_with_transport(
    room: &super::super::Room,
    user_id: &UserId,
    media_transport: &MediaTransport,
) {
    create_transport_session_offer(room, user_id, media_transport).await;
    assert!(
        room.test_api()
            .lifecycle()
            .mark_session_ready(user_id, test_client_rtp_capabilities(), media_transport,)
            .await
    );
}

pub(super) async fn create_transport_session_offer(
    room: &super::super::Room,
    user_id: &UserId,
    media_transport: &MediaTransport,
) {
    let connection_id = user_connection_id(room, user_id).await;
    let session_key = room.transport_user_key(user_id, connection_id).await;
    media_transport
        .create_initial_session_offer("test-room", &session_key)
        .await
        .expect("real RTC test user should create an initial offer");
}

pub(super) struct StagedPublishScenario {
    pub(super) room: Arc<super::super::Room>,
    pub(super) adapter: MediaTransport,
    pub(super) user_id: UserId,
    pub(super) connection_id: ConnectionId,
    publisher_rx: UserOutboundReceiver,
    subscriber_rx: UserOutboundReceiver,
}

impl StagedPublishScenario {
    fn operation(&self) -> super::super::RoomUserOperation<'_> {
        self.room
            .user_operation(&self.user_id, self.connection_id, &self.adapter)
    }

    pub(super) async fn new() -> Self {
        let (room, adapter, publisher_rx, subscriber_rx) = setup_two_ready_users().await;
        let user_id = UserId::Integer(1);
        let connection_id = user_connection_id(&room, &user_id).await;
        Self {
            room,
            adapter,
            user_id,
            connection_id,
            publisher_rx,
            subscriber_rx,
        }
    }

    pub(super) async fn stage_source(&self, stream_type: TestSourceKind) -> PublishStageOutcome {
        self.operation()
            .stage_negotiated_publish(&source_publish_intent_for_source(stream_type))
            .await
            .expect("stage publish should not hit transport failure")
    }

    pub(super) async fn stage_scalable_video(&self) -> PublishStageOutcome {
        self.stage_source(TestSourceKind::ScalableVideo).await
    }

    pub(super) async fn rollback_scalable_video(&self) -> bool {
        self.operation()
            .rollback_staged_publish(&stream_id_for_source(TestSourceKind::ScalableVideo))
            .await
    }

    pub(super) async fn commit(&self) {
        let applied_answer = AppliedSessionAnswer::from_negotiated_producers([(
            self.staged_media_id(TestSourceKind::ScalableVideo).await,
            test_simulcast_video_rtp_parameters(),
        )]);
        self.operation()
            .commit_staged_publishes(&applied_answer)
            .await;
    }

    pub(super) async fn close_user(&self) {
        self.room
            .remove_user(&self.user_id, self.connection_id, &self.adapter)
            .await;
    }

    pub(super) async fn staged_count(&self) -> usize {
        self.room
            .staged_count(&self.user_id, self.connection_id)
            .await
    }

    pub(super) async fn scalable_video_is_published(&self) -> bool {
        let stream_id = stream_id_for_source(TestSourceKind::ScalableVideo);
        self.room
            .test_api()
            .inspect()
            .is_stream_published(&self.user_id, &stream_id)
            .await
    }

    pub(super) async fn staged_media_id(&self, stream_type: TestSourceKind) -> TransportMediaId {
        self.room
            .staged_media_id(&self.user_id, self.connection_id, stream_type)
            .await
            .expect("staged publish should expose its transport media id")
    }

    pub(super) fn drain_publisher(&mut self) -> Vec<UserOutbound> {
        drain_outbound(&mut self.publisher_rx)
    }

    pub(super) fn drain_subscriber(&mut self) -> Vec<UserOutbound> {
        drain_outbound(&mut self.subscriber_rx)
    }

    pub(super) fn assert_no_outbound(&mut self) {
        assert!(self.drain_publisher().is_empty());
        assert!(self.drain_subscriber().is_empty());
    }

    pub(super) async fn route_for_staged_media_exists(
        &self,
        transport_media_id: TransportMediaId,
    ) -> bool {
        self.adapter
            .test_api()
            .route_entry_by_media_id(transport_media_id)
            .await
            .is_some()
    }
}

#[derive(Clone, Copy)]
struct ReadyRoomFixtureOptions {
    publish_camera_before_subscriber_ready: bool,
}

pub(super) struct ReadyRoomFixture {
    room: Arc<super::super::Room>,
    adapter: MediaTransport,
    first_rx: UserOutboundReceiver,
    second_rx: UserOutboundReceiver,
}

impl ReadyRoomFixtureOptions {
    const fn two_ready_users() -> Self {
        Self {
            publish_camera_before_subscriber_ready: false,
        }
    }

    const fn publisher_ready_before_subscriber() -> Self {
        Self {
            publish_camera_before_subscriber_ready: true,
        }
    }
}

async fn setup_ready_room_fixture(options: ReadyRoomFixtureOptions) -> ReadyRoomFixture {
    setup_ready_room_fixture_with_adapter(options, real_adapter()).await
}

async fn setup_ready_room_fixture_with_adapter(
    options: ReadyRoomFixtureOptions,
    adapter: MediaTransport,
) -> ReadyRoomFixture {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await
        .expect("test room should be served");
    let (first_tx, first_rx) = test_sender();
    let (second_tx, second_rx) = test_sender();
    join_user_with_sender(&room, UserId::Integer(1), first_tx).await;
    join_user_with_sender(&room, UserId::Integer(2), second_tx).await;

    make_session_ready_with_transport(&room, &UserId::Integer(1), &adapter).await;

    if options.publish_camera_before_subscriber_ready {
        try_publish_camera(&room, &UserId::Integer(1), &adapter).await;
        create_transport_session_offer(&room, &UserId::Integer(2), &adapter).await;
    }

    if !options.publish_camera_before_subscriber_ready {
        make_session_ready_with_transport(&room, &UserId::Integer(2), &adapter).await;
    }

    ReadyRoomFixture {
        room,
        adapter,
        first_rx,
        second_rx,
    }
}

pub(super) async fn setup_two_ready_users() -> (
    Arc<super::super::Room>,
    MediaTransport,
    UserOutboundReceiver,
    UserOutboundReceiver,
) {
    let fixture = setup_ready_room_fixture(ReadyRoomFixtureOptions::two_ready_users()).await;
    (
        fixture.room,
        fixture.adapter,
        fixture.first_rx,
        fixture.second_rx,
    )
}

pub(super) async fn setup_two_ready_users_with_media_metrics() -> (
    Arc<super::super::Room>,
    MediaTransport,
    Arc<RuntimeMetrics>,
    UserOutboundReceiver,
    UserOutboundReceiver,
) {
    let metrics = Arc::new(RuntimeMetrics::default());
    let adapter = real_adapter_with_metrics(Arc::clone(&metrics));
    let fixture =
        setup_ready_room_fixture_with_adapter(ReadyRoomFixtureOptions::two_ready_users(), adapter)
            .await;
    (
        fixture.room,
        fixture.adapter,
        metrics,
        fixture.first_rx,
        fixture.second_rx,
    )
}

pub(super) async fn setup_pending_consumer_readiness_scenario() -> (
    Arc<super::super::Room>,
    MediaTransport,
    UserOutboundReceiver,
    UserOutboundReceiver,
) {
    let fixture =
        setup_ready_room_fixture(ReadyRoomFixtureOptions::publisher_ready_before_subscriber())
            .await;
    (
        fixture.room,
        fixture.adapter,
        fixture.first_rx,
        fixture.second_rx,
    )
}

pub(super) async fn setup_ready_users_with_transport(
    user_ids: &[i64],
) -> (Arc<super::super::Room>, MediaTransport) {
    let (room, adapter, _receivers) = setup_ready_users_with_transport_receivers(user_ids).await;
    (room, adapter)
}

pub(super) async fn setup_ready_users_with_transport_and_media_limits(
    user_ids: &[i64],
    media_limits: RoomMediaLimits,
) -> (Arc<super::super::Room>, MediaTransport) {
    let manager = RoomManager::for_test_with_media_limits(media_limits);
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await
        .expect("test room should be served");
    let adapter = real_adapter();
    for &raw_user_id in user_ids {
        let (sender, _receiver) = test_sender();
        let user_id = UserId::Integer(raw_user_id);
        join_user_without_transport_teardown(&room, &adapter, user_id.clone(), sender).await;
        make_session_ready_with_transport(&room, &user_id, &adapter).await;
    }
    (room, adapter)
}

pub(super) async fn setup_ready_users_with_transport_and_tuning(
    user_ids: &[i64],
    tuning: VideoAdaptationTuning,
) -> (Arc<super::super::Room>, MediaTransport) {
    let manager = RoomManager::for_test_with_video_adaptation_tuning(tuning);
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await
        .expect("test room should be served");
    let adapter = real_adapter();
    for &raw_user_id in user_ids {
        let (sender, _receiver) = test_sender();
        let user_id = UserId::Integer(raw_user_id);
        join_user_without_transport_teardown(&room, &adapter, user_id.clone(), sender).await;
        make_session_ready_with_transport(&room, &user_id, &adapter).await;
    }
    (room, adapter)
}

pub(super) async fn setup_ready_users_with_transport_receivers(
    user_ids: &[i64],
) -> (
    Arc<super::super::Room>,
    MediaTransport,
    Vec<UserOutboundReceiver>,
) {
    let manager = RoomManager::for_test();
    let room = manager
        .serve_room("issuer-a", TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await
        .expect("test room should be served");
    let adapter = real_adapter();
    let mut receivers = Vec::with_capacity(user_ids.len());
    for &raw_user_id in user_ids {
        let (sender, receiver) = test_sender();
        receivers.push(receiver);
        let user_id = UserId::Integer(raw_user_id);
        join_user_without_transport_teardown(&room, &adapter, user_id.clone(), sender).await;
        make_session_ready_with_transport(&room, &user_id, &adapter).await;
    }
    (room, adapter, receivers)
}

pub(super) async fn setup_three_ready_users_with_transport()
-> (Arc<super::super::Room>, MediaTransport) {
    setup_ready_users_with_transport(&[1, 2, 3]).await
}

pub(super) struct SourcePolicyScenario {
    pub(super) policy_now: Cell<Instant>,
    pub(super) room: Arc<super::super::Room>,
    pub(super) adapter: MediaTransport,
}

impl SourcePolicyScenario {
    pub(super) async fn with_ready_users(user_ids: &[i64]) -> Self {
        let (room, adapter) = setup_ready_users_with_transport(user_ids).await;
        Self {
            room,
            adapter,
            policy_now: Cell::new(Instant::now()),
        }
    }

    pub(super) async fn with_ready_users_and_media_limits(
        user_ids: &[i64],
        media_limits: RoomMediaLimits,
    ) -> Self {
        let (room, adapter) =
            setup_ready_users_with_transport_and_media_limits(user_ids, media_limits).await;
        Self {
            room,
            adapter,
            policy_now: Cell::new(Instant::now()),
        }
    }

    pub(super) async fn with_ready_users_and_tuning(
        user_ids: &[i64],
        tuning: VideoAdaptationTuning,
    ) -> Self {
        let (room, adapter) = setup_ready_users_with_transport_and_tuning(user_ids, tuning).await;
        Self {
            room,
            adapter,
            policy_now: Cell::new(Instant::now()),
        }
    }

    pub(super) async fn three_ready_users() -> Self {
        Self::with_ready_users(&[1, 2, 3]).await
    }

    pub(super) async fn publish_audio_and_camera(&self, raw_user_id: i64) {
        publish_audio_and_camera(&self.room, &UserId::Integer(raw_user_id), &self.adapter).await;
    }

    pub(super) async fn publish_audio_and_camera_for_users(&self, user_ids: &[i64]) {
        for raw_user_id in user_ids {
            self.publish_audio_and_camera(*raw_user_id).await;
        }
    }

    pub(super) async fn audio_media_id(&self, raw_user_id: i64) -> TransportMediaId {
        source_media_id(
            &self.room,
            &UserId::Integer(raw_user_id),
            TestSourceKind::AudioDetector,
        )
        .await
    }

    pub(super) async fn mark_active_speaker(&self, transport_media_id: TransportMediaId) {
        self.mark_active_speakers([transport_media_id]).await;
    }

    pub(super) async fn mark_active_speakers(
        &self,
        transport_media_ids: impl IntoIterator<Item = TransportMediaId>,
    ) {
        let observed_at = Instant::now();
        for transport_media_id in transport_media_ids {
            self.adapter
                .test_api()
                .observe_audio_activity_with_level(
                    transport_media_id,
                    DEFAULT_ACTIVE_SPEAKER_AUDIO_LEVEL_DBOV,
                    observed_at,
                )
                .await;
        }
    }

    pub(super) async fn mark_active_speakers_with_levels(
        &self,
        speakers: impl IntoIterator<Item = (TransportMediaId, i8)>,
    ) {
        let observed_at = Instant::now();
        for (transport_media_id, audio_level_dbov) in speakers {
            self.adapter
                .test_api()
                .observe_audio_activity_with_level(
                    transport_media_id,
                    audio_level_dbov,
                    observed_at,
                )
                .await;
        }
    }

    pub(super) async fn refresh_policy(&self) {
        refresh_source_policy(&self.room, &self.adapter).await;
    }

    pub(super) async fn refresh_policy_until_upgrades_settle(&self) {
        let sources = self.adapter.active_speaker_source_snapshot().await;
        let now = self.policy_now.get().max(Instant::now());
        for elapsed in [Duration::ZERO, VideoAdaptationTuning::DEFAULT_UPGRADE_DWELL] {
            let tx = {
                let state = self.room.state.read().await;
                super::super::source_policy::SourcePolicyTransaction::plan(
                    &state,
                    &sources,
                    &ReceiverBandwidthSnapshot::default(),
                    &TransportBitrateSnapshot::default(),
                    now + elapsed,
                )
            };
            if let Some(tx) = tx {
                tx.execute(&self.room, &self.adapter).await;
            }
        }
        self.policy_now
            .set(now + VideoAdaptationTuning::DEFAULT_UPGRADE_DWELL);
    }

    pub(super) async fn set_deaf(&self, raw_user_id: i64, is_deaf: bool) {
        let user_id = UserId::Integer(raw_user_id);
        let connection_id = user_connection_id(&self.room, &user_id).await;
        self.set_deaf_for_connection(&user_id, connection_id, is_deaf)
            .await;
    }

    /// Sends a deafen presence update on an explicit connection.
    ///
    /// Tests use this to replay a presence update from a connection that is no
    /// longer current, which room state must reject.
    pub(super) async fn set_deaf_for_connection(
        &self,
        user_id: &UserId,
        connection_id: ConnectionId,
        is_deaf: bool,
    ) {
        self.room
            .update_user_info(
                user_id,
                connection_id,
                &self.adapter,
                UserInfo {
                    is_deaf: Some(is_deaf),
                    ..UserInfo::default()
                },
            )
            .await;
    }

    /// Replays a receiver audio subscribe intent, as a reconnecting client does.
    pub(super) async fn subscribe_audio(
        &self,
        receiver_user_id: i64,
        source_user_id: i64,
        active: bool,
    ) {
        self.subscribe(
            receiver_user_id,
            source_user_id,
            TestSubscriptionStates {
                audio_detector: Some(active),
                ..TestSubscriptionStates::default()
            },
        )
        .await;
    }

    pub(super) async fn subscribe_scalable_video(
        &self,
        receiver_user_id: i64,
        source_user_id: i64,
        active: bool,
    ) {
        self.subscribe(
            receiver_user_id,
            source_user_id,
            TestSubscriptionStates {
                scalable_video: Some(active),
                ..TestSubscriptionStates::default()
            },
        )
        .await;
    }

    async fn subscribe(
        &self,
        receiver_user_id: i64,
        source_user_id: i64,
        states: TestSubscriptionStates,
    ) {
        let receiver_user_id = UserId::Integer(receiver_user_id);
        let source_user_id = UserId::Integer(source_user_id);
        let intents = subscription_intents_from_test_states(&states);
        assert!(
            self.room
                .test_api()
                .media()
                .update_subscription(&receiver_user_id, &source_user_id, &intents, &self.adapter)
                .await
        );
    }

    pub(super) async fn set_scalable_video_layout(
        &self,
        receiver_user_id: i64,
        source_user_id: i64,
        layout: VideoLayoutIntent,
    ) {
        let receiver_user_id = UserId::Integer(receiver_user_id);
        let source_user_id = UserId::Integer(source_user_id);
        let intents = subscription_intents_from_test_states(&TestSubscriptionStates {
            scalable_video_layout: Some(layout),
            ..TestSubscriptionStates::default()
        });
        assert!(
            self.room
                .test_api()
                .media()
                .update_subscription(&receiver_user_id, &source_user_id, &intents, &self.adapter,)
                .await
        );
    }
}

pub(super) async fn try_publish_camera(
    room: &Arc<super::super::Room>,
    user_id: &UserId,
    media_transport: &MediaTransport,
) -> Option<UserStreamId> {
    room.test_api()
        .media()
        .publish_track(
            user_id,
            TestSourceKind::ScalableVideo,
            MediaKind::Video,
            test_video_rtp_parameters(),
            media_transport,
        )
        .await
}

pub(super) async fn publish_simulcast_camera(
    room: &Arc<super::super::Room>,
    user_id: &UserId,
    media_transport: &MediaTransport,
) -> UserStreamId {
    publish_track(
        room,
        user_id,
        TestSourceKind::ScalableVideo,
        MediaKind::Video,
        test_simulcast_video_rtp_parameters(),
        media_transport,
    )
    .await
}

pub(super) async fn publish_three_layer_camera(
    room: &Arc<super::super::Room>,
    user_id: &UserId,
    media_transport: &MediaTransport,
) -> UserStreamId {
    publish_track(
        room,
        user_id,
        TestSourceKind::ScalableVideo,
        MediaKind::Video,
        sample_three_layer_simulcast_video_rtp_parameters(None),
        media_transport,
    )
    .await
}

pub(super) async fn publish_audio_and_camera(
    room: &Arc<super::super::Room>,
    user_id: &UserId,
    media_transport: &MediaTransport,
) {
    publish_track(
        room,
        user_id,
        TestSourceKind::AudioDetector,
        MediaKind::Audio,
        test_audio_rtp_parameters(),
        media_transport,
    )
    .await;
    publish_simulcast_camera(room, user_id, media_transport).await;
}

pub(super) async fn publish_track(
    room: &Arc<super::super::Room>,
    user_id: &UserId,
    stream_type: TestSourceKind,
    media_kind: MediaKind,
    rtp_parameters: MediaStream,
    media_transport: &MediaTransport,
) -> UserStreamId {
    room.test_api()
        .media()
        .publish_track(
            user_id,
            stream_type,
            media_kind,
            rtp_parameters,
            media_transport,
        )
        .await
        .expect("publication should succeed")
}

pub(super) async fn source_media_id(
    room: &Arc<super::super::Room>,
    user_id: &UserId,
    stream_type: TestSourceKind,
) -> TransportMediaId {
    let Some(connection_id) = room.test_api().inspect().user_connection_id(user_id).await else {
        panic!("user should exist");
    };
    let Some(transport_media_id) = room
        .test_api()
        .inspect()
        .producer_transport_media_id(user_id, connection_id, stream_type)
        .await
    else {
        panic!("{stream_type:?} producer should expose a transport media id");
    };
    transport_media_id
}

pub(super) fn drain_outbound(rx: &mut UserOutboundReceiver) -> Vec<UserOutbound> {
    let mut msgs = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        msgs.push(msg);
    }
    msgs
}
