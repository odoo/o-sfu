#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::too_many_lines,
    reason = "this benchmark-owned module is a readable fixed scenario rather than a reusable public API"
)]

use std::{
    cmp::Ordering,
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr},
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Result, anyhow};
use o_sfu_core::{
    prelude::{
        Bitrate, CodecPreferences, MediaCodecFlags, MediaSession, RoomMediaLimits,
        RoomWorkerPolicy, RtcUdpIoBackend, RuntimeFeatureFlags, SessionBitrateLimits, SfuCore,
        SourceSubscriptionIntent, UserStreamId, VideoBitrateLimits,
    },
    server::{
        metrics::RuntimeMetrics,
        packet_sinks::RoomPacketSinkRegistry,
        room::{
            JoinUserRequest, Room, RoomAdmissionPolicy, RoomConfig, RoomManager, RoomRuntimePolicy,
            UserOutboundReceiver, UserOutboundSender,
            test_support::{
                TestSourceKind, TestSubscriptionStates, stream_id_for_source,
                subscription_intents_from_test_states,
            },
        },
        session::{UserId, UserPermissions, VideoLayoutIntent},
        transport::{
            MediaTransport, MediaTransportConfig, MediaTransportDeps,
            SourcePolicyUpdateSubscription, TransportMediaId, test_support::test_rtc_port_range,
        },
    },
};
use o_sfu_router::test_support::rtp_samples;
use tokio::runtime::{Builder, Runtime};

const TEST_ROOM_KEY: &str = "Y2hhbm5lbC1rZXk=";
const WORKER_COUNT: usize = 4;
const OUTBOUND_QUEUE_CAPACITY: usize = 1024;
const MEDIA_TICK_MS: u64 = 20;
const POLICY_REFRESH_TICKS: usize = 25;
const SPEAKER_PATTERN: [RawUserId; 8] = [1, 2, 1, 3, 4, 2, 3, 4];

type RawUserId = i64;

#[derive(Debug, Default, Clone, Copy)]
pub struct GeneralCallStats {
    joins: usize,
    leaves: usize,
    publications: usize,
    unpublications: usize,
    subscription_updates: usize,
    audio_observations: usize,
    policy_refreshes: usize,
    route_inspections: usize,
    outbound_events: usize,
    producer_count: usize,
    consumer_count: usize,
    router_count: usize,
    worker_assignments: usize,
}

impl GeneralCallStats {
    pub fn total_work(self) -> usize {
        [
            self.joins,
            self.leaves,
            self.publications,
            self.unpublications,
            self.subscription_updates,
            self.audio_observations,
            self.policy_refreshes,
            self.route_inspections,
            self.outbound_events,
            self.producer_count,
            self.consumer_count,
            self.router_count,
            self.worker_assignments,
        ]
        .into_iter()
        .fold(0, usize::saturating_add)
    }
}

pub struct GeneralCallFixture {
    runtime: Runtime,
    scenario: GeneralCallScenario,
}

impl Default for GeneralCallFixture {
    fn default() -> Self {
        Self::new()
    }
}

impl GeneralCallFixture {
    #[expect(
        clippy::panic,
        reason = "benchmark setup must fail loudly when deterministic core-room setup is invalid"
    )]
    pub fn new() -> Self {
        let runtime = build_runtime();
        let scenario = match runtime.block_on(GeneralCallScenario::new()) {
            Ok(scenario) => scenario,
            Err(error) => panic!("failed to build general call benchmark scenario: {error}"),
        };
        Self { runtime, scenario }
    }

    #[expect(
        clippy::panic,
        reason = "benchmark execution must fail loudly when the fixed room flow stops being valid"
    )]
    pub fn run_total_work(self) -> usize {
        let Self { runtime, scenario } = self;
        match runtime.block_on(scenario.run()) {
            Ok(stats) => stats.total_work(),
            Err(error) => panic!("general call benchmark failed: {error}"),
        }
    }
}

#[expect(
    clippy::panic,
    reason = "benchmark setup must fail loudly when the current-thread runtime cannot boot"
)]
fn build_runtime() -> Runtime {
    match Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => panic!("failed to build current-thread benchmark runtime: {error}"),
    }
}

#[derive(Debug, Clone, Copy)]
struct VideoSubscription {
    receiver: RawUserId,
    publisher: RawUserId,
    enabled: bool,
    layout: VideoLayoutIntent,
}

impl VideoSubscription {
    const fn enabled(receiver: RawUserId, publisher: RawUserId, layout: VideoLayoutIntent) -> Self {
        Self {
            receiver,
            publisher,
            enabled: true,
            layout,
        }
    }

    const fn hidden(receiver: RawUserId, publisher: RawUserId) -> Self {
        Self {
            receiver,
            publisher,
            enabled: false,
            layout: VideoLayoutIntent::Hidden,
        }
    }
}

#[derive(Debug, Default)]
struct PublishedMedia {
    audio: BTreeMap<RawUserId, TransportMediaId>,
    camera: BTreeMap<RawUserId, TransportMediaId>,
}

struct GeneralCallScenario {
    core: SfuCore,
    manager: Arc<RoomManager>,
    media: PublishedMedia,
    media_transport: MediaTransport,
    outbound_metrics: Arc<RuntimeMetrics>,
    receivers: BTreeMap<RawUserId, UserOutboundReceiver>,
    room: Arc<Room>,
    source_policy_updates: SourcePolicyUpdateSubscription,
    stats: GeneralCallStats,
    synthetic_now: Instant,
    user_sessions: BTreeMap<RawUserId, MediaSession>,
}

impl GeneralCallScenario {
    async fn new() -> Result<Self> {
        let media_transport = media_transport()?;
        let source_policy_updates = media_transport.source_policy_subscription();
        let manager = room_manager(&media_transport)?;
        let room = manager
            .serve_room(
                "general-call-benchmark",
                TEST_ROOM_KEY.into(),
                &RoomConfig::default(),
                Some("general-call-benchmark"),
            )
            .await?;
        let core = SfuCore::new(media_transport.clone(), Arc::clone(&manager));
        Ok(Self {
            core,
            manager,
            media: PublishedMedia::default(),
            media_transport,
            outbound_metrics: Arc::new(RuntimeMetrics::default()),
            receivers: BTreeMap::new(),
            room,
            source_policy_updates,
            stats: GeneralCallStats::default(),
            synthetic_now: Instant::now(),
            user_sessions: BTreeMap::new(),
        })
    }

    async fn run(mut self) -> Result<GeneralCallStats> {
        self.join_ready_batch(&[1, 2, 3, 4, 5, 6, 7, 8]).await?;
        self.publish_audio_batch(&[(1, 11_101), (2, 11_102), (3, 11_103), (4, 11_104)])
            .await?;
        self.publish_camera_batch(&[1, 2, 3]).await?;
        self.subscribe_all_live_users_to_audio().await?;
        self.apply_video_subscriptions(&[
            VideoSubscription::enabled(5, 1, VideoLayoutIntent::Featured),
            VideoSubscription::enabled(5, 2, VideoLayoutIntent::VisibleThumbnail),
            VideoSubscription::enabled(5, 3, VideoLayoutIntent::VisibleThumbnail),
            VideoSubscription::enabled(6, 1, VideoLayoutIntent::Featured),
            VideoSubscription::enabled(6, 2, VideoLayoutIntent::VisibleThumbnail),
            VideoSubscription::enabled(7, 3, VideoLayoutIntent::VisibleThumbnail),
            VideoSubscription::enabled(8, 1, VideoLayoutIntent::VisibleThumbnail),
            VideoSubscription::hidden(8, 2),
        ])
        .await?;
        self.run_media_time(Duration::from_secs(2)).await?;

        self.join_ready_batch(&[9, 10]).await?;
        self.publish_camera_batch(&[4]).await?;
        self.subscribe_all_live_users_to_audio().await?;
        self.apply_video_subscriptions(&[
            VideoSubscription::enabled(5, 3, VideoLayoutIntent::Featured),
            VideoSubscription::enabled(5, 1, VideoLayoutIntent::VisibleThumbnail),
            VideoSubscription::hidden(5, 2),
            VideoSubscription::enabled(5, 4, VideoLayoutIntent::VisibleThumbnail),
            VideoSubscription::enabled(6, 3, VideoLayoutIntent::Featured),
            VideoSubscription::enabled(6, 4, VideoLayoutIntent::VisibleThumbnail),
            VideoSubscription::enabled(9, 1, VideoLayoutIntent::Featured),
            VideoSubscription::enabled(9, 3, VideoLayoutIntent::VisibleThumbnail),
            VideoSubscription::enabled(10, 4, VideoLayoutIntent::Featured),
            VideoSubscription::hidden(10, 2),
        ])
        .await?;
        self.run_media_time(Duration::from_secs(3)).await?;

        self.deactivate_camera(2).await?;
        self.close_user(7).await?;
        self.join_ready_batch(&[11]).await?;
        self.subscribe_all_live_users_to_audio().await?;
        self.apply_video_subscriptions(&[
            VideoSubscription::enabled(5, 4, VideoLayoutIntent::Featured),
            VideoSubscription::enabled(5, 1, VideoLayoutIntent::VisibleThumbnail),
            VideoSubscription::enabled(6, 3, VideoLayoutIntent::Featured),
            VideoSubscription::enabled(9, 4, VideoLayoutIntent::VisibleThumbnail),
            VideoSubscription::enabled(10, 1, VideoLayoutIntent::VisibleThumbnail),
            VideoSubscription::enabled(11, 3, VideoLayoutIntent::Featured),
            VideoSubscription::enabled(11, 4, VideoLayoutIntent::VisibleThumbnail),
        ])
        .await?;
        self.run_media_time(Duration::from_secs(2)).await?;

        self.close_user(8).await?;
        self.join_ready_batch(&[12]).await?;
        self.subscribe_all_live_users_to_audio().await?;
        self.apply_video_subscriptions(&[
            VideoSubscription::enabled(5, 1, VideoLayoutIntent::Featured),
            VideoSubscription::enabled(5, 4, VideoLayoutIntent::VisibleThumbnail),
            VideoSubscription::enabled(6, 4, VideoLayoutIntent::Featured),
            VideoSubscription::enabled(9, 3, VideoLayoutIntent::Featured),
            VideoSubscription::enabled(10, 1, VideoLayoutIntent::VisibleThumbnail),
            VideoSubscription::enabled(11, 4, VideoLayoutIntent::VisibleThumbnail),
            VideoSubscription::enabled(12, 1, VideoLayoutIntent::Featured),
            VideoSubscription::enabled(12, 3, VideoLayoutIntent::VisibleThumbnail),
            VideoSubscription::hidden(12, 4),
        ])
        .await?;
        self.run_media_time(Duration::from_secs(3)).await?;

        self.refresh_source_policy().await;
        self.inspect_route_state().await;
        self.drain_outbound_events();
        Ok(self.stats)
    }

    async fn join_ready_batch(&mut self, raw_user_ids: &[RawUserId]) -> Result<()> {
        for raw_user_id in raw_user_ids.iter().copied() {
            self.join_ready(raw_user_id).await?;
        }
        Ok(())
    }

    async fn join_ready(&mut self, raw_user_id: RawUserId) -> Result<()> {
        let target_worker =
            usize::try_from(raw_user_id.saturating_sub(1) / 3)?.min(WORKER_COUNT.saturating_sub(1));
        self.media_transport.test_api().set_packet_loop_delays_ms(
            (0..WORKER_COUNT)
                .map(|worker| match worker.cmp(&target_worker) {
                    Ordering::Less => {
                        Some(RoomWorkerPolicy::DEFAULT_PACKET_LOOP_DELAY_THRESHOLD_MS)
                    }
                    Ordering::Equal => Some(0),
                    Ordering::Greater => None,
                })
                .collect(),
        );
        let (sender, receiver) = UserOutboundSender::channel(
            OUTBOUND_QUEUE_CAPACITY,
            Arc::clone(&self.outbound_metrics),
        );
        let session = self
            .core
            .admit_user(
                self.room.uuid(),
                JoinUserRequest {
                    user_id: user(raw_user_id),
                    label: Some(format!("user-{raw_user_id}")),
                    permissions: UserPermissions::default(),
                    sender,
                },
            )
            .await
            .map_err(|error| anyhow!("user {raw_user_id} join failed: {error:?}"))?;
        self.room
            .test_api()
            .make_session_ready(session.user_id(), &self.media_transport)
            .await
            .map_err(|error| anyhow!("user {raw_user_id} readiness failed: {error:?}"))?;
        if self.user_sessions.insert(raw_user_id, session).is_some() {
            return Err(anyhow!("user {raw_user_id} session already exists"));
        }
        if self.receivers.insert(raw_user_id, receiver).is_some() {
            return Err(anyhow!("user {raw_user_id} receiver already exists"));
        }
        self.stats.joins = self.stats.joins.saturating_add(1);
        self.drain_outbound_events();
        Ok(())
    }

    async fn publish_audio_batch(&mut self, publishers: &[(RawUserId, u32)]) -> Result<()> {
        for (raw_user_id, ssrc) in publishers.iter().copied() {
            self.publish_audio(raw_user_id, ssrc).await?;
        }
        Ok(())
    }

    async fn publish_audio(&mut self, raw_user_id: RawUserId, ssrc: u32) -> Result<()> {
        let user_id = user(raw_user_id);
        let published_stream = self
            .room
            .test_api()
            .publish_track(
                &user_id,
                TestSourceKind::AudioDetector,
                rtp_samples::sample_audio_rtp_parameters(ssrc),
                &self.media_transport,
            )
            .await
            .ok_or_else(|| anyhow!("user {raw_user_id} audio publish failed"))?;
        if published_stream != stream_id_for_source(TestSourceKind::AudioDetector) {
            return Err(anyhow!(
                "user {raw_user_id} audio publish returned wrong stream id"
            ));
        }
        let media_id = self
            .producer_media_id(raw_user_id, TestSourceKind::AudioDetector)
            .await?;
        if self.media.audio.insert(raw_user_id, media_id).is_some() {
            return Err(anyhow!("user {raw_user_id} audio media already exists"));
        }
        self.stats.publications = self.stats.publications.saturating_add(1);
        Ok(())
    }

    async fn publish_camera_batch(&mut self, raw_user_ids: &[RawUserId]) -> Result<()> {
        for raw_user_id in raw_user_ids.iter().copied() {
            self.publish_camera(raw_user_id).await?;
        }
        Ok(())
    }

    async fn publish_camera(&mut self, raw_user_id: RawUserId) -> Result<()> {
        let user_id = user(raw_user_id);
        let published_stream = self
            .room
            .test_api()
            .publish_track(
                &user_id,
                TestSourceKind::ScalableVideo,
                rtp_samples::sample_simulcast_video_rtp_parameters(Some(video_mid(raw_user_id)?)),
                &self.media_transport,
            )
            .await
            .ok_or_else(|| anyhow!("user {raw_user_id} camera publish failed"))?;
        if published_stream != stream_id_for_source(TestSourceKind::ScalableVideo) {
            return Err(anyhow!(
                "user {raw_user_id} camera publish returned wrong stream id"
            ));
        }
        let media_id = self
            .producer_media_id(raw_user_id, TestSourceKind::ScalableVideo)
            .await?;
        if self.media.camera.insert(raw_user_id, media_id).is_some() {
            return Err(anyhow!("user {raw_user_id} camera media already exists"));
        }
        self.stats.publications = self.stats.publications.saturating_add(1);
        Ok(())
    }

    async fn deactivate_camera(&mut self, raw_user_id: RawUserId) -> Result<()> {
        let user_id = user(raw_user_id);
        let stream_id = stream_id_for_source(TestSourceKind::ScalableVideo);
        if self
            .room
            .test_api()
            .deactivate_publication(&user_id, &stream_id, &self.media_transport)
            .await
        {
            if !self.media.camera.contains_key(&raw_user_id) {
                return Err(anyhow!("user {raw_user_id} camera media was not tracked"));
            }
            self.stats.unpublications = self.stats.unpublications.saturating_add(1);
            return Ok(());
        }
        Err(anyhow!("user {raw_user_id} camera publication was missing"))
    }

    async fn close_user(&mut self, raw_user_id: RawUserId) -> Result<()> {
        let mut session = self
            .user_sessions
            .remove(&raw_user_id)
            .ok_or_else(|| anyhow!("user {raw_user_id} session was not tracked"))?;
        if !session.close().await {
            return Err(anyhow!("user {raw_user_id} close did not remove a session"));
        }
        if self.media.audio.remove(&raw_user_id).is_some() {
            self.stats.unpublications = self.stats.unpublications.saturating_add(1);
        }
        if self.media.camera.remove(&raw_user_id).is_some() {
            self.stats.unpublications = self.stats.unpublications.saturating_add(1);
        }
        if let Some(mut receiver) = self.receivers.remove(&raw_user_id) {
            self.stats.outbound_events = self
                .stats
                .outbound_events
                .saturating_add(drain_receiver(&mut receiver));
        }
        self.stats.leaves = self.stats.leaves.saturating_add(1);
        self.drain_outbound_events();
        Ok(())
    }

    async fn subscribe_all_live_users_to_audio(&mut self) -> Result<()> {
        let receivers = self.user_sessions.keys().copied().collect::<Vec<_>>();
        let publishers = self.media.audio.keys().copied().collect::<Vec<_>>();
        for receiver in receivers {
            for publisher in publishers.iter().copied() {
                if receiver != publisher {
                    self.update_audio_subscription(receiver, publisher).await?;
                }
            }
        }
        Ok(())
    }

    async fn update_audio_subscription(
        &mut self,
        receiver: RawUserId,
        publisher: RawUserId,
    ) -> Result<()> {
        let intents = subscription_intents_from_test_states(&TestSubscriptionStates {
            audio_detector: Some(true),
            ..TestSubscriptionStates::default()
        });
        self.update_subscription(receiver, publisher, &intents)
            .await
    }

    async fn apply_video_subscriptions(&mut self, updates: &[VideoSubscription]) -> Result<()> {
        for update in updates.iter().copied() {
            let intents = subscription_intents_from_test_states(&TestSubscriptionStates {
                scalable_video: Some(update.enabled),
                scalable_video_layout: Some(update.layout),
                ..TestSubscriptionStates::default()
            });
            self.update_subscription(update.receiver, update.publisher, &intents)
                .await?;
        }
        Ok(())
    }

    async fn update_subscription(
        &mut self,
        receiver: RawUserId,
        publisher: RawUserId,
        intents: &BTreeMap<UserStreamId, SourceSubscriptionIntent>,
    ) -> Result<()> {
        let publisher_user_id = user(publisher);
        let session = self
            .user_sessions
            .get(&receiver)
            .ok_or_else(|| anyhow!("user {receiver} session was missing"))?;
        session
            .subscribe(&publisher_user_id, intents)
            .await
            .map_err(|error| anyhow!("user {receiver} subscription update failed: {error}"))?;
        self.stats.subscription_updates = self.stats.subscription_updates.saturating_add(1);
        Ok(())
    }

    async fn run_media_time(&mut self, duration: Duration) -> Result<()> {
        let ticks = usize::try_from(duration.as_millis() / u128::from(MEDIA_TICK_MS))
            .map_err(|error| anyhow!("synthetic media tick count overflowed: {error}"))?;
        for tick in 0..ticks {
            self.observe_vad_tick(tick).await;
            if tick % POLICY_REFRESH_TICKS == 0 {
                self.refresh_source_policy().await;
                self.inspect_route_state().await;
            }
        }
        Ok(())
    }

    async fn observe_vad_tick(&mut self, tick: usize) {
        let pattern_index = tick % SPEAKER_PATTERN.len();
        if let Some(raw_user_id) = SPEAKER_PATTERN.get(pattern_index).copied()
            && let Some(media_id) = self.media.audio.get(&raw_user_id).copied()
        {
            self.media_transport
                .test_api()
                .observe_audio_activity_with_level(
                    media_id,
                    audio_level_for_tick(tick),
                    self.synthetic_now,
                )
                .await;
            self.stats.audio_observations = self.stats.audio_observations.saturating_add(1);
        }
        self.synthetic_now += Duration::from_millis(MEDIA_TICK_MS);
    }

    async fn refresh_source_policy(&mut self) {
        let room_ids = self.source_policy_updates.take_pending_updates();
        if room_ids.is_empty() {
            return;
        }
        self.manager
            .sync_source_packet_selection_policies_for_runtime_ids(&room_ids, &self.media_transport)
            .await;
        self.stats.policy_refreshes = self.stats.policy_refreshes.saturating_add(1);
    }

    async fn inspect_route_state(&mut self) {
        let inspect = self.room.test_api();
        self.stats.producer_count = inspect.producer_count().await;
        self.stats.consumer_count = inspect.consumer_count().await;
        self.stats.router_count = inspect.router_count().await;

        let users = self.user_sessions.keys().copied().collect::<Vec<_>>();
        for raw_user_id in users {
            if inspect
                .home_media_worker_id(&user(raw_user_id))
                .await
                .is_some()
            {
                self.stats.worker_assignments = self.stats.worker_assignments.saturating_add(1);
            }
        }

        for media_id in self
            .media
            .audio
            .values()
            .chain(self.media.camera.values())
            .copied()
        {
            if self
                .media_transport
                .test_api()
                .route_entry_by_media_id(media_id)
                .await
                .is_some()
            {
                self.stats.route_inspections = self.stats.route_inspections.saturating_add(1);
            }
        }
    }

    async fn producer_media_id(
        &self,
        raw_user_id: RawUserId,
        source_kind: TestSourceKind,
    ) -> Result<TransportMediaId> {
        let user_id = user(raw_user_id);
        let connection_id = self
            .user_sessions
            .get(&raw_user_id)
            .map(MediaSession::connection_id)
            .ok_or_else(|| anyhow!("user {raw_user_id} connection was missing"))?;
        self.room
            .test_api()
            .producer_transport_media_id(&user_id, connection_id, source_kind)
            .await
            .ok_or_else(|| anyhow!("user {raw_user_id} {source_kind:?} media id was missing"))
    }

    fn drain_outbound_events(&mut self) {
        for receiver in self.receivers.values_mut() {
            self.stats.outbound_events = self
                .stats
                .outbound_events
                .saturating_add(drain_receiver(receiver));
        }
    }
}

fn media_transport() -> Result<MediaTransport> {
    let config = MediaTransportConfig {
        worker_count: WORKER_COUNT,
        announced_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        bitrate_limits: SessionBitrateLimits::new(Bitrate::from_mbps(8), Bitrate::from_mbps(10)),
        video_bitrate_limits: VideoBitrateLimits::default(),
        rtc_port_range: test_rtc_port_range(),
        rtc_udp_io_backend: RtcUdpIoBackend::Tokio,
        codec_flags: MediaCodecFlags::default(),
        codec_preferences: CodecPreferences::default(),
        media_quality_interval: None,
    };
    let deps = MediaTransportDeps {
        packet_sink_registry: Arc::new(RoomPacketSinkRegistry::default()),
        metrics: Arc::new(RuntimeMetrics::default()),
    };
    MediaTransport::build(config, deps)
        .map_err(|error| anyhow!("benchmark media transport build failed: {error}"))
}

fn room_manager(media_transport: &MediaTransport) -> Result<Arc<RoomManager>> {
    let media_limits = RoomMediaLimits::try_new(4, 3)?;
    let max_local_routers = NonZeroUsize::new(WORKER_COUNT)
        .ok_or_else(|| anyhow!("benchmark worker count should be positive"))?;
    let delay_threshold = NonZeroU64::new(RoomWorkerPolicy::DEFAULT_PACKET_LOOP_DELAY_THRESHOLD_MS)
        .ok_or_else(|| anyhow!("default delay threshold should be positive"))?;
    Ok(Arc::new(RoomManager::for_test_with_runtime_policy(
        RoomRuntimePolicy::new(
            RoomAdmissionPolicy::new(12),
            RuntimeFeatureFlags::default(),
            media_transport.router_rtp_capabilities(),
        )
        .with_room_worker_policy(RoomWorkerPolicy::new(max_local_routers, delay_threshold))
        .with_media_limits(media_limits),
    )))
}

const fn user(raw_user_id: RawUserId) -> UserId {
    UserId::Integer(raw_user_id)
}

fn video_mid(raw_user_id: RawUserId) -> Result<&'static str> {
    match raw_user_id {
        1 => Ok("v1"),
        2 => Ok("v2"),
        3 => Ok("v3"),
        4 => Ok("v4"),
        _ => Err(anyhow!("user {raw_user_id} has no benchmark camera MID")),
    }
}

fn audio_level_for_tick(tick: usize) -> i8 {
    i8::try_from(tick % 16).map_or(-12, |offset| -12 - offset)
}

fn drain_receiver(receiver: &mut UserOutboundReceiver) -> usize {
    let mut drained = 0_usize;
    while let Ok(_message) = receiver.try_recv() {
        drained = drained.saturating_add(1);
    }
    if receiver.has_overflowed() {
        drained = drained.saturating_add(1);
    }
    drained
}
