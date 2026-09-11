#![allow(
    clippy::expect_used,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::too_many_lines,
    reason = "this benchmark-owned module is a readable fixed scenario rather than a reusable public API"
)]

//! room-level source-policy fixture for video budget solver benchmarks
//!
//! the packet-loop scenario benchmark records receiver bandwidth estimates and
//! wakes source-policy recomputation, but the recomputation itself lives in the
//! room layer and needs a `RoomState`
//! this fixture owns that half: a real room with real subscriptions, driven
//! through repeated source-policy turns against a bandwidth trace
//!
//! this module is the single description of the scenario. the Callgrind driver
//! in `source_policy_callgrind.rs` only runs the turns
//!
//! # what is not measured
//!
//! this scenario does not benchmark the media workers
//! a policy turn ends when its route effects are handed to the transport, and the
//! worker-side cost of applying them belongs to `packet_loop_callgrind` and
//! `meeting_flow_callgrind`
//! the room here is real, so the effects do reach a worker, but the numbers move
//! with the cost of deciding rather than the cost of forwarding
//!
//! # why a bandwidth trace
//!
//! production reads receiver bandwidth out of the media transport, which only
//! holds values that real `str0m` BWE events produced
//! a socket-free benchmark never produces those events, which is why the room
//! benchmark that came before this one left `receiver_bandwidth` unset and
//! skipped `effective_video_budget`, `apply_overload_policy`, hysteresis and
//! every `BudgetPressure` decision
//! injecting the snapshot is what makes those paths run
//!
//! Each bandwidth plateau lasts through the 750 ms dwell. Injected time makes
//! pressure and upgrade eligibility deterministic.

use std::{
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
            ConsumerRouteState, JoinUserRequest, Room, RoomAdmissionPolicy, RoomConfig,
            RoomManager, RoomRuntimePolicy, UserOutboundReceiver, UserOutboundSender,
            benchmark_support::run_source_policy_turn_for_benchmark,
            test_support::{
                TestSourceKind, TestSubscriptionStates, stream_id_for_source,
                subscription_intents_from_test_states,
            },
        },
        session::{UserId, UserPermissions, VideoLayoutIntent},
        transport::{
            ActiveSpeakerSource, MediaTransport, MediaTransportConfig, MediaTransportDeps,
            ReceiverBandwidthSnapshot, TransportSessionKey, test_support::test_rtc_port_range,
        },
    },
};
use o_sfu_router::test_support::rtp_samples;
use o_sfu_telemetry::{
    DEFAULT_MEDIA_QUALITY_INTERVAL, diagnostics::types::DiagnosticsPolicyPauseReason,
};
use tokio::runtime::{Builder, Runtime};

const TEST_ROOM_KEY: &str = "Y2hhbm5lbC1rZXk=";
const WORKER_COUNT: usize = 4;
const OUTBOUND_QUEUE_CAPACITY: usize = 1024;

/// participants in the modelled room, matching the packet-loop scenario
const PARTICIPANTS: usize = 12;
/// participants publishing a simulcast camera
const VIDEO_PUBLISHERS: usize = 6;
/// video subscriptions per receiver: one featured plus four thumbnails
const VIDEO_SUBSCRIPTIONS: usize = 5;
/// audio sources the room admits at once, below the number of publishers so the
/// speaker limiter actually binds
const MAX_ACTIVE_AUDIO_SPEAKERS: usize = 4;
/// video downloads the room admits per receiver
const MAX_VIDEO_DOWNLOADS: usize = 3;

/// one policy turn per receiver bandwidth report of a six second call
const POLICY_TURNS: usize = 24;

/// receiver bandwidth walked from headroom into hard overload and back
///
/// The hard limit admits three routes with a combined 450 kbps floor. The
/// 400 kbps plateau forces a pause after the dwell and later plateaus recover.
const BANDWIDTH_TRACE_BPS: [u64; 12] = [
    2_500_000, 2_500_000, 2_500_000, 2_500_000, 400_000, 400_000, 400_000, 400_000, 900_000,
    900_000, 900_000, 900_000,
];
/// bandwidth pair used by the out-of-window differential observation
const RELAXED_BANDWIDTH_BPS: u64 = 2_500_000;
const PRESSURED_BANDWIDTH_BPS: u64 = 400_000;
/// turns driven at each bandwidth before observing, so the 750 ms dwell expires
const OBSERVATION_TURNS: usize = 4;
/// top layer of the three-layer simulcast ladder the publishers offer
const TOP_SIMULCAST_RID: &str = "hi";

type RawUserId = i64;

/// what the room's diagnostics report after one worst-case bandwidth turn
#[derive(Debug, Default, Clone, Copy)]
pub struct BudgetPressureObservation {
    /// video subscriptions paused specifically for budget pressure
    pub budget_pressure_pauses: usize,
    /// video subscriptions the solver left on the top simulcast layer
    pub top_layer_subscriptions: usize,
}

/// monotonic counters proving the solver kept making decisions
#[derive(Debug, Default, Clone, Copy)]
pub struct SourcePolicyStats {
    turns: usize,
    turns_with_work: usize,
    outbound_events: usize,
}

impl SourcePolicyStats {
    pub fn total_work(self) -> usize {
        [self.turns, self.turns_with_work, self.outbound_events]
            .into_iter()
            .fold(0, usize::saturating_add)
    }
}

pub struct SourcePolicyFixture {
    runtime: Runtime,
    scenario: SourcePolicyScenario,
}

impl Default for SourcePolicyFixture {
    fn default() -> Self {
        Self::new()
    }
}

impl SourcePolicyFixture {
    /// builds the room, its publications and its subscriptions
    ///
    /// Four turns spanning 750 ms at full headroom settle initial selections so the
    /// measured turns are all bandwidth-driven changes
    #[expect(
        clippy::panic,
        reason = "benchmark setup must fail loudly when deterministic room setup is invalid"
    )]
    pub fn new() -> Self {
        let runtime = build_runtime();
        let scenario = match runtime.block_on(SourcePolicyScenario::new()) {
            Ok(scenario) => scenario,
            Err(error) => panic!("failed to build source policy benchmark scenario: {error}"),
        };
        Self { runtime, scenario }
    }

    /// Adds a fixed speaker snapshot containing active, inactive and foreign sources.
    ///
    /// Publications are real. Injecting their observations keeps worker polling and
    /// wall-clock expiry outside the measured room-selection boundary.
    pub fn mixed_speakers() -> Self {
        let mut fixture = Self::new();
        fixture
            .runtime
            .block_on(fixture.scenario.prepare_mixed_speakers())
            .expect("mixed speaker publications should be available");
        fixture
    }

    /// Checks admitted audio and featured output against the mixed speaker snapshot.
    pub fn assert_speaker_selection(&self) {
        assert!(self.scenario.speaker_snapshot.is_some());
        self.runtime.block_on(async {
            let receiver = user(raw_user_id(PARTICIPANTS - 1));
            let audio = stream_id_for_source(TestSourceKind::AudioDetector);
            for (publisher, expected) in [
                (MAX_ACTIVE_AUDIO_SPEAKERS - 1, ConsumerRouteState::Active),
                (MAX_ACTIVE_AUDIO_SPEAKERS, ConsumerRouteState::Inactive),
            ] {
                assert_eq!(
                    self.scenario
                        .room
                        .test_api()
                        .consumer_route_state(&receiver, &user(raw_user_id(publisher)), &audio)
                        .await,
                    Some(expected)
                );
            }
            let (_, info) = self
                .scenario
                .room
                .test_api()
                .user_info_snapshot(&user(raw_user_id(0)))
                .await
                .expect("the leading local speaker should remain present");
            assert_eq!(info.is_featured, Some(true));
        });
    }

    /// runs the measured source-policy turns and returns the accumulated work
    #[expect(
        clippy::panic,
        reason = "benchmark execution must fail loudly when the fixed room flow stops being valid"
    )]
    pub fn run_policy_turns(&mut self) -> usize {
        let Self { runtime, scenario } = self;
        match runtime.block_on(scenario.run()) {
            Ok(stats) => stats.total_work(),
            Err(error) => panic!("source policy benchmark failed: {error}"),
        }
    }

    /// asserts the solver committed a plan on every turn it was given
    ///
    /// `total_work` cannot show this. `turns` grows on every attempt and is part
    /// of the total, so the accumulated work stays healthy even when the solver
    /// runs every turn and decides nothing. `turns_with_work` only grows when a
    /// turn produced a transaction and committed it
    pub fn assert_every_turn_planned(&self) {
        let stats = self.scenario.stats;
        assert_eq!(stats.turns, POLICY_TURNS);
        assert_eq!(
            stats.turns_with_work, stats.turns,
            "each bandwidth or dwell turn must commit its receiver plan, got {} of {}",
            stats.turns_with_work, stats.turns
        );
    }

    /// asserts the video budget solver really constrained the plan
    ///
    /// this runs outside the measured window because it is pure observation
    ///
    /// the check is differential on purpose: absolute facts like "some
    /// subscription is not on the top layer" are true even with no bandwidth at
    /// all, because thumbnails are supposed to sit on a low layer. only comparing
    /// a relaxed run against a pressured one proves the budget changed the plan
    #[expect(
        clippy::panic,
        reason = "a scenario that stopped constraining the plan must fail loudly"
    )]
    pub fn assert_budget_pressure_observed(&mut self) {
        let Self { runtime, scenario } = self;
        let (relaxed, pressured) = match runtime.block_on(async {
            let relaxed = scenario.observe_at(RELAXED_BANDWIDTH_BPS).await?;
            let pressured = scenario.observe_at(PRESSURED_BANDWIDTH_BPS).await?;
            Ok::<_, anyhow::Error>((relaxed, pressured))
        }) {
            Ok(observations) => observations,
            Err(error) => panic!("budget pressure observation failed: {error}"),
        };
        assert!(
            pressured.top_layer_subscriptions < relaxed.top_layer_subscriptions
                || pressured.budget_pressure_pauses > relaxed.budget_pressure_pauses,
            "dropping receiver bandwidth from {RELAXED_BANDWIDTH_BPS} to {PRESSURED_BANDWIDTH_BPS} bps did not change the plan, so the video budget solver never saw it: relaxed={relaxed:?} pressured={pressured:?}"
        );
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

struct SourcePolicyScenario {
    manager: Arc<RoomManager>,
    media_transport: MediaTransport,
    outbound_metrics: Arc<RuntimeMetrics>,
    receivers: BTreeMap<RawUserId, UserOutboundReceiver>,
    room: Arc<Room>,
    now: Instant,
    session_keys: Vec<TransportSessionKey>,
    stats: SourcePolicyStats,
    user_sessions: BTreeMap<RawUserId, MediaSession>,
    speaker_snapshot: Option<Vec<ActiveSpeakerSource>>,
}

impl SourcePolicyScenario {
    async fn new() -> Result<Self> {
        let media_transport = media_transport()?;
        let manager = room_manager(&media_transport)?;
        let room = manager
            .serve_room(
                "source-policy-benchmark",
                TEST_ROOM_KEY,
                &RoomConfig::default(),
                Some("source-policy-benchmark"),
            )
            .await?;
        let core = SfuCore::new(media_transport.clone(), Arc::clone(&manager));
        let mut scenario = Self {
            manager,
            media_transport,
            outbound_metrics: Arc::new(RuntimeMetrics::default()),
            receivers: BTreeMap::new(),
            room,
            now: Instant::now(),
            session_keys: Vec::with_capacity(PARTICIPANTS),
            stats: SourcePolicyStats::default(),
            user_sessions: BTreeMap::new(),
            speaker_snapshot: None,
        };
        scenario.build_room(&core).await?;
        scenario.now = Instant::now();
        // Settle initial exact-target upgrades before measuring pressure turns.
        for _ in 0..OBSERVATION_TURNS {
            scenario.run_turn(BANDWIDTH_TRACE_BPS[0]).await?;
        }
        scenario.stats = SourcePolicyStats::default();
        Ok(scenario)
    }

    async fn prepare_mixed_speakers(&mut self) -> Result<()> {
        let mut sources = Vec::with_capacity(PARTICIPANTS + 1);
        for (index, (raw_user, session)) in self.user_sessions.iter().enumerate() {
            let media_id = self
                .room
                .test_api()
                .producer_transport_media_id(
                    &user(*raw_user),
                    session.connection_id(),
                    TestSourceKind::AudioDetector,
                )
                .await
                .ok_or_else(|| anyhow!("local audio publication is missing"))?;
            // Rank excluded speakers first to exercise filtering before the speaker limit.
            let recency = if index == PARTICIPANTS - 1 {
                PARTICIPANTS + 1
            } else {
                PARTICIPANTS - index
            };
            sources.push(ActiveSpeakerSource::new(
                media_id,
                self.now + Duration::from_millis(u64::try_from(recency)?),
            ));
        }
        let audio = stream_id_for_source(TestSourceKind::AudioDetector);
        if !self
            .room
            .test_api()
            .deactivate_publication(
                &user(raw_user_id(PARTICIPANTS - 1)),
                &audio,
                &self.media_transport,
            )
            .await
        {
            return Err(anyhow!("inactive speaker publication was not deactivated"));
        }
        let foreign_room = self
            .manager
            .serve_room(
                "foreign-speaker-benchmark",
                TEST_ROOM_KEY,
                &RoomConfig::default(),
                None,
            )
            .await?;
        let foreign_user = raw_user_id(PARTICIPANTS);
        let (sender, receiver) = UserOutboundSender::channel(
            OUTBOUND_QUEUE_CAPACITY,
            Arc::clone(&self.outbound_metrics),
        );
        let connection = foreign_room
            .test_api()
            .join_user(user(foreign_user), None, UserPermissions::default(), sender)
            .await
            .map_err(|error| anyhow!("foreign speaker admission failed: {error:?}"))?;
        foreign_room
            .test_api()
            .make_session_ready(&user(foreign_user), &self.media_transport)
            .await?;
        foreign_room
            .test_api()
            .publish_track(
                &user(foreign_user),
                TestSourceKind::AudioDetector,
                rtp_samples::sample_audio_rtp_parameters(12_000),
                &self.media_transport,
            )
            .await
            .ok_or_else(|| anyhow!("foreign audio publication is missing"))?;
        let foreign_media = foreign_room
            .test_api()
            .producer_transport_media_id(
                &user(foreign_user),
                connection,
                TestSourceKind::AudioDetector,
            )
            .await
            .ok_or_else(|| anyhow!("foreign audio identity is missing"))?;
        self.now += Duration::from_millis(u64::try_from(PARTICIPANTS + 2)?);
        sources.push(ActiveSpeakerSource::new(foreign_media, self.now));
        self.receivers.insert(foreign_user, receiver);
        self.speaker_snapshot = Some(sources);
        self.run_turn(BANDWIDTH_TRACE_BPS[0]).await?;
        self.stats = SourcePolicyStats::default();
        Ok(())
    }

    async fn run(&mut self) -> Result<SourcePolicyStats> {
        for turn in 0..POLICY_TURNS {
            let bandwidth_bps = BANDWIDTH_TRACE_BPS
                .get(turn % BANDWIDTH_TRACE_BPS.len())
                .copied()
                .ok_or_else(|| anyhow!("bandwidth trace index {turn} was out of range"))?;
            self.run_turn(bandwidth_bps).await?;
        }
        self.drain_outbound_events();
        Ok(self.stats)
    }

    async fn run_turn(&mut self, bandwidth_bps: u64) -> Result<()> {
        let bandwidth = ReceiverBandwidthSnapshot {
            per_session: self
                .session_keys
                .iter()
                .cloned()
                .map(|session_key| (session_key, Bitrate::from_bps(bandwidth_bps)))
                .collect(),
        };
        let produced_work = run_source_policy_turn_for_benchmark(
            &self.room,
            &self.media_transport,
            &bandwidth,
            self.speaker_snapshot.as_deref(),
            self.now,
        )
        .await;
        self.now += Duration::from_millis(250);
        self.stats.turns = self.stats.turns.saturating_add(1);
        if produced_work {
            self.stats.turns_with_work = self.stats.turns_with_work.saturating_add(1);
        }
        self.drain_outbound_events();
        Ok(())
    }

    /// settles the room at one bandwidth, then reads what the solver decided
    async fn observe_at(&mut self, bandwidth_bps: u64) -> Result<BudgetPressureObservation> {
        for _ in 0..OBSERVATION_TURNS {
            self.run_turn(bandwidth_bps).await?;
        }
        let capture = self.room.diagnostics_detail_capture().await;
        let session_keys = capture.session_keys();
        let source_keys = capture.source_keys().cloned().collect::<Vec<_>>();
        let bitrate = self
            .media_transport
            .transport_bitrate_snapshot(session_keys);
        let quality = self
            .media_transport
            .transport_quality_snapshot(session_keys);
        let health = self.media_transport.transport_health_snapshot(session_keys);
        let source_diagnostics = self
            .media_transport
            .source_diagnostics_snapshot(&source_keys)
            .await;
        let (_, users, _) = capture.into_views(&bitrate, &quality, &health, &source_diagnostics);
        let video_stream_id = stream_id_for_source(TestSourceKind::ScalableVideo).to_string();
        let mut observation = BudgetPressureObservation::default();
        for subscription in users
            .iter()
            .flat_map(|user| user.subscriptions.iter())
            .filter(|subscription| subscription.stream_id == video_stream_id)
        {
            if subscription.selection.policy_pause_reason
                == Some(DiagnosticsPolicyPauseReason::BudgetPressure)
            {
                observation.budget_pressure_pauses =
                    observation.budget_pressure_pauses.saturating_add(1);
            }
            if subscription.selection.selected_rid.as_deref() == Some(TOP_SIMULCAST_RID) {
                observation.top_layer_subscriptions =
                    observation.top_layer_subscriptions.saturating_add(1);
            }
        }
        Ok(observation)
    }

    async fn build_room(&mut self, core: &SfuCore) -> Result<()> {
        for participant in 0..PARTICIPANTS {
            self.join_ready(core, raw_user_id(participant)).await?;
        }
        for participant in 0..PARTICIPANTS {
            self.publish_audio(raw_user_id(participant)).await?;
        }
        for participant in 0..VIDEO_PUBLISHERS {
            self.publish_camera(participant).await?;
        }
        self.subscribe_all_audio().await?;
        self.subscribe_video().await?;
        self.collect_session_keys().await?;
        Ok(())
    }

    async fn join_ready(&mut self, core: &SfuCore, raw_user_id: RawUserId) -> Result<()> {
        let (sender, receiver) = UserOutboundSender::channel(
            OUTBOUND_QUEUE_CAPACITY,
            Arc::clone(&self.outbound_metrics),
        );
        let session = core
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
        self.drain_outbound_events();
        Ok(())
    }

    async fn publish_audio(&self, raw_user_id: RawUserId) -> Result<()> {
        let ssrc = 11_000_u32.saturating_add(u32::try_from(raw_user_id).unwrap_or(0));
        let published_stream = self
            .room
            .test_api()
            .publish_track(
                &user(raw_user_id),
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
        Ok(())
    }

    async fn publish_camera(&self, participant: usize) -> Result<()> {
        let raw_user_id = raw_user_id(participant);
        let published_stream = self
            .room
            .test_api()
            .publish_track(
                &user(raw_user_id),
                TestSourceKind::ScalableVideo,
                rtp_samples::sample_three_layer_simulcast_video_rtp_parameters(Some(video_mid(
                    participant,
                ))),
                &self.media_transport,
            )
            .await
            .ok_or_else(|| anyhow!("user {raw_user_id} camera publish failed"))?;
        if published_stream != stream_id_for_source(TestSourceKind::ScalableVideo) {
            return Err(anyhow!(
                "user {raw_user_id} camera publish returned wrong stream id"
            ));
        }
        Ok(())
    }

    async fn subscribe_all_audio(&self) -> Result<()> {
        let intents = subscription_intents_from_test_states(&TestSubscriptionStates {
            audio_detector: Some(true),
            ..TestSubscriptionStates::default()
        });
        for receiver in 0..PARTICIPANTS {
            for publisher in 0..PARTICIPANTS {
                if receiver != publisher {
                    self.update_subscription(receiver, publisher, &intents)
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// gives every receiver one featured publisher and four thumbnails
    async fn subscribe_video(&self) -> Result<()> {
        for receiver in 0..PARTICIPANTS {
            for (subscription, publisher) in video_publishers_for(receiver).into_iter().enumerate()
            {
                let layout = if subscription == 0 {
                    VideoLayoutIntent::Featured
                } else {
                    VideoLayoutIntent::VisibleThumbnail
                };
                let intents = subscription_intents_from_test_states(&TestSubscriptionStates {
                    scalable_video: Some(true),
                    scalable_video_layout: Some(layout),
                    ..TestSubscriptionStates::default()
                });
                self.update_subscription(receiver, publisher, &intents)
                    .await?;
            }
        }
        Ok(())
    }

    async fn update_subscription(
        &self,
        receiver: usize,
        publisher: usize,
        intents: &BTreeMap<UserStreamId, SourceSubscriptionIntent>,
    ) -> Result<()> {
        let receiver_id = raw_user_id(receiver);
        let session = self
            .user_sessions
            .get(&receiver_id)
            .ok_or_else(|| anyhow!("user {receiver_id} session was missing"))?;
        session
            .subscribe(&user(raw_user_id(publisher)), intents)
            .await
            .map_err(|error| anyhow!("user {receiver_id} subscription update failed: {error}"))?;
        Ok(())
    }

    async fn collect_session_keys(&mut self) -> Result<()> {
        for (raw_user_id, session) in &self.user_sessions {
            let session_key = self
                .room
                .transport_user_key(&user(*raw_user_id), session.connection_id())
                .await;
            self.session_keys.push(session_key);
        }
        if self.session_keys.len() != PARTICIPANTS {
            return Err(anyhow!(
                "expected {PARTICIPANTS} transport session keys, got {}",
                self.session_keys.len()
            ));
        }
        Ok(())
    }

    fn drain_outbound_events(&mut self) {
        for receiver in self.receivers.values_mut() {
            let mut drained = 0_usize;
            while receiver.try_recv().is_ok() {
                drained = drained.saturating_add(1);
            }
            if receiver.has_overflowed() {
                drained = drained.saturating_add(1);
            }
            self.stats.outbound_events = self.stats.outbound_events.saturating_add(drained);
        }
    }
}

/// builds the transport with the defaults an operator actually runs
///
/// the room benchmark that came before this one disabled media-quality sampling
/// and forced bounded spillover, neither of which matches a normal deployment
fn media_transport() -> Result<MediaTransport> {
    let rtc_port_range = test_rtc_port_range();
    let config = MediaTransportConfig {
        worker_count: WORKER_COUNT,
        announced_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        bitrate_limits: SessionBitrateLimits::new(Bitrate::from_mbps(8), Bitrate::from_mbps(10)),
        video_bitrate_limits: VideoBitrateLimits::default(),
        rtc_port_range,
        rtc_udp_io_backend: RtcUdpIoBackend::Tokio,
        codec_flags: MediaCodecFlags::default(),
        codec_preferences: CodecPreferences::default(),
        media_quality_interval: Some(DEFAULT_MEDIA_QUALITY_INTERVAL),
    };
    let deps = MediaTransportDeps {
        packet_sink_registry: Arc::new(RoomPacketSinkRegistry::default()),
        metrics: Arc::new(RuntimeMetrics::default()),
    };
    MediaTransport::build(config, deps)
        .map_err(|error| anyhow!("benchmark media transport build failed: {error}"))
}

fn room_manager(media_transport: &MediaTransport) -> Result<Arc<RoomManager>> {
    let media_limits = RoomMediaLimits::try_new(MAX_ACTIVE_AUDIO_SPEAKERS, MAX_VIDEO_DOWNLOADS)?;
    Ok(Arc::new(RoomManager::for_test_with_runtime_policy(
        RoomRuntimePolicy::new(
            RoomAdmissionPolicy::new(PARTICIPANTS),
            RuntimeFeatureFlags::default(),
            media_transport.router_rtp_capabilities(),
        )
        .with_room_worker_policy(RoomWorkerPolicy::new(
            NonZeroUsize::new(WORKER_COUNT).expect("benchmark worker count must be positive"),
            NonZeroU64::new(RoomWorkerPolicy::DEFAULT_PACKET_LOOP_DELAY_THRESHOLD_MS)
                .expect("benchmark packet loop delay threshold must be positive"),
        ))
        .with_media_limits(media_limits),
    )))
}

const fn user(raw_user_id: RawUserId) -> UserId {
    UserId::Integer(raw_user_id)
}

/// the video publishers one receiver subscribes to, featured first
///
/// a receiver never subscribes to its own camera, so the self entry is filtered
/// out before the subscriptions are counted. skipping it afterwards instead would
/// cost the six publishers their featured slot, leaving the solver a room with
/// half the featured routes it is supposed to weigh
fn video_publishers_for(receiver: usize) -> Vec<usize> {
    (0..VIDEO_PUBLISHERS)
        .map(|offset| (receiver + offset) % VIDEO_PUBLISHERS)
        .filter(|publisher| *publisher != receiver)
        .take(VIDEO_SUBSCRIPTIONS)
        .collect()
}

fn raw_user_id(participant: usize) -> RawUserId {
    RawUserId::try_from(participant)
        .unwrap_or(0)
        .saturating_add(1)
}

fn video_mid(participant: usize) -> &'static str {
    const MIDS: [&str; VIDEO_PUBLISHERS] = ["v0", "v1", "v2", "v3", "v4", "v5"];
    MIDS.get(participant).copied().unwrap_or("v0")
}
