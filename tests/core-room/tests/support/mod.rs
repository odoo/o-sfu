#![allow(
    clippy::missing_errors_doc,
    clippy::must_use_candidate,
    reason = "shared integration-test harness helpers return contextual anyhow errors and are not production APIs"
)]

use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr},
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
};

use anyhow::{Result, anyhow};
use o_sfu_core::{
    ConnectionId,
    prelude::{
        Bitrate, CodecPreferences, MediaCodecFlags, MediaSession, RoomWorkerPolicy,
        RtcUdpIoBackend, RuntimeFeatureFlags, SessionBitrateLimits, SfuCore, UserStreamId,
        VideoBitrateLimits,
    },
    server::{
        metrics::RuntimeMetrics,
        packet_sinks::RoomPacketSinkRegistry,
        room::{
            JoinUserRequest, Room, RoomAdmissionPolicy, RoomConfig, RoomManager, RoomRuntimePolicy,
            UserOutboundReceiver, UserOutboundSender, test_support::TestSourceKind,
        },
        session::{UserId, UserPermissions},
        transport::{
            MediaTransport, MediaTransportConfig, MediaTransportDeps,
            test_support::test_rtc_port_range,
        },
    },
};
use o_sfu_router::{
    rtp::MediaStream,
    test_support::rtp_samples::{
        sample_audio_rtp_parameters, sample_client_rtp_capabilities,
        sample_simulcast_video_rtp_parameters, sample_video_rtp_parameters,
    },
};

pub const TEST_ROOM_KEY: &str = "Y2hhbm5lbC1rZXk=";
const DEFAULT_MAX_SESSIONS: usize = 100;

pub fn test_sender() -> (UserOutboundSender, UserOutboundReceiver) {
    UserOutboundSender::channel(1024, Arc::new(RuntimeMetrics::default()))
}

pub fn media_transport() -> Result<MediaTransport> {
    MediaTransport::build(
        MediaTransportConfig {
            worker_count: 4,
            announced_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            bitrate_limits: SessionBitrateLimits::new(
                Bitrate::from_mbps(8),
                Bitrate::from_mbps(10),
            ),
            video_bitrate_limits: VideoBitrateLimits::default(),
            rtc_port_range: test_rtc_port_range(),
            rtc_udp_io_backend: RtcUdpIoBackend::Tokio,
            codec_flags: MediaCodecFlags::default(),
            codec_preferences: CodecPreferences::default(),
            media_quality_interval: None,
        },
        MediaTransportDeps {
            packet_sink_registry: Arc::new(RoomPacketSinkRegistry::default()),
            metrics: Arc::new(RuntimeMetrics::default()),
        },
    )
    .map_err(|error| anyhow!("test media transport should build: {error}"))
}

pub fn manager_with_policy(policy: RoomWorkerPolicy) -> RoomManager {
    let runtime_policy = RoomRuntimePolicy::new(
        RoomAdmissionPolicy::new(DEFAULT_MAX_SESSIONS),
        RuntimeFeatureFlags::default(),
        sample_client_rtp_capabilities(),
    )
    .with_room_worker_policy(policy);
    RoomManager::for_test_with_runtime_policy(runtime_policy)
}

pub fn spillover_policy(max_local_routers: usize) -> Result<RoomWorkerPolicy> {
    let max_local_routers = NonZeroUsize::new(max_local_routers)
        .ok_or_else(|| anyhow!("test router cap should be positive"))?;
    let delay_threshold = NonZeroU64::new(RoomWorkerPolicy::DEFAULT_PACKET_LOOP_DELAY_THRESHOLD_MS)
        .ok_or_else(|| anyhow!("default delay threshold should be positive"))?;
    Ok(RoomWorkerPolicy::new(max_local_routers, delay_threshold))
}

pub async fn serve_room(manager: &RoomManager, issuer: &str) -> Result<Arc<Room>> {
    Ok(manager
        .serve_room(issuer, TEST_ROOM_KEY, &RoomConfig::default(), None)
        .await?)
}

pub async fn join_user(
    manager: &RoomManager,
    room: &Arc<Room>,
    raw_user_id: i64,
    media_transport: &MediaTransport,
) -> Result<ConnectionId> {
    join_user_with_receiver(manager, room, raw_user_id, media_transport)
        .await
        .map(|(connection_id, _receiver)| connection_id)
}

pub async fn join_user_with_receiver(
    manager: &RoomManager,
    room: &Arc<Room>,
    raw_user_id: i64,
    media_transport: &MediaTransport,
) -> Result<(ConnectionId, UserOutboundReceiver)> {
    let (sender, receiver) = test_sender();
    let session = manager
        .join_user(
            room.uuid(),
            JoinUserRequest {
                user_id: UserId::Integer(raw_user_id),
                label: None,
                permissions: UserPermissions::default(),
                sender,
            },
            media_transport,
        )
        .await
        .map_err(|error| anyhow!("user should join through manager: {error:?}"))?;
    Ok((session.connection_id, receiver))
}

pub async fn close_user(
    manager: &RoomManager,
    room: &Arc<Room>,
    raw_user_id: i64,
    connection_id: ConnectionId,
    media_transport: &MediaTransport,
) -> Result<()> {
    manager
        .close_session(
            room.uuid(),
            &UserId::Integer(raw_user_id),
            connection_id,
            media_transport,
        )
        .await
        .then_some(())
        .ok_or_else(|| anyhow!("user session should close"))
}

pub struct ReadyRoom {
    pub room: Arc<Room>,
    pub media_transport: MediaTransport,
    receivers: BTreeMap<i64, UserOutboundReceiver>,
    pub sessions: BTreeMap<i64, MediaSession>,
}

impl ReadyRoom {
    pub fn drain_user(&mut self, raw_user_id: i64) -> Result<()> {
        let receiver = self
            .receivers
            .get_mut(&raw_user_id)
            .ok_or_else(|| anyhow!("ready test user should have an outbound receiver"))?;
        while receiver.try_recv().is_ok() {}
        Ok(())
    }

    pub fn assert_no_outbound(&mut self, raw_user_id: i64) -> Result<()> {
        let receiver = self
            .receivers
            .get_mut(&raw_user_id)
            .ok_or_else(|| anyhow!("ready test user should have an outbound receiver"))?;
        assert!(receiver.try_recv().is_err());
        Ok(())
    }
}

pub async fn join_ready_users(user_ids: &[i64]) -> Result<ReadyRoom> {
    let manager = Arc::new(RoomManager::for_test());
    let room = serve_room(&manager, "issuer-core-room-ready").await?;
    let media_transport = media_transport()?;
    let core = SfuCore::new(media_transport.clone(), Arc::clone(&manager));
    let mut receivers = BTreeMap::new();
    let mut sessions = BTreeMap::new();
    for &raw_user_id in user_ids {
        let (sender, receiver) = test_sender();
        let session = core
            .admit_user(
                room.uuid(),
                JoinUserRequest {
                    user_id: UserId::Integer(raw_user_id),
                    label: None,
                    permissions: UserPermissions::default(),
                    sender,
                },
            )
            .await
            .map_err(|error| anyhow!("user should join through core: {error:?}"))?;
        room.test_api()
            .make_session_ready(session.user_id(), &media_transport)
            .await?;
        receivers.insert(raw_user_id, receiver);
        sessions.insert(raw_user_id, session);
    }
    Ok(ReadyRoom {
        room,
        media_transport,
        receivers,
        sessions,
    })
}

pub async fn home_worker(room: &Room, raw_user_id: i64) -> Option<usize> {
    room.test_api()
        .home_media_worker_id(&UserId::Integer(raw_user_id))
        .await
}

pub async fn router_count(room: &Room) -> usize {
    room.test_api().router_count().await
}

pub fn test_video_rtp_parameters() -> MediaStream {
    sample_video_rtp_parameters(None, 22_222)
}

pub fn test_audio_rtp_parameters() -> MediaStream {
    sample_audio_rtp_parameters(11_111)
}

pub async fn publish_track(
    room: &Arc<Room>,
    user_id: &UserId,
    stream_type: TestSourceKind,
    rtp_parameters: MediaStream,
    media_transport: &MediaTransport,
) -> Result<UserStreamId> {
    room.test_api()
        .publish_track(user_id, stream_type, rtp_parameters, media_transport)
        .await
        .ok_or_else(|| anyhow!("track should publish"))
}

pub async fn publish_audio_and_camera(
    room: &Arc<Room>,
    user_id: &UserId,
    media_transport: &MediaTransport,
) -> Result<()> {
    publish_track(
        room,
        user_id,
        TestSourceKind::AudioDetector,
        test_audio_rtp_parameters(),
        media_transport,
    )
    .await?;
    publish_track(
        room,
        user_id,
        TestSourceKind::ScalableVideo,
        sample_simulcast_video_rtp_parameters(None),
        media_transport,
    )
    .await?;
    Ok(())
}
