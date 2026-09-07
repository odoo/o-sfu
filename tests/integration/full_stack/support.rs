#[path = "support/flows.rs"]
pub(crate) mod flows;
#[path = "support/media.rs"]
pub(crate) mod media;
#[path = "support/metrics.rs"]
pub(crate) mod metrics;
#[path = "support/protocol.rs"]
pub(crate) mod protocol;
#[path = "support/setup.rs"]
pub(crate) mod setup;
#[path = "support/spillover.rs"]
pub(crate) mod spillover;

pub(super) use std::time::{Duration, Instant};
use std::{
    cmp::Ordering,
    num::{NonZeroU64, NonZeroUsize},
};

pub(super) use o_sfu::{
    config::{Config, MediaCodecFlags, RoomMediaLimits, RoomWorkerPolicy},
    http::IncomingBitRateStatsResponse,
};
pub(super) use o_sfu_protocol::wire::{
    ClientRequest, DownloadStates, RecordingOptions, ServerMessage, ServerRequest, StreamType,
    TrackBinding, UserId, UserInfo, VideoLayoutIntent,
};
pub(super) use o_sfu_rfc::rtp::CodecName;
pub(super) use o_sfu_telemetry::diagnostics::{
    DiagnosticsActiveSpeakerReason, DiagnosticsActiveSpeakerState, DiagnosticsVideoLayoutRole,
};
pub(super) use o_sfu_tests::support::{
    TEST_ROOM_KEY, TestResult, TestServer, create_room,
    fake_media::{
        FakeClock, FakeMediaSource, SyntheticH264Stream, SyntheticOpusStream, SyntheticVp8Stream,
        project_synthetic_vp8_payload,
    },
    fake_rtc_peer::{
        DroppedRtpPacket, ReceivedRtpPacket, RtcPeerTrace, RtcTraceDirection, TracedRtpPacket,
    },
    metrics_text,
    protocol_full_stack::{
        ProtocolFakePeer, connect_fake_peer, connect_ridless_video_fake_peer,
        connect_two_fake_peers, connect_two_rtc_ready_fake_peers,
    },
    require_some, spawn_room_server_with_config, spawn_test_server, stats, test_config,
};
pub(super) use tokio::{
    join,
    sync::{Mutex, MutexGuard},
    task::yield_now,
    time::{sleep, timeout},
};
pub(super) use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

pub(super) fn spillover_policy(max_local_routers: usize) -> RoomWorkerPolicy {
    let Some(max_local_routers) = NonZeroUsize::new(max_local_routers) else {
        panic!("test router cap should be positive");
    };
    let Some(delay_threshold) =
        NonZeroU64::new(RoomWorkerPolicy::DEFAULT_PACKET_LOOP_DELAY_THRESHOLD_MS)
    else {
        panic!("default delay threshold should be positive");
    };
    RoomWorkerPolicy::new(max_local_routers, delay_threshold)
}

pub(super) fn placement_delays_for_worker(
    worker_count: usize,
    target_worker: usize,
) -> Vec<Option<u64>> {
    (0..worker_count)
        .map(|worker| match worker.cmp(&target_worker) {
            Ordering::Less => Some(RoomWorkerPolicy::DEFAULT_PACKET_LOOP_DELAY_THRESHOLD_MS),
            Ordering::Equal => Some(0),
            Ordering::Greater => None,
        })
        .collect()
}
