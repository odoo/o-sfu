#![allow(
    clippy::unwrap_used,
    clippy::missing_panics_doc,
    clippy::cast_lossless,
    clippy::as_conversions,
    reason = "benchmark-only fixtures require standard test helper unwraps and conversions"
)]

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use str0m::Input;
use tokio::sync::mpsc;

use super::super::{
    RtpProfile,
    bootstrap::ensure_session_rtc_state,
    packet_loop::{drain_relay_packets, forwarded_packet::ForwardedPacket},
    state::{PacketLoopState, RtcSnapshotState, bitrate::BitrateRegistry},
    test_support::{sample_forwarded_packet, test_transport_session_key},
    worker::{PacketLoopBuffers, SessionDrainContext, drain_ready_sessions},
};
use crate::{
    Bitrate, CodecPreferences, MediaCodecFlags,
    engine::{
        UserId,
        media_transport::SourcePolicySignal,
        metrics::{RtcMetricsRecorder, RuntimeMetrics},
    },
};

const SESSION_DRAIN_SESSION_COUNT: u32 = 128;
const SESSION_DRAIN_STATS_INTERVAL: Duration = Duration::from_secs(1);

/// Drains one queued peer-statistics event per initialized RTC session.
///
/// Setup consumes initial RTC output. Keeping its clock fixed excludes
/// randomized DTLS retry deadlines from the measured drain.
pub struct SessionDrainBenchFixture {
    state: PacketLoopState,
    snapshot_state: Arc<Mutex<RtcSnapshotState>>,
    metrics: RuntimeMetrics,
    rtc_metrics: Arc<RtcMetricsRecorder>,
    bitrate_registry: Arc<Mutex<BitrateRegistry>>,
    source_policy_signal: SourcePolicySignal,
    buffers: PacketLoopBuffers,
    now: Instant,
}

impl SessionDrainBenchFixture {
    #[must_use]
    pub fn new() -> Self {
        let mut state = PacketLoopState::default();
        let metrics = RuntimeMetrics::default();
        let rtc_metrics = metrics.register_rtc_worker();
        let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 46_300));
        let profile =
            RtpProfile::compile(MediaCodecFlags::default(), CodecPreferences::default()).unwrap();
        for session_idx in 0..SESSION_DRAIN_SESSION_COUNT {
            let session_key = test_transport_session_key(
                111,
                0,
                10_000 + u64::from(session_idx),
                UserId::Integer(20_000 + i64::from(session_idx)),
            );
            ensure_session_rtc_state(
                &mut state.users,
                Arc::from("test-room"),
                &session_key,
                candidate_addr,
                Bitrate::from_mbps(10),
                &profile,
                Some(SESSION_DRAIN_STATS_INTERVAL),
            )
            .unwrap();
            state.mark_session_dirty(&session_key);
        }
        let mut fixture = Self {
            state,
            snapshot_state: Arc::new(Mutex::new(RtcSnapshotState::default())),
            metrics,
            rtc_metrics,
            bitrate_registry: Arc::new(Mutex::new(BitrateRegistry::default())),
            source_policy_signal: SourcePolicySignal::default(),
            buffers: PacketLoopBuffers::new(),
            now: Instant::now(),
        };
        fixture.drain_sessions();
        // Peer stats remain observable before SRTP setup, unlike TWCC estimates.
        fixture.now += SESSION_DRAIN_STATS_INTERVAL;
        for key in fixture.state.users.keys().cloned().collect::<Vec<_>>() {
            let session = fixture.state.users.get_mut(&key).unwrap();
            session
                .rtc
                .handle_input(Input::Timeout(fixture.now))
                .unwrap();
            fixture.state.mark_session_dirty(&key);
        }
        fixture
    }

    pub fn drain_sessions(&mut self) {
        self.buffers.clear();
        let context = SessionDrainContext::new(
            &self.snapshot_state,
            &self.bitrate_registry,
            &self.metrics,
            &self.rtc_metrics,
            &self.source_policy_signal,
        );
        let _ = drain_ready_sessions(&mut self.state, &context, &mut self.buffers, self.now);
    }

    /// Verifies every queued peer sample was consumed before its future deadline.
    pub fn assert_drained(&self) {
        assert_eq!(
            self.state.users.len(),
            usize::try_from(SESSION_DRAIN_SESSION_COUNT).unwrap()
        );
        assert!(!self.state.has_dirty_sessions());
        let keys: Vec<_> = self.state.users.keys().cloned().collect();
        let quality = self
            .snapshot_state
            .lock()
            .unwrap()
            .transport_quality_snapshot(&keys);
        assert_eq!(quality.len(), keys.len());
        for (key, sample) in &quality {
            assert_eq!(sample.sample_count, 1);
            let session = self.state.users.get(key).unwrap();
            assert!(!session.packet_loop_dirty);
            assert!(
                session
                    .next_timeout
                    .is_some_and(|deadline| deadline > self.now)
            );
        }
    }
}

impl Default for SessionDrainBenchFixture {
    fn default() -> Self {
        Self::new()
    }
}

pub struct RelayDrainBenchFixture {
    packet_count: usize,
    rx: mpsc::Receiver<ForwardedPacket>,
    tx: mpsc::Sender<ForwardedPacket>,
    buffers: PacketLoopBuffers,
    rtc_metrics: Arc<RtcMetricsRecorder>,
}

impl RelayDrainBenchFixture {
    #[must_use]
    pub fn new() -> Self {
        Self::with_packets(256)
    }

    fn with_packets(packet_count: usize) -> Self {
        let (tx, rx) = mpsc::channel(packet_count);
        let source_session = test_transport_session_key(2, 0, 3, UserId::Integer(4));
        let metrics = RuntimeMetrics::default();
        let rtc_metrics = metrics.register_rtc_worker();
        while tx
            .try_send(sample_forwarded_packet(
                source_session.clone(),
                "cam-up",
                b"payload",
            ))
            .is_ok()
        {}

        Self {
            packet_count,
            rx,
            tx,
            buffers: PacketLoopBuffers::new(),
            rtc_metrics,
        }
    }

    /// Retains staging capacity after one complete relay burst.
    #[must_use]
    pub fn warmed(packet_count: usize) -> Self {
        let mut fixture = Self::with_packets(packet_count);
        assert_eq!(fixture.drain_relay(), packet_count);
        for packet in fixture.buffers.pending_packets.drain(..) {
            fixture.tx.try_send(packet).unwrap();
        }
        fixture
    }

    #[must_use]
    pub fn staging_capacity(&self) -> usize {
        self.buffers.pending_packets.capacity()
    }

    pub fn assert_drained(&self) {
        assert_eq!(self.buffers.pending_packets.len(), self.packet_count);
        assert!(self.rx.is_empty());
    }

    pub fn drain_relay(&mut self) -> usize {
        self.buffers.clear();
        drain_relay_packets(
            &mut self.rx,
            &mut self.buffers.pending_packets,
            self.packet_count,
            &self.rtc_metrics,
        )
    }
}

impl Default for RelayDrainBenchFixture {
    fn default() -> Self {
        Self::new()
    }
}
