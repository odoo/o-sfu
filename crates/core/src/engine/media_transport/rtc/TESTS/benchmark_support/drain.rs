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
    time::Instant,
};

use str0m::{Input, bwe::Bitrate as Str0mBitrate};
use tokio::sync::mpsc;

use super::super::{
    bootstrap::test_support::ensure_session_rtc_state,
    packet_loop::{drain_relay_packets, forwarded_packet::ForwardedPacket},
    state::{PacketLoopState, RtcSnapshotState, bitrate::BitrateRegistry},
    test_support::{sample_forwarded_packet, test_transport_session_key},
    worker::{PacketLoopBuffers, SessionDrainContext, drain_ready_sessions},
};
use crate::{
    Bitrate,
    engine::{
        UserId,
        media_transport::SourcePolicySignal,
        metrics::{RtcMetricsRecorder, RuntimeMetrics},
    },
};

const SESSION_DRAIN_SESSION_COUNT: u32 = 128;
const SESSION_DRAIN_INITIAL_BITRATE: Bitrate = Bitrate::from_mbps(10);
const SESSION_DRAIN_UPDATED_BITRATE: Bitrate = Bitrate::from_mbps(20);

/// Drains one queued bandwidth change per initialized RTC session.
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
        for session_idx in 0..SESSION_DRAIN_SESSION_COUNT {
            let session_key = test_transport_session_key(
                111,
                0,
                10_000 + u64::from(session_idx),
                UserId::Integer(20_000 + i64::from(session_idx)),
            );
            ensure_session_rtc_state(
                &mut state.users,
                &session_key,
                candidate_addr,
                SESSION_DRAIN_INITIAL_BITRATE,
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
        for key in fixture.state.users.keys().cloned().collect::<Vec<_>>() {
            let session = fixture.state.users.get_mut(&key).unwrap();
            session
                .rtc
                .bwe()
                .reset(Str0mBitrate::bps(SESSION_DRAIN_UPDATED_BITRATE.as_bps()));
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

    /// Verifies every queued estimate changed before its future deadline.
    pub fn assert_drained(&self) {
        assert_eq!(
            self.state.users.len(),
            usize::try_from(SESSION_DRAIN_SESSION_COUNT).unwrap()
        );
        assert!(!self.state.has_dirty_sessions());
        let keys: Vec<_> = self.state.users.keys().cloned().collect();
        let bandwidth = self
            .snapshot_state
            .lock()
            .unwrap()
            .receiver_bandwidth_snapshot(&keys);
        assert_eq!(bandwidth.per_session.len(), keys.len());
        for (key, estimate) in &bandwidth.per_session {
            assert!(*estimate > SESSION_DRAIN_INITIAL_BITRATE);
            assert!(*estimate <= SESSION_DRAIN_UPDATED_BITRATE);
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
    rx: mpsc::Receiver<ForwardedPacket>,
    _tx: mpsc::Sender<ForwardedPacket>,
    buffers: PacketLoopBuffers,
    rtc_metrics: Arc<RtcMetricsRecorder>,
}

impl RelayDrainBenchFixture {
    #[must_use]
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(256);
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
            rx,
            _tx: tx,
            buffers: PacketLoopBuffers::new(),
            rtc_metrics,
        }
    }

    pub fn drain_relay(&mut self) -> usize {
        self.buffers.clear();
        drain_relay_packets(
            &mut self.rx,
            &mut self.buffers.pending_packets,
            256,
            &self.rtc_metrics,
        )
    }
}

impl Default for RelayDrainBenchFixture {
    fn default() -> Self {
        Self::new()
    }
}
