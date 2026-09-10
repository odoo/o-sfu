#![allow(
    clippy::unwrap_used,
    clippy::panic,
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
        media_transport::{SourcePolicySignal, TransportSessionKey},
        metrics::{RtcMetricsRecorder, RuntimeMetrics},
    },
};

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
        let session_count = 128_u32;

        for session_idx in 0..session_count {
            let u64_idx = u64::from(session_idx);
            let i64_idx = i64::from(session_idx);
            let session_key = test_transport_session_key(
                111,
                0,
                10_000 + u64_idx,
                UserId::Integer(20_000 + i64_idx),
            );
            let _ = ensure_session_rtc_state(
                &mut state.users,
                &session_key,
                candidate_addr,
                Bitrate::from_mbps(10),
            );
            state.mark_session_dirty(&session_key);
        }

        Self {
            state,
            snapshot_state: Arc::new(Mutex::new(RtcSnapshotState::default())),
            metrics,
            rtc_metrics,
            bitrate_registry: Arc::new(Mutex::new(BitrateRegistry::default())),
            source_policy_signal: SourcePolicySignal::default(),
            buffers: PacketLoopBuffers::new(),
            now: Instant::now(),
        }
    }

    pub fn drain_sessions(&mut self) -> usize {
        for session_key in self.state.users.keys().cloned().collect::<Vec<_>>() {
            self.state.mark_session_dirty(&session_key);
        }

        self.buffers.clear();
        let context = SessionDrainContext::new(
            &self.snapshot_state,
            &self.bitrate_registry,
            &self.metrics,
            &self.rtc_metrics,
            &self.source_policy_signal,
        );
        let _ = drain_ready_sessions(&mut self.state, &context, &mut self.buffers, self.now);
        self.buffers.pending_packets.len()
    }
}

impl Default for SessionDrainBenchFixture {
    fn default() -> Self {
        Self::new()
    }
}

pub struct RelayDrainBenchFixture {
    rx: mpsc::Receiver<ForwardedPacket>,
    tx: mpsc::Sender<ForwardedPacket>,
    buffers: PacketLoopBuffers,
    rtc_metrics: Arc<RtcMetricsRecorder>,
    source_session: TransportSessionKey,
}

impl RelayDrainBenchFixture {
    #[must_use]
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(256);
        let source_session = test_transport_session_key(2, 0, 3, UserId::Integer(4));
        let metrics = RuntimeMetrics::default();
        let rtc_metrics = metrics.register_rtc_worker();

        Self {
            rx,
            tx,
            buffers: PacketLoopBuffers::new(),
            rtc_metrics,
            source_session,
        }
    }

    pub fn drain_relay(&mut self) -> usize {
        while self
            .tx
            .try_send(sample_forwarded_packet(
                self.source_session.clone(),
                "cam-up",
                b"payload",
            ))
            .is_ok()
        {}

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
