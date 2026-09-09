use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use str0m::media::{KeyframeRequestKind, Mid, Rid};
use tokio::sync::mpsc;

use super::super::{
    commands::{RemoteSourceControl, RtcWorkerCommand},
    recovery::{PendingKeyframeRequest, apply_src_decoder_ready, flush_pending_kf_reqs_at},
    state::{PacketLoopState, relay_registry::RelayTargetId, route_control::PacketLayerGate},
    test_support::{MediaWorkerScenario, test_transport_session_key},
    worker::PacketLoopBuffers,
};
use crate::engine::{
    UserId,
    media_transport::{
        ProducerActivity, SourceActivityRevision, SourceActivityUpdate, TransportMediaId,
        TransportSessionKey, TransportSourceKey,
    },
    metrics::{RtcMetricsRecorder, RuntimeMetrics},
};

pub const SELECTED_RID_DESTINATIONS: usize = 256;
pub const KEYFRAME_COALESCING_REQUESTS: usize = 512;
const KEYFRAME_COALESCING_REQUESTS_I64: i64 = 512;

/// fixed selected-RID route-control fixture for readiness benchmarks
///
/// setup creates one remote source with many local destinations blocked on the
/// same selected RID
/// the measured method records one RID packet and applies the production
/// readiness transition that activates the pending gates as a coalesced source
/// update
pub struct RidReadinessBenchFixture {
    state: PacketLoopState,
    source_session: TransportSessionKey,
    src_media: TransportMediaId,
    rid: Rid,
    now: Instant,
    rtc_metrics: Arc<RtcMetricsRecorder>,
    _control_rx: mpsc::Receiver<RtcWorkerCommand>,
}

impl RidReadinessBenchFixture {
    #[must_use]
    pub fn pending_selected_rid() -> Self {
        let source_session = test_transport_session_key(81, 1, 82, UserId::Integer(83));
        let consumer_session = test_transport_session_key(81, 1, 84, UserId::Integer(85));
        let src_media = TransportMediaId::new(8_100);
        let rid = Rid::from("hi");
        let now = fixed_now();
        let mut state = PacketLoopState::default();
        let rtc_metrics = RuntimeMetrics::default().register_rtc_worker();
        let (control_tx, control_rx) = mpsc::channel(8);
        let source = TransportSourceKey::new(source_session.clone(), src_media);
        let _ = state.routes.register_remote_source(
            &source,
            RemoteSourceControl::new(control_tx, RelayTargetId::new(1), Arc::clone(&rtc_metrics)),
        );
        let _ = state.routes.apply_source_activity(
            src_media,
            SourceActivityUpdate::new(ProducerActivity::Active, SourceActivityRevision::default()),
        );

        let mut scenario = MediaWorkerScenario::new(&mut state);
        for destination_idx in 0..SELECTED_RID_DESTINATIONS {
            let mid = Mid::from(format!("cam-down-{destination_idx}").as_str());
            scenario.destination_with_pending_gate(
                src_media,
                consumer_session.clone(),
                mid,
                PacketLayerGate::Rid(rid),
            );
        }

        Self {
            state,
            source_session,
            src_media,
            rid,
            now,
            rtc_metrics,
            _control_rx: control_rx,
        }
    }

    #[must_use]
    pub fn activate_selected_rid(&mut self) -> usize {
        let first_observed = self.state.routes.observe_producer_packet(
            self.src_media,
            Some(self.rid),
            false,
            self.now,
        );
        let changed = apply_src_decoder_ready(
            &mut self.state,
            &self.rtc_metrics,
            &self.source_session,
            self.src_media,
            Some(self.rid),
            true,
            self.now,
        );
        usize::from(first_observed) + usize::from(changed)
    }
}

/// fixed keyframe-feedback fixture for coalescing benchmarks
///
/// setup stages many consumer-local feedback requests for one remote source
/// the measured method resolves, sorts and coalesces them before sending one
/// producer-side request through the normal source-control path
pub struct KeyframeCoalescingBenchFixture {
    state: PacketLoopState,
    buffers: PacketLoopBuffers,
    rtc_metrics: Arc<RtcMetricsRecorder>,
    _control_rx: mpsc::Receiver<RtcWorkerCommand>,
}

impl KeyframeCoalescingBenchFixture {
    #[must_use]
    pub fn remote_source_requests() -> Self {
        let source_session = test_transport_session_key(91, 1, 92, UserId::Integer(93));
        let src_media = TransportMediaId::new(9_100);
        let mut state = PacketLoopState::default();
        let mut buffers = PacketLoopBuffers::new();
        let rtc_metrics = RuntimeMetrics::default().register_rtc_worker();
        let (control_tx, control_rx) = mpsc::channel(1);
        let source = TransportSourceKey::new(source_session, src_media);
        let _ = state.routes.register_remote_source(
            &source,
            RemoteSourceControl::new(control_tx, RelayTargetId::new(2), Arc::clone(&rtc_metrics)),
        );
        let _ = state.routes.apply_source_activity(
            src_media,
            SourceActivityUpdate::new(ProducerActivity::Active, SourceActivityRevision::default()),
        );

        let mut scenario = MediaWorkerScenario::new(&mut state);
        for request_idx in 0..KEYFRAME_COALESCING_REQUESTS_I64 {
            let connection_offset = u64::try_from(request_idx).unwrap_or(0);
            let consumer_session = test_transport_session_key(
                91,
                1,
                10_000 + connection_offset,
                UserId::Integer(20_000 + request_idx),
            );
            let mid = Mid::from(format!("cam-down-{request_idx}").as_str());
            scenario.destination(src_media, consumer_session.clone(), mid);
            let kind = if request_idx == KEYFRAME_COALESCING_REQUESTS_I64 - 1 {
                KeyframeRequestKind::Fir
            } else {
                KeyframeRequestKind::Pli
            };
            buffers.pending_keyframe_requests.push((
                consumer_session,
                PendingKeyframeRequest::benchmark_request(mid, None, kind),
            ));
        }

        Self {
            state,
            buffers,
            rtc_metrics,
            _control_rx: control_rx,
        }
    }

    #[must_use]
    pub fn flush_requests(&mut self) -> usize {
        flush_pending_kf_reqs_at(
            &mut self.state,
            &self.rtc_metrics,
            &mut self.buffers.pending_keyframe_requests,
            &mut self.buffers.coalesced_keyframe_requests,
            fixed_now(),
        );
        usize::from(self.buffers.pending_keyframe_requests.is_empty())
    }
}

fn fixed_now() -> Instant {
    Instant::now() + Duration::from_secs(1)
}
