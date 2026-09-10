#![allow(
    clippy::panic,
    reason = "media worker fixtures use panic only for mandatory setup failures"
)]

use std::{sync::Arc, time::Instant};

use str0m::media::{Mid, Rid};
use tokio::sync::mpsc;

use super::super::apply_media_control_batch;
use crate::{
    Bitrate,
    engine::{
        media_transport::{
            ConsumerRouteControl, ProducerActivity, SourceActivityRevision, SourceActivityUpdate,
            TransportConsumerRoute, TransportMediaId, TransportSessionKey, TransportSourceKey,
            rtc::{
                commands::{
                    RemoteSourceControl, RtcWorkerCommand, WorkerMediaControlBatch,
                    WorkerMediaControlBatchOutcome,
                },
                recovery::observe_src_rid_ready,
                state::{
                    PacketLoopState, relay_registry::RelayTargetId, route_control::PacketLayerGate,
                },
                test_support::{
                    add_source_rid_stream, assert_consumer_packet_gate, drain_ready_sessions,
                    install_video_route_with_gate, prepare_source_session_with_rid,
                    test_consumer_session_key, test_consumer_session_key_on_worker,
                    test_source_session_key,
                },
            },
        },
        metrics::{RtcMetricsRecorder, RuntimeMetrics},
    },
};

const SOURCE_MID: &str = "cam-up";
const CONSUMER_MID: &str = "cam-down";

pub(super) fn set_consumer_packet_gate_at(
    state: &mut PacketLoopState,
    route: &TransportConsumerRoute,
    packet_gate: PacketLayerGate,
    now: Instant,
) {
    let rtc_metrics = RuntimeMetrics::default().register_rtc_worker();
    let _ = apply_media_control_batch(
        state,
        &rtc_metrics,
        Bitrate::from_mbps(10),
        now,
        WorkerMediaControlBatch::ConsumerGates {
            source: route.source().clone(),
            updates: vec![(0, route.clone(), packet_gate)],
        },
    );
}

pub(super) struct LocalVideoRoute {
    pub state: PacketLoopState,
    pub metrics: RuntimeMetrics,
    pub rtc_metrics: Arc<RtcMetricsRecorder>,
    pub source_session: TransportSessionKey,
    pub consumer_session: TransportSessionKey,
    pub src_media: TransportMediaId,
    pub consumer_media: TransportMediaId,
}

impl LocalVideoRoute {
    pub fn new(seed: u64, ssrc: u32) -> Self {
        Self::build(seed, ssrc, None, PacketLayerGate::Open)
    }

    pub fn with_rid(seed: u64, ssrc: u32, rid: Rid) -> Self {
        Self::build(seed, ssrc, Some(rid), PacketLayerGate::Open)
    }

    pub fn with_rid_gate(seed: u64, ssrc: u32, rid: Rid, packet_gate: PacketLayerGate) -> Self {
        Self::build(seed, ssrc, Some(rid), packet_gate)
    }

    fn build(seed: u64, ssrc: u32, rid: Option<Rid>, packet_gate: PacketLayerGate) -> Self {
        let source_session = test_source_session_key(seed);
        let consumer_session = test_consumer_session_key(seed);
        let mut state = PacketLoopState::default();
        let metrics = RuntimeMetrics::default();
        let rtc_metrics = metrics.register_rtc_worker();
        let src_media = prepare_source_session_with_rid(
            &mut state,
            &source_session,
            Mid::from(SOURCE_MID),
            ssrc,
            rid,
        );
        let consumer_media = install_video_route_with_gate(
            &mut state,
            src_media,
            &consumer_session,
            Mid::from(CONSUMER_MID),
            packet_gate,
        );
        Self {
            state,
            metrics,
            rtc_metrics,
            source_session,
            consumer_session,
            src_media,
            consumer_media,
        }
    }

    pub fn request_kf(&mut self) {
        let route = self.consumer_route();
        let _ = apply_media_control_batch(
            &mut self.state,
            &self.rtc_metrics,
            Bitrate::from_mbps(10),
            Instant::now(),
            WorkerMediaControlBatch::ConsumerFollowUp(vec![(
                0,
                ConsumerRouteControl::new(route).request_keyframe(true),
            )]),
        );
    }

    pub fn assert_source_ready(&mut self) {
        assert_eq!(
            drain_ready_sessions(&mut self.state),
            vec![self.source_session.clone()]
        );
    }

    pub fn consumer_route(&self) -> TransportConsumerRoute {
        TransportConsumerRoute::new(
            self.consumer_session.clone(),
            self.consumer_media,
            TransportSourceKey::new(self.source_session.clone(), self.src_media),
        )
    }

    pub fn observe_rid_ready(&mut self, rid: Rid, keyframe: bool, now: Instant) -> bool {
        observe_src_rid_ready(
            &mut self.state,
            &self.rtc_metrics,
            &self.source_session,
            self.src_media,
            rid,
            keyframe,
            now,
        )
    }

    pub fn assert_packet_gate(
        &self,
        packet_gate: PacketLayerGate,
        pending_gate: Option<PacketLayerGate>,
    ) {
        assert_consumer_packet_gate(
            &self.state,
            self.src_media,
            &self.consumer_session,
            &packet_gate,
            pending_gate.as_ref(),
        );
    }
}

pub(super) struct RemoteVideoRoute {
    pub state: PacketLoopState,
    pub metrics: RuntimeMetrics,
    pub rtc_metrics: Arc<RtcMetricsRecorder>,
    pub source_session: TransportSessionKey,
    pub consumer_session: TransportSessionKey,
    pub src_media: TransportMediaId,
    pub consumer_media: TransportMediaId,
    pub target_id: RelayTargetId,
    pub control_rx: mpsc::Receiver<RtcWorkerCommand>,
}

impl RemoteVideoRoute {
    pub fn new(seed: u64, src_media: u64, target_id: u64) -> Self {
        Self::with_gate(seed, src_media, target_id, PacketLayerGate::Open)
    }

    pub fn with_gate(
        seed: u64,
        src_media: u64,
        target_id: u64,
        packet_gate: PacketLayerGate,
    ) -> Self {
        let source_session = test_source_session_key(seed);
        let consumer_session = test_consumer_session_key_on_worker(seed, 1);
        let src_media = TransportMediaId::new(src_media);
        let target_id = RelayTargetId::new(target_id);
        let mut state = PacketLoopState::default();
        let metrics = RuntimeMetrics::default();
        let rtc_metrics = metrics.register_rtc_worker();
        let (control_tx, control_rx) = mpsc::channel(1);
        let source = TransportSourceKey::new(source_session.clone(), src_media);
        assert!(
            state
                .routes
                .register_remote_source(
                    &source,
                    RemoteSourceControl::new(control_tx, target_id, Arc::clone(&rtc_metrics)),
                )
                .is_ok()
        );
        assert_eq!(
            state.routes.apply_source_activity(
                src_media,
                SourceActivityUpdate::new(
                    ProducerActivity::Active,
                    SourceActivityRevision::default(),
                ),
            ),
            Ok(true)
        );
        let consumer_media = install_video_route_with_gate(
            &mut state,
            src_media,
            &consumer_session,
            Mid::from(CONSUMER_MID),
            packet_gate,
        );
        Self {
            state,
            metrics,
            rtc_metrics,
            source_session,
            consumer_session,
            src_media,
            consumer_media,
            target_id,
            control_rx,
        }
    }

    pub fn request_kf(&mut self) -> WorkerMediaControlBatchOutcome {
        let route = TransportConsumerRoute::new(
            self.consumer_session.clone(),
            self.consumer_media,
            TransportSourceKey::new(self.source_session.clone(), self.src_media),
        );
        apply_media_control_batch(
            &mut self.state,
            &self.rtc_metrics,
            Bitrate::from_mbps(10),
            Instant::now(),
            WorkerMediaControlBatch::ConsumerFollowUp(vec![(
                0,
                ConsumerRouteControl::new(route).request_keyframe(true),
            )]),
        )
    }
}

pub(super) struct PendingSelectedRidRoute {
    pub state: PacketLoopState,
    pub metrics: RuntimeMetrics,
    pub rtc_metrics: Arc<RtcMetricsRecorder>,
    pub source_session: TransportSessionKey,
    pub consumer_session: TransportSessionKey,
    pub src_media: TransportMediaId,
    pub selected_rid: Rid,
    pub fallback_rid: Rid,
}

impl PendingSelectedRidRoute {
    pub fn observe_rid_ready(&mut self, rid: Rid, keyframe: bool, now: Instant) -> bool {
        observe_src_rid_ready(
            &mut self.state,
            &self.rtc_metrics,
            &self.source_session,
            self.src_media,
            rid,
            keyframe,
            now,
        )
    }

    pub fn assert_packet_gate(
        &self,
        packet_gate: PacketLayerGate,
        pending_gate: Option<PacketLayerGate>,
    ) {
        assert_consumer_packet_gate(
            &self.state,
            self.src_media,
            &self.consumer_session,
            &packet_gate,
            pending_gate.as_ref(),
        );
    }
}

pub(super) fn prepare_pending_selected_rid_route() -> PendingSelectedRidRoute {
    let source_session = test_source_session_key(231);
    let consumer_session = test_consumer_session_key(231);
    let source_mid = Mid::from(SOURCE_MID);
    let consumer_mid = Mid::from(CONSUMER_MID);
    let selected_rid = Rid::from("hi");
    let fallback_rid = Rid::from("lo");
    let mut state = PacketLoopState::default();
    let metrics = RuntimeMetrics::default();
    let rtc_metrics = metrics.register_rtc_worker();
    let src_media = prepare_source_session_with_rid(
        &mut state,
        &source_session,
        source_mid,
        88_301,
        Some(selected_rid),
    );
    add_source_rid_stream(
        &mut state,
        &source_session,
        source_mid,
        88_302,
        fallback_rid,
    );
    let consumer_media = install_video_route_with_gate(
        &mut state,
        src_media,
        &consumer_session,
        consumer_mid,
        PacketLayerGate::Open,
    );
    let route = TransportConsumerRoute::new(
        consumer_session.clone(),
        consumer_media,
        TransportSourceKey::new(source_session.clone(), src_media),
    );
    set_consumer_packet_gate_at(
        &mut state,
        &route,
        PacketLayerGate::Rid(selected_rid),
        Instant::now(),
    );
    PendingSelectedRidRoute {
        state,
        metrics,
        rtc_metrics,
        source_session,
        consumer_session,
        src_media,
        selected_rid,
        fallback_rid,
    }
}
