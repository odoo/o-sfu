//! Remote-source registration and bounded mailbox setup for tests and benchmarks.

use std::sync::Arc;

use tokio::sync::mpsc;

use super::super::{
    commands::{RemoteSourceControl, RouteControlRequest, RtcWorkerCommand},
    state::{PacketLoopState, relay_registry::RelayTargetId, route_control::PacketLayerGate},
};
use crate::engine::{
    media_transport::{TransportMediaId, TransportSessionKey, TransportSourceKey},
    metrics::RtcMetricsRecorder,
};

pub fn saturated_remote_control(
    source: &TransportSourceKey,
    target_id: RelayTargetId,
) -> (
    mpsc::Sender<RtcWorkerCommand>,
    mpsc::Receiver<RtcWorkerCommand>,
) {
    let (control_tx, control_rx) = mpsc::channel(1);
    assert!(
        control_tx
            .try_send(RtcWorkerCommand::RouteControl {
                request: RouteControlRequest::SetRemoteSourcePacketGate {
                    source: source.clone(),
                    target_id,
                    packet_gate: PacketLayerGate::Open,
                },
                response: None,
            })
            .is_ok()
    );
    (control_tx, control_rx)
}

pub fn register_remote_source_control(
    state: &mut PacketLoopState,
    source: &TransportSourceKey,
    control_tx: mpsc::Sender<RtcWorkerCommand>,
    target_id: RelayTargetId,
    rtc_metrics: Arc<RtcMetricsRecorder>,
) {
    let control = RemoteSourceControl::new(control_tx, target_id, rtc_metrics);
    assert!(state.routes.register_remote_source(source, control).is_ok());
}

pub fn register_saturated_remote_source(
    state: &mut PacketLoopState,
    src_media: TransportMediaId,
    src_key: &TransportSessionKey,
    target_id: RelayTargetId,
    rtc_metrics: Arc<RtcMetricsRecorder>,
) -> mpsc::Receiver<RtcWorkerCommand> {
    let source = TransportSourceKey::new(src_key.clone(), src_media);
    let (control_tx, control_rx) = saturated_remote_control(&source, target_id);
    register_remote_source_control(state, &source, control_tx, target_id, rtc_metrics);
    control_rx
}
