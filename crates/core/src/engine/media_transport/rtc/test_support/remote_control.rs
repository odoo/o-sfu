//! Remote-source registration and bounded mailbox setup for tests and benchmarks.

use std::sync::Arc;

use o_sfu_telemetry::metrics::RtcMetricsRecorder;
use tokio::sync::mpsc;

#[cfg(test)]
use super::super::{commands::RouteControlRequest, state::route_control::PacketLayerGate};
use super::super::{
    commands::{RelayTarget, RemoteSourceControl, RtcWorkerCommand},
    state::{
        PacketLoopState,
        relay_registry::{RelayPacketMailbox, RelayTargetId},
    },
};
use crate::engine::media_transport::{TransportMediaId, TransportSessionKey, TransportSourceKey};

/// Builds a consumer-side control path whose own mailbox and queue no source
/// worker reads.
pub fn remote_source_control(
    control_tx: mpsc::Sender<RtcWorkerCommand>,
    target_id: RelayTargetId,
    rtc_metrics: Arc<RtcMetricsRecorder>,
) -> RemoteSourceControl {
    let (mailbox_tx, _mailbox_rx) = mpsc::channel(1);
    RemoteSourceControl::new(
        control_tx,
        RelayTarget::with_control_queue(target_id, RelayPacketMailbox::new(mailbox_tx), 1).0,
        rtc_metrics,
    )
}

#[cfg(test)]
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
            .try_send(RtcWorkerCommand::RouteControl(
                RouteControlRequest::SetRemoteSourcePacketGate {
                    source: source.clone(),
                    target_id,
                    packet_gate: PacketLayerGate::Open,
                }
            ))
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
    let control = remote_source_control(control_tx, target_id, rtc_metrics);
    assert!(state.routes.register_remote_source(source, control).is_ok());
}

/// Registers a remote source whose relay target add fills the source queue,
/// so every later gate waits for retry.
pub fn register_saturated_remote_source(
    state: &mut PacketLoopState,
    src_media: TransportMediaId,
    src_key: &TransportSessionKey,
    target_id: RelayTargetId,
    rtc_metrics: Arc<RtcMetricsRecorder>,
) -> mpsc::Receiver<RtcWorkerCommand> {
    let source = TransportSourceKey::new(src_key.clone(), src_media);
    let (control_tx, control_rx) = mpsc::channel(1);
    register_remote_source_control(state, &source, control_tx, target_id, rtc_metrics);
    control_rx
}
