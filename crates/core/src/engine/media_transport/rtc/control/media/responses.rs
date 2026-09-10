//! command adapters for worker-local media control

use std::time::Instant;

use super::routes;
use crate::{
    Bitrate,
    engine::{
        media_transport::{
            ConsumerRouteControlFailure, ConsumerRouteControlOutcome,
            rtc::{
                commands::{
                    RouteControlRequest, RtcWorkerResponse, WorkerMediaControlBatch,
                    WorkerMediaControlBatchOutcome,
                },
                control::bwe,
                recovery::{
                    worker_request_consumer_kf, worker_request_remote_kf,
                    worker_request_resumed_video_kf,
                },
                state::PacketLoopState,
            },
        },
        metrics::RtcMetricsRecorder,
    },
};

fn map_updates<T, R>(updates: Vec<(usize, T)>, mut apply: impl FnMut(T) -> R) -> Vec<R> {
    updates.into_iter().map(|(_, value)| apply(value)).collect()
}

pub fn apply_route_control_request(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    request: RouteControlRequest,
    response: Option<RtcWorkerResponse<()>>,
) {
    let result = match request {
        RouteControlRequest::AddRelayTarget {
            source,
            target_id,
            target,
        } => routes::worker_add_relay_target(state, &source, target_id, target),
        RouteControlRequest::RemoveRelayTarget { source, target_id } => {
            routes::worker_remove_relay_target(state, &source, target_id)
        }
        RouteControlRequest::SetRelayTargetActive {
            source,
            target_id,
            active,
        } => routes::worker_set_relay_target_active(state, &source, target_id, active),
        RouteControlRequest::SetRemoteSourceActivity { source, update } => {
            routes::worker_set_remote_source_activity(state, &source, update)
        }
        RouteControlRequest::RequestRemoteKeyframe {
            source,
            target_id,
            rid,
            kind,
        } => {
            worker_request_remote_kf(state, metrics, &source, target_id, rid, kind);
            Ok(())
        }
        RouteControlRequest::SetRemoteSourcePacketGate {
            source,
            target_id,
            packet_gate,
        } => {
            routes::set_remote_src_pkt_gate(state, &source, target_id, packet_gate);
            Ok(())
        }
    };
    if let Some(response) = response {
        let _ = response.send(result);
    }
}

pub fn apply_media_control_batch(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    max_bitrate_out: Bitrate,
    now: Instant,
    batch: WorkerMediaControlBatch,
) -> WorkerMediaControlBatchOutcome {
    use WorkerMediaControlBatch::{ConsumerFollowUp, ConsumerGates, ProducerActivity, ReceiverBwe};
    use WorkerMediaControlBatchOutcome::{Applied, Consumers};

    match batch {
        ReceiverBwe(updates) => Applied(map_updates(updates, |update| {
            bwe::apply_receiver_bwe_target(&mut state.users, max_bitrate_out, &update)
        })),
        ProducerActivity(updates) => Applied(map_updates(updates, |control| {
            let accepted =
                routes::worker_apply_producer_activity(state, &control.source, control.update)?;
            if accepted && control.update.activity().is_active() {
                worker_request_resumed_video_kf(state, metrics, &control.source, now);
            }
            Ok(())
        })),
        ConsumerGates { source, updates } => {
            let updates = updates
                .into_iter()
                .map(|(_, route, packet_gate)| (route, packet_gate));
            Applied(state.set_consumer_packet_gates(&source, updates))
        }
        ConsumerFollowUp(updates) => Consumers(map_updates(updates, |control| {
            if let Some(activity) = control.activity
                && let Err(error) = state.set_consumer_active(&control.route, activity.is_active())
            {
                return ConsumerRouteControlOutcome(Some(ConsumerRouteControlFailure::Activity(
                    error,
                )));
            }
            if control.request_keyframe
                && let Err(error) = worker_request_consumer_kf(state, metrics, &control.route)
            {
                return ConsumerRouteControlOutcome(Some(ConsumerRouteControlFailure::Keyframe(
                    error,
                )));
            }
            ConsumerRouteControlOutcome::default()
        })),
    }
}
