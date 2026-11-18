//! command adapters for worker-local media control

use std::time::Instant;

use o_sfu_telemetry::metrics::RtcMetricsRecorder;

use crate::{
    Bitrate,
    engine::media_transport::{
        ConsumerRouteControlFailure, ConsumerRouteControlOutcome, TransportSourceKey,
        rtc::{
            commands::{
                RouteControlRequest, WorkerMediaControlBatch, WorkerMediaControlBatchOutcome,
            },
            control::bwe,
            recovery::{
                worker_request_consumer_kf, worker_request_remote_kf,
                worker_request_resumed_video_kf,
            },
            state::PacketLoopState,
        },
    },
};

fn map_updates<T, R>(updates: Vec<(usize, T)>, mut apply: impl FnMut(T) -> R) -> Vec<R> {
    updates.into_iter().map(|(_, value)| apply(value)).collect()
}

pub fn apply_route_control_request(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    now: Instant,
    request: RouteControlRequest,
) {
    match request {
        RouteControlRequest::AddRelayTarget {
            source,
            target,
            packet_gate,
        } => {
            if is_local_producer(state, &source) {
                state.routes.add_relay_target(&source, target, packet_gate);
            }
        }
        RouteControlRequest::RemoveRelayTarget { source, target_id } => {
            if is_local_producer(state, &source) {
                state
                    .routes
                    .remove_relay_target(source.transport_media_id(), target_id);
            }
        }
        RouteControlRequest::SetRemoteSourceActivity { source, update } => {
            // Nothing waits for this answer. A stale or misaddressed one
            // changes no route.
            state.set_remote_source_activity(&source, update);
        }
        RouteControlRequest::RequestRemoteKeyframe {
            source,
            target_id,
            rid,
            kind,
        } => worker_request_remote_kf(state, metrics, &source, target_id, rid, kind, now),
        RouteControlRequest::SetRemoteSourcePacketGate {
            source,
            target_id,
            packet_gate,
        } => {
            if is_local_producer(state, &source) {
                state.routes.set_relay_pkt_gate(
                    source.transport_media_id(),
                    target_id,
                    packet_gate,
                );
            }
        }
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
            let accepted = state.apply_producer_activity(&control.source, control.update)?;
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

/// Relay control from consumer workers can lag producer removal. Commands for
/// a missing producer are stale and change nothing.
fn is_local_producer(state: &PacketLoopState, source: &TransportSourceKey) -> bool {
    state
        .ensure_local_producer_mid(source.session_key(), source.transport_media_id())
        .is_ok()
}
