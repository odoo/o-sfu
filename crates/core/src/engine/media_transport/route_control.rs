use std::{collections::BTreeMap, mem::take};

use tracing::{debug, warn};

use super::{
    ConsumerActivity, MediaTransport, ReceiverBweTargetUpdate, SourceActivityUpdate,
    SourcePacketGate, TransportAdapterError, TransportConsumerRoute, TransportResult,
    TransportSourceKey,
    rtc::{
        PacketLayerGate, RtcWorkerCommand, WorkerMediaControlBatch as WorkerBatch,
        WorkerMediaControlBatchOutcome as WorkerBatchOutcome,
    },
};

/// Maximum mutations in one synchronous worker command.
///
/// Larger plans are split so one room effect cannot monopolize control
/// dispatch. The packet loop runs its pump phase between chunks.
const MAX_MEDIA_CONTROL_BATCH_ITEMS: usize = 64;
const UNAVAILABLE: TransportAdapterError = TransportAdapterError::TransportUnavailable;

#[derive(Debug)]
#[must_use = "media-control plans must be executed or intentionally dropped"]
pub(crate) struct MediaControlPlan<P = (), C = ()> {
    producers: Vec<(ProducerRouteControl, P)>,
    consumers: Vec<(ConsumerRouteControl, C)>,
    receiver_bwe_targets: Vec<ReceiverBweTargetUpdate>,
}

impl<P, C> MediaControlPlan<P, C> {
    pub(crate) fn push_producer(
        &mut self,
        source: TransportSourceKey,
        update: SourceActivityUpdate,
        finish: P,
    ) {
        self.producers
            .push((ProducerRouteControl { source, update }, finish));
    }

    pub(crate) fn push_consumer(&mut self, control: ConsumerRouteControl, finish: C) {
        assert!(
            control.packet_gate.is_some() || control.activity.is_some() || control.request_keyframe,
            "consumer media control must contain an operation"
        );
        self.consumers.push((control, finish));
    }

    pub(crate) fn set_receiver_bwe_targets(&mut self, updates: Vec<ReceiverBweTargetUpdate>) {
        self.receiver_bwe_targets = updates;
    }

    pub(crate) fn append(&mut self, other: Self) {
        self.producers.extend(other.producers);
        self.consumers.extend(other.consumers);
        self.receiver_bwe_targets.extend(other.receiver_bwe_targets);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.producers.is_empty()
            && self.consumers.is_empty()
            && self.receiver_bwe_targets.is_empty()
    }
}

impl<P, C> Default for MediaControlPlan<P, C> {
    fn default() -> Self {
        Self {
            producers: Vec::new(),
            consumers: Vec::new(),
            receiver_bwe_targets: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub(in crate::engine::media_transport) struct ProducerRouteControl {
    pub(in crate::engine::media_transport) source: TransportSourceKey,
    pub(in crate::engine::media_transport) update: SourceActivityUpdate,
}

#[derive(Debug)]
pub(crate) struct ConsumerRouteControl {
    pub(in crate::engine::media_transport) route: TransportConsumerRoute,
    packet_gate: Option<SourcePacketGate>,
    pub(in crate::engine::media_transport) activity: Option<ConsumerActivity>,
    pub(in crate::engine::media_transport) request_keyframe: bool,
}

impl ConsumerRouteControl {
    pub(crate) const fn new(route: TransportConsumerRoute) -> Self {
        Self {
            route,
            packet_gate: None,
            activity: None,
            request_keyframe: false,
        }
    }

    pub(crate) fn packet_gate(mut self, packet_gate: SourcePacketGate) -> Self {
        self.packet_gate = Some(packet_gate);
        self
    }

    pub(crate) const fn activity(mut self, activity: ConsumerActivity) -> Self {
        self.activity = Some(activity);
        self
    }

    pub(crate) const fn request_keyframe(mut self, request: bool) -> Self {
        self.request_keyframe = request;
        self
    }

    fn failure(&self, error: TransportAdapterError) -> ConsumerRouteControlOutcome {
        let failure = match (self.packet_gate.is_some(), self.activity.is_some()) {
            (true, _) => ConsumerRouteControlFailure::PacketGate(error),
            (false, true) => ConsumerRouteControlFailure::Activity(error),
            (false, false) => ConsumerRouteControlFailure::Keyframe(error),
        };
        ConsumerRouteControlOutcome(Some(failure))
    }
}

#[derive(Debug)]
pub(crate) struct MediaControlOutcome<P, C> {
    pub(crate) producers: Vec<(P, TransportResult<()>)>,
    pub(crate) consumers: Vec<(C, ConsumerRouteControlOutcome)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::engine::media_transport) enum ConsumerRouteControlFailure {
    PacketGate(TransportAdapterError),
    Activity(TransportAdapterError),
    Keyframe(TransportAdapterError),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ConsumerRouteControlOutcome(
    pub(in crate::engine::media_transport) Option<ConsumerRouteControlFailure>,
);

impl ConsumerRouteControlOutcome {
    pub(crate) const fn packet_gate_failed(self) -> bool {
        matches!(self.0, Some(ConsumerRouteControlFailure::PacketGate(_)))
    }

    pub(crate) const fn activity_failed(self) -> bool {
        matches!(self.0, Some(ConsumerRouteControlFailure::Activity(_)))
    }

    pub(crate) const fn keyframe_failed(self) -> bool {
        matches!(self.0, Some(ConsumerRouteControlFailure::Keyframe(_)))
    }

    pub(crate) fn error(self) -> Option<TransportAdapterError> {
        self.0.map(|failure| match failure {
            ConsumerRouteControlFailure::PacketGate(error)
            | ConsumerRouteControlFailure::Activity(error)
            | ConsumerRouteControlFailure::Keyframe(error) => error,
        })
    }

    #[cfg(test)]
    pub(crate) const fn keyframe_error(error: TransportAdapterError) -> Self {
        Self(Some(ConsumerRouteControlFailure::Keyframe(error)))
    }
}

/// Returns one result per planned item or fails the whole batch.
///
/// A mismatched response shape has no safe positional mapping back to room
/// completions.
pub(super) fn reconcile_applied(
    response: TransportResult<WorkerBatchOutcome>,
    expected: usize,
) -> Vec<TransportResult<()>> {
    match response {
        Ok(WorkerBatchOutcome::Applied(results)) if results.len() == expected => results,
        Err(error) => vec![Err(error); expected],
        _ => vec![Err(UNAVAILABLE); expected],
    }
}

type WorkerBatches<T> = BTreeMap<usize, Vec<(usize, T)>>;
type GateUpdate = (usize, TransportConsumerRoute, PacketLayerGate);

fn push<K: Ord, T>(batches: &mut BTreeMap<K, Vec<T>>, key: K, item: T) {
    batches.entry(key).or_default().push(item);
}

fn into_batches<T>(mut items: Vec<T>) -> impl Iterator<Item = Vec<T>> {
    let mut batches = Vec::with_capacity(items.len().div_ceil(MAX_MEDIA_CONTROL_BATCH_ITEMS));
    // Split off the tail so each split_off moves at most one batch. rev() below
    // restores source order.
    while items.len() > MAX_MEDIA_CONTROL_BATCH_ITEMS {
        let tail_len = (items.len() - 1) % MAX_MEDIA_CONTROL_BATCH_ITEMS + 1;
        batches.push(items.split_off(items.len() - tail_len));
    }
    batches.push(items);
    batches.into_iter().rev().filter(|batch| !batch.is_empty())
}

struct MediaControlExecution<P, C> {
    receiver_bwe: WorkerBatches<ReceiverBweTargetUpdate>,
    producers: WorkerBatches<ProducerRouteControl>,
    producer_results: Vec<(P, TransportResult<()>)>,
    gates: BTreeMap<(usize, TransportSourceKey), Vec<GateUpdate>>,
    consumers: WorkerBatches<ConsumerRouteControl>,
    consumer_results: Vec<(C, ConsumerRouteControlOutcome)>,
}

#[expect(
    clippy::indexing_slicing,
    reason = "plan indexes are created by enumerate alongside their result slots"
)]
impl<P, C> MediaControlExecution<P, C> {
    fn new(plan: MediaControlPlan<P, C>) -> Self {
        let MediaControlPlan {
            producers,
            consumers,
            receiver_bwe_targets,
        } = plan;
        let mut execution = Self {
            receiver_bwe: BTreeMap::new(),
            producers: BTreeMap::new(),
            producer_results: Vec::with_capacity(producers.len()),
            gates: BTreeMap::new(),
            consumers: BTreeMap::new(),
            consumer_results: Vec::with_capacity(consumers.len()),
        };
        for (index, update) in receiver_bwe_targets.into_iter().enumerate() {
            let worker = update.session_key().media_worker_id().as_usize();
            push(&mut execution.receiver_bwe, worker, (index, update));
        }
        for (index, (control, finish)) in producers.into_iter().enumerate() {
            let worker = control.source.session_key().media_worker_id().as_usize();
            execution.producer_results.push((finish, Err(UNAVAILABLE)));
            push(&mut execution.producers, worker, (index, control));
        }
        for (index, (mut control, finish)) in consumers.into_iter().enumerate() {
            // Worker placement is not room authority. Reject cross-room controls
            // before translating them into worker-local mutations.
            if !control.route.is_single_room() {
                execution
                    .consumer_results
                    .push((finish, control.failure(TransportAdapterError::InvalidInput)));
                continue;
            }
            execution
                .consumer_results
                .push((finish, ConsumerRouteControlOutcome::default()));
            let worker = control
                .route
                .consumer_session_key()
                .media_worker_id()
                .as_usize();
            if let Some(gate) = control.packet_gate.take() {
                let gate = match gate {
                    SourcePacketGate::Open => PacketLayerGate::Open,
                    SourcePacketGate::Rid(rid) => PacketLayerGate::Rid(rid.as_str().into()),
                };
                push(
                    &mut execution.gates,
                    (worker, control.route.source().clone()),
                    (index, control.route.clone(), gate),
                );
            }
            if control.activity.is_some() || control.request_keyframe {
                push(&mut execution.consumers, worker, (index, control));
            }
        }
        execution
    }

    async fn apply_receiver_bwe(&mut self, transport: &MediaTransport) {
        for (worker, updates) in take(&mut self.receiver_bwe) {
            for updates in into_batches(updates) {
                let details: Vec<_> = updates
                    .iter()
                    .map(|(_, update)| (update.session_key().clone(), update.target()))
                    .collect();
                let response = transport
                    .execute_batch(worker, WorkerBatch::ReceiverBwe(updates))
                    .await;
                let results = reconcile_applied(response, details.len());
                for ((session_key, target), result) in details.into_iter().zip(results) {
                    match result {
                        Ok(()) => {}
                        Err(TransportAdapterError::InvalidInput) => debug!(
                            ?session_key,
                            target = target.as_bps(),
                            "media transport skipped a stale receiver BWE target"
                        ),
                        Err(error) => warn!(
                            ?error,
                            ?session_key,
                            target = target.as_bps(),
                            "media transport failed to update a receiver BWE target"
                        ),
                    }
                }
            }
        }
    }

    async fn apply_producers(&mut self, transport: &MediaTransport) {
        for (worker, controls) in take(&mut self.producers) {
            for controls in into_batches(controls) {
                let indexes: Vec<_> = controls.iter().map(|(index, _)| *index).collect();
                let response = transport
                    .execute_batch(worker, WorkerBatch::ProducerActivity(controls))
                    .await;
                let results = reconcile_applied(response, indexes.len());
                for (index, result) in indexes.into_iter().zip(results) {
                    self.producer_results[index].1 = result;
                }
            }
        }
    }

    async fn apply_gates(&mut self, transport: &MediaTransport) {
        for ((worker, source), updates) in take(&mut self.gates) {
            let indexes: Vec<_> = updates.iter().map(|(index, _, _)| *index).collect();
            let response = transport
                .execute_batch(worker, WorkerBatch::ConsumerGates { source, updates })
                .await;
            let results = reconcile_applied(response, indexes.len());
            for (index, result) in indexes.into_iter().zip(results) {
                if let Err(error) = result {
                    let failure = ConsumerRouteControlFailure::PacketGate(error);
                    self.consumer_results[index].1 = ConsumerRouteControlOutcome(Some(failure));
                }
            }
        }
    }

    async fn apply_consumers(&mut self, transport: &MediaTransport) {
        let results = &self.consumer_results;
        // Room policy cannot commit a selection whose gate was rejected. Skip its
        // dependent activity and keyframe phases.
        for controls in self.consumers.values_mut() {
            controls.retain(
                |(index, _)| matches!(results.get(*index), Some((_, result)) if result.error().is_none()),
            );
        }
        for (worker, controls) in take(&mut self.consumers) {
            for controls in into_batches(controls) {
                let indexes: Vec<_> = controls
                    .iter()
                    .map(|(index, control)| (*index, control.activity.is_some()))
                    .collect();
                let response = transport
                    .execute_batch(worker, WorkerBatch::ConsumerFollowUp(controls))
                    .await;
                let results = match response {
                    Ok(WorkerBatchOutcome::Consumers(results))
                        if results.len() == indexes.len() =>
                    {
                        results
                    }
                    response => {
                        let error = response.err().unwrap_or(UNAVAILABLE);
                        indexes
                            .iter()
                            .map(|(_, has_activity)| {
                                let failure = if *has_activity {
                                    ConsumerRouteControlFailure::Activity(error)
                                } else {
                                    ConsumerRouteControlFailure::Keyframe(error)
                                };
                                ConsumerRouteControlOutcome(Some(failure))
                            })
                            .collect()
                    }
                };
                for ((index, _), result) in indexes.into_iter().zip(results) {
                    self.consumer_results[index].1 = result;
                }
            }
        }
    }
}

impl MediaTransport {
    async fn execute_batch(
        &self,
        worker_index: usize,
        batch: WorkerBatch,
    ) -> TransportResult<WorkerBatchOutcome> {
        let worker = self.worker_for_index(worker_index).ok_or(UNAVAILABLE)?;
        #[cfg(test)]
        self.observe_media_control_batch(worker_index, &batch);
        worker
            .request_worker(|response| RtcWorkerCommand::ApplyMediaControlBatch { batch, response })
            .await
    }

    /// Applies a media-control plan and returns completions in insertion order.
    pub(crate) async fn apply_media_control<P, C>(
        &self,
        plan: MediaControlPlan<P, C>,
    ) -> MediaControlOutcome<P, C> {
        let mut execution = MediaControlExecution::new(plan);
        execution.apply_receiver_bwe(self).await;
        execution.apply_producers(self).await;
        // Room policy may commit only a selection whose gate and activity reached
        // worker state. Apply gates before their dependent activity and keyframe work.
        execution.apply_gates(self).await;
        execution.apply_consumers(self).await;
        MediaControlOutcome {
            producers: execution.producer_results,
            consumers: execution.consumer_results,
        }
    }
}
