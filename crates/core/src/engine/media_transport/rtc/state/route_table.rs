//! Source-scoped forwarding decisions and decoder-recovery state.
//!
//! [`RouteTable`] joins published-source activity with local consumer demand,
//! relay demand and packet observations. Each [`RouteSource`]
//! retains requested gates separately from the gates currently safe to forward.
//! [`ForwardView`] gives the packet planner local routes, relay targets and the
//! aggregate source gate from one source lookup.
//!
//! [`PacketLoopState`](super::PacketLoopState) couples destination changes to
//! stream retirement. [`recovery`](super::super::recovery)
//! dispatches producer feedback after `PacketLoopState` commits decoder gates
//! together with RTX invalidation.
//! The table retains source/RID retry state across those feedback attempts.
//!
//! Packet liveness alone cannot activate a pending decoder gate. An observable
//! refresh can activate a matching gate and clear source/RID retries. Consumer
//! feedback and opaque recovery keep bounded retry tails. Packet observations
//! must precede planning for each packet, as described by
//! [`PacketForwarder`](super::super::packet_loop::PacketForwarder).

mod active_rank;
mod remote;
mod source;

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, btree_map::Entry},
    sync::Arc,
    time::{Duration, Instant},
};

use itertools::Itertools;
use o_sfu_router::rtp::MediaStream;
use o_sfu_telemetry::metrics::RtcMetricsRecorder;
use str0m::media::{KeyframeRequestKind, Mid, Pt, Rid};
use tracing::debug;

use self::{
    active_rank::ActiveSpeakerRank,
    remote::RemoteSources,
    source::{ForwardingChange, RemovedConsumerRoute, RouteSource},
};
pub(in super::super) use self::{
    remote::{RemoteSourceRegistration, RemoteSourceTransaction},
    source::{RidReadinessRouteUpdate, RidReadinessSelectedGateUpdate},
};
use super::{
    super::{
        codec,
        commands::{RelayTarget, RemoteControlSendOutcome, RemoteSourceControl},
    },
    bitrate::MediaBitrateCounter,
    keyframe_tracker::{
        KeyframeRequestDecision, KeyframeRequestOrigin, KeyframeRequestTracker,
        PublisherKeyframeLimiter, PublisherRequestDemand, SourceKeyframeRequest,
    },
    relay_registry::{ActiveRelayTarget, RelayTargetId},
    route_control::PacketLayerGate,
    slots::{ConsumerStreamHandle, RouteDestinationHandle},
    source_route::{MediaRouteDestination, MediaRouteEntry},
};

#[derive(Clone, Copy)]
pub(in super::super) struct ConsumerRouteUpdate {
    pub(in super::super) route_changed: bool,
    pub(in super::super) repair_delivery_changed: bool,
}

impl ConsumerRouteUpdate {
    fn new(route_changed: bool, repair_delivery_changed: bool) -> Self {
        Self {
            route_changed,
            repair_delivery_changed,
        }
    }
}

/// reusable selected-RID readiness vectors owned by packet-loop state
///
/// route updates clear these vectors after every packet and retain capacity
#[derive(Default)]
pub(in super::super) struct RidReadinessScratch {
    pub(in super::super) ready: Vec<Rid>,
    pub(in super::super) stale: Vec<Rid>,
    pub(in super::super) pending_selected: Vec<Rid>,
}

impl RidReadinessScratch {
    pub(in super::super) fn clear(&mut self) {
        self.ready.clear();
        self.stale.clear();
        self.pending_selected.clear();
    }
}
use crate::engine::media_transport::{
    ActiveSpeakerSource, ActiveSpeakerSourceDiagnostic, SourceActivityUpdate,
    TransportAdapterError, TransportMediaId, TransportSessionKey, TransportSourceActivity,
    TransportSourceKey,
};

/// Packet-facing destinations and the source-wide gate from one route lookup.
#[derive(Default)]
pub(in super::super) struct ForwardView<'a> {
    pub route: Option<&'a MediaRouteEntry>,
    pub relays: Option<RelayForwardView<'a>>,
    pub source_gate: Option<PacketLayerGate>,
}

/// Active relay targets and their independently retained packet gates.
///
/// At least one target is active. Gate updates can precede target registration
/// or activation, so the view borrows both collections from the same source.
#[derive(Clone, Copy)]
pub(in super::super) struct RelayForwardView<'a> {
    targets: &'a [ActiveRelayTarget],
    gates: &'a BTreeMap<RelayTargetId, PacketLayerGate>,
}

impl<'a> RelayForwardView<'a> {
    pub(in super::super) const fn len(self) -> usize {
        self.targets.len()
    }

    /// Yields active targets whose gate permits the packet RID.
    ///
    /// A missing target gate imposes no restriction beyond the source gate.
    pub(in super::super) fn targets_for_packet(
        self,
        rid: Option<Rid>,
    ) -> impl Iterator<Item = &'a ActiveRelayTarget> {
        self.targets.iter().filter(move |target| {
            self.gates
                .get(&target.target_id)
                .is_none_or(|gate| gate.permits(rid))
        })
    }
}

/// Wake interval while cross-worker route control waits for queue capacity.
///
/// Room commands waiting for a full worker queue take released permits before
/// `try_send` can, so an idle worker must retry without other input.
pub(in super::super) const REMOTE_CONTROL_RETRY_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Debug, Default)]
pub(in super::super) struct RouteTable {
    sources: BTreeMap<TransportMediaId, RouteSource>,
    forwarding_sources: usize,
    active: ActiveSpeakerRank,
    remote: RemoteSources,
    /// Relay targets owed the current activity of a local source.
    relay_activity: BTreeSet<(TransportMediaId, RelayTargetId)>,
    keyframe_requests: KeyframeRequestTracker,
    pub(in super::super) publisher_keyframes: PublisherKeyframeLimiter,
}

impl RouteTable {
    #[cfg(any(test, feature = "testing-transport"))]
    pub(in super::super) fn relay_target_count(&self, source_id: TransportMediaId) -> usize {
        self.sources
            .get(&source_id)
            .map_or(0, |source| source.relay_target_ids().count())
    }

    pub(in super::super) fn forward_view(
        &self,
        source_id: TransportMediaId,
        include_relays: bool,
    ) -> ForwardView<'_> {
        let Some(source) = self.sources.get(&source_id) else {
            return ForwardView::default();
        };
        let (route, relays) = if source.source_is_active() {
            (
                source.local_route(),
                if include_relays {
                    source.relay_forward_view()
                } else {
                    None
                },
            )
        } else {
            (None, None)
        };
        ForwardView {
            route,
            relays,
            source_gate: source.packet_filter_gate(),
        }
    }

    pub(in super::super) fn local_route(
        &self,
        source_id: TransportMediaId,
    ) -> Option<&MediaRouteEntry> {
        self.sources.get(&source_id)?.local_route()
    }

    pub(in super::super) fn local_route_and_activity(
        &self,
        source_id: TransportMediaId,
    ) -> Option<(&MediaRouteEntry, bool)> {
        let source = self.sources.get(&source_id)?;
        Some((source.local_route()?, source.source_is_active()))
    }

    pub(in super::super) fn source_is_active(&self, source_id: TransportMediaId) -> bool {
        self.sources
            .get(&source_id)
            .is_some_and(RouteSource::source_is_active)
    }

    pub(in super::super) fn has_forwarding_sources(&self) -> bool {
        self.forwarding_sources != 0
    }

    fn source_mut(&mut self, source_id: TransportMediaId) -> &mut RouteSource {
        self.sources.entry(source_id).or_default()
    }

    pub(in super::super) fn add_consumer_route(
        &mut self,
        source_id: TransportMediaId,
        destination: MediaRouteDestination,
    ) -> RouteDestinationHandle {
        let (handle, became_forwarding) = self
            .sources
            .entry(source_id)
            .or_default()
            .add_consumer_route(destination);
        if became_forwarding {
            self.forwarding_sources += 1;
        }
        self.refresh_src_pkt_gate(source_id);
        handle
    }

    pub(in super::super) fn remove_consumer_route(
        &mut self,
        source_id: TransportMediaId,
        session_key: &TransportSessionKey,
        media_id: TransportMediaId,
    ) -> Option<RemovedConsumerRoute> {
        let removed = self
            .sources
            .get_mut(&source_id)?
            .remove_consumer_route(session_key, media_id)?;
        if removed.stopped_forwarding {
            self.forwarding_sources -= 1;
        }
        if !self.has_local_kf_demand(source_id, None) {
            self.publisher_keyframes.retire_local(source_id);
        }
        self.prune_unrouted_remote_src(source_id);
        self.refresh_src_pkt_gate(source_id);
        self.prune_empty(source_id);
        Some(removed)
    }

    pub(in super::super) fn apply_source_activity(
        &mut self,
        source_id: TransportMediaId,
        update: SourceActivityUpdate,
    ) -> Result<bool, TransportAdapterError> {
        let source = self
            .sources
            .get_mut(&source_id)
            .ok_or(TransportAdapterError::TransportUnavailable)?;
        let accepted = source.apply_source_activity(update);
        if accepted {
            self.relay_activity.extend(
                source
                    .relay_target_ids()
                    .map(|target_id| (source_id, target_id)),
            );
        }
        if accepted && !update.activity().is_active() {
            self.keyframe_requests.forget_source(source_id);
            self.publisher_keyframes.cancel_source(source_id);
        }
        Ok(accepted)
    }

    pub(in super::super) fn set_consumer_active(
        &mut self,
        source_id: TransportMediaId,
        destination: RouteDestinationHandle,
        media_id: TransportMediaId,
        active: bool,
    ) -> Result<ConsumerRouteUpdate, TransportAdapterError> {
        let update = self
            .sources
            .get_mut(&source_id)
            .ok_or(TransportAdapterError::TransportUnavailable)?
            .set_consumer_active(destination, media_id, active)?;
        if !self.has_local_kf_demand(source_id, None) {
            self.publisher_keyframes.retire_local(source_id);
        }
        if update.route_changed {
            self.refresh_src_pkt_gate(source_id);
        }
        Ok(update)
    }

    pub(in super::super) fn set_consumer_pkt_gate(
        &mut self,
        source_id: TransportMediaId,
        destination: RouteDestinationHandle,
        media_id: TransportMediaId,
        packet_gate: PacketLayerGate,
    ) -> Result<ConsumerRouteUpdate, TransportAdapterError> {
        self.sources
            .get_mut(&source_id)
            .ok_or(TransportAdapterError::TransportUnavailable)?
            .set_consumer_pkt_gate(destination, media_id, packet_gate)
    }

    pub(in super::super) fn bind_consumer_egress(
        &mut self,
        source_id: TransportMediaId,
        destination: RouteDestinationHandle,
        media_id: TransportMediaId,
        mid: Mid,
        stream: ConsumerStreamHandle,
    ) {
        if let Some(source) = self.sources.get_mut(&source_id) {
            source.bind_consumer_egress(destination, media_id, mid, stream);
        }
    }

    pub(in super::super) fn update_decoder_readiness(
        &mut self,
        source_id: TransportMediaId,
        incoming_rid: Option<Rid>,
        is_keyframe: bool,
        scratch: &mut RidReadinessScratch,
        on_repair_delivery_changed: impl FnMut(&MediaRouteDestination),
    ) -> RidReadinessRouteUpdate {
        let Some(source) = self.sources.get_mut(&source_id) else {
            return RidReadinessRouteUpdate::default();
        };
        let update = source.update_decoder_readiness(
            source_id,
            incoming_rid,
            is_keyframe,
            scratch,
            on_repair_delivery_changed,
        );
        if update.changed_gate() {
            self.refresh_src_pkt_gate(source_id);
        }
        update
    }

    pub(in super::super) fn take_route(
        &mut self,
        source_id: TransportMediaId,
    ) -> Option<MediaRouteEntry> {
        let removed = self.sources.get_mut(&source_id)?.take_route()?;
        if removed.stopped_forwarding {
            self.forwarding_sources -= 1;
        }
        self.prune_unrouted_remote_src(source_id);
        self.refresh_src_pkt_gate(source_id);
        self.prune_empty(source_id);
        Some(removed.route)
    }

    pub(in super::super) fn remove_dsts_for_session(&mut self, session_key: &TransportSessionKey) {
        let mut affected = Vec::new();
        let mut removed_forwarding_sources = 0;
        for (source_id, source) in &mut self.sources {
            if let Some(change) = source.remove_dsts_for_session(session_key) {
                removed_forwarding_sources += usize::from(change.stopped_forwarding());
                affected.push(*source_id);
            }
        }
        self.forwarding_sources -= removed_forwarding_sources;
        for source_id in affected {
            self.prune_unrouted_remote_src(source_id);
            self.refresh_src_pkt_gate(source_id);
            self.prune_empty(source_id);
        }
    }

    pub(in super::super) fn refresh_src_pkt_gate(&mut self, source_id: TransportMediaId) {
        let Some(source) = self.sources.get_mut(&source_id) else {
            return;
        };
        let remote_packet_gate = source.refresh_route_pkt_gate();
        self.remote.publish_gate(source_id, remote_packet_gate);
        let effective_packet_gate = source.effective_packet_gate();
        debug!(
            ?source_id,
            ?effective_packet_gate,
            "updated source packet gate"
        );
    }

    pub(in super::super) fn has_kf_demand(
        &self,
        source_id: TransportMediaId,
        rid: Option<Rid>,
    ) -> bool {
        self.sources
            .get(&source_id)
            .is_some_and(|source| source.has_kf_demand(rid))
    }

    pub(in super::super) fn has_local_kf_demand(
        &self,
        source_id: TransportMediaId,
        rid: Option<Rid>,
    ) -> bool {
        self.sources
            .get(&source_id)
            .is_some_and(|source| source.has_local_kf_demand(rid))
    }

    pub(in super::super) fn request_publisher_kf(
        &mut self,
        src_media: TransportMediaId,
        rid: Option<Rid>,
        kind: KeyframeRequestKind,
        demand: PublisherRequestDemand,
        now: Instant,
    ) -> Option<KeyframeRequestKind> {
        let source = self.sources.get(&src_media);
        self.publisher_keyframes
            .request(src_media, rid, kind, demand, now, |pending_demand| {
                source.is_some_and(|source| match pending_demand {
                    PublisherRequestDemand::Local(requested_rid) => {
                        source.has_local_kf_demand(requested_rid)
                    }
                    PublisherRequestDemand::Source(requested_rid) => {
                        source.has_kf_demand(requested_rid)
                    }
                    PublisherRequestDemand::Relay(target_id) => {
                        source.source_is_active() && source.is_relay_target_active(target_id)
                    }
                })
            })
    }

    pub(in super::super) fn register_local_source(&mut self, source_id: TransportMediaId) {
        self.source_mut(source_id).producer.registered = true;
    }

    pub(in super::super) fn unregister_local_source(&mut self, source_id: TransportMediaId) {
        // Worker-initiated session closes retire producers without a room
        // release. Relay targets end with their producer.
        if self
            .sources
            .get_mut(&source_id)
            .and_then(RouteSource::remove_relay)
            .is_some_and(ForwardingChange::stopped_forwarding)
        {
            self.forwarding_sources -= 1;
        }
        self.forget_packet_state(source_id);
        self.keyframe_requests.forget_source(source_id);
        self.publisher_keyframes.forget_source(source_id);
        self.prune_empty(source_id);
    }

    /// Registers the control path and announces this worker as relay target.
    ///
    /// A new registration starts inactive until the source worker answers with
    /// `SetRemoteSourceActivity`. A current registration keeps its state and
    /// returns `None` because it needs no rollback.
    ///
    /// Drop the transaction to commit or pass it to `rollback_remote_source`
    /// before another registration change for this source.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError::InvalidInput`] when the media id already
    /// belongs to a different remote source identity and
    /// [`TransportAdapterError::TransportUnavailable`] when the source worker
    /// queue is closed.
    pub(in super::super) fn register_remote_source(
        &mut self,
        source: &TransportSourceKey,
        source_control: RemoteSourceControl,
    ) -> Result<Option<RemoteSourceTransaction>, TransportAdapterError> {
        let transaction = self.remote.register(source, source_control)?;
        if transaction.is_some() {
            self.source_mut(source.transport_media_id())
                .set_source_active(false);
        }
        Ok(transaction)
    }

    #[expect(
        clippy::needless_pass_by_value,
        reason = "rollback consumes the registration token so it cannot be reused"
    )]
    pub(in super::super) fn rollback_remote_source(
        &mut self,
        transaction: RemoteSourceTransaction,
    ) {
        self.remove_remote_source(transaction.source_id);
    }

    pub(in super::super) fn remote_source(
        &self,
        source_id: TransportMediaId,
    ) -> Option<&RemoteSourceRegistration> {
        self.remote.get(source_id)
    }

    #[cfg(any(test, feature = "internal-benchmarks"))]
    pub(in super::super) fn publish_remote_pkt_gate(
        &mut self,
        source_id: TransportMediaId,
        packet_gate: PacketLayerGate,
    ) {
        self.remote.publish_gate(source_id, packet_gate);
    }

    /// Retries cross-worker route control that found a full queue.
    ///
    /// Relay targets that report a closed queue are removed.
    pub(in super::super) fn flush_remote_control(&mut self, rtc_metrics: &RtcMetricsRecorder) {
        self.remote.flush();
        if self.relay_activity.is_empty() {
            return;
        }
        let mut closed = Vec::new();
        let sources = &self.sources;
        self.relay_activity.retain(|&(source_id, target_id)| {
            match sources
                .get(&source_id)
                .and_then(|source| source.send_relay_activity(target_id, rtc_metrics))
            {
                Some(RemoteControlSendOutcome::Full) => true,
                Some(RemoteControlSendOutcome::Closed) => {
                    closed.push((source_id, target_id));
                    false
                }
                Some(RemoteControlSendOutcome::Forwarded) | None => false,
            }
        });
        for (source_id, target_id) in closed {
            self.remove_relay_target(source_id, target_id);
        }
    }

    pub(in super::super) fn next_remote_control_retry(&self, now: Instant) -> Option<Instant> {
        (self.remote.has_pending() || !self.relay_activity.is_empty())
            .then(|| now + REMOTE_CONTROL_RETRY_INTERVAL)
    }

    // Prune before refreshing gates so the last route spends no queue slot
    // on Block ahead of its withdrawal.
    pub(in super::super) fn prune_unrouted_remote_src(&mut self, source_id: TransportMediaId) {
        if self
            .local_route(source_id)
            .is_some_and(|route| !route.destinations().is_empty())
        {
            return;
        }
        if self.remote_source(source_id).is_some() {
            self.remove_remote_source(source_id);
        }
    }

    pub(in super::super) fn prune_unrouted_remote_srcs(
        &mut self,
        mut keep_local: impl FnMut(&TransportMediaId) -> bool,
    ) {
        let source_ids = self.sources.keys().copied().collect::<Vec<_>>();
        for source_id in source_ids {
            let has_remote_source = self.remote_source(source_id).is_some();
            let keep_registered = keep_local(&source_id) || has_remote_source;
            if has_remote_source && self.local_route(source_id).is_none() {
                self.remove_remote_source(source_id);
                continue;
            }
            if !keep_registered {
                self.forget_packet_state(source_id);
                self.keyframe_requests.forget_source(source_id);
                self.publisher_keyframes.forget_source(source_id);
            }
            self.prune_empty(source_id);
        }
    }

    pub(in super::super) fn inspect_packet(
        &self,
        source_id: TransportMediaId,
        payload_type: Pt,
        payload: &[u8],
        has_rid: bool,
    ) -> codec::Packet {
        self.sources
            .get(&source_id)
            .map_or(codec::Packet::default(), |source| {
                source
                    .producer
                    .packet_inspector
                    .inspect(payload_type, payload, has_rid)
            })
    }

    pub(in super::super) fn clear_packet_inspector(&mut self, source_id: TransportMediaId) {
        if let Some(source) = self.sources.get_mut(&source_id) {
            source.producer.packet_inspector = codec::PacketInspector::default();
        }
    }

    pub(in super::super) fn refresh_packet_inspector(
        &mut self,
        source_id: TransportMediaId,
        parameters: &MediaStream,
    ) {
        let packet_inspector = codec::PacketInspector::from_parameters(parameters);
        self.source_mut(source_id).producer.packet_inspector = packet_inspector;
    }

    pub(in super::super) fn observe_producer_packet(
        &mut self,
        source_id: TransportMediaId,
        rid: Option<Rid>,
        is_keyframe: bool,
        now: Instant,
    ) -> bool {
        let observed = self
            .source_mut(source_id)
            .producer
            .observe_packet(rid, is_keyframe, now);
        self.prune_empty(source_id);
        observed
    }

    pub(in super::super) fn source_activity_snapshot(
        &self,
        source_ids: &[TransportMediaId],
        now: Instant,
        incoming_bitrate_counters: &BTreeMap<TransportMediaId, Arc<MediaBitrateCounter>>,
    ) -> Vec<TransportSourceActivity> {
        source_ids
            .iter()
            .filter_map(|source_id| {
                let source = self.sources.get(source_id)?;
                let source_last_packet_age = incoming_bitrate_counters
                    .get(source_id)
                    .and_then(|counter| counter.last_observed_age(now));
                source.diagnostic(*source_id, source_last_packet_age, now)
            })
            .collect()
    }

    pub(in super::super) fn collect_ready_producer_rids(
        &self,
        source_id: TransportMediaId,
        now: Instant,
        max_age: Duration,
        ready_rids: &mut Vec<Rid>,
    ) {
        if let Some(source) = self.sources.get(&source_id) {
            source.producer.collect_ready_rids(now, max_age, ready_rids);
        }
    }

    pub(in super::super) fn track_kf_req(
        &mut self,
        source_id: TransportMediaId,
        rid: Option<Rid>,
        kind: KeyframeRequestKind,
        origin: KeyframeRequestOrigin,
        now: Instant,
    ) -> KeyframeRequestDecision {
        self.keyframe_requests
            .track(source_id, rid, kind, origin, now)
    }

    pub(in super::super) fn forget_kf_req(
        &mut self,
        source_id: TransportMediaId,
        rid: Option<Rid>,
    ) {
        self.keyframe_requests.forget(source_id, rid);
    }

    pub(in super::super) fn observe_decoder_refresh(
        &mut self,
        source_id: TransportMediaId,
        rid: Option<Rid>,
    ) -> usize {
        self.publisher_keyframes.observe_refresh(source_id, rid);
        self.keyframe_requests.observe_refresh(source_id, rid)
    }

    pub(in super::super) fn decoder_refresh_is_observable(
        &self,
        source_id: TransportMediaId,
    ) -> bool {
        self.sources.get(&source_id).is_some_and(|source| {
            source
                .producer
                .packet_inspector
                .decoder_refresh_is_observable()
        })
    }

    pub(in super::super) fn drain_due_kf_reqs(
        &mut self,
        now: Instant,
        retries: &mut Vec<SourceKeyframeRequest>,
    ) {
        self.keyframe_requests.drain_due(now, retries);
    }

    pub(in super::super) fn next_kf_deadline(&self) -> Option<Instant> {
        self.keyframe_requests.next_deadline()
    }

    pub(in super::super) fn observe_audio_activity(
        &mut self,
        source_id: TransportMediaId,
        voice_activity: Option<bool>,
        audio_level_dbov: Option<i8>,
        now: Instant,
    ) -> bool {
        let packet_gate_changed = match self.sources.entry(source_id) {
            Entry::Occupied(mut entry) => {
                let source = entry.get_mut();
                let previous_packet_gate = source.effective_packet_gate();
                if !source.observe_audio_activity(voice_activity, audio_level_dbov, now) {
                    return false;
                }
                previous_packet_gate != source.effective_packet_gate()
            }
            Entry::Vacant(entry) => {
                if voice_activity.is_none() && audio_level_dbov.is_none() {
                    return false;
                }
                let mut source = RouteSource::default();
                if !source.observe_audio_activity(voice_activity, audio_level_dbov, now) {
                    return false;
                }
                if source.is_empty() {
                    return false;
                }
                entry.insert(source);
                true
            }
        };
        self.active
            .update_src(source_id, self.sources.get(&source_id), now)
            || packet_gate_changed
    }

    /// Applies a target's remote gate. A `Block` gate deactivates the target.
    pub(in super::super) fn set_relay_pkt_gate(
        &mut self,
        source_id: TransportMediaId,
        target_id: RelayTargetId,
        packet_gate: PacketLayerGate,
    ) {
        let active = self
            .sources
            .get_mut(&source_id)
            .and_then(|source| source.set_relay_pkt_gate(target_id, packet_gate));
        if active == Some(false) {
            self.publisher_keyframes.retire_relay(source_id, target_id);
        }
    }

    pub(in super::super) fn active_speaker_sources(
        &self,
        now: Instant,
    ) -> Vec<ActiveSpeakerSource> {
        self.sources
            .iter()
            .filter_map(|(source_id, source)| source.active_speaker_source(*source_id, now))
            .sorted_unstable_by_key(|source| {
                (
                    Reverse(source.observed_at()),
                    source.transport_media_id().as_u64(),
                )
            })
            .collect()
    }

    pub(in super::super) fn active_speaker_diagnostics(
        &self,
        source_ids: &[TransportMediaId],
        now: Instant,
    ) -> Vec<ActiveSpeakerSourceDiagnostic> {
        source_ids
            .iter()
            .filter_map(|source_id| {
                self.sources
                    .get(source_id)?
                    .active_speaker_diagnostic(*source_id, now)
            })
            .collect()
    }

    pub(in super::super) fn next_active_speaker_deadline(&self, now: Instant) -> Option<Instant> {
        self.active.next_deadline(now)
    }

    pub(in super::super) fn take_expired_speakers(
        &mut self,
        now: Instant,
    ) -> Vec<TransportMediaId> {
        self.active.take_expired(now)
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(in super::super) fn effective_packet_gate(
        &self,
        source_id: TransportMediaId,
    ) -> Option<PacketLayerGate> {
        self.sources
            .get(&source_id)
            .and_then(RouteSource::effective_packet_gate)
    }

    /// Registers `target` for a local source and owes it the current activity.
    ///
    /// Every add answers a new consumer registration, which stays inactive
    /// until this activity arrives.
    pub(in super::super) fn add_relay_target(
        &mut self,
        source: &TransportSourceKey,
        target: RelayTarget,
        packet_gate: PacketLayerGate,
    ) {
        let source_id = source.transport_media_id();
        let target_id = target.id;
        if self
            .sources
            .entry(source_id)
            .or_default()
            .add_relay_target(source, target, packet_gate)
        {
            self.forwarding_sources += 1;
        }
        self.relay_activity.insert((source_id, target_id));
    }

    pub(in super::super) fn remove_relay_target(
        &mut self,
        source_id: TransportMediaId,
        target_id: RelayTargetId,
    ) {
        let Some(change) = self
            .sources
            .get_mut(&source_id)
            .and_then(|source| source.remove_relay_target(target_id))
        else {
            return;
        };
        if change.stopped_forwarding() {
            self.forwarding_sources -= 1;
        }
        self.publisher_keyframes.retire_relay(source_id, target_id);
        self.prune_empty(source_id);
    }

    pub(in super::super) fn source_relay_target_is_active(
        &self,
        source_id: TransportMediaId,
        target_id: RelayTargetId,
    ) -> bool {
        self.sources.get(&source_id).is_some_and(|source| {
            source.source_is_active() && source.is_relay_target_active(target_id)
        })
    }

    fn remove_remote_source(&mut self, source_id: TransportMediaId) {
        self.remote.remove(source_id);
        self.forget_packet_state(source_id);
        self.keyframe_requests.forget_source(source_id);
        self.publisher_keyframes.forget_source(source_id);
        self.prune_empty(source_id);
    }

    fn prune_empty(&mut self, source_id: TransportMediaId) {
        // Remote registrations retain a source entry for activity and gate updates.
        if self
            .sources
            .get(&source_id)
            .is_some_and(RouteSource::is_empty)
            && self.remote.get(source_id).is_none()
        {
            self.sources.remove(&source_id);
            self.keyframe_requests.forget_source(source_id);
            self.publisher_keyframes.forget_source(source_id);
            self.active.drop_src(source_id);
        }
    }

    fn forget_packet_state(&mut self, source_id: TransportMediaId) {
        if let Some(source) = self.sources.get_mut(&source_id) {
            source.forget_packet_state();
        }
        self.active.drop_src(source_id);
    }
}

#[cfg(test)]
#[path = "TESTS/route_table.rs"]
mod tests;
