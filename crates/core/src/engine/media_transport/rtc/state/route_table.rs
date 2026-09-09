//! Source-scoped forwarding decisions and decoder-recovery state.
//!
//! [`RouteTable`] joins published-source activity with local consumer demand,
//! relay demand and packet observations. Each [`RouteSource`]
//! retains requested gates separately from the gates currently safe to forward.
//! [`ForwardView`] gives the packet planner local routes, relay targets and the
//! aggregate source gate from one source lookup.
//!
//! [`PacketLoopState`](super::PacketLoopState) couples destination changes to
//! session MID indexes and stream retirement. [`recovery`](super::super::recovery)
//! combines decoder-gate transitions with RTX invalidation and producer feedback.
//! The table retains source/RID retry state across those feedback attempts.
//!
//! Packet liveness alone cannot activate a pending decoder gate. An observable
//! refresh can activate a matching gate and clear source/RID retries. Consumer
//! feedback and opaque recovery keep bounded retry tails. Packet observations
//! must precede planning for each packet, as described by
//! [`forward_flush`](super::super::packet_loop::forward_flush).

mod active_rank;
mod source;

use std::{
    cmp::Reverse,
    collections::{BTreeMap, VecDeque, btree_map::Entry},
    mem,
    sync::Arc,
    time::{Duration, Instant},
};

use itertools::Itertools;
use o_sfu_router::rtp::MediaStream;
use str0m::{
    media::{KeyframeRequestKind, Pt, Rid},
    rtp::Ssrc,
};
use tracing::debug;

pub(in super::super) use self::source::{RidReadinessRouteUpdate, RidReadinessSelectedGateUpdate};
use self::{
    active_rank::ActiveSpeakerRank,
    source::{RemovedConsumerRoute, RouteSource},
};
use super::{
    super::{codec, commands::RemoteSourceControl},
    bitrate::MediaBitrateCounter,
    keyframe_tracker::{
        KeyframeRequestDecision, KeyframeRequestOrigin, KeyframeRequestTracker,
        SourceKeyframeRequest,
    },
    relay_registry::{ActiveRelayTarget, RelayPacketMailbox, RelayTargetId},
    route_control::PacketLayerGate,
    source_route::{MediaRouteDestination, MediaRouteEntry, RemoteSourceRegistration},
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
    pub relays: Option<&'a [ActiveRelayTarget]>,
    pub source_gate: Option<PacketLayerGate>,
}

#[derive(Debug, Default)]
pub(in super::super) struct RouteTable {
    sources: BTreeMap<TransportMediaId, RouteSource>,
    forwarding_sources: usize,
    active: ActiveSpeakerRank,
    remote_gate_queue: VecDeque<TransportMediaId>,
    keyframe_requests: KeyframeRequestTracker,
}

impl RouteTable {
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
                    source.active_relay_targets()
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
    ) -> usize {
        let (index, became_forwarding) = self
            .sources
            .entry(source_id)
            .or_default()
            .add_consumer_route(destination);
        if became_forwarding {
            self.forwarding_sources += 1;
        }
        self.refresh_src_pkt_gate(source_id);
        index
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
        self.refresh_src_pkt_gate(source_id);
        self.prune_unrouted_remote_src(source_id);
        self.prune_empty(source_id);
        Some(removed)
    }

    #[cfg(test)]
    pub(in super::super) fn set_source_active(
        &mut self,
        source_id: TransportMediaId,
        active: bool,
    ) -> Result<(), TransportAdapterError> {
        self.sources
            .get_mut(&source_id)
            .ok_or(TransportAdapterError::TransportUnavailable)?
            .set_source_active(active);
        if !active {
            self.keyframe_requests.forget_source(source_id);
        }
        Ok(())
    }

    pub(in super::super) fn apply_source_activity(
        &mut self,
        source_id: TransportMediaId,
        update: SourceActivityUpdate,
    ) -> Result<bool, TransportAdapterError> {
        let accepted = self
            .sources
            .get_mut(&source_id)
            .ok_or(TransportAdapterError::TransportUnavailable)?
            .apply_source_activity(update);
        if accepted && !update.activity().is_active() {
            self.keyframe_requests.forget_source(source_id);
        }
        Ok(accepted)
    }

    pub(in super::super) fn set_consumer_active(
        &mut self,
        source_id: TransportMediaId,
        dst_idx: usize,
        session_key: &TransportSessionKey,
        media_id: TransportMediaId,
        active: bool,
    ) -> Result<ConsumerRouteUpdate, TransportAdapterError> {
        let update = self
            .sources
            .get_mut(&source_id)
            .ok_or(TransportAdapterError::TransportUnavailable)?
            .set_consumer_active(dst_idx, session_key, media_id, active)?;
        if update.route_changed {
            self.refresh_src_pkt_gate(source_id);
        }
        Ok(update)
    }

    pub(in super::super) fn set_consumer_pkt_gate(
        &mut self,
        source_id: TransportMediaId,
        dst_idx: usize,
        session_key: &TransportSessionKey,
        media_id: TransportMediaId,
        packet_gate: PacketLayerGate,
    ) -> Result<ConsumerRouteUpdate, TransportAdapterError> {
        self.sources
            .get_mut(&source_id)
            .ok_or(TransportAdapterError::TransportUnavailable)?
            .set_consumer_pkt_gate(dst_idx, session_key, media_id, packet_gate)
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
        self.refresh_src_pkt_gate(source_id);
        self.prune_unrouted_remote_src(source_id);
        self.prune_empty(source_id);
        Some(removed.route)
    }

    pub(in super::super) fn remove_dsts_for_session(
        &mut self,
        session_key: &TransportSessionKey,
        mut reindex: impl FnMut(TransportMediaId, &MediaRouteDestination, usize),
    ) {
        let mut affected = Vec::new();
        let mut removed_forwarding_sources = 0;
        for (source_id, source) in &mut self.sources {
            if let Some(change) = source.remove_dsts_for_session(session_key) {
                // Retaining destinations preserves fanout order but shifts MID
                // indexes. Repair surviving feedback targets before returning.
                if let Some(route) = source.local_route() {
                    for (dst_idx, destination) in route.destinations.iter().enumerate() {
                        reindex(*source_id, destination, dst_idx);
                    }
                }
                removed_forwarding_sources += usize::from(change.stopped_forwarding());
                affected.push(*source_id);
            }
        }
        self.forwarding_sources -= removed_forwarding_sources;
        for source_id in affected {
            self.refresh_src_pkt_gate(source_id);
            self.prune_unrouted_remote_src(source_id);
            self.prune_empty(source_id);
        }
    }

    pub(in super::super) fn refresh_src_pkt_gate(&mut self, source_id: TransportMediaId) {
        let Some(source) = self.sources.get_mut(&source_id) else {
            return;
        };
        let remote_packet_gate = source.refresh_route_pkt_gate();
        if source.publish_remote_pkt_gate(remote_packet_gate) {
            self.remote_gate_queue.push_back(source_id);
        }
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

    pub(in super::super) fn register_local_source(&mut self, source_id: TransportMediaId) {
        self.source_mut(source_id).producer.registered = true;
    }

    pub(in super::super) fn unregister_local_source(&mut self, source_id: TransportMediaId) {
        self.forget_packet_state(source_id);
        self.keyframe_requests.forget_source(source_id);
        self.prune_empty(source_id);
    }

    pub(in super::super) fn replace_producer_ssrcs(
        &mut self,
        source_id: TransportMediaId,
        ssrcs: Vec<Ssrc>,
    ) {
        self.source_mut(source_id).producer.ssrcs = ssrcs;
    }

    pub(in super::super) fn remember_producer_ssrc(
        &mut self,
        source_id: TransportMediaId,
        ssrc: Ssrc,
    ) {
        if let Some(source) = self.sources.get_mut(&source_id)
            && !source.producer.is_empty()
            && !source.producer.ssrcs.contains(&ssrc)
        {
            source.producer.ssrcs.push(ssrc);
        }
    }

    pub(in super::super) fn producer_ssrcs(&self, source_id: TransportMediaId) -> Option<&[Ssrc]> {
        self.sources
            .get(&source_id)
            .map(|source| source.producer.ssrcs.as_slice())
    }

    pub(in super::super) fn clear_producer_ssrcs(
        &mut self,
        source_id: TransportMediaId,
    ) -> Option<Vec<Ssrc>> {
        let source = self.sources.get_mut(&source_id)?;
        (!source.producer.is_empty()).then(|| mem::take(&mut source.producer.ssrcs))
    }

    pub(in super::super) fn register_remote_source(
        &mut self,
        source: &TransportSourceKey,
        source_control: RemoteSourceControl,
    ) -> Result<Option<RemoteSourceRegistration>, TransportAdapterError> {
        let source_id = source.transport_media_id();
        let registration = RemoteSourceRegistration::new(source.clone(), source_control);
        self.sources
            .entry(source_id)
            .or_default()
            .register_remote_source(source, registration)
    }

    pub(in super::super) fn restore_remote_source(
        &mut self,
        source_id: TransportMediaId,
        previous_registration: Option<RemoteSourceRegistration>,
    ) {
        if let Some(previous_registration) = previous_registration {
            let pending = previous_registration.has_pending_gate();
            self.sources
                .entry(source_id)
                .or_default()
                .restore_remote_source(previous_registration);
            if pending {
                self.queue_remote_gate(source_id);
            }
        } else {
            self.remove_remote_source(source_id);
        }
    }

    pub(in super::super) fn remote_source(
        &self,
        source_id: TransportMediaId,
    ) -> Option<&RemoteSourceRegistration> {
        self.sources.get(&source_id).and_then(RouteSource::remote)
    }

    #[cfg(any(test, feature = "internal-benchmarks"))]
    pub(in super::super) fn publish_remote_pkt_gate(
        &mut self,
        source_id: TransportMediaId,
        packet_gate: PacketLayerGate,
    ) {
        if self
            .sources
            .get_mut(&source_id)
            .is_some_and(|source| source.publish_remote_pkt_gate(packet_gate))
        {
            self.remote_gate_queue.push_back(source_id);
        }
    }

    pub(in super::super) fn flush_remote_pkt_gates(&mut self) {
        // bound this pass to the queue length at entry so a failed retry waits for the
        // next packet-loop turn instead of being retried immediately
        let count = self.remote_gate_queue.len();
        for _ in 0..count {
            let Some(source_id) = self.remote_gate_queue.pop_front() else {
                break;
            };
            if self
                .sources
                .get_mut(&source_id)
                .is_some_and(RouteSource::flush_remote_pkt_gate)
            {
                self.remote_gate_queue.push_back(source_id);
            }
        }
    }

    pub(in super::super) fn prune_unrouted_remote_src(&mut self, source_id: TransportMediaId) {
        if self
            .local_route(source_id)
            .is_some_and(|route| !route.destinations.is_empty())
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

    #[cfg(test)]
    pub(in super::super) fn producer_rid_is_ready(
        &self,
        source_id: TransportMediaId,
        rid: Rid,
        now: Instant,
        max_age: Duration,
    ) -> bool {
        self.sources
            .get(&source_id)
            .is_some_and(|source| source.producer.rid_is_ready(rid, now, max_age))
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

    #[cfg(test)]
    pub(in super::super) fn set_local_pkt_gate(
        &mut self,
        source_id: TransportMediaId,
        packet_gate: Option<PacketLayerGate>,
    ) {
        match self.sources.entry(source_id) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().set_local_pkt_gate(packet_gate);
                if entry.get().is_empty() {
                    entry.remove();
                }
            }
            Entry::Vacant(entry) => {
                let Some(packet_gate) = packet_gate else {
                    return;
                };
                let mut source = RouteSource::default();
                source.set_local_pkt_gate(Some(packet_gate));
                entry.insert(source);
            }
        }
        debug!(
            ?source_id,
            effective_packet_gate = ?self.effective_packet_gate(source_id),
            "updated source packet gate"
        );
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

    pub(in super::super) fn set_relay_pkt_gate(
        &mut self,
        source_id: TransportMediaId,
        target_id: RelayTargetId,
        packet_gate: PacketLayerGate,
    ) {
        self.sources
            .entry(source_id)
            .or_default()
            .set_relay_pkt_gate(target_id, packet_gate);
    }

    pub(in super::super) fn relay_packet_gate(
        &self,
        source_id: TransportMediaId,
        target_id: RelayTargetId,
    ) -> Option<&PacketLayerGate> {
        self.sources
            .get(&source_id)
            .and_then(|source| source.relay_packet_gate(target_id))
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

    #[cfg(test)]
    pub(in super::super) fn relay_targets_for_source(
        &self,
        source_id: TransportMediaId,
    ) -> Option<&[ActiveRelayTarget]> {
        self.sources
            .get(&source_id)
            .and_then(RouteSource::active_relay_targets)
    }

    pub(in super::super) fn add_relay_target(
        &mut self,
        source_id: TransportMediaId,
        target_id: RelayTargetId,
        target: RelayPacketMailbox,
    ) {
        if self
            .sources
            .entry(source_id)
            .or_default()
            .add_relay_target(target_id, target)
        {
            self.forwarding_sources += 1;
        }
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
        self.prune_empty(source_id);
    }

    pub(in super::super) fn set_relay_target_active(
        &mut self,
        source_id: TransportMediaId,
        target_id: RelayTargetId,
        active: bool,
    ) {
        if let Some(source) = self.sources.get_mut(&source_id) {
            source.set_relay_target_active(target_id, active);
        }
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

    #[cfg(test)]
    pub(in super::super) fn relay_target_count(&self, source_id: TransportMediaId) -> usize {
        self.sources
            .get(&source_id)
            .map_or(0, RouteSource::relay_target_count)
    }

    #[cfg(test)]
    pub(in super::super) fn active_relay_target_count(&self, source_id: TransportMediaId) -> usize {
        self.sources
            .get(&source_id)
            .map_or(0, RouteSource::active_relay_target_count)
    }

    fn remove_remote_source(&mut self, source_id: TransportMediaId) {
        if let Some(source) = self.sources.get_mut(&source_id) {
            source.remove_remote_source();
        }
        self.forget_packet_state(source_id);
        self.remote_gate_queue.retain(|queued| *queued != source_id);
        self.keyframe_requests.forget_source(source_id);
        self.prune_empty(source_id);
    }

    fn queue_remote_gate(&mut self, source_id: TransportMediaId) {
        if self
            .sources
            .get_mut(&source_id)
            .is_some_and(RouteSource::queue_remote_gate)
        {
            self.remote_gate_queue.push_back(source_id);
        }
    }

    fn prune_empty(&mut self, source_id: TransportMediaId) {
        if self
            .sources
            .get(&source_id)
            .is_some_and(RouteSource::is_empty)
        {
            self.sources.remove(&source_id);
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
