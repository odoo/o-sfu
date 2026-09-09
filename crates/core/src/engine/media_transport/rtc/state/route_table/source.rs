use std::{
    collections::BTreeMap,
    mem,
    time::{Duration, Instant},
};

use str0m::{
    media::{Mid, Rid},
    rtp::Ssrc,
};
use tracing::debug;

use super::{
    super::{
        relay_registry::{
            ActiveRelayTarget, RelayPacketMailbox, RelaySourceRegistration, RelayTargetId,
        },
        route_control::{
            PacketLayerGate, SourceAudioPolicyState, aggregate_packet_gates, intersect_packet_gates,
        },
        source_route::{
            DestinationKeyframeTarget, MediaRouteDestination, MediaRouteEntry,
            RemoteSourceRegistration,
        },
    },
    ConsumerRouteUpdate, RidReadinessScratch,
};
use crate::engine::media_transport::{
    ActiveSpeakerSource, ActiveSpeakerSourceDiagnostic, SourceActivityRevision,
    SourceActivityUpdate, TransportAdapterError, TransportMediaId, TransportRidActivity,
    TransportSessionKey, TransportSourceActivity, TransportSourceKey, rtc::codec,
};

#[derive(Default)]
pub struct RidReadinessRouteUpdate {
    pub suspended_stale_gate: bool,
    pub selected_gate: RidReadinessSelectedGateUpdate,
}

impl RidReadinessRouteUpdate {
    pub fn changed_gate(&self) -> bool {
        matches!(
            self.selected_gate,
            RidReadinessSelectedGateUpdate::Activated
                | RidReadinessSelectedGateUpdate::BootstrapFallback
        ) || self.suspended_stale_gate
    }

    fn mark_pending_selected_gate(&mut self) {
        if matches!(self.selected_gate, RidReadinessSelectedGateUpdate::None) {
            self.selected_gate = RidReadinessSelectedGateUpdate::Pending;
        }
    }

    fn mark_activated_pending_gate(&mut self) {
        self.selected_gate = RidReadinessSelectedGateUpdate::Activated;
    }

    fn mark_bootstrap_fallback(&mut self) {
        self.selected_gate = RidReadinessSelectedGateUpdate::BootstrapFallback;
    }

    fn mark_suspended_stale_gate(&mut self) {
        self.suspended_stale_gate = true;
    }
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub enum RidReadinessSelectedGateUpdate {
    #[default]
    None,
    Pending,
    Activated,
    BootstrapFallback,
}

#[derive(Debug)]
pub struct MovedConsumerRoute {
    pub session_key: TransportSessionKey,
    pub mid: Mid,
    pub media_id: TransportMediaId,
    pub dst_idx: usize,
}

#[derive(Debug)]
pub struct RemovedConsumerRoute {
    pub destination: MediaRouteDestination,
    pub moved: Option<MovedConsumerRoute>,
    pub(super) stopped_forwarding: bool,
}

#[derive(Clone, Copy)]
pub(super) enum ForwardingChange {
    StillForwarding,
    StoppedForwarding,
}

impl ForwardingChange {
    pub(super) const fn stopped_forwarding(self) -> bool {
        matches!(self, Self::StoppedForwarding)
    }
}

pub(super) struct RemovedRoute {
    pub(super) route: MediaRouteEntry,
    pub(super) stopped_forwarding: bool,
}

#[derive(Debug)]
pub(super) struct RouteSource {
    source_active: bool,
    source_activity_revision: SourceActivityRevision,
    local_route: Option<MediaRouteEntry>,
    remote: RemoteSourceState,
    relay: Option<RelaySourceRegistration>,
    packet: SourcePacketState,
    pub(super) producer: ProducerSourceState,
}

impl Default for RouteSource {
    fn default() -> Self {
        Self {
            source_active: true,
            source_activity_revision: SourceActivityRevision::default(),
            local_route: None,
            remote: RemoteSourceState::default(),
            relay: None,
            packet: SourcePacketState::default(),
            producer: ProducerSourceState::default(),
        }
    }
}

#[derive(Debug, Default)]
enum RemoteSourceState {
    #[default]
    None,
    Registered {
        registration: RemoteSourceRegistration,
        queued: bool,
    },
}

#[derive(Debug, Default)]
struct SourcePacketState {
    local: Option<PacketLayerGate>,
    relays: BTreeMap<RelayTargetId, PacketLayerGate>,
    audio: Option<SourceAudioPolicyState>,
    effective: Option<PacketLayerGate>,
}

impl RouteSource {
    pub(super) const fn source_is_active(&self) -> bool {
        self.source_active
    }

    pub(super) fn local_route(&self) -> Option<&MediaRouteEntry> {
        self.local_route.as_ref()
    }

    pub(super) fn remote(&self) -> Option<&RemoteSourceRegistration> {
        self.remote.registration()
    }

    pub(super) fn active_relay_targets(&self) -> Option<&[ActiveRelayTarget]> {
        self.relay.as_ref().and_then(|registration| {
            registration
                .has_active_targets()
                .then(|| registration.active_targets())
        })
    }

    pub(super) fn add_consumer_route(
        &mut self,
        destination: MediaRouteDestination,
    ) -> (usize, bool) {
        let became_forwarding = self.local_route.is_none() && self.relay.is_none();
        let route = self.local_route.get_or_insert_with(MediaRouteEntry::new);
        let index = route.destinations.len();
        route.push_destination(destination);
        (index, became_forwarding)
    }

    pub(super) fn remove_consumer_route(
        &mut self,
        session_key: &TransportSessionKey,
        media_id: TransportMediaId,
    ) -> Option<RemovedConsumerRoute> {
        let route = self.local_route.as_mut()?;
        let index = route.destinations.iter().position(|destination| {
            destination.dest_session == *session_key
                && destination.dest_transport_media_id == media_id
        })?;
        let destination = route.remove_destination(index);
        let moved = route
            .destinations
            .get(index)
            .map(|destination| MovedConsumerRoute {
                session_key: destination.dest_session.clone(),
                mid: destination.dest_mid,
                media_id: destination.dest_transport_media_id,
                dst_idx: index,
            });
        let stopped_forwarding = route.destinations.is_empty() && self.relay.is_none();
        if route.destinations.is_empty() {
            self.local_route = None;
        }
        Some(RemovedConsumerRoute {
            destination,
            moved,
            stopped_forwarding,
        })
    }

    pub(super) fn set_source_active(&mut self, active: bool) {
        if self.source_active == active {
            return;
        }
        self.source_active = active;
        if let Some(route) = &mut self.local_route {
            if active {
                route.advance_delivery();
            } else {
                route.pause_delivery();
            }
        }
    }

    pub(super) fn apply_source_activity(&mut self, update: SourceActivityUpdate) -> bool {
        let active = update.activity().is_active();
        if update.revision() < self.source_activity_revision
            || (update.revision() == self.source_activity_revision && active == self.source_active)
        {
            return false;
        }
        self.source_activity_revision = update.revision();
        self.set_source_active(active);
        true
    }

    pub(super) fn set_consumer_active(
        &mut self,
        dst_idx: usize,
        session_key: &TransportSessionKey,
        media_id: TransportMediaId,
        active: bool,
    ) -> Result<ConsumerRouteUpdate, TransportAdapterError> {
        let route = self
            .local_route
            .as_mut()
            .ok_or(TransportAdapterError::TransportUnavailable)?;
        let destination = validate_destination(route, dst_idx, session_key, media_id)?;
        let repair_enabled = destination.repair_enabled;
        let changed = route.set_destination_active(dst_idx, active);
        Ok(ConsumerRouteUpdate::new(changed, changed && repair_enabled))
    }

    pub(super) fn set_consumer_pkt_gate(
        &mut self,
        dst_idx: usize,
        session_key: &TransportSessionKey,
        media_id: TransportMediaId,
        packet_gate: PacketLayerGate,
    ) -> Result<ConsumerRouteUpdate, TransportAdapterError> {
        let route = self
            .local_route
            .as_mut()
            .ok_or(TransportAdapterError::TransportUnavailable)?;
        let dst = validate_destination(route, dst_idx, session_key, media_id)?;
        if dst.packet_gate == packet_gate {
            let changed = dst.pending_gate.take().is_some();
            return Ok(ConsumerRouteUpdate::new(changed, false));
        }
        if dst.pending_gate == Some(packet_gate) {
            return Ok(ConsumerRouteUpdate::new(false, false));
        }
        if dst.requires_decoder_refresh {
            dst.packet_gate = PacketLayerGate::Block;
            dst.pending_gate = Some(packet_gate);
        } else {
            dst.packet_gate = packet_gate;
            dst.pending_gate = None;
        }
        dst.advance_delivery();
        Ok(ConsumerRouteUpdate::new(true, dst.repair_enabled))
    }

    pub(super) fn update_decoder_readiness(
        &mut self,
        source_id: TransportMediaId,
        incoming_rid: Option<Rid>,
        is_keyframe: bool,
        scratch: &mut RidReadinessScratch,
        on_repair_delivery_changed: impl FnMut(&MediaRouteDestination),
    ) -> RidReadinessRouteUpdate {
        if !self.source_active {
            return RidReadinessRouteUpdate::default();
        }
        let Some(route) = self.local_route.as_mut() else {
            return RidReadinessRouteUpdate::default();
        };
        update_decoder_ready_dsts(
            route,
            source_id,
            incoming_rid,
            is_keyframe,
            scratch,
            on_repair_delivery_changed,
        )
    }

    pub(super) fn take_route(&mut self) -> Option<RemovedRoute> {
        let route = self.local_route.take()?;
        Some(RemovedRoute {
            stopped_forwarding: self.relay.is_none(),
            route,
        })
    }

    pub(super) fn remove_dsts_for_session(
        &mut self,
        session_key: &TransportSessionKey,
    ) -> Option<ForwardingChange> {
        let route = self.local_route.as_mut()?;
        let destination_count = route.destinations.len();
        route
            .destinations
            .retain(|destination| destination.dest_session != *session_key);
        if route.destinations.len() == destination_count {
            return None;
        }
        route.active_destination_count = route
            .destinations
            .iter()
            .filter(|destination| destination.active)
            .count();
        let stopped_forwarding = route.destinations.is_empty() && self.relay.is_none();
        if route.destinations.is_empty() {
            self.local_route = None;
        }
        Some(if stopped_forwarding {
            ForwardingChange::StoppedForwarding
        } else {
            ForwardingChange::StillForwarding
        })
    }

    pub(super) fn refresh_route_pkt_gate(&mut self) -> PacketLayerGate {
        let route = self.local_route.as_ref();
        let local_packet_gate = route.and_then(local_src_pkt_gate);
        let remote_packet_gate = remote_pkt_gate_for_route(route, local_packet_gate);
        self.set_local_pkt_gate(local_packet_gate);
        remote_packet_gate
    }

    pub(super) fn has_kf_demand(&self, rid: Option<Rid>) -> bool {
        if !self.source_active {
            return false;
        }
        let local_demand = self.local_route.as_ref().is_some_and(|route| {
            route.destinations.iter().any(|destination| {
                destination.active
                    && matches!(
                        destination.keyframe_target_rid(None),
                        DestinationKeyframeTarget::Current(target_rid)
                            if rid.is_none() || target_rid == rid
                    )
            })
        });
        local_demand || self.active_relay_targets().is_some()
    }

    pub(super) fn register_remote_source(
        &mut self,
        source: &TransportSourceKey,
        registration: RemoteSourceRegistration,
    ) -> Result<Option<RemoteSourceRegistration>, TransportAdapterError> {
        let previous = self.remote.register(source, registration)?;
        // first remote registration starts inactive until a
        // `SetRemoteSourceActivity` update arrives
        // re-registration keeps the activity already learned
        if previous.is_none() {
            self.set_source_active(false);
        }
        Ok(previous)
    }

    pub(super) fn restore_remote_source(&mut self, registration: RemoteSourceRegistration) {
        self.remote.restore(registration);
    }

    pub(super) fn remove_remote_source(&mut self) {
        self.remote.remove();
    }

    pub(super) fn publish_remote_pkt_gate(&mut self, packet_gate: PacketLayerGate) -> bool {
        self.remote.publish_gate(packet_gate)
    }

    pub(super) fn flush_remote_pkt_gate(&mut self) -> bool {
        self.remote.flush_gate()
    }

    pub(super) fn queue_remote_gate(&mut self) -> bool {
        self.remote.queue_gate()
    }

    pub(super) fn diagnostic(
        &self,
        source_id: TransportMediaId,
        source_last_packet_age: Option<Duration>,
        now: Instant,
    ) -> Option<TransportSourceActivity> {
        let rids = self.producer.rids.as_slice();
        let rid_last_seen = rids.iter().map(|rid| rid.last_seen).max();
        let last_keyframe = self
            .producer
            .last_keyframe
            .into_iter()
            .chain(rids.iter().filter_map(|rid| rid.last_keyframe))
            .max();
        let last_packet_age = source_last_packet_age
            .or_else(|| rid_last_seen.map(|last_seen| now.saturating_duration_since(last_seen)))
            .or_else(|| {
                last_keyframe.map(|last_keyframe| now.saturating_duration_since(last_keyframe))
            })?;
        Some(TransportSourceActivity::new(
            source_id,
            last_packet_age,
            last_keyframe.map(|last_keyframe| now.saturating_duration_since(last_keyframe)),
            rids.iter().map(|rid| rid.diagnostic(now)).collect(),
        ))
    }

    pub(super) fn active_speaker_source(
        &self,
        source_id: TransportMediaId,
        now: Instant,
    ) -> Option<ActiveSpeakerSource> {
        self.packet
            .audio
            .as_ref()
            .and_then(|audio| audio.active_speaker_source(source_id, now))
    }

    pub(super) fn active_speaker_diagnostic(
        &self,
        source_id: TransportMediaId,
        now: Instant,
    ) -> Option<ActiveSpeakerSourceDiagnostic> {
        self.packet
            .audio
            .as_ref()
            .map(|audio| audio.diagnostic(source_id, now))
    }

    pub(super) fn observe_audio_activity(
        &mut self,
        voice_activity: Option<bool>,
        audio_level_dbov: Option<i8>,
        now: Instant,
    ) -> bool {
        let previous_gate = self
            .local_route
            .is_some()
            .then(|| self.packet_filter_gate());
        let changed = self
            .packet
            .observe_audio_activity(voice_activity, audio_level_dbov, now);
        if previous_gate.is_some_and(|gate| gate != self.packet_filter_gate()) {
            self.advance_local_delivery();
        }
        changed
    }

    pub(super) fn set_local_pkt_gate(&mut self, gate: Option<PacketLayerGate>) {
        self.packet.set_local_gate(gate);
    }

    pub(super) fn set_relay_pkt_gate(&mut self, target_id: RelayTargetId, gate: PacketLayerGate) {
        self.packet.set_relay_gate(target_id, gate);
    }

    pub(super) fn relay_packet_gate(&self, target_id: RelayTargetId) -> Option<&PacketLayerGate> {
        self.packet.relays.get(&target_id)
    }

    pub(super) fn next_active_speaker_deadline(&self, now: Instant) -> Option<Instant> {
        self.packet
            .audio
            .as_ref()
            .and_then(|audio| audio.active_deadline_after(now))
    }

    pub(super) const fn effective_packet_gate(&self) -> Option<PacketLayerGate> {
        self.packet.effective
    }

    pub(super) const fn packet_filter_gate(&self) -> Option<PacketLayerGate> {
        match self.packet.effective {
            Some(PacketLayerGate::Open) | None => None,
            gate => gate,
        }
    }

    pub(super) fn add_relay_target(
        &mut self,
        target_id: RelayTargetId,
        target: RelayPacketMailbox,
    ) -> bool {
        let became_forwarding = self.local_route.is_none() && self.relay.is_none();
        self.relay
            .get_or_insert_with(RelaySourceRegistration::default)
            .add_target(target_id, target);
        became_forwarding
    }

    pub(super) fn remove_relay_target(
        &mut self,
        target_id: RelayTargetId,
    ) -> Option<ForwardingChange> {
        let registration = self.relay.as_mut()?;
        if registration.remove_target(target_id)? {
            self.relay = None;
        }
        let change = if self.relay.is_none() && self.local_route.is_none() {
            ForwardingChange::StoppedForwarding
        } else {
            ForwardingChange::StillForwarding
        };
        self.packet.forget_relay_gate(target_id);
        Some(change)
    }

    pub(super) fn set_relay_target_active(&mut self, target_id: RelayTargetId, active: bool) {
        if let Some(registration) = self.relay.as_mut() {
            registration.set_target_active(target_id, active);
        }
    }

    pub(super) fn is_relay_target_active(&self, target_id: RelayTargetId) -> bool {
        self.relay
            .as_ref()
            .is_some_and(|registration| registration.is_target_active(target_id))
    }

    #[cfg(test)]
    pub(super) fn relay_target_count(&self) -> usize {
        self.relay
            .as_ref()
            .map_or(0, RelaySourceRegistration::target_count)
    }

    #[cfg(test)]
    pub(super) fn active_relay_target_count(&self) -> usize {
        self.relay
            .as_ref()
            .map_or(0, RelaySourceRegistration::active_target_count)
    }

    pub(super) fn forget_packet_state(&mut self) {
        let previous_gate = self.packet_filter_gate();
        self.packet.clear();
        if previous_gate != self.packet_filter_gate() {
            self.advance_local_delivery();
        }
        self.producer = ProducerSourceState::default();
    }

    fn advance_local_delivery(&mut self) {
        if let Some(route) = &mut self.local_route {
            route.advance_delivery();
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.local_route.is_none()
            && self.relay.is_none()
            && self.remote.is_empty()
            && self.packet.is_empty()
            && self.producer.is_empty()
    }
}

impl RemoteSourceState {
    fn registration(&self) -> Option<&RemoteSourceRegistration> {
        match self {
            Self::None => None,
            Self::Registered { registration, .. } => Some(registration),
        }
    }

    fn register(
        &mut self,
        source: &TransportSourceKey,
        registration: RemoteSourceRegistration,
    ) -> Result<Option<RemoteSourceRegistration>, TransportAdapterError> {
        match self {
            Self::None => {
                *self = Self::Registered {
                    registration,
                    queued: false,
                };
                Ok(None)
            }
            Self::Registered {
                registration: current,
                ..
            } if current.source() == source => Ok(Some(mem::replace(current, registration))),
            Self::Registered { .. } => Err(TransportAdapterError::InvalidInput),
        }
    }

    fn restore(&mut self, registration: RemoteSourceRegistration) {
        // rollback restore keeps retry membership with the temporary registration
        let queued = match self {
            Self::Registered { queued, .. } => *queued,
            Self::None => false,
        };
        *self = Self::Registered {
            registration,
            queued,
        };
    }

    fn remove(&mut self) {
        *self = Self::None;
    }

    fn publish_gate(&mut self, packet_gate: PacketLayerGate) -> bool {
        let Self::Registered {
            registration,
            queued,
        } = self
        else {
            return false;
        };
        if !registration.publish_packet_gate_needs_retry(packet_gate) {
            return false;
        }
        if *queued {
            return false;
        }
        *queued = true;
        true
    }

    fn flush_gate(&mut self) -> bool {
        let Self::Registered {
            registration,
            queued,
        } = self
        else {
            return false;
        };
        let needs_retry = registration.flush_pending_gate();
        *queued = needs_retry;
        needs_retry
    }

    fn queue_gate(&mut self) -> bool {
        let Self::Registered { queued, .. } = self else {
            return false;
        };
        if *queued {
            return false;
        }
        *queued = true;
        true
    }

    const fn is_empty(&self) -> bool {
        matches!(self, Self::None)
    }
}

impl SourcePacketState {
    fn observe_audio_activity(
        &mut self,
        voice_activity: Option<bool>,
        audio_level_dbov: Option<i8>,
        now: Instant,
    ) -> bool {
        let previous = self.audio.clone();
        let Some(mut audio) = self.audio.take().or_else(|| {
            (voice_activity.is_some() || audio_level_dbov.is_some())
                .then(SourceAudioPolicyState::default)
        }) else {
            return false;
        };
        audio.observe_packet(voice_activity, audio_level_dbov, now);
        self.audio = Some(audio);
        let changed = self.audio != previous;
        if changed {
            self.refresh_effective();
        }
        changed
    }

    fn set_local_gate(&mut self, gate: Option<PacketLayerGate>) {
        self.local = gate;
        self.refresh_effective();
    }

    fn set_relay_gate(&mut self, target_id: RelayTargetId, gate: PacketLayerGate) {
        self.relays.insert(target_id, gate);
        self.refresh_effective();
    }

    fn clear(&mut self) {
        self.audio = None;
        self.local = None;
        self.relays.clear();
        self.effective = None;
    }

    fn is_empty(&self) -> bool {
        self.audio.is_none()
            && self.local.is_none()
            && self.relays.is_empty()
            && self.effective.is_none()
    }

    fn forget_relay_gate(&mut self, target_id: RelayTargetId) {
        self.relays.remove(&target_id);
        self.refresh_effective();
    }

    fn refresh_effective(&mut self) {
        self.effective = intersect_packet_gates(
            aggregate_packet_gates(self.local.iter().chain(self.relays.values())),
            self.audio.as_ref().map(SourceAudioPolicyState::packet_gate),
        );
    }
}

#[derive(Debug, Default)]
pub(super) struct ProducerSourceState {
    pub(super) registered: bool,
    pub(super) ssrcs: Vec<Ssrc>,
    pub(super) packet_inspector: codec::PacketInspector,
    last_keyframe: Option<Instant>,
    rids: Vec<ProducerRidLiveness>,
}

impl ProducerSourceState {
    pub(super) fn observe_packet(
        &mut self,
        rid: Option<Rid>,
        is_keyframe: bool,
        now: Instant,
    ) -> bool {
        let Some(rid) = rid else {
            if is_keyframe {
                self.last_keyframe = Some(now);
            }
            return false;
        };
        if let Some(liveness) = self.rids.iter_mut().find(|liveness| liveness.rid == rid) {
            liveness.observe(is_keyframe, now);
            return false;
        }
        self.rids.push(ProducerRidLiveness::new_with_keyframe(
            rid,
            is_keyframe,
            now,
        ));
        true
    }

    #[cfg(test)]
    pub(super) fn rid_is_ready(&self, rid: Rid, now: Instant, max_age: Duration) -> bool {
        self.rids
            .iter()
            .any(|liveness| liveness.rid == rid && liveness.is_ready(now, max_age))
    }

    pub(super) fn collect_ready_rids(
        &self,
        now: Instant,
        max_age: Duration,
        ready_rids: &mut Vec<Rid>,
    ) {
        ready_rids.extend(
            self.rids
                .iter()
                .filter(|liveness| liveness.is_ready(now, max_age))
                .map(|liveness| liveness.rid),
        );
    }

    pub(super) fn is_empty(&self) -> bool {
        !self.registered
            && self.ssrcs.is_empty()
            && self.packet_inspector.is_empty()
            && self.last_keyframe.is_none()
            && self.rids.is_empty()
    }
}

#[derive(Debug, Clone)]
struct ProducerRidLiveness {
    rid: Rid,
    last_seen: Instant,
    last_keyframe: Option<Instant>,
}

impl ProducerRidLiveness {
    fn new_with_keyframe(rid: Rid, is_keyframe: bool, observed_at: Instant) -> Self {
        Self {
            rid,
            last_seen: observed_at,
            last_keyframe: is_keyframe.then_some(observed_at),
        }
    }

    fn observe(&mut self, is_keyframe: bool, observed_at: Instant) {
        self.last_seen = observed_at;
        if is_keyframe {
            self.last_keyframe = Some(observed_at);
        }
    }

    fn is_ready(&self, now: Instant, max_age: Duration) -> bool {
        now.duration_since(self.last_seen) <= max_age
    }

    fn diagnostic(&self, now: Instant) -> TransportRidActivity {
        TransportRidActivity::new(
            self.rid.to_string(),
            now.saturating_duration_since(self.last_seen),
            self.last_keyframe
                .map(|last_keyframe| now.saturating_duration_since(last_keyframe)),
        )
    }
}

fn validate_destination<'a>(
    route: &'a mut MediaRouteEntry,
    index: usize,
    session_key: &TransportSessionKey,
    media_id: TransportMediaId,
) -> Result<&'a mut MediaRouteDestination, TransportAdapterError> {
    let dst = route
        .destinations
        .get_mut(index)
        .ok_or(TransportAdapterError::TransportUnavailable)?;
    if dst.dest_session != *session_key || dst.dest_transport_media_id != media_id {
        return Err(TransportAdapterError::TransportUnavailable);
    }
    Ok(dst)
}

fn update_decoder_ready_dsts(
    route: &mut MediaRouteEntry,
    source_id: TransportMediaId,
    incoming_rid: Option<Rid>,
    is_keyframe: bool,
    scratch: &mut RidReadinessScratch,
    mut on_repair_delivery_changed: impl FnMut(&MediaRouteDestination),
) -> RidReadinessRouteUpdate {
    let mut update = RidReadinessRouteUpdate::default();
    for dst in &mut route.destinations {
        if let Some(incoming_rid) = incoming_rid {
            suspend_stale_dst_gate(
                dst,
                source_id,
                incoming_rid,
                &scratch.ready,
                &mut scratch.stale,
                &mut on_repair_delivery_changed,
                &mut update,
            );
        }
        let Some(pending_gate) = dst.pending_gate else {
            continue;
        };
        if let Some(selected_rid) = pending_gate.selected_rid() {
            push_unique_rid(&mut scratch.pending_selected, selected_rid);
            if Some(selected_rid) != incoming_rid {
                continue;
            }
        } else if !matches!(pending_gate, PacketLayerGate::Open) {
            continue;
        }
        update.mark_pending_selected_gate();
        if is_keyframe {
            debug!(
                ?source_id,
                consumer_session_key = ?dst.dest_session,
                consumer_transport_media_id = ?dst.dest_transport_media_id,
                ?incoming_rid,
                activated_packet_gate = ?pending_gate,
                "activated deferred strict RID packet gate after producer RID became live"
            );
            dst.activate_refresh(pending_gate);
            notify_repair_delivery_changed(dst, &mut on_repair_delivery_changed);
            update.mark_activated_pending_gate();
        }
    }
    if is_keyframe
        && update.selected_gate != RidReadinessSelectedGateUpdate::Activated
        && let Some(incoming_rid) = incoming_rid
    {
        activate_bootstrap_dsts(
            route,
            source_id,
            incoming_rid,
            &mut on_repair_delivery_changed,
            &mut update,
        );
    }
    update
}

fn suspend_stale_dst_gate(
    dst: &mut MediaRouteDestination,
    source_id: TransportMediaId,
    incoming_rid: Rid,
    ready: &[Rid],
    stale: &mut Vec<Rid>,
    on_repair_delivery_changed: &mut impl FnMut(&MediaRouteDestination),
    update: &mut RidReadinessRouteUpdate,
) {
    if dst.pending_gate.is_some() {
        return;
    }
    let Some(selected_rid) = dst.packet_gate.selected_rid() else {
        return;
    };
    if selected_rid == incoming_rid || ready.contains(&selected_rid) {
        return;
    }
    let packet_gate = dst.packet_gate;
    debug!(
        ?source_id,
        consumer_session_key = ?dst.dest_session,
        consumer_transport_media_id = ?dst.dest_transport_media_id,
        ?incoming_rid,
        stale_rid = ?selected_rid,
        pending_packet_gate = ?packet_gate,
        "blocked stale selected RID route until selected producer RID resumes"
    );
    dst.pause_delivery();
    push_unique_rid(stale, selected_rid);
    notify_repair_delivery_changed(dst, on_repair_delivery_changed);
    update.mark_suspended_stale_gate();
}

fn activate_bootstrap_dsts(
    route: &mut MediaRouteEntry,
    source_id: TransportMediaId,
    incoming_rid: Rid,
    on_repair_delivery_changed: &mut impl FnMut(&MediaRouteDestination),
    update: &mut RidReadinessRouteUpdate,
) {
    for dst in &mut route.destinations {
        let Some(selected_rid) = dst
            .pending_gate
            .as_ref()
            .and_then(PacketLayerGate::selected_rid)
        else {
            continue;
        };
        if selected_rid == incoming_rid || !matches!(dst.packet_gate, PacketLayerGate::Block) {
            continue;
        }
        debug!(
            ?source_id,
            consumer_session_key = ?dst.dest_session,
            consumer_transport_media_id = ?dst.dest_transport_media_id,
            fallback_rid = ?incoming_rid,
            pending_selected_rid = ?selected_rid,
            "activated bootstrap fallback RID packet gate while selected producer RID is pending"
        );
        dst.activate_bootstrap_refresh(PacketLayerGate::Rid(incoming_rid));
        notify_repair_delivery_changed(dst, on_repair_delivery_changed);
        update.mark_bootstrap_fallback();
    }
}

fn notify_repair_delivery_changed(
    destination: &MediaRouteDestination,
    on_changed: &mut impl FnMut(&MediaRouteDestination),
) {
    if destination.repair_enabled {
        on_changed(destination);
    }
}

fn push_unique_rid(rids: &mut Vec<Rid>, rid: Rid) {
    if !rids.contains(&rid) {
        rids.push(rid);
    }
}

fn local_src_pkt_gate(route_entry: &MediaRouteEntry) -> Option<PacketLayerGate> {
    aggregate_packet_gates(
        route_entry
            .destinations
            .iter()
            .filter(|destination| destination.active)
            .map(|destination| &destination.packet_gate),
    )
}

fn remote_pkt_gate_for_route(
    route_entry: Option<&MediaRouteEntry>,
    local_packet_gate: Option<PacketLayerGate>,
) -> PacketLayerGate {
    // Keep the upstream relay open while a local route can receive. The consumer
    // worker needs every RID to detect a stale selection and admit a decodable
    // fallback.
    match (route_entry, local_packet_gate) {
        (Some(_), Some(PacketLayerGate::Open | PacketLayerGate::Rid(_))) => PacketLayerGate::Open,
        // A pending decoder-refresh gate needs Open upstream so its required
        // refresh can arrive despite the aggregate Block.
        (Some(route_entry), Some(PacketLayerGate::Block))
            if route_entry
                .destinations
                .iter()
                .any(|destination| destination.pending_gate.is_some()) =>
        {
            PacketLayerGate::Open
        }
        (_, Some(packet_gate)) => packet_gate,
        (_, None) => PacketLayerGate::Block,
    }
}
