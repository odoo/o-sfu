//! Packet observation and fanout in staged order.
//!
//! [`PacketForwarder`] completes observation, planning and destination writes for
//! each packet before the next packet can change route state. Origin sinks run
//! before relays and local RTC. Relayed packets skip origin sinks and extra hops.
//!
//! RID readiness is observed across the batch before broad ingress recovery and
//! source-policy wakeups are flushed. Reusable scratch and the sink route cache
//! stay with this owner. Room policy must already be projected into route gates.

use core::hint::cold_path;
#[cfg(feature = "internal-benchmarks")]
use std::mem::take;
use std::time::Instant;

use str0m::media::{KeyframeRequestKind, MediaKind, Rid};
use tokio::sync::mpsc;
use tracing::debug;

use super::{
    super::{
        recovery::{
            KeyframeRequestMode, KeyframeRequestTarget, apply_src_decoder_ready,
            request_kf_for_target,
        },
        state::{
            PacketLoopState, media_registry::RegisteredMediaHandle,
            relay_registry::RelayEnqueueOutcome,
        },
    },
    forwarded_packet::{ForwardedPacket, ForwardedPacketSource, PacketFacts},
    forwarding_destination::{ForwardingDestination, relay_enqueue_result},
    forwarding_planner::{PacketGateDecision, plan_forwards},
};
use crate::engine::{
    RoomInstanceId,
    media_transport::{SourcePolicySignal, TransportMediaId, TransportSessionKey},
    metrics::{
        RtcKeyframeRequestOutcome, RtcMetricsRecorder, RtcRouteControlOutcome,
        RtpDecoderRefreshScope, RtpForwardDestinationKind, RtpMetricsRecorder, RtpRelayDropKind,
        RuntimeMetrics,
    },
    packet_sink_registry::{PacketSinkRouteCache, RoomPacketSinkRegistry},
};

#[cfg(any(test, feature = "internal-benchmarks"))]
#[path = "forward_flush/TESTS/support.rs"]
pub(super) mod test_support;

/// Effects produced while forwarding a batch on one RTC worker.
pub(in super::super) struct ForwardingEffects<'a> {
    pub packet_sinks: &'a RoomPacketSinkRegistry,
    pub source_policy_signal: &'a SourcePolicySignal,
    pub metrics: &'a RuntimeMetrics,
    pub rtp_metrics: &'a RtpMetricsRecorder,
    pub rtc_metrics: &'a RtcMetricsRecorder,
}

/// Per-worker forwarding state with scratch retained across packet batches.
///
/// Each plan expires before the next packet is observed. Batch completion
/// consumes deferred observations while preserving all allocation capacities.
pub(in super::super) struct PacketForwarder {
    packet_sink_cache: PacketSinkRouteCache,
    forwards: Vec<ForwardingDestination>,
    observed_rids: Vec<(TransportMediaId, Rid)>,
    pending_first_video_keyframes: Vec<PendingFirstVideoKeyframe>,
    rid_readiness_changed_sources: Vec<TransportMediaId>,
    dirty_source_policy_channel_ids: Vec<RoomInstanceId>,
    #[cfg(feature = "internal-benchmarks")]
    planned_forwards: usize,
}

struct PendingFirstVideoKeyframe {
    source: ForwardedPacketSource,
    src_media: TransportMediaId,
    observed_at: Instant,
}

impl Default for PacketForwarder {
    fn default() -> Self {
        Self {
            packet_sink_cache: PacketSinkRouteCache::default(),
            forwards: Vec::with_capacity(64),
            observed_rids: Vec::with_capacity(8),
            pending_first_video_keyframes: Vec::with_capacity(8),
            rid_readiness_changed_sources: Vec::with_capacity(8),
            dirty_source_policy_channel_ids: Vec::with_capacity(8),
            #[cfg(feature = "internal-benchmarks")]
            planned_forwards: 0,
        }
    }
}

impl PacketForwarder {
    /// Observes and forwards a batch before dispatching its deferred recovery.
    ///
    /// Unresolved packets are omitted. Destination failures remain isolated to
    /// that destination. Successful local writes mark sessions for their next
    /// drain. Packet order and origin-sink precedence are preserved.
    ///
    /// The packet slice remains available to its staging owner. All forwarding
    /// plans and observation scratch are empty when this operation returns.
    pub(in super::super) fn forward_batch(
        &mut self,
        state: &mut PacketLoopState,
        packets: &mut [ForwardedPacket],
        effects: &ForwardingEffects<'_>,
    ) {
        self.packet_sink_cache.refresh_from(effects.packet_sinks);
        for packet in packets {
            // A later refresh must not admit an earlier delta from this batch.
            let visits_origin = packet.visits_origin_sinks();
            let Some(facts) = record_incoming_packet(
                state,
                effects.rtc_metrics,
                effects.rtp_metrics,
                self,
                packet,
            ) else {
                continue;
            };
            if let Some(decision) = plan_forwards(
                facts,
                visits_origin,
                &state.routes,
                &self.packet_sink_cache,
                &mut self.forwards,
            ) {
                effects
                    .rtc_metrics
                    .record_rtc_route_control(match decision {
                        PacketGateDecision::Allowed => RtcRouteControlOutcome::LayerAllowed,
                        PacketGateDecision::Dropped => RtcRouteControlOutcome::LayerDropped,
                    });
            }
            flush_packet_forwards(
                state,
                effects.metrics,
                effects.rtp_metrics,
                effects.rtc_metrics,
                packet,
                &self.forwards,
            );
            #[cfg(feature = "internal-benchmarks")]
            {
                self.planned_forwards = self.planned_forwards.saturating_add(self.forwards.len());
            }
            self.forwards.clear();
        }
        finish_incoming_stats(
            state,
            effects.source_policy_signal,
            effects.rtc_metrics,
            self,
        );
        self.observed_rids.clear();
    }

    #[cfg(feature = "internal-benchmarks")]
    pub(in super::super) fn take_planned_forwards_for_benchmark(&mut self) -> usize {
        take(&mut self.planned_forwards)
    }

    fn observe_rid_once(&mut self, src_media: TransportMediaId, rid: Rid) -> bool {
        if self.observed_rids.contains(&(src_media, rid)) {
            return false;
        }
        self.observed_rids.push((src_media, rid));
        true
    }

    fn push_first_video_keyframe(
        &mut self,
        source: &ForwardedPacketSource,
        src_media: TransportMediaId,
        observed_at: Instant,
    ) {
        if self
            .pending_first_video_keyframes
            .iter()
            .any(|pending| pending.src_media == src_media)
        {
            return;
        }
        self.pending_first_video_keyframes
            .push(PendingFirstVideoKeyframe {
                source: source.clone(),
                src_media,
                observed_at,
            });
    }

    fn flush_source_policy_dirty(&mut self, source_policy_signal: &SourcePolicySignal) {
        if self.dirty_source_policy_channel_ids.is_empty() {
            return;
        }
        self.dirty_source_policy_channel_ids.sort_unstable();
        self.dirty_source_policy_channel_ids.dedup();
        source_policy_signal.mark_dirty_rooms(self.dirty_source_policy_channel_ids.iter().copied());
        self.dirty_source_policy_channel_ids.clear();
    }
}

fn learn_producer_packet_binding(
    state: &mut PacketLoopState,
    packet: &ForwardedPacket,
    transport_media_id: TransportMediaId,
) {
    if packet.route_control_mid().is_none() {
        return;
    }
    // RFC 9143 associates an unknown SSRC through MID. RFC 8852 scopes RID and
    // repaired RID to that media section. Persist the session-checked binding
    // so later packets may omit those header extensions.
    // https://www.rfc-editor.org/rfc/rfc9143.html#section-9.2
    // https://www.rfc-editor.org/rfc/rfc8852.html#section-3
    let ssrc = packet.route_control_ssrc();
    let learned = state.learn_producer_ssrc_from_pkt(
        packet.source(),
        transport_media_id,
        ssrc,
        packet.route_control_rid_extension(),
    );
    if !learned || state.routes.source_is_active(transport_media_id) {
        return;
    }
    let ForwardedPacketSource::Local(session_handle) = packet.source() else {
        return;
    };
    let Some(session_state) = state.users.get_mut_by_handle(*session_handle) else {
        return;
    };
    let mut api = session_state.rtc.direct_api();
    if let Some(stream_rx) = api.stream_rx(&ssrc) {
        stream_rx.suppress_nack(true);
    }
}

/// Records source identity, activity, decoder readiness and bitrate for one
/// incoming packet.
///
/// Borrows cached facts for planning or returns `None` when source resolution fails.
/// `forwarder` stages policy wakeups and broad recovery while RID recovery may be
/// requested immediately.
pub(in super::super) fn record_incoming_packet<'a>(
    state: &mut PacketLoopState,
    control: &RtcMetricsRecorder,
    rtp: &RtpMetricsRecorder,
    forwarder: &mut PacketForwarder,
    packet: &'a mut ForwardedPacket,
) -> Option<&'a PacketFacts> {
    let _ = packet.resolve_facts(state);
    let facts = packet.cached_facts()?;
    let payload_len = packet.payload().len();
    let transport_media_id = facts.src_media;
    let decoder_refresh = facts.codec.decoder_refresh();
    learn_producer_packet_binding(state, packet, transport_media_id);
    let audio_policy_changed = state.routes.observe_audio_activity(
        transport_media_id,
        facts.voice_activity,
        facts.audio_level,
        packet.received_at(),
    );
    if decoder_refresh {
        let cleared = state
            .routes
            .observe_decoder_refresh(transport_media_id, facts.rid);
        for _ in 0..cleared {
            control.record_rtc_keyframe_request(RtcKeyframeRequestOutcome::Cleared);
        }
    }
    if audio_policy_changed {
        cold_path();
        forwarder
            .dirty_source_policy_channel_ids
            .push(facts.room_instance_id);
    }
    let packet_rid = facts.rid;
    if packet_rid.is_some() || decoder_refresh {
        state.routes.observe_producer_packet(
            transport_media_id,
            packet_rid,
            decoder_refresh,
            packet.received_at(),
        );
    }
    if decoder_refresh {
        let scope = if packet_rid.is_some() {
            RtpDecoderRefreshScope::Rid
        } else {
            RtpDecoderRefreshScope::Source
        };
        rtp.record_decoder_refresh(scope);
    }
    // Readiness scans every destination for the source. Delta packets cannot open
    // a gate, so one scan per source/RID per turn is enough. Decoder refreshes
    // remain uncoalesced because they can activate pending gates.
    let check_readiness = decoder_refresh
        || packet_rid.is_some_and(|rid| forwarder.observe_rid_once(transport_media_id, rid));
    if check_readiness {
        let route_changed = packet.src_key(state).cloned().is_some_and(|src_key| {
            apply_src_decoder_ready(
                state,
                control,
                &src_key,
                transport_media_id,
                packet_rid,
                decoder_refresh,
                packet.received_at(),
            )
        });
        if route_changed {
            forwarder
                .rid_readiness_changed_sources
                .push(transport_media_id);
        }
    }
    let bitrate_observation = state
        .record_incoming_bitrate(transport_media_id, packet.received_at(), payload_len)
        .unwrap_or_default();
    if bitrate_observation.policy_dirty() {
        forwarder
            .dirty_source_policy_channel_ids
            .push(facts.room_instance_id);
    }
    if bitrate_observation.ingress_started() {
        cold_path();
        let Some(src_key) = packet.src_key(state) else {
            return Some(facts);
        };
        debug!(
            user_id = ?src_key.user_id(),
            media_worker_id = src_key.media_worker_id().as_usize(),
            ?transport_media_id,
            payload_bytes = payload_len,
            "observed RTP ingress for published media"
        );
        // A later packet in this turn may change a RID gate by supplying its
        // refresh or triggering RID-specific recovery. Defer the broad ingress
        // PLI until the batch is fully observed.
        forwarder.push_first_video_keyframe(
            packet.source(),
            transport_media_id,
            packet.received_at(),
        );
    }
    rtp.record_ingress(payload_len);
    Some(facts)
}

/// Flushes deferred keyframe recovery and source-policy wakeups for one
/// observed packet batch.
pub(in super::super) fn finish_incoming_stats(
    state: &mut PacketLoopState,
    source_policy_signal: &SourcePolicySignal,
    control: &RtcMetricsRecorder,
    forwarder: &mut PacketForwarder,
) {
    flush_first_video_kfs(state, control, forwarder);
    forwarder.flush_source_policy_dirty(source_policy_signal);
}

fn flush_first_video_kfs(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    forwarder: &mut PacketForwarder,
) {
    for pending in forwarder.pending_first_video_keyframes.drain(..) {
        // A gate transition consumed a refresh or scheduled RID-specific
        // recovery. The source-wide ingress PLI would duplicate that work.
        if forwarder
            .rid_readiness_changed_sources
            .contains(&pending.src_media)
        {
            continue;
        }
        // The first packet after registration or a full idle window may be a
        // delta. A RID-unspecified PLI tells the producer that receiver
        // prediction may be broken.
        request_first_video_kf(
            state,
            metrics,
            &pending.source,
            pending.src_media,
            pending.observed_at,
        );
    }
    forwarder.rid_readiness_changed_sources.clear();
}

/// Requests source-wide recovery when active video ingress starts or resumes.
fn request_first_video_kf(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    source: &ForwardedPacketSource,
    transport_media_id: TransportMediaId,
    now: Instant,
) {
    let Some(src_key) = source.session_key(state).cloned() else {
        return;
    };
    request_first_video_kf_for_session(state, metrics, &src_key, transport_media_id, now);
}

fn request_first_video_kf_for_session(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    src_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
    now: Instant,
) {
    if state.routes.source_is_active(transport_media_id)
        && source_is_video(state, src_key, transport_media_id)
    {
        request_kf_for_target(
            state,
            metrics,
            KeyframeRequestTarget::Local(src_key, transport_media_id),
            None,
            KeyframeRequestKind::Pli,
            KeyframeRequestMode::for_recovery(
                now,
                state
                    .routes
                    .decoder_refresh_is_observable(transport_media_id),
            ),
        );
    }
}

fn source_is_video(
    state: &PacketLoopState,
    src_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
) -> bool {
    let Some(RegisteredMediaHandle::Producer { session_key, mid }) =
        state.media_handle(transport_media_id)
    else {
        return false;
    };
    if session_key != src_key {
        return false;
    }
    state
        .users
        .get(src_key)
        .and_then(|session_state| session_state.rtc.media(*mid))
        .is_some_and(|media| matches!(media.kind(), MediaKind::Video))
}

/// Drains at most `max_packets` from the relay mailbox.
///
/// Returns the number of packets appended to `pending_packets`.
pub fn drain_relay_packets(
    relay_rx: &mut mpsc::Receiver<ForwardedPacket>,
    pending_packets: &mut Vec<ForwardedPacket>,
    max_packets: usize,
    metrics: &RtcMetricsRecorder,
) -> usize {
    let mut drained_packets = 0;
    while drained_packets < max_packets {
        match relay_rx.try_recv() {
            Ok(packet) => {
                pending_packets.push(packet);
                drained_packets += 1;
            }
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                break;
            }
        }
    }
    let cap_hit = max_packets > 0 && drained_packets == max_packets && !relay_rx.is_empty();
    if drained_packets > 0 {
        metrics.record_rtc_relay_drain_batch(drained_packets, cap_hit);
    }
    drained_packets
}

/// Executes the forwarding destinations planned for one packet.
///
/// Stale local routes and failed relay enqueues are isolated to their
/// destination. Local RTC destinations enqueue into str0m and mark the session
/// dirty.
pub(in super::super) fn flush_packet_forwards(
    state: &mut PacketLoopState,
    metrics: &RuntimeMetrics,
    rtp_metrics: &RtpMetricsRecorder,
    rtc_recorder: &RtcMetricsRecorder,
    packet: &ForwardedPacket,
    forwards: &[ForwardingDestination],
) {
    let payload_len = packet.payload().len();
    for destination in forwards {
        match destination {
            ForwardingDestination::LocalRtc(destination) => {
                if let Some(payload_len) = destination.send(state, packet) {
                    rtp_metrics.record_egress(payload_len);
                    rtp_metrics.record_forwarded(RtpForwardDestinationKind::LocalRtc, payload_len);
                }
            }
            ForwardingDestination::PacketSink(destination) => {
                destination.send(state, packet);
                rtp_metrics.record_forwarded(destination.metrics_kind(), payload_len);
            }
            ForwardingDestination::Relay(destination) => {
                let Some(report) = destination.send(state, packet) else {
                    continue;
                };
                rtc_recorder.record_rtc_relay_enqueue(relay_enqueue_result(report));
                rtc_recorder.record_rtc_relay_mailbox_depth(report.mailbox_depth);
                match report.outcome {
                    RelayEnqueueOutcome::Enqueued => {
                        rtp_metrics.record_forwarded(
                            RtpForwardDestinationKind::IntraNodeRelay,
                            payload_len,
                        );
                    }
                    RelayEnqueueOutcome::Overloaded => {
                        metrics.record_rtp_relay_overload_drop(RtpRelayDropKind::IntraNodeRelay);
                    }
                    RelayEnqueueOutcome::Closed => {}
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "forward_flush/TESTS/mod.rs"]
mod tests;
