//! Packet observation and forwarding flush for the packet loop.
//!
//! Local session output and relay intake share one batch. Each packet then moves
//! through observation, planning and destination flush before the next packet
//! can change route state. This prevents a later decoder refresh from admitting
//! an earlier delta packet in the same batch.
//!
//! ```text
//! staged RTP packets (local str0m output + relay mailbox)
//!                     |
//!                     v
//!   +---------------------------------------------------+
//!   | 1. resolve_facts(packet)                          |
//!   |    - resolve source media, RID and audio facts    |
//!   |    - inspect codec / decoder-refresh marker       |
//!   +---------------------------------------------------+
//!                     |
//!                     v
//!   +---------------------------------------------------+
//!   | 2. packet observations & gate activation          |
//!   |    - observe_audio_activity (voice level & rank)  |
//!   |    - observe_decoder_refresh (clear retry tail)   |
//!   |    - apply_src_decoder_ready when applicable      |
//!   +---------------------------------------------------+
//!                     |
//!                     v
//!   +---------------------------------------------------+
//!   | 3. plan_forwards(packet, route_table)             |
//!   |    - append origin sink, then gate routed fanout  |
//!   +---------------------------------------------------+
//!                     |
//!                     v
//!   +---------------------------------------------------+
//!   | 4. flush_packet_forwards                          |
//!   |    +--> packet sink (recording / monitoring)      |
//!   |    +--> relay mailbox (cross-worker mpsc channel) |
//!   |    +--> local RTC (str0m session send queue)      |
//!   +---------------------------------------------------+
//! ```
//!
//! - use MID to learn the packet SSRC and optional RID binding
//! - cache codec-neutral decoder and rewrite facts once per packet
//! - update active-speaker and incoming bitrate observations
//! - stage broad ingress recovery and request RID recovery during observation
//! - drain the staged relay wake packet and a bounded relay batch
//! - send each planned packet to packet sink, relay or local RTC destinations
//!
//! Room policy decisions must already be projected into route-control state.
//! This module observes packets and executes planned sends. It does not decide
//! subscriptions or room membership.

use core::hint::cold_path;
#[cfg(any(test, feature = "internal-benchmarks"))]
use std::mem::take;
use std::time::Instant;

use str0m::media::{KeyframeRequestKind, MediaKind};
use tokio::sync::mpsc;
use tracing::debug;

use super::{
    super::{
        forwarded_packet::{ForwardedPacket, ForwardedPacketSource},
        forwarding_destination::{ForwardingDestination, relay_enqueue_result},
        media_registry::RegisteredMediaHandle,
        relay_registry::RelayEnqueueOutcome,
        state::PacketLoopState,
        worker::{
            KeyframeRequestMode, KeyframeRequestTarget, apply_src_decoder_ready,
            request_kf_for_target,
        },
    },
    buffers::PacketLoopBuffers,
};
use crate::engine::{
    media_transport::{SourcePolicySignal, TransportMediaId, TransportSessionKey},
    metrics::{
        RtcKeyframeRequestOutcome, RtcMetricsRecorder, RtpDecoderRefreshScope,
        RtpForwardDestinationKind, RtpMetricsRecorder, RtpRelayDropKind, RuntimeMetrics,
    },
};

#[cfg(any(test, feature = "internal-benchmarks"))]
pub(super) fn record_incoming_stats(
    state: &mut PacketLoopState,
    source_policy_signal: &SourcePolicySignal,
    control: &RtcMetricsRecorder,
    rtp: &RtpMetricsRecorder,
    buffers: &mut PacketLoopBuffers,
) {
    let mut pending_packets = take(&mut buffers.pending_packets);
    for packet in &mut pending_packets {
        record_incoming_packet(state, control, rtp, buffers, packet);
    }
    buffers.pending_packets = pending_packets;
    // Finish after every applicable RID-readiness observation in the batch. Gate
    // transitions can then suppress the broad ingress PLI and coalesce policy wakeups.
    finish_incoming_stats(state, source_policy_signal, control, buffers);
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
/// Packets without a resolvable source are ignored. `buffers` stages policy
/// wakeups and broad recovery while RID recovery may be requested immediately.
pub(super) fn record_incoming_packet(
    state: &mut PacketLoopState,
    control: &RtcMetricsRecorder,
    rtp: &RtpMetricsRecorder,
    buffers: &mut PacketLoopBuffers,
    packet: &mut ForwardedPacket,
) {
    let Some(facts) = packet.resolve_facts(state) else {
        return;
    };
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
        buffers
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
        || packet_rid.is_some_and(|rid| buffers.observe_rid_once(transport_media_id, rid));
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
            buffers
                .rid_readiness_changed_sources
                .push(transport_media_id);
        }
    }
    let bitrate_observation = state
        .record_incoming_bitrate(transport_media_id, packet.received_at(), payload_len)
        .unwrap_or_default();
    if bitrate_observation.policy_dirty() {
        buffers
            .dirty_source_policy_channel_ids
            .push(facts.room_instance_id);
    }
    if bitrate_observation.ingress_started() {
        cold_path();
        let Some(src_key) = packet.src_key(state) else {
            return;
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
        buffers.push_first_video_keyframe(
            packet.source(),
            transport_media_id,
            packet.received_at(),
        );
    }
    rtp.record_ingress(payload_len);
}

/// Flushes deferred keyframe recovery and source-policy wakeups for one
/// observed packet batch.
pub(super) fn finish_incoming_stats(
    state: &mut PacketLoopState,
    source_policy_signal: &SourcePolicySignal,
    control: &RtcMetricsRecorder,
    buffers: &mut PacketLoopBuffers,
) {
    flush_first_video_kfs(state, control, buffers);
    buffers.flush_source_policy_dirty(source_policy_signal);
}

#[cfg(feature = "internal-benchmarks")]
pub fn record_incoming_stats_for_benchmark(
    state: &mut PacketLoopState,
    source_policy_signal: &SourcePolicySignal,
    control: &RtcMetricsRecorder,
    rtp: &RtpMetricsRecorder,
    buffers: &mut PacketLoopBuffers,
) {
    record_incoming_stats(state, source_policy_signal, control, rtp, buffers);
}

fn flush_first_video_kfs(
    state: &mut PacketLoopState,
    metrics: &RtcMetricsRecorder,
    buffers: &mut PacketLoopBuffers,
) {
    for pending in buffers.pending_first_video_keyframes.drain(..) {
        // A gate transition consumed a refresh or scheduled RID-specific
        // recovery. The source-wide ingress PLI would duplicate that work.
        if buffers
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
    buffers.rid_readiness_changed_sources.clear();
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
pub(in crate::engine::media_transport::rtc) fn flush_packet_forwards(
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
