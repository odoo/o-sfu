//! Transport event observation for packet-loop sessions.
//!
//! `str0m` emits many events while the packet loop drains a session. Only a
//! small subset changes the SFU's observable transport state. This module contains
//! that translation so session draining can stay focused on moving `str0m`
//! outputs into packet-loop buffers.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use o_sfu_telemetry::schema;
use str0m::{
    Event, IceConnectionState,
    bwe::BweKind,
    stats::{MediaEgressStats, MediaIngressStats, PeerStats},
};
use tracing::{debug, info, trace};

use super::super::state::{RtcNackTotals, RtcSnapshotState, TransportSessionHealth};
use crate::{
    Bitrate,
    engine::{
        media_transport::{SourcePolicySignal, TransportSessionKey},
        metrics::{
            self, MediaQualityLossDirection, MediaQualitySample, RtcMetricsRecorder,
            RtcNackDirection, RuntimeMetrics, TransportIceState,
        },
    },
};

/// Log a transport event at the level useful for packet-loop diagnostics.
///
/// Connection transitions are debug-level because they describe lifecycle.
/// Other events are trace-level to avoid turning the media path into a logging
/// hot spot during normal calls.
pub(in super::super) fn log_rtc_event(session_key: &TransportSessionKey, event: &Event) {
    match event {
        Event::IceConnectionStateChange(state) => {
            debug!(
                user_id = ?session_key.user_id(),
                media_worker_id = session_key.media_worker_id().as_usize(),
                ?state,
                "rtc ICE connection state transition"
            );
        }
        Event::Connected => {
            debug!(
                user_id = ?session_key.user_id(),
                media_worker_id = session_key.media_worker_id().as_usize(),
                "rtc DTLS transport reached connected state"
            );
        }
        _ => {
            trace!(
                user_id = ?session_key.user_id(),
                media_worker_id = session_key.media_worker_id().as_usize(),
                ?event,
                "rtc packet loop event"
            );
        }
    }
}

pub(in super::super) struct RtcEventContext<'a> {
    pub(in super::super) snapshot_state: &'a Arc<Mutex<RtcSnapshotState>>,
    pub(in super::super) metrics: &'a RuntimeMetrics,
    pub(in super::super) rtc_metrics: &'a RtcMetricsRecorder,
    pub(in super::super) nack_totals: &'a mut RtcNackTotals,
    pub(in super::super) source_policy_signal: &'a SourcePolicySignal,
    pub(in super::super) room_id: &'a str,
    pub(in super::super) session_key: &'a TransportSessionKey,
    pub(in super::super) event: &'a Event,
}

/// Projects one `str0m` event into metrics, snapshots and diagnostics.
pub(in super::super) fn observe_rtc_event(context: RtcEventContext<'_>) {
    let RtcEventContext {
        snapshot_state,
        metrics,
        rtc_metrics,
        nack_totals,
        source_policy_signal,
        room_id,
        session_key,
        event,
    } = context;
    match event {
        Event::IceConnectionStateChange(state) => {
            metrics.record_transport_ice_state_change(transport_ice_state(*state));
        }
        Event::Connected => {
            metrics.record_transport_dtls_connected();
        }
        Event::EgressBitrateEstimate(kind) => {
            observe_receiver_bandwidth(snapshot_state, source_policy_signal, session_key, kind);
        }
        Event::PeerStats(stats) => {
            observe_peer_quality(snapshot_state, metrics, session_key, stats);
        }
        Event::MediaIngressStats(stats) => {
            observe_media_ingress_quality(
                snapshot_state,
                metrics,
                rtc_metrics,
                nack_totals,
                session_key,
                stats,
            );
        }
        Event::MediaEgressStats(stats) => {
            observe_media_egress_quality(
                snapshot_state,
                metrics,
                rtc_metrics,
                nack_totals,
                session_key,
                stats,
            );
        }
        _ => {}
    }
    let Some(health) = transport_health_from_event(event) else {
        return;
    };
    let previous = {
        let Ok(mut snapshot_state) = snapshot_state.lock() else {
            return;
        };
        snapshot_state.set_transport_health(session_key, health)
    };
    metrics.record_transport_health_transition(
        previous.map(metrics::transport_health_state),
        Some(metrics::transport_health_state(health)),
    );
    if previous == Some(health) {
        return;
    }
    info!(
        event = schema::event::TRANSPORT_HEALTH_CHANGED,
        room_id,
        user_id = %session_key.user_id().path_segment(),
        media_worker_id = session_key.media_worker_id().as_usize(),
        from = previous.map(transport_health_name),
        to = transport_health_name(health),
        "transport health changed"
    );
}

const fn transport_health_name(health: TransportSessionHealth) -> &'static str {
    match health {
        TransportSessionHealth::Connected => "connected",
        TransportSessionHealth::Disconnected => "disconnected",
    }
}

fn observe_peer_quality(
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    metrics: &RuntimeMetrics,
    session_key: &TransportSessionKey,
    stats: &PeerStats,
) {
    metrics.record_media_quality_sample(MediaQualitySample::Peer);
    if let Some(rtt) = stats.rtt {
        metrics.record_media_quality_rtt(MediaQualitySample::Peer, rtt);
    }
    if let Some(bwe_bps) = stats.bwe_tx.map(|bwe| bwe.as_u64()) {
        metrics.record_media_quality_bwe_bps(bwe_bps);
    }
    if let Some(loss_ppm) = loss_fraction_ppm(stats.ingress_loss_fraction) {
        metrics.record_media_quality_loss_ppm(MediaQualityLossDirection::Ingress, loss_ppm);
    }
    if let Some(loss_ppm) = loss_fraction_ppm(stats.egress_loss_fraction) {
        metrics.record_media_quality_loss_ppm(MediaQualityLossDirection::Egress, loss_ppm);
    }
    let Ok(mut snapshot_state) = snapshot_state.lock() else {
        return;
    };
    snapshot_state.update_transport_quality(session_key, |sample| {
        if let Some(bwe_bps) = stats.bwe_tx.map(|bwe| bwe.as_u64()) {
            sample.latest_bwe_bps = Some(bwe_bps);
        }
        if let Some(rtt_ms) = stats.rtt.map(duration_millis) {
            sample.rtt_ms = Some(rtt_ms);
        }
        if let Some(loss_ppm) = loss_fraction_ppm(stats.ingress_loss_fraction) {
            sample.ingress_loss_ppm = Some(loss_ppm);
        }
        if let Some(loss_ppm) = loss_fraction_ppm(stats.egress_loss_fraction) {
            sample.egress_loss_ppm = Some(loss_ppm);
        }
    });
}

fn observe_media_ingress_quality(
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    metrics: &RuntimeMetrics,
    rtc_metrics: &RtcMetricsRecorder,
    nack_totals: &mut RtcNackTotals,
    session_key: &TransportSessionKey,
    stats: &MediaIngressStats,
) {
    metrics.record_media_quality_sample(MediaQualitySample::MediaIngress);
    if let Some(rtt) = stats.rtt {
        metrics.record_media_quality_rtt(MediaQualitySample::MediaIngress, rtt);
    }
    if let Some(loss_ppm) = loss_fraction_ppm(stats.loss) {
        metrics.record_media_quality_loss_ppm(MediaQualityLossDirection::Ingress, loss_ppm);
    }
    let nack_delta = nack_totals.sent_to_publisher(stats.mid, stats.rid, stats.nacks);
    rtc_metrics.record_rtc_nacks(RtcNackDirection::SentToPublisher, nack_delta);
    let Ok(mut snapshot_state) = snapshot_state.lock() else {
        return;
    };
    snapshot_state.update_transport_quality(session_key, |sample| {
        if let Some(rtt_ms) = stats.rtt.map(duration_millis) {
            sample.rtt_ms = Some(rtt_ms);
        }
        if let Some(loss_ppm) = loss_fraction_ppm(stats.loss) {
            sample.ingress_loss_ppm = Some(loss_ppm);
        }
    });
}

fn observe_media_egress_quality(
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    metrics: &RuntimeMetrics,
    rtc_metrics: &RtcMetricsRecorder,
    nack_totals: &mut RtcNackTotals,
    session_key: &TransportSessionKey,
    stats: &MediaEgressStats,
) {
    metrics.record_media_quality_sample(MediaQualitySample::MediaEgress);
    if let Some(rtt) = stats.rtt {
        metrics.record_media_quality_rtt(MediaQualitySample::MediaEgress, rtt);
    }
    if let Some(loss_ppm) = loss_fraction_ppm(stats.loss) {
        metrics.record_media_quality_loss_ppm(MediaQualityLossDirection::Egress, loss_ppm);
    }
    if let Some(remote) = stats.remote.as_ref() {
        metrics.record_media_quality_jitter_rtp_timestamp_units(u64::from(remote.jitter));
    }
    let nack_delta = nack_totals.received_from_subscriber(stats.mid, stats.rid, stats.nacks);
    rtc_metrics.record_rtc_nacks(RtcNackDirection::ReceivedFromSubscriber, nack_delta);
    let Ok(mut snapshot_state) = snapshot_state.lock() else {
        return;
    };
    snapshot_state.update_transport_quality(session_key, |sample| {
        if let Some(rtt_ms) = stats.rtt.map(duration_millis) {
            sample.rtt_ms = Some(rtt_ms);
        }
        if let Some(loss_ppm) = loss_fraction_ppm(stats.loss) {
            sample.egress_loss_ppm = Some(loss_ppm);
        }
        if let Some(remote) = stats.remote.as_ref() {
            sample.egress_jitter_rtp_timestamp_units = Some(u64::from(remote.jitter));
        }
    });
}

fn loss_fraction_ppm(loss: Option<f32>) -> Option<u64> {
    loss.filter(|loss| loss.is_finite())
        .map(|loss| loss.clamp(0.0, 1.0))
        .map(scaled_loss_ppm)
}

#[expect(
    clippy::as_conversions,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "loss is finite and clamped to 0..=1 before scaling to integer ppm"
)]
fn scaled_loss_ppm(loss: f32) -> u64 {
    (loss * 1_000_000.0).round() as u64
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// Store receiver bandwidth estimates and wake source-policy recomputation.
///
/// Bandwidth estimates can change source selection, but that policy is owned by
/// the room layer. The packet loop records the latest estimate in the snapshot
/// and marks the room dirty only when the value changed.
fn observe_receiver_bandwidth(
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    source_policy_signal: &SourcePolicySignal,
    session_key: &TransportSessionKey,
    kind: &BweKind,
) {
    let (BweKind::Twcc(bitrate) | BweKind::Remb(_, bitrate)) = kind else {
        return;
    };
    let estimate = Bitrate::from_bps(bitrate.as_u64());
    let changed = {
        let Ok(mut snapshot_state) = snapshot_state.lock() else {
            return;
        };
        snapshot_state.set_receiver_bandwidth(session_key, estimate) != Some(estimate)
    };
    if changed {
        source_policy_signal.mark_dirty(session_key.room_instance_id());
    }
}

#[cfg(feature = "internal-benchmarks")]
pub fn observe_rtc_event_for_benchmark(
    snapshot_state: &Arc<Mutex<RtcSnapshotState>>,
    metrics: &RuntimeMetrics,
    rtc_metrics: &RtcMetricsRecorder,
    source_policy_signal: &SourcePolicySignal,
    room_id: &str,
    session_key: &TransportSessionKey,
    event: &Event,
) {
    let mut nack_totals = RtcNackTotals::default();
    observe_rtc_event(RtcEventContext {
        snapshot_state,
        metrics,
        rtc_metrics,
        nack_totals: &mut nack_totals,
        source_policy_signal,
        room_id,
        session_key,
        event,
    });
}

/// Convert `str0m` ICE connection state into the metrics enum.
pub fn transport_ice_state(state: IceConnectionState) -> TransportIceState {
    match state {
        IceConnectionState::New => TransportIceState::New,
        IceConnectionState::Checking => TransportIceState::Checking,
        IceConnectionState::Connected => TransportIceState::Connected,
        IceConnectionState::Completed => TransportIceState::Completed,
        IceConnectionState::Disconnected => TransportIceState::Disconnected,
    }
}

/// Convert a `str0m` event into the public transport-health snapshot value.
///
/// Not every event is a health transition. `None` means the event should not
/// update the session health snapshot.
pub fn transport_health_from_event(event: &Event) -> Option<TransportSessionHealth> {
    match event {
        Event::Connected => Some(TransportSessionHealth::Connected),
        Event::IceConnectionStateChange(state) => transport_health_from_ice_state(*state),
        _ => None,
    }
}

fn transport_health_from_ice_state(state: IceConnectionState) -> Option<TransportSessionHealth> {
    if state.is_connected() {
        Some(TransportSessionHealth::Connected)
    } else if state.is_disconnected() {
        Some(TransportSessionHealth::Disconnected)
    } else {
        None
    }
}
