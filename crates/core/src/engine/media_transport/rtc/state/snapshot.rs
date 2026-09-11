//! Read-side transport observations shared outside the worker.

use std::collections::BTreeMap;

use super::TransportSessionHealth;
use crate::{
    Bitrate,
    engine::media_transport::{
        ReceiverBandwidthSnapshot, TransportHealthSnapshot, TransportQualitySample,
        TransportQualitySnapshot, TransportSessionKey,
    },
};

/// read-side RTC transport snapshot shared outside the packet loop
///
/// this state mirrors facts that diagnostics, placement and transport policy
/// need without exposing mutable [`super::PacketLoopState`]
/// it is protected by a cold-path mutex while packet-path state remains
/// worker-local and single-threaded
#[derive(Debug, Default)]
pub struct RtcSnapshotState {
    /// latest observed transport health by session
    transport_health: BTreeMap<TransportSessionKey, TransportSessionHealth>,
    /// latest receiver bandwidth estimate by session
    receiver_bandwidth: BTreeMap<TransportSessionKey, Bitrate>,
    /// latest sampled media quality by session
    transport_quality: BTreeMap<TransportSessionKey, TransportQualitySample>,
}

impl RtcSnapshotState {
    /// remove every read-side fact owned by one session
    ///
    /// returns the previous transport health so teardown metrics can record the
    /// final transition without doing a second lookup
    pub(in super::super) fn remove_session(
        &mut self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        self.receiver_bandwidth.remove(session_key);
        self.transport_quality.remove(session_key);
        self.transport_health.remove(session_key)
    }

    /// replace the latest transport health observation for one session
    ///
    /// returns the previous value so callers can record health transitions
    pub(in super::super) fn set_transport_health(
        &mut self,
        session_key: &TransportSessionKey,
        health: TransportSessionHealth,
    ) -> Option<TransportSessionHealth> {
        self.transport_health.insert(session_key.clone(), health)
    }

    /// return the latest health observation for a session
    ///
    /// missing health means the packet loop has not observed a transport event
    /// for that session or the session was removed
    pub fn transport_health(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        self.transport_health.get(session_key).copied()
    }

    /// Build a transport-health snapshot for the requested sessions.
    pub fn transport_health_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportHealthSnapshot {
        session_keys
            .iter()
            .filter_map(|key| {
                self.transport_health
                    .get(key)
                    .copied()
                    .map(|health| (key.clone(), health))
            })
            .collect()
    }

    /// replace the latest receiver bandwidth estimate for one session
    pub(in super::super) fn set_receiver_bandwidth(
        &mut self,
        session_key: &TransportSessionKey,
        estimate: Bitrate,
    ) -> Option<Bitrate> {
        self.receiver_bandwidth
            .insert(session_key.clone(), estimate)
    }

    /// build a receiver bandwidth snapshot for the requested sessions
    ///
    /// sessions without an estimate are omitted so callers can distinguish
    /// missing observations from a zero bitrate estimate
    pub fn receiver_bandwidth_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> ReceiverBandwidthSnapshot {
        ReceiverBandwidthSnapshot {
            per_session: session_keys
                .iter()
                .filter_map(|session_key| {
                    self.receiver_bandwidth
                        .get(session_key)
                        .copied()
                        .map(|estimate| (session_key.clone(), estimate))
                })
                .collect(),
        }
    }

    /// update sampled transport-quality observations for one session
    pub(in super::super) fn update_transport_quality(
        &mut self,
        session_key: &TransportSessionKey,
        update: impl FnOnce(&mut TransportQualitySample),
    ) {
        let sample = self
            .transport_quality
            .entry(session_key.clone())
            .or_default();
        sample.sample_count = sample.sample_count.saturating_add(1);
        update(sample);
    }

    /// build a transport-quality snapshot for the requested sessions
    pub fn transport_quality_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportQualitySnapshot {
        session_keys
            .iter()
            .filter_map(|session_key| {
                self.transport_quality
                    .get(session_key)
                    .copied()
                    .map(|sample| (session_key.clone(), sample))
            })
            .collect()
    }
}
