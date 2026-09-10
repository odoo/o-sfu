//! RTC worker ownership below the media transport boundary.
//!
//! `MediaTransport` owns the process-local RTC worker topology, maps each
//! transport session to the worker selected by its media-worker id and
//! coordinates cross-worker relay cleanup. Packet-loop hot paths live inside
//! `engine::media_transport::rtc`.

#[cfg(any(test, feature = "testing-transport"))]
use std::sync::atomic::Ordering;
use std::{cmp::Reverse, collections::BTreeMap, ptr};

use str0m::media::MediaKind as Str0mMediaKind;

use super::rtc::{RtcWorker, RtcWorkerCommand};
use crate::engine::{
    MediaWorkerId,
    media_transport::{
        ActiveSpeakerSource, MediaTransport, ReceiverBandwidthSnapshot, TransportAdapterError,
        TransportBitrateSnapshot, TransportHealthSnapshot, TransportMediaId,
        TransportQualitySnapshot, TransportRelayRouteAction, TransportRelayRouteEffect,
        TransportSessionHealth, TransportSessionKey, TransportSourceActivityEffect,
        TransportSourceDiagnosticsSnapshot, TransportSourceKey, TransportTeardown,
        TransportWorkerPressureSnapshot,
    },
};

impl MediaTransport {
    /// Selects the worker that owns a transport session.
    ///
    /// The mapping is deterministic and depends only on the runtime-assigned
    /// media-worker id in the session key. Room and signaling code must not
    /// infer topology from user identity.
    pub(super) fn worker_for_user(&self, session_key: &TransportSessionKey) -> Option<&RtcWorker> {
        self.worker_for_index(session_key.media_worker_id().as_usize())
    }

    /// Returns the latest bitrate estimates for the requested sessions.
    ///
    /// Missing sessions are omitted from the snapshot. Estimates are suitable
    /// for diagnostics and policy input, not for accounting.
    #[must_use]
    pub fn transport_bitrate_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportBitrateSnapshot {
        let mut snapshot = TransportBitrateSnapshot::default();
        self.for_session_workers(session_keys, |worker, worker_session_keys| {
            let worker_snapshot = worker.transport_bitrate_snapshot(worker_session_keys);
            snapshot.total = snapshot.total.saturating_add(worker_snapshot.total);
            snapshot.per_media.extend(worker_snapshot.per_media);
        });
        snapshot
    }

    /// Returns receiver-side bandwidth estimates for the requested sessions.
    ///
    /// Room policy may use these estimates as source-selection input. They are
    /// best-effort observations from the transport backend.
    #[must_use]
    pub fn receiver_bandwidth_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> ReceiverBandwidthSnapshot {
        let mut snapshot = ReceiverBandwidthSnapshot::default();
        self.for_session_workers(session_keys, |worker, worker_session_keys| {
            let worker_snapshot = worker.receiver_bandwidth_snapshot(worker_session_keys);
            snapshot.per_session.extend(worker_snapshot.per_session);
        });
        snapshot
    }

    /// Returns sampled transport-quality observations for the requested sessions.
    #[must_use]
    pub fn transport_quality_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportQualitySnapshot {
        let mut snapshot = TransportQualitySnapshot::default();
        self.for_session_workers(session_keys, |worker, worker_session_keys| {
            snapshot.extend(worker.transport_quality_snapshot(worker_session_keys));
        });
        snapshot
    }

    /// Returns transport health for the requested sessions with one lock per worker.
    ///
    /// Missing sessions and unavailable worker snapshots contribute no facts
    #[must_use]
    pub fn transport_health_snapshot(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> TransportHealthSnapshot {
        let mut snapshot = TransportHealthSnapshot::default();
        self.for_session_workers(session_keys, |worker, worker_session_keys| {
            snapshot.extend(worker.transport_health_snapshot(worker_session_keys));
        });
        snapshot
    }

    /// Returns source activity and active-speaker facts with one command per worker.
    ///
    /// Missing sources and worker dispatch failures contribute no facts
    pub async fn source_diagnostics_snapshot(
        &self,
        sources: &[TransportSourceKey],
    ) -> TransportSourceDiagnosticsSnapshot {
        let mut snapshot = TransportSourceDiagnosticsSnapshot::default();
        let mut media_ids_by_worker = BTreeMap::<usize, Vec<TransportMediaId>>::new();
        for source in sources {
            let Some(worker_index) = self.worker_index_for_user(source.session_key()) else {
                continue;
            };
            media_ids_by_worker
                .entry(worker_index)
                .or_default()
                .push(source.transport_media_id());
        }
        for (worker_index, transport_media_ids) in media_ids_by_worker {
            let Some(worker) = self.worker_for_index(worker_index) else {
                continue;
            };
            #[cfg(any(test, feature = "testing-transport"))]
            self.source_diagnostics_requests
                .fetch_add(1, Ordering::Relaxed);
            let worker_snapshot = worker
                .source_diagnostics_snapshot(&transport_media_ids)
                .await;
            snapshot.activity.extend(worker_snapshot.activity);
            snapshot
                .active_speaker_diagnostics
                .extend(worker_snapshot.active_speaker_diagnostics);
        }
        snapshot
    }

    /// Returns transport pressure for every media worker.
    #[must_use]
    pub fn worker_pressure_snapshots(&self) -> Vec<TransportWorkerPressureSnapshot> {
        self.workers
            .iter()
            .enumerate()
            .map(|(worker_index, worker)| {
                worker.worker_pressure_snapshot(MediaWorkerId::from_raw(worker_index))
            })
            .collect()
    }

    pub(crate) fn packet_loop_delays_ms(&self) -> Vec<Option<u64>> {
        self.workers
            .iter()
            .map(RtcWorker::packet_loop_delay_ms)
            .collect()
    }

    /// Returns each media source's newest active-speaker observation in recency
    /// order, preferring its strongest audio level on timestamp ties.
    pub async fn active_speaker_source_snapshot(&self) -> Vec<ActiveSpeakerSource> {
        let mut snapshot = Vec::new();
        for worker in self.workers.iter() {
            snapshot.extend(worker.active_speaker_source_snapshot().await);
        }
        // A relayed source is observed on its owner and consumer workers. Keep one
        // policy fact per media ID, preferring recency then the strongest level as
        // the deterministic same-timestamp tie-break.
        snapshot.sort_unstable_by_key(|source| {
            (
                source.transport_media_id().as_u64(),
                Reverse(source.observed_at()),
                Reverse(source.last_audio_level_dbov().unwrap_or(i8::MIN)),
            )
        });
        snapshot.dedup_by_key(|source| source.transport_media_id());
        snapshot.sort_unstable_by_key(|source| {
            (
                Reverse(source.observed_at()),
                source.transport_media_id().as_u64(),
            )
        });
        snapshot
    }

    /// Applies one cross-worker relay mutation on the source worker.
    ///
    /// The target worker contributes its relay identity and mailbox. The source
    /// packet loop owns registration and activity because it decides fanout before
    /// packets cross workers.
    pub(super) async fn execute_relay_route_effect(
        &self,
        effect: &TransportRelayRouteEffect,
    ) -> Result<(), TransportAdapterError> {
        if effect.action == TransportRelayRouteAction::Release {
            self.teardown([TransportTeardown::ReleaseRelayRoute {
                source: effect.source.clone(),
                target_media_worker_id: effect.target_media_worker_id,
            }])
            .await;
            return Ok(());
        }
        let source_worker = self.require_worker_for_user(effect.source.session_key())?;
        let target_worker =
            self.require_worker_for_media_worker_id(effect.target_media_worker_id)?;
        if ptr::eq(source_worker, target_worker) {
            return Ok(());
        }
        let request = target_worker.relay_route_request(effect.source.clone(), effect.action);
        source_worker
            .request_worker(|response| RtcWorkerCommand::RouteControl {
                request,
                response: Some(response),
            })
            .await
    }

    pub(super) async fn execute_remote_source_activity_effect(
        &self,
        effect: &TransportSourceActivityEffect,
    ) -> Result<(), TransportAdapterError> {
        let target_worker =
            self.require_worker_for_media_worker_id(effect.target_media_worker_id)?;
        let request =
            RtcWorker::remote_source_activity_request(effect.source.clone(), effect.update);
        target_worker
            .request_worker(|response| RtcWorkerCommand::RouteControl {
                request,
                response: Some(response),
            })
            .await
    }

    /// Returns the MID stored by the current transport media handle.
    ///
    /// `None` means `session_key` selects no worker, the worker request failed or
    /// `transport_media_id` has no registered handle.
    pub(crate) async fn transport_media_mid(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Option<String> {
        self.worker_for_user(session_key)?
            .request_worker(|response| RtcWorkerCommand::ResolveMediaMid {
                transport_media_id,
                response,
            })
            .await
            .ok()
            .flatten()
    }

    /// Returns the latest known transport health for one session.
    ///
    /// Health is connectivity evidence only. It should not be used as the
    /// source of truth for whether a participant belongs to a room.
    #[must_use]
    pub fn session_transport_health(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<TransportSessionHealth> {
        self.worker_for_user(session_key)?
            .session_transport_health(session_key)
    }

    fn worker_index_for_user(&self, session_key: &TransportSessionKey) -> Option<usize> {
        let worker_index = session_key.media_worker_id().as_usize();
        (worker_index < self.workers.len()).then_some(worker_index)
    }

    pub(super) fn require_worker_for_user(
        &self,
        session_key: &TransportSessionKey,
    ) -> Result<&RtcWorker, TransportAdapterError> {
        self.worker_for_user(session_key)
            .ok_or(TransportAdapterError::TransportUnavailable)
    }

    pub(super) fn require_worker_for_media_worker_id(
        &self,
        media_worker_id: MediaWorkerId,
    ) -> Result<&RtcWorker, TransportAdapterError> {
        self.worker_for_index(media_worker_id.as_usize())
            .ok_or(TransportAdapterError::TransportUnavailable)
    }

    fn session_keys_by_worker(
        &self,
        session_keys: &[TransportSessionKey],
    ) -> BTreeMap<usize, Vec<TransportSessionKey>> {
        let mut keys_by_worker = BTreeMap::<usize, Vec<TransportSessionKey>>::new();
        for session_key in session_keys {
            if let Some(worker_index) = self.worker_index_for_user(session_key) {
                keys_by_worker
                    .entry(worker_index)
                    .or_default()
                    .push(session_key.clone());
            }
        }
        keys_by_worker
    }

    fn for_session_workers(
        &self,
        session_keys: &[TransportSessionKey],
        mut visit: impl FnMut(&RtcWorker, &[TransportSessionKey]),
    ) {
        for (worker_index, worker_session_keys) in self.session_keys_by_worker(session_keys) {
            if let Some(worker) = self.worker_for_index(worker_index) {
                visit(worker, &worker_session_keys);
            }
        }
    }

    /// Enforces room isolation before worker or media lookup.
    ///
    /// `MediaWorkerId` selects execution ownership only. It never authorizes a
    /// route between room instances.
    pub(super) fn ensure_same_room(
        consumer_session_key: &TransportSessionKey,
        source_session_key: &TransportSessionKey,
    ) -> Result<(), TransportAdapterError> {
        if consumer_session_key.room_instance_id() == source_session_key.room_instance_id() {
            return Ok(());
        }
        Err(TransportAdapterError::InvalidInput)
    }

    pub(super) fn worker_for_index(&self, worker_index: usize) -> Option<&RtcWorker> {
        self.workers.get(worker_index)
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(super) fn all_workers(&self) -> impl Iterator<Item = &RtcWorker> {
        self.workers.iter()
    }
}

pub(super) fn signaling_to_str0m_media_kind(kind: o_sfu_router::MediaKind) -> Str0mMediaKind {
    match kind {
        o_sfu_router::MediaKind::Audio => Str0mMediaKind::Audio,
        o_sfu_router::MediaKind::Video => Str0mMediaKind::Video,
    }
}
