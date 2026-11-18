//! RTC worker ownership below the media transport boundary.
//!
//! `MediaTransport` owns the process-local RTC worker topology and maps each
//! transport session to the worker selected by its media-worker id. Consumer
//! workers set up cross-worker relays themselves. Packet-loop hot paths live
//! inside `engine::media_transport::rtc`.

#[cfg(any(test, feature = "testing-transport"))]
use std::sync::atomic::Ordering;
use std::{cmp::Reverse, collections::BTreeMap};

use futures_util::{StreamExt, TryStreamExt, future::try_join_all, stream};
use str0m::media::MediaKind as Str0mMediaKind;

use super::rtc::RtcWorker;
use crate::{
    MediaWorkerId,
    engine::media_transport::{
        ActiveSpeakerSource, MediaTransport, ReceiverBandwidthSnapshot, TransportAdapterError,
        TransportBitrateSnapshot, TransportHealthSnapshot, TransportMediaId,
        TransportQualitySnapshot, TransportSessionHealth, TransportSessionKey,
        TransportSourceDiagnosticsSnapshot, TransportSourceKey, TransportWorkerPressureSnapshot,
    },
};

const WORKER_OBSERVATION_CONCURRENCY: usize = 8;

/// Worker availability is independent of missing or delayed loop samples.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkerPlacementState {
    Running(Option<u64>),
    Unavailable,
}

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
    /// # Errors
    ///
    /// Returns [`TransportAdapterError::TransportUnavailable`] if any source
    /// selects an unavailable worker or its observation does not complete.
    pub async fn source_diagnostics_snapshot(
        &self,
        sources: &[TransportSourceKey],
    ) -> Result<TransportSourceDiagnosticsSnapshot, TransportAdapterError> {
        let mut media_ids_by_worker = BTreeMap::<usize, Vec<TransportMediaId>>::new();
        for source in sources {
            let worker_index = self
                .worker_index_for_user(source.session_key())
                .ok_or(TransportAdapterError::TransportUnavailable)?;
            media_ids_by_worker
                .entry(worker_index)
                .or_default()
                .push(source.transport_media_id());
        }
        let worker_snapshots = stream::iter(media_ids_by_worker)
            .map(|(worker_index, transport_media_ids)| async move {
                let worker = self
                    .worker_for_index(worker_index)
                    .ok_or(TransportAdapterError::TransportUnavailable)?;
                #[cfg(any(test, feature = "testing-transport"))]
                self.source_diagnostics_requests
                    .fetch_add(1, Ordering::Relaxed);
                let snapshot = worker
                    .source_diagnostics_snapshot(&transport_media_ids)
                    .await?;
                Ok::<_, TransportAdapterError>((worker_index, snapshot))
            })
            .buffer_unordered(WORKER_OBSERVATION_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        let mut worker_snapshots = worker_snapshots;
        worker_snapshots.sort_unstable_by_key(|(worker_index, _)| *worker_index);
        let mut snapshot = TransportSourceDiagnosticsSnapshot::default();
        for (_, worker_snapshot) in worker_snapshots {
            snapshot.activity.extend(worker_snapshot.activity);
            snapshot
                .active_speaker_diagnostics
                .extend(worker_snapshot.active_speaker_diagnostics);
        }
        Ok(snapshot)
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

    #[cfg(any(test, feature = "testing-transport"))]
    pub(crate) fn packet_loop_delays_ms(&self) -> Vec<Option<u64>> {
        self.workers
            .iter()
            .map(RtcWorker::packet_loop_delay_ms)
            .collect()
    }

    /// Reports whether the worker still owns usable transport sessions.
    pub(crate) fn worker_is_usable(&self, worker_index: usize) -> bool {
        self.worker_for_index(worker_index)
            .is_some_and(RtcWorker::is_usable)
    }

    pub(crate) fn worker_placement_states(&self) -> Vec<WorkerPlacementState> {
        self.workers
            .iter()
            .map(|worker| {
                if worker.is_usable() {
                    WorkerPlacementState::Running(worker.packet_loop_delay_ms())
                } else {
                    WorkerPlacementState::Unavailable
                }
            })
            .collect()
    }

    /// Returns one worker's active-speaker inventory.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError::TransportUnavailable`] when the worker
    /// is missing or cannot answer before the observation deadline.
    pub(crate) async fn active_speaker_source_snapshot_for_worker(
        &self,
        worker_index: usize,
    ) -> Result<Vec<ActiveSpeakerSource>, TransportAdapterError> {
        let worker = self
            .worker_for_index(worker_index)
            .ok_or(TransportAdapterError::TransportUnavailable)?;
        worker.active_speaker_source_snapshot().await
    }

    /// Returns one observation from each requested worker before merging relayed sources.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError::TransportUnavailable`] if a requested
    /// worker is unavailable. Callers must retry rather than use partial facts.
    pub(crate) async fn active_speaker_source_snapshot_for_workers(
        &self,
        worker_indices: &[usize],
    ) -> Result<Vec<ActiveSpeakerSource>, TransportAdapterError> {
        let snapshots = try_join_all(
            worker_indices
                .iter()
                .map(|&worker_index| self.active_speaker_source_snapshot_for_worker(worker_index)),
        )
        .await?;
        let mut snapshot = snapshots.into_iter().flatten().collect::<Vec<_>>();
        // A relayed source can be observed on its owner and consumer workers.
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
        Ok(snapshot)
    }

    /// Returns active-speaker observations from all workers.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError::TransportUnavailable`] if any worker
    /// observation fails. No partial observation is returned to policy.
    pub async fn active_speaker_source_snapshot(
        &self,
    ) -> Result<Vec<ActiveSpeakerSource>, TransportAdapterError> {
        let workers = (0..self.workers.len()).collect::<Vec<_>>();
        self.active_speaker_source_snapshot_for_workers(&workers)
            .await
    }

    /// Returns the MID stored by the current transport media handle.
    ///
    /// `None` means `transport_media_id` has no registered handle.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError::TransportUnavailable`] if the selected
    /// worker is unavailable or cannot answer before the observation deadline.
    #[cfg(test)]
    pub(crate) async fn transport_media_mid(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) -> Result<Option<String>, TransportAdapterError> {
        self.require_worker_for_user(session_key)?
            .request_worker(|response| super::rtc::RtcWorkerCommand::ResolveMediaMid {
                transport_media_id,
                response,
            })
            .await
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
            .filter(|worker| worker.is_usable())
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
