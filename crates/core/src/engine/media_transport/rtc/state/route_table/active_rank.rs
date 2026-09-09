use std::{cmp::Reverse, collections::BTreeMap, time::Instant};

use super::source::RouteSource;
use crate::engine::media_transport::TransportMediaId;

#[derive(Debug, Default)]
pub(super) struct ActiveSpeakerRank {
    // Ranked newest first. Equal hold windows make expired entries a suffix so
    // deadline and expiry work can inspect the rank tail.
    entries: Vec<ActiveSpeakerRankEntry>,
    by_src: BTreeMap<TransportMediaId, usize>,
}

impl ActiveSpeakerRank {
    pub(super) fn update_src(
        &mut self,
        source_id: TransportMediaId,
        source: Option<&RouteSource>,
        now: Instant,
    ) -> bool {
        let new_entry =
            source.and_then(|source| ActiveSpeakerRankEntry::from_source(source_id, source, now));
        let old_idx = self.idx_for(source_id);
        let active_len = self.active_len(now);
        let old_rank_idx = old_idx.filter(|idx| *idx < active_len);

        if let (Some(idx), Some(entry)) = (old_rank_idx, new_entry)
            && self.can_replace(idx, entry, active_len)
            && let Some(slot) = self.entries.get_mut(idx)
        {
            *slot = entry;
            return false;
        }

        if let Some(idx) = old_idx {
            self.remove_idx(idx);
        }
        let new_rank_idx = new_entry.map(|entry| self.insert_entry(entry));
        old_rank_idx != new_rank_idx
    }

    pub(super) fn drop_src(&mut self, source_id: TransportMediaId) {
        if let Some(idx) = self.idx_for(source_id) {
            self.remove_idx(idx);
        }
    }

    pub(super) fn next_deadline(&self, now: Instant) -> Option<Instant> {
        self.entries
            .iter()
            .rev()
            .find_map(|entry| (entry.expires_at > now).then_some(entry.expires_at))
    }

    pub(super) fn take_expired(&mut self, now: Instant) -> Vec<TransportMediaId> {
        let active_len = self.active_len(now);
        let by_src = &mut self.by_src;
        self.entries
            .drain(active_len..)
            .map(|entry| {
                by_src.remove(&entry.source_id);
                entry.source_id
            })
            .collect()
    }

    fn active_len(&self, now: Instant) -> usize {
        self.entries
            .iter()
            .rposition(|entry| entry.expires_at > now)
            .map_or(0, |idx| idx + 1)
    }

    fn can_replace(&self, idx: usize, entry: ActiveSpeakerRankEntry, active_len: usize) -> bool {
        let key = entry.rank_key();
        let after_prev = idx
            .checked_sub(1)
            .and_then(|prev| self.entries.get(prev))
            .map_or(idx == 0, |prev| prev.rank_key() <= key);
        let next_idx = idx.saturating_add(1);
        let before_next = next_idx >= active_len
            || self
                .entries
                .get(next_idx)
                .is_some_and(|next| key <= next.rank_key());
        after_prev && before_next
    }

    fn insert_entry(&mut self, entry: ActiveSpeakerRankEntry) -> usize {
        let idx = self.insert_idx(entry);
        self.entries.insert(idx, entry);
        self.reindex_from(idx);
        idx
    }

    fn remove_idx(&mut self, idx: usize) {
        let entry = self.entries.remove(idx);
        self.by_src.remove(&entry.source_id);
        self.reindex_from(idx);
    }

    fn insert_idx(&self, entry: ActiveSpeakerRankEntry) -> usize {
        self.entries
            .binary_search_by_key(&entry.rank_key(), ActiveSpeakerRankEntry::rank_key)
            .unwrap_or_else(|idx| idx)
    }

    fn reindex_from(&mut self, idx: usize) {
        for (idx, entry) in self.entries.iter().enumerate().skip(idx) {
            self.by_src.insert(entry.source_id, idx);
        }
    }

    fn idx_for(&self, source_id: TransportMediaId) -> Option<usize> {
        self.by_src.get(&source_id).copied()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ActiveSpeakerRankEntry {
    source_id: TransportMediaId,
    observed_at: Instant,
    audio_level_dbov: Option<i8>,
    expires_at: Instant,
}

impl ActiveSpeakerRankEntry {
    fn from_source(
        source_id: TransportMediaId,
        source: &RouteSource,
        now: Instant,
    ) -> Option<Self> {
        let active = source.active_speaker_source(source_id, now)?;
        Some(Self {
            source_id,
            observed_at: active.observed_at(),
            audio_level_dbov: active.last_audio_level_dbov(),
            expires_at: source.next_active_speaker_deadline(now)?,
        })
    }

    fn rank_key(&self) -> (Reverse<Instant>, Reverse<i8>, u64) {
        (
            Reverse(self.observed_at),
            Reverse(self.audio_level_dbov.unwrap_or(i8::MIN)),
            self.source_id.as_u64(),
        )
    }
}
