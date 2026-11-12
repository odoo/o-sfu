use std::time::{Duration, Instant};

use super::super::state::route_table::RouteTable;
use crate::engine::media_transport::TransportMediaId;

const ACTIVE_SPEAKER_SOURCE_COUNT: usize = 128;
const ACTIVE_SPEAKER_OBSERVATIONS: usize = 4;

/// fixed active-speaker fixture for packet-level audio policy benchmarks
///
/// setup keeps only the route-control state needed by the packet loop. the
/// measured path observes speech for many sources, snapshots active speakers,
/// checks the next expiry deadline and queries expired source ids
pub struct ActiveSpeakerBenchFixture {
    state: RouteTable,
    now: Instant,
    sources: Vec<TransportMediaId>,
}

impl ActiveSpeakerBenchFixture {
    #[must_use]
    pub fn many_sources() -> Self {
        let sources = (0..ACTIVE_SPEAKER_SOURCE_COUNT)
            .map(|source_idx| TransportMediaId::new(u64::try_from(source_idx + 1).unwrap_or(1)))
            .collect();
        Self {
            state: RouteTable::default(),
            now: Instant::now(),
            sources,
        }
    }

    #[must_use]
    pub fn observe_sources(&mut self) -> usize {
        let mut changes = 0;
        let query_at = self.now
            + Duration::from_millis(u64::try_from(ACTIVE_SPEAKER_OBSERVATIONS - 1).unwrap_or(0));
        for observation_idx in 0..ACTIVE_SPEAKER_OBSERVATIONS {
            let observed_at =
                self.now + Duration::from_millis(u64::try_from(observation_idx).unwrap_or(0));
            for source in &self.sources {
                changes += usize::from(self.state.observe_audio_activity(
                    *source,
                    Some(true),
                    Some(-32),
                    observed_at,
                ));
            }
        }

        let active = self.state.active_speaker_sources(query_at).len();
        let has_deadline = usize::from(self.state.next_active_speaker_deadline(query_at).is_some());
        let expired = self
            .state
            .take_expired_speakers(query_at + Duration::from_millis(300))
            .len();
        self.now = query_at + Duration::from_millis(500);
        changes + active + has_deadline + expired
    }
}
