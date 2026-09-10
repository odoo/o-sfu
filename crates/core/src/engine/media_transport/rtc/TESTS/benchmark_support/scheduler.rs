use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use super::super::{
    bootstrap::test_support::ensure_session_rtc_state,
    state::{PacketLoopState, slots::SessionHandle},
    test_support::test_transport_session_key,
};
use crate::{
    Bitrate,
    engine::{UserId, media_transport::TransportSessionKey},
};

const SCHEDULER_SESSION_COUNT: usize = 128;

/// fixed scheduler fixture for dirty-session and timeout-heap benchmarks
///
/// setup creates live worker sessions. the measured path marks every session
/// dirty, replaces its timeout once to leave a stale heap entry and drains ready
/// handles through the production scheduler
pub struct SchedulerBenchFixture {
    state: PacketLoopState,
    handles: Vec<SessionHandle>,
    session_keys: Vec<TransportSessionKey>,
    ready_sessions: Vec<SessionHandle>,
    now: Instant,
    turn: u32,
}

impl SchedulerBenchFixture {
    #[must_use]
    pub fn stale_timeouts() -> Self {
        let mut state = PacketLoopState::default();
        let candidate_addr = SocketAddr::from(([127, 0, 0, 1], 46_300));
        let mut handles = Vec::with_capacity(SCHEDULER_SESSION_COUNT);
        let mut session_keys = Vec::with_capacity(SCHEDULER_SESSION_COUNT);

        for session_idx in 0..SCHEDULER_SESSION_COUNT {
            let session_key = test_transport_session_key(
                111,
                0,
                10_000 + u64::try_from(session_idx).unwrap_or(0),
                UserId::Integer(20_000 + i64::try_from(session_idx).unwrap_or(0)),
            );
            let _ = ensure_session_rtc_state(
                &mut state.users,
                &session_key,
                candidate_addr,
                Bitrate::from_mbps(10),
            );
            if let Some(handle) = state.users.handle_for_key(&session_key) {
                handles.push(handle);
                session_keys.push(session_key);
            }
        }
        state.dirty_sessions.reserve_exact(SCHEDULER_SESSION_COUNT);

        Self {
            state,
            handles,
            session_keys,
            ready_sessions: Vec::with_capacity(SCHEDULER_SESSION_COUNT),
            now: Instant::now(),
            turn: 0,
        }
    }

    #[must_use]
    pub fn collect_ready_and_next_timeout(&mut self) -> usize {
        let now = self.now + Duration::from_millis(u64::from(self.turn) * 100);
        self.turn = self.turn.wrapping_add(1);
        for (session_key, handle) in self.session_keys.iter().zip(&self.handles) {
            self.state.mark_session_dirty(session_key);
            self.state
                .update_session_timeout_by_handle(*handle, Some(now));
            self.state
                .update_session_timeout_by_handle(*handle, Some(now + Duration::from_nanos(1)));
        }

        self.ready_sessions.clear();
        self.state
            .collect_ready_sessions(now + Duration::from_nanos(1), &mut self.ready_sessions);
        for handle in &self.handles {
            self.state
                .update_session_timeout_by_handle(*handle, Some(now + Duration::from_millis(10)));
            self.state
                .update_session_timeout_by_handle(*handle, Some(now + Duration::from_millis(20)));
        }
        let ready = self.ready_sessions.len();
        let next_deadline = usize::from(self.state.next_timeout_deadline().is_some());
        ready + next_deadline
    }
}
