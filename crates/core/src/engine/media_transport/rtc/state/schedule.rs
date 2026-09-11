//! Dirty-session and deadline scheduling with generation-checked handles.

use std::{cmp::Reverse, time::Instant};

use super::{PacketLoopState, slots::SessionHandle};
use crate::engine::media_transport::TransportSessionKey;

impl PacketLoopState {
    /// schedule a live session for the next packet-loop poll
    ///
    /// missing sessions are ignored because teardown may race with already
    /// queued wakeups
    /// each live session can appear at most once until
    /// [`Self::collect_ready_sessions`] clears its dirty bit
    pub(in super::super) fn mark_session_dirty(&mut self, session_key: &TransportSessionKey) {
        let Some(session_handle) = self.users.handle_for_key(session_key) else {
            return;
        };
        let Some(session_state) = self.users.get_mut_by_handle(session_handle) else {
            return;
        };
        if session_state.packet_loop_dirty {
            return;
        }
        session_state.packet_loop_dirty = true;
        self.dirty_sessions.push(session_handle);
    }

    /// report whether the worker has session work that is due immediately
    pub(in super::super) fn has_dirty_sessions(&self) -> bool {
        !self.dirty_sessions.is_empty()
    }

    /// drain dirty sessions and due `str0m` timeouts into caller-owned scratch
    ///
    /// this method is the session scheduler for the packet loop
    /// it clears dirty bits for live sessions, skips removed sessions and lazily
    /// discards timeout heap entries whose deadline no longer matches
    /// [`super::RtcSessionState::next_timeout`]
    ///
    /// stale handles are skipped before replacement sessions can be polled
    ///
    /// the output is sorted and deduplicated so a session that is both dirty
    /// and timed out is polled once in the current turn
    pub(in super::super) fn collect_ready_sessions(
        &mut self,
        now: Instant,
        ready_sessions: &mut Vec<SessionHandle>,
    ) {
        for session_handle in self.dirty_sessions.drain(..) {
            if let Some(session_state) = self.users.get_mut_by_handle(session_handle) {
                // Clear before polling. A later mutation in this turn must be able
                // to enqueue the session again instead of being hidden by this mark.
                session_state.packet_loop_dirty = false;
                ready_sessions.push(session_handle);
            }
        }
        while let Some(&Reverse((deadline, session_handle))) = self.timeout_queue.peek() {
            if deadline > now {
                break;
            }
            self.timeout_queue.pop();
            let Some(session_state) = self.users.get_mut_by_handle(session_handle) else {
                continue;
            };
            if session_state.next_timeout == Some(deadline) {
                session_state.next_timeout = None;
                ready_sessions.push(session_handle);
            }
        }
        ready_sessions.sort_unstable();
        ready_sessions.dedup();
    }

    /// replace the next `str0m` timeout deadline by worker-local handle
    ///
    /// stale handles are ignored because the session has already left this
    /// worker or the slot now belongs to a later generation
    pub(in super::super) fn update_session_timeout_by_handle(
        &mut self,
        session_handle: SessionHandle,
        next_timeout: Option<Instant>,
    ) {
        let Some(session_state) = self.users.get_mut_by_handle(session_handle) else {
            return;
        };
        // Packet-driven polls can return the same future str0m deadline.
        if session_state.next_timeout == next_timeout {
            return;
        }
        session_state.next_timeout = next_timeout;
        if let Some(next_timeout) = next_timeout {
            self.timeout_queue
                .push(Reverse((next_timeout, session_handle)));
        }
    }

    #[cfg(test)]
    pub(in super::super) fn update_session_timeout(
        &mut self,
        session_key: &TransportSessionKey,
        next_timeout: Option<Instant>,
    ) {
        let Some(session_handle) = self.users.handle_for_key(session_key) else {
            return;
        };
        self.update_session_timeout_by_handle(session_handle, next_timeout);
    }

    /// return the earliest live `str0m` timeout deadline
    ///
    /// stale heap entries are removed while searching
    /// this includes entries for handles whose slot generation no longer names
    /// a live session
    /// callers may invoke this before awaiting because it does not borrow any
    /// session state after returning
    pub(in super::super) fn next_timeout_deadline(&mut self) -> Option<Instant> {
        loop {
            let &Reverse((deadline, session_handle)) = self.timeout_queue.peek()?;
            if self
                .users
                .get_by_handle(session_handle)
                .is_some_and(|session| session.next_timeout == Some(deadline))
            {
                return Some(deadline);
            }
            self.timeout_queue.pop();
        }
    }

    /// remove all explicit scheduler state for a session being torn down
    ///
    /// stale timeout heap entries can remain because the session no longer
    /// validates their deadline
    /// stale handles are also rejected by generation checks before polling
    pub(in super::super) fn clear_session_schedule(&mut self, session_key: &TransportSessionKey) {
        let Some(session_handle) = self.users.handle_for_key(session_key) else {
            return;
        };
        self.dirty_sessions.retain(|dirty| *dirty != session_handle);
        if let Some(session_state) = self.users.get_mut_by_handle(session_handle) {
            session_state.packet_loop_dirty = false;
            session_state.next_timeout = None;
        }
    }
}
