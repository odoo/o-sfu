//! tracks in-flight recording requests and their timeout timers

use std::mem;

use super::{
    Command, Commands, PendingRequestKind,
    timers::{REQUEST_TIMEOUT_TIMER_BASE, RequestTimeoutId},
};
use crate::signaling::RequestId;

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingRequestState {
    request_id: RequestId,
    kind: PendingRequestKind,
    timeout_timer_id: RequestTimeoutId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PendingRequestStart {
    pub(super) request_id: RequestId,
    pub(super) timeout_timer_id: RequestTimeoutId,
}

/// Owns recording request identities, timeout identities and resolve-once state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RequestTracker {
    next_request_counter: u64,
    next_timeout_timer_id: RequestTimeoutId,
    pending_requests: Vec<PendingRequestState>,
}

impl Default for RequestTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl RequestTracker {
    pub(super) const fn new() -> Self {
        Self {
            next_request_counter: 0,
            next_timeout_timer_id: REQUEST_TIMEOUT_TIMER_BASE,
            pending_requests: Vec::new(),
        }
    }

    pub(super) fn begin(&mut self, kind: PendingRequestKind) -> PendingRequestStart {
        let request_id = self.allocate_request_id();
        let timeout_timer_id = self.allocate_timeout_timer_id();
        self.pending_requests.push(PendingRequestState {
            request_id: request_id.clone(),
            kind,
            timeout_timer_id,
        });
        PendingRequestStart {
            request_id,
            timeout_timer_id,
        }
    }

    pub(super) fn resolve_response(
        &mut self,
        response_to: &RequestId,
        expected_kind: PendingRequestKind,
        ok: bool,
    ) -> Commands {
        let Some(index) = self.pending_requests.iter().position(|pending_request| {
            pending_request.request_id == *response_to && pending_request.kind == expected_kind
        }) else {
            return Vec::new();
        };
        // Remove before settling so duplicate responses and stale timers cannot settle twice.
        vec![complete_pending_request(
            self.pending_requests.remove(index),
            ok,
        )]
    }

    pub(super) fn resolve_timeout(&mut self, timeout_id: RequestTimeoutId) -> Option<Commands> {
        let index = self
            .pending_requests
            .iter()
            .position(|pending_request| pending_request.timeout_timer_id == timeout_id)?;
        Some(vec![reject_pending_request(
            self.pending_requests.remove(index),
            "bus request timed out",
        )])
    }

    pub(super) fn clear(&mut self) {
        self.pending_requests.clear();
    }

    /// Fails pending requests in begin order.
    pub(super) fn fail_all(&mut self) -> Commands {
        mem::take(&mut self.pending_requests)
            .into_iter()
            .map(|pending_request| reject_pending_request(pending_request, "bus closed"))
            .collect()
    }

    fn allocate_request_id(&mut self) -> RequestId {
        let request_id = RequestId::new(self.next_request_counter.to_string());
        self.next_request_counter = self.next_request_counter.saturating_add(1);
        request_id
    }

    fn allocate_timeout_timer_id(&mut self) -> RequestTimeoutId {
        let timer_id = self.next_timeout_timer_id;
        self.next_timeout_timer_id = timer_id.next();
        timer_id
    }
}

fn complete_pending_request(pending_request: PendingRequestState, ok: bool) -> Command {
    Command::CompletePendingRequest {
        request_id: pending_request.request_id,
        timeout_timer_id: pending_request.timeout_timer_id.raw(),
        ok,
    }
}

fn reject_pending_request(pending_request: PendingRequestState, error: &str) -> Command {
    Command::RejectPendingRequest {
        request_id: pending_request.request_id,
        timeout_timer_id: pending_request.timeout_timer_id.raw(),
        error: error.to_owned(),
    }
}
