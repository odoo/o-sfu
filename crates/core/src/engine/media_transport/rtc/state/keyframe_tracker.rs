//! keyframe request tracking for RTC producer sources
//!
//! duplicate feedback is absorbed while one request is pending
//! feedback and opaque recovery use bounded retries
//! observable decoder transitions retry until route demand clears

use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    time::{Duration, Instant},
};

use itertools::{Itertools, partition};
use str0m::media::{KeyframeRequestKind, Rid};

use crate::engine::media_transport::TransportMediaId;

pub(in crate::engine::media_transport::rtc) const KEYFRAME_REQUEST_RETRY_DELAY: Duration =
    Duration::from_secs(1);
pub(in crate::engine::media_transport::rtc) const KEYFRAME_REQUEST_RETRY_ATTEMPTS: u8 = 5;
const KEYFRAME_RETRY_DRAIN_LIMIT: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyframeRequestDecision {
    Forward,
    Absorb,
}

/// Selects the retry lifetime for one coalesced source and RID request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::engine::media_transport) enum KeyframeRequestOrigin {
    /// Receiver PLI or FIR with a bounded retry tail.
    ConsumerFeedback,
    /// Recovery request with no RTP-visible completion signal.
    RecoveryHint,
    /// Blocked decoder route retried until refresh or demand removal.
    DecoderTransition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceKeyframeRequest {
    pub src_media: TransportMediaId,
    pub rid: Option<Rid>,
    pub kind: KeyframeRequestKind,
}

impl SourceKeyframeRequest {
    fn targets(self, src_media: TransportMediaId, rid: Option<Rid>) -> bool {
        self.src_media == src_media && self.rid == rid
    }
}

#[derive(Debug, Default)]
pub struct KeyframeRequestTracker {
    pending: Vec<KeyframeRequestState>,
    deadlines: BinaryHeap<Reverse<KeyframeRequestDeadline>>,
    next_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct KeyframeRequestDeadline {
    deadline: Instant,
    id: u64,
}

pub fn coalesce_kf_kind(
    current: KeyframeRequestKind,
    incoming: KeyframeRequestKind,
) -> KeyframeRequestKind {
    match (current, incoming) {
        (KeyframeRequestKind::Fir, _) | (_, KeyframeRequestKind::Fir) => KeyframeRequestKind::Fir,
        _ => current,
    }
}

impl KeyframeRequestTracker {
    /// Arms one source and RID request or strengthens its pending state.
    ///
    /// FIR takes precedence over PLI. A decoder transition also upgrades a
    /// bounded request to retry while route demand remains.
    pub fn track(
        &mut self,
        src_media: TransportMediaId,
        rid: Option<Rid>,
        kind: KeyframeRequestKind,
        origin: KeyframeRequestOrigin,
        now: Instant,
    ) -> KeyframeRequestDecision {
        let Some(request) = self
            .pending
            .iter_mut()
            .find(|request| request.request.targets(src_media, rid))
        else {
            let request = SourceKeyframeRequest {
                src_media,
                rid,
                kind,
            };
            let pending = KeyframeRequestState {
                request,
                deadline: now + KEYFRAME_REQUEST_RETRY_DELAY,
                id: self.next_id,
                retry_policy: RetryPolicy::for_origin(origin),
            };
            self.next_id = self.next_id.saturating_add(1);
            self.deadlines.push(Reverse(pending.deadline()));
            self.pending.push(pending);
            return KeyframeRequestDecision::Forward;
        };
        request.request.kind = coalesce_kf_kind(request.request.kind, kind);
        if origin == KeyframeRequestOrigin::DecoderTransition {
            request.retry_policy = RetryPolicy::WhileDemand;
        }
        KeyframeRequestDecision::Absorb
    }

    pub fn forget(&mut self, src_media: TransportMediaId, rid: Option<Rid>) {
        self.remove_pending(|request| request.request.targets(src_media, rid));
    }

    pub fn forget_source(&mut self, src_media: TransportMediaId) {
        self.remove_pending(|request| request.request.src_media == src_media);
    }

    pub fn observe_refresh(&mut self, src_media: TransportMediaId, rid: Option<Rid>) -> usize {
        self.remove_pending(|request| {
            request.request.src_media == src_media
                && (request.request.rid.is_none() || request.request.rid == rid)
        })
    }

    pub fn drain_due(&mut self, now: Instant, retries: &mut Vec<SourceKeyframeRequest>) {
        let mut drain_budget = KEYFRAME_RETRY_DRAIN_LIMIT;
        while matches!(
            self.deadlines.peek(),
            Some(Reverse(deadline)) if deadline.deadline <= now
        ) && drain_budget > 0
        {
            let Some(Reverse(deadline)) = self.deadlines.pop() else {
                break;
            };
            let Some((index, request)) = self
                .pending
                .iter_mut()
                .find_position(|request| request.matches_deadline(deadline))
            else {
                continue;
            };
            drain_budget -= 1;
            let retry = request.request;
            let reschedule = match &mut request.retry_policy {
                RetryPolicy::Bounded(attempts_remaining) if *attempts_remaining > 0 => {
                    *attempts_remaining -= 1;
                    *attempts_remaining > 0
                }
                RetryPolicy::Bounded(_) => {
                    self.pending.swap_remove(index);
                    continue;
                }
                RetryPolicy::WhileDemand => true,
            };
            retries.push(retry);
            if reschedule {
                request.deadline = now + KEYFRAME_REQUEST_RETRY_DELAY;
                request.id = self.next_id;
                self.next_id = self.next_id.saturating_add(1);
                self.deadlines.push(Reverse(request.deadline()));
            } else {
                self.pending.swap_remove(index);
            }
        }
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        self.deadlines
            .peek()
            .map(|Reverse(deadline)| deadline.deadline)
    }

    /// partitions pending requests in place and purges removed deadlines in one retain pass
    ///
    /// ```text
    /// initial `self.pending`
    ///   +--------------+--------------+--------------+--------------+
    ///   | req A (keep) | req B (drop) | req C (keep) | req D (drop) |
    ///   +--------------+--------------+--------------+--------------+
    ///                          |
    ///                          v  itertools::partition(&mut pending, !should_remove)
    ///   +--------------+--------------+--------------+--------------+
    ///   | req A (keep) | req C (keep) | req B (drop) | req D (drop) |
    ///   +--------------+--------------+--------------+--------------+
    ///   <------- retained_len -------><------- removed = 2 --------->
    ///                          |
    ///                          v  deadlines.retain(|d| removed.all(id != d.id))
    ///                          v  pending.truncate(retained_len)
    /// final `self.pending`
    ///   +--------------+--------------+
    ///   | req A (keep) | req C (keep) |
    ///   +--------------+--------------+
    /// ```
    fn remove_pending(&mut self, should_remove: impl Fn(&KeyframeRequestState) -> bool) -> usize {
        let retained_len = partition(&mut self.pending, |request| !should_remove(request));
        let removed = self.pending.len() - retained_len;
        if removed == 0 {
            return 0;
        }
        self.deadlines.retain(|Reverse(deadline)| {
            self.pending
                .iter()
                .rev()
                .take(removed)
                .all(|request| request.id != deadline.id)
        });
        self.pending.truncate(retained_len);
        removed
    }
}

#[derive(Debug, Clone, Copy)]
struct KeyframeRequestState {
    request: SourceKeyframeRequest,
    deadline: Instant,
    id: u64,
    retry_policy: RetryPolicy,
}

#[derive(Debug, Clone, Copy)]
enum RetryPolicy {
    Bounded(u8),
    WhileDemand,
}

impl RetryPolicy {
    const fn for_origin(origin: KeyframeRequestOrigin) -> Self {
        match origin {
            KeyframeRequestOrigin::ConsumerFeedback | KeyframeRequestOrigin::RecoveryHint => {
                Self::Bounded(KEYFRAME_REQUEST_RETRY_ATTEMPTS)
            }
            KeyframeRequestOrigin::DecoderTransition => Self::WhileDemand,
        }
    }
}

impl KeyframeRequestState {
    fn deadline(self) -> KeyframeRequestDeadline {
        KeyframeRequestDeadline {
            deadline: self.deadline,
            id: self.id,
        }
    }

    fn matches_deadline(self, deadline: KeyframeRequestDeadline) -> bool {
        self.deadline == deadline.deadline && self.id == deadline.id
    }
}
