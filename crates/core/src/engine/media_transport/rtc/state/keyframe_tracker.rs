//! keyframe request tracking for RTC producer sources
//!
//! duplicate feedback is absorbed while one request is pending
//! feedback and opaque recovery use bounded retries
//! observable decoder transitions retry until route demand clears

use std::{
    cmp::{Ordering, Reverse},
    collections::{BTreeSet, BinaryHeap, HashMap},
    time::{Duration, Instant},
};

use itertools::{Itertools, partition};
use str0m::media::{KeyframeRequestKind, Rid};

use super::relay_registry::RelayTargetId;
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

/// Dispatch timing for requests delivered to one producer worker.
///
/// This state is independent of consumer retry tracking so a decoder refresh
/// cannot reset the publisher's rate limit. Only deferred requests have a
/// deadline in the worker's wakeup queue.
#[derive(Debug, Default)]
pub struct PublisherKeyframeLimiter {
    targets: HashMap<(TransportMediaId, Option<Rid>), PublisherKeyframeTarget>,
    deadlines: BTreeSet<PublisherDeadline>,
}

/// libwebrtc uses 300 ms as the configurable default for encoder feedback.
/// <https://webrtc.googlesource.com/src/+/refs/heads/main/video/encoder_rtcp_feedback.cc>
const PUBLISHER_KEYFRAME_INTERVAL: Duration = Duration::from_millis(300);
pub const PUBLISHER_KEYFRAME_DRAIN_LIMIT: usize = 64;

#[derive(Debug, Default)]
struct PublisherKeyframeTarget {
    last_sent: Option<Instant>,
    deferred: Option<DeferredPublisherRequest>,
    pending_deadline: Option<Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PublisherDeadline {
    at: Instant,
    key: (TransportMediaId, Option<Rid>),
}

impl PartialOrd for PublisherDeadline {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PublisherDeadline {
    fn cmp(&self, other: &Self) -> Ordering {
        self.at
            .cmp(&other.at)
            .then_with(|| self.key.0.cmp(&other.key.0))
            .then_with(|| self.key.1.as_deref().cmp(&other.key.1.as_deref()))
    }
}

#[derive(Debug, Default)]
struct DeferredPublisherRequest {
    demands: Vec<PublisherDemandRequest>,
}

#[derive(Debug, Clone, Copy)]
struct PublisherDemandRequest {
    demand: PublisherRequestDemand,
    kind: KeyframeRequestKind,
}

impl DeferredPublisherRequest {
    fn record(&mut self, demand: PublisherRequestDemand, kind: KeyframeRequestKind) {
        if let Some(existing) = self.demands.iter_mut().find(|entry| entry.demand == demand) {
            existing.kind = coalesce_kf_kind(existing.kind, kind);
        } else {
            self.demands.push(PublisherDemandRequest { demand, kind });
        }
    }

    fn active_kind(
        &self,
        mut active: impl FnMut(PublisherRequestDemand) -> bool,
    ) -> Option<KeyframeRequestKind> {
        self.demands
            .iter()
            .filter(|entry| active(entry.demand))
            .map(|entry| entry.kind)
            .reduce(coalesce_kf_kind)
    }
}

/// Route demand authorizing a publisher request.
///
/// The requested RID stays distinct from the concrete publisher RID used for
/// cooldown so source-wide Open-gate demand survives RID expansion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublisherRequestDemand {
    Local(Option<Rid>),
    Source(Option<Rid>),
    Relay(RelayTargetId),
}

#[derive(Debug)]
pub struct DuePublisherRequest {
    pub src_media: TransportMediaId,
    pub rid: Option<Rid>,
    pending: DeferredPublisherRequest,
}

impl DuePublisherRequest {
    pub fn active_kind(
        &self,
        active: impl FnMut(PublisherRequestDemand) -> bool,
    ) -> Option<KeyframeRequestKind> {
        self.pending.active_kind(active)
    }
}

impl PublisherKeyframeLimiter {
    /// Returns the strongest valid request or retains it until the interval ends.
    pub fn request(
        &mut self,
        src_media: TransportMediaId,
        rid: Option<Rid>,
        kind: KeyframeRequestKind,
        demand: PublisherRequestDemand,
        now: Instant,
        mut demand_active: impl FnMut(PublisherRequestDemand) -> bool,
    ) -> Option<KeyframeRequestKind> {
        let key = (src_media, rid);
        let target = self.targets.entry(key).or_default();
        if let Some(pending) = &mut target.deferred {
            pending.demands.retain(|entry| demand_active(entry.demand));
            if pending.demands.is_empty() {
                target.deferred = None;
                if let Some(at) = target.pending_deadline.take() {
                    self.deadlines.remove(&PublisherDeadline { at, key });
                }
            }
        }
        if let Some(last_sent) = target.last_sent
            && now.saturating_duration_since(last_sent) < PUBLISHER_KEYFRAME_INTERVAL
        {
            target
                .deferred
                .get_or_insert_with(Default::default)
                .record(demand, kind);
            if target.pending_deadline.is_none() {
                let at = last_sent + PUBLISHER_KEYFRAME_INTERVAL;
                target.pending_deadline = Some(at);
                self.deadlines.insert(PublisherDeadline { at, key });
            }
            return None;
        }
        if let Some(at) = target.pending_deadline.take() {
            self.deadlines.remove(&PublisherDeadline { at, key });
        }
        Some(
            target
                .deferred
                .take()
                .and_then(|pending| pending.active_kind(|_| true))
                .map_or(kind, |pending_kind| coalesce_kf_kind(pending_kind, kind)),
        )
    }

    pub fn sent(&mut self, src_media: TransportMediaId, rid: Option<Rid>, now: Instant) {
        if let Some(target) = self.targets.get_mut(&(src_media, rid)) {
            target.last_sent = Some(now);
        }
    }

    pub fn observe_refresh(&mut self, src_media: TransportMediaId, rid: Option<Rid>) {
        if self.deadlines.is_empty() {
            return;
        }
        self.cancel_target((src_media, rid));
        if rid.is_some() {
            self.cancel_target((src_media, None));
        }
    }

    pub fn cancel_source(&mut self, src_media: TransportMediaId) {
        for (key, target) in &mut self.targets {
            if key.0 == src_media {
                target.deferred = None;
                if let Some(at) = target.pending_deadline.take() {
                    self.deadlines.remove(&PublisherDeadline { at, key: *key });
                }
            }
        }
    }

    pub fn forget_source(&mut self, src_media: TransportMediaId) {
        let deadlines = &mut self.deadlines;
        self.targets.retain(|key, target| {
            if key.0 != src_media {
                return true;
            }
            if let Some(at) = target.pending_deadline {
                deadlines.remove(&PublisherDeadline { at, key: *key });
            }
            false
        });
    }

    pub fn retire_local(&mut self, src_media: TransportMediaId) {
        self.retire_demands(src_media, |demand| {
            matches!(demand, PublisherRequestDemand::Local(_))
        });
    }

    pub fn retire_relay(&mut self, src_media: TransportMediaId, target_id: RelayTargetId) {
        self.retire_demands(src_media, |demand| {
            demand == PublisherRequestDemand::Relay(target_id)
        });
    }

    fn retire_demands(
        &mut self,
        src_media: TransportMediaId,
        remove: impl Fn(PublisherRequestDemand) -> bool,
    ) {
        for (key, target) in &mut self.targets {
            if key.0 != src_media {
                continue;
            }
            if let Some(pending) = &mut target.deferred {
                pending.demands.retain(|entry| !remove(entry.demand));
                if pending.demands.is_empty() {
                    target.deferred = None;
                    if let Some(at) = target.pending_deadline.take() {
                        self.deadlines.remove(&PublisherDeadline { at, key: *key });
                    }
                }
            }
        }
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        self.deadlines.first().map(|deadline| deadline.at)
    }

    pub fn take_due(&mut self, now: Instant) -> Option<DuePublisherRequest> {
        let deadline = *self.deadlines.first()?;
        if deadline.at > now {
            return None;
        }
        self.deadlines.pop_first();
        let target = self.targets.get_mut(&deadline.key)?;
        target.pending_deadline = None;
        let pending = target.deferred.take()?;
        Some(DuePublisherRequest {
            src_media: deadline.key.0,
            rid: deadline.key.1,
            pending,
        })
    }

    fn cancel_target(&mut self, key: (TransportMediaId, Option<Rid>)) {
        if let Some(target) = self.targets.get_mut(&key) {
            target.deferred = None;
            if let Some(at) = target.pending_deadline.take() {
                self.deadlines.remove(&PublisherDeadline { at, key });
            }
        }
    }
}

#[cfg(test)]
#[path = "TESTS/publisher_keyframes.rs"]
mod publisher_tests;
