use super::{PolicyPauseReason, ReceiverVideoBudgetDiagnostics, SourceEncodingId};
use crate::Bitrate;

/// Resolved packet-selection command for one consumer/source route.
///
/// The budget planner writes selectors into room state. A later projection step
/// turns them into transport packet gates such as "open" or "forward this RID".
/// # Example situations
///
/// [`Self::Open`] means the route has no source-level packet gate.
/// [`Self::Encoding`] means "forward the negotiated RID for this encoding".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SourceSelector {
    /// Forward the source without a source-level packet gate.
    ///
    /// This is the default for sources that are not controlled by receiver-video
    /// adaptation or when the planner has not selected a narrower gate.
    #[default]
    Open,
    /// Forward only one advertised source encoding.
    ///
    /// Projection maps the encoding id to its negotiated RID. If the encoding
    /// has no RID, projection fails rather than guessing at packet identity.
    Encoding(SourceEncodingId),
}

impl SourceSelector {
    #[must_use]
    pub const fn selected_encoding(self) -> Option<SourceEncodingId> {
        match self {
            Self::Encoding(encoding_id) => Some(encoding_id),
            Self::Open => None,
        }
    }
}

/// Receiver-side policy state for one attached publication.
///
/// `active` preserves stored subscription intent while `policy_pause_reason`
/// may withhold delivery without erasing that intent. `selector` is a resolved
/// room choice that projection maps to a transport packet gate.
///
/// `ConsumerSourceSelection` carries no publication or route identity. Async
/// updates must still match the current `PublishedSourceId` and exact consumer
/// route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerSourceSelection {
    active: bool,
    selector: SourceSelector,
    policy_pause_reason: Option<PolicyPauseReason>,
    budget: ReceiverVideoBudgetDiagnostics,
}

impl ConsumerSourceSelection {
    #[must_use]
    pub const fn open(active: bool) -> Self {
        Self {
            active,
            selector: SourceSelector::Open,
            policy_pause_reason: None,
            budget: ReceiverVideoBudgetDiagnostics::new(None, None, 0, Bitrate::zero()),
        }
    }

    #[must_use]
    pub const fn active(self) -> bool {
        self.active
    }

    #[must_use]
    pub const fn selector(self) -> SourceSelector {
        self.selector
    }

    #[must_use]
    pub const fn policy_pause_reason(self) -> Option<PolicyPauseReason> {
        self.policy_pause_reason
    }

    #[must_use]
    pub const fn policy_allows_delivery(self) -> bool {
        self.policy_pause_reason.is_none()
    }

    /// Returns whether this receiver selection currently permits packet delivery.
    ///
    /// Use this for route-state projections, load accounting and keyframe
    /// targeting. Source-policy planners should read [`Self::active`] so
    /// policy-paused routes can be resumed.
    #[must_use]
    pub const fn delivery_active(self) -> bool {
        self.active && self.policy_allows_delivery()
    }

    #[must_use]
    pub const fn budget(self) -> ReceiverVideoBudgetDiagnostics {
        self.budget
    }

    pub const fn set_active(&mut self, active: bool) {
        self.active = active;
    }

    pub const fn set_selector(&mut self, selector: SourceSelector) {
        self.selector = selector;
    }

    pub const fn set_policy_pause_reason(&mut self, reason: Option<PolicyPauseReason>) {
        self.policy_pause_reason = reason;
    }

    pub const fn set_budget(&mut self, budget: ReceiverVideoBudgetDiagnostics) {
        self.budget = budget;
    }
}
