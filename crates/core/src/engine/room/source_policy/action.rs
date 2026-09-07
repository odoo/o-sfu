use std::time::Instant;

use super::super::media_graph::{PendingUpgrade, SubscriptionKey};
use crate::{
    Bitrate,
    engine::{
        ConnectionId, UserId,
        media_transport::{
            ConsumerActivity, ConsumerRouteControl, SourcePacketGate, TransportConsumerRoute,
        },
        source_model::{
            ConsumerSourceSelection, PolicyPauseReason, PublishedSourceId,
            ReceiverVideoBudgetDiagnostics, SourceSelector,
        },
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum VideoRouteTransition {
    Degraded,
    Paused { reason: PolicyPauseReason },
    Resumed { cleared_reason: PolicyPauseReason },
}

#[derive(Debug, Clone, Copy)]
pub(super) struct VideoRouteAllocationState {
    pub(super) selector: SourceSelector,
    pub(super) policy_pause_reason: Option<PolicyPauseReason>,
    pub(super) selected_bitrate: Bitrate,
}

impl VideoRouteAllocationState {
    pub(super) fn matches_selection(self, selection: ConsumerSourceSelection) -> bool {
        self.selector == selection.selector()
            && self.policy_pause_reason == selection.policy_pause_reason()
    }
}

#[derive(Debug)]
pub(super) struct VideoRouteAllocation {
    pub(super) key: SubscriptionKey,
    pub(super) source_id: PublishedSourceId,
    pub(super) route: TransportConsumerRoute,
    pub(super) interrupts_upgrade: bool,
    pub(super) captured: VideoRouteAllocationState,
    pub(super) planned: VideoRouteAllocationState,
}

#[derive(Debug)]
pub(super) struct ReceiverVideoBudgetPlan {
    pub(super) receiver: UserId,
    pub(super) planned_budget: ReceiverVideoBudgetDiagnostics,
    /// Ordered by [`SubscriptionKey`] for linear reconciliation after transport work.
    pub(super) routes: Vec<VideoRouteAllocation>,
}

/// Receiver timing captured before transport work, including cancellation.
#[derive(Debug)]
pub(super) struct ReceiverPolicyTiming {
    pub receiver: UserId,
    pub connection_id: ConnectionId,
    pub soft_pause_deadline: Option<Instant>,
    pub next_deadline: Option<Instant>,
    /// Existing eligible wakeups survive rejection of an unrelated control.
    pub retained_deadline: Option<Instant>,
}

/// Only changed upgrade state requires validation and a topology write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum UpgradeChange {
    Unchanged,
    Set(Option<PendingUpgrade>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::engine::room) struct ConsumerPacketSelectionUpdate {
    pub(super) key: SubscriptionKey,
    pub(super) source_id: PublishedSourceId,
    pub(in crate::engine::room) route: TransportConsumerRoute,
    pub(super) selector: SourceSelector,
    pub(super) policy_pause_reason: Option<PolicyPauseReason>,
    pub(super) planned_budget: ReceiverVideoBudgetDiagnostics,
    pub(super) transition: Option<VideoRouteTransition>,
    pub(super) selected_estimated_bitrate: Option<Bitrate>,
    pub(super) upgrade: UpgradeChange,
    pub(super) interrupts_upgrade: bool,
    pub(super) packet_gate: Option<SourcePacketGate>,
    pub(super) route_activity_changed: bool,
    pub(super) request_keyframe: bool,
}

impl ConsumerPacketSelectionUpdate {
    pub(in crate::engine::room) fn route_activity(
        key: SubscriptionKey,
        source_id: PublishedSourceId,
        route: TransportConsumerRoute,
        current_selection: ConsumerSourceSelection,
        policy_pause_reason: Option<PolicyPauseReason>,
    ) -> Option<Self> {
        (policy_pause_reason != current_selection.policy_pause_reason()).then(|| Self {
            key,
            source_id,
            route,
            selector: current_selection.selector(),
            policy_pause_reason,
            planned_budget: current_selection.budget(),
            transition: None,
            selected_estimated_bitrate: None,
            upgrade: UpgradeChange::Unchanged,
            interrupts_upgrade: false,
            packet_gate: None,
            route_activity_changed: true,
            request_keyframe: false,
        })
    }

    pub(super) const fn requires_media_transport_effect(&self) -> bool {
        self.packet_gate.is_some() || self.route_activity_changed || self.request_keyframe
    }

    pub(in crate::engine::room) fn route_control(&self) -> ConsumerRouteControl {
        let mut control =
            ConsumerRouteControl::new(self.route.clone()).request_keyframe(self.request_keyframe);
        if self.route_activity_changed {
            control = control.activity(ConsumerActivity::from_active(self.route_active()));
        }
        if let Some(packet_gate) = &self.packet_gate {
            control = control.packet_gate(packet_gate.clone());
        }
        control
    }

    pub(in crate::engine::room) const fn route_active(&self) -> bool {
        self.policy_pause_reason.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct FeaturedUserUpdate {
    pub(super) user_id: UserId,
    pub(super) connection_id: ConnectionId,
    pub(super) featured: Option<bool>,
}
