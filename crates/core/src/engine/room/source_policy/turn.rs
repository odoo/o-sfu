//! source-policy apply ownership without transport awaits under the room lock

use std::{borrow::Cow, collections::BTreeMap, time::Instant};

use o_sfu_router::MediaKind;
use o_sfu_telemetry::schema::event as telemetry_event;
use tracing::info;

use super::{
    action::{
        ConsumerPacketSelectionUpdate, FeaturedUserUpdate, ReceiverPolicyTiming,
        ReceiverVideoBudgetPlan, UpgradeChange, VideoRouteTransition,
    },
    audio,
    input::SourcePolicySnapshot,
    video,
};
use crate::engine::{
    ConnectionId,
    media_transport::{
        ActiveSpeakerSource, MediaTransport, ReceiverBandwidthSnapshot, ReceiverBweTargetUpdate,
        TransportBitrateSnapshot,
    },
    metrics::{self, BudgetSolverOutcome},
    room::{
        Room, RoomEventMessage, SourcePolicyGuard, effects::transport::RoomRouteEffects,
        outbound::MessageFanout, state::RoomState,
    },
    source_model::{
        PolicyPauseReason, ReceiverVideoBudgetDiagnostics, SourceAdaptationPolicy,
        SourceEncodingId, SourceSelector,
    },
};

/// Deferred request to recompute one room's source policy.
///
/// `RoomEffects` decides whether the turn runs before or after its transport
/// work. [`Self::execute`] serializes it with publication activity.
#[derive(Debug, Default)]
pub struct SourcePolicyTurn {
    requested: bool,
}

impl SourcePolicyTurn {
    pub const fn packet_selection() -> Self {
        Self { requested: true }
    }

    pub fn request(&mut self) {
        self.requested = true;
    }

    pub async fn execute(
        self,
        room: &Room,
        media_transport: Option<&MediaTransport>,
        active_speaker_sources: Option<&[ActiveSpeakerSource]>,
    ) {
        if !self.requested {
            return;
        }
        let guard = room.lock_source_policy().await;
        self.execute_guarded(&guard, media_transport, active_speaker_sources)
            .await;
    }

    pub(in crate::engine::room) async fn execute_guarded(
        self,
        guard: &SourcePolicyGuard<'_>,
        media_transport: Option<&MediaTransport>,
        active_speaker_sources: Option<&[ActiveSpeakerSource]>,
    ) {
        self.execute_observed(
            guard,
            media_transport,
            active_speaker_sources,
            None,
            Instant::now(),
        )
        .await;
    }

    async fn execute_observed(
        self,
        guard: &SourcePolicyGuard<'_>,
        media_transport: Option<&MediaTransport>,
        active_speaker_sources: Option<&[ActiveSpeakerSource]>,
        bandwidth: Option<&ReceiverBandwidthSnapshot>,
        now: Instant,
    ) -> bool {
        if !self.requested {
            return false;
        }
        let Some(media_transport) = media_transport else {
            return false;
        };
        let room = guard.room();
        let transaction = if let Some(sources) = active_speaker_sources {
            run_packet_selection(room, sources, media_transport, bandwidth, now).await
        } else {
            let sources = media_transport.active_speaker_source_snapshot().await;
            run_packet_selection(room, &sources, media_transport, bandwidth, now).await
        };
        let Some(transaction) = transaction else {
            media_transport.set_source_policy_deadline(room.instance_id(), None);
            return false;
        };
        transaction.commit(room, media_transport).await;
        true
    }
}

#[cfg(feature = "internal-benchmarks")]
pub async fn run_source_policy_turn_for_benchmark(
    room: &Room,
    media_transport: &MediaTransport,
    bandwidth: &ReceiverBandwidthSnapshot,
    now: Instant,
) -> bool {
    let guard = room.lock_source_policy().await;
    SourcePolicyTurn::packet_selection()
        .execute_observed(&guard, Some(media_transport), None, Some(bandwidth), now)
        .await
}

async fn run_packet_selection(
    room: &Room,
    active_speakers: &[ActiveSpeakerSource],
    media_transport: &MediaTransport,
    bandwidth_override: Option<&ReceiverBandwidthSnapshot>,
    now: Instant,
) -> Option<SourcePolicyTransaction> {
    let sessions = {
        let state = room.state.read().await;
        state
            .transport_user_entries()
            .map(|(user_id, connection_id)| state.transport_user_key(user_id, connection_id))
            .collect::<Vec<_>>()
    };
    let receiver_bandwidth = bandwidth_override.map_or_else(
        || Cow::Owned(media_transport.receiver_bandwidth_snapshot(&sessions)),
        Cow::Borrowed,
    );
    let source_bitrate = media_transport.transport_bitrate_snapshot(&sessions);
    let state = room.state.read().await;
    SourcePolicyTransaction::plan(
        &state,
        active_speakers,
        &receiver_bandwidth,
        &source_bitrate,
        now,
    )
}

#[derive(Debug)]
pub(in crate::engine::room) struct SourcePolicyTransaction {
    route_effects: RoomRouteEffects,
    state_updates: Vec<ConsumerPacketSelectionUpdate>,
    receiver_video_budget_plans: Vec<ReceiverVideoBudgetPlan>,
    featured_users: Vec<FeaturedUserUpdate>,
    video_allocation_revision: u64,
    outstanding_controls: BTreeMap<ConnectionId, usize>,
    receiver_timing: Vec<ReceiverPolicyTiming>,
    planned_at: Instant,
}

impl SourcePolicyTransaction {
    pub(in crate::engine::room) fn plan(
        state: &RoomState,
        active_speakers: &[ActiveSpeakerSource],
        receiver_bandwidth: &ReceiverBandwidthSnapshot,
        source_bitrate: &TransportBitrateSnapshot,
        now: Instant,
    ) -> Option<Self> {
        let input = SourcePolicySnapshot::from_state(
            state,
            active_speakers,
            receiver_bandwidth,
            source_bitrate,
        );
        let mut tx = Self {
            video_allocation_revision: state.topology.video_allocation_revision(),
            route_effects: RoomRouteEffects::default(),
            state_updates: Vec::new(),
            receiver_video_budget_plans: Vec::new(),
            featured_users: Vec::new(),
            outstanding_controls: BTreeMap::new(),
            receiver_timing: Vec::new(),
            planned_at: now,
        };
        audio::append_audio_route_activity(&mut tx, &input);
        video::append_receiver_video_policy(&mut tx, state, &input, now);
        tx.featured_users = input.featured_user_updates;
        (!tx.is_empty()).then_some(tx)
    }

    pub(super) fn push_state_update(&mut self, update: ConsumerPacketSelectionUpdate) {
        self.state_updates.push(update);
    }

    pub(super) fn push_route_update(&mut self, update: ConsumerPacketSelectionUpdate) {
        self.expect_route_update(update.route.consumer_session_key().connection_id());
        self.route_effects.source_policy_update(update);
    }

    pub(super) fn expect_route_update(&mut self, connection: ConnectionId) {
        *self.outstanding_controls.entry(connection).or_default() += 1;
    }

    pub(super) fn push_receiver_timing(&mut self, timing: ReceiverPolicyTiming) {
        self.receiver_timing.push(timing);
    }

    pub(super) fn push_receiver_video_budget_plan(&mut self, plan: ReceiverVideoBudgetPlan) {
        self.receiver_video_budget_plans.push(plan);
    }

    pub(super) fn set_receiver_bwe_targets(&mut self, targets: Vec<ReceiverBweTargetUpdate>) {
        self.route_effects.set_receiver_bwe_targets(targets);
    }

    async fn commit(self, room: &Room, media_transport: &MediaTransport) {
        let Self {
            route_effects,
            mut state_updates,
            receiver_video_budget_plans,
            featured_users,
            video_allocation_revision,
            mut outstanding_controls,
            receiver_timing,
            planned_at,
        } = self;
        let accepted_route_updates = if route_effects.is_empty() {
            Vec::new()
        } else {
            // Only accepted transport controls join state-only updates. Room
            // state must not claim a selection that its worker rejected.
            route_effects.execute(room.uuid(), media_transport).await
        };
        // Rejections gate only their receiver's temporal state. PR1 budget
        // reconciliation still observes acceptance across the whole transaction.
        for update in &accepted_route_updates {
            if let Some(remaining) =
                outstanding_controls.get_mut(&update.route.consumer_session_key().connection_id())
            {
                *remaining -= 1;
            }
        }
        state_updates.extend(accepted_route_updates);
        if state_updates.is_empty()
            && receiver_video_budget_plans.is_empty()
            && featured_users.is_empty()
            && receiver_timing.is_empty()
        {
            media_transport.set_source_policy_deadline(room.instance_id(), None);
            return;
        }
        let mut state = room.state.write().await;
        let (committed_updates, deadline) = commit_packet_updates(
            &mut state,
            state_updates,
            &receiver_video_budget_plans,
            video_allocation_revision,
            &outstanding_controls,
            &receiver_timing,
            planned_at,
        );
        let info_fanout = commit_featured_user_updates(&mut state, &featured_users);
        drop(state);
        record_committed_selection_updates(room, &committed_updates);
        if let Some(info_fanout) = info_fanout {
            info_fanout.emit();
        }
        media_transport.set_source_policy_deadline(room.instance_id(), deadline);
    }

    #[cfg(test)]
    pub(in crate::engine::room) async fn execute(
        self,
        room: &Room,
        media_transport: &MediaTransport,
    ) {
        self.commit(room, media_transport).await;
    }

    fn is_empty(&self) -> bool {
        self.state_updates.is_empty()
            && self.route_effects.is_empty()
            && self.receiver_video_budget_plans.is_empty()
            && self.featured_users.is_empty()
            && self.receiver_timing.is_empty()
    }
}

fn record_committed_selection_updates(room: &Room, updates: &[ConsumerPacketSelectionUpdate]) {
    for update in updates {
        if update.packet_gate.is_some() {
            room.metrics
                .record_source_selection_update(metrics::source_selection_kind(update.selector));
        }
        let Some(transition) = update.transition else {
            continue;
        };
        let (metric_outcome, outcome, reason) = match transition {
            VideoRouteTransition::Degraded => (BudgetSolverOutcome::Degraded, "degraded", None),
            VideoRouteTransition::Paused { reason } => {
                (BudgetSolverOutcome::Paused, "paused", Some(reason))
            }
            VideoRouteTransition::Resumed { cleared_reason } => (
                BudgetSolverOutcome::Resumed,
                "resumed",
                Some(cleared_reason),
            ),
        };
        room.metrics.record_budget_solver_outcome(metric_outcome);
        let consumer = update.route.consumer_session_key();
        let source = update.route.source_session_key();
        info!(
            event = telemetry_event::SOURCE_POLICY_ROUTE_CHANGED,
            room_id = room.uuid(),
            user_id = %consumer.user_id().path_segment(),
            connection_id = consumer.connection_id().as_u64(),
            media_worker_id = consumer.media_worker_id().as_usize(),
            transport_media_id = update.route.consumer_transport_media_id().as_u64(),
            producer_user_id = %source.user_id().path_segment(),
            source_transport_media_id = update.route.source_transport_media_id().as_u64(),
            stream_id = %update.key.stream,
            outcome,
            reason = reason.map(policy_pause_reason_name),
            latest_receiver_bandwidth_estimate_bps = update
                .planned_budget
                .latest_receiver_bandwidth()
                .map(crate::Bitrate::as_bps),
            selected_video_budget_bps = update
                .planned_budget
                .selected_video_budget()
                .map(crate::Bitrate::as_bps),
            planned_active_video_route_count = update.planned_budget.active_video_route_count(),
            planned_selected_video_bitrate_bps = update
                .planned_budget
                .selected_video_bitrate()
                .as_bps(),
            selector = source_selector_name(update.selector),
            selected_encoding_id = update
                .selector
                .selected_encoding()
                .map(SourceEncodingId::as_u64),
            selected_estimated_bitrate_bps = update
                .selected_estimated_bitrate
                .map(crate::Bitrate::as_bps),
            "source policy route changed"
        );
    }
}

const fn source_selector_name(selector: SourceSelector) -> &'static str {
    match selector {
        SourceSelector::Open => "open",
        SourceSelector::Encoding(_) => "encoding",
    }
}

const fn policy_pause_reason_name(reason: PolicyPauseReason) -> &'static str {
    match reason {
        PolicyPauseReason::BudgetPressure => "budget_pressure",
        PolicyPauseReason::HiddenTile => "hidden_tile",
        PolicyPauseReason::OverflowTile => "overflow_tile",
        PolicyPauseReason::MissingUsableLayer => "missing_usable_layer",
        PolicyPauseReason::AudioSpeakerLimit => "audio_speaker_limit",
        PolicyPauseReason::ReceiverDeafened => "receiver_deafened",
        PolicyPauseReason::VideoDownloadLimit => "video_download_limit",
        PolicyPauseReason::SourceBitrateLimit => "source_bitrate_limit",
    }
}

fn commit_packet_updates(
    state: &mut RoomState,
    mut updates: Vec<ConsumerPacketSelectionUpdate>,
    receiver_video_budget_plans: &[ReceiverVideoBudgetPlan],
    video_allocation_revision: u64,
    outstanding_controls: &BTreeMap<ConnectionId, usize>,
    receiver_timing: &[ReceiverPolicyTiming],
    now: Instant,
) -> (Vec<ConsumerPacketSelectionUpdate>, Option<Instant>) {
    let allocation_plan_is_current =
        state.topology.video_allocation_revision() == video_allocation_revision;
    let all_route_controls_accepted = outstanding_controls.values().all(|count| *count == 0);
    let reconcile_planned_budgets = !allocation_plan_is_current || !all_route_controls_accepted;
    let receiver_controls_accepted = |connection| {
        outstanding_controls
            .get(&connection)
            .is_none_or(|count| *count == 0)
    };
    let mut next_deadline = None;
    // The allocation revision covers the captured subscriptions, sources and
    // exact routes. Check it before selection writes advance it within this turn.
    if allocation_plan_is_current {
        for timing in receiver_timing {
            let accepted = receiver_controls_accepted(timing.connection_id);
            let Some(user) = state.user_mut_for_connection(&timing.receiver, timing.connection_id)
            else {
                continue;
            };
            // Recovery ends continuity even when a sibling control rejects.
            if timing.soft_pause_deadline.is_none() || accepted {
                user.video_soft_pause_deadline = timing.soft_pause_deadline;
            }
            // A rejected sibling cannot cancel an already committed future hold.
            // Newly proposed deadlines still require acceptance and due retries
            // remain unscheduled.
            let deadline = if accepted {
                timing.next_deadline
            } else {
                timing.retained_deadline
            };
            if let Some(deadline) = deadline.filter(|deadline| *deadline > now) {
                next_deadline =
                    Some(next_deadline.map_or(deadline, |next: Instant| next.min(deadline)));
            }
        }
        // Cancellation follows eligibility, not transport acceptance. Rejected
        // downsteps must not leave an old upgrade eligible through an interruption.
        for route in receiver_video_budget_plans
            .iter()
            .flat_map(|plan| &plan.routes)
            .filter(|route| route.interrupts_upgrade)
        {
            state
                .topology
                .update_consumer_upgrade(&route.key, route.source_id, &route.route, None);
        }
        for update in updates.iter().filter(|update| update.interrupts_upgrade) {
            state.topology.update_consumer_upgrade(
                &update.key,
                update.source_id,
                &update.route,
                None,
            );
        }
    }
    updates.retain_mut(|update| {
        let commit_planned_budget = allocation_plan_is_current
            && (!reconcile_planned_budgets
                || !receiver_video_budget_plans
                    .iter()
                    .any(|plan| plan.receiver == update.key.receiver));
        let committed = state.topology.update_consumer_source_selection(
            &update.key,
            update.source_id,
            &update.route,
            |selection| {
                selection.set_selector(update.selector);
                selection.set_policy_pause_reason(update.policy_pause_reason);
                if commit_planned_budget {
                    selection.set_budget(update.planned_budget);
                }
            },
        );
        if committed
            && update.transition.is_some()
            && !commit_planned_budget
            && !route_transition_remains_observable(state, update)
        {
            update.transition = None;
        }
        if let UpgradeChange::Set(pending_upgrade) = update.upgrade
            && committed
            && allocation_plan_is_current
            && receiver_controls_accepted(update.route.consumer_session_key().connection_id())
            && state.user_connection_id(&update.key.receiver)
                == Some(update.route.consumer_session_key().connection_id())
        {
            state.topology.update_consumer_upgrade(
                &update.key,
                update.source_id,
                &update.route,
                pending_upgrade,
            );
        }
        committed
    });
    if reconcile_planned_budgets {
        for plan in receiver_video_budget_plans {
            reconcile_receiver_video_budget(state, plan);
        }
    }
    (updates, next_deadline)
}

fn route_transition_remains_observable(
    state: &RoomState,
    update: &ConsumerPacketSelectionUpdate,
) -> bool {
    state.user_connection_id(&update.key.receiver)
        == Some(update.route.consumer_session_key().connection_id())
        && state
            .topology
            .committed_consumer_route_for_key(&update.key)
            .is_some_and(|route| {
                route.source.descriptor.source_id() == update.source_id
                    && route.route == &update.route
                    && route.source.active
                    && route.selection.active()
            })
}

fn reconcile_receiver_video_budget(state: &mut RoomState, plan: &ReceiverVideoBudgetPlan) {
    let receiver = &plan.receiver;
    let receiver_connection_id = state.user_connection_id(receiver);
    // Async route controls may accept only part of `plan`. Rebuild shared
    // diagnostics from captured bitrates because ridless observations are
    // unavailable after the transport await. If any participating route no longer
    // matches its captured or planned allocation, retain the previous diagnostics
    // rather than publish a partial receiver-wide view.
    let mut active_route_count = 0;
    let mut selected_video_bitrate = crate::Bitrate::zero();
    let mut update_targets = Vec::with_capacity(plan.routes.len());
    let mut plan_index = 0;
    for route in state
        .topology
        .committed_consumer_routes_for_user(receiver)
        .filter(|route| {
            receiver_connection_id == Some(route.route.consumer_session_key().connection_id())
        })
    {
        let policy = route.source.descriptor.policy();
        if route.source.descriptor.media_kind() != MediaKind::Video
            || (policy.adaptation() == SourceAdaptationPolicy::None
                && policy.video_bitrate_cap().is_none())
        {
            continue;
        }
        while plan
            .routes
            .get(plan_index)
            .is_some_and(|planned| &planned.key < route.key)
        {
            plan_index += 1;
        }
        let participating = route.source.active && route.selection.active();
        let Some(planned) = plan
            .routes
            .get(plan_index)
            .filter(|planned| &planned.key == route.key)
        else {
            if participating {
                return;
            }
            continue;
        };
        plan_index += 1;
        if planned.source_id != route.source.descriptor.source_id() || planned.route != *route.route
        {
            if participating {
                return;
            }
            continue;
        }
        let planned_state = planned.planned;
        let selected_bitrate = if planned_state.matches_selection(route.selection) {
            planned_state.selected_bitrate
        } else if planned.captured.matches_selection(route.selection) {
            planned.captured.selected_bitrate
        } else {
            if participating {
                return;
            }
            continue;
        };
        update_targets.push(planned);
        if participating && route.selection.policy_pause_reason().is_none() {
            active_route_count += 1;
            selected_video_bitrate = selected_video_bitrate.saturating_add(selected_bitrate);
        }
    }
    let budget = ReceiverVideoBudgetDiagnostics::new(
        plan.planned_budget.latest_receiver_bandwidth(),
        plan.planned_budget.selected_video_budget(),
        active_route_count,
        selected_video_bitrate,
    );
    for target in update_targets {
        let updated = state.topology.update_consumer_source_selection(
            &target.key,
            target.source_id,
            &target.route,
            |selection| selection.set_budget(budget),
        );
        debug_assert!(
            updated,
            "validated route should remain committed under the lock"
        );
    }
}

fn commit_featured_user_updates(
    state: &mut RoomState,
    updates: &[FeaturedUserUpdate],
) -> Option<MessageFanout> {
    let mut changed_user_ids = Vec::new();
    for update in updates {
        let Some(user) = state.user_mut_for_connection(&update.user_id, update.connection_id)
        else {
            continue;
        };
        if user.featured() == update.featured {
            continue;
        }
        user.set_featured(update.featured);
        changed_user_ids.push(update.user_id.clone());
    }
    if changed_user_ids.is_empty() {
        return None;
    }
    let snapshot = changed_user_ids
        .into_iter()
        .filter_map(|user_id| state.user_info_snapshot(&user_id))
        .collect();
    Some(state.fanout_all(RoomEventMessage::UserInfoChanged(snapshot)))
}
