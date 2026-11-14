use o_sfu_router::MediaKind;

use super::{
    super::action::{ConsumerPacketSelectionUpdate, UpgradeChange, VideoRouteTransition},
    solver::{PlannedReceiverRoute, selector_bitrate},
};
use crate::engine::{
    media_transport::SourcePacketGate,
    source_model::{PublishedSourceDescriptor, ReceiverVideoBudgetDiagnostics, SourceSelector},
};

/// Projects a room selector into a source-worker packet gate.
///
/// Returns `None` when the selected encoding is unknown or has no negotiated RID.
pub(super) fn source_packet_gate_for_selector(
    source: &PublishedSourceDescriptor,
    selector: SourceSelector,
) -> Option<SourcePacketGate> {
    match selector {
        SourceSelector::Open => Some(SourcePacketGate::Open),
        SourceSelector::Encoding(encoding_id) => {
            // Never fall back to `Open`: it would forward every encoding while
            // room state records one selected encoding.
            let encoding = source.encoding(encoding_id)?;
            let rid = encoding.rid()?;
            Some(SourcePacketGate::Rid(rid.as_str().to_owned()))
        }
    }
}

pub(super) fn consumer_packet_selection_update(
    planned_route: &PlannedReceiverRoute<'_>,
    planned_budget: ReceiverVideoBudgetDiagnostics,
) -> Option<ConsumerPacketSelectionUpdate> {
    let input = planned_route.input;
    let selection = planned_route.selection;
    let current_selection = input.current_selection;
    if selection.selector == current_selection.selector()
        && selection.policy_pause_reason == current_selection.policy_pause_reason()
        && planned_budget == current_selection.budget()
        && selection.pending_upgrade.as_ref() == input.pending_upgrade
    {
        return None;
    }
    let packet_gate = if selection.selector == current_selection.selector() {
        None
    } else {
        Some(source_packet_gate_for_selector(
            input.source,
            selection.selector,
        )?)
    };
    let route_activity_changed =
        selection.policy_pause_reason != current_selection.policy_pause_reason();
    // A newly selected RID or resumed route may not share the receiver's last
    // decodable reference chain, so request a keyframe for either transition.
    let request_keyframe = selection.policy_pause_reason.is_none()
        && (selection.request_keyframe
            || selection.selector != current_selection.selector()
            || !current_selection.policy_allows_delivery());
    let transition = route_transition(planned_route);
    let selected_estimated_bitrate =
        transition.and_then(|_| selector_bitrate(input, selection.selector));
    Some(ConsumerPacketSelectionUpdate {
        key: input.key.clone(),
        source_id: input.source.source_id(),
        route: input.route.clone(),
        selector: selection.selector,
        policy_pause_reason: selection.policy_pause_reason,
        planned_budget,
        transition,
        selected_estimated_bitrate,
        upgrade: if selection.pending_upgrade.as_ref() == input.pending_upgrade {
            UpgradeChange::Unchanged
        } else {
            UpgradeChange::Set(selection.pending_upgrade)
        },
        interrupts_upgrade: selection.interrupts_upgrade(input.pending_upgrade),
        packet_gate,
        route_activity_changed,
        request_keyframe: request_keyframe && input.source.media_kind() == MediaKind::Video,
    })
}

fn route_transition(route: &PlannedReceiverRoute<'_>) -> Option<VideoRouteTransition> {
    let selection = route.selection;
    match (
        selection.policy_pause_reason,
        route.input.current_selection.policy_pause_reason(),
    ) {
        (None, Some(cleared_reason)) => Some(VideoRouteTransition::Resumed { cleared_reason }),
        (Some(reason), None) => Some(VideoRouteTransition::Paused { reason }),
        (Some(_), Some(_)) => None,
        (None, None) if selection.selector == route.input.current_selection.selector() => None,
        (None, None) => {
            let current_selector @ SourceSelector::Encoding(_) =
                route.input.current_selection.selector()
            else {
                return None;
            };
            let selected_selector @ SourceSelector::Encoding(_) = selection.selector else {
                return None;
            };
            let current_bitrate = selector_bitrate(route.input, current_selector)?;
            let selected_bitrate = selector_bitrate(route.input, selected_selector)?;
            (selected_bitrate < current_bitrate).then_some(VideoRouteTransition::Degraded)
        }
    }
}
