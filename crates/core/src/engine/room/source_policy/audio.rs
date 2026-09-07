use o_sfu_router::MediaKind;

use super::{
    action::ConsumerPacketSelectionUpdate, input::SourcePolicySnapshot,
    turn::SourcePolicyTransaction,
};
use crate::engine::{room::media_graph::ConsumerRouteView, source_model::PolicyPauseReason};

const AUDIO_PAUSE_REASONS: [PolicyPauseReason; 2] = [
    PolicyPauseReason::AudioSpeakerLimit,
    PolicyPauseReason::ReceiverDeafened,
];

/// Evaluates and stages audio forwarding activity for active room audio routes.
///
/// The policy computes one room-wide admitted active-speaker set and applies it
/// to every considered receiver route.
///
/// # Policy Invariants
///
/// 1. **Deafness Dominance**: If a receiver is deafened, each considered audio route is paused
///    with [`PolicyPauseReason::ReceiverDeafened`]. When undeafened, routes are not blindly
///    resumed; they are re-evaluated against the current active speaker quota.
/// 2. **Low-Latency Voice Onset**: For a receiver that is not deafened, sources outside the
///    room-wide active-source set remain unpaused by audio route policy. Worker-local VAD gating
///    is separate and opens before destination planning on a VAD-true packet, so that packet does
///    not wait for a policy turn.
/// 3. **Active Speaker Admission**: When the room has more active speakers than
///    `max_active_audio_speakers`, screen-sharers are admitted first. Sources otherwise retain
///    room ranking by newest observation, audio level and media ID. Sources beyond the limit are
///    paused with [`PolicyPauseReason::AudioSpeakerLimit`].
/// 4. **Pause Reason Ownership**: Audio policy sets only reasons in [`AUDIO_PAUSE_REASONS`].
///    It clears a pause only when the current reason belongs to that set.
///
/// ```text
///                     Incoming Audio Route (Receiver, Source)
///                                         |
///                                         v
///                         +-------------------------------+
///                         |   Is Receiver Deafened?       | -- yes --> [ Pause: ReceiverDeafened ]
///                         +-------------------------------+
///                                         | no
///                                         v
///                         +-------------------------------+
///                         |   Is Source in Room Active    | -- no  --> [ No Audio-Owned Pause ]
///                         |   Set?                        |             (VAD gate is separate)
///                         +-------------------------------+
///                                         | yes
///                                         v
///                         +-------------------------------+
///                         |   Within Active Speaker Cap?  | -- yes --> [ No Audio-Owned Pause ]
///                         |  (Screen-sharers prioritized) |
///                         +-------------------------------+
///                                         | no
///                                         v
///                             [ Pause: AudioSpeakerLimit ]
/// ```
pub(super) fn append_audio_route_activity(
    tx: &mut SourcePolicyTransaction,
    input: &SourcePolicySnapshot<'_>,
) {
    for route in &input.routes {
        if route.source.descriptor.media_kind() != MediaKind::Audio {
            continue;
        }
        let next_reason = audio_pause_reason(input, route);
        // Clear only pause reasons owned by audio policy. A refresh must not
        // resume a route still withheld by another policy.
        if next_reason.is_none() && !owns_pause_reason(route.selection.policy_pause_reason()) {
            continue;
        }
        if let Some(update) = ConsumerPacketSelectionUpdate::route_activity(
            route.key.clone(),
            route.source.descriptor.source_id(),
            route.route.clone(),
            route.selection,
            next_reason,
        ) {
            tx.push_route_update(update);
        }
    }
}

fn audio_pause_reason(
    input: &SourcePolicySnapshot<'_>,
    route: &ConsumerRouteView<'_>,
) -> Option<PolicyPauseReason> {
    // Deafness dominates speaker admission. Undeafening recomputes the cap
    // instead of blindly resuming every audio route.
    if input
        .deaf_receiver_connection_ids
        .contains(&route.route.consumer_session_key().connection_id())
    {
        return Some(PolicyPauseReason::ReceiverDeafened);
    }
    input
        .limited_audio_media_ids
        .contains(&route.route.source_transport_media_id())
        .then_some(PolicyPauseReason::AudioSpeakerLimit)
}

fn owns_pause_reason(current_reason: Option<PolicyPauseReason>) -> bool {
    current_reason.is_some_and(|reason| AUDIO_PAUSE_REASONS.contains(&reason))
}
