use crate::engine::room::{
    UserOutbound,
    outbound::{MessageFanout, OutboundSender, VersionedRemoteTrackSnapshot},
    state::LifecycleEffects,
};

#[derive(Debug, Default)]
pub(super) struct RoomOutputPlan {
    pub(super) track_snapshots: Vec<(OutboundSender, VersionedRemoteTrackSnapshot)>,
    pub(super) user_info_before_policy: Option<MessageFanout>,
    pub(super) user_info: Option<MessageFanout>,
    pub(super) lifecycle: LifecycleEffects,
}

impl RoomOutputPlan {
    pub(super) fn emit_before_policy(&mut self) {
        for (recipient, snapshot) in self.track_snapshots.drain(..) {
            let _ = recipient.send_remote_tracks(snapshot);
        }
        self.emit_user_info_before_policy();
    }

    pub(super) fn emit_user_info_before_policy(&mut self) {
        if let Some(fanout) = self.user_info_before_policy.take() {
            fanout.emit();
        }
    }

    pub(super) fn emit_after_policy(self) {
        if let Some(fanout) = self.user_info {
            fanout.emit();
        }
        for close_request in self.lifecycle.close_requests {
            let _ = close_request
                .sender
                .send(UserOutbound::Close(close_request.reason));
        }
        for (recipient, snapshot) in self.lifecycle.track_snapshots {
            let _ = recipient.send_remote_tracks(snapshot);
        }
        for fanout in self.lifecycle.fanouts {
            fanout.emit();
        }
    }
}
