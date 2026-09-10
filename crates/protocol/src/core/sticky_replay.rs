use std::collections::BTreeMap;

use crate::{
    shared::{DownloadStates, StreamType, UserId, UserInfo},
    signaling::{ClientEnvelope, ClientMessage, EnvelopeBatch, SubscribePayload},
};

/// Retains client intent across recoverable WebSocket replacement.
///
/// Authentication replays subscriptions and user info. Transport readiness
/// replays active publications.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct StickyReplayState {
    audio_publication_active: bool,
    camera_publication_active: bool,
    screen_publication_active: bool,
    desired_subscriptions: BTreeMap<UserId, DownloadStates>,
    desired_info: Option<UserInfo>,
}

impl StickyReplayState {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn clear(&mut self) {
        self.audio_publication_active = false;
        self.camera_publication_active = false;
        self.screen_publication_active = false;
        self.desired_subscriptions.clear();
        self.desired_info = None;
    }

    pub(super) fn set_publish_active(&mut self, stream_type: StreamType, active: bool) {
        match stream_type {
            StreamType::Audio => self.audio_publication_active = active,
            StreamType::Camera => self.camera_publication_active = active,
            StreamType::Screen => self.screen_publication_active = active,
        }
    }

    pub(super) fn active_publications(&self) -> impl Iterator<Item = StreamType> + '_ {
        [
            (StreamType::Audio, self.audio_publication_active),
            (StreamType::Camera, self.camera_publication_active),
            (StreamType::Screen, self.screen_publication_active),
        ]
        .into_iter()
        .filter_map(|(stream_type, active)| active.then_some(stream_type))
    }

    pub(super) fn remember_subscription_states(
        &mut self,
        user_id: &UserId,
        states: &DownloadStates,
    ) {
        let existing_states = self
            .desired_subscriptions
            .entry(user_id.clone())
            .or_default();
        existing_states.apply_partial_update(states);
        if *existing_states == DownloadStates::default() {
            self.desired_subscriptions.remove(user_id);
        }
    }

    pub(super) fn remember_info(&mut self, info: &UserInfo) {
        let existing_info = self.desired_info.get_or_insert_with(UserInfo::default);
        let is_featured = existing_info.is_featured;
        existing_info.apply_partial_update(info);
        existing_info.is_featured = is_featured;
    }

    pub(super) fn replay_session_batch(&self) -> Option<EnvelopeBatch> {
        let mut replay_batch: EnvelopeBatch = self
            .desired_subscriptions
            .iter()
            .filter_map(|(user_id, states)| {
                ClientEnvelope::Message(ClientMessage::Subscribe(SubscribePayload {
                    user_id: user_id.clone(),
                    states: states.clone(),
                }))
                .into_envelope()
                .ok()
            })
            .collect();
        if let Some(info) = self.desired_info.clone() {
            let envelope = ClientEnvelope::Message(ClientMessage::Info(info))
                .into_envelope()
                .ok()?;
            replay_batch.push(envelope);
        }
        if replay_batch.is_empty() {
            None
        } else {
            Some(replay_batch)
        }
    }
}
