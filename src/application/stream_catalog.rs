use std::collections::BTreeMap;

use o_sfu_protocol::wire::{DownloadStates, StreamType, UserInfo};
use o_sfu_router::MediaKind;

use crate::core::prelude::{
    ActiveSpeakerGroup, ActiveSpeakerPolicy, ActiveSpeakerSourceRole, SourceAdaptationPolicy,
    SourceDeactivateIntent, SourceLayoutPolicy, SourcePolicy, SourcePublishIntent,
    SourceRoomPolicySelector, SourceSubscriptionIntent, UserStreamId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DiscussStream {
    stream_type: StreamType,
    label: &'static str,
    media_kind: MediaKind,
    policy: SourcePolicy,
}

impl DiscussStream {
    /// Returns descriptors in audio, camera and screen order.
    pub(crate) fn all() -> [Self; 3] {
        [StreamType::Audio, StreamType::Camera, StreamType::Screen].map(Self::for_type)
    }

    pub(crate) const fn for_type(stream_type: StreamType) -> Self {
        match stream_type {
            StreamType::Audio => Self {
                stream_type,
                label: "audio",
                media_kind: MediaKind::Audio,
                policy: SourcePolicy::new(
                    None,
                    SourceAdaptationPolicy::None,
                    Some(ActiveSpeakerPolicy::new(
                        ActiveSpeakerGroup::MAIN,
                        ActiveSpeakerSourceRole::Detector,
                    )),
                ),
            },
            StreamType::Camera => Self {
                stream_type,
                label: "camera",
                media_kind: MediaKind::Video,
                policy: SourcePolicy::new(
                    Some(SourceLayoutPolicy::new(
                        SourceRoomPolicySelector::VisibleThumbnail,
                        Some(SourceRoomPolicySelector::ActiveSpeaker),
                    )),
                    SourceAdaptationPolicy::ScalableVideo,
                    Some(ActiveSpeakerPolicy::new(
                        ActiveSpeakerGroup::MAIN,
                        ActiveSpeakerSourceRole::Promotable,
                    )),
                ),
            },
            StreamType::Screen => Self {
                stream_type,
                label: "screen",
                media_kind: MediaKind::Video,
                policy: SourcePolicy::new(
                    Some(SourceLayoutPolicy::new(
                        SourceRoomPolicySelector::ReadableDetail,
                        None,
                    )),
                    SourceAdaptationPolicy::ReadableDetail,
                    None,
                ),
            },
        }
    }

    pub(crate) fn for_stream_id(stream_id: &UserStreamId) -> Option<Self> {
        Self::all()
            .into_iter()
            .find(|stream| stream.label == stream_id.as_str())
    }

    pub(crate) const fn label(self) -> &'static str {
        self.label
    }

    pub(crate) fn stream_id(self) -> UserStreamId {
        UserStreamId::new(self.label)
    }

    pub(crate) fn publish_intent(self) -> SourcePublishIntent {
        SourcePublishIntent::new(self.stream_id(), self.media_kind, self.policy)
            .with_presence(self.publication_presence(true))
    }

    pub(crate) fn deactivate_intent(self) -> SourceDeactivateIntent {
        SourceDeactivateIntent::new(self.stream_id())
            .with_presence(self.publication_presence(false))
    }

    pub(crate) fn subscription_intent_if_requested(
        self,
        states: &DownloadStates,
    ) -> Option<(UserStreamId, SourceSubscriptionIntent)> {
        let (active, layout) = match self.stream_type {
            StreamType::Audio => (states.audio, None),
            StreamType::Camera => (states.camera, states.camera_layout),
            StreamType::Screen => (states.screen, states.screen_layout),
        };
        let intent = SourceSubscriptionIntent::new(active, layout);
        (!intent.is_empty()).then(|| (self.stream_id(), intent))
    }

    fn publication_presence(self, active: bool) -> Option<UserInfo> {
        match self.stream_type {
            StreamType::Audio => None,
            StreamType::Camera => Some(UserInfo {
                is_camera_on: Some(active),
                ..UserInfo::default()
            }),
            StreamType::Screen => Some(UserInfo {
                is_screen_sharing_on: Some(active),
                ..UserInfo::default()
            }),
        }
    }
}

pub(crate) fn source_publish_intent_for_stream_type(
    stream_type: StreamType,
) -> SourcePublishIntent {
    DiscussStream::for_type(stream_type).publish_intent()
}

pub(crate) fn stream_id_for_stream_type(stream_type: StreamType) -> UserStreamId {
    DiscussStream::for_type(stream_type).stream_id()
}

pub(crate) fn stream_type_for_stream_id(stream_id: &UserStreamId) -> Option<StreamType> {
    DiscussStream::for_stream_id(stream_id).map(|stream| stream.stream_type)
}

pub(crate) fn counter_for_stream_type(
    by_stream: &BTreeMap<UserStreamId, u64>,
    stream_type: StreamType,
) -> u64 {
    by_stream
        .get(&stream_id_for_stream_type(stream_type))
        .copied()
        .unwrap_or(0)
}

#[cfg(test)]
#[path = "TESTS/stream_catalog.rs"]
mod tests;
