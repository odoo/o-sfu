use std::collections::BTreeMap;

use o_sfu_router::MediaKind;

use super::{
    ActiveSpeakerGroup, ActiveSpeakerPolicy, ActiveSpeakerSourceRole, SourceAdaptationPolicy,
    SourceLayoutPolicy, SourcePolicy, SourcePublishIntent, SourceRoomPolicySelector,
    SourceSubscriptionIntent, UserStreamId,
};
use crate::engine::VideoLayoutIntent;

const AUDIO_DETECTOR_SOURCE_ID: &str = "test-audio-detector";
const SCALABLE_VIDEO_SOURCE_ID: &str = "test-scalable-video";
const READABLE_VIDEO_SOURCE_ID: &str = "test-readable-video";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TestSourceSpec {
    kind: TestSourceKind,
    stream_id: &'static str,
    media_kind: MediaKind,
    policy: SourcePolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TestSourceKind {
    AudioDetector,
    ScalableVideo,
    ReadableVideo,
}

const TEST_SOURCE_SPECS: [TestSourceSpec; 3] = [
    TestSourceSpec {
        kind: TestSourceKind::AudioDetector,
        stream_id: AUDIO_DETECTOR_SOURCE_ID,
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
    TestSourceSpec {
        kind: TestSourceKind::ScalableVideo,
        stream_id: SCALABLE_VIDEO_SOURCE_ID,
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
    TestSourceSpec {
        kind: TestSourceKind::ReadableVideo,
        stream_id: READABLE_VIDEO_SOURCE_ID,
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
];

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TestSubscriptionStates {
    pub audio_detector: Option<bool>,
    pub scalable_video: Option<bool>,
    pub readable_video: Option<bool>,
    pub scalable_video_layout: Option<VideoLayoutIntent>,
    pub readable_video_layout: Option<VideoLayoutIntent>,
}

#[must_use]
pub fn source_publish_intent_for_source(source: TestSourceKind) -> SourcePublishIntent {
    let spec = spec_for_source(source);
    SourcePublishIntent::new(
        UserStreamId::new(spec.stream_id),
        spec.media_kind,
        spec.policy,
    )
}

#[must_use]
pub fn stream_id_for_source(source: TestSourceKind) -> UserStreamId {
    UserStreamId::new(spec_for_source(source).stream_id)
}

#[must_use]
pub fn source_kind_for_stream_id(stream_id: &UserStreamId) -> Option<TestSourceKind> {
    TEST_SOURCE_SPECS
        .iter()
        .find(|spec| spec.stream_id == stream_id.as_str())
        .map(|spec| spec.kind)
}

#[must_use]
pub fn subscription_intents_from_test_states(
    states: &TestSubscriptionStates,
) -> BTreeMap<UserStreamId, SourceSubscriptionIntent> {
    let mut intents = BTreeMap::new();
    if states.audio_detector.is_some() {
        intents.insert(
            stream_id_for_source(TestSourceKind::AudioDetector),
            SourceSubscriptionIntent::new(states.audio_detector, None),
        );
    }
    if states.scalable_video.is_some() || states.scalable_video_layout.is_some() {
        intents.insert(
            stream_id_for_source(TestSourceKind::ScalableVideo),
            SourceSubscriptionIntent::new(states.scalable_video, states.scalable_video_layout),
        );
    }
    if states.readable_video.is_some() || states.readable_video_layout.is_some() {
        intents.insert(
            stream_id_for_source(TestSourceKind::ReadableVideo),
            SourceSubscriptionIntent::new(states.readable_video, states.readable_video_layout),
        );
    }
    intents
}

fn spec_for_source(source: TestSourceKind) -> TestSourceSpec {
    let [audio_detector, scalable_video, readable_video] = TEST_SOURCE_SPECS;
    match source {
        TestSourceKind::AudioDetector => audio_detector,
        TestSourceKind::ScalableVideo => scalable_video,
        TestSourceKind::ReadableVideo => readable_video,
    }
}
