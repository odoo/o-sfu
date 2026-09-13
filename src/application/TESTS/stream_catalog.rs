use o_sfu_protocol::wire::VideoLayoutIntent;

use super::*;

#[test]
fn maps_discuss_stream_labels_to_stable_core_ids() {
    assert_eq!(
        stream_id_for_stream_type(StreamType::Audio).as_str(),
        "audio"
    );
    assert_eq!(
        stream_id_for_stream_type(StreamType::Camera).as_str(),
        "camera"
    );
    assert_eq!(
        stream_id_for_stream_type(StreamType::Screen).as_str(),
        "screen"
    );
    assert_eq!(
        stream_type_for_stream_id(&UserStreamId::new("audio")),
        Some(StreamType::Audio)
    );
    assert_eq!(
        stream_type_for_stream_id(&UserStreamId::new("camera")),
        Some(StreamType::Camera)
    );
    assert_eq!(
        stream_type_for_stream_id(&UserStreamId::new("screen")),
        Some(StreamType::Screen)
    );
    assert_eq!(
        stream_type_for_stream_id(&UserStreamId::new("custom-source")),
        None
    );
}

#[test]
fn maps_discuss_streams_to_core_source_policy() {
    assert_audio_policy(&source_publish_intent_for_stream_type(StreamType::Audio));
    assert_camera_policy(&source_publish_intent_for_stream_type(StreamType::Camera));
    assert_screen_policy(&source_publish_intent_for_stream_type(StreamType::Screen));
    assert!(
        DiscussStream::for_type(StreamType::Audio)
            .deactivate_intent()
            .presence()
            .is_none()
    );
    assert!(matches!(
        DiscussStream::for_type(StreamType::Camera)
            .deactivate_intent()
            .presence(),
        Some(info) if info.is_camera_on == Some(false)
    ));
    assert!(matches!(
        DiscussStream::for_type(StreamType::Screen)
            .deactivate_intent()
            .presence(),
        Some(info) if info.is_screen_sharing_on == Some(false)
    ));
}

fn assert_audio_policy(intent: &SourcePublishIntent) {
    assert_eq!(intent.stream_id().as_str(), "audio");
    assert_eq!(intent.media_kind(), MediaKind::Audio);

    let policy = intent.policy();
    assert_eq!(policy.layout(), None);
    assert_eq!(policy.adaptation(), SourceAdaptationPolicy::None);
    assert_eq!(policy.video_bitrate_cap(), None);
    assert_eq!(
        policy.active_speaker(),
        Some(ActiveSpeakerPolicy::new(
            ActiveSpeakerGroup::MAIN,
            ActiveSpeakerSourceRole::Detector,
        ))
    );
}

fn assert_camera_policy(intent: &SourcePublishIntent) {
    assert_eq!(intent.stream_id().as_str(), "camera");
    assert_eq!(intent.media_kind(), MediaKind::Video);
    assert!(matches!(intent.presence(), Some(info) if info.is_camera_on == Some(true)));

    let policy = intent.policy();
    assert_eq!(policy.adaptation(), SourceAdaptationPolicy::ScalableVideo);
    assert_eq!(policy.video_bitrate_cap(), None);
    assert_eq!(
        policy.active_speaker(),
        Some(ActiveSpeakerPolicy::new(
            ActiveSpeakerGroup::MAIN,
            ActiveSpeakerSourceRole::Promotable,
        ))
    );

    assert_eq!(
        policy.layout().map(|layout| layout.resolve(None, false)),
        Some(SourceRoomPolicySelector::VisibleThumbnail)
    );
    assert_eq!(
        policy.layout().map(|layout| layout.resolve(None, true)),
        Some(SourceRoomPolicySelector::ActiveSpeaker)
    );
    assert_eq!(
        policy
            .layout()
            .map(|layout| layout.resolve(Some(VideoLayoutIntent::Pinned), true)),
        Some(SourceRoomPolicySelector::Pinned)
    );
}

fn assert_screen_policy(intent: &SourcePublishIntent) {
    assert_eq!(intent.stream_id().as_str(), "screen");
    assert_eq!(intent.media_kind(), MediaKind::Video);
    assert!(matches!(intent.presence(), Some(info) if info.is_screen_sharing_on == Some(true)));

    let policy = intent.policy();
    assert_eq!(policy.adaptation(), SourceAdaptationPolicy::ReadableDetail);
    assert_eq!(policy.active_speaker(), None);
    assert_eq!(policy.video_bitrate_cap(), None);

    assert_eq!(
        policy.layout().map(|layout| layout.resolve(None, false)),
        Some(SourceRoomPolicySelector::ReadableDetail)
    );
    assert_eq!(
        policy.layout().map(|layout| layout.resolve(None, true)),
        Some(SourceRoomPolicySelector::ReadableDetail)
    );
}
