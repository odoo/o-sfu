use super::{DownloadStates, UserId, VideoLayoutIntent};

#[test]
fn user_id_normalization_keeps_numeric_runtime_identity_canonical() {
    assert_eq!(
        UserId::String("42".to_owned()).normalized_for_runtime(),
        UserId::Integer(42)
    );
    assert_eq!(
        UserId::Integer(42).normalized_for_runtime(),
        UserId::Integer(42)
    );
}

#[test]
fn user_id_normalization_preserves_arbitrary_string_ids() {
    assert_eq!(
        UserId::String("guest-42".to_owned()).normalized_for_runtime(),
        UserId::String("guest-42".to_owned())
    );
}

#[test]
fn user_id_path_segment_preserves_raw_representation() {
    assert_eq!(UserId::Integer(42).path_segment(), "42");
    assert_eq!(
        UserId::String("guest-42".to_owned()).path_segment(),
        "guest-42"
    );
    assert_eq!(UserId::String("007".to_owned()).path_segment(), "007");
}

#[test]
fn user_id_decode_bounds_utf8_bytes_without_normalizing_wire_values() -> serde_json::Result<()> {
    for value in [
        "a".repeat(256),
        "\u{e9}".repeat(128),
        format!("{}7", "0".repeat(255)),
    ] {
        let encoded = serde_json::to_string(&value)?;
        assert_eq!(
            serde_json::from_str::<UserId>(&encoded)?,
            UserId::String(value)
        );
    }
    assert_eq!(
        serde_json::from_str::<UserId>(r#""0007""#)?.normalized_for_runtime(),
        UserId::Integer(7)
    );
    assert_eq!(serde_json::from_str::<UserId>("7")?, UserId::Integer(7));
    Ok(())
}

#[test]
fn user_id_decode_rejects_oversized_strings_before_numeric_normalization() -> serde_json::Result<()>
{
    for value in [
        "a".repeat(257),
        "\u{e9}".repeat(129),
        format!("{}7", "0".repeat(256)),
    ] {
        let encoded = serde_json::to_string(&value)?;
        assert!(serde_json::from_str::<UserId>(&encoded).is_err());
    }
    Ok(())
}

#[test]
fn user_id_decode_counts_decoded_utf8_bytes_in_escaped_json() -> serde_json::Result<()> {
    let accepted = format!(r#""{}""#, r"\u00e9".repeat(128));
    let rejected = format!(r#""{}""#, r"\u00e9".repeat(129));
    assert_eq!(
        serde_json::from_str::<UserId>(&accepted)?,
        UserId::String("\u{e9}".repeat(128))
    );
    assert!(serde_json::from_str::<UserId>(&rejected).is_err());
    Ok(())
}

#[test]
fn video_layout_intents_keep_their_wire_values() -> serde_json::Result<()> {
    for (intent, wire_value) in [
        (VideoLayoutIntent::Automatic, "automatic"),
        (VideoLayoutIntent::Featured, "featured"),
        (VideoLayoutIntent::Pinned, "pinned"),
        (VideoLayoutIntent::VisibleThumbnail, "visible_thumbnail"),
        (VideoLayoutIntent::Hidden, "hidden"),
        (VideoLayoutIntent::Overflow, "overflow"),
    ] {
        assert_eq!(serde_json::to_value(intent)?, wire_value);
        assert_eq!(
            serde_json::from_value::<VideoLayoutIntent>(wire_value.into())?,
            intent
        );
    }
    Ok(())
}

#[test]
fn automatic_layout_replaces_only_its_layout_until_another_is_selected() {
    let mut states = DownloadStates {
        camera: Some(false),
        camera_layout: Some(VideoLayoutIntent::Pinned),
        screen_layout: Some(VideoLayoutIntent::Hidden),
        ..DownloadStates::default()
    };

    states.apply_partial_update(&DownloadStates {
        camera_layout: Some(VideoLayoutIntent::Automatic),
        ..DownloadStates::default()
    });
    states.apply_partial_update(&DownloadStates {
        audio: Some(true),
        ..DownloadStates::default()
    });
    assert_eq!(
        states,
        DownloadStates {
            audio: Some(true),
            camera: Some(false),
            camera_layout: Some(VideoLayoutIntent::Automatic),
            screen_layout: Some(VideoLayoutIntent::Hidden),
            ..DownloadStates::default()
        }
    );

    states.apply_partial_update(&DownloadStates {
        camera_layout: Some(VideoLayoutIntent::Featured),
        ..DownloadStates::default()
    });
    assert_eq!(states.camera_layout, Some(VideoLayoutIntent::Featured));
}
