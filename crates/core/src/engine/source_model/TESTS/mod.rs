#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::unwrap_used,
    reason = "test assertions use expect, unwrap and direct indexing for direct fixture failures"
)]

use o_sfu_model::UserId;
use o_sfu_router::{
    MediaKind,
    rtp::{MediaCodec, MediaFormat, PayloadType, Rid, Ssrc},
};

use super::*;
use crate::Bitrate;

fn video_format(payload_type: u8) -> MediaFormat {
    MediaFormat::new(
        MediaKind::Video,
        MediaCodec::from("VP8"),
        PayloadType::new(payload_type),
        90_000,
    )
}

fn source_encoding(
    source_id: PublishedSourceId,
    encoding_id: SourceEncodingId,
    rid: &str,
) -> SourceEncodingDescriptor {
    source_encoding_with_options(
        source_id,
        encoding_id,
        Some(rid),
        Some(Bitrate::from_kbps(150 * encoding_id.as_u64())),
    )
}

fn source_encoding_with_options(
    source_id: PublishedSourceId,
    encoding_id: SourceEncodingId,
    rid: Option<&str>,
    max_bitrate: Option<Bitrate>,
) -> SourceEncodingDescriptor {
    source_encoding_with_policy_role(source_id, encoding_id, rid, max_bitrate, None)
}

fn source_encoding_with_policy_role(
    source_id: PublishedSourceId,
    encoding_id: SourceEncodingId,
    rid: Option<&str>,
    max_bitrate: Option<Bitrate>,
    policy_role: Option<UploadLayerPolicyRole>,
) -> SourceEncodingDescriptor {
    let raw_encoding_id =
        u32::try_from(encoding_id.as_u64()).expect("test encoding id should fit in u32");
    SourceEncodingDescriptor::new(SourceEncodingDescriptorParts {
        encoding_id,
        source_id,
        rid: rid.map(Rid::new),
        primary_ssrc: Some(Ssrc::new(100 + raw_encoding_id)),
        repair_ssrc: Some(Ssrc::new(200 + raw_encoding_id)),
        max_bitrate,
        resolution_scale: None,
        max_framerate: None,
        policy_role,
        negotiated_format: Some(video_format(96)),
    })
}

fn try_source_descriptor(
    source_id: PublishedSourceId,
    encodings: Vec<SourceEncodingDescriptor>,
) -> Result<PublishedSourceDescriptor, SourceModelError> {
    PublishedSourceDescriptor::new(PublishedSourceDescriptorParts {
        source_id,
        owner: PublishedSourceOwner::new(UserId::Integer(42)),
        stream_id: UserStreamId::new("main-video"),
        media_kind: MediaKind::Video,
        policy: SourcePolicy::hidden(),
        mid: None,
        encodings,
    })
}

fn valid_source_descriptor(
    source_id: PublishedSourceId,
    encodings: Vec<SourceEncodingDescriptor>,
) -> PublishedSourceDescriptor {
    try_source_descriptor(source_id, encodings).expect("source descriptor should be valid")
}

fn selectable_encoding_ids(descriptor: &PublishedSourceDescriptor) -> Vec<SourceEncodingId> {
    descriptor
        .selectable_encodings()
        .map(SourceEncodingDescriptor::encoding_id)
        .collect()
}

#[test]
fn consumer_selection_delivery_requires_active_intent_and_policy_admission() {
    let mut selection = ConsumerSourceSelection::open(true);
    assert!(selection.delivery_active());

    selection.set_policy_pause_reason(Some(PolicyPauseReason::VideoDownloadLimit));
    assert!(!selection.delivery_active());

    selection.set_policy_pause_reason(None);
    selection.set_active(false);
    assert!(!selection.delivery_active());
}

#[test]
fn descriptor_orders_selectable_encodings_by_bitrate() {
    let source_id = PublishedSourceId::from_raw(7);
    let low_encoding_id = SourceEncodingId::from_raw(1);
    let middle_encoding_id = SourceEncodingId::from_raw(2);
    let high_encoding_id = SourceEncodingId::from_raw(3);
    let descriptor = valid_source_descriptor(
        source_id,
        vec![
            source_encoding_with_options(
                source_id,
                high_encoding_id,
                Some("hi"),
                Some(Bitrate::from_kbps(900)),
            ),
            source_encoding_with_options(
                source_id,
                low_encoding_id,
                Some("lo"),
                Some(Bitrate::from_kbps(150)),
            ),
            source_encoding_with_options(
                source_id,
                middle_encoding_id,
                Some("mid"),
                Some(Bitrate::from_kbps(450)),
            ),
        ],
    );

    assert_eq!(
        selectable_encoding_ids(&descriptor),
        vec![low_encoding_id, middle_encoding_id, high_encoding_id]
    );
    assert_eq!(
        descriptor
            .selectable_encoding_by_rank(1)
            .map(SourceEncodingDescriptor::encoding_id),
        Some(middle_encoding_id)
    );
}

#[test]
fn descriptor_keeps_declared_selectable_order_without_bitrates() {
    let source_id = PublishedSourceId::from_raw(7);
    let first_encoding_id = SourceEncodingId::from_raw(1);
    let second_encoding_id = SourceEncodingId::from_raw(2);
    let third_encoding_id = SourceEncodingId::from_raw(3);
    let descriptor = valid_source_descriptor(
        source_id,
        vec![
            source_encoding_with_options(source_id, first_encoding_id, Some("a"), None),
            source_encoding_with_options(source_id, second_encoding_id, Some("b"), None),
            source_encoding_with_options(source_id, third_encoding_id, Some("c"), None),
        ],
    );

    assert_eq!(
        selectable_encoding_ids(&descriptor),
        vec![first_encoding_id, second_encoding_id, third_encoding_id]
    );
}

#[test]
fn descriptor_orders_selectable_encodings_by_policy_role_without_bitrates() {
    let source_id = PublishedSourceId::from_raw(7);
    let high_encoding_id = SourceEncodingId::from_raw(1);
    let low_encoding_id = SourceEncodingId::from_raw(2);
    let descriptor = valid_source_descriptor(
        source_id,
        vec![
            source_encoding_with_policy_role(
                source_id,
                high_encoding_id,
                Some("hi"),
                None,
                Some(UploadLayerPolicyRole::Featured),
            ),
            source_encoding_with_policy_role(
                source_id,
                low_encoding_id,
                Some("lo"),
                None,
                Some(UploadLayerPolicyRole::Thumbnail),
            ),
        ],
    );

    assert_eq!(
        selectable_encoding_ids(&descriptor),
        vec![low_encoding_id, high_encoding_id]
    );
}

#[test]
fn descriptor_excludes_selectable_encodings_when_rid_is_missing() {
    let source_id = PublishedSourceId::from_raw(7);
    let descriptor = valid_source_descriptor(
        source_id,
        vec![
            source_encoding_with_options(
                source_id,
                SourceEncodingId::from_raw(1),
                Some("lo"),
                Some(Bitrate::from_kbps(150)),
            ),
            source_encoding_with_options(
                source_id,
                SourceEncodingId::from_raw(2),
                None,
                Some(Bitrate::from_kbps(450)),
            ),
        ],
    );

    assert_eq!(descriptor.selectable_encoding_count(), 0);
    assert!(descriptor.selectable_encoding_by_rank(0).is_none());
}

#[test]
fn descriptor_rejects_encoding_from_another_source() {
    let source_id = PublishedSourceId::from_raw(7);
    let other_source_id = PublishedSourceId::from_raw(8);
    let encoding_id = SourceEncodingId::from_raw(1);

    assert_eq!(
        try_source_descriptor(
            source_id,
            vec![source_encoding(other_source_id, encoding_id, "lo")],
        )
        .unwrap_err(),
        SourceModelError::EncodingSourceMismatch {
            source_id,
            encoding_id,
            encoding_source_id: other_source_id,
        }
    );
}

#[test]
fn descriptor_rejects_duplicate_encoding_ids() {
    let source_id = PublishedSourceId::from_raw(7);
    let encoding_id = SourceEncodingId::from_raw(1);

    assert_eq!(
        try_source_descriptor(
            source_id,
            vec![
                source_encoding(source_id, encoding_id, "lo"),
                source_encoding(source_id, SourceEncodingId::from_raw(2), "mid"),
                source_encoding(source_id, encoding_id, "hi"),
            ],
        )
        .unwrap_err(),
        SourceModelError::DuplicateEncodingId {
            source_id,
            encoding_id,
        }
    );
}

#[test]
fn id_allocation_is_monotonic_and_topology_neutral() {
    let mut next_source_id = 1;
    let mut next_encoding_id = 1;

    assert_eq!(PublishedSourceId::allocate(&mut next_source_id).as_u64(), 1);
    assert_eq!(PublishedSourceId::allocate(&mut next_source_id).as_u64(), 2);
    assert_eq!(
        SourceEncodingId::allocate(&mut next_encoding_id).as_u64(),
        1
    );
    assert_eq!(
        SourceEncodingId::allocate(&mut next_encoding_id).as_u64(),
        2
    );
}
