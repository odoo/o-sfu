#![allow(
    clippy::panic,
    reason = "local send rewrite tests use panic only for mandatory fixture setup failures"
)]

use std::net::SocketAddr;

use o_sfu_rfc::rtp::{CodecName, RTP_SEQUENCE_NUMBER_MODULUS};
use o_sfu_router::{
    MediaKind,
    rtp::{MediaFormat, MediaStream, PayloadType},
};
use str0m::{
    Rtc,
    media::{MediaKind as Str0mMediaKind, Pt},
};

use super::*;
use crate::{
    Bitrate,
    engine::{
        UserId,
        media_transport::{
            TransportMediaId,
            rtc::{
                bootstrap,
                consumer_egress::LocalPacketDestination,
                state::PacketLoopState,
                test_support::{sample_forwarded_packet, test_transport_session_key},
            },
        },
    },
};

const VP8_PAYLOAD_TYPE: u8 = 96;

fn projected(
    streams: &mut ConsumerStreamStore,
    stream_handle: ConsumerStreamHandle,
    source_ssrc: Ssrc,
    source_timestamp: u32,
    codec_packet: codec::Packet,
) -> ProjectedIdentity {
    let source_seq_no = streams
        .streams
        .get(stream_handle)
        .and_then(|stream| stream.rtp.next_source_seq(source_ssrc))
        .unwrap_or_default();
    projected_packet(
        streams,
        stream_handle,
        0,
        source_ssrc,
        source_seq_no,
        source_timestamp,
        codec_packet,
    )
}

fn projected_packet(
    streams: &mut ConsumerStreamStore,
    stream_handle: ConsumerStreamHandle,
    delivery_generation: u64,
    source_ssrc: Ssrc,
    source_seq_no: SeqNo,
    source_timestamp: u32,
    codec_packet: codec::Packet,
) -> ProjectedIdentity {
    let Some(identity) = streams.project_identity(
        stream_handle,
        source_identity(
            delivery_generation,
            source_ssrc,
            source_seq_no,
            source_timestamp,
            false,
        ),
        codec_packet.identity(),
    ) else {
        panic!("consumer stream handle should be live");
    };
    identity
}

const fn source_identity(
    delivery_generation: u64,
    ssrc: Ssrc,
    seq_no: SeqNo,
    timestamp: u32,
    was_repair: bool,
) -> SourceRtpIdentity {
    SourceRtpIdentity {
        delivery_generation,
        ssrc,
        seq_no,
        timestamp,
        was_repair,
    }
}

fn allocate_at(streams: &mut ConsumerStreamStore, next_seq_no: u64) -> ConsumerStreamHandle {
    streams.streams.insert(ConsumerStream {
        rtp: RtpProjection::new(next_seq_no.into()),
        ..ConsumerStream::default()
    })
}

fn vp8_inspector() -> codec::PacketInspector {
    codec::PacketInspector::from_parameters(&MediaStream::new(
        vec![MediaFormat::new(
            MediaKind::Video,
            CodecName::Vp8,
            PayloadType::new(VP8_PAYLOAD_TYPE),
            90_000,
        )],
        vec![],
        vec![],
    ))
}

fn vp8_packet(
    inspector: &codec::PacketInspector,
    picture_id: u16,
    tl0_pic_idx: u8,
) -> codec::Packet {
    let [picture_id_high, picture_id_low] = (picture_id & 0x7fff).to_be_bytes();
    inspector.inspect(
        Pt::from(VP8_PAYLOAD_TYPE),
        &[
            0x90,
            0xe0,
            0x80 | picture_id_high,
            picture_id_low,
            tl0_pic_idx,
            0,
            0,
        ],
        true,
    )
}

fn repair_rtc(mid: Mid, primary: Ssrc, repair: Ssrc) -> Rtc {
    let mut rtc = Rtc::builder().set_rtp_mode(true).build(Instant::now());
    {
        let mut api = rtc.direct_api();
        api.declare_media(mid, Str0mMediaKind::Video);
        api.declare_stream_tx(primary, Some(repair), mid, None);
    }
    rtc
}

#[test]
fn missing_transmit_stream_does_not_mutate_consumer_state() {
    let consumer = test_transport_session_key(91, 0, 92, UserId::Integer(93));
    let source = test_transport_session_key(91, 0, 94, UserId::Integer(95));
    let mid = Mid::from("cam-down");
    let primary = Ssrc::from(96);
    let repair = Ssrc::from(97);
    let payload = b"payload";
    let mut state = PacketLoopState::default();
    assert!(
        bootstrap::test_support::ensure_session_rtc_state(
            &mut state.users,
            &consumer,
            SocketAddr::from(([127, 0, 0, 1], 47_200)),
            Bitrate::from_mbps(10),
        )
        .is_ok()
    );
    let Some(session) = state.users.get_mut(&consumer) else {
        panic!("consumer session should exist after RTC state bootstrap");
    };
    let stream_handle = session.consumer_streams.allocate(mid);
    let destination =
        LocalPacketDestination::new(TransportMediaId::new(98), stream_handle, 0, mid, None, true);
    let packet = sample_forwarded_packet(source, "cam-up", payload);
    let rtp = packet.local_send_packet();
    let Some(stream) = session.consumer_streams.streams.get(stream_handle) else {
        panic!("consumer stream handle should be live");
    };
    let initial_sequence = stream.rtp.next_seq_no;

    assert_eq!(session.send_consumer_packet(&destination, &rtp, None), None);
    let Some(stream) = session.consumer_streams.streams.get(stream_handle) else {
        panic!("consumer stream handle should remain live");
    };
    assert_eq!(stream.rtp.next_seq_no, initial_sequence);
    assert!(matches!(stream.rtp.timeline, RtpTimeline::Empty));
    assert_eq!(stream.primary_ssrc, None);
    assert_eq!(stream.queued_primary_writes, 0);
    assert_eq!(stream.stale_primary_writes, 0);
    assert_eq!(stream.rtx_cache_deadline, None);
    assert!(
        session
            .consumer_streams
            .repair_streams_by_primary
            .is_empty()
    );
    assert!(session.consumer_streams.retired_repair_streams.is_empty());

    {
        let mut api = session.rtc.direct_api();
        api.declare_media(mid, Str0mMediaKind::Video);
        api.declare_stream_tx(primary, Some(repair), mid, None);
    }
    assert_eq!(
        session.send_consumer_packet(&destination, &rtp, None),
        Some(payload.len())
    );

    let Some(stream) = session.consumer_streams.streams.get(stream_handle) else {
        panic!("consumer stream handle should remain live");
    };
    let RtpTimeline::Active {
        src_seq_anchor,
        dst_seq_anchor,
        highest_src_seq,
        ..
    } = stream.rtp.timeline
    else {
        panic!("first accepted packet should initialize consumer RTP identity");
    };
    assert_eq!(src_seq_anchor, 1_u64.into());
    assert_eq!(dst_seq_anchor, initial_sequence);
    assert_eq!(highest_src_seq, 1_u64.into());
    assert_eq!(stream.primary_ssrc, Some(primary));
    assert_eq!(stream.queued_primary_writes, 1);
    assert_eq!(stream.stale_primary_writes, 0);
    assert_eq!(stream.rtx_cache_deadline, None);
    assert_eq!(
        session
            .consumer_streams
            .repair_streams_by_primary
            .get(&primary),
        Some(&stream_handle)
    );
    assert!(session.consumer_streams.retired_repair_streams.is_empty());
}

#[test]
fn stalled_repair_stream_expires_once_and_rearms_on_send_progress() {
    let now = Instant::now();
    let mid = Mid::from("cam-down");
    let primary = Ssrc::from(301);
    let repair = Ssrc::from(302);
    let mut rtc = repair_rtc(mid, primary, repair);
    let mut streams = ConsumerStreamStore::default();
    let handle = streams.allocate(mid);
    streams.queue_repairable_write(handle, primary);
    streams.note_repairable_transmit(primary, now);

    streams.expire_rtx_streams(
        &mut rtc,
        now + RTX_CACHE_LIFETIME.saturating_sub(Duration::from_nanos(1)),
    );
    assert!(streams.rtx_cache_is_armed(handle));

    streams.expire_rtx_streams(&mut rtc, now + RTX_CACHE_LIFETIME);
    assert!(
        streams
            .streams
            .get(handle)
            .is_some_and(|stream| stream.rtx_cache_deadline.is_none())
    );
    streams.expire_rtx_streams(&mut rtc, now + RTX_CACHE_LIFETIME * 2);
    {
        let mut api = rtc.direct_api();
        let Some(stream) = api.stream_tx_by_mid(mid, None) else {
            panic!("cache rotation should preserve the transmit stream");
        };
        assert_eq!(stream.ssrc(), primary);
        assert_eq!(stream.rtx(), Some(repair));
    }

    streams.queue_repairable_write(handle, primary);
    streams.note_repairable_transmit(primary, now + RTX_CACHE_LIFETIME * 2);
    assert!(streams.rtx_cache_is_armed(handle));
}

#[test]
fn stale_earliest_repair_deadline_recomputes_at_its_boundary() {
    let now = Instant::now();
    let first_mid = Mid::from("cam-a");
    let second_mid = Mid::from("cam-b");
    let first_primary = Ssrc::from(311);
    let mut rtc = repair_rtc(first_mid, first_primary, Ssrc::from(312));
    rtc.direct_api()
        .declare_media(second_mid, Str0mMediaKind::Video);
    rtc.direct_api()
        .declare_stream_tx(Ssrc::from(313), Some(Ssrc::from(314)), second_mid, None);
    let mut streams = ConsumerStreamStore::default();
    let first = streams.allocate(first_mid);
    let second = streams.allocate(second_mid);
    streams.queue_repairable_write(first, first_primary);
    streams.queue_repairable_write(second, Ssrc::from(313));
    streams.note_repairable_transmit(first_primary, now);
    streams.note_repairable_transmit(Ssrc::from(313), now + Duration::from_secs(1));
    assert_eq!(streams.invalidate_rtx_stream(first), Some(first_primary));

    streams.expire_rtx_streams(&mut rtc, now + RTX_CACHE_LIFETIME);
    assert_eq!(
        streams.next_rtx_deadline,
        Some(now + Duration::from_secs(1) + RTX_CACHE_LIFETIME)
    );
    assert!(streams.rtx_cache_is_armed(second));
}

#[test]
fn stale_queued_primary_rotates_after_transmit_before_rearming() {
    let now = Instant::now();
    let mid = Mid::from("cam-down");
    let primary = Ssrc::from(315);
    let mut streams = ConsumerStreamStore::default();
    let stream = streams.allocate(mid);
    streams.queue_repairable_write(stream, primary);

    assert_eq!(streams.invalidate_rtx_stream(stream), Some(primary));
    assert_eq!(
        streams.note_repairable_transmit(primary, now),
        Some(primary)
    );
    assert!(!streams.rtx_cache_is_armed(stream));

    streams.queue_repairable_write(stream, primary);
    assert_eq!(streams.note_repairable_transmit(primary, now), None);
    assert!(streams.rtx_cache_is_armed(stream));
}

#[test]
fn replacement_stream_does_not_inherit_repair_state() {
    let now = Instant::now();
    let primary = Ssrc::from(321);
    let mut streams = ConsumerStreamStore::default();
    let stale = streams.allocate(Mid::from("old"));
    streams.queue_repairable_write(stale, primary);
    streams.queue_repairable_write(stale, primary);
    assert_eq!(streams.invalidate_rtx_stream(stale), Some(primary));

    streams.release(stale);
    let replacement = streams.allocate(Mid::from("old"));
    streams.queue_repairable_write(replacement, primary);

    assert!(!streams.rtx_cache_is_armed(stale));
    assert!(!streams.rtx_cache_is_armed(replacement));
    assert_eq!(
        streams.note_repairable_transmit(primary, now),
        Some(primary)
    );
    assert_eq!(
        streams.note_repairable_transmit(primary, now),
        Some(primary)
    );
    assert_eq!(streams.note_repairable_transmit(primary, now), None);
    assert!(streams.rtx_cache_is_armed(replacement));
}

#[test]
fn inactive_media_reset_discards_phantom_queued_writes() {
    let mid = Mid::from("cam-down");
    let primary = Ssrc::from(322);
    let mut streams = ConsumerStreamStore::default();
    let stream = streams.allocate(mid);
    streams.queue_repairable_write(stream, primary);

    streams.reset_rtx_streams(mid);
    streams.release(stream);

    assert!(streams.retired_repair_streams.is_empty());
    assert_eq!(
        streams.note_repairable_transmit(primary, Instant::now()),
        None
    );
}

#[test]
fn projected_sequence_numbers_start_in_initial_roc_and_increment_per_stream() {
    let mut streams = ConsumerStreamStore::default();
    let stream_handle = streams.allocate(Mid::default());

    let first = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(111),
        1234,
        codec::Packet::default(),
    );
    let second = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(111),
        1234,
        codec::Packet::default(),
    );

    assert_eq!(first.seq_no.roc(), 0);
    assert!(first.seq_no.is_next(second.seq_no));
}

#[test]
fn reordered_packets_do_not_move_codec_high_water_before_source_switch() {
    let mut streams = ConsumerStreamStore::default();
    let stream_handle = streams.allocate(Mid::default());
    let source_ssrc = Ssrc::from(111);
    let switched_ssrc = Ssrc::from(222);
    let inspector = vp8_inspector();
    let first_packet = vp8_packet(&inspector, 10, 10);
    let after_loss_packet = vp8_packet(&inspector, 13, 13);
    let reordered_packet = vp8_packet(&inspector, 12, 12);
    let switched_packet = vp8_packet(&inspector, 1, 1);

    let first = projected_packet(
        &mut streams,
        stream_handle,
        0,
        source_ssrc,
        10_u64.into(),
        10_000,
        first_packet,
    );
    let after_loss = projected_packet(
        &mut streams,
        stream_handle,
        0,
        source_ssrc,
        13_u64.into(),
        13_000,
        after_loss_packet,
    );
    let reordered = projected_packet(
        &mut streams,
        stream_handle,
        0,
        source_ssrc,
        12_u64.into(),
        12_000,
        reordered_packet,
    );
    let switched = projected_packet(
        &mut streams,
        stream_handle,
        0,
        switched_ssrc,
        1_u64.into(),
        20_000,
        switched_packet,
    );

    assert_eq!(*after_loss.seq_no - *first.seq_no, 3);
    assert_eq!(*reordered.seq_no - *first.seq_no, 2);
    assert!(after_loss.seq_no.is_next(switched.seq_no));
    assert_eq!(
        switched.transition,
        SourceTransition::Switched {
            previous_ssrc: source_ssrc
        }
    );

    let mut projection = codec::Projection::default();
    assert_eq!(
        first.codec,
        projection.project(first_packet.identity(), false)
    );
    assert_eq!(
        after_loss.codec,
        projection.project(after_loss_packet.identity(), false)
    );
    let mut reordered_projection = projection;
    assert_eq!(
        reordered.codec,
        reordered_projection.project(reordered_packet.identity(), false)
    );
    assert_eq!(
        switched.codec,
        codec::Projection::default().project(vp8_packet(&inspector, 14, 14).identity(), false)
    );
    assert!(switched_packet.rewrite(switched.codec).is_some());
}

#[test]
fn delivery_generation_compacts_only_intentional_gaps() {
    let mut streams = ConsumerStreamStore::default();
    let stream_handle = streams.allocate(Mid::default());
    let source_ssrc = Ssrc::from(111);
    let inspector = vp8_inspector();
    let before_pause_packet = vp8_packet(&inspector, 10, 10);
    let after_resume_packet = vp8_packet(&inspector, 1_000, 200);

    let before_pause = projected_packet(
        &mut streams,
        stream_handle,
        0,
        source_ssrc,
        10_u64.into(),
        10_000,
        before_pause_packet,
    );
    let after_resume = projected_packet(
        &mut streams,
        stream_handle,
        1,
        source_ssrc,
        1_000_u64.into(),
        20_000,
        after_resume_packet,
    );

    assert!(before_pause.seq_no.is_next(after_resume.seq_no));
    let mut projection = codec::Projection::default();
    assert_eq!(
        before_pause.codec,
        projection.project(before_pause_packet.identity(), false)
    );
    assert_eq!(
        after_resume.codec,
        projection.project(after_resume_packet.identity(), true)
    );
    assert!(
        streams
            .project_identity(
                stream_handle,
                source_identity(0, source_ssrc, 11_u64.into(), 11_000, false),
                vp8_packet(&inspector, 11, 11).identity(),
            )
            .is_none()
    );
}

#[test]
fn repair_fills_only_a_gap_in_the_current_delivery_epoch() {
    let mut streams = ConsumerStreamStore::default();
    let stream_handle = streams.allocate(Mid::default());
    let source_ssrc = Ssrc::from(111);

    let first = streams.project_identity(
        stream_handle,
        source_identity(0, source_ssrc, 10_u64.into(), 10_000, false),
        codec::PacketIdentity::default(),
    );
    let after_gap = streams.project_identity(
        stream_handle,
        source_identity(0, source_ssrc, 12_u64.into(), 12_000, false),
        codec::PacketIdentity::default(),
    );
    let repaired = streams.project_identity(
        stream_handle,
        source_identity(0, source_ssrc, 11_u64.into(), 11_000, true),
        codec::PacketIdentity::default(),
    );
    let stale_epoch = streams.project_identity(
        stream_handle,
        source_identity(1, source_ssrc, 11_u64.into(), 11_000, true),
        codec::PacketIdentity::default(),
    );
    let mut new_streams = ConsumerStreamStore::default();
    let new_stream = new_streams.allocate(Mid::default());
    assert!(
        new_streams
            .project_identity(
                new_stream,
                source_identity(0, source_ssrc, 12_u64.into(), 12_000, false),
                codec::PacketIdentity::default(),
            )
            .is_some()
    );
    let new_route = new_streams.project_identity(
        new_stream,
        source_identity(0, source_ssrc, 11_u64.into(), 11_000, true),
        codec::PacketIdentity::default(),
    );

    let (Some(first), Some(after_gap), Some(repaired)) = (first, after_gap, repaired) else {
        panic!("current-epoch primary packets and gap repair should project");
    };
    assert_eq!(*after_gap.seq_no - *first.seq_no, 2);
    assert!(repaired.seq_no.is_next(after_gap.seq_no));
    assert!(stale_epoch.is_none());
    assert!(new_route.is_none());
}

#[test]
fn repair_projection_preserves_extended_sequence_wrap() {
    let source_ssrc = Ssrc::from(111);
    let mut streams = ConsumerStreamStore::default();
    let stream = streams.allocate(Mid::default());
    let rollover = u64::from(RTP_SEQUENCE_NUMBER_MODULUS);

    let before_wrap = streams.project_identity(
        stream,
        source_identity(0, source_ssrc, (rollover - 1).into(), 10_000, false),
        codec::PacketIdentity::default(),
    );
    let after_gap = streams.project_identity(
        stream,
        source_identity(0, source_ssrc, (rollover + 1).into(), 12_000, false),
        codec::PacketIdentity::default(),
    );
    let repaired = streams.project_identity(
        stream,
        source_identity(0, source_ssrc, rollover.into(), 11_000, true),
        codec::PacketIdentity::default(),
    );

    let (Some(before_wrap), Some(after_gap), Some(repaired)) = (before_wrap, after_gap, repaired)
    else {
        panic!("repair across RTP sequence wrap should project");
    };
    assert_eq!(*after_gap.seq_no - *before_wrap.seq_no, 2);
    assert!(repaired.seq_no.is_next(after_gap.seq_no));
}

#[test]
fn rejected_source_deltas_preserve_the_next_valid_projection() {
    let source_ssrc = Ssrc::from(111);
    let inspector = vp8_inspector();
    for (receiver_anchor, source_anchor, rejected_sequence) in
        [(0, 10_u64, 9), (u64::MAX - 3, 10, 15), (0, 0, u64::MAX)]
    {
        let mut stream = ConsumerStream {
            rtp: RtpProjection::new(receiver_anchor.into()),
            ..ConsumerStream::default()
        };
        assert!(
            stream
                .project(
                    0,
                    source_ssrc,
                    source_anchor.into(),
                    10_000,
                    vp8_packet(&inspector, 10, 10).identity(),
                    false,
                )
                .is_some()
        );
        let mut control = stream;
        assert!(
            stream
                .project(
                    0,
                    source_ssrc,
                    rejected_sequence.into(),
                    50_000,
                    vp8_packet(&inspector, 100, 100).identity(),
                    false,
                )
                .is_none()
        );
        for (ssrc, sequence, timestamp, codec_identity) in [
            (
                source_ssrc,
                source_anchor + 1,
                11_000,
                vp8_packet(&inspector, 11, 11).identity(),
            ),
            (
                Ssrc::from(222),
                1,
                1_000,
                vp8_packet(&inspector, 1, 1).identity(),
            ),
        ] {
            let actual = stream.project(0, ssrc, sequence.into(), timestamp, codec_identity, false);
            let expected =
                control.project(0, ssrc, sequence.into(), timestamp, codec_identity, false);
            assert!(expected.is_some());
            assert_eq!(actual, expected);
        }
    }
}

#[test]
fn repair_admission_preserves_the_active_projection_window() {
    let source_ssrc = Ssrc::from(111);
    let inspector = vp8_inspector();
    let mut stream = ConsumerStream {
        rtp: RtpProjection::new(0_u64.into()),
        ..ConsumerStream::default()
    };
    let mut control = stream;
    assert!(
        stream
            .project(
                0,
                source_ssrc,
                9_u64.into(),
                9_000,
                vp8_packet(&inspector, 9, 9).identity(),
                true,
            )
            .is_none()
    );
    for sequence in [10_u64, 12] {
        let actual = stream.project(
            0,
            source_ssrc,
            sequence.into(),
            12_000,
            vp8_packet(&inspector, 12, 12).identity(),
            false,
        );
        let expected = control.project(
            0,
            source_ssrc,
            sequence.into(),
            12_000,
            vp8_packet(&inspector, 12, 12).identity(),
            false,
        );
        assert!(expected.is_some());
        assert_eq!(actual, expected);
    }
    for (generation, ssrc, sequence) in [
        (0, source_ssrc, 9_u64),
        (0, source_ssrc, 12),
        (0, source_ssrc, 13),
        (0, Ssrc::from(222), 11),
        (1, source_ssrc, 11),
    ] {
        assert!(
            stream
                .project(
                    generation,
                    ssrc,
                    sequence.into(),
                    50_000,
                    vp8_packet(&inspector, 100, 100).identity(),
                    true,
                )
                .is_none()
        );
    }
    for (sequence, expected_receiver_sequence) in [(10_u64, 0_u64), (11, 1)] {
        let Some(repair) = stream.project(
            0,
            source_ssrc,
            sequence.into(),
            11_000,
            vp8_packet(&inspector, 11, 11).identity(),
            true,
        ) else {
            panic!("repairs within the source projection window should be admitted");
        };
        assert_eq!(repair.seq_no, expected_receiver_sequence.into());
        assert_eq!(repair.transition, SourceTransition::Unchanged);
        assert!(!repair.resets_rtx_cache);
    }
    let actual = stream.project(
        0,
        Ssrc::from(222),
        1_u64.into(),
        1_000,
        vp8_packet(&inspector, 1, 1).identity(),
        false,
    );
    let expected = control.project(
        0,
        Ssrc::from(222),
        1_u64.into(),
        1_000,
        vp8_packet(&inspector, 1, 1).identity(),
        false,
    );
    assert!(expected.is_some());
    assert_eq!(actual, expected);
}

#[test]
fn delivery_generation_wrap_preserves_serial_order() {
    let source_ssrc = Ssrc::from(111);
    let mut stream = ConsumerStream {
        delivery_generation: u64::MAX,
        ..ConsumerStream::default()
    };
    let Some(before_wrap) = stream.project(
        u64::MAX,
        source_ssrc,
        10_u64.into(),
        10_000,
        codec::PacketIdentity::default(),
        false,
    ) else {
        panic!("current delivery generation should initialize the projection");
    };
    let Some(after_wrap) = stream.project(
        0,
        source_ssrc,
        100_u64.into(),
        100_000,
        codec::PacketIdentity::default(),
        false,
    ) else {
        panic!("generation zero should follow the maximum wrapping generation");
    };
    assert!(before_wrap.seq_no.is_next(after_wrap.seq_no));
    assert_eq!(after_wrap.rtp_timestamp, before_wrap.rtp_timestamp + 1);
    assert!(after_wrap.resets_rtx_cache);
    let mut control = stream;
    assert!(
        stream
            .project(
                u64::MAX,
                source_ssrc,
                11_u64.into(),
                11_000,
                codec::PacketIdentity::default(),
                false,
            )
            .is_none()
    );
    let actual = stream.project(
        0,
        source_ssrc,
        101_u64.into(),
        101_000,
        codec::PacketIdentity::default(),
        false,
    );
    let expected = control.project(
        0,
        source_ssrc,
        101_u64.into(),
        101_000,
        codec::PacketIdentity::default(),
        false,
    );
    assert!(expected.is_some());
    assert_eq!(actual, expected);
}

#[test]
fn projected_sequence_numbers_are_scoped_by_consumer_stream() {
    let mut streams = ConsumerStreamStore::default();
    let stream_handle = allocate_at(&mut streams, 10);
    let other_stream_handle = allocate_at(&mut streams, 10);

    let first = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(111),
        1234,
        codec::Packet::default(),
    );
    let second = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(111),
        1234,
        codec::Packet::default(),
    );
    let other = projected(
        &mut streams,
        other_stream_handle,
        Ssrc::from(111),
        1234,
        codec::Packet::default(),
    );

    assert_eq!(first.seq_no.roc(), 0);
    assert!(first.seq_no.is_next(second.seq_no));
    assert_eq!(other.seq_no, first.seq_no);
}

#[test]
fn forgetting_transport_media_streams_drops_all_stream_state_for_that_consumer() {
    let mut streams = ConsumerStreamStore::default();
    let stream_handle = allocate_at(&mut streams, 10);

    let first = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(111),
        1234,
        codec::Packet::default(),
    );
    let second = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(111),
        1234,
        codec::Packet::default(),
    );

    streams.release(stream_handle);
    let replacement_stream_handle = allocate_at(&mut streams, 10);

    let reset = projected(
        &mut streams,
        replacement_stream_handle,
        Ssrc::from(111),
        1234,
        codec::Packet::default(),
    );
    assert!(first.seq_no.is_next(second.seq_no));
    assert_eq!(reset.seq_no, first.seq_no);
}

#[test]
fn released_consumer_stream_handle_is_ignored_after_slot_reuse() {
    let mut streams = ConsumerStreamStore::default();
    let stream_handle = streams.allocate(Mid::default());

    streams.release(stream_handle);
    let replacement_handle = streams.allocate(Mid::default());

    let replacement = projected(
        &mut streams,
        replacement_handle,
        Ssrc::from(222),
        5678,
        codec::Packet::default(),
    );

    assert!(
        streams
            .project_identity(
                stream_handle,
                source_identity(0, Ssrc::from(111), 0_u64.into(), 1234, false),
                codec::Packet::default().identity(),
            )
            .is_none()
    );
    let replacement_next = projected(
        &mut streams,
        replacement_handle,
        Ssrc::from(222),
        5678,
        codec::Packet::default(),
    );
    assert!(replacement.seq_no.is_next(replacement_next.seq_no));
}

#[test]
fn projected_timestamps_preserve_source_deltas_on_one_ssrc() {
    let mut streams = ConsumerStreamStore::default();
    let stream_handle = streams.allocate(Mid::default());

    let first = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(111),
        10_000,
        codec::Packet::default(),
    );
    let second = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(111),
        13_000,
        codec::Packet::default(),
    );

    assert_eq!(first.rtp_timestamp, 10_000);
    assert_eq!(second.rtp_timestamp, 13_000);
    assert_eq!(first.transition, SourceTransition::Unchanged);
    assert_eq!(second.transition, SourceTransition::Unchanged);
}

#[test]
fn projected_timestamps_stay_monotonic_across_simulcast_ssrc_switches() {
    let mut streams = ConsumerStreamStore::default();
    let stream_handle = streams.allocate(Mid::default());

    let low = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(111),
        90_000,
        codec::Packet::default(),
    );
    let high = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(222),
        1_000,
        codec::Packet::default(),
    );
    let high_next = projected(
        &mut streams,
        stream_handle,
        Ssrc::from(222),
        4_000,
        codec::Packet::default(),
    );

    assert_eq!(low.rtp_timestamp, 90_000);
    assert_eq!(high.rtp_timestamp, 90_001);
    assert_eq!(high_next.rtp_timestamp, 93_001);
    assert_eq!(low.transition, SourceTransition::Unchanged);
    assert_eq!(
        high.transition,
        SourceTransition::Switched {
            previous_ssrc: Ssrc::from(111)
        }
    );
    assert_eq!(high_next.transition, SourceTransition::Unchanged);
}
