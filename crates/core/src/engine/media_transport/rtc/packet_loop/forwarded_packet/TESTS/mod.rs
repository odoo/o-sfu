use std::{net::SocketAddr, sync::Mutex};

use o_sfu_rfc::rtp::{CodecName, RTP_SEQUENCE_NUMBER_MODULUS};
use o_sfu_router::{
    MediaKind as RouterMediaKind,
    rtp::{MediaFormat, MediaStream as RouterRtpParameters, PayloadType, StreamBinding},
};
use str0m::media::{Mid, Rid};

use super::{
    test_support::{
        sample_forwarded_packet, sample_forwarded_packet_with_rid,
        sample_forwarded_packet_without_mid, sample_local_forwarded_packet,
    },
    *,
};
use crate::{
    Bitrate,
    engine::{
        UserId,
        media_transport::rtc::{
            bootstrap::ensure_session_rtc_state,
            state::{bitrate::BitrateRegistry, media_registry::RegisteredMediaHandle},
            test_support::test_transport_session_key,
        },
    },
};

fn install_test_session(
    state: &mut PacketLoopState,
    session_key: &TransportSessionKey,
) -> Option<SessionHandle> {
    assert!(
        ensure_session_rtc_state(
            &mut state.users,
            session_key,
            SocketAddr::from(([127, 0, 0, 1], 9)),
            Bitrate::from_bps(1_000_000),
        )
        .is_ok()
    );
    state.users.handle_for_key(session_key)
}

fn video_format(codec: CodecName, payload_type: u8) -> MediaFormat {
    MediaFormat::new(
        RouterMediaKind::Video,
        codec,
        PayloadType::new(payload_type),
        90_000,
    )
}

fn project_codec_packet(packet: &codec::Packet) -> codec::ProjectedPacket {
    codec::Projection::default().project(packet.identity(), false)
}

#[test]
fn local_forwarded_packet_resolves_transport_media_id_through_session_handle() {
    let session_key = test_transport_session_key(51, 0, 19, UserId::Integer(17));
    let mut state = PacketLoopState::default();
    let session_handle = install_test_session(&mut state, &session_key);
    assert!(session_handle.is_some());
    let Some(session_handle) = session_handle else {
        return;
    };
    let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: session_key.clone(),
        mid: Mid::from("aud-up"),
    });
    let mut packet = sample_local_forwarded_packet(session_handle, "aud-up", b"payload");

    assert_eq!(packet.src_key(&state), Some(&session_key));
    assert_eq!(packet.resolve_src_media(&state), Some(transport_media_id));
}

#[test]
fn stale_local_forwarded_packet_does_not_resolve_through_reused_slot() {
    let session_key = test_transport_session_key(52, 0, 20, UserId::Integer(18));
    let replacement_session_key = test_transport_session_key(52, 0, 21, UserId::Integer(19));
    let mut state = PacketLoopState::default();
    let stale_handle = install_test_session(&mut state, &session_key);
    assert!(stale_handle.is_some());
    let Some(stale_handle) = stale_handle else {
        return;
    };
    let _removed = state.users.remove(&session_key);
    let _replacement_handle = install_test_session(&mut state, &replacement_session_key);
    state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: replacement_session_key,
        mid: Mid::from("aud-up"),
    });
    let mut packet = sample_local_forwarded_packet(stale_handle, "aud-up", b"payload");

    assert!(packet.src_key(&state).is_none());
    assert!(packet.resolve_facts(&state).is_none());
    assert!(
        packet
            .share_for_relay(&state, TransportMediaId::new(99))
            .is_none()
    );
}

#[test]
fn forwarded_packet_decoder_refresh_follows_the_packet_payload_type() {
    let session_key = test_transport_session_key(53, 0, 21, UserId::Integer(19));
    let producer_mid = Mid::from("cam-up");
    let mut state = PacketLoopState::default();
    let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: session_key.clone(),
        mid: producer_mid,
    });
    let parameters = RouterRtpParameters::new(
        vec![
            video_format(CodecName::Vp8, 96),
            video_format(CodecName::H264, 111),
        ],
        vec![],
        vec![],
    );
    state
        .routes
        .refresh_packet_inspector(transport_media_id, &parameters);
    let mut vp8_packet = sample_forwarded_packet(
        session_key.clone(),
        "cam-up",
        &[
            0x10, 0x30, 0x00, 0x00, 0x9d, 0x01, 0x2a, 0x80, 0x02, 0x68, 0x01,
        ],
    );
    if let ForwardedPacketData::RelayRtp(rtp) = &mut vp8_packet.data {
        rtp.header.payload_type = 96.into();
    }
    let vp8_facts = vp8_packet.resolve_facts(&state);
    assert!(vp8_facts.is_some_and(|facts| facts.codec.decoder_refresh()));

    let mut h264_packet =
        sample_forwarded_packet_with_rid(session_key, "cam-up", Some("hi"), &[0x65, 0x88]);
    if let ForwardedPacketData::RelayRtp(rtp) = &mut h264_packet.data {
        rtp.header.payload_type = 111.into();
    }
    let h264_facts = h264_packet.resolve_facts(&state);
    assert!(h264_facts.is_some_and(|facts| !facts.codec.decoder_refresh()));
}

#[test]
fn producer_packet_inspector_rebuilds_on_renegotiation_and_clear() {
    let session_key = test_transport_session_key(54, 0, 22, UserId::Integer(20));
    let producer_mid = Mid::from("cam-up");
    let payload_type = 96.into();
    let keyframe = [
        0x10, 0x30, 0x00, 0x00, 0x9d, 0x01, 0x2a, 0x80, 0x02, 0x68, 0x01,
    ];
    let mut state = PacketLoopState::default();
    let source_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: session_key.clone(),
        mid: producer_mid,
    });
    let vp8 = RouterRtpParameters::new(vec![video_format(CodecName::Vp8, 96)], vec![], vec![]);
    let h264 = RouterRtpParameters::new(vec![video_format(CodecName::H264, 96)], vec![], vec![]);

    state.refresh_producer_ssrcs(&session_key, producer_mid, &vp8);
    assert!(
        state
            .routes
            .inspect_packet(source_id, payload_type, &keyframe, false)
            .decoder_refresh()
    );

    state.refresh_producer_ssrcs(&session_key, producer_mid, &h264);
    assert!(
        !state
            .routes
            .inspect_packet(source_id, payload_type, &keyframe, true)
            .decoder_refresh()
    );

    state.refresh_producer_ssrcs(&session_key, producer_mid, &vp8);
    assert!(
        state
            .routes
            .inspect_packet(source_id, payload_type, &keyframe, false)
            .decoder_refresh()
    );
    state.clear_producer_ssrcs_for_mid(&session_key, producer_mid);
    assert!(
        !state
            .routes
            .inspect_packet(source_id, payload_type, &keyframe, true)
            .decoder_refresh()
    );
}

#[test]
fn forwarded_packet_relay_clone_preserves_source_facts() {
    let session_key = test_transport_session_key(49, 0, 17, UserId::Integer(15));
    let mut source_state = PacketLoopState::default();
    let session_handle = install_test_session(&mut source_state, &session_key);
    assert!(session_handle.is_some());
    let Some(session_handle) = session_handle else {
        return;
    };
    let src_media = source_state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key,
        mid: Mid::from("cam-up"),
    });
    source_state.routes.refresh_packet_inspector(
        src_media,
        &RouterRtpParameters::new(vec![video_format(CodecName::Vp8, 111)], vec![], vec![]),
    );
    let mut source_packet = sample_local_forwarded_packet(
        session_handle,
        "cam-up",
        &[
            0x90, 0xe0, 0x80, 0x02, 0x09, 0x00, 0x00, 0x00, 0x00, 0x9d, 0x01, 0x2a, 0x80, 0x02,
            0x68, 0x01,
        ],
    );
    let post_wrap_sequence = u64::from(RTP_SEQUENCE_NUMBER_MODULUS) + 1;
    if let ForwardedPacketData::RelayRtp(rtp) = &mut source_packet.data {
        rtp.header.ext_vals.rid = Some(Rid::from("hi"));
        rtp.sequence_number = post_wrap_sequence.into();
    }
    source_packet.was_repair = true;
    let source_facts = source_packet.resolve_facts(&source_state).copied();
    assert!(source_facts.is_some_and(|facts| facts.codec.decoder_refresh()));

    let relay_packet = source_packet.share_for_relay(&source_state, src_media);
    assert!(relay_packet.is_some());
    let Some(mut relay_packet) = relay_packet else {
        return;
    };

    let relay_facts = relay_packet
        .resolve_facts(&PacketLoopState::default())
        .copied();
    assert!(relay_facts.is_some());
    let (Some(source_facts), Some(relay_facts)) = (source_facts, relay_facts) else {
        return;
    };
    assert_eq!(relay_facts.src_media, source_facts.src_media);
    assert_eq!(relay_facts.rid, source_facts.rid);
    assert_eq!(relay_facts.room_instance_id, source_facts.room_instance_id);
    assert_eq!(relay_facts.voice_activity, source_facts.voice_activity);
    assert_eq!(relay_facts.audio_level, source_facts.audio_level);
    assert!(relay_facts.codec.decoder_refresh());
    assert!(relay_packet.was_repair);
    assert!(matches!(
        relay_packet.data,
        ForwardedPacketData::RelayRtp(ref rtp)
            if rtp.sequence_number == post_wrap_sequence.into()
    ));
    let source_codec = project_codec_packet(&source_facts.codec);
    assert_ne!(source_codec, codec::ProjectedPacket::default());
    assert_eq!(project_codec_packet(&relay_facts.codec), source_codec);
}

#[test]
fn forwarded_packet_relay_clone_keeps_payload_and_explicit_source_media_id() {
    let session_key = test_transport_session_key(43, 0, 11, UserId::Integer(9));
    let state = PacketLoopState::default();
    let packet = sample_forwarded_packet(session_key.clone(), "aud-up", b"payload");
    let relay_packet = packet.share_for_relay(&state, TransportMediaId::new(18));
    assert!(relay_packet.is_some());
    let Some(mut relay_packet) = relay_packet else {
        return;
    };

    assert_eq!(relay_packet.src_key(&state), Some(&session_key));
    assert_eq!(relay_packet.payload(), b"payload");
    assert_eq!(
        relay_packet.resolve_src_media(&PacketLoopState::default()),
        Some(TransportMediaId::new(18))
    );
}

#[test]
fn forwarded_packet_facts_carry_codec_packet() {
    let session_key = test_transport_session_key(50, 0, 18, UserId::Integer(16));
    let mut state = PacketLoopState::default();
    let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: session_key.clone(),
        mid: Mid::from("cam-up"),
    });
    let parameters =
        RouterRtpParameters::new(vec![video_format(CodecName::Vp8, 111)], vec![], vec![]);
    state
        .routes
        .refresh_packet_inspector(transport_media_id, &parameters);
    let payload = [0x90, 0xe0, 0x80, 0x02, 0x09, 0x00, 0x00];
    let mut packet = sample_forwarded_packet_with_rid(session_key, "cam-up", Some("hi"), &payload);
    let facts = packet.resolve_facts(&state);
    assert!(facts.is_some());
    let Some(facts) = facts else {
        return;
    };
    assert_eq!(facts.src_media, transport_media_id);
    assert_eq!(facts.rid, Some(Rid::from("hi")));
    let expected = project_codec_packet(
        &codec::PacketInspector::from_parameters(&parameters).inspect(111.into(), &payload, true),
    );
    assert_ne!(expected, codec::ProjectedPacket::default());
    assert_eq!(project_codec_packet(&facts.codec), expected);
    assert_eq!(
        packet.local_codec_packet().map(project_codec_packet),
        Some(expected)
    );
}

#[test]
fn forwarded_packet_recovers_rid_from_ssrc_binding_when_extension_is_absent() {
    let session_key = test_transport_session_key(47, 0, 15, UserId::Integer(13));
    let producer_mid = Mid::from("cam-up");
    let producer_ssrc = 76_543_u32;
    let mut state = PacketLoopState::default();
    state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: session_key.clone(),
        mid: producer_mid,
    });
    let parameters = RouterRtpParameters::new(
        vec![video_format(CodecName::Vp8, 96)],
        vec![],
        vec![StreamBinding::new().with_ssrc(producer_ssrc).with_rid("hi")],
    )
    .with_mid(producer_mid.to_string());
    state.refresh_producer_ssrcs(&session_key, producer_mid, &parameters);
    let payload = [0x90, 0xe0, 0x80, 0x02, 0x09, 0x00, 0x00];
    let mut packet = sample_forwarded_packet_without_mid(session_key, producer_ssrc, &payload);
    if let ForwardedPacketData::RelayRtp(rtp) = &mut packet.data {
        rtp.header.payload_type = 96.into();
    }

    let facts = packet.resolve_facts(&state).copied();
    assert!(facts.is_some());
    let Some(facts) = facts else {
        return;
    };

    assert_eq!(facts.rid, Some(Rid::from("hi")));
    let expected = project_codec_packet(
        &codec::PacketInspector::from_parameters(&parameters).inspect(96.into(), &payload, true),
    );
    assert_ne!(expected, codec::ProjectedPacket::default());
    assert_eq!(project_codec_packet(&facts.codec), expected);
    let relay_facts = packet
        .share_for_relay(&state, facts.src_media)
        .and_then(|mut relayed| relayed.resolve_facts(&PacketLoopState::default()).copied());
    assert_eq!(relay_facts.map(|facts| facts.rid), Some(facts.rid));
    assert_eq!(
        relay_facts.map(|facts| project_codec_packet(&facts.codec)),
        Some(expected)
    );
}

#[test]
fn forwarded_packet_falls_back_to_ssrc_when_mid_is_missing() {
    let session_key = test_transport_session_key(44, 0, 12, UserId::Integer(10));
    let producer_mid = Mid::from("cam-up");
    let producer_ssrc = 65_432_u32;
    let mut state = PacketLoopState::default();
    let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: session_key.clone(),
        mid: producer_mid,
    });
    state.refresh_producer_ssrcs(
        &session_key,
        producer_mid,
        &RouterRtpParameters::new(
            vec![],
            vec![],
            vec![StreamBinding::new().with_ssrc(producer_ssrc)],
        )
        .with_mid(producer_mid.to_string()),
    );
    let mut packet = sample_forwarded_packet_without_mid(session_key, producer_ssrc, b"payload");

    assert_eq!(packet.resolve_src_media(&state), Some(transport_media_id));
}

#[test]
fn forwarded_packet_caches_the_resolved_src_media() {
    let session_key = test_transport_session_key(45, 0, 13, UserId::Integer(11));
    let mut state = PacketLoopState::default();
    let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: session_key.clone(),
        mid: Mid::from("aud-up"),
    });
    let mut packet = sample_forwarded_packet(session_key, "aud-up", b"payload");

    assert_eq!(packet.resolve_src_media(&state), Some(transport_media_id));
    assert!(
        state
            .unregister_media_handle(&Mutex::new(BitrateRegistry::default()), transport_media_id)
            .is_ok()
    );
    assert_eq!(packet.resolve_src_media(&state), Some(transport_media_id));
}
