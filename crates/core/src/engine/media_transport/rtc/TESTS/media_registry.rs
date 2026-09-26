use std::time::{Duration, Instant};

use o_sfu_router::rtp::{MediaStream as RouterRtpParameters, StreamBinding};
use tokio::sync::mpsc;

use super::{super::relay_registry::RelayTargetId, *};
use crate::engine::{
    UserId,
    media_transport::{
        TransportSourceKey,
        rtc::{commands::RemoteSourceControl, test_support::test_transport_session_key},
    },
    metrics::RuntimeMetrics,
};

fn rtp_parameters_with_ssrc(mid: Mid, ssrc: u32) -> RouterRtpParameters {
    RouterRtpParameters::new(vec![], vec![], vec![StreamBinding::new().with_ssrc(ssrc)])
        .with_mid(mid.to_string())
}

#[test]
fn consumer_media_lookup_uses_the_reverse_index() {
    let mut state = PacketLoopState::default();
    let src_media = TransportMediaId::new(8);
    let consumer_session = test_transport_session_key(12, 0, 13, UserId::Integer(14));
    let consumer_mid = Mid::from("aud-down");

    state.register_media_handle(RegisteredMediaHandle::Consumer {
        session_key: consumer_session.clone(),
        mid: consumer_mid,
        src_media,
    });

    assert_eq!(
        state.consumer_src_media_for_mid(&consumer_session, consumer_mid),
        Some(src_media)
    );
}

#[test]
fn consumer_media_lookup_clears_when_the_handle_is_removed() {
    let mut state = PacketLoopState::default();
    let src_media = TransportMediaId::new(9);
    let consumer_session = test_transport_session_key(15, 0, 16, UserId::Integer(17));
    let consumer_mid = Mid::from("cam-down");

    let consumer_media = state.register_media_handle(RegisteredMediaHandle::Consumer {
        session_key: consumer_session.clone(),
        mid: consumer_mid,
        src_media,
    });
    assert_eq!(
        state.consumer_src_media_for_mid(&consumer_session, consumer_mid),
        Some(src_media)
    );

    let removed_handle = state.remove_media_handle(consumer_media);

    assert!(matches!(
        removed_handle,
        Some(RegisteredMediaHandle::Consumer {
            session_key,
            mid,
            src_media: removed_src_media,
        }) if session_key == consumer_session
            && mid == consumer_mid
            && removed_src_media == src_media
    ));
    assert_eq!(
        state.consumer_src_media_for_mid(&consumer_session, consumer_mid),
        None
    );
}

#[test]
fn session_media_index_drives_bulk_session_removal() {
    let mut state = PacketLoopState::default();
    let removed_session = test_transport_session_key(15, 0, 16, UserId::Integer(17));
    let kept_session = test_transport_session_key(18, 0, 19, UserId::Integer(20));
    let producer_mid = Mid::from("cam-up");
    let consumer_mid = Mid::from("cam-down");
    let kept_mid = Mid::from("kept-cam-up");
    let producer_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: removed_session.clone(),
        mid: producer_mid,
    });
    let consumer_media_id = state.register_media_handle(RegisteredMediaHandle::Consumer {
        session_key: removed_session.clone(),
        mid: consumer_mid,
        src_media: producer_media_id,
    });
    let kept_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: kept_session.clone(),
        mid: kept_mid,
    });

    let mut removed_ids = state.remove_session_media_handles(&removed_session);
    removed_ids.sort_unstable();
    assert_eq!(removed_ids, vec![producer_media_id, consumer_media_id]);
    assert!(!state.session_has_registered_media(&removed_session));
    assert_eq!(
        state.src_media_for_mid(&removed_session, producer_mid),
        None
    );
    assert_eq!(
        state.consumer_src_media_for_mid(&removed_session, consumer_mid),
        None
    );
    assert_eq!(
        state.src_media_for_mid(&kept_session, kept_mid),
        Some(kept_media_id)
    );
}

#[test]
fn producer_media_lookup_is_session_scoped_by_mid() {
    let mut state = PacketLoopState::default();
    let first_session = test_transport_session_key(16, 0, 18, UserId::Integer(19));
    let second_session = test_transport_session_key(17, 0, 18, UserId::Integer(19));
    let shared_mid = Mid::from("cam-up");
    let first_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: first_session.clone(),
        mid: shared_mid,
    });
    let second_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: second_session.clone(),
        mid: shared_mid,
    });

    assert_eq!(
        state.src_media_for_mid(&first_session, shared_mid),
        Some(first_media_id)
    );
    assert_eq!(
        state.src_media_for_mid(&second_session, shared_mid),
        Some(second_media_id)
    );

    let _removed_handle = state.remove_media_handle(first_media_id);

    assert_eq!(state.src_media_for_mid(&first_session, shared_mid), None);
    assert_eq!(
        state.src_media_for_mid(&second_session, shared_mid),
        Some(second_media_id)
    );
}

#[test]
fn producer_media_lookup_falls_back_to_negotiated_ssrc() {
    let producer_session = test_transport_session_key(18, 0, 19, UserId::Integer(20));
    let producer_mid = Mid::from("cam-up");
    let producer_ssrc = 55_555_u32;
    let mut state = PacketLoopState::default();
    let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: producer_session.clone(),
        mid: producer_mid,
    });
    state.refresh_producer_ssrcs(
        &producer_session,
        producer_mid,
        &rtp_parameters_with_ssrc(producer_mid, producer_ssrc),
    );

    assert_eq!(
        state.src_media_for_ssrc(&producer_session, Ssrc::from(producer_ssrc)),
        Some(transport_media_id)
    );
}

#[test]
fn dynamic_producer_ssrc_rid_lookup_clears_with_media_handle() {
    let producer_session = test_transport_session_key(25, 0, 26, UserId::Integer(27));
    let source = ForwardedPacketSource::Relayed(producer_session.clone());
    let producer_mid = Mid::from("cam-up");
    let producer_ssrc = Ssrc::from(99_999_u32);
    let learned_rid = Rid::from("hi");
    let mut state = PacketLoopState::default();
    let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: producer_session.clone(),
        mid: producer_mid,
    });

    state.refresh_producer_ssrcs(
        &producer_session,
        producer_mid,
        &RouterRtpParameters::new(vec![], vec![], vec![StreamBinding::new().with_rid("hi")]),
    );
    let binding = ProducerStreamBinding {
        rid: Some(learned_rid),
        primary: producer_ssrc,
        repair: None,
    };
    assert_eq!(
        state.bind_producer_stream(&source, transport_media_id, binding),
        ProducerSsrcUpdate::Learned,
    );
    assert_eq!(
        state.bind_producer_stream(&source, transport_media_id, binding),
        ProducerSsrcUpdate::Unchanged,
    );
    let repair_ssrc = Ssrc::from(100_000_u32);
    assert_eq!(
        state.bind_producer_stream(
            &source,
            transport_media_id,
            ProducerStreamBinding {
                repair: Some(repair_ssrc),
                ..binding
            }
        ),
        ProducerSsrcUpdate::Learned,
    );
    assert_eq!(
        state.src_media_for_ssrc(&producer_session, repair_ssrc),
        Some(transport_media_id)
    );

    assert_eq!(
        state.src_media_for_ssrc(&producer_session, producer_ssrc),
        Some(transport_media_id)
    );
    assert_eq!(
        state.source_rid_for_ssrc(&producer_session, producer_ssrc),
        Some(learned_rid)
    );

    let _removed_handle = state.remove_media_handle(transport_media_id);

    assert_eq!(
        state.src_media_for_ssrc(&producer_session, producer_ssrc),
        None
    );
    assert_eq!(
        state.source_rid_for_ssrc(&producer_session, producer_ssrc),
        None
    );
}

#[test]
fn producer_ssrc_lookup_refresh_replaces_stale_bindings() {
    let producer_session = test_transport_session_key(21, 0, 22, UserId::Integer(23));
    let producer_mid = Mid::from("cam-up");
    let first_ssrc = 77_777_u32;
    let second_ssrc = 88_888_u32;
    let mut state = PacketLoopState::default();
    let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: producer_session.clone(),
        mid: producer_mid,
    });

    state.refresh_producer_ssrcs(
        &producer_session,
        producer_mid,
        &rtp_parameters_with_ssrc(producer_mid, first_ssrc),
    );
    assert_eq!(
        state.src_media_for_ssrc(&producer_session, Ssrc::from(first_ssrc)),
        Some(transport_media_id)
    );

    state.refresh_producer_ssrcs(
        &producer_session,
        producer_mid,
        &rtp_parameters_with_ssrc(producer_mid, second_ssrc),
    );

    assert_eq!(
        state.src_media_for_ssrc(&producer_session, Ssrc::from(first_ssrc)),
        None
    );
    assert_eq!(
        state.src_media_for_ssrc(&producer_session, Ssrc::from(second_ssrc)),
        Some(transport_media_id)
    );
}

#[test]
fn producer_mid_lookup_survives_ssrc_binding_refresh() {
    let producer_session = test_transport_session_key(28, 0, 29, UserId::Integer(30));
    let producer_mid = Mid::from("cam-up");
    let producer_ssrc = 44_444_u32;
    let mut state = PacketLoopState::default();
    let transport_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: producer_session.clone(),
        mid: producer_mid,
    });

    assert_eq!(
        state.src_media_for_mid(&producer_session, producer_mid),
        Some(transport_media_id)
    );

    state.refresh_producer_ssrcs(
        &producer_session,
        producer_mid,
        &rtp_parameters_with_ssrc(producer_mid, producer_ssrc),
    );

    assert_eq!(
        state.src_media_for_mid(&producer_session, producer_mid),
        Some(transport_media_id)
    );
    assert_eq!(
        state.src_media_for_ssrc(&producer_session, Ssrc::from(producer_ssrc)),
        Some(transport_media_id)
    );
}

#[test]
fn dynamic_producer_ssrc_binding_cannot_steal_another_media_id() {
    let producer_session = test_transport_session_key(28, 0, 31, UserId::Integer(32));
    let source = ForwardedPacketSource::Relayed(producer_session.clone());
    let first_mid = Mid::from("cam-up-a");
    let second_mid = Mid::from("cam-up-b");
    let shared_ssrc = Ssrc::from(66_666_u32);
    let mut state = PacketLoopState::default();
    let first_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: producer_session.clone(),
        mid: first_mid,
    });
    let second_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: producer_session.clone(),
        mid: second_mid,
    });

    for (mid, rid) in [(first_mid, "hi"), (second_mid, "lo")] {
        state.refresh_producer_ssrcs(
            &producer_session,
            mid,
            &RouterRtpParameters::new(vec![], vec![], vec![StreamBinding::new().with_rid(rid)]),
        );
    }
    let original_encoding = Rid::from("hi");
    assert_eq!(
        state.bind_producer_stream(
            &source,
            first_media_id,
            ProducerStreamBinding {
                rid: Some(original_encoding),
                primary: shared_ssrc,
                repair: None,
            }
        ),
        ProducerSsrcUpdate::Learned,
    );
    assert_eq!(
        state.bind_producer_stream(
            &source,
            second_media_id,
            ProducerStreamBinding {
                rid: Some(Rid::from("lo")),
                primary: shared_ssrc,
                repair: None,
            }
        ),
        ProducerSsrcUpdate::Rejected,
    );

    assert_eq!(
        state.src_media_for_ssrc(&producer_session, shared_ssrc),
        Some(first_media_id)
    );
    assert_eq!(
        state.source_rid_for_ssrc(&producer_session, shared_ssrc),
        Some(original_encoding)
    );
    assert_eq!(
        state.src_media_for_mid(&producer_session, second_mid),
        Some(second_media_id)
    );
    let unbound_primary = Ssrc::from(66_668_u32);
    assert_eq!(
        state.bind_producer_stream(
            &source,
            second_media_id,
            ProducerStreamBinding {
                rid: Some(Rid::from("lo")),
                primary: unbound_primary,
                repair: Some(shared_ssrc),
            }
        ),
        ProducerSsrcUpdate::Rejected,
    );
    assert_eq!(
        state.src_media_for_ssrc(&producer_session, unbound_primary),
        None
    );
    assert!(
        state
            .routes
            .producer_ssrcs(second_media_id)
            .is_some_and(<[Ssrc]>::is_empty)
    );
    assert_eq!(
        state.routes.producer_ssrcs(first_media_id),
        Some([shared_ssrc].as_slice())
    );
}

#[test]
fn rejected_negotiated_ssrc_binding_does_not_clear_existing_owner() {
    let producer_session = test_transport_session_key(28, 0, 33, UserId::Integer(34));
    let first_mid = Mid::from("cam-up-a");
    let second_mid = Mid::from("cam-up-b");
    let shared_ssrc = 66_667_u32;
    let mut state = PacketLoopState::default();
    let first_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: producer_session.clone(),
        mid: first_mid,
    });
    let second_media_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: producer_session.clone(),
        mid: second_mid,
    });

    state.refresh_producer_ssrcs(
        &producer_session,
        first_mid,
        &rtp_parameters_with_ssrc(first_mid, shared_ssrc),
    );
    state.refresh_producer_ssrcs(
        &producer_session,
        second_mid,
        &rtp_parameters_with_ssrc(second_mid, shared_ssrc),
    );
    state.clear_producer_ssrcs_for_mid(&producer_session, second_mid);

    assert_eq!(
        state.src_media_for_ssrc(&producer_session, Ssrc::from(shared_ssrc)),
        Some(first_media_id)
    );
    assert_eq!(
        state.src_media_for_mid(&producer_session, second_mid),
        Some(second_media_id)
    );
}

#[test]
fn expired_local_and_relay_speakers_resolve_the_same_room_once() {
    let mut state = PacketLoopState::default();
    let rtc_metrics = RuntimeMetrics::default().register_rtc_worker();
    let session = test_transport_session_key(31, 0, 32, UserId::Integer(33));
    let local_id = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: session.clone(),
        mid: Mid::from("cam-up-a"),
    });
    let relay_id = TransportMediaId::new(40);
    let (control_tx, _control_rx) = mpsc::channel(1);
    assert!(
        state
            .routes
            .register_remote_source(
                &TransportSourceKey::new(session.clone(), relay_id),
                RemoteSourceControl::new(control_tx, RelayTargetId::new(1), rtc_metrics),
            )
            .is_ok()
    );
    let observed_at = Instant::now();
    for source_id in [local_id, relay_id] {
        state
            .routes
            .observe_audio_activity(source_id, Some(true), None, observed_at);
    }
    let expired_at = observed_at + Duration::from_millis(251);

    assert_eq!(
        state.take_expired_speaker_rooms(expired_at),
        BTreeSet::from([session.room_instance_id()])
    );
    assert!(state.take_expired_speaker_rooms(expired_at).is_empty());
}

#[test]
fn producer_ssrc_rotation_keeps_only_current_encoding_binding() {
    let session = test_transport_session_key(51, 0, 52, UserId::Integer(53));
    let source = ForwardedPacketSource::Relayed(session.clone());
    let mid = Mid::from("camera");
    let rid = Rid::from("hi");
    let mut state = PacketLoopState::default();
    let media = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: session.clone(),
        mid,
    });
    state.refresh_producer_ssrcs(
        &session,
        mid,
        &RouterRtpParameters::new(vec![], vec![], vec![StreamBinding::new().with_rid("hi")]),
    );
    for ssrc in 1..=10_000 {
        assert_ne!(
            state.bind_producer_stream(
                &source,
                media,
                ProducerStreamBinding {
                    rid: Some(rid),
                    primary: ssrc.into(),
                    repair: None,
                }
            ),
            ProducerSsrcUpdate::Rejected,
        );
    }
    assert_eq!(
        state.routes.producer_ssrcs(media),
        Some([10_000.into()].as_slice())
    );
    assert_eq!(
        state
            .session_media
            .get(&session)
            .map(|lookup| lookup.producer_ssrcs.entries.len()),
        Some(1)
    );
    assert_eq!(state.src_media_for_ssrc(&session, 1.into()), None);
    assert_eq!(
        state.src_media_for_ssrc(&session, 10_000.into()),
        Some(media)
    );
}

#[test]
fn producer_encoding_rotation_bounds_primary_and_repair_indexes() {
    for rids in [vec![None], vec![Some("lo"), Some("mid"), Some("hi")]] {
        let session = test_transport_session_key(54, 0, 55, UserId::Integer(56));
        let source = ForwardedPacketSource::Relayed(session.clone());
        let mid = Mid::from("source");
        let mut state = PacketLoopState::default();
        let media = state.register_media_handle(RegisteredMediaHandle::Producer {
            session_key: session.clone(),
            mid,
        });
        let bindings = rids
            .iter()
            .map(|rid| {
                rid.map_or_else(StreamBinding::new, |rid| StreamBinding::new().with_rid(rid))
            })
            .collect();
        state.refresh_producer_ssrcs(
            &session,
            mid,
            &RouterRtpParameters::new(vec![], vec![], bindings),
        );
        for rotation in 1..=10_000_u32 {
            for (index, rid) in (0_u32..).zip(&rids) {
                let primary = (rotation * 10 + index).into();
                let binding = ProducerStreamBinding {
                    rid: rid.map(Rid::from),
                    primary,
                    repair: Some((rotation * 10 + index + 3).into()),
                };
                assert_ne!(
                    state.bind_producer_stream(&source, media, binding),
                    ProducerSsrcUpdate::Rejected
                );
                assert_eq!(state.source_rid_for_ssrc(&session, primary), binding.rid);
                assert!(
                    state
                        .routes
                        .producer_ssrcs(media)
                        .is_some_and(|ssrcs| ssrcs.len() <= rids.len() * 2)
                );
                assert!(
                    state.session_media.get(&session).is_some_and(|lookup| lookup.producer_ssrcs.entries.len() <= rids.len() * 2)
                );
            }
        }
        assert_eq!(
            state.routes.producer_ssrcs(media).map(<[Ssrc]>::len),
            Some(rids.len() * 2)
        );
        assert_eq!(state.src_media_for_ssrc(&session, 10.into()), None);
        assert_eq!(
            state.src_media_for_ssrc(&session, 100_000.into()),
            Some(media)
        );
        assert_eq!(
            state.src_media_for_ssrc(&session, 100_003.into()),
            Some(media)
        );
    }
}

#[test]
fn rejected_producer_binding_preserves_both_indexes() {
    let session = test_transport_session_key(57, 0, 58, UserId::Integer(59));
    let source = ForwardedPacketSource::Relayed(session.clone());
    let mid = Mid::from("source");
    let mut state = PacketLoopState::default();
    let media = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: session.clone(),
        mid,
    });
    state.refresh_producer_ssrcs(
        &session,
        mid,
        &RouterRtpParameters::new(
            vec![],
            vec![],
            vec![
                StreamBinding::new()
                    .with_ssrc(1)
                    .with_repair_ssrc(2)
                    .with_rid("hi"),
            ],
        ),
    );
    let current = ProducerStreamBinding {
        rid: Some("hi".into()),
        primary: 3.into(),
        repair: Some(4.into()),
    };
    assert_eq!(
        state.bind_producer_stream(&source, media, current),
        ProducerSsrcUpdate::Replaced
    );
    for rejected in [
        ProducerStreamBinding {
            primary: 1.into(),
            ..current
        },
        ProducerStreamBinding {
            rid: Some("unknown".into()),
            primary: 5.into(),
            ..current
        },
        ProducerStreamBinding {
            primary: 4.into(),
            ..current
        },
    ] {
        assert_eq!(
            state.bind_producer_stream(&source, media, rejected),
            ProducerSsrcUpdate::Rejected
        );
        assert_eq!(
            state.routes.producer_ssrcs(media),
            Some([3.into(), 4.into()].as_slice())
        );
        assert_eq!(
            state
                .session_media
                .get(&session)
                .map(|lookup| lookup.producer_ssrcs.entries.len()),
            Some(2)
        );
        assert_eq!(state.src_media_for_ssrc(&session, 1.into()), None);
        assert_eq!(state.src_media_for_ssrc(&session, 5.into()), None);
    }
}

#[test]
fn answer_refresh_keeps_preceding_ssrc_rejected_for_same_encoding() {
    let session = test_transport_session_key(58, 0, 59, UserId::Integer(60));
    let source = ForwardedPacketSource::Relayed(session.clone());
    let mid = Mid::from("source");
    let mut state = PacketLoopState::default();
    let media = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: session.clone(),
        mid,
    });
    let parameters = RouterRtpParameters::new(
        vec![],
        vec![],
        vec![StreamBinding::new().with_rid("hi").with_ssrc(1)],
    );
    state.refresh_producer_ssrcs(&session, mid, &parameters);
    let current = ProducerStreamBinding {
        rid: Some("hi".into()),
        primary: 2.into(),
        repair: None,
    };
    assert_eq!(
        state.bind_producer_stream(&source, media, current),
        ProducerSsrcUpdate::Replaced
    );
    let parameters = RouterRtpParameters::new(
        vec![],
        vec![],
        vec![StreamBinding::new().with_rid("hi").with_ssrc(2)],
    );
    state.refresh_answer_producer_ssrcs(&session, &[mid], &[(mid, parameters)]);
    assert_eq!(
        state.bind_producer_stream(
            &source,
            media,
            ProducerStreamBinding {
                primary: 1.into(),
                ..current
            }
        ),
        ProducerSsrcUpdate::Rejected
    );
    assert_eq!(state.src_media_for_ssrc(&session, 1.into()), None);
    assert_eq!(state.src_media_for_ssrc(&session, 2.into()), Some(media));
}

#[test]
fn answer_refresh_reassigns_ssrcs_between_producer_mids() {
    let session = test_transport_session_key(59, 0, 60, UserId::Integer(61));
    let first_mid = Mid::from("first");
    let second_mid = Mid::from("second");
    let mut state = PacketLoopState::default();
    let first_media = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: session.clone(),
        mid: first_mid,
    });
    let second_media = state.register_media_handle(RegisteredMediaHandle::Producer {
        session_key: session.clone(),
        mid: second_mid,
    });
    state.refresh_producer_ssrcs(&session, first_mid, &rtp_parameters_with_ssrc(first_mid, 1));
    state.refresh_producer_ssrcs(
        &session,
        second_mid,
        &rtp_parameters_with_ssrc(second_mid, 2),
    );
    state.refresh_answer_producer_ssrcs(
        &session,
        &[first_mid, second_mid],
        &[
            (first_mid, rtp_parameters_with_ssrc(first_mid, 2)),
            (second_mid, rtp_parameters_with_ssrc(second_mid, 1)),
        ],
    );
    assert_eq!(
        state.src_media_for_ssrc(&session, 1.into()),
        Some(second_media)
    );
    assert_eq!(
        state.src_media_for_ssrc(&session, 2.into()),
        Some(first_media)
    );
}
