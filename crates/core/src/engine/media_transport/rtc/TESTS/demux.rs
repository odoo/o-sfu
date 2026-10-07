use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use super::RemoteAddrDemux;
use crate::engine::{
    UserId,
    media_transport::{TransportSessionKey, rtc::test_support::test_transport_session_key},
};

fn session_key(room_instance_id: u64, session_numeric_id: i64) -> TransportSessionKey {
    test_transport_session_key(0, 0, room_instance_id, UserId::Integer(session_numeric_id))
}

#[test]
fn remember_remote_addr_reports_stable_mapping_without_churn() {
    let mut demux = RemoteAddrDemux::default();
    let source_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 46_001);
    let session_key = session_key(9, 3);

    assert!(demux.remember_remote_addr(source_addr, &session_key));
    assert!(!demux.remember_remote_addr(source_addr, &session_key));
    assert_eq!(
        demux.session_key_for_remote_addr(source_addr),
        Some(&session_key)
    );
    assert_eq!(
        demux.session_addrs_for(&session_key),
        Some([source_addr].as_slice())
    );
}

#[test]
fn replace_remote_candidates_deduplicates_and_cleans_previous_entries() {
    let mut demux = RemoteAddrDemux::default();
    let first_session = session_key(9, 3);
    let second_session = session_key(9, 4);
    let first_candidate = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 46_001);
    let second_candidate = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 46_002);

    demux.replace_remote_candidates(
        &first_session,
        [first_candidate, first_candidate, second_candidate],
    );
    demux.replace_remote_candidates(&second_session, [second_candidate]);

    assert_eq!(
        demux.remote_candidate_addrs_for(&first_session),
        Some([first_candidate, second_candidate].as_slice())
    );
    assert_eq!(
        demux.candidates_for_src_addr(second_candidate),
        Some([first_session.clone(), second_session.clone()].as_slice())
    );

    demux.replace_remote_candidates(&first_session, [first_candidate]);

    assert_eq!(
        demux.remote_candidate_addrs_for(&first_session),
        Some([first_candidate].as_slice())
    );
    assert_eq!(
        demux.candidates_for_src_addr(second_candidate),
        Some([second_session].as_slice())
    );
}

#[test]
fn learned_source_cap_preserves_selected_pair_and_spare_for_ice_change() {
    use super::MAX_REMOTE_ADDRS_PER_SESSION;
    let mut demux = RemoteAddrDemux::default();
    let session = session_key(9, 3);
    let selected = SocketAddr::from((Ipv4Addr::LOCALHOST, 47_000));
    demux.remember_selected_remote_addr(&session, selected);
    for offset in 1..MAX_REMOTE_ADDRS_PER_SESSION + 32 {
        let source = SocketAddr::from((
            Ipv4Addr::LOCALHOST,
            47_000 + u16::try_from(offset).unwrap_or(0),
        ));
        assert!(demux.remember_remote_addr(source, &session));
        assert!(
            demux
                .session_addrs_for(&session)
                .is_some_and(|addrs| addrs.len() <= MAX_REMOTE_ADDRS_PER_SESSION)
        );
    }
    assert_eq!(demux.session_key_for_remote_addr(selected), Some(&session));
    assert!(
        demux
            .session_key_for_remote_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, 47_001)))
            .is_none()
    );
    let next_selected = SocketAddr::from((Ipv4Addr::LOCALHOST, 48_000));
    demux.remember_selected_remote_addr(&session, next_selected);
    assert_eq!(
        demux.session_key_for_remote_addr(next_selected),
        Some(&session)
    );
    for offset in 0..MAX_REMOTE_ADDRS_PER_SESSION {
        let source = SocketAddr::from((
            Ipv4Addr::LOCALHOST,
            48_001 + u16::try_from(offset).unwrap_or(0),
        ));
        assert!(demux.remember_remote_addr(source, &session));
    }
    assert!(demux.session_key_for_remote_addr(selected).is_none());
    assert_eq!(
        demux.session_key_for_remote_addr(next_selected),
        Some(&session)
    );
    assert_eq!(demux.overflowed_session_count(), 1);
    demux.forget_user_remote_addrs(&session);
    assert_eq!(demux.overflowed_session_count(), 0);
}

#[test]
fn selected_pin_remap_and_teardown_clear_both_indexes() {
    let mut demux = RemoteAddrDemux::default();
    let first = session_key(9, 3);
    let second = session_key(9, 4);
    let selected = SocketAddr::from((Ipv4Addr::LOCALHOST, 49_000));
    demux.remember_selected_remote_addr(&first, selected);
    assert!(demux.remember_remote_addr(selected, &second));
    assert!(demux.session_addrs_for(&first).is_none());
    assert_eq!(
        demux.session_addrs_for(&second),
        Some([selected].as_slice())
    );
    demux.forget_user_remote_addrs(&second);
    assert!(demux.session_key_for_remote_addr(selected).is_none());
    assert!(demux.session_addrs_for(&second).is_none());
}
