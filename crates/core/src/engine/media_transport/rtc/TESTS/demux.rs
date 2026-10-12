use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use str0m::net::Protocol;

use super::{RemoteAddrDemux, RemoteTransportAddr};
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

    assert!(demux.remember_remote_addr(RemoteTransportAddr::udp(source_addr), &session_key));
    assert!(!demux.remember_remote_addr(RemoteTransportAddr::udp(source_addr), &session_key));
    assert_eq!(
        demux.session_key_for_remote_addr(RemoteTransportAddr::udp(source_addr)),
        Some(&session_key)
    );
    assert_eq!(
        demux.session_addrs_for(&session_key),
        Some([RemoteTransportAddr::udp(source_addr)].as_slice())
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
        [first_candidate, first_candidate, second_candidate].map(RemoteTransportAddr::udp),
    );
    demux.replace_remote_candidates(
        &second_session,
        [second_candidate].map(RemoteTransportAddr::udp),
    );

    assert_eq!(
        demux.remote_candidate_addrs_for(&first_session),
        Some(
            [
                RemoteTransportAddr::udp(first_candidate),
                RemoteTransportAddr::udp(second_candidate)
            ]
            .as_slice()
        )
    );
    assert_eq!(
        demux.candidates_for_src_addr(RemoteTransportAddr::udp(second_candidate)),
        Some([first_session.clone(), second_session.clone()].as_slice())
    );

    demux.replace_remote_candidates(
        &first_session,
        [first_candidate].map(RemoteTransportAddr::udp),
    );

    assert_eq!(
        demux.remote_candidate_addrs_for(&first_session),
        Some([RemoteTransportAddr::udp(first_candidate)].as_slice())
    );
    assert_eq!(
        demux.candidates_for_src_addr(RemoteTransportAddr::udp(second_candidate)),
        Some([second_session].as_slice())
    );
}

#[test]
fn learned_source_cap_preserves_selected_pair_and_spare_for_ice_change() {
    use super::MAX_REMOTE_ADDRS_PER_SESSION;
    let mut demux = RemoteAddrDemux::default();
    let session = session_key(9, 3);
    let selected = SocketAddr::from((Ipv4Addr::LOCALHOST, 47_000));
    demux.remember_selected_remote_addr(&session, RemoteTransportAddr::udp(selected));
    for offset in 1..MAX_REMOTE_ADDRS_PER_SESSION + 32 {
        let source = SocketAddr::from((
            Ipv4Addr::LOCALHOST,
            47_000 + u16::try_from(offset).unwrap_or(0),
        ));
        assert!(demux.remember_remote_addr(RemoteTransportAddr::udp(source), &session));
        assert!(
            demux
                .session_addrs_for(&session)
                .is_some_and(|addrs| addrs.len() <= MAX_REMOTE_ADDRS_PER_SESSION)
        );
    }
    assert_eq!(
        demux.session_key_for_remote_addr(RemoteTransportAddr::udp(selected)),
        Some(&session)
    );
    assert!(
        demux
            .session_key_for_remote_addr(RemoteTransportAddr::udp(SocketAddr::from((
                Ipv4Addr::LOCALHOST,
                47_001,
            ))))
            .is_none()
    );
    let next_selected = SocketAddr::from((Ipv4Addr::LOCALHOST, 48_000));
    demux.remember_selected_remote_addr(&session, RemoteTransportAddr::udp(next_selected));
    assert_eq!(
        demux.session_key_for_remote_addr(RemoteTransportAddr::udp(next_selected)),
        Some(&session)
    );
    for offset in 0..MAX_REMOTE_ADDRS_PER_SESSION {
        let source = SocketAddr::from((
            Ipv4Addr::LOCALHOST,
            48_001 + u16::try_from(offset).unwrap_or(0),
        ));
        assert!(demux.remember_remote_addr(RemoteTransportAddr::udp(source), &session));
    }
    assert!(
        demux
            .session_key_for_remote_addr(RemoteTransportAddr::udp(selected))
            .is_none()
    );
    assert_eq!(
        demux.session_key_for_remote_addr(RemoteTransportAddr::udp(next_selected)),
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
    demux.remember_selected_remote_addr(&first, RemoteTransportAddr::udp(selected));
    assert!(demux.remember_remote_addr(RemoteTransportAddr::udp(selected), &second));
    assert!(demux.session_addrs_for(&first).is_none());
    assert_eq!(
        demux.session_addrs_for(&second),
        Some([RemoteTransportAddr::udp(selected)].as_slice())
    );
    demux.forget_user_remote_addrs(&second);
    assert!(
        demux
            .session_key_for_remote_addr(RemoteTransportAddr::udp(selected))
            .is_none()
    );
    assert!(demux.session_addrs_for(&second).is_none());
}

#[test]
fn same_address_udp_tcp_keep_different_pins() {
    let mut demux = RemoteAddrDemux::default();
    let tcp_session_key = session_key(9, 3);
    let udp_session_key = session_key(9, 4);
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 49_000));
    let udp_remote_addr = RemoteTransportAddr::udp(addr);
    let tcp_remote_addr = RemoteTransportAddr::new(addr, Protocol::Tcp);
    assert!(demux.remember_remote_addr(udp_remote_addr, &udp_session_key));
    assert!(demux.remember_remote_addr(tcp_remote_addr, &tcp_session_key));
    assert_eq!(
        demux.session_key_for_remote_addr(udp_remote_addr),
        Some(&udp_session_key)
    );
    assert_eq!(
        demux.session_key_for_remote_addr(tcp_remote_addr),
        Some(&tcp_session_key)
    );
    assert_eq!(
        demux.session_addrs_for(&udp_session_key),
        Some([udp_remote_addr].as_slice())
    );
    assert_eq!(
        demux.session_addrs_for(&tcp_session_key),
        Some([tcp_remote_addr].as_slice())
    );
    demux.forget_remote_addr(udp_remote_addr);
    assert!(demux.session_key_for_remote_addr(udp_remote_addr).is_none());
    assert_eq!(
        demux.session_key_for_remote_addr(tcp_remote_addr),
        Some(&tcp_session_key)
    );
}

#[test]
fn same_address_udp_tcp_keep_different_candidate_hints() {
    let mut demux = RemoteAddrDemux::default();
    let udp_session_key = session_key(9, 3);
    let tcp_session_key = session_key(9, 4);
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 49_001));
    let udp_remote_addr = RemoteTransportAddr::udp(addr);
    let tcp_remote_addr = RemoteTransportAddr::new(addr, Protocol::Tcp);
    demux.replace_remote_candidates(&udp_session_key, [udp_remote_addr]);
    demux.replace_remote_candidates(&tcp_session_key, [tcp_remote_addr]);
    assert_eq!(
        demux.candidates_for_src_addr(udp_remote_addr),
        Some([udp_session_key].as_slice())
    );
    assert_eq!(
        demux.candidates_for_src_addr(tcp_remote_addr),
        Some([tcp_session_key].as_slice())
    );
}
