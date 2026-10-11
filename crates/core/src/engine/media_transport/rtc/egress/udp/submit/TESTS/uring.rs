use std::net::{Ipv4Addr, SocketAddr};

use str0m::net::{Protocol, Transmit};

#[test]
fn groups_stop_at_repeat_or_limit() {
    for (destinations, expected) in [
        (vec![1, 2, 1], (2, 2)),
        (vec![1, 1], (2, 1)),
        (vec![1], (1, 1)),
        (vec![1, 1, 2, 2, 1], (4, 2)),
        ((1..=33).collect(), (32, 32)),
        (vec![1; 64], (64, 1)),
        ((1..=10).flat_map(|port| [port; 4]).collect(), (40, 10)),
    ] {
        assert_eq!(
            super::group_size(&transmits(&destinations)),
            expected,
            "{destinations:?}"
        );
    }
}

fn transmits(destinations: &[u16]) -> Vec<Transmit> {
    destinations
        .iter()
        .map(|port| Transmit {
            proto: Protocol::Udp,
            source: SocketAddr::from((Ipv4Addr::LOCALHOST, 10000)),
            destination: SocketAddr::from((Ipv4Addr::LOCALHOST, *port)),
            contents: vec![].into(),
        })
        .collect()
}
