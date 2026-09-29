//! ICE-TCP cheks
//!
//! An ICE-lite server from [`bootstrap`] offers UDP and passive TCP host
//! candidates. A full-ICE peer with one active TCP candidate answers without
//! candidates, so the server learns the peer address from incoming checks.
//!
//! [`Rtc::accepts`] returns `true` for any input from the nominated remote
//! address regardless of protocol or STUN integrity. [`Rtc::handle_input`]
//! still drops wrong-integrity requests. TCP ingress must bind each packet to
//! its accepted connection instead of relying on `accepts()` for
//! authentication or transport identity.
//!
//! RFC 4571 framing, sockets, pre-answer checks, role conflicts, reconnection
//! and browser interoperability are out of scope.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::{Duration, Instant},
};

use o_sfu_rfc::webrtc::{
    IceTransport,
    ice::{candidate_attribute, candidate_type},
};
use str0m::{
    Candidate, Event, IceConnectionState, Input, Output, Rtc,
    change::{SdpAnswer, SdpOffer, SdpPendingOffer},
    ice::{StunMessage, TransId},
    media::{Direction, MediaKind},
    net::{Protocol, Receive, TcpType, Transmit},
};

use super::super::{
    bootstrap,
    state::slots::SessionStore,
    test_support::{serialize_stun_message, test_transport_session_key},
};
use crate::{Bitrate, engine::UserId};

const UDP_PORT: u16 = 5000;
const TCP_PORT: u16 = 5001;
const REMOTE_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 55_000);
const FLOW_LIMIT: Duration = Duration::from_secs(30);
const MAX_FIXTURE_STEPS: usize = 10_000;

#[test]
fn offer_preserves_candidates_and_priorities() {
    for tcp_port in [UDP_PORT, TCP_PORT] {
        let mut server = server_rtc(Some(tcp_port));
        let (offer, _) = audio_offer(&mut server);
        let sdp = offer.to_sdp_string();
        assert!(sdp.lines().any(|line| line == "a=ice-lite"));
        let candidates: Vec<_> = sdp
            .lines()
            .filter_map(|line| line.strip_prefix("a=candidate:"))
            .map(|line| line.split_whitespace().collect::<Vec<_>>())
            .collect();
        assert_eq!(candidates.len(), 2, "{sdp}");
        let udp = candidates
            .iter()
            .find(|line| IceTransport::parse(line[2]) == Some(IceTransport::Udp))
            .expect("UDP candidate");
        let tcp = candidates
            .iter()
            .find(|line| IceTransport::parse(line[2]) == Some(IceTransport::Tcp))
            .expect("TCP candidate");
        for (candidate, port) in [(udp, UDP_PORT), (tcp, tcp_port)] {
            assert_eq!(candidate[1], "1");
            assert_eq!(candidate[4], "127.0.0.1");
            assert_eq!(candidate[5].parse::<u16>().expect("candidate port"), port);
            assert_eq!(
                &candidate[6..8],
                &[candidate_attribute::TYPE_LABEL, candidate_type::HOST],
            );
        }
        assert!(!udp.contains(&"tcptype"));
        assert_eq!(&tcp[8..10], &["tcptype", "passive"]);
        let udp_priority = udp[3].parse::<u32>().expect("UDP priority");
        let tcp_priority = tcp[3].parse::<u32>().expect("TCP priority");
        assert!(
            udp_priority > tcp_priority,
            "{udp_priority} <= {tcp_priority}"
        );
    }
}

#[test]
fn binding_responses_match_transport() {
    for tcp_port in [UDP_PORT, TCP_PORT] {
        let mut pair = Pair::new(Some(tcp_port));
        let password = pair.server.rtc.direct_api().local_ice_credentials().pass;
        let (packet, transaction) = pair.binding_packet(&password);
        for (protocol, port) in [(Protocol::Tcp, tcp_port), (Protocol::Udp, UDP_PORT)] {
            let destination = server_addr(port);
            let input = Input::Receive(
                pair.now,
                Receive::new(protocol, REMOTE_ADDR, destination, &packet).expect("STUN input"),
            );
            assert!(pair.server.rtc.accepts(&input));
            pair.server
                .rtc
                .handle_input(input)
                .expect("binding request");
            let responses = pair.server.drain(pair.now);
            assert_binding_response(&responses, protocol, destination, transaction);
        }
    }
    let mut pair = Pair::new(None);
    let password = pair.server.rtc.direct_api().local_ice_credentials().pass;
    let (packet, _) = pair.binding_packet(&password);
    let input = Input::Receive(
        pair.now,
        Receive::new(Protocol::Tcp, REMOTE_ADDR, server_addr(UDP_PORT), &packet)
            .expect("STUN input"),
    );
    assert!(
        pair.server.rtc.accepts(&input),
        "credentials identify the session"
    );
    pair.server
        .rtc
        .handle_input(input)
        .expect("unmatched candidate request");
    assert!(pair.server.drain(pair.now).is_empty());
}

#[test]
fn tcp_only_peer_connects() {
    let mut pair = Pair::new(Some(TCP_PORT));
    pair.connect();
    assert!(pair.server.ice.is_connected());
    assert!(pair.remote.ice.is_connected());
    assert!(
        pair.server.connected && pair.remote.connected,
        "ICE and DTLS events"
    );
    assert!(pair.server.rtc.is_connected() && pair.remote.rtc.is_connected());
    assert!(!pair.server.nominated_routes.is_empty());
    for route in pair.server.nominated_routes {
        assert_eq!(route, (Protocol::Tcp, server_addr(TCP_PORT), REMOTE_ADDR));
    }
}

#[test]
fn nominated_source_bypasses_accepts_authentication() {
    let mut pair = Pair::new(Some(TCP_PORT));
    pair.connect();
    let (packet, _) = pair.binding_packet("wrong-password");
    for protocol in [Protocol::Tcp, Protocol::Udp] {
        let input = Input::Receive(
            pair.now,
            Receive::new(protocol, REMOTE_ADDR, server_addr(TCP_PORT), &packet)
                .expect("wrong-integrity STUN input"),
        );
        assert!(
            pair.server.rtc.accepts(&input),
            "nominated source with {protocol:?}"
        );
        pair.server
            .rtc
            .handle_input(input)
            .expect("wrong-integrity request");
        assert!(
            pair.server.drain(pair.now).is_empty(),
            "wrong integrity must not get a reply"
        );
    }
    let other_source = SocketAddr::from(([127, 0, 0, 3], REMOTE_ADDR.port()));
    let input = Input::Receive(
        pair.now,
        Receive::new(Protocol::Tcp, other_source, server_addr(TCP_PORT), &packet)
            .expect("unrecognized source"),
    );
    assert!(!pair.server.rtc.accepts(&input));
    let password = pair.server.rtc.direct_api().local_ice_credentials().pass;
    let (packet, transaction) = pair.binding_packet(&password);
    let input = Input::Receive(
        pair.now,
        Receive::new(Protocol::Tcp, REMOTE_ADDR, server_addr(TCP_PORT), &packet)
            .expect("valid STUN input"),
    );
    pair.server
        .rtc
        .handle_input(input)
        .expect("valid request after rejection");
    let responses = pair.server.drain(pair.now);
    assert_binding_response(
        &responses,
        Protocol::Tcp,
        server_addr(TCP_PORT),
        transaction,
    );
}

fn server_addr(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

fn server_rtc(tcp_port: Option<u16>) -> Rtc {
    let mut sessions = SessionStore::default();
    let key = test_transport_session_key(1, 0, 1, UserId::Integer(1));
    assert!(
        bootstrap::test_support::ensure_session_rtc_state(
            &mut sessions,
            &key,
            server_addr(UDP_PORT),
            Bitrate::from_mbps(10),
        )
        .expect("server bootstrap")
    );
    let mut rtc = sessions.remove(&key).expect("bootstrapped session").rtc;
    if let Some(port) = tcp_port {
        rtc.add_local_candidate(
            Candidate::builder()
                .tcp()
                .host(server_addr(port))
                .tcptype(TcpType::Passive)
                .build()
                .expect("passive TCP candidate"),
        )
        .expect("TCP candidate must coexist with UDP");
    }
    rtc
}

fn audio_offer(server: &mut Rtc) -> (SdpOffer, SdpPendingOffer) {
    let mut changes = server.sdp_api();
    changes.add_media(MediaKind::Audio, Direction::SendRecv, None, None, None);
    changes.apply().expect("audio offer")
}

fn assert_binding_response(
    responses: &[Transmit],
    protocol: Protocol,
    source: SocketAddr,
    transaction: TransId,
) {
    assert_eq!(responses.len(), 1, "{responses:?}");
    let response = &responses[0];
    let stun = StunMessage::parse(&response.contents).expect("STUN response");
    assert!(stun.is_successful_binding_response());
    assert_eq!(
        (
            response.proto,
            response.source,
            response.destination,
            stun.trans_id()
        ),
        (protocol, source, REMOTE_ADDR, transaction),
    );
}

struct Peer {
    rtc: Rtc,
    ice: IceConnectionState,
    connected: bool,
    deadline: Instant,
    nominated_routes: Vec<(Protocol, SocketAddr, SocketAddr)>,
}

impl Peer {
    fn new(rtc: Rtc, now: Instant) -> Self {
        Self {
            rtc,
            ice: IceConnectionState::New,
            connected: false,
            deadline: now,
            nominated_routes: Vec::new(),
        }
    }

    fn drain(&mut self, now: Instant) -> Vec<Transmit> {
        let mut packets = Vec::new();
        for _ in 0..MAX_FIXTURE_STEPS {
            match self.rtc.poll_output().expect("RTC output") {
                Output::Timeout(deadline) if deadline <= now => {
                    self.rtc
                        .handle_input(Input::Timeout(now))
                        .expect("immediate timeout");
                }
                Output::Timeout(deadline) => {
                    self.deadline = deadline;
                    return packets;
                }
                Output::Event(Event::IceConnectionStateChange(state)) => self.ice = state,
                Output::Event(Event::Connected) => self.connected = true,
                Output::Event(_) => {}
                Output::Transmit(packet) => {
                    if self.ice.is_connected() {
                        self.nominated_routes.push((
                            packet.proto,
                            packet.source,
                            packet.destination,
                        ));
                    }
                    packets.push(packet);
                }
            }
        }
        panic!(
            "RTC did not reach a future timeout, last ICE state: {:?}",
            self.ice
        );
    }
}

struct Pair {
    server: Peer,
    remote: Peer,
    now: Instant,
    last_packet: String,
}

impl Pair {
    fn new(tcp_port: Option<u16>) -> Self {
        let mut server = server_rtc(tcp_port);
        let now = Instant::now();
        let mut remote = Rtc::builder().set_ice_lite(false).build(now);
        remote
            .add_local_candidate(
                Candidate::builder()
                    .tcp()
                    .host(REMOTE_ADDR)
                    .tcptype(TcpType::Active)
                    .build()
                    .expect("active TCP candidate"),
            )
            .expect("remote TCP candidate");
        let (offer, pending) = audio_offer(&mut server);
        let answer = remote.sdp_api().accept_offer(offer).expect("remote answer");
        // The client can answer before gathering finishes, so learn the TCP peer from checks.
        let mut answer_sdp = answer
            .to_sdp_string()
            .lines()
            .filter(|line| !line.starts_with("a=candidate:"))
            .collect::<Vec<_>>()
            .join("\r\n");
        answer_sdp.push_str("\r\n");
        assert!(
            !answer_sdp.contains("a=candidate:"),
            "TCP peer must be learned from its checks rather than the answer",
        );
        let answer = SdpAnswer::from_sdp_string(&answer_sdp).expect("candidate-free answer");
        server
            .sdp_api()
            .accept_answer(pending, answer)
            .expect("server accepts answer");
        let mut pair = Self {
            server: Peer::new(server, now),
            remote: Peer::new(remote, now),
            now,
            last_packet: String::from("none"),
        };
        assert!(
            pair.server.drain(now).is_empty(),
            "ICE-lite awaits peer checks"
        );
        pair
    }

    fn binding_packet(&mut self, password: &str) -> (Vec<u8>, TransId) {
        let server = self.server.rtc.direct_api().local_ice_credentials();
        let remote = self.remote.rtc.direct_api().local_ice_credentials();
        let username = format!("{}:{}", server.ufrag, remote.ufrag);
        let transaction = TransId::new();
        let request = StunMessage::binding_request(&username, transaction, true, 1, 1, false);
        let packet = serialize_stun_message(&request, Some(password.as_bytes()))
            .expect("authenticated binding request");
        (packet, transaction)
    }

    fn connect(&mut self) {
        let deadline = self.now + FLOW_LIMIT;
        let mut to_server = self.remote.drain(self.now);
        let mut to_remote = Vec::new();
        for _ in 0..MAX_FIXTURE_STEPS {
            for packet in to_server.drain(..) {
                let receive = Receive::try_from(&packet).expect("server packet parse");
                self.last_packet = format!("remote -> server: {receive:?}");
                let input = Input::Receive(self.now, receive);
                assert!(self.server.rtc.accepts(&input), "{}", self.last_packet);
                self.server
                    .rtc
                    .handle_input(input)
                    .expect("server packet input");
                to_remote.extend(self.server.drain(self.now));
            }
            for packet in to_remote.drain(..) {
                let receive = Receive::try_from(&packet).expect("remote packet parse");
                self.last_packet = format!("server -> remote: {receive:?}");
                let input = Input::Receive(self.now, receive);
                assert!(self.remote.rtc.accepts(&input), "{}", self.last_packet);
                self.remote
                    .rtc
                    .handle_input(input)
                    .expect("remote packet input");
                to_server.extend(self.remote.drain(self.now));
            }
            if !to_server.is_empty() {
                continue;
            }
            if self.server.connected && self.remote.connected {
                return;
            }
            self.now = self.server.deadline.min(self.remote.deadline);
            if self.now > deadline {
                break;
            }
            self.server
                .rtc
                .handle_input(Input::Timeout(self.now))
                .expect("server timeout");
            to_remote.extend(self.server.drain(self.now));
            self.remote
                .rtc
                .handle_input(Input::Timeout(self.now))
                .expect("remote timeout");
            to_server.extend(self.remote.drain(self.now));
        }
        panic!(
            "TCP flow stalled: server ICE {:?}, remote ICE {:?}, DTLS connected ({}, {}), last packet {}",
            self.server.ice,
            self.remote.ice,
            self.server.connected,
            self.remote.connected,
            self.last_packet,
        );
    }
}
