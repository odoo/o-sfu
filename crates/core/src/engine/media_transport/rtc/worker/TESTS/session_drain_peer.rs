use std::{
    collections::VecDeque,
    net::SocketAddr,
    time::{Duration, Instant},
};

use o_sfu_rfc::{
    rtp::{self, RtpRtcpMuxPacketKind},
    webrtc::ice::transport,
};
use str0m::{
    Candidate, Event, Input, Output, Rtc,
    net::{Protocol, Receive, Transmit},
    rtp::{RawPacket, Ssrc, rtcp::Rtcp},
};

pub(super) struct TestDatagram {
    pub(super) source: SocketAddr,
    pub(super) destination: SocketAddr,
    pub(super) contents: Vec<u8>,
    proto: Protocol,
}

impl TestDatagram {
    pub(super) fn udp(source: SocketAddr, destination: SocketAddr, contents: Vec<u8>) -> Self {
        Self {
            source,
            destination,
            contents,
            proto: Protocol::Udp,
        }
    }

    pub(super) fn deliver(&self, rtc: &mut Rtc, now: Instant) -> Result<(), &'static str> {
        let receive = Receive::new(self.proto, self.source, self.destination, &self.contents)
            .map_err(|_error| "test datagram should parse")?;
        rtc.handle_input(Input::Receive(now, receive))
            .map_err(|_error| "test datagram should reach RTC")
    }

    fn is_rtcp(&self) -> bool {
        // RTP and RTCP sharing one port use the second octet to distinguish
        // RTCP feedback from media.
        // https://www.rfc-editor.org/rfc/rfc5761.html#section-4
        matches!(
            rtp::classify_rtp_rtcp_mux(&self.contents),
            Some(RtpRtcpMuxPacketKind::Rtcp)
        )
    }

    fn is_rtp(&self, ssrc: Ssrc, sequence_number: u16) -> bool {
        // The fixed RTP header supplies both stream and packet identity.
        // https://www.rfc-editor.org/rfc/rfc3550.html#section-5.1
        rtp::parse_muxed_rtp_fixed_header(&self.contents).is_some_and(|header| {
            header.ssrc().value() == *ssrc && header.sequence_number() == sequence_number
        })
    }
}

impl From<Transmit> for TestDatagram {
    fn from(transmit: Transmit) -> Self {
        Self {
            proto: transmit.proto,
            source: transmit.source,
            destination: transmit.destination,
            contents: Vec::from(transmit.contents),
        }
    }
}

fn poll_to_timeout(
    rtc: &mut Rtc,
    outgoing: &mut VecDeque<TestDatagram>,
) -> Result<Instant, &'static str> {
    loop {
        match rtc
            .poll_output()
            .map_err(|_error| "RTC output should poll")?
        {
            Output::Transmit(transmit) => outgoing.push_back(transmit.into()),
            Output::Event(_) => {}
            Output::Timeout(deadline) => return Ok(deadline),
        }
    }
}

pub(super) fn drain_mutation(rtc: &mut Rtc) -> Result<(), &'static str> {
    poll_to_timeout(rtc, &mut VecDeque::new()).map(|_deadline| ())
}

pub(super) fn take_written_rtp(
    rtc: &mut Rtc,
    now: Instant,
    ssrc: Ssrc,
    sequence_number: u16,
) -> Result<TestDatagram, &'static str> {
    let mut outgoing = VecDeque::new();
    loop {
        let deadline = poll_to_timeout(rtc, &mut outgoing)?;
        if deadline > now {
            break;
        }
        rtc.handle_input(Input::Timeout(now))
            .map_err(|_error| "sender timeout should apply")?;
    }
    let mut packets = outgoing
        .into_iter()
        .filter(|packet| packet.is_rtp(ssrc, sequence_number));
    let packet = packets.next().ok_or("written RTP should be transmitted")?;
    if packets.next().is_some() {
        return Err("written RTP should have one matching transmit");
    }
    Ok(packet)
}

pub(super) fn deliver_rtp(
    rtc: &mut Rtc,
    datagram: &TestDatagram,
    now: Instant,
) -> Result<(), &'static str> {
    datagram.deliver(rtc, now)?;
    let mut discarded = VecDeque::new();
    loop {
        let deadline = poll_to_timeout(rtc, &mut discarded)?;
        if deadline > now {
            return Ok(());
        }
        rtc.handle_input(Input::Timeout(now))
            .map_err(|_error| "receiver timeout should apply")?;
    }
}

pub(super) fn take_rtcp(rtc: &mut Rtc, now: Instant) -> Result<Vec<TestDatagram>, &'static str> {
    let mut outgoing = VecDeque::new();
    loop {
        let deadline = poll_to_timeout(rtc, &mut outgoing)?;
        if deadline > now {
            break;
        }
        rtc.handle_input(Input::Timeout(now))
            .map_err(|_error| "RTC timeout should apply")?;
    }
    Ok(outgoing.into_iter().filter(TestDatagram::is_rtcp).collect())
}

pub(super) fn connect_rtc_pair(
    server: &mut Rtc,
    peer: &mut Rtc,
    server_addr: SocketAddr,
    peer_addr: SocketAddr,
    mut now: Instant,
) -> Result<Instant, &'static str> {
    let server_candidate = Candidate::host(server_addr, transport::UDP)
        .map_err(|_error| "server candidate should build")?;
    let peer_candidate = Candidate::host(peer_addr, transport::UDP)
        .map_err(|_error| "peer candidate should build")?;
    server.add_remote_candidate(peer_candidate.clone());
    peer.add_local_candidate(peer_candidate)
        .ok_or("peer candidate should register")?;
    peer.add_remote_candidate(server_candidate);

    let server_fingerprint = server.direct_api().local_dtls_fingerprint().clone();
    let peer_fingerprint = peer.direct_api().local_dtls_fingerprint().clone();
    server.direct_api().set_remote_fingerprint(peer_fingerprint);
    peer.direct_api().set_remote_fingerprint(server_fingerprint);
    let server_credentials = server.direct_api().local_ice_credentials();
    let peer_credentials = peer.direct_api().local_ice_credentials();
    server
        .direct_api()
        .set_remote_ice_credentials(peer_credentials);
    peer.direct_api()
        .set_remote_ice_credentials(server_credentials);
    peer.direct_api().set_ice_controlling(true);
    server.direct_api().set_ice_controlling(false);
    peer.direct_api()
        .start_dtls(true)
        .map_err(|_error| "peer DTLS should start")?;
    server
        .direct_api()
        .start_dtls(false)
        .map_err(|_error| "server DTLS should start")?;

    server
        .handle_input(Input::Timeout(now))
        .map_err(|_error| "server clock should start")?;
    peer.handle_input(Input::Timeout(now))
        .map_err(|_error| "peer clock should start")?;

    let mut to_server = VecDeque::new();
    let mut to_peer = VecDeque::new();
    let mut server_deadline = poll_to_timeout(server, &mut to_peer)?;
    let mut peer_deadline = poll_to_timeout(peer, &mut to_server)?;

    for _ in 0..4096 {
        if server.is_connected() && peer.is_connected() {
            return Ok(now);
        }
        if let Some(datagram) = to_server.pop_front() {
            datagram.deliver(server, now)?;
            server_deadline = poll_to_timeout(server, &mut to_peer)?;
            continue;
        }
        if let Some(datagram) = to_peer.pop_front() {
            datagram.deliver(peer, now)?;
            peer_deadline = poll_to_timeout(peer, &mut to_server)?;
            continue;
        }
        if server_deadline <= peer_deadline {
            now = next_test_time(now, server_deadline);
            server
                .handle_input(Input::Timeout(now))
                .map_err(|_error| "server timeout should apply")?;
            server_deadline = poll_to_timeout(server, &mut to_peer)?;
        } else {
            now = next_test_time(now, peer_deadline);
            peer.handle_input(Input::Timeout(now))
                .map_err(|_error| "peer timeout should apply")?;
            peer_deadline = poll_to_timeout(peer, &mut to_server)?;
        }
    }
    Err("RTC pair should connect")
}

fn next_test_time(now: Instant, deadline: Instant) -> Instant {
    if deadline <= now {
        now + Duration::from_millis(1)
    } else {
        deadline
    }
}

pub(super) struct CapturedNack {
    pub(super) datagram: TestDatagram,
    pub(super) reports: Vec<(u32, u16, u16)>,
}

pub(super) fn capture_compound_nack(
    peer: &mut Rtc,
    nack_at: Instant,
    expected_packets: usize,
    expected_requests: u32,
) -> Result<CapturedNack, &'static str> {
    peer.handle_input(Input::Timeout(nack_at))
        .map_err(|_error| "peer NACK timeout should apply")?;
    let mut requested = 0;
    let mut nack_packets = 0;
    let mut reports = Vec::new();
    let mut transmits = Vec::new();
    loop {
        match peer
            .poll_output()
            .map_err(|_error| "peer feedback should poll")?
        {
            Output::Transmit(transmit) => transmits.push(TestDatagram::from(transmit)),
            Output::Event(Event::RawPacket(packet)) => {
                if let RawPacket::RtcpTx(Rtcp::Nack(nack)) = packet.as_ref() {
                    nack_packets += 1;
                    reports.extend(
                        nack.reports
                            .iter()
                            .map(|entry| (*nack.ssrc, entry.pid, entry.blp)),
                    );
                    requested += nack
                        .reports
                        .iter()
                        .map(|entry| 1 + entry.blp.count_ones())
                        .sum::<u32>();
                }
            }
            Output::Event(_) => {}
            Output::Timeout(_) => break,
        }
    }
    if nack_packets != expected_packets || requested != expected_requests {
        return Err("peer should emit the expected Generic NACKs");
    }
    let mut feedback = transmits.into_iter().filter(TestDatagram::is_rtcp);
    let datagram = feedback
        .next()
        .ok_or("peer should emit compound SRTCP NACK")?;
    if feedback.next().is_some() {
        return Err("compound SRTCP NACK should fit one datagram");
    }
    Ok(CapturedNack { datagram, reports })
}
