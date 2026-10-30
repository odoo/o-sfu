//! RTC over TCP through the real acceptor.

use std::{
    io::ErrorKind,
    net::{SocketAddr, TcpListener},
    time::{Duration, Instant},
};

use o_sfu_model::UserId;
use o_sfu_rfc::tcp_framing::{TcpFrame, TcpFrameDecoder};
use str0m::{
    Candidate, Input, Rtc,
    change::SdpOffer,
    ice,
    net::{Protocol, Receive, TcpType},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    select,
    time::{self, sleep, sleep_until, timeout},
};

use crate::{
    RtcTcpConfig,
    engine::media_transport::{
        TransportTeardown,
        rtc::{
            tcp_acceptor::{TCP_RESOLVE_UFRAG_MAX, TcpFailure, take_closure_reason},
            test_support::{Peer, serialize_stun_message},
        },
    },
    server::transport::{
        MediaTransport, TransportSessionKey,
        test_support::{
            test_media_transport_config, test_media_transport_deps, test_rtc_port_range,
            test_transport_session_key,
        },
    },
};

const HANDSHAKE_DEADLINE: Duration = Duration::from_secs(10);
const WRONG_ICE_PASSWORD: &[u8] = b"wrong-ice-password";
const CLOSURE_REPORT_DEADLINE: Duration = Duration::from_secs(5);

/// Creates a `MediaTransport` with the TCP acceptor enabled and started.
///
/// Returns the address clients connect to. The caller must keep the transport alive:
/// dropping it stops the acceptor and closes every stream.
fn tcp_transport(max_conn: Option<usize>) -> (SocketAddr, MediaTransport) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .expect("should be able to bind listener to available port");
    let addr = listener
        .local_addr()
        .expect("should be able to access local addr of the listener");
    drop(listener);
    let mut config = test_media_transport_config(1, test_rtc_port_range());
    config.rtc_tcp_config = Some(RtcTcpConfig {
        bind_addr: addr,
        announced_addr: addr,
        max_conn: max_conn.unwrap_or(1024),
    });
    let transport = MediaTransport::build(config, test_media_transport_deps())
        .expect("should be able to start test media transport");
    (addr, transport)
}

/// Writes a `TcpFrame` (length prefix and payload) to the given `TcpStream`.
///
/// # Panics
///
/// Panics when the write fails.
async fn write_tcp_frame(stream: &mut TcpStream, frame: TcpFrame) {
    let slices = frame.io_slices();
    let mut bytes = Vec::new();
    for slice in &slices {
        bytes.extend_from_slice(slice);
    }
    stream
        .write_all(&bytes)
        .await
        .expect("should be able to send bytes on the stream");
}

async fn create_session_offer(transport: &MediaTransport) -> (TransportSessionKey, String) {
    let session_key = test_transport_session_key(45, 0, 12, UserId::Integer(9));
    let offer = transport
        .create_initial_session_offer("test-room", &session_key)
        .await
        .expect("should be able to create test offer")
        .into_parts()
        .0;
    (session_key, offer)
}

/// A STUN binding request framed for TCP, signed with `password`. Only a request signed with
/// the session's ICE password gets a reply.
fn binding_request_frame(username: &str, password: &[u8]) -> TcpFrame {
    let stun_msg = serialize_stun_message(
        &ice::StunMessage::binding_request(username, ice::TransId::new(), true, 1, 1, false),
        Some(password),
    )
    .expect("should be able to serialize valid stun message");
    TcpFrame::try_new(stun_msg).expect("should be able to create frame from valid stun message")
}

#[tokio::test]
async fn acceptor_accepts_tcp_connections() {
    let (addr, _transport) = tcp_transport(None);
    TcpStream::connect(addr)
        .await
        .expect("should be able to create test stream");
}

#[tokio::test]
async fn acceptor_closes_connection_on_binding_request_with_unknown_ufrag() {
    let (addr, _transport) = tcp_transport(None);
    let mut stream = TcpStream::connect(addr)
        .await
        .expect("should be able to create test stream");
    let peer_addr = stream
        .local_addr()
        .expect("should be able to get stream addr");
    write_tcp_frame(
        &mut stream,
        binding_request_frame("unknown:client", WRONG_ICE_PASSWORD),
    )
    .await;
    let reason = closure_reason(peer_addr).await;
    assert!(
        matches!(&reason, TcpFailure::UnknownUfrag(ufrag) if ufrag == "unknown"),
        "closed for another reason: {reason:?}"
    );
}

#[tokio::test]
async fn acceptor_closes_connection_when_peer_goes_silent() {
    let (addr, transport) = tcp_transport(None);
    let (_session_key, _stream, _client, client_addr) =
        connect_client_over_tcp(addr, &transport).await;
    let reason = closure_reason(client_addr).await;
    assert!(
        matches!(reason, TcpFailure::StallPeer),
        "closed for another reason: {reason:?}"
    );
}

#[tokio::test]
async fn acceptor_closes_connection_with_wrong_binding_req_password() {
    let (addr, transport) = tcp_transport(None);
    let (_session_key, offer) = create_session_offer(&transport).await;
    let local_ufrag = offer
        .lines()
        .find_map(|line| line.strip_prefix("a=ice-ufrag:"))
        .expect("offer should carry an ICE ufrag")
        .trim();
    let mut stream = TcpStream::connect(addr)
        .await
        .expect("should be able to create test stream");
    let peer_addr = stream
        .local_addr()
        .expect("should be able to get stream addr");
    // str0m drops a binding request signed with the wrong password without replying.
    write_tcp_frame(
        &mut stream,
        binding_request_frame(&format!("{local_ufrag}:client"), WRONG_ICE_PASSWORD),
    )
    .await;
    let reason = closure_reason(peer_addr).await;
    assert!(
        matches!(reason, TcpFailure::AuthTimeout),
        "closed for another reason: {reason:?}"
    );
}

#[tokio::test]
async fn acceptor_closes_connection_when_no_ufrag_arrives_in_time() {
    let (addr, _transport) = tcp_transport(None);
    let stream = TcpStream::connect(addr)
        .await
        .expect("should be able to create test stream");
    let peer_addr = stream
        .local_addr()
        .expect("should be able to get stream addr");
    let reason = closure_reason(peer_addr).await;
    assert!(
        matches!(reason, TcpFailure::UfragTimeout),
        "closed for another reason: {reason:?}"
    );
}

/// Waits for the acceptor to report why it closed the stream of `peer_addr`.
async fn closure_reason(peer_addr: SocketAddr) -> TcpFailure {
    timeout(CLOSURE_REPORT_DEADLINE, async {
        loop {
            if let Some(reason) = take_closure_reason(peer_addr) {
                return reason;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the acceptor should have closed the stream")
}

async fn closed_within(stream: &mut TcpStream, wait: Duration) -> bool {
    match timeout(wait, stream.read(&mut [0u8; 16])).await {
        Ok(Ok(0) | Err(_)) => true,
        Ok(Ok(_)) => panic!("server should not send bytes before a ufrag"),
        Err(_elapsed) => false,
    }
}

#[tokio::test]
async fn acceptor_closes_connections_over_the_max_conn_limit() {
    let quick_close = TCP_RESOLVE_UFRAG_MAX / 4;
    let (addr, _transport) = tcp_transport(Some(1));
    let held = TcpStream::connect(addr)
        .await
        .expect("should be able to create test stream");
    let mut over_limit = TcpStream::connect(addr)
        .await
        .expect("the stream is accepted before being closed");
    assert!(
        closed_within(&mut over_limit, quick_close).await,
        "the stream over the limit should be closed right away"
    );
    drop(held);
    let started = Instant::now();
    loop {
        let mut retry = TcpStream::connect(addr)
            .await
            .expect("should be able to create test stream");
        if !closed_within(&mut retry, quick_close).await {
            break;
        }
        assert!(
            started.elapsed() < TCP_RESOLVE_UFRAG_MAX,
            "closing a stream should free its slot"
        );
    }
}

async fn relay_until_connected(
    client: &mut Peer,
    stream: &mut TcpStream,
    server_addr: SocketAddr,
    client_addr: SocketAddr,
) {
    let mut decoder = TcpFrameDecoder::new();
    loop {
        for packet in client.drain(Instant::now()) {
            let frame = TcpFrame::try_new(packet.contents.into())
                .expect("client packet should fit in a frame");
            write_tcp_frame(stream, frame).await;
        }
        if client.ice.is_connected() && client.connected {
            return;
        }
        select! {
            () = sleep_until(time::Instant::from_std(client.deadline)) => {}
            readable = stream.readable() => {
                readable.expect("client stream should be ready");
                match decoder.feed_with(|buffer| stream.try_read(buffer)) {
                    Ok(0) => panic!("server closed the TCP connection"),
                    Ok(_) => {}
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {}
                    Err(error) => panic!("client read failed: {error}"),
                }
                for frame in decoder.by_ref() {
                    let frame = frame.expect("test frames should match expected format");
                    let receive = Receive::new(Protocol::Tcp, server_addr, client_addr, frame.payload())
                        .expect("server frame should hold a valid packet");
                    client
                        .rtc
                        .handle_input(Input::Receive(Instant::now(), receive))
                        .expect("client should accept the server packet");
                }
            }
        }
    }
}

/// Connects a str0m client to the acceptor over TCP and waits for ICE and DTLS to complete.
///
/// Returns the session key, the connected stream, the client and the client's local
/// address.
///
/// # Panics
///
/// Panics when the handshake doesn't complete within [`HANDSHAKE_DEADLINE`].
async fn connect_client_over_tcp(
    server_addr: SocketAddr,
    transport: &MediaTransport,
) -> (TransportSessionKey, TcpStream, Peer, SocketAddr) {
    let (session_key, offer) = create_session_offer(transport).await;
    let offer = SdpOffer::from_sdp_string(&offer).expect("should be able to parse created offer");
    let mut client = Peer::new(
        Rtc::builder().set_ice_lite(false).build(Instant::now()),
        Instant::now(),
    );
    let sdp_answer = client
        .rtc
        .sdp_api()
        .accept_offer(offer)
        .expect("should accept offer")
        .to_sdp_string();
    transport
        .apply_session_answer(&session_key, &sdp_answer)
        .await
        .expect("should be able to register the session");
    let mut stream = TcpStream::connect(server_addr)
        .await
        .expect("should be able to create test stream");
    let client_addr = stream
        .local_addr()
        .expect("should be able to get stream addr");
    client
        .rtc
        .add_local_candidate(
            Candidate::builder()
                .tcp()
                .tcptype(TcpType::Active)
                .host(client_addr)
                .build()
                .expect("should be able to build candidate"),
        )
        .expect("should be able to add the candidate");
    // str0m only starts checking a new pair on its next timeout input. Give it one
    // now, so the first check beats the acceptor's ufrag deadline.
    client
        .rtc
        .handle_input(Input::Timeout(Instant::now()))
        .expect("client should accept a timeout input");
    let handshake = timeout(
        HANDSHAKE_DEADLINE,
        relay_until_connected(&mut client, &mut stream, server_addr, client_addr),
    )
    .await;
    assert!(
        handshake.is_ok(),
        "TCP handshake stalled: ICE {:?}, DTLS connected {}",
        client.ice,
        client.connected,
    );
    (session_key, stream, client, client_addr)
}

#[tokio::test]
async fn client_connects_over_tcp() {
    let (server_addr, transport) = tcp_transport(None);
    let (_session_key, _stream, client, client_addr) =
        connect_client_over_tcp(server_addr, &transport).await;
    assert!(!client.nominated_routes.is_empty());
    for route in &client.nominated_routes {
        assert_eq!(*route, (Protocol::Tcp, client_addr, server_addr));
    }
}

#[tokio::test]
async fn acceptor_closes_authenticated_connection_when_session_is_closed() {
    let (server_addr, transport) = tcp_transport(None);
    let (session_key, _stream, _client, client_addr) =
        connect_client_over_tcp(server_addr, &transport).await;
    transport
        .teardown([TransportTeardown::close_session(session_key)])
        .await;
    let reason = closure_reason(client_addr).await;
    assert!(
        matches!(reason, TcpFailure::OutboxClosed),
        "closed for another reason: {reason:?}"
    );
}
