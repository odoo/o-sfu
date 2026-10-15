//! RTC over TCP through the real acceptor.

use std::{
    io::ErrorKind,
    net::{SocketAddr, TcpListener},
    time::{Duration, Instant},
};

use o_sfu_model::UserId;
use o_sfu_rfc::tcp_framing::{FrameDecoder, TcpFrame};
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
    time::{self, sleep_until, timeout},
};

use crate::{
    RtcTcpConfig,
    engine::media_transport::rtc::{
        tcp_acceptor::{TCP_RESOLVE_UFRAG_MAX, TCP_STALL_PEER_DELAY},
        test_support::{Peer, serialize_stun_message},
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

fn binding_request_frame(username: &str) -> TcpFrame {
    let stun_msg = serialize_stun_message(
        &ice::StunMessage::binding_request(username, ice::TransId::new(), true, 1, 1, false),
        Some(b"pwd"),
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
async fn acceptor_closes_connection_without_ufrag() {
    let (addr, _transport) = tcp_transport(None);
    let mut stream = TcpStream::connect(addr)
        .await
        .expect("should be able to create test stream");
    write_tcp_frame(&mut stream, binding_request_frame("unknown:client")).await;
    let received = timeout(Duration::from_secs(2), stream.read(&mut [0u8; 16]))
        .await
        .expect("should reject right away")
        .expect("should receive EOF right away");
    assert_eq!(received, 0);
}

#[tokio::test]
async fn acceptor_closes_stalled_connections() {
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
    write_tcp_frame(
        &mut stream,
        binding_request_frame(&format!("{local_ufrag}:client")),
    )
    .await;
    let silent_since = Instant::now();
    let received = timeout(TCP_STALL_PEER_DELAY * 2, stream.read(&mut [0u8; 16]))
        .await
        .expect("server should close a silent stream")
        .expect("should receive EOF");
    assert_eq!(received, 0);
    assert!(silent_since.elapsed() >= TCP_STALL_PEER_DELAY);
}

#[tokio::test]
async fn acceptor_times_out_without_ufrag() {
    let (addr, _transport) = tcp_transport(None);
    let now = Instant::now();
    let mut stream = TcpStream::connect(addr)
        .await
        .expect("should be able to create test stream");
    let received = timeout(Duration::from_secs(2), stream.read(&mut [0u8; 16]))
        .await
        .expect("should reject after TCP_RESOLVE_UFRAG_MAX")
        .expect("should receive EOF right away");
    assert_eq!(received, 0);
    assert!(now.elapsed() >= TCP_RESOLVE_UFRAG_MAX);
}

async fn closed_within(stream: &mut TcpStream, wait: Duration) -> bool {
    match timeout(wait, stream.read(&mut [0u8; 16])).await {
        Ok(Ok(0) | Err(_)) => true,
        Ok(Ok(_)) => panic!("server should not send bytes before a ufrag"),
        Err(_elapsed) => false,
    }
}

#[tokio::test]
async fn acceptor_enforces_max_conn() {
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
    let mut decoder = FrameDecoder::new();
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
                    let receive =
                        Receive::new(Protocol::Tcp, server_addr, client_addr, &frame.payload)
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

#[tokio::test]
async fn client_connects_over_tcp() {
    let (server_addr, transport) = tcp_transport(None);
    let (session_key, offer) = create_session_offer(&transport).await;
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
    assert!(!client.nominated_routes.is_empty());
    for route in &client.nominated_routes {
        assert_eq!(*route, (Protocol::Tcp, client_addr, server_addr));
    }
}
