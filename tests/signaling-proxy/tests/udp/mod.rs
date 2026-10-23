use std::{io, net::SocketAddr};

use tokio::{net::UdpSocket, time};

use super::*;

const CONTROL: &[u8] = &[
    0, 1, 0, 0, 0x21, 0x12, 0xa4, 0x42, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];
const RTCP: &[u8] = &[0x80, 200, 0, 1, 0, 0, 0, 1];

fn rtp(id: u8) -> Vec<u8> {
    vec![0x80, 96, 0, id, 0, 0, 0, 0, 0, 0, 0, 1]
}

fn sdp(address: SocketAddr) -> String {
    format!(
        "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 96\r\na=ice-ufrag:test\r\na=candidate:udp 1 UDP 100 {} {} typ host generation 0\r\na=candidate:tcp 1 TCP 90 127.0.0.1 9 typ host tcptype passive\r\na=end-of-candidates\r\n",
        address.ip(),
        address.port()
    )
}

fn offer(address: SocketAddr) -> Message {
    Message::text(json!([{"t":"offer","q":"offer-1","p":{"sdp":sdp(address)}}]).to_string())
}

fn projected(message: Message) -> TestResult<(Value, SocketAddr)> {
    let Message::Text(text) = message else {
        return Err("expected projected text offer".into());
    };
    let value: Value = serde_json::from_str(&text)?;
    let sdp = value
        .pointer("/0/p/sdp")
        .and_then(Value::as_str)
        .ok_or("projected SDP missing")?;
    let mut candidates = sdp.lines().filter(|line| line.starts_with("a=candidate:"));
    let candidate = candidates.next().ok_or("projected candidate missing")?;
    assert!(
        candidates.next().is_none(),
        "TCP bypass candidate survived projection"
    );
    let words: Vec<_> = candidate.split_ascii_whitespace().collect();
    assert_eq!(words.get(2), Some(&"UDP"));
    let address = format!(
        "{}:{}",
        words.get(4).ok_or("candidate IP missing")?,
        words.get(5).ok_or("candidate port missing")?
    )
    .parse()?;
    Ok((value, address))
}

async fn start(upstream: &str, rules: Value) -> TestResult<Proxy> {
    Proxy::configure(
        json!({"upstream":upstream,"udp":{"rules":rules}}),
        Stdio::inherit(),
    )
    .await
}

async fn received(socket: &UdpSocket) -> TestResult<(Vec<u8>, SocketAddr)> {
    let mut bytes = vec![0; 65_536];
    let (length, source) = timeout(Duration::from_secs(2), socket.recv_from(&mut bytes)).await??;
    bytes.truncate(length);
    Ok((bytes, source))
}

async fn stats(proxy: &mut Proxy, request: u64) -> TestResult<Value> {
    proxy
        .send(json!({"command":"stats","requestId":request}))
        .await?;
    let event = proxy.event("stats").await?;
    assert_eq!(event.get("requestId"), Some(&json!(request)));
    Ok(event.get("udp").ok_or("UDP statistics missing")?.clone())
}

#[tokio::test]
async fn candidate_projection_reuses_routes_and_isolates_browser_sources() -> TestResult<()> {
    let upstream_udp = UdpSocket::bind("127.0.0.1:0").await?;
    let target = upstream_udp.local_addr()?;
    let (upstream, server) = echo_server().await?;
    let mut proxy = start(&upstream, json!([])).await?;
    let client = service_fn(http_client::send);
    let mut socket = client
        .websocket(proxy.url.as_str())
        .handshake(Extensions::default())
        .await?;
    let id = event_id(&proxy.event("connected").await?)?;
    socket.send_message(offer(target)).await?;
    let (first, relay) = projected(timeout(Duration::from_secs(2), socket.recv_message()).await??)?;
    assert_ne!(relay, target);
    let bound = proxy.event("udp_bound").await?;
    assert_eq!(event_id(&bound)?, id);
    assert_eq!(bound.get("address"), Some(&json!(relay.to_string())));
    let expected_sdp = sdp(target).replace(
        &format!("{} typ host generation", target.port()),
        &format!("{} typ host generation", relay.port()),
    );
    let expected_sdp: String = expected_sdp
        .split_inclusive('\n')
        .filter(|line| !line.contains("a=candidate:tcp"))
        .collect();
    assert_eq!(first.pointer("/0/p/sdp"), Some(&json!(expected_sdp)));
    let renegotiate = json!([{"t":"renegotiate","q":"offer-2","p":{"sdp":sdp(target)}}]);
    socket
        .send_message(Message::text(renegotiate.to_string()))
        .await?;
    let (_, second_relay) =
        projected(timeout(Duration::from_secs(2), socket.recv_message()).await??)?;
    assert_eq!(relay, second_relay);
    // A duplicate bind would appear before this command's statistics event.
    proxy.send(json!({"command":"stats","requestId":1})).await?;
    let event = next_event(&mut proxy).await?;
    assert_eq!(
        event.get("event"),
        Some(&json!("stats")),
        "route rebound on renegotiation"
    );
    let one = UdpSocket::bind("127.0.0.1:0").await?;
    let two = UdpSocket::bind("127.0.0.1:0").await?;
    one.send_to(&[9, 1], relay).await?;
    let (first_packet, first_source) = received(&upstream_udp).await?;
    two.send_to(&[9, 2], relay).await?;
    let (second_packet, second_source) = received(&upstream_udp).await?;
    assert_eq!(first_packet, [9, 1]);
    assert_eq!(second_packet, [9, 2]);
    assert_ne!(first_source, second_source);
    upstream_udp.send_to(&[9, 12], second_source).await?;
    upstream_udp.send_to(&[9, 11], first_source).await?;
    assert_eq!(received(&one).await?, (vec![9, 11], relay));
    assert_eq!(received(&two).await?, (vec![9, 12], relay));
    assert_eq!(
        stats(&mut proxy, 2).await?.get("activeFlows"),
        Some(&json!(2))
    );
    proxy.stop().await?;
    server.abort();
    Ok(())
}

#[tokio::test]
async fn immediate_upstream_burst_keeps_connection_and_packet_order() -> TestResult<()> {
    for backlog in [0, 128] {
        let upstream_udp = UdpSocket::bind("127.0.0.1:0").await?;
        let (upstream, server) = echo_server().await?;
        let rules = if backlog == 0 {
            json!([])
        } else {
            json!([{"direction":"client_to_server","packet":"rtp","effects":[{"delay":{"ms":5000}}]}])
        };
        let mut proxy = start(&upstream, rules).await?;
        let client = service_fn(http_client::send);
        let mut socket = client
            .websocket(proxy.url.as_str())
            .handshake(Extensions::default())
            .await?;
        socket
            .send_message(offer(upstream_udp.local_addr()?))
            .await?;
        let (_, relay) = projected(timeout(Duration::from_secs(2), socket.recv_message()).await??)?;
        let browser = UdpSocket::bind("127.0.0.1:0").await?;
        browser.send_to(CONTROL, relay).await?;
        let (_, source) = received(&upstream_udp).await?;
        for id in 0..backlog {
            browser.send_to(&rtp(id), relay).await?;
        }
        wait_stat(&mut proxy, "pending", backlog.into()).await?;
        // Immediate return traffic must pass even while delayed traffic fills the budget.
        let sent = async {
            for id in 0..160 {
                upstream_udp.send_to(&rtp(id), source).await?;
            }
            Ok::<(), Box<dyn Error + Send + Sync>>(())
        };
        let delivered = async {
            for id in 0..160 {
                assert_eq!(received(&browser).await?, (rtp(id), relay));
            }
            Ok::<(), Box<dyn Error + Send + Sync>>(())
        };
        let result = tokio::try_join!(sent, delivered);
        let metrics = stats(&mut proxy, 40).await?;
        assert_eq!(metrics.get("overflow"), Some(&json!(0)), "{metrics}");
        result?;
        browser.send_to(CONTROL, relay).await?;
        assert_eq!(received(&upstream_udp).await?, (CONTROL.to_vec(), source));
        assert_eq!(
            wait_stat(&mut proxy, "pending", backlog.into())
                .await?
                .get("activeFlows"),
            Some(&json!(1))
        );
        proxy.stop().await?;
        server.abort();
    }
    Ok(())
}

#[tokio::test]
async fn directional_media_drop_preserves_stun_rtcp_and_unmatched_datagrams() -> TestResult<()> {
    let upstream_udp = UdpSocket::bind("127.0.0.1:0").await?;
    let (upstream, server) = echo_server().await?;
    let mut proxy = start(
        &upstream,
        json!([
            {"direction":"client_to_server","packet":"rtp","limit":1,"effects":["drop"]},
            {"direction":"server_to_client","packet":"rtp","limit":1,"effects":["drop"]}
        ]),
    )
    .await?;
    let client = service_fn(http_client::send);
    let mut socket = client
        .websocket(proxy.url.as_str())
        .handshake(Extensions::default())
        .await?;
    socket
        .send_message(offer(upstream_udp.local_addr()?))
        .await?;
    let (_, relay) = projected(timeout(Duration::from_secs(2), socket.recv_message()).await??)?;
    let browser = UdpSocket::bind("127.0.0.1:0").await?;
    browser.send_to(CONTROL, relay).await?;
    let (control, flow) = received(&upstream_udp).await?;
    assert_eq!(control, CONTROL);
    upstream_udp.send_to(CONTROL, flow).await?;
    assert_eq!(received(&browser).await?.0, CONTROL);
    browser.send_to(&rtp(1), relay).await?;
    let mut buffer = [0; 64];
    assert!(
        timeout(
            Duration::from_millis(100),
            upstream_udp.recv_from(&mut buffer)
        )
        .await
        .is_err()
    );
    browser.send_to(&rtp(2), relay).await?;
    assert_eq!(received(&upstream_udp).await?, (rtp(2), flow));
    upstream_udp.send_to(&rtp(3), flow).await?;
    assert!(
        timeout(Duration::from_millis(100), browser.recv_from(&mut buffer))
            .await
            .is_err()
    );
    upstream_udp.send_to(RTCP, flow).await?;
    assert_eq!(received(&browser).await?.0, RTCP);
    upstream_udp.send_to(&rtp(4), flow).await?;
    assert_eq!(received(&browser).await?.0, rtp(4));
    browser.send_to(&[9, 7], relay).await?;
    assert_eq!(received(&upstream_udp).await?, (vec![9, 7], flow));
    let metrics = stats(&mut proxy, 3).await?;
    assert_eq!(metrics.get("dropped"), Some(&json!(2)));
    assert_eq!(metrics.get("rules"), Some(&json!([1, 1])));
    proxy.stop().await?;
    server.abort();
    Ok(())
}

#[tokio::test]
async fn selective_delay_reorders_without_blocking_and_duplicates_exact_bytes() -> TestResult<()> {
    let upstream_udp = UdpSocket::bind("127.0.0.1:0").await?;
    let (upstream, server) = echo_server().await?;
    let mut proxy = start(
        &upstream,
        json!([
            {"direction":"client_to_server","packet":"rtp","every":2,"limit":1,"effects":[
                {"delay":{"ms":150}}, {"duplicate":{"copies":1,"spacing_ms":30}}
            ]}
        ]),
    )
    .await?;
    let client = service_fn(http_client::send);
    let mut socket = client
        .websocket(proxy.url.as_str())
        .handshake(Extensions::default())
        .await?;
    socket
        .send_message(offer(upstream_udp.local_addr()?))
        .await?;
    let (_, relay) = projected(timeout(Duration::from_secs(2), socket.recv_message()).await??)?;
    let browser = UdpSocket::bind("127.0.0.1:0").await?;
    for id in 1..=3 {
        browser.send_to(&rtp(id), relay).await?;
    }
    for id in [1, 3, 2, 2] {
        assert_eq!(received(&upstream_udp).await?.0, rtp(id));
    }
    let metrics = wait_stat(&mut proxy, "pending", 0).await?;
    assert_eq!(metrics.get("delayed"), Some(&json!(2)));
    assert_eq!(metrics.get("duplicated"), Some(&json!(1)));
    proxy.stop().await?;
    server.abort();
    Ok(())
}

async fn wait_stat(proxy: &mut Proxy, field: &str, expected: u64) -> TestResult<Value> {
    timeout(Duration::from_secs(2), async {
        loop {
            let current = stats(proxy, 20).await?;
            if current.get(field).and_then(Value::as_u64) == Some(expected) {
                return Ok(current);
            }
            time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?
}

async fn rebind(address: SocketAddr) -> TestResult<UdpSocket> {
    // Zero gauges can precede socket destruction. Retain each rebound socket
    // so both ports remain reclaimed while the test observes pending delivery.
    timeout(Duration::from_secs(2), async {
        loop {
            match UdpSocket::bind(address).await {
                Ok(socket) => return Ok(socket),
                Err(error) if error.kind() == io::ErrorKind::AddrInUse => {}
                Err(error) => return Err(error.into()),
            }
            time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?
}

#[tokio::test]
async fn connection_close_cancels_pending_media_and_releases_sockets() -> TestResult<()> {
    let upstream_udp = UdpSocket::bind("127.0.0.1:0").await?;
    let (upstream, server) = echo_server().await?;
    let mut proxy = start(
        &upstream,
        json!([
            {"direction":"client_to_server","packet":"rtp","effects":[{"delay":{"ms":5000}}]}
        ]),
    )
    .await?;
    let client = service_fn(http_client::send);
    let mut socket = client
        .websocket(proxy.url.as_str())
        .handshake(Extensions::default())
        .await?;
    socket
        .send_message(offer(upstream_udp.local_addr()?))
        .await?;
    let (_, relay) = projected(timeout(Duration::from_secs(2), socket.recv_message()).await??)?;
    let browser = UdpSocket::bind("127.0.0.1:0").await?;
    browser.send_to(CONTROL, relay).await?;
    let (_, flow) = received(&upstream_udp).await?;
    browser.send_to(&rtp(1), relay).await?;
    wait_stat(&mut proxy, "pending", 1).await?;
    socket.send_message(Message::Close(None)).await?;
    assert!(matches!(
        timeout(Duration::from_secs(2), socket.recv_message()).await??,
        Message::Close(_)
    ));
    socket.flush().await?;
    drop(socket);
    wait_stat(&mut proxy, "activeFlows", 0).await?;
    wait_stat(&mut proxy, "pending", 0).await?;
    let _released_endpoint = rebind(relay).await?;
    let _released_flow = rebind(flow).await?;
    let mut buffer = [0; 64];
    assert!(
        timeout(
            Duration::from_millis(100),
            upstream_udp.recv_from(&mut buffer)
        )
        .await
        .is_err()
    );
    proxy.stop().await?;
    server.abort();
    Ok(())
}

#[tokio::test]
async fn stdin_eof_cancels_pending_media_before_process_exit() -> TestResult<()> {
    let upstream_udp = UdpSocket::bind("127.0.0.1:0").await?;
    let (upstream, server) = echo_server().await?;
    let mut proxy = start(
        &upstream,
        json!([
            {"direction":"client_to_server","packet":"rtp","effects":[{"delay":{"ms":5000}}]}
        ]),
    )
    .await?;
    let client = service_fn(http_client::send);
    let mut socket = client
        .websocket(proxy.url.as_str())
        .handshake(Extensions::default())
        .await?;
    socket
        .send_message(offer(upstream_udp.local_addr()?))
        .await?;
    let (_, relay) = projected(timeout(Duration::from_secs(2), socket.recv_message()).await??)?;
    let browser = UdpSocket::bind("127.0.0.1:0").await?;
    browser.send_to(CONTROL, relay).await?;
    let (_, flow) = received(&upstream_udp).await?;
    browser.send_to(&rtp(1), relay).await?;
    wait_stat(&mut proxy, "pending", 1).await?;
    drop(proxy.input);
    assert!(
        timeout(Duration::from_secs(2), proxy.child.wait())
            .await??
            .success()
    );
    let _released_endpoint = UdpSocket::bind(relay).await?;
    let _released_flow = UdpSocket::bind(flow).await?;
    let mut buffer = [0; 64];
    assert!(
        timeout(
            Duration::from_millis(100),
            upstream_udp.recv_from(&mut buffer)
        )
        .await
        .is_err()
    );
    drop(socket);
    server.abort();
    Ok(())
}

#[tokio::test]
async fn unsupported_candidates_close_without_forwarding_a_bypass_offer() -> TestResult<()> {
    let (upstream, server) = echo_server().await?;
    let proxy = start(&upstream, json!([])).await?;
    let client = service_fn(http_client::send);
    for candidate in [
        "a=candidate:a 1 UDP 100 192.0.2.1 9000 typ host",
        "a=candidate:a 1 UDP 100 localhost 9000 typ host",
        "a=candidate:a 1 UDP 100 ::1 9000 typ host",
        "a=candidate:a 1 UDP 100 127.0.0.1 9000 typ srflx",
        "a=candidate:a 2 UDP 100 127.0.0.1 9000 typ host",
        "a=candidate:a 1 TCP 100 127.0.0.1 9000 typ host tcptype passive",
    ] {
        let mut socket = client
            .websocket(proxy.url.as_str())
            .handshake(Extensions::default())
            .await?;
        socket
            .send_message(Message::text(
                json!([{"t":"offer","q":"unsafe","p":{"sdp":format!("v=0\r\n{candidate}\r\n")}}])
                    .to_string(),
            ))
            .await?;
        match timeout(Duration::from_secs(2), socket.recv_message()).await? {
            Ok(Message::Close(_)) | Err(_) => {}
            Ok(message) => {
                return Err(format!("unsupported offer was forwarded: {message:?}").into());
            }
        }
    }
    proxy.stop().await?;
    server.abort();
    Ok(())
}

#[tokio::test]
async fn pending_packet_bound_closes_connection_and_drains_reservations() -> TestResult<()> {
    let upstream_udp = UdpSocket::bind("127.0.0.1:0").await?;
    let (upstream, server) = echo_server().await?;
    let mut proxy = Proxy::configure(
        json!({"upstream":upstream,"udp":{"rules":[
            {"direction":"client_to_server","packet":"rtp","effects":[{"delay":{"ms":5000}}]}
        ]}}),
        Stdio::piped(),
    )
    .await?;
    let stderr = proxy
        .child
        .stderr
        .take()
        .ok_or("proxy stderr unavailable")?;
    let diagnostics = tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        let mut output = String::new();
        while let Some(line) = lines.next_line().await? {
            output.push_str(&line);
            output.push('\n');
        }
        Ok::<_, io::Error>(output)
    });
    let client = service_fn(http_client::send);
    let mut socket = client
        .websocket(proxy.url.as_str())
        .handshake(Extensions::default())
        .await?;
    socket
        .send_message(offer(upstream_udp.local_addr()?))
        .await?;
    let (_, relay) = projected(timeout(Duration::from_secs(2), socket.recv_message()).await??)?;
    let browser = UdpSocket::bind("127.0.0.1:0").await?;
    for _ in 0..129 {
        browser.send_to(&rtp(1), relay).await?;
    }
    wait_stat(&mut proxy, "overflow", 1).await?;
    wait_stat(&mut proxy, "activeFlows", 0).await?;
    wait_stat(&mut proxy, "pending", 0).await?;
    let mut buffer = [0; 64];
    assert!(
        timeout(
            Duration::from_millis(100),
            upstream_udp.recv_from(&mut buffer)
        )
        .await
        .is_err()
    );
    proxy.stop().await?;
    let diagnostics = timeout(Duration::from_secs(2), diagnostics).await???;
    assert!(
        diagnostics.contains("UDP relay pending packet limit exceeded"),
        "{diagnostics}"
    );
    assert!(diagnostics.contains("connectionId=1"), "{diagnostics}");
    server.abort();
    Ok(())
}

async fn next_event(proxy: &mut Proxy) -> TestResult<Value> {
    let line = timeout(Duration::from_secs(2), proxy.output.next_line())
        .await??
        .ok_or("proxy event missing")?;
    Ok(serde_json::from_str(&line)?)
}

#[tokio::test]
async fn signaling_effects_precede_udp_projection_and_packet_effects() -> TestResult<()> {
    let upstream_udp = UdpSocket::bind("127.0.0.1:0").await?;
    let (upstream, server) = echo_server().await?;
    for action in ["hold", "drop"] {
        let mut proxy = Proxy::configure(
            json!({
                "upstream":upstream,
                "rule":{"direction":"server_to_client","t":"offer","action":action},
                "udp":{"rules":[{"direction":"client_to_server","packet":"rtp","effects":[{"duplicate":{"copies":1}}]}]}
            }),
            Stdio::inherit(),
        ).await?;
        let client = service_fn(http_client::send);
        let mut socket = client
            .websocket(proxy.url.as_str())
            .handshake(Extensions::default())
            .await?;
        let id = event_id(&proxy.event("connected").await?)?;
        socket
            .send_message(offer(upstream_udp.local_addr()?))
            .await?;
        // Filtering for the fault event could discard an erroneous early bind.
        let fault = next_event(&mut proxy).await?;
        assert_eq!(event_id(&fault)?, id);
        assert_eq!(
            fault.get("event"),
            Some(&json!(if action == "hold" {
                "blocked"
            } else {
                "dropped"
            }))
        );
        proxy
            .send(json!({"command":"stats","requestId":30}))
            .await?;
        let barrier = next_event(&mut proxy).await?;
        assert_eq!(
            barrier.get("event"),
            Some(&json!("stats")),
            "candidate projected before signaling effect completed"
        );
        assert_eq!(barrier.get("requestId"), Some(&json!(30)));
        assert_eq!(barrier.pointer("/udp/activeFlows"), Some(&json!(0)));
        assert_eq!(barrier.pointer("/udp/forwarded"), Some(&json!(0)));
        assert!(
            timeout(Duration::from_millis(100), socket.recv_message())
                .await
                .is_err(),
            "held or dropped offer reached the browser"
        );
        if action == "hold" {
            proxy
                .send(json!({"command":"release","connectionId":id}))
                .await?;
            let released = next_event(&mut proxy).await?;
            assert_eq!(released.get("event"), Some(&json!("released")));
            assert_eq!(event_id(&released)?, id);
        } else {
            socket
                .send_message(offer(upstream_udp.local_addr()?))
                .await?;
        }
        let bound = next_event(&mut proxy).await?;
        assert_eq!(bound.get("event"), Some(&json!("udp_bound")));
        assert_eq!(event_id(&bound)?, id);
        let (_, relay) = projected(timeout(Duration::from_secs(2), socket.recv_message()).await??)?;
        assert_eq!(bound.get("address"), Some(&json!(relay.to_string())));
        let browser = UdpSocket::bind("127.0.0.1:0").await?;
        browser.send_to(&rtp(7), relay).await?;
        let (first, flow) = received(&upstream_udp).await?;
        assert_eq!(first, rtp(7));
        assert_eq!(received(&upstream_udp).await?, (rtp(7), flow));
        upstream_udp.send_to(CONTROL, flow).await?;
        assert_eq!(received(&browser).await?, (CONTROL.to_vec(), relay));
        assert_eq!(stats(&mut proxy, 31).await?.get("rules"), Some(&json!([1])));
        proxy.stop().await?;
    }
    server.abort();
    Ok(())
}
