use std::{convert::Infallible, error::Error, net, process::Stdio, sync::Arc, time::Duration};

use rama_core::{
    Layer, extensions::Extensions, futures::SinkExt, layer::ArcLayer, rt::Executor,
    service::service_fn,
};
use rama_http::{
    headers::SecWebSocketProtocol, layer::error_handling::ErrorHandlerLayer, service::web::Router,
};
use rama_http_backend::server::HttpServer;
use rama_tcp::server::TcpListener;
use rama_ws::{
    Message,
    handshake::{
        client::HttpClientWebSocketExt,
        server::{ServerWebSocket, WebSocketAcceptor},
    },
    protocol::{CloseFrame, frame::coding::CloseCode},
};
use serde_json::{Value, json};
use tokio::{
    io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    net::TcpListener as AsyncTcpListener,
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{Mutex, mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};

#[path = "../src/http_client.rs"]
mod http_client;
mod udp;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

struct Proxy {
    child: Child,
    input: ChildStdin,
    output: Lines<BufReader<ChildStdout>>,
    url: String,
}

impl Proxy {
    async fn start(upstream: &str, rule: Value) -> TestResult<Self> {
        Self::spawn(upstream, rule, false, Stdio::inherit()).await
    }

    async fn spawn(upstream: &str, rule: Value, deferred: bool, stderr: Stdio) -> TestResult<Self> {
        Self::configure(
            json!({"upstream":upstream,"rule":rule,"deferred":deferred}),
            stderr,
        )
        .await
    }

    async fn configure(config: Value, stderr: Stdio) -> TestResult<Self> {
        let mut child = Command::new(env!("CARGO_BIN_EXE_o-sfu-signaling-proxy"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(stderr)
            .kill_on_drop(true)
            .spawn()?;
        let input = child.stdin.take().ok_or("proxy stdin unavailable")?;
        let output = BufReader::new(child.stdout.take().ok_or("proxy stdout unavailable")?).lines();
        let mut proxy = Self {
            child,
            input,
            output,
            url: String::new(),
        };
        proxy.send(config).await?;
        let ready = proxy.event("ready").await?;
        ready
            .get("url")
            .and_then(Value::as_str)
            .ok_or("missing ready URL")?
            .clone_into(&mut proxy.url);
        Ok(proxy)
    }

    async fn send(&mut self, command: Value) -> TestResult<()> {
        self.input.write_all(command.to_string().as_bytes()).await?;
        self.input.write_all(b"\n").await?;
        self.input.flush().await?;
        Ok(())
    }

    async fn event(&mut self, name: &str) -> TestResult<Value> {
        loop {
            let line = timeout(Duration::from_secs(5), self.output.next_line())
                .await??
                .ok_or("proxy stdout closed")?;
            let event: Value = serde_json::from_str(&line)?;
            if event.get("event").and_then(Value::as_str) == Some(name) {
                return Ok(event);
            }
        }
    }

    async fn stop(mut self) -> TestResult<()> {
        self.send(json!({"command":"stop"})).await?;
        assert!(
            timeout(Duration::from_secs(5), self.child.wait())
                .await??
                .success()
        );
        Ok(())
    }
}

fn event_id(event: &Value) -> TestResult<u64> {
    Ok(event
        .get("connectionId")
        .and_then(Value::as_u64)
        .ok_or("missing connection ID")?)
}

async fn echo_server() -> TestResult<(String, JoinHandle<()>)> {
    let exec = Executor::default();
    let listener = TcpListener::bind_address("127.0.0.1:0", exec.clone()).await?;
    let address = listener.local_addr()?;
    let service = HttpServer::new_http1(exec).service(
        (ArcLayer::new(), ErrorHandlerLayer::new())
            .into_layer(Router::new().with_get("/", WebSocketAcceptor::new().into_echo_service())),
    );
    let task = tokio::spawn(async move {
        listener.serve(service).await;
    });
    Ok((format!("ws://{address}/"), task))
}

#[tokio::test]
async fn holds_only_matching_direction_and_preserves_complete_frames() -> TestResult<()> {
    let (upstream, server) = echo_server().await?;
    let mut proxy = Proxy::start(
        &upstream,
        json!({
            "direction":"client_to_server", "t":"signal", "response":false, "action":"hold"
        }),
    )
    .await?;
    let client = service_fn(http_client::send);
    let mut socket = client
        .websocket(proxy.url.as_str())
        .handshake(Extensions::default())
        .await?;
    let id = event_id(&proxy.event("connected").await?)?;
    let binary = Message::binary(vec![0, 1, 2, 255]);
    socket.send_message(binary.clone()).await?;
    assert_eq!(
        timeout(Duration::from_secs(2), socket.recv_message()).await??,
        binary
    );
    let held = Message::text(r#"[{"t":"other","q":"first"},{"t":"signal","q":"one"}]"#);
    socket.send_message(held.clone()).await?;
    assert_eq!(event_id(&proxy.event("blocked").await?)?, id);
    assert!(
        timeout(Duration::from_millis(100), socket.recv_message())
            .await
            .is_err()
    );
    proxy
        .send(json!({"command":"release","connectionId":id}))
        .await?;
    assert_eq!(event_id(&proxy.event("released").await?)?, id);
    // A release event precedes forwarding, so a lost message needs a deadline.
    assert_eq!(
        timeout(Duration::from_secs(2), socket.recv_message()).await??,
        held
    );
    let next = Message::text(r#"[{"t":"signal","q":"two"}]"#);
    socket.send_message(next.clone()).await?;
    assert_eq!(
        timeout(Duration::from_secs(2), socket.recv_message()).await??,
        next
    );
    let close = Message::Close(Some(CloseFrame {
        code: CloseCode::Normal,
        reason: "finished".into(),
    }));
    socket.send_message(close.clone()).await?;
    assert_eq!(
        timeout(Duration::from_secs(2), socket.recv_message()).await??,
        close
    );
    drop(socket);
    proxy.stop().await?;
    server.abort();
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "Keep response hold and reconnect assertions together to verify one-shot faults across connections."
)]
async fn holds_response_then_disconnects_once_across_reconnects() -> TestResult<()> {
    let exec = Executor::default();
    let listener = TcpListener::bind_address("127.0.0.1:0", exec.clone()).await?;
    let upstream = format!("ws://{}/", listener.local_addr()?);
    let (received_tx, mut received_rx) = mpsc::unbounded_channel();
    let ws =
        WebSocketAcceptor::new().into_service(service_fn(move |mut socket: ServerWebSocket| {
            let received_tx = received_tx.clone();
            async move {
                while let Ok(message) = socket.recv_message().await {
                    let _ = received_tx.send(message.clone());
                    if socket.send_message(message).await.is_err() {
                        break;
                    }
                }
                Ok::<(), Infallible>(())
            }
        }));
    let service = HttpServer::new_http1(exec).service(
        (ArcLayer::new(), ErrorHandlerLayer::new()).into_layer(Router::new().with_get("/", ws)),
    );
    let server = tokio::spawn(async move {
        listener.serve(service).await;
    });
    let mut hold = Proxy::start(
        &upstream,
        json!({
            "direction":"server_to_client", "t":"signal", "response":true, "action":"hold"
        }),
    )
    .await?;
    let client = service_fn(http_client::send);
    let mut socket = client
        .websocket(hold.url.as_str())
        .handshake(Extensions::default())
        .await?;
    let id = event_id(&hold.event("connected").await?)?;
    let request = Message::text(r#"[{"t":"signal","q":"request-id"}]"#);
    socket.send_message(request.clone()).await?;
    assert_eq!(
        timeout(Duration::from_secs(2), socket.recv_message()).await??,
        request
    );
    assert_eq!(
        timeout(Duration::from_secs(2), received_rx.recv()).await?,
        Some(request)
    );
    let response = Message::text(r#"[{"t":"signal","r":"request-id"}]"#);
    socket.send_message(response.clone()).await?;
    // An echo alone cannot show whether the message was held before or after reaching the server.
    assert_eq!(
        timeout(Duration::from_secs(2), received_rx.recv()).await?,
        Some(response.clone())
    );
    assert_eq!(event_id(&hold.event("blocked").await?)?, id);
    assert!(
        timeout(Duration::from_millis(100), socket.recv_message())
            .await
            .is_err()
    );
    hold.send(json!({"command":"release","connectionId":id}))
        .await?;
    hold.event("released").await?;
    assert_eq!(
        timeout(Duration::from_secs(2), socket.recv_message()).await??,
        response
    );
    drop(socket);
    hold.stop().await?;
    let mut disconnect = Proxy::start(
        &upstream,
        json!({
            "direction":"client_to_server", "t":"signal", "action":"disconnect"
        }),
    )
    .await?;
    let mut first = client
        .websocket(disconnect.url.as_str())
        .handshake(Extensions::default())
        .await?;
    let id = event_id(&disconnect.event("connected").await?)?;
    first.send_message(response.clone()).await?;
    assert_eq!(event_id(&disconnect.event("disconnected").await?)?, id);
    assert!(
        timeout(Duration::from_secs(2), first.recv_message())
            .await?
            .is_err()
    );
    let mut second = client
        .websocket(disconnect.url.as_str())
        .handshake(Extensions::default())
        .await?;
    disconnect.event("connected").await?;
    second.send_message(response.clone()).await?;
    assert_eq!(
        timeout(Duration::from_secs(2), second.recv_message()).await??,
        response
    );
    drop(second);
    drop(disconnect.input);
    assert!(
        timeout(Duration::from_secs(5), disconnect.child.wait())
            .await??
            .success()
    );
    server.abort();
    Ok(())
}

#[tokio::test]
async fn upstream_close_receives_its_reply() -> TestResult<()> {
    let exec = Executor::default();
    let listener = TcpListener::bind_address("127.0.0.1:0", exec.clone()).await?;
    let upstream = format!("ws://{}/", listener.local_addr()?);
    let close = Message::Close(Some(CloseFrame {
        code: CloseCode::Normal,
        reason: "server finished".into(),
    }));
    let expected = close.clone();
    let (replies, mut reply_rx) = mpsc::channel(1);
    let ws =
        WebSocketAcceptor::new().into_service(service_fn(move |mut socket: ServerWebSocket| {
            let replies = replies.clone();
            let close = close.clone();
            async move {
                let reply = async {
                    socket.send_message(close).await?;
                    socket.recv_message().await
                }
                .await;
                let _ = replies.send(reply).await;
                Ok::<(), Infallible>(())
            }
        }));
    let service = HttpServer::new_http1(exec).service(
        (ArcLayer::new(), ErrorHandlerLayer::new()).into_layer(Router::new().with_get("/", ws)),
    );
    let server = tokio::spawn(async move {
        listener.serve(service).await;
    });
    let mut proxy = Proxy::start(
        &upstream,
        json!({"direction":"server_to_client", "t":"signal", "action":"hold"}),
    )
    .await?;
    let client = service_fn(http_client::send);
    let mut socket = client
        .websocket(proxy.url.as_str())
        .handshake(Extensions::default())
        .await?;
    proxy.event("connected").await?;
    assert_eq!(
        timeout(Duration::from_secs(2), socket.recv_message()).await??,
        expected
    );
    timeout(Duration::from_secs(2), socket.flush()).await??;
    assert_eq!(
        timeout(Duration::from_secs(2), reply_rx.recv())
            .await?
            .ok_or("upstream close result unavailable")??,
        expected
    );
    proxy.stop().await?;
    server.abort();
    Ok(())
}

async fn gated_server(close: Message) -> TestResult<(String, oneshot::Sender<()>, JoinHandle<()>)> {
    let exec = Executor::default();
    let listener = TcpListener::bind_address("127.0.0.1:0", exec.clone()).await?;
    let address = listener.local_addr()?;
    let (gate_tx, gate_rx) = oneshot::channel();
    let gate = Arc::new(Mutex::new(Some(gate_rx)));
    let ws =
        WebSocketAcceptor::new().into_service(service_fn(move |mut socket: ServerWebSocket| {
            let gate = Arc::clone(&gate);
            let close = close.clone();
            async move {
                if let Ok(message) = socket.recv_message().await {
                    if let Some(rx) = gate.lock().await.take() {
                        let _ = rx.await;
                    }
                    let _ = socket.send_message(message).await;
                    let _ = socket.send_message(close).await;
                    let _ = socket.recv_message().await;
                }
                Ok::<(), Infallible>(())
            }
        }));
    let service = HttpServer::new_http1(exec).service(
        (ArcLayer::new(), ErrorHandlerLayer::new()).into_layer(Router::new().with_get("/", ws)),
    );
    let task = tokio::spawn(async move {
        listener.serve(service).await;
    });
    Ok((format!("ws://{address}/"), gate_tx, task))
}

#[tokio::test]
async fn reverse_direction_continues_while_client_frame_is_held() -> TestResult<()> {
    let close = Message::Close(Some(CloseFrame {
        code: CloseCode::Normal,
        reason: "shutdown while forward message is held".into(),
    }));
    let (upstream, gate, server) = gated_server(close.clone()).await?;
    let mut proxy = Proxy::start(
        &upstream,
        json!({
            "direction":"client_to_server", "t":"signal", "action":"hold"
        }),
    )
    .await?;
    let client = service_fn(http_client::send);
    let mut socket = client
        .websocket(proxy.url.as_str())
        .handshake(Extensions::default())
        .await?;
    let id = event_id(&proxy.event("connected").await?)?;
    let earlier = Message::binary(vec![17, 19, 23]);
    socket.send_message(earlier.clone()).await?;
    socket
        .send_message(Message::text(r#"[{"t":"signal"}]"#))
        .await?;
    assert_eq!(event_id(&proxy.event("blocked").await?)?, id);
    gate.send(()).map_err(|()| "gate receiver dropped")?;
    assert_eq!(
        timeout(Duration::from_secs(2), socket.recv_message()).await??,
        earlier
    );
    // An opposite-side close must finish without releasing the held message.
    assert_eq!(
        timeout(Duration::from_secs(2), socket.recv_message()).await??,
        close
    );
    timeout(Duration::from_secs(2), socket.flush()).await??;
    proxy.stop().await?;
    server.abort();
    Ok(())
}

#[tokio::test]
async fn deferred_rule_arms_after_passthrough_and_fires_only_once() -> TestResult<()> {
    let (upstream, server) = echo_server().await?;
    let mut proxy = Proxy::spawn(
        &upstream,
        json!({
            "direction":"client_to_server", "t":"signal", "action":"hold"
        }),
        true,
        Stdio::inherit(),
    )
    .await?;
    let client = service_fn(http_client::send);
    let mut first = client
        .websocket(proxy.url.as_str())
        .handshake(Extensions::default())
        .await?;
    let id = event_id(&proxy.event("connected").await?)?;
    let matching = Message::text(r#"[{"t":"signal","q":"one"}]"#);
    first.send_message(matching.clone()).await?;
    assert_eq!(
        timeout(Duration::from_secs(2), first.recv_message()).await??,
        matching
    );
    proxy.send(json!({"command":"arm"})).await?;
    proxy.event("armed").await?;
    first.send_message(matching.clone()).await?;
    assert_eq!(event_id(&proxy.event("blocked").await?)?, id);
    assert!(
        timeout(Duration::from_millis(100), first.recv_message())
            .await
            .is_err()
    );
    proxy
        .send(json!({"command":"release","connectionId":id}))
        .await?;
    assert_eq!(event_id(&proxy.event("released").await?)?, id);
    assert_eq!(
        timeout(Duration::from_secs(2), first.recv_message()).await??,
        matching
    );
    proxy.send(json!({"command":"arm"})).await?;
    proxy.event("armed").await?;
    first.send_message(matching.clone()).await?;
    assert_eq!(
        timeout(Duration::from_secs(2), first.recv_message()).await??,
        matching
    );
    drop(first);
    let mut second = client
        .websocket(proxy.url.as_str())
        .handshake(Extensions::default())
        .await?;
    assert_ne!(event_id(&proxy.event("connected").await?)?, id);
    second.send_message(matching.clone()).await?;
    assert_eq!(
        timeout(Duration::from_secs(2), second.recv_message()).await??,
        matching
    );
    proxy.stop().await?;
    server.abort();
    Ok(())
}

#[tokio::test]
async fn upstream_handshake_failure_reports_connection_and_cause() -> TestResult<()> {
    let reserve = net::TcpListener::bind("127.0.0.1:0")?;
    let upstream = format!("ws://{}/", reserve.local_addr()?);
    drop(reserve);
    let proxy = Proxy::spawn(
        &upstream,
        json!({"direction":"client_to_server","t":"signal","action":"hold"}),
        false,
        Stdio::piped(),
    )
    .await?;
    let client = service_fn(http_client::send);
    assert!(
        timeout(
            Duration::from_secs(2),
            client
                .websocket(proxy.url.as_str())
                .handshake(Extensions::default())
        )
        .await?
        .is_err(),
        "an unavailable upstream accepted the browser upgrade"
    );
    drop(proxy.input);
    let result = timeout(Duration::from_secs(5), proxy.child.wait_with_output()).await??;
    assert!(result.status.success());
    let stderr = String::from_utf8(result.stderr)?;
    assert!(stderr.contains("connectionId=1"), "{stderr}");
    assert!(stderr.contains("upstream handshake failed"), "{stderr}");
    assert!(stderr.contains("refused"), "{stderr}");
    Ok(())
}

#[tokio::test]
async fn upstream_handshake_timeout_rejects_upgrade_and_reports_cause() -> TestResult<()> {
    let listener = AsyncTcpListener::bind("127.0.0.1:0").await?;
    let upstream = format!("ws://{}/", listener.local_addr()?);
    let (accepted_tx, accepted_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let _ = accepted_tx.send(());
        // Keep TCP open without answering HTTP to isolate the handshake deadline.
        io::copy(&mut stream, &mut io::sink()).await
    });
    let proxy = Proxy::spawn(
        &upstream,
        json!({"direction":"client_to_server","t":"signal","action":"hold"}),
        false,
        Stdio::piped(),
    )
    .await?;
    let client = service_fn(http_client::send);
    let handshake = timeout(
        Duration::from_secs(8),
        client
            .websocket(proxy.url.as_str())
            .handshake(Extensions::default()),
    )
    .await;
    timeout(Duration::from_secs(2), accepted_rx).await??;
    drop(proxy.input);
    let result = timeout(Duration::from_secs(5), proxy.child.wait_with_output()).await??;
    server.abort();
    assert!(result.status.success());
    assert!(
        handshake?.is_err(),
        "a stalled upstream accepted the browser upgrade"
    );
    let stderr = String::from_utf8(result.stderr)?;
    assert!(stderr.contains("connectionId=1"), "{stderr}");
    assert!(stderr.contains("upstream handshake"), "{stderr}");
    assert!(stderr.contains("timed out"), "{stderr}");
    Ok(())
}

#[tokio::test]
async fn forwards_handshake_metadata_and_negotiates_compressed_messages() -> TestResult<()> {
    let exec = Executor::default();
    let listener = TcpListener::bind_address("127.0.0.1:0", exec.clone()).await?;
    let path_query = "/signaling?fixture=one%20two&mode=relay";
    let upstream = format!("ws://{}{path_query}", listener.local_addr()?);
    let (observed_tx, mut observed_rx) = mpsc::channel(1);
    let ws = WebSocketAcceptor::new()
        .with_protocols(SecWebSocketProtocol::new("fault.test".try_into()?))
        .with_per_message_deflate()
        .into_service(service_fn(move |mut socket: ServerWebSocket| {
            let observed_tx = observed_tx.clone();
            async move {
                let metadata = json!({
                    "uri": socket.request.uri.to_string(),
                    "origin": socket.request.headers.get("origin").and_then(|v| v.to_str().ok()),
                    "cookie": socket.request.headers.get("cookie").and_then(|v| v.to_str().ok()),
                    "protocol": socket.request.headers.get("sec-websocket-protocol").and_then(|v| v.to_str().ok()),
                    "extensions": socket.request.headers.get("sec-websocket-extensions").and_then(|v| v.to_str().ok()),
                });
                let _ = observed_tx.send(metadata).await;
                while let Ok(message) = socket.recv_message().await {
                    if socket.send_message(message).await.is_err() {
                        break;
                    }
                }
                Ok::<(), Infallible>(())
            }
        }));
    let service = HttpServer::new_http1(exec).service(
        (ArcLayer::new(), ErrorHandlerLayer::new())
            .into_layer(Router::new().with_get("/client-signaling", ws)),
    );
    let server = tokio::spawn(async move {
        listener.serve(service).await;
    });
    let mut proxy = Proxy::start(
        &upstream,
        json!({"direction":"client_to_server","t":"signal","action":"hold"}),
    )
    .await?;
    assert!(proxy.url.ends_with(path_query), "{}", proxy.url);
    // The client can replace the path and query without changing the fixed upstream origin.
    let client_path_query = "/client-signaling?client=two%2Bthree&mode=override";
    let client_url = format!(
        "{}{client_path_query}",
        proxy
            .url
            .strip_suffix(path_query)
            .ok_or("ready URL lost upstream path")?
    );
    let client = service_fn(http_client::send);
    let mut socket = timeout(
        Duration::from_secs(2),
        client
            .websocket(client_url.as_str())
            .with_header("origin", "https://signaling.test")
            .with_header("cookie", "fixture=handshake")
            .with_protocols(SecWebSocketProtocol::new("fault.test".try_into()?))
            .with_per_message_deflate()
            .handshake(Extensions::default()),
    )
    .await??;
    proxy.event("connected").await?;
    let observed = timeout(Duration::from_secs(2), observed_rx.recv())
        .await?
        .ok_or("upstream handshake metadata unavailable")?;
    assert_eq!(observed.get("uri"), Some(&json!(client_path_query)));
    assert_eq!(
        observed.get("origin"),
        Some(&json!("https://signaling.test"))
    );
    assert_eq!(observed.get("cookie"), Some(&json!("fixture=handshake")));
    assert_eq!(observed.get("protocol"), Some(&json!("fault.test")));
    assert!(
        observed
            .get("extensions")
            .and_then(Value::as_str)
            .is_some_and(|value| value.contains("permessage-deflate")),
        "{observed}"
    );
    assert_eq!(socket.accepted_protocol(), Some("fault.test"));
    assert!(
        socket
            .response()
            .headers
            .get("sec-websocket-extensions")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.contains("permessage-deflate"))
    );
    // Negotiation alone cannot detect a relay that resets on the first compressed frame.
    for message in [
        Message::text("compressed text ".repeat(128)),
        Message::binary(vec![42; 4096]),
    ] {
        socket.send_message(message.clone()).await?;
        assert_eq!(
            timeout(Duration::from_secs(2), socket.recv_message()).await??,
            message
        );
    }
    proxy.stop().await?;
    server.abort();
    Ok(())
}

#[tokio::test]
async fn injected_close_preserves_code_reason_and_global_consumption() -> TestResult<()> {
    let (upstream, server) = echo_server().await?;
    for reason in [Some("session ended"), None] {
        let mut close = json!({"code":4106});
        if let Some(reason) = reason {
            close["reason"] = json!(reason);
        }
        let mut proxy = Proxy::start(
            &upstream,
            json!({"direction":"client_to_server","t":"signal","action":{"close":close}}),
        )
        .await?;
        let client = service_fn(http_client::send);
        let mut first = client
            .websocket(proxy.url.as_str())
            .handshake(Extensions::default())
            .await?;
        let id = event_id(&proxy.event("connected").await?)?;
        let matching = Message::text(r#"[{"t":"other"},{"t":"signal"}]"#);
        first.send_message(matching.clone()).await?;
        let closed = proxy.event("closed").await?;
        assert_eq!(event_id(&closed)?, id);
        assert_eq!(closed.get("code"), Some(&json!(4106)));
        assert_eq!(
            timeout(Duration::from_secs(2), first.recv_message()).await??,
            Message::Close(Some(CloseFrame {
                code: CloseCode::from(4106),
                reason: reason.unwrap_or_default().into(),
            }))
        );
        timeout(Duration::from_secs(2), first.flush()).await??;
        let mut second = client
            .websocket(proxy.url.as_str())
            .handshake(Extensions::default())
            .await?;
        assert_ne!(event_id(&proxy.event("connected").await?)?, id);
        second.send_message(matching.clone()).await?;
        assert_eq!(
            timeout(Duration::from_secs(2), second.recv_message()).await??,
            matching
        );
        proxy.stop().await?;
    }
    server.abort();
    Ok(())
}

#[tokio::test]
async fn invalid_configuration_is_rejected_before_listening() -> TestResult<()> {
    let config = json!({
        "upstream":"ws://127.0.0.1:1/",
        "rule":{"direction":"client_to_server","t":"signal","action":"hold"},
    });
    let mut invalid_configs = Vec::new();
    for close in [
        json!({"code":1006}),
        json!({"code":999}),
        json!({"code":4106,"reason":"\u{e9}".repeat(62)}),
    ] {
        let mut invalid = config.clone();
        invalid["rule"]["action"] = json!({"close":close});
        invalid_configs.push(invalid);
    }
    for upstream in [
        "http://127.0.0.1:1/",
        "wss://127.0.0.1:1/",
        "ws://localhost:1/",
        "ws://192.0.2.1:1/",
        "ws://user@127.0.0.1:1/",
        "ws://127.0.0.1/",
    ] {
        let mut invalid = config.clone();
        invalid["upstream"] = json!(upstream);
        invalid_configs.push(invalid);
    }
    let mut unknown_config_field = config.clone();
    unknown_config_field["unknown"] = json!(true);
    invalid_configs.push(unknown_config_field);
    let mut unknown_rule_field = config;
    unknown_rule_field["rule"]["unknown"] = json!(true);
    invalid_configs.push(unknown_rule_field);
    for config in invalid_configs {
        let mut child = Command::new(env!("CARGO_BIN_EXE_o-sfu-signaling-proxy"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let mut input = child.stdin.take().ok_or("proxy stdin unavailable")?;
        input.write_all(config.to_string().as_bytes()).await?;
        input.write_all(b"\n").await?;
        drop(input);
        let result = timeout(Duration::from_secs(2), child.wait_with_output()).await??;
        assert!(
            !result.status.success(),
            "invalid config accepted: {config}"
        );
        assert!(
            result.stdout.is_empty(),
            "invalid config started listening: {config}"
        );
        assert!(
            !result.stderr.is_empty(),
            "invalid config lacked diagnostics: {config}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn shutdown_flushes_events_while_a_message_is_held() -> TestResult<()> {
    const ARM_COMMANDS: usize = 1024;
    let (upstream, server) = echo_server().await?;
    for stop_command in [false, true] {
        let mut proxy = Proxy::start(
            &upstream,
            json!({
                "direction":"client_to_server", "t":"signal", "action":"hold"
            }),
        )
        .await?;
        let client = service_fn(http_client::send);
        let mut socket = client
            .websocket(proxy.url.as_str())
            .handshake(Extensions::default())
            .await?;
        let id = event_id(&proxy.event("connected").await?)?;
        socket
            .send_message(Message::text(r#"[{"t":"signal"}]"#))
            .await?;
        assert_eq!(event_id(&proxy.event("blocked").await?)?, id);
        // Batch commands so shutdown races queued acknowledgements while a relay remains held.
        let mut commands = "{\"command\":\"arm\"}\n".repeat(ARM_COMMANDS);
        if stop_command {
            commands.push_str("{\"command\":\"stop\"}\n");
        }
        proxy.input.write_all(commands.as_bytes()).await?;
        drop(proxy.input);
        for _ in 0..ARM_COMMANDS {
            let line = timeout(Duration::from_secs(5), proxy.output.next_line())
                .await??
                .ok_or("shutdown lost a queued acknowledgement")?;
            assert_eq!(
                serde_json::from_str::<Value>(&line)?,
                json!({"event":"armed"})
            );
        }
        assert!(
            timeout(Duration::from_secs(5), proxy.child.wait())
                .await??
                .success()
        );
    }
    server.abort();
    Ok(())
}
