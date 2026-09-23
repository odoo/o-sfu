use std::{
    io,
    net::SocketAddr,
    pin::pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Result, anyhow};
use axum::{
    Router,
    extract::{ConnectInfo, WebSocketUpgrade},
    routing::get,
};
use futures_util::poll;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Notify, Semaphore},
    task::yield_now,
    time::{Instant, advance, pause, resume, timeout},
};
use tokio_tungstenite::connect_async;
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};

use super::{AdmittedSocket, serve_connection, serve_connections};
use crate::{
    config::HttpConfig,
    runtime::{
        RuntimeMetrics,
        telemetry::metrics::{MetricName, test_support::RuntimeMetricsSnapshotLookup},
    },
};

const HEADER_TIMEOUT: Duration = Duration::from_secs(10);
const TEST_TIMEOUT: Duration = Duration::from_secs(2);

fn config() -> HttpConfig {
    HttpConfig {
        bind_address: SocketAddr::from(([127, 0, 0, 1], 0)),
        trust_proxy_headers: false,
        trusted_proxies: Vec::new(),
        max_http_connections: 1,
        header_read_timeout: HEADER_TIMEOUT,
        shutdown_timeout_ms: 10_000,
    }
}

async fn socket_pair() -> Result<(TcpStream, TcpStream, SocketAddr)> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let client = TcpStream::connect(listener.local_addr()?).await?;
    let (server, peer) = listener.accept().await?;
    Ok((client, server, peer))
}

fn start_connection(
    server: TcpStream,
    peer: SocketAddr,
    deadline: Instant,
) -> Result<(AbortOnDropHandle<()>, Arc<Semaphore>)> {
    let permits = Arc::new(Semaphore::new(1));
    let task = tokio::spawn(serve_connection(
        AdmittedSocket {
            stream: server,
            _permit: Arc::clone(&permits).try_acquire_owned()?,
        },
        Router::new().route("/", get(|| async { "ok" })),
        peer,
        deadline,
        HEADER_TIMEOUT,
        CancellationToken::new(),
    ));
    Ok((AbortOnDropHandle::new(task), permits))
}

async fn assert_closed(client: &mut TcpStream) -> Result<()> {
    let mut bytes = Vec::new();
    match timeout(TEST_TIMEOUT, client.read_to_end(&mut bytes)).await? {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::ConnectionReset => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

async fn response(client: &mut TcpStream) -> Result<String> {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        bytes.push(client.read_u8().await?);
    }
    let header = String::from_utf8(bytes)?;
    let length = header
        .lines()
        .find_map(|line| line.strip_prefix("content-length: "))
        .ok_or_else(|| anyhow!("response must specify content length"))?
        .parse::<usize>()?;
    let mut body = vec![0; length];
    client.read_exact(&mut body).await?;
    Ok(String::from_utf8(body)?)
}

#[tokio::test]
async fn initial_headers_expire_from_acceptance() -> Result<()> {
    for first_poll_delay in [Duration::ZERO, Duration::from_secs(9)] {
        for prefix in [
            b"".as_slice(),
            b"G",
            b"PRI * HT",
            b"GET / HTTP/1.1\r\nHost: ",
        ] {
            let (mut client, server, peer) = socket_pair().await?;
            client.write_all(prefix).await?;
            pause();
            let accepted_at = Instant::now();
            advance(first_poll_delay).await;
            let (task, permits) = start_connection(server, peer, accepted_at + HEADER_TIMEOUT)?;
            yield_now().await;
            advance(Duration::from_secs(9).saturating_sub(first_poll_delay)).await;
            assert!(
                !task.is_finished(),
                "partial input closed before its deadline"
            );
            advance(Duration::from_secs(1)).await;
            timeout(TEST_TIMEOUT, task).await??;
            assert_eq!(permits.available_permits(), 1);
            resume();
            assert_closed(&mut client).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn delayed_task_does_not_extend_first_header_deadline() -> Result<()> {
    let (mut client, server, peer) = socket_pair().await?;
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await?;
    pause();
    let accepted_at = Instant::now();
    advance(HEADER_TIMEOUT).await;
    let (task, permits) = start_connection(server, peer, accepted_at + HEADER_TIMEOUT)?;
    timeout(TEST_TIMEOUT, task).await??;
    assert_eq!(Instant::now(), accepted_at + HEADER_TIMEOUT);
    assert_eq!(permits.available_permits(), 1);
    resume();
    assert_closed(&mut client).await
}

#[tokio::test]
async fn expired_first_headers_never_reach_router_after_suspension() -> Result<()> {
    let (mut client, server, peer) = socket_pair().await?;
    let server = server.into_std()?;
    let readiness = TcpStream::from_std(server.try_clone()?)?;
    let server = TcpStream::from_std(server)?;
    let requests = Arc::new(AtomicUsize::new(0));
    let received = Arc::clone(&requests);
    let router = Router::new().route(
        "/",
        get(move || {
            received.fetch_add(1, Ordering::Relaxed);
            async { "ok" }
        }),
    );
    let permits = Arc::new(Semaphore::new(1));
    let accepted_at = Instant::now();
    let mut connection = pin!(serve_connection(
        AdmittedSocket {
            stream: server,
            _permit: Arc::clone(&permits).try_acquire_owned()?,
        },
        router,
        peer,
        accepted_at + HEADER_TIMEOUT,
        HEADER_TIMEOUT,
        CancellationToken::new(),
    ));
    assert!(poll!(connection.as_mut()).is_pending());
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await?;
    timeout(TEST_TIMEOUT, readiness.readable()).await??;
    pause();
    assert!(Instant::now() < accepted_at + HEADER_TIMEOUT);
    // Advancing stops at its first yield so the clock passes the deadline
    // without letting the timer driver notify the registered sleeps.
    let remaining = accepted_at + HEADER_TIMEOUT - Instant::now();
    let mut advance_clock = pin!(advance(remaining));
    assert!(poll!(advance_clock.as_mut()).is_pending());
    assert_eq!(Instant::now(), accepted_at + HEADER_TIMEOUT);
    let finished = poll!(connection.as_mut()).is_ready();
    assert_eq!(requests.load(Ordering::Relaxed), 0);
    if !finished {
        timeout(TEST_TIMEOUT, connection).await?;
    }
    assert_eq!(requests.load(Ordering::Relaxed), 0);
    assert_eq!(permits.available_permits(), 1);
    resume();
    Ok(())
}

#[tokio::test]
async fn subsequent_request_headers_have_a_deadline() -> Result<()> {
    let (mut client, server, peer) = socket_pair().await?;
    let (task, permits) = start_connection(server, peer, Instant::now() + HEADER_TIMEOUT)?;
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await?;
    assert_eq!(timeout(TEST_TIMEOUT, response(&mut client)).await??, "ok");
    client.write_all(b"GET / HTTP/1.1\r\nHost: ").await?;
    pause();
    yield_now().await;
    advance(HEADER_TIMEOUT).await;
    timeout(TEST_TIMEOUT, task).await??;
    assert_eq!(permits.available_permits(), 1);
    resume();
    assert_closed(&mut client).await
}

async fn start_listener(
    router: Router,
) -> Result<(
    SocketAddr,
    AbortOnDropHandle<()>,
    CancellationToken,
    Arc<RuntimeMetrics>,
)> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let address = listener.local_addr()?;
    let shutdown = CancellationToken::new();
    let metrics = Arc::new(RuntimeMetrics::default());
    let server = tokio::spawn(serve_connections(
        listener,
        router,
        config(),
        Arc::clone(&metrics),
        shutdown.clone(),
    ));
    Ok((address, AbortOnDropHandle::new(server), shutdown, metrics))
}

#[tokio::test]
async fn upgrade_retains_capacity_until_socket_closes() -> Result<()> {
    let closed = Arc::new(Notify::new());
    let socket_closed = Arc::clone(&closed);
    let router = Router::new()
        .route(
            "/",
            get(move |upgrade: WebSocketUpgrade| {
                let closed = Arc::clone(&socket_closed);
                async move {
                    upgrade.on_upgrade(move |mut socket| async move {
                        while socket.recv().await.is_some() {}
                        drop(socket);
                        closed.notify_one();
                    })
                }
            }),
        )
        .route(
            "/peer",
            get(|ConnectInfo(peer): ConnectInfo<SocketAddr>| async move { peer.to_string() }),
        );
    let (address, server, shutdown, metrics) = start_listener(router).await?;
    let (socket, _) = timeout(TEST_TIMEOUT, connect_async(format!("ws://{address}/"))).await??;
    let mut rejected = TcpStream::connect(address).await?;
    assert_closed(&mut rejected).await?;
    assert_eq!(
        metrics
            .snapshot()
            .counter_value(MetricName::HttpConnectionRejectionsTotal, &[]),
        1
    );
    drop(socket);
    timeout(TEST_TIMEOUT, closed.notified()).await?;
    let mut admitted = TcpStream::connect(address).await?;
    let peer = admitted.local_addr()?;
    admitted
        .write_all(b"GET /peer HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await?;
    assert_eq!(
        timeout(TEST_TIMEOUT, response(&mut admitted)).await??,
        peer.to_string()
    );
    shutdown.cancel();
    timeout(TEST_TIMEOUT, server).await??;
    assert_closed(&mut admitted).await
}

#[tokio::test]
async fn shutdown_drains_an_accepted_request() -> Result<()> {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let route_entered = Arc::clone(&entered);
    let route_release = Arc::clone(&release);
    let router = Router::new().route(
        "/",
        get(move || {
            let entered = Arc::clone(&route_entered);
            let release = Arc::clone(&route_release);
            async move {
                entered.notify_one();
                release.notified().await;
                "ok"
            }
        }),
    );
    let (address, server, shutdown, _metrics) = start_listener(router).await?;
    let mut client = TcpStream::connect(address).await?;
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await?;
    timeout(TEST_TIMEOUT, entered.notified()).await?;
    shutdown.cancel();
    assert!(!server.is_finished());
    release.notify_one();
    assert_eq!(timeout(TEST_TIMEOUT, response(&mut client)).await??, "ok");
    timeout(TEST_TIMEOUT, server).await??;
    assert_closed(&mut client).await
}

#[tokio::test]
async fn shutdown_closes_connection_awaiting_first_headers() -> Result<()> {
    let (mut client, server, peer) = socket_pair().await?;
    client.write_all(b"GET / HTTP/1.1\r\nHost: ").await?;
    timeout(TEST_TIMEOUT, server.readable()).await??;
    let shutdown = CancellationToken::new();
    let permits = Arc::new(Semaphore::new(1));
    let mut connection = pin!(serve_connection(
        AdmittedSocket {
            stream: server,
            _permit: Arc::clone(&permits).try_acquire_owned()?,
        },
        Router::new().route("/", get(|| async { "ok" })),
        peer,
        Instant::now() + HEADER_TIMEOUT,
        HEADER_TIMEOUT,
        shutdown.clone(),
    ));
    assert!(poll!(connection.as_mut()).is_pending());
    shutdown.cancel();
    timeout(Duration::from_millis(100), connection).await?;
    assert_eq!(permits.available_permits(), 1);
    assert_closed(&mut client).await
}

#[tokio::test]
async fn cancellation_closes_unfinished_http_sockets() -> Result<()> {
    let router = Router::new().route("/", get(|| async { "ok" }));
    let (address, server, _shutdown, _metrics) = start_listener(router).await?;
    let mut client = TcpStream::connect(address).await?;
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await?;
    assert_eq!(timeout(TEST_TIMEOUT, response(&mut client)).await??, "ok");
    client.write_all(b"GET / HTTP/1.1\r\n").await?;
    server.abort();
    assert!(server.await.is_err());
    assert_closed(&mut client).await
}
