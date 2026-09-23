use secrecy::SecretString;

#[cfg(unix)]
use std::{
    env,
    process::{self, Command},
};
use std::{
    future::pending,
    io,
    net::SocketAddr,
    sync::{Arc, Weak},
    time::Duration,
};

use anyhow::anyhow;
use o_sfu_core::server::room::RoomConfig;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::{JoinHandle, yield_now},
    time::{sleep, timeout},
};
use tokio_util::{sync::CancellationToken, task::task_tracker::TaskTrackerToken};

use super::{
    AnyResult, RoomManager, RoomPacketSinkRegistry, Runtime, RuntimeServices, ServeError,
    serve_http_on,
    test_support::{RuntimeTestBuilder, test_room_key},
};
use crate::runtime::{auth::AuthenticationError, metrics::RoomGaugeValues};

const TEST_TIMEOUT: Duration = Duration::from_secs(1);

#[test]
fn runtime_rejects_invalid_programmatic_operator_tokens_before_starting_workers() {
    for token in [
        "",
        "                                ",
        "short-secret",
        "                      short-secret                      ",
        "operator-secret-with-at-least-32-bytes\0",
    ] {
        let mut config = RuntimeTestBuilder::new().config().clone();
        config.diagnostics.auth_token = Some(SecretString::from(token));
        let error = Runtime::new(&config).err().map(|error| error.to_string());
        assert!(
            error
                .as_deref()
                .is_some_and(|error| error.starts_with("DIAGNOSTICS_AUTH_TOKEN"))
        );
        if !token.is_empty() {
            assert!(!error.as_deref().is_some_and(|error| error.contains(token)));
        }
    }
}

#[tokio::test]
async fn runtime_accepts_absent_or_minimum_length_operator_token() -> AnyResult<()> {
    for token in [
        None,
        Some("12345678901234567890123456789012"), // DevSkim: ignore DS173237 (synthetic test token)
        Some("  12345678901234567890123456789012 \n"),
    ] {
        let mut config = RuntimeTestBuilder::new().config().clone();
        config.diagnostics.auth_token = token.map(SecretString::from);
        Runtime::new(&config)?
            .serve(|_state, _shutdown| async { Ok(()) }, async { Ok(()) })
            .await?;
    }
    Ok(())
}

#[test]
fn runtime_rejects_invalid_programmatic_auth_keys() -> AnyResult<()> {
    let mut config = RuntimeTestBuilder::new().config().clone();
    for (key, expected) in [
        (
            "invalid-base64!",
            AuthenticationError::InvalidBase64Encoding,
        ),
        ("", AuthenticationError::KeyTooShort),
        ("YQ==", AuthenticationError::KeyTooShort),
    ] {
        config.auth.key = key.into();
        let error = Runtime::new(&config)
            .err()
            .ok_or_else(|| anyhow!("invalid authentication key must fail runtime construction"))?;
        assert_eq!(error.downcast_ref::<AuthenticationError>(), Some(&expected));
    }
    Ok(())
}

#[tokio::test]
async fn runtime_shutdown_cancels_drains_and_preserves_errors() -> AnyResult<()> {
    let runtime = Runtime::new(RuntimeTestBuilder::new().config())?;
    let rooms = Arc::downgrade(&runtime.room_manager);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(runtime.serve(
        move |state, listener_shutdown| {
            let _result = shutdown_tx.send((listener_shutdown, state.session_shutdown));
            pending::<io::Result<()>>()
        },
        pending::<io::Result<()>>(),
    ));
    let (listener_shutdown, session_shutdown) = timeout(TEST_TIMEOUT, shutdown_rx).await??;

    server.abort();
    assert!(server.await.is_err());
    assert!(listener_shutdown.is_cancelled());
    assert!(session_shutdown.is_cancelled());

    timeout(TEST_TIMEOUT, wait_for_room_manager_drop(&rooms)).await?;

    let (shutdown, task, mut server, worker_resources, address) = tracked_runtime(1_000).await?;
    shutdown.cancel();
    timeout(TEST_TIMEOUT, listener_refused(address)).await?;
    assert!(
        timeout(Duration::from_millis(20), &mut server)
            .await
            .is_err()
    );
    assert!(worker_resources.upgrade().is_some());
    drop(task);
    let result = timeout(TEST_TIMEOUT, server).await??;
    result?;
    assert!(worker_resources.upgrade().is_none());

    let (shutdown, task, server, _resources, _address) = tracked_runtime(20).await?;
    shutdown.cancel();
    assert!(matches!(
        timeout(TEST_TIMEOUT, server).await??,
        Err(ServeError::ShutdownIncomplete {
            remaining_sessions: 1
        })
    ));
    drop(task);

    let mut config = RuntimeTestBuilder::new().config().clone();
    let runtime = Runtime::new(&config)?;
    let result = runtime
        .serve(
            |_state, _shutdown| async { Err(io::ErrorKind::BrokenPipe.into()) },
            pending(),
        )
        .await;
    assert!(matches!(
        result,
        Err(ServeError::Io(error)) if error.kind() == io::ErrorKind::BrokenPipe
    ));

    config.http.shutdown_timeout_ms = 20;
    let runtime = Runtime::new(&config)?;
    let (task_tx, task_rx) = oneshot::channel();
    let server = tokio::spawn(runtime.serve(
        move |state, _shutdown| async move {
            let _result = task_tx.send(state.session_tasks.token());
            Err(io::ErrorKind::BrokenPipe.into())
        },
        pending(),
    ));
    let task = task_rx.await?;
    assert!(matches!(
        timeout(TEST_TIMEOUT, server).await??,
        Err(ServeError::ShutdownIncomplete {
            remaining_sessions: 1
        })
    ));
    drop(task);
    Ok(())
}

#[cfg(unix)]
const SIGNAL_CHILD_ENV: &str = "O_SFU_SHUTDOWN_SIGNAL_CHILD";

#[cfg(unix)]
#[tokio::test]
async fn shutdown_signals_stop_isolated_process() -> io::Result<()> {
    if let Ok(signal) = env::var(SIGNAL_CHILD_ENV) {
        let kill = tokio::spawn(async move {
            yield_now().await;
            Command::new("kill")
                .args([format!("-{signal}"), process::id().to_string()])
                .status()
        });
        super::shutdown_signal().await?;
        assert!(kill.await.map_err(io::Error::other)??.success());
        return Ok(());
    }
    let executable = env::current_exe()?;
    for signal in ["INT", "TERM"] {
        let status = Command::new(&executable)
            .args(["shutdown_signals_stop_isolated_process", "--nocapture"])
            .env(SIGNAL_CHILD_ENV, signal)
            .status()?;
        assert!(status.success());
    }
    Ok(())
}

async fn tracked_runtime(
    shutdown_timeout_ms: u64,
) -> AnyResult<(
    CancellationToken,
    TaskTrackerToken,
    JoinHandle<Result<(), ServeError>>,
    Weak<RoomPacketSinkRegistry>,
    SocketAddr,
)> {
    let mut config = RuntimeTestBuilder::new().config().clone();
    config.http.shutdown_timeout_ms = shutdown_timeout_ms;
    let services = RuntimeServices::default();
    let worker_resources = Arc::downgrade(&services.packet_sink_registry);
    let runtime = Runtime::from_services(&config, services)?;
    let shutdown = CancellationToken::new();
    let trigger = shutdown.clone();
    let (task_tx, task_rx) = oneshot::channel();
    let listener = TcpListener::bind(config.http.bind_address).await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(runtime.serve(
        move |state, listener_shutdown| {
            let _result = task_tx.send(state.session_tasks.token());
            serve_http_on(listener, state, listener_shutdown)
        },
        async move {
            trigger.cancelled().await;
            Ok(())
        },
    ));
    Ok((shutdown, task_rx.await?, server, worker_resources, address))
}

async fn listener_refused(address: SocketAddr) {
    while TcpStream::connect(address).await.is_ok() {
        yield_now().await;
    }
}

async fn wait_for_room_manager_drop(rooms: &Weak<RoomManager>) {
    while rooms.upgrade().is_some() {
        sleep(Duration::from_millis(1)).await;
    }
}

/// reservation window that the wait below clears with room for a reaper tick
///
/// reservation orderings are pinned in the room-engine tests. this layer only
/// owns the wiring: the configured window reaches the manager, and the spawned
/// reaper keeps ticking
const RESERVATION_TTL: Duration = Duration::from_secs(30);

#[tokio::test(start_paused = true)]
async fn expired_room_reservation_is_reaped() -> AnyResult<()> {
    let builder = RuntimeTestBuilder::new().room_reservation_ttl(RESERVATION_TTL);
    let runtime = Runtime::new(builder.config())?;
    let rooms = Arc::clone(&runtime.room_manager);
    tokio::spawn(runtime.serve(
        |_state, _listener_shutdown| pending::<io::Result<()>>(),
        pending::<io::Result<()>>(),
    ));
    let config = RoomConfig::default();

    let room = rooms
        .serve_room("issuer", test_room_key(), &config, None)
        .await
        .map_err(|error| anyhow!("test room should be served: {error:?}"))?;
    sleep(RESERVATION_TTL * 2).await;

    assert!(
        rooms.get_by_uuid(room.uuid()).await.is_none(),
        "the reaper task should remove the expired directory row"
    );
    assert_eq!(rooms.room_gauges().await, RoomGaugeValues::default());
    let room_again = rooms
        .serve_room("issuer", test_room_key(), &config, None)
        .await
        .map_err(|error| anyhow!("test room should be served: {error:?}"))?;

    assert_ne!(
        room.uuid(),
        room_again.uuid(),
        "new room request from same issuer after reservation expiration gives new uuid"
    );
    Ok(())
}
