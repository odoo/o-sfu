//! Loopback HTTP upgrades and WebSocket relay lifetimes.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use rama_core::{
    Layer, Service,
    error::{BoxError, ErrorContext},
    io::BridgeIo,
    layer::ArcLayer,
    rt::Executor,
    service::service_fn,
    telemetry::tracing::{self, Instrument},
};
use rama_http::{
    Request, Response, StatusCode, header,
    io::upgrade::Upgraded,
    layer::{error_handling::ErrorHandlerLayer, upgrade::mitm::HttpUpgradeMitmRelayLayer},
};
use rama_http_backend::server::HttpServer;
use rama_net::{address::AuthorityRef, uri::Uri};
use rama_tcp::server::TcpListener;
use rama_ws::handshake::{
    matcher::HttpWebSocketRelayServiceRequestMatcher,
    mitm::{WebSocketRelayEventInput, WebSocketRelayEventService},
};
use tokio::{sync::Notify, time};

use crate::{
    Result,
    events::{Event, Events},
    http_client,
    scenario::Scenario,
};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(1);

/// Binds the listener before commands can stop it. Returns `BoxError` on bind/address failure.
pub async fn bind(
    upstream: Uri,
    scenario: Arc<Scenario>,
    events: Events,
) -> Result<impl Future<Output = ()>> {
    let exec = Executor::default();
    let listener = TcpListener::bind_address("127.0.0.1:0", exec.clone()).await?;
    let address = listener.local_addr()?;
    let next_id = AtomicU64::new(1);
    let target = upstream.clone();
    let handler_events = events.clone();
    let handler = service_fn(move |request: Request| {
        let id = next_id.fetch_add(1, Ordering::Relaxed);
        proxy_request(
            request,
            target.clone(),
            Arc::clone(&scenario),
            handler_events.clone(),
            id,
        )
    });
    let server = HttpServer::new_http1(exec)
        .service((ArcLayer::new(), ErrorHandlerLayer::new()).into_layer(handler));
    let path = upstream.request_target();
    events.emit(Event::Ready {
        url: format!("ws://{address}{path}"),
    });
    Ok(async move {
        listener.serve(server).await;
    })
}

async fn proxy_request(
    request: Request,
    upstream: Uri,
    scenario: Arc<Scenario>,
    events: Events,
    id: u64,
) -> Result<Response> {
    let relay = service_fn(move |bridge: BridgeIo<Upgraded, Upgraded>| {
        relay_connection(bridge, Arc::clone(&scenario), events.clone(), id)
            .instrument(tracing::info_span!("signaling_proxy", connectionId = id))
    });
    let client =
        service_fn(move |request: Request| upstream_request(request, upstream.clone(), id));
    HttpUpgradeMitmRelayLayer::new(
        Executor::default(),
        HttpWebSocketRelayServiceRequestMatcher::new(relay),
    )
    .with_error_sink(move |error: BoxError| {
        tracing::warn!(connectionId = id, %error, "WebSocket upgrade failed");
    })
    .into_layer(client)
    // Erase the nested service future here so Rust can prove Send when
    // this handler is composed into the HTTP listener.
    .boxed()
    .serve(request)
    .await
}

async fn relay_connection(
    bridge: BridgeIo<Upgraded, Upgraded>,
    scenario: Arc<Scenario>,
    events: Events,
    id: u64,
) -> Result<()> {
    let abort = Arc::new(Notify::new());
    let connection = scenario.connect(id, Arc::clone(&abort)).await?;
    let middleware_connection = Arc::clone(&connection);
    let middleware = service_fn(move |input: WebSocketRelayEventInput| {
        let connection = Arc::clone(&middleware_connection);
        // Rama documents OpaqueError for higher-ranked BoxError lifetime bounds.
        async move { connection.apply(input).await.into_opaque_error() }
    });
    let relay =
        WebSocketRelayEventService::new(middleware).with_close_handshake_timeout(CLOSE_TIMEOUT);
    events.emit(Event::Connected { connection_id: id });
    // A middleware error sends close 1011. Drop the entire relay
    // future to model an abrupt transport loss with browser code 1006.
    tokio::select! {
        biased;
        () = abort.notified() => {}
        _ = relay.serve(bridge) => {}
    }
    scenario.disconnect(&connection).await;
    Ok(())
}

async fn upstream_request(mut request: Request, upstream: Uri, id: u64) -> Result<Response> {
    // Only the browser's path/query vary. Neither an absolute URI nor
    // a Host header may change the configured loopback destination.
    request
        .uri_mut()
        .maybe_set_scheme(upstream.scheme().cloned());
    request
        .uri_mut()
        .maybe_set_authority(upstream.authority().map(AuthorityRef::into_owned));
    let host = request
        .uri()
        .authority()
        .ok_or("missing upstream authority")?
        .to_string()
        .parse()?;
    request.headers_mut().insert(header::HOST, host);
    let result = time::timeout(HANDSHAKE_TIMEOUT, http_client::send(request)).await;
    match result {
        Ok(Ok(response)) => {
            if response.status() != StatusCode::SWITCHING_PROTOCOLS {
                tracing::warn!(connectionId = id, status = %response.status(), "upstream handshake failed");
            }
            Ok(response)
        }
        Ok(Err(error)) => {
            tracing::warn!(connectionId = id, %error, "upstream handshake failed");
            Err(error)
        }
        Err(_) => {
            tracing::warn!(connectionId = id, "upstream handshake failed: timed out");
            Err("upstream handshake timed out".into())
        }
    }
}
