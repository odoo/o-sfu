use std::{io, net::SocketAddr, sync::Arc};

use axum::{
    Router,
    extract::{DefaultBodyLimit, MatchedPath, Path, Request, State},
    http::{StatusCode, header},
    middleware,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use o_sfu_core::server::room::RoomManagerServeError;
use o_sfu_protocol::wire::StreamType;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info};

use crate::{
    application::stream_catalog::counter_for_stream_type,
    runtime::{
        RuntimeState, diagnostics,
        http_server::{
            contract::{
                IncomingBitRateStatsResponse, NoopResponse, RoomResponse, RoomStatsResponse,
                UsersStatsResponse, route,
            },
            extractors::{
                DiagnosticsServices, MetricsServices, OperatorAccess, OperatorAccessPolicy,
                RoomServices, VerifiedDisconnectClaims, VerifiedRoomRequest,
            },
        },
        metrics::{HttpRoute, RuntimeMetrics},
        prometheus::{PROMETHEUS_CONTENT_TYPE, render_prometheus},
        room::RuntimeRoomStatsSnapshot,
        telemetry::{self, schema::event as telemetry_event},
        websocket_server,
    },
};

const MAX_DISCONNECT_BODY_BYTES: usize = 16 * 1024;

pub(crate) async fn serve_http(
    state: RuntimeState,
    shutdown_token: CancellationToken,
) -> io::Result<()> {
    let listener = TcpListener::bind(state.config.http.bind_address).await?;
    serve_http_on(listener, state, shutdown_token).await
}

pub(crate) async fn serve_http_on(
    listener: TcpListener,
    state: RuntimeState,
    shutdown_token: CancellationToken,
) -> io::Result<()> {
    let local_address = listener.local_addr()?;
    info!(
        event = telemetry_event::HTTP_LISTENER_READY,
        bind_address = %state.config.http.bind_address,
        local_address = %local_address,
        trust_proxy_headers = state.config.http.trust_proxy_headers,
        "booted HTTP and WebSocket listener"
    );
    axum::serve(
        listener,
        app(state, local_address).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_token.cancelled_owned())
    .await
}

/// builds the Axum router for the HTTP control plane and WebSocket listener
pub(crate) fn app(state: RuntimeState, listener_address: SocketAddr) -> Router {
    let metrics = Arc::clone(&state.metrics);
    let operator_policy = OperatorAccessPolicy::new(
        state.config.diagnostics.auth_token.as_deref(),
        listener_address,
    );
    Router::new()
        .route(route::WEBSOCKET, get(websocket_server::upgrade))
        .merge(http_router(metrics, operator_policy.clone()))
        .merge(diagnostics_router(operator_policy))
        .with_state(state)
}

fn http_router(
    runtime_metrics: Arc<RuntimeMetrics>,
    operator_policy: OperatorAccessPolicy,
) -> Router<RuntimeState> {
    let operator_layer =
        middleware::from_extractor_with_state::<OperatorAccess, _>(operator_policy);
    Router::new()
        .route(route::v1::NOOP, get(noop))
        .route(
            route::v1::STATS,
            get(stats).route_layer(operator_layer.clone()),
        )
        .route(route::v1::CHANNEL, get(room))
        .route(
            route::v1::DISCONNECT,
            post(disconnect).layer(DefaultBodyLimit::max(MAX_DISCONNECT_BODY_BYTES)),
        )
        .route(route::METRICS, get(metrics).route_layer(operator_layer))
        .route_layer(middleware::from_fn_with_state(
            runtime_metrics,
            track_http_request,
        ))
}

async fn track_http_request(
    State(metrics): State<Arc<RuntimeMetrics>>,
    path: MatchedPath,
    request: Request,
    next: middleware::Next,
) -> Response {
    let route = match path.as_str() {
        route::v1::NOOP => HttpRoute::Noop,
        route::v1::STATS => HttpRoute::Stats,
        route::v1::CHANNEL => HttpRoute::Room,
        route::v1::DISCONNECT => HttpRoute::Disconnect,
        route::METRICS => HttpRoute::Metrics,
        _ => return next.run(request).await,
    };
    let _guard = metrics.track_http_request(route);
    next.run(request).await
}

fn diagnostics_router(operator_policy: OperatorAccessPolicy) -> Router<RuntimeState> {
    Router::new()
        .route(route::diagnostics::SUMMARY, get(diagnostics_summary))
        .route(route::diagnostics::ROOMS, get(diagnostics_rooms))
        .route(route::diagnostics::WORKERS, get(diagnostics_workers))
        .route(route::diagnostics::ROOM, get(diagnostics_room_detail))
        .route(route::diagnostics::ROOM_USERS, get(diagnostics_room_users))
        .route(route::diagnostics::ROOM_USER, get(diagnostics_user_detail))
        .route(route::diagnostics::ROOM_GRAPH, get(diagnostics_room_graph))
        .route(route::diagnostics::USER_GRAPH, get(diagnostics_user_graph))
        .route_layer(middleware::from_extractor_with_state::<OperatorAccess, _>(
            operator_policy,
        ))
}

/// liveness endpoint for a cheap control-plane round trip
async fn noop() -> impl IntoResponse {
    async { axum::Json(NoopResponse::ok()) }
        .instrument(telemetry::http_request_span("noop"))
        .await
}

/// compatibility room-stat endpoint consumed by Odoo's SFU control plane
async fn stats(State(services): State<RoomServices>) -> impl IntoResponse {
    async {
        axum::Json(
            services
                .room_manager
                .stats_snapshots(&services.media_transport)
                .await
                .into_iter()
                .map(http_room_stats)
                .collect::<Vec<_>>(),
        )
    }
    .instrument(telemetry::http_request_span("stats"))
    .await
}

/// prometheus scrape endpoint for process, room, HTTP and media-transport metrics
async fn metrics(State(services): State<MetricsServices>) -> impl IntoResponse {
    async {
        let room_gauges = services.room_manager.room_gauges().await;
        (
            [(header::CONTENT_TYPE, PROMETHEUS_CONTENT_TYPE)],
            render_prometheus(&services.metrics, room_gauges),
        )
    }
    .instrument(telemetry::http_request_span("metrics"))
    .await
}

/// room creation endpoint used by Odoo to bind a channel key to an SFU room
///
/// `VerifiedRoomRequest` owns JWT verification and request-origin projection
async fn room(State(services): State<RoomServices>, request: VerifiedRoomRequest) -> Response {
    async {
        let serve_result = services
            .room_manager
            .serve_room(
                &request.issuer,
                request.room_key,
                &request.config,
                Some(request.origin.remote_address.as_str()),
            )
            .await;
        match serve_result {
            Ok(room) => {
                services.metrics.record_http_room_success();
                (
                    StatusCode::OK,
                    axum::Json(RoomResponse {
                        uuid: room.uuid().to_owned(),
                        url: request.origin.base_url,
                    }),
                )
                    .into_response()
            }
            Err(RoomManagerServeError::ConflictingReservation) => {
                services.metrics.record_http_room_conflict();
                StatusCode::CONFLICT.into_response()
            }
        }
    }
    .instrument(telemetry::http_request_span("room"))
    .await
}

/// bulk-disconnect endpoint used by Odoo to remove users from active rooms
///
/// `VerifiedDisconnectClaims` owns JWT verification and request-body decoding
async fn disconnect(
    State(services): State<RoomServices>,
    VerifiedDisconnectClaims(claims): VerifiedDisconnectClaims,
) -> Response {
    async {
        for (room_id, user_ids) in &claims.user_ids_by_room {
            services
                .room_manager
                .disconnect_users(room_id, user_ids, &services.media_transport)
                .await;
        }
        services.metrics.record_http_disconnect_success();
        StatusCode::OK.into_response()
    }
    .instrument(telemetry::http_request_span("disconnect"))
    .await
}

/// diagnostics overview for room, user and publication totals
async fn diagnostics_summary(State(services): State<DiagnosticsServices>) -> Response {
    axum::Json(
        diagnostics::summary_response(&services.room_manager, &services.media_transport).await,
    )
    .into_response()
}

/// diagnostics inventory for active rooms
async fn diagnostics_rooms(State(services): State<DiagnosticsServices>) -> Response {
    axum::Json(diagnostics::rooms_response(&services.room_manager, &services.media_transport).await)
        .into_response()
}

/// diagnostics inventory for media workers and load pressure
async fn diagnostics_workers(State(services): State<DiagnosticsServices>) -> Response {
    axum::Json(
        diagnostics::workers_response(&services.room_manager, &services.media_transport).await,
    )
    .into_response()
}

/// live room diagnostics with users and sources
async fn diagnostics_room_detail(
    State(services): State<DiagnosticsServices>,
    Path(room_id): Path<String>,
) -> Response {
    let payload = diagnostics::room_detail_response(
        &services.room_manager,
        &services.media_transport,
        &room_id,
    )
    .await;
    diagnostics_optional_response(payload)
}

/// live user rows for one room
async fn diagnostics_room_users(
    State(services): State<DiagnosticsServices>,
    Path(room_id): Path<String>,
) -> Response {
    let payload = diagnostics::room_users_response(
        &services.room_manager,
        &services.media_transport,
        &room_id,
    )
    .await;
    diagnostics_optional_response(payload)
}

/// node-graph projection for one room diagnostics payload
async fn diagnostics_room_graph(
    State(services): State<DiagnosticsServices>,
    Path(room_id): Path<String>,
) -> Response {
    let payload = diagnostics::room_detail_response(
        &services.room_manager,
        &services.media_transport,
        &room_id,
    )
    .await
    .map(|payload| diagnostics::build_graph(&payload));
    diagnostics_optional_response(payload)
}

/// node-graph projection rooted at one user in one room
async fn diagnostics_user_graph(
    State(services): State<DiagnosticsServices>,
    Path((room_id, user_key)): Path<(String, String)>,
) -> Response {
    let payload = diagnostics::room_detail_response(
        &services.room_manager,
        &services.media_transport,
        &room_id,
    )
    .await
    .and_then(|payload| diagnostics::build_user_graph(&payload, &user_key));
    diagnostics_optional_response(payload)
}

fn diagnostics_optional_response<T>(payload: Option<T>) -> Response
where
    axum::Json<T>: IntoResponse,
{
    payload.map_or_else(
        || StatusCode::NOT_FOUND.into_response(),
        |payload| axum::Json(payload).into_response(),
    )
}

/// diagnostics for one user in one room
async fn diagnostics_user_detail(
    State(services): State<DiagnosticsServices>,
    Path((room_id, user_key)): Path<(String, String)>,
) -> Response {
    diagnostics_optional_response(
        diagnostics::user_detail_response(
            &services.room_manager,
            &services.media_transport,
            &room_id,
            &user_key,
        )
        .await,
    )
}

fn http_room_stats(snapshot: RuntimeRoomStatsSnapshot) -> RoomStatsResponse {
    let incoming_bitrate = &snapshot.users_stats.incoming_bitrate;
    let active_stream_counts = &snapshot.users_stats.active_stream_counts;
    RoomStatsResponse {
        create_date: snapshot.create_date,
        uuid: snapshot.uuid,
        remote_address: snapshot.remote_address,
        users_stats: UsersStatsResponse {
            incoming_bit_rate: IncomingBitRateStatsResponse {
                total: incoming_bitrate.total,
                audio: counter_for_stream_type(&incoming_bitrate.by_stream, StreamType::Audio),
                camera: counter_for_stream_type(&incoming_bitrate.by_stream, StreamType::Camera),
                screen: counter_for_stream_type(&incoming_bitrate.by_stream, StreamType::Screen),
            },
            count: snapshot.users_stats.count,
            camera_count: counter_for_stream_type(active_stream_counts, StreamType::Camera),
            screen_count: counter_for_stream_type(active_stream_counts, StreamType::Screen),
        },
        web_rtc_enabled: snapshot.web_rtc_enabled,
    }
}
