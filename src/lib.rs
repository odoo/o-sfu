//! Odoo's Selective Forwarding Unit (SFU) for audio/video calls.
//!
//! `o-sfu` provides a dedicated server and an embeddable Rust runtime for room
//! admission, media policy, signaling and packet forwarding. Applications
//! provision rooms over HTTP, browsers connect over WebSocket and media travels
//! over UDP without transcoding or mixing.
//!
//! `str0m` supplies the WebRTC state machine for ICE, DTLS and SRTP. `o-sfu`
//! supplies the UDP sockets, worker threads and packet loops that drive it.
//!
//! # Reading Map
//!
//! - **Start or embed the server**: [`run`], [`Runtime`] and [`config`].
//! - **Understand admission**: [`auth`], [`http`] and [`websocket`].
//! - **Change room and media behavior**: [`core::server::room::Room`],
//!   [`o_sfu_router::Router`] and [`core::server::transport::MediaTransport`].
//! - **Integrate a browser client**: `SfuClient` exposes the Odoo-facing API.
//!   [`o_sfu_protocol::host::ProtocolCore`] supplies its signaling state machine.
//!
//!
//! # Core Concepts
//!
//! `o-sfu` separates the **control plane** for admission and room policy, the
//! **routing plane** for user-to-connection placement and the **packet plane**
//! for RTP forwarding on worker loops.
//!
//! - **[`Runtime`]**: Owns the process lifecycle, the HTTP/WebSocket servers and graceful shutdown.
//! - **[`core::server::room::Room`]**: The control plane boundary for a set of participants. It commits membership and media relationships.
//! - **[`o_sfu_router::Router`]**: The routing plane, a pure, sans-I/O engine owning the placement graph that maps users to connections.
//! - **[`core::prelude::MediaSession`]**: Orchestrates a user's connection, bridging room intent to transport effects.
//! - **[`core::server::transport::MediaTransport`]**: Owns the media workers and abstracts their threading model. Each worker holds a packet loop and applies projected routes to incoming datagrams.
//!
//! # Architecture
//!
//! Control and routing decisions happen above the packet loops, which apply the
//! resulting transport state to UDP datagrams.
//!
//! ```text
//! HTTP control API ------------------------------> RoomManager
//!                                                       |
//! WebSocket session -> SfuCore -> MediaSession          |
//!                                    \                  /
//!                                     +----> Room <----+
//!                                             |
//!                                             +<----> Router
//!                                             |
//!                                             v
//!                                       MediaTransport
//!                                        |    |    |
//!                                        v    v    v
//!                          RTC workers / packet loops
//!                           (one thread per worker)
//!                         |            |             |
//!                      UDP socket   UDP socket   UDP socket
//!                         |            |             |
//!                         v            v             v
//!                      fanout       fanout        fanout
//!                      UDP OUT      UDP OUT       UDP OUT
//! ```
//!
//! # Admission Edge
//!
//! Applications provision rooms through
//! [`RoomManager::serve_room`](core::server::room::RoomManager::serve_room).
//! WebSocket clients join through
//! [`SfuCore::admit_user`](core::prelude::SfuCore::admit_user). Both paths require
//! JWT authentication before admission.
//!
//! ```text
//! Incoming HTTP / WebSocket I/O
//!     |
//!     v
//! Axum Router
//!     |
//!     +-> GET /v1/noop
//!     |     public liveness -> noop
//!     |
//!     +-> GET /v1/channel
//!     |     Authorization JWT -> VerifiedRoomRequest -> room
//!     |
//!     +-> POST /v1/disconnect
//!     |     request-body JWT -> VerifiedDisconnectClaims -> disconnect
//!     |
//!     +-> WebSocket /
//!     |     upgrade -> first-frame JWT -> admit_user -> session
//!     |
//!     +-> GET /v1/stats, /metrics and /internal/diagnostics/...
//!           authorize_operator -> stats, metrics or diagnostics
//! ```
//!
//! - **HTTP**: Parses server-to-server requests using [`http::CreateRoomQuery`]. Verifies [`auth::HttpRoomClaims`]. The request that creates the current room fixes its signing key.
//! - **WebSocket**: Client connection frames are decoded by [`websocket::decode_auth_payload_text`]. A hint selects a candidate key. Claims are verified, normalized into [`auth::WebSocketConnectClaims`] and trusted for access.
//!
//! # Security Model
//!
//! `o-sfu` secures two planes independently. Application-layer JWTs gate room
//! admission on the control plane. `str0m` encrypts media on the packet plane
//! with DTLS-SRTP. An external reverse proxy provides TLS for HTTP and
//! WebSocket signaling.
//!
//! ```text
//! control plane    JWT HS256           admission trust
//! packet plane     DTLS-SRTP (str0m)   media confidentiality
//! signaling wire   TLS (via proxy)     transport confidentiality
//! ```
//!
//! ## JWT Admission
//!
//! Tokens are `HS256` only. [`auth::verify`] rejects any other `alg`, checks the
//! HMAC in constant time and validates `exp`, `nbf` and the `iat` future-skew
//! bound when those claims are present. It caps token size at
//! [`auth::MAX_JWT_TOKEN_BYTES`].
//!
//! There are two different keys:
//!
//! - **Server-to-server key**: `AUTH_KEY` (base64, at least 32 bytes) verifies
//!   the HTTP [`http::CreateRoomQuery`] path through [`auth::HttpRoomClaims`] and
//!   [`auth::HttpDisconnectClaims`]. See [`config`].
//! - **Per-room key**: the request that creates the current room pins the signing
//!   key from the `key` or `keySeed` claim in [`auth::HttpRoomClaims`]. For more
//!   security, prefer the `keySeed` claim, which derives a per-room key with the
//!   `AUTH_KEY` and provided seed using the following KDF:
//!   ```text
//!   room_key = Base64StdPad(HMAC-SHA256(
//!       key = Base64Decode(AUTH_KEY),
//!       message = Base64Decode(keySeed)
//!   ))
//!   ```
//!   WebSocket [`auth::WebSocketConnectClaims`] verify against that room key,
//!   never against `AUTH_KEY`.
//!
//! HTTP room creation uses the
//! `Authorization` header, HTTP disconnect uses the request body and the
//! WebSocket client sends a first-frame auth envelope decoded by
//! [`websocket::decode_auth_payload_text`]. An unverified room id selects only a
//! candidate key, then the same token is re-verified against it. Modern
//! [`auth::WebSocketConnectClaims`] must name the selected room. Legacy Odoo
//! tokens select it through the auth envelope's `channel` and are normalized
//! only after verification with that room's key.
//!
//! Admission establishes identity and room scope. It does not enforce the
//! per-user `permissions` claim provided by each tenant.
//!
//! ## Media Transport
//!
//! `str0m` handles ICE, DTLS and SRTP over UDP. `o-sfu` builds and drives the
//! `str0m` sessions to forward RTP between participants.
//!
//! - **Keying**: the DTLS handshake derives SRTP keys per RFC 5764 DTLS-SRTP.
//! - **Certificate**: `str0m` generates a self-signed certificate when each RTC
//!   session is built and advertises its SHA-256 fingerprint in the SDP offer.
//!   Accepting the answer stores the expected remote fingerprint. The DTLS
//!   handshake verifies it against the peer certificate.
//! - **ICE**: `o-sfu` runs ICE-lite with `a=setup:actpass` and advertises
//!   `ANNOUNCED_IP`, so media UDP must reach the host directly.
//!
//! ## Signaling Transport
//!
//! HTTPS and WSS are expected to be terminated by an external reverse proxy.
//! Setting `PROXY=true` in [`config`] trusts forwarded headers for every request.
//! It does not restrict trust to selected proxy addresses. Every request must
//! then pass through a proxy that strips or overwrites client-supplied
//! `x-forwarded-*` headers. See [`http::resolve_request_origin`] for origin
//! resolution and [`http`] for operator route access.
//!
//! # Room and Router Ownership
//!
//! Room transitions produce typed commits while holding short exclusive state
//! locks. `RoomEffects` consumes deferred transport, source-policy and WebSocket
//! output work after the lock is released.
//!
//! ```text
//! room state lock held                  lock released
//! +--------------------------------+    +------------------------------+
//! | validate user and connection   |    | MediaTransport commands      |
//! | commit room topology           |    | source-policy turn           |
//! | capture transition commit      |--->| websocket output             |
//! |                                |    | idempotent teardown          |
//! +--------------------------------+    +------------------------------+
//! ```
//!
//! [`o_sfu_router::Router`] owns exact user-to-connection placement. Receiver shadows are foreign local sessions derived from active consumer dependencies, disappearing with their final consumer.
//!
//! # Signaling and Client Bundle
//!
//! Browsers use `SfuClient` for connection, publication, subscription and room
//! control. Signaling state stays in [`o_sfu_protocol::host::ProtocolCore`] and
//! yields ordered [`o_sfu_protocol::host::Command`] values. The WASM bridge
//! serializes commands for `BrowserRuntime`, which executes effects through
//! browser `WebSocket`, `RTCPeerConnection` and timer APIs. Protocol events are
//! mapped to Odoo bundle updates during command serialization. `BrowserRuntime`
//! applies those updates to client state and notifies the application.
//!
//! ```text
//! SfuClient (public API)
//!        |
//!        v
//! BrowserRuntime
//!        |
//!        v
//! ProtocolCore (sans-I/O) -> Vec<Command>
//!        |
//!        v
//! WASM serialization -> TypeScript command union
//!        |
//!        v
//! BrowserRuntime -> WebSocket, RTCPeerConnection, timers
//!        ^                                        |
//!        +--------------- browser events ---------+
//! ```
//!
//! # Packet Path
//!
//! [`core::server::transport::MediaTransport`] owns the media workers, which hold the packet loops. These loops receive UDP datagrams, drive WebRTC state (`str0m`), apply route tables and forward RTP.
//!
//! ```text
//! UDP datagram
//!     |
//!     v
//! worker ingress and `str0m` drain
//!     |
//!     v
//! packet facts (source, RID and codec)
//!     |
//!     +-> origin packet sinks
//!     |
//!     +-> source and destination packet gates
//!              |
//!              +-> relay fanout
//!              +-> local RTC -> RTP identity and codec rewrite
//! ```
//!
//! Registered origin packet sinks observe publisher packets before route gates,
//! including publishers without active receivers. Source, relay and receiver
//! gates then narrow routed fanout. Same-process relays share payload data with
//! another worker for local delivery.
//!
//! Worker BWE and audio observations feed into room source policy, which updates route gates for later packets.
//!
//! ```text
//! worker BWE and audio observations
//!                |
//!                v
//!        room source policy
//!                |
//!                v
//!   route gates for later packets
//! ```
//!
//! # Observability
//!
//! Monitored through the [`o_sfu_telemetry`] sub-crate. See [`http::telemetry`] for the HTTP contracts.
//!
//! - **Metrics**: [`http::telemetry::metrics`] exposes Prometheus text exposition.
//! - **Diagnostics**: [`http::telemetry::diagnostics`] exposes JSON state summaries.
//!
//! # Scaling
//!
//! Rooms use one [`o_sfu_router::Router`] facade and default to one local router.
//! [`config::RoomWorkerPolicy`] can enable additional same-process local routers.
//! Joins then prefer assigned workers with a known delay below the configured
//! threshold. When none qualifies, a join may attach an unused healthy worker
//! within the router cap and worker count. If expansion is unavailable, joins
//! reuse an assigned worker even when it exceeds the delay threshold. The
//! single-router policy always reuses the room's primary placement.
//!
//! # Feature Flags
//!
//! Core media behavior is configured at runtime through [`config`], not Cargo features.
//! The default feature `otel-tracing` enables OpenTelemetry tracing support through [`o_sfu_telemetry::TraceExportConfig`].
//! Other features are only used for tests and benchmarking.
//!
//! # Sub-crates
//!
//! | Crate | Role |
//! | --- | --- |
//! | [`o_sfu_rfc`] | RFC-backed JWT, RTP, RTCP, SDP and WebRTC consts/types |
//! | `o-sfu-model` | Shared call data ([`o_sfu_protocol::wire::UserId`], etc.) |
//! | [`o_sfu_router`] | Sans-I/O [`o_sfu_router::Router`] facade for room placement and routed media lifetimes |
//! | [`o_sfu_core`] | Room engine, [`core::prelude::SourcePolicy`], recording taps and [`core::server::transport::MediaTransport`] projection |
//! | [`o_sfu_protocol`] | Sans-I/O [`o_sfu_protocol::host::ProtocolCore`] and typed commands |
//! | [`o_sfu_telemetry`] | Tracing setup, metrics, diagnostics response types and graph payloads |
pub mod config;
pub mod core {
    pub use o_sfu_core::{prelude, server};
}
pub(crate) mod application;
mod runtime;

pub mod auth {
    pub use crate::runtime::auth::{
        AuthenticationError, HttpDisconnectClaims, HttpRoomClaims, MAX_JWT_TOKEN_BYTES,
        RegisteredJwtClaims, WebSocketConnectClaims, sign, verify,
    };
}

/// HTTP route and payload contracts.
///
/// `/v1/stats`, `/metrics` and diagnostics require the configured
/// [`crate::config::DiagnosticsConfig::auth_token`] on every listener. Without
/// one, the actual listener must be loopback. Missing or invalid tokens return
/// `401 Unauthorized`. Tokenless non-loopback access returns `403 Forbidden`.
pub mod http {
    pub use crate::runtime::{
        http_server::contract::{
            CreateRoomQuery, IncomingBitRateStatsResponse, NoopResponse, RoomResponse,
            StatsResponse, route,
        },
        request_origin::{RequestOrigin, resolve_request_origin},
    };

    /// Operator-facing metrics and diagnostics contracts.
    pub mod telemetry {
        /// Prometheus metric scrape contract.
        ///
        /// `GET` [`metrics::PATH`] returns `200 OK` Prometheus text exposition
        /// with [`metrics::CONTENT_TYPE`].
        ///
        /// This is a scrape endpoint.
        /// Configure Prometheus to scrape [`metrics::PATH`], then issue `PromQL`
        /// queries to Prometheus.
        /// Histogram families render `<name>_bucket` with the additional `le`
        /// label plus `<name>_sum` and `<name>_count`.
        ///
        /// # Scrape and Query
        ///
        /// ```yaml
        /// scrape_configs:
        ///   - job_name: o-sfu
        ///     scheme: https
        ///     metrics_path: /metrics
        ///     authorization:
        ///       type: Bearer
        ///       credentials_file: /run/secrets/o_sfu_diagnostics_token
        ///     tls_config:
        ///       ca_file: /run/secrets/o_sfu_observability_ca
        ///       server_name: o-sfu-observability.internal
        ///     static_configs:
        ///       - targets: ["o-sfu-observability.internal:443"]
        /// ```
        ///
        /// The endpoint returns Prometheus text exposition.
        ///
        /// ```text
        /// # HELP osfu_rooms_active Current number of active rooms owned by this runtime.
        /// # TYPE osfu_rooms_active gauge
        /// osfu_rooms_active 3
        /// # TYPE osfu_worker_rtp_packets_total counter
        /// osfu_worker_rtp_packets_total{media_worker_id="0",direction="ingress"} 1240
        /// ```
        ///
        /// Query clients send `PromQL` to the Prometheus-compatible backend
        /// rather than to o-sfu.
        /// These examples cover a gauge, counter and histogram.
        ///
        /// ```promql
        /// sum(osfu_users_active)
        /// sum by (stage) (rate(osfu_ws_connections_total[5m]))
        /// histogram_quantile(
        ///   0.95,
        ///   sum by (le, route) (rate(osfu_http_request_duration_seconds_bucket[10m]))
        /// )
        /// ```
        ///
        /// Read the [`metrics::MetricName`] variants below to find every
        /// exported name and its meaning.
        /// Query [`metrics::PATH`] to see each family's `HELP`, `TYPE`, label
        /// keys and current label values before building selectors.
        pub mod metrics {
            pub use o_sfu_telemetry::{
                metrics::MetricName, prometheus::PROMETHEUS_CONTENT_TYPE as CONTENT_TYPE,
            };

            pub use crate::http::route::METRICS as PATH;
        }

        /// JSON diagnostics contract.
        ///
        /// Every constant in [`diagnostics::route`] is a `GET` endpoint returning
        /// `200 OK` JSON on success.
        ///
        /// # Routes and Parameters
        ///
        /// | request | JSON response | parameter source |
        /// | --- | --- | --- |
        /// | `GET /internal/diagnostics/summary` | one [`diagnostics::DiagnosticsSummaryResponse`] | none |
        /// | `GET /internal/diagnostics/rooms` | array of [`diagnostics::DiagnosticsRoomSummary`] | none |
        /// | `GET /internal/diagnostics/workers` | array of [`diagnostics::DiagnosticsWorkerSummary`] | none |
        /// | `GET /internal/diagnostics/rooms/{uuid}` | one [`diagnostics::DiagnosticsRoomDetail`] | `uuid` from the rooms response |
        /// | `GET /internal/diagnostics/rooms/{uuid}/users` | array of [`diagnostics::DiagnosticsUserSummary`] | `uuid` from the rooms response |
        /// | `GET /internal/diagnostics/rooms/{uuid}/users/{id}` | one [`diagnostics::DiagnosticsUserDetail`] | `uuid` from rooms and `userKey` from room users |
        /// | `GET /internal/diagnostics/node-graph/rooms/{uuid}` | `{ "nodes": [], "edges": [] }` | `uuid` from the rooms response |
        /// | `GET /internal/diagnostics/node-graph/rooms/{uuid}/users/{id}` | `{ "nodes": [], "edges": [] }` | `uuid` from rooms and `userKey` from room users |
        ///
        /// `userId` may be a JSON number or string.
        /// `userKey` is always the string to put into `{id}`.
        /// URL-encode both path values before substitution.
        ///
        /// # Summary Request and Response over HTTPS
        ///
        /// ```text
        /// GET /internal/diagnostics/summary HTTP/1.1
        /// Host: o-sfu-observability.internal
        /// Authorization: Bearer <diagnostics-token>
        /// Accept: application/json
        ///
        /// HTTP/1.1 200 OK
        /// Content-Type: application/json
        ///
        /// {
        ///   "roomsActive": 1,
        ///   "publicationsActive": 1,
        ///   "recordingRoomsActive": 0,
        ///   "usersActive": 2,
        ///   "subscriptionsActive": 1,
        ///   "transport": {
        ///     "connectedUsers": 2,
        ///     "disconnectedUsers": 0,
        ///     "totalUsers": 2,
        ///     "unknownUsers": 0
        ///   }
        /// }
        /// ```
        ///
        /// # JavaScript Fetch Example
        ///
        /// ```javascript
        /// const origin = "https://o-sfu-observability.internal";
        /// const headers = {
        ///   Authorization: `Bearer ${process.env.DIAGNOSTICS_AUTH_TOKEN}`,
        /// };
        ///
        /// async function getJson(path) {
        ///   const response = await fetch(`${origin}${path}`, { headers });
        ///   if (!response.ok) {
        ///     throw new Error(`${response.status} ${await response.text()}`);
        ///   }
        ///   return response.json();
        /// }
        ///
        /// async function main() {
        ///   const rooms = await getJson("/internal/diagnostics/rooms");
        ///   const roomUuid = encodeURIComponent(rooms[0].uuid);
        ///   const room = await getJson(`/internal/diagnostics/rooms/${roomUuid}`);
        ///   const users = await getJson(`/internal/diagnostics/rooms/${roomUuid}/users`);
        ///   const userKey = encodeURIComponent(users[0].userKey);
        ///   const graph = await getJson(
        ///     `/internal/diagnostics/node-graph/rooms/${roomUuid}/users/${userKey}`,
        ///   );
        ///
        ///   console.log(room.summary, room.users, room.sources);
        ///   console.log(graph.nodes, graph.edges);
        /// }
        ///
        /// main().catch((error) => {
        ///   console.error(error);
        ///   process.exitCode = 1;
        /// });
        /// ```
        ///
        /// The rooms response has this shape.
        ///
        /// ```json
        /// [
        ///   {
        ///     "createDate": "2026-07-15T10:20:30.000Z",
        ///     "mediaWorkerId": 0,
        ///     "publicationCount": 1,
        ///     "recordingState": {
        ///       "recording": false,
        ///       "audio": false,
        ///       "transcription": false,
        ///       "video": false
        ///     },
        ///     "remoteAddress": "203.0.113.10",
        ///     "sourceCount": 1,
        ///     "userCount": 2,
        ///     "subscriptionCount": 1,
        ///     "transport": {
        ///       "connectedUsers": 2,
        ///       "disconnectedUsers": 0,
        ///       "totalUsers": 2,
        ///       "unknownUsers": 0
        ///     },
        ///     "uuid": "550e8400-e29b-41d4-a716-446655440000",
        ///     "webRtcEnabled": true
        ///   }
        /// ]
        /// ```
        ///
        /// The room users response has this shape.
        ///
        /// ```json
        /// [
        ///   {
        ///     "audioIncomingBitrateBps": 32000,
        ///     "cameraIncomingBitrateBps": 600000,
        ///     "connectionId": 91,
        ///     "health": "connected",
        ///     "incomingBitrateBps": 632000,
        ///     "mediaWorkerId": 0,
        ///     "publicationCount": 2,
        ///     "roomId": "550e8400-e29b-41d4-a716-446655440000",
        ///     "screenIncomingBitrateBps": 0,
        ///     "subscriptionCount": 1,
        ///     "userId": 42,
        ///     "userKey": "42"
        ///   }
        /// ]
        /// ```
        ///
        /// The response structs below list every field in each payload.
        /// Wire names are `camelCase` unless a field documents an exception.
        ///
        /// User detail is room-scoped because the same user key can be active
        /// in several rooms.
        pub mod diagnostics {
            pub use o_sfu_telemetry::diagnostics::{
                DiagnosticsRoomDetail, DiagnosticsRoomSummary, DiagnosticsSummaryResponse,
                DiagnosticsUserDetail, DiagnosticsUserSummary, DiagnosticsWorkerSummary,
            };

            pub use crate::http::route::diagnostics as route;
        }
    }
}

pub mod websocket {
    pub use crate::runtime::websocket_server::{
        ClientBatchDecodeError, ClientBatchDecodeFailureKind, MAX_CLIENT_BATCH_ENVELOPES,
        MAX_CLIENT_FRAME_BYTES, decode_auth_payload_text, decode_client_batch,
    };
}

pub use self::runtime::{Runtime, ServeError, run};
