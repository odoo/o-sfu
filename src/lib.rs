//! Odoo's Selective Forwarding Unit (SFU) for audio/video calls.
//!
//! `o-sfu` receives participants' audio and video and forwards selected streams
//! to other participants in the same room. Each sender uploads media to the SFU
//! instead of sending a separate copy to every receiver.
//!
//! ```text
//! Alice -- audio/video --> SFU --+--> Bob
//!                                |
//!                                +--> Carol
//! ```
//!
//! A room groups the participants, published media and subscriptions for a call.
//! One server can host independently authenticated rooms for multiple tenants,
//! such as different Odoo databases. This fits the Odoo saas and Odoo.sh
//! architecture.
//!
//! # A call through the SFU
//!
//! The application backend provisions a room and gives participants credentials
//! to join it. Browser clients use WebSocket signaling to join, publish media
//! and choose what to receive. Media travels separately from signaling, over WebRTC.
//!
//! ```text
//! Odoo backend -- HTTP ------> room creation and access
//!                                       |
//!                                       v
//! Browser <---- WebSocket ---> room membership and media requests
//!                                       |
//!                                       v
//! Browser <---- WebRTC/UDP --> media workers <----> other browsers
//! ```
//!
//! This separates decisions about the call from the work of forwarding its
//! media. Signaling changes which streams should flow. Media workers deliver
//! those streams as packets arrive.
//!
//! # Architecture
//!
//! [`Runtime`] brings together the HTTP/WebSocket servers, rooms and media
//! workers. The HTTP control API manages rooms through
//! [`RoomManager`](core::server::room::RoomManager). WebSocket sessions admit
//! participants through [`SfuCore`](core::prelude::SfuCore) and use a
//! [`MediaSession`](core::prelude::MediaSession) for each connection to publish
//! media and manage subscriptions.
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
//! A [`Room`](core::server::room::Room) manages who is present and which media
//! they publish or receive. The [`Router`](o_sfu_router::Router) keeps
//! track of the connections needed to deliver that media. Its decisions do not
//! perform network I/O, so routing behavior can be tested without sockets.
//!
//! [`MediaTransport`](core::server::transport::MediaTransport) runs the worker
//! threads and uses `str0m` for WebRTC. Room policy selects media using
//! subscriptions, video layout, active speakers and receiver bandwidth. Each
//! worker forwards incoming media to the selected receivers, shown as fanout
//! in the diagram.
//!
//! Rooms start with one worker. [`config::RoomWorkerPolicy`] can spread new
//! joins across additional workers in the same process. Participants still
//! belong to one room and can receive media from participants on other workers.
//!
//! # Access and encryption
//!
//! The backend signs tokens for room creation and disconnect requests with
//! the server's `AUTH_KEY`. Participant tokens are verified with their room's
//! signing key. This lets a tenant authorize its participants without giving
//! them the server key. See [`auth`] for token types and verification.
//!
//! Admission establishes participant identity and room scope. It does not
//! enforce the per-user `permissions` claim supplied by the tenant.
//!
//! Signaling and media need separate transport protection:
//!
//! - An external reverse proxy terminates HTTPS and WSS. Proxy mode requires
//!   configured trusted proxies and the public edge must overwrite
//!   client-supplied forwarded host and protocol headers. See [`config`] and
//!   [`http::resolve_request_origin`].
//! - `str0m` encrypts media between each browser and the SFU with DTLS-SRTP.
//!   Media UDP must reach the server's configured `ANNOUNCED_IP`.
//!
//! # Browser client
//!
//! The JavaScript bundle exposes `SfuClient`, the Odoo-facing API for joining
//! calls, publishing media and subscribing to other participants. For example,
//! with a room token supplied by the application:
//!
//! ```javascript
//! import { SfuClient } from "/bundle/odoo_sfu.js";
//!
//! const sfu = new SfuClient();
//! sfu.connect("https://sfu.example.com/ws", jsonWebToken, {
//!     channelUUID: "mail-channel-uuid",
//! });
//! ```
//!
//! `connect()` returns before authentication and media negotiation finish.
//! The application observes `stateChange` and `handledError` for the result.
//!
//! The client shares its signaling logic through
//! [`o_sfu_protocol::host::ProtocolCore`]. The browser adapter runs that Rust
//! logic as WebAssembly and connects it to browser WebSocket and WebRTC APIs.
//! This keeps protocol decisions separate from browser-specific operations.
//!
//! # Running and observing the server
//!
//! [`run`] starts the server from configuration. Use [`Runtime`] to embed it
//! with a caller-provided listener and shutdown signal. [`config`] controls
//! network addresses, admission limits and media worker policy at runtime.
//! The default Cargo feature `otel-tracing` enables OpenTelemetry tracing.
//!
//! [`http::telemetry::metrics`] exposes Prometheus metrics for monitoring over
//! time. [`http::telemetry::diagnostics`] exposes room, participant and worker
//! state for investigating a call. These operator endpoints require separate
//! access control, described in [`http`].
//!
//! # Where to read next
//!
//! | Area | Entry point |
//! | --- | --- |
//! | Configuration and server lifecycle | [`config`], [`Runtime`] and [`run`] |
//! | Authentication and signaling messages | [`auth`], [`http`] and [`websocket`] |
//! | Room behavior and media transport | [`o_sfu_core`] |
//! | Connections and media routing | [`o_sfu_router`] |
//! | Client signaling and Odoo bundle compatibility | [`o_sfu_protocol`] |
//! | Metrics, diagnostics and tracing | [`o_sfu_telemetry`] |
//! | Shared call identifiers | `o-sfu-model`, including [`o_sfu_protocol::wire::UserId`] |
//! | Protocol constants and types | [`o_sfu_rfc`] |
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

#[cfg(any(test, feature = "testing"))]
pub mod test_support {
    pub use crate::runtime::auth::test_support::TestHttpRoomClaims;
}

/// HTTP route and payload contracts.
///
/// `/v1/stats`, `/metrics` and diagnostics require the configured
/// [`crate::config::DiagnosticsConfig::auth_token`] on every listener. Without
/// one, the actual listener must be loopback. Missing or invalid tokens return
/// `401 Unauthorized` with `WWW-Authenticate: Bearer realm="o-sfu"`.
/// Tokenless non-loopback access returns `403 Forbidden`.
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
        /// Prometheus metrics for monitoring calls and server load.
        ///
        /// `GET` [`metrics::PATH`] returns `200 OK` in Prometheus text exposition
        /// format with content type [`metrics::CONTENT_TYPE`]. Configure
        /// Prometheus to scrape this endpoint. Send `PromQL` queries to
        /// Prometheus, for example:
        ///
        /// ```promql
        /// sum(osfu_users_active)
        /// sum by (stage) (rate(osfu_ws_connections_total[5m]))
        /// ```
        ///
        /// [`metrics::MetricName`] lists exported names and their meanings.
        /// The scrape response includes each metric's `HELP`, `TYPE` and labels.
        pub mod metrics {
            pub use o_sfu_telemetry::{
                metrics::MetricName, prometheus::PROMETHEUS_CONTENT_TYPE as CONTENT_TYPE,
            };

            pub use crate::http::route::METRICS as PATH;
        }

        /// JSON snapshots for investigating rooms, participants and workers.
        ///
        /// Each route in [`diagnostics::route`] accepts `GET` and returns
        /// `200 OK` JSON on success. The response types document their fields.
        ///
        /// | Request | Response |
        /// | --- | --- |
        /// | `/internal/diagnostics/summary` | [`diagnostics::DiagnosticsSummaryResponse`] |
        /// | `/internal/diagnostics/rooms` | Array of [`diagnostics::DiagnosticsRoomSummary`] |
        /// | `/internal/diagnostics/workers` | Array of [`diagnostics::DiagnosticsWorkerSummary`] |
        /// | `/internal/diagnostics/rooms/{uuid}` | [`diagnostics::DiagnosticsRoomDetail`] |
        /// | `/internal/diagnostics/rooms/{uuid}/users` | Array of [`diagnostics::DiagnosticsUserSummary`] |
        /// | `/internal/diagnostics/rooms/{uuid}/users/{id}` | [`diagnostics::DiagnosticsUserDetail`] |
        ///
        /// Use `uuid` from the rooms response and `userKey` from the room users
        /// response. `userId` may be a number or string, while `userKey` is
        /// always the string for `{id}`. URL-encode both path values.
        ///
        /// ```http
        /// GET /internal/diagnostics/summary HTTP/1.1
        /// Host: o-sfu-observability.internal
        /// Authorization: Bearer <diagnostics-token>
        /// Accept: application/json
        /// ```
        ///
        /// Wire names use `camelCase` unless a field documents an exception.
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
