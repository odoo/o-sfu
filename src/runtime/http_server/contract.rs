//! HTTP control-plane contracts
//!
//! Odoo uses these paths and payloads to create rooms, disconnect users and read
//! runtime stats

use serde::{Deserialize, Serialize};

pub mod route {
    pub const WEBSOCKET: &str = "/";
    /// Prometheus scrape endpoint, not a `PromQL` API.
    /// See [`crate::http::telemetry::metrics`] for queries and examples.
    pub const METRICS: &str = "/metrics";

    pub mod v1 {
        pub const NOOP: &str = "/v1/noop";
        /// Compatibility channel statistics.
        pub const STATS: &str = "/v1/stats";
        pub const CHANNEL: &str = "/v1/channel";
        pub const DISCONNECT: &str = "/v1/disconnect";
    }

    /// Diagnostics `GET` routes.
    pub mod diagnostics {
        /// returns [`crate::http::telemetry::diagnostics::DiagnosticsSummaryResponse`].
        pub const SUMMARY: &str = "/internal/diagnostics/summary";
        /// returns a JSON array of
        /// [`crate::http::telemetry::diagnostics::DiagnosticsRoomSummary`].
        pub const ROOMS: &str = "/internal/diagnostics/rooms";
        /// returns a JSON array of
        /// [`crate::http::telemetry::diagnostics::DiagnosticsWorkerSummary`].
        pub const WORKERS: &str = "/internal/diagnostics/workers";
        /// returns [`crate::http::telemetry::diagnostics::DiagnosticsRoomDetail`]
        /// or `404 Not Found`.
        pub const ROOM: &str = "/internal/diagnostics/rooms/{uuid}";
        /// returns a JSON array of
        /// [`crate::http::telemetry::diagnostics::DiagnosticsUserSummary`] or
        /// `404 Not Found`.
        pub const ROOM_USERS: &str = "/internal/diagnostics/rooms/{uuid}/users";
        /// returns [`crate::http::telemetry::diagnostics::DiagnosticsUserDetail`]
        /// or `404 Not Found`.
        pub const ROOM_USER: &str = "/internal/diagnostics/rooms/{uuid}/users/{id}";
    }
}

/// noop response payload
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoopResponse {
    pub result: String,
}

impl NoopResponse {
    #[must_use]
    pub fn ok() -> Self {
        Self {
            result: "ok".to_owned(),
        }
    }
}

/// channel creation query parameters
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateRoomQuery {
    #[serde(rename = "webRTC", skip_serializing_if = "Option::is_none")]
    pub web_rtc: Option<bool>,
    /// compatibility field preserved until persistent recording output lands
    #[serde(rename = "recordingAddress", skip_serializing_if = "Option::is_none")]
    pub recording_address: Option<String>,
}

impl CreateRoomQuery {
    #[must_use]
    pub fn web_rtc_enabled(&self) -> bool {
        self.web_rtc.unwrap_or(true)
    }
}

/// created-room response payload
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoomResponse {
    pub uuid: String,
    pub url: String,
}

/// incoming bitrate stats by compatibility stream type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IncomingBitRateStatsResponse {
    pub total: u64,
    pub screen: u64,
    pub audio: u64,
    pub camera: u64,
}

/// active-user stats for one room
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsersStatsResponse {
    pub incoming_bit_rate: IncomingBitRateStatsResponse,
    pub count: u64,
    pub camera_count: u64,
    pub screen_count: u64,
}

/// stats entry for one active room
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RoomStatsResponse {
    pub create_date: String,
    pub uuid: String,
    pub remote_address: String,
    #[serde(rename = "sessionsStats")]
    pub users_stats: UsersStatsResponse,
    pub web_rtc_enabled: bool,
}

/// stats response payload
pub type StatsResponse = Vec<RoomStatsResponse>;

#[cfg(test)]
#[path = "TESTS/contract.rs"]
mod tests;
