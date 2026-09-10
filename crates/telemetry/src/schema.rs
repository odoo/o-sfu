//! telemetry event and field names
//!
//! runtime logs, traces and diagnostics use these constants so observability
//! names are reviewed in one place
//! call sites should import schema names instead of hard-codding event or field
//! strings when the name is part of the public telemetry contract

pub mod event {
    pub const RUNTIME_LOG: &str = "runtime.log";
    pub const HTTP_LISTENER_READY: &str = "http.listener.ready";
    pub const RUNTIME_BOOT: &str = "runtime.boot";
    pub const RUNTIME_TELEMETRY_INITIALIZED: &str = "runtime.telemetry_initialized";
    pub const ROOM_CREATED: &str = "room.created";
    pub const ROOM_RESERVATION_EXPIRED: &str = "room.reservation.expired";
    pub const ROOM_RESERVATION_CONFLICT: &str = "room.reservation_conflict";
    pub const ROOM_DESTROYED: &str = "room.destroyed";
    pub const USER_JOINED: &str = "user.joined";
    pub const USER_CLOSED: &str = "user.closed";
    pub const USER_DISCONNECTED: &str = "user.disconnected";
    pub const USERS_BULK_DISCONNECTED: &str = "users.bulk_disconnected";
    pub const WS_CONNECTION_CLOSED: &str = "ws.closed";
    pub const WS_HANDSHAKE_REJECTED: &str = "ws.handshake_rejected";
    pub const WS_JOIN_FAILED: &str = "ws.join_failed";
    pub const NEGOTIATION_FAILED: &str = "negotiation.failed";
    pub const PUBLISH_COMMITTED: &str = "publish.committed";
    pub const PUBLISH_ABORTED: &str = "publish.aborted";
    pub const SUBSCRIBE_SUCCEEDED: &str = "subscribe.succeeded";
    pub const SUBSCRIBE_REJECTED: &str = "subscribe.rejected";
    pub const SUBSCRIPTION_ACTIVITY_CHANGED: &str = "subscription.activity_changed";
    pub const PUBLICATION_ACTIVITY_CHANGED: &str = "publication.activity_changed";
    pub const SOURCE_POLICY_ROUTE_CHANGED: &str = "source_policy.route_changed";
    pub const TRANSPORT_HEALTH_CHANGED: &str = "transport.health.changed";
}

pub mod field {
    pub const ROOM_ID: &str = "room_id";
    pub const CLOSE_CODE: &str = "close_code";
    pub const CONNECTION_ID: &str = "connection_id";
    pub const DEPLOYMENT_ENVIRONMENT: &str = "deployment.environment";
    pub const DURATION_MS: &str = "duration_ms";
    pub const ERROR_KIND: &str = "error_kind";
    pub const EVENT: &str = "event";
    pub const LEVEL: &str = "level";
    pub const MESSAGE: &str = "message";
    pub const MEDIA_WORKER_ID: &str = "media_worker_id";
    pub const OPERATION: &str = "operation";
    pub const OUTCOME: &str = "outcome";
    pub const REASON: &str = "reason";
    pub const REMOTE_ADDRESS: &str = "remote_address";
    pub const SERVICE_INSTANCE_ID: &str = "service.instance.id";
    pub const SERVICE_NAME: &str = "service.name";
    pub const SERVICE_VERSION: &str = "service.version";
    pub const USER_ID: &str = "user_id";
    pub const TARGET: &str = "target";
    pub const TIMESTAMP: &str = "timestamp";
    pub const TRACE_ID: &str = "trace_id";
    pub const TRANSPORT_MEDIA_ID: &str = "transport_media_id";
}

pub const COMMON_FIELD_NAMES: &[&str] = &[
    field::TIMESTAMP,
    field::LEVEL,
    field::EVENT,
    field::MESSAGE,
    field::TARGET,
    field::SERVICE_NAME,
    field::SERVICE_VERSION,
    field::SERVICE_INSTANCE_ID,
    field::DEPLOYMENT_ENVIRONMENT,
];

pub const CORRELATION_FIELD_NAMES: &[&str] = &[
    field::ROOM_ID,
    field::USER_ID,
    field::CONNECTION_ID,
    field::REMOTE_ADDRESS,
    field::TRANSPORT_MEDIA_ID,
    field::MEDIA_WORKER_ID,
    field::TRACE_ID,
    field::OPERATION,
    field::OUTCOME,
    field::REASON,
    field::CLOSE_CODE,
    field::ERROR_KIND,
    field::DURATION_MS,
];
