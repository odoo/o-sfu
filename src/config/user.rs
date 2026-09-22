use std::time::Duration;

use anyhow::{Result, ensure};

use super::{
    UserConfig,
    env::{Env, positive},
};
use crate::core::server::room::{
    DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY, DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
};

/// Upper bound for duration settings. Past a day, a deadline derived from
/// `Instant::now()` stops being operationally meaningful, and a value this
/// large is far likelier a unit mistake than intent.
const MAX_DURATION_SECS: u64 = 24 * 60 * 60;

impl UserConfig {
    pub(super) fn from_env(env: &Env<'_>) -> Result<Self> {
        Ok(Self {
            room_size: env.var("ROOM_SIZE").check(positive).default(100)?,
            timeout_ms: env.var("USER_TIMEOUT_MS").check(positive).default(10_000)?,
            ping_interval_ms: env
                .var("PING_INTERVAL_MS")
                .check(positive)
                .default(60_000)?,
            outbound_queue_capacity: env
                .var("USER_OUTBOUND_QUEUE_CAPACITY")
                .check(positive)
                .default(DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY)?,
            outbound_queue_byte_capacity: env
                .var("USER_OUTBOUND_QUEUE_BYTE_CAPACITY")
                .check(positive)
                .default(DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY)?,
            room_reservation_ttl: env
                .var("ROOM_RESERVATION_TTL")
                .check(bounded_duration)
                .default(Duration::from_mins(1))?,
            departure_grace: env
                .var("ROOM_DEPARTURE_GRACE")
                .check(bounded_duration)
                .default(Duration::from_mins(1))?,
        })
    }
}

fn bounded_duration(key: &'static str, value: Duration) -> Result<Duration> {
    ensure!(
        value.as_secs() <= MAX_DURATION_SECS,
        "{key} must not exceed {MAX_DURATION_SECS} seconds"
    );
    Ok(value)
}
