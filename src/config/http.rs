use std::net::SocketAddr;

use anyhow::Result;

use super::{
    HttpConfig,
    env::{Env, positive},
};

impl HttpConfig {
    pub(super) fn from_env(env: &Env<'_>) -> Result<Self> {
        Ok(Self {
            bind_address: env
                .var("HTTP_INTERFACE")
                .alias("BIND_ADDRESS")
                .default(SocketAddr::from(([0, 0, 0, 0], 8070)))?,
            trust_proxy_headers: env.var("PROXY").default(false)?,
            shutdown_timeout_ms: env
                .var("SHUTDOWN_TIMEOUT_MS")
                .check(positive)
                .default(10_000)?,
        })
    }
}
