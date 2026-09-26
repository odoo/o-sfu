use std::{net::SocketAddr, time::Duration};

use anyhow::{Context, Result, ensure};
use ipnet::IpNet;
use tokio::sync::Semaphore;

use super::{DeadlineDuration, HttpConfig, env::Env};

impl HttpConfig {
    /// Validates proxy policy and listener limits before runtime startup.
    ///
    /// # Errors
    /// Returns [`anyhow::Error`] for proxy mode without trusted proxies, zero or
    /// unsupported semaphore capacity or a header deadline outside one second
    /// through one day.
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            !self.trust_proxy_headers || !self.trusted_proxies.is_empty(),
            "TRUSTED_PROXIES must contain at least one proxy CIDR when PROXY=true"
        );
        ensure!(
            (1..=Semaphore::MAX_PERMITS).contains(&self.max_http_connections),
            "MAX_HTTP_CONNECTIONS must be between 1 and {}",
            Semaphore::MAX_PERMITS
        );
        ensure!(
            (Duration::from_secs(1)..=Duration::from_hours(24)).contains(&self.header_read_timeout),
            "HEADER_READ_TIMEOUT must be between 1 and 86400 seconds"
        );
        Ok(())
    }

    pub(super) fn from_env(env: &Env<'_>) -> Result<Self> {
        let trust_proxy_headers = env.var("PROXY").default(false)?;
        let trusted_proxies = env
            .var::<String>("TRUSTED_PROXIES")
            .optional()?
            .map(|raw| {
                raw.split(',')
                    .map(|cidr| {
                        cidr.trim()
                            .parse::<IpNet>()
                            .context("TRUSTED_PROXIES must be a comma-separated list of IP CIDRs")
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?
            .unwrap_or_default();
        let config = Self {
            bind_address: env
                .var("BIND_ADDRESS")
                .default(SocketAddr::from(([0, 0, 0, 0], 8070)))?,
            trust_proxy_headers,
            trusted_proxies,
            max_http_connections: env.var("MAX_HTTP_CONNECTIONS").default(4096)?,
            header_read_timeout: env
                .var("HEADER_READ_TIMEOUT")
                .default(Duration::from_secs(10))?,
            shutdown_timeout: env
                .var("SHUTDOWN_TIMEOUT_MS")
                .default(DeadlineDuration::from_millis(10_000)?)?,
        };
        config.validate()?;
        Ok(config)
    }
}
