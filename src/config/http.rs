use std::net::SocketAddr;

use anyhow::{Context, Result, ensure};
use ipnet::IpNet;

use super::{
    HttpConfig,
    env::{Env, positive},
};

impl HttpConfig {
    /// Validates trusted proxy configuration before runtime startup.
    ///
    /// # Errors
    /// Returns [`anyhow::Error`] for proxy mode without trusted proxies.
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            !self.trust_proxy_headers || !self.trusted_proxies.is_empty(),
            "TRUSTED_PROXIES must contain at least one proxy CIDR when PROXY=true"
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
            shutdown_timeout_ms: env
                .var("SHUTDOWN_TIMEOUT_MS")
                .check(positive)
                .default(10_000)?,
        };
        config.validate()?;
        Ok(config)
    }
}
