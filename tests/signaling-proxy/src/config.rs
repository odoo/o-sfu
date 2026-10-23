use rama_net::uri::Uri;
use serde::Deserialize;

use crate::{fault::Rule, udp};

/// Configuration from the first stdin line. `upstream` must use `ws://`,
/// host `127.0.0.1`, an explicit port and no userinfo.
pub struct Config {
    pub upstream: Uri,
    pub rule: Option<Rule>,
    pub udp: Option<udp::Config>,
    /// Keep both transports unimpaired until stdin receives an `arm` command.
    pub deferred: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    upstream: String,
    rule: Option<Rule>,
    udp: Option<udp::Config>,
    #[serde(default)]
    deferred: bool,
}

impl Config {
    /// Returns `BoxError` for invalid JSON, upstream URLs or fault configuration.
    pub fn parse(line: &str) -> crate::Result<Self> {
        let raw: RawConfig = serde_json::from_str(line)?;
        let upstream: Uri = raw.upstream.parse()?;
        let local_upstream = upstream.scheme_str() == Some("ws")
            && upstream.authority().is_some_and(|authority| {
                authority.userinfo().is_none()
                    && authority.host().to_str() == "127.0.0.1"
                    && authority.port_u16().is_some()
            });
        if !local_upstream {
            return Err("upstream must be a ws://127.0.0.1:PORT URL".into());
        }
        if raw.rule.is_none() && raw.udp.is_none() {
            return Err("configure a signaling rule or UDP relay".into());
        }
        if let Some(rule) = &raw.rule {
            rule.validate()?;
        }
        if let Some(udp) = &raw.udp {
            udp.validate()?;
        }
        Ok(Self {
            upstream,
            rule: raw.rule,
            udp: raw.udp,
            deferred: raw.deferred,
        })
    }
}

#[derive(Deserialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
pub enum Command {
    Arm,
    Release {
        #[serde(rename = "connectionId")]
        connection_id: u64,
    },
    Stats {
        #[serde(rename = "requestId")]
        request_id: u64,
    },
    Stop,
}
