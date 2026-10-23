//! Loopback signaling and UDP impairment for O-SFU browser tests.
//!
//! Stdin takes JSON [`Config`], then [`Command`] lines. EOF stops the proxy.
//! Stdout emits NDJSON events without payloads.
//!
//! Signaling rules fire once across connections. UDP rules compose packet
//! delivery effects per flow. `deferred` waits for `arm` without resetting rules.
//! A signaling hold stalls its direction. UDP delay lets later packets pass.

mod config;
mod events;
mod fault;
mod http_client;
mod proxy;
mod scenario;
mod udp;

use std::{io as std_io, result, sync::Arc};

use rama_core::{error::BoxError, telemetry::tracing};
use serde::Deserialize;
use tokio::io::{self, AsyncBufReadExt, BufReader, Lines, Stdin};
use tracing_subscriber::{
    Layer as _, filter::Targets, fmt, layer::SubscriberExt, util::SubscriberInitExt,
};

use self::{
    config::{Command, Config},
    events::EventWriter,
    scenario::Scenario,
};

type Result<T> = result::Result<T, BoxError>;

#[derive(Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    ClientToServer,
    ServerToClient,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Rama traces relay transport failures without returning them to middleware.
    // Limit tracing targets to keep HTTP headers and payloads out of logs.
    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .with_writer(std_io::stderr)
                .with_ansi(false)
                .without_time()
                .with_target(false)
                .with_filter(
                    Targets::new()
                        .with_target("o_sfu_signaling_proxy", tracing::Level::INFO)
                        .with_target("rama_ws::handshake::mitm", tracing::Level::DEBUG),
                ),
        )
        .init();
    let mut lines = BufReader::new(io::stdin()).lines();
    let first = lines
        .next_line()
        .await?
        .ok_or("missing configuration line")?;
    let Config {
        upstream,
        rule,
        deferred,
        udp,
    } = Config::parse(&first)?;
    let writer = EventWriter::new();
    let scenario = Arc::new(Scenario::new(rule, udp, deferred, writer.sender()));
    let server = proxy::bind(upstream, Arc::clone(&scenario), writer.sender()).await?;
    let result = tokio::select! {
        () = server => Ok(()),
        result = commands(&mut lines, &scenario) => result,
    };
    // Connection scopes stop packet tasks before the stdout end marker.
    scenario.shutdown().await;
    writer.finish().await;
    result
}

async fn commands(lines: &mut Lines<BufReader<Stdin>>, scenario: &Scenario) -> Result<()> {
    while let Some(line) = lines.next_line().await? {
        match serde_json::from_str::<Command>(&line)? {
            Command::Arm => scenario.arm(),
            Command::Release { connection_id } => scenario.release(connection_id).await,
            Command::Stats { request_id } => scenario.stats(request_id),
            Command::Stop => break,
        }
    }
    Ok(())
}
