//! Private WebRTC backend below [`MediaTransport`](super::MediaTransport).
//!
//! Each [`RtcWorker`] drives its sessions through one shared UDP socket and
//! bounded mailboxes. [`control`] and [`packet_loop`] share [`recovery`]:
//!
//! ```text
//! worker
//!   +-- commands ------> control ------+
//!   |                                  +--> recovery
//!   `-- media packets -> packet_loop --+
//!                            |
//!                            `--> consumer_egress
//! ```
//!
//! [`state::PacketLoopState`] holds the sessions, media identity, routes and
//! scheduling indexes used by these operations. Its lifecycle methods keep
//! dependent indexes and stream retirement together. [`state::RtcSnapshotState`]
//! exposes observations without granting access to that mutable state.
//!
//! [`worker::loop_driver`] governs turn ordering. [`consumer_egress`] provides
//! receiver RTP writes. Packet processing and recovery do not call command handlers.
//!
//! [`commands`] defines shared mailbox contracts, [`codec`] provides codec rules
//! and [`bootstrap`] initializes sockets and sessions.

use std::{sync::Arc, time::Duration};

use crate::{SessionBitrateLimits, VideoBitrateLimits};

#[cfg(test)]
#[expect(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;
#[cfg(feature = "internal-benchmarks")]
#[path = "TESTS/benchmark_support/mod.rs"]
pub mod benchmark_support;
mod bootstrap;
mod codec;
mod commands;
mod consumer_egress;
mod control;
#[cfg(any(test, fuzzing))]
#[path = "TESTS/fuzz_support/mod.rs"]
pub(crate) mod fuzz_support;
mod packet_loop;
mod recovery;
mod state;
#[cfg(any(test, feature = "testing-transport", feature = "internal-benchmarks"))]
#[path = "TESTS/test_support/mod.rs"]
pub mod test_support;
mod worker;

pub(super) use codec::RtpProfile;
#[cfg(any(test, fuzzing))]
pub use codec::client_rtp_capabilities_from_answer;
pub(super) use commands::{ParsedSessionAnswer, RtcSessionOffer};
pub use commands::{
    RtcWorkerCommand, RtcWorkerResponse, WorkerMediaControlBatch, WorkerMediaControlBatchOutcome,
};
pub use worker::RtcWorker;

#[cfg(any(test, feature = "testing-transport"))]
pub use self::packet_loop::forwarded_packet::ForwardedPacket;
pub(super) use self::state::route_control::PacketLayerGate;

#[derive(Clone, Debug)]
struct RtcWorkerConfig {
    bitrate_limits: SessionBitrateLimits,
    video_bitrate_limits: VideoBitrateLimits,
    profile: Arc<RtpProfile>,
    media_quality_interval: Option<Duration>,
    media_id_base: u64,
}
