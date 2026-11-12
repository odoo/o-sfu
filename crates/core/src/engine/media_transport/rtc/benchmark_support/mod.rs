//! feature-gated packet-loop benchmark fixtures
//!
//! the types in this module build deterministic scenarios for Callgrind
//! benchmarks without creating a second packet-loop model
//! each measured method calls the same RTC-engine helpers used by the worker
//! packet loop

#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "fixed benchmark fixtures must fail on invalid setup or missing coverage"
)]

mod active_speaker;
mod consumer_gates;
mod drain;
mod fanout;
mod ingress;
mod interleaved;
mod local_send;
mod meeting;
mod observation;
mod relay;
mod remote_gates;
mod scheduler;
mod sinks;
mod video;
mod worker;

pub use active_speaker::ActiveSpeakerBenchFixture;
pub use consumer_gates::ConsumerGateBatchBenchFixture;
pub use drain::{RelayDrainBenchFixture, SessionDrainBenchFixture};
pub use fanout::{FanoutBenchTopology, ROUTE_PLANNING_TURNS, RelayFanoutBenchFixture};
pub use ingress::{IngressBurstBenchFixture, IngressRoutingBenchFixture};
pub use interleaved::InterleavedRelayActivityBenchFixture;
pub use local_send::LocalSendBenchFixture;
pub use meeting::{MeetingFlowBenchFixture, MeetingWorkProfile};
pub use observation::IncomingObservationBenchFixture;
pub use relay::{RELAY_MAILBOX_ATTEMPTS, RelayPressureBenchFixture};
pub use remote_gates::RemoteGateRetryBenchFixture;
pub use scheduler::SchedulerBenchFixture;
pub use sinks::PacketSinkFanoutBenchFixture;
pub use video::{KeyframeCoalescingBenchFixture, RidReadinessBenchFixture};
pub use worker::WorkerLoopBenchFixture;

pub use super::{
    consumer_egress::LocalRewriteBenchFixture,
    packet_loop::routing_miss::packet_fingerprint_for_benchmark as routing_miss_packet_fingerprint,
};
