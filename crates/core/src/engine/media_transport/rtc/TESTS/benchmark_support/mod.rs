//! feature-gated packet-loop benchmark fixtures
//!
//! the types in this module build deterministic scenarios for Callgrind
//! benchmarks without creating a second packet-loop model
//! each measured method calls the same RTC-engine helpers used by the worker
//! packet loop

mod active_speaker;
mod consumer_gates;
mod drain;
mod fanout;
mod ingress;
mod local_send;
mod meeting;
mod observation;
mod relay;
mod remote_gates;
mod scheduler;
mod sinks;
mod video;
#[cfg(feature = "internal-benchmarks")]
mod worker;

pub use active_speaker::ActiveSpeakerBenchFixture;
pub use consumer_gates::ConsumerGateBatchBenchFixture;
pub use drain::{RelayDrainBenchFixture, SessionDrainBenchFixture};
pub use fanout::{FanoutBenchTopology, ROUTE_PLANNING_TURNS};
pub use ingress::{
    INGRESS_COMPLETED_BURST_DATAGRAMS, INGRESS_DEMUX_ATTEMPTS, IngressBurstBenchFixture,
    IngressRoutingBenchFixture,
};
pub use local_send::LocalSendBenchFixture;
pub use meeting::{
    MEETING_ADMITTED_AUDIO_SOURCES, MEETING_LONG_SECONDS, MEETING_PARTICIPANTS,
    MEETING_SHORT_SECONDS, MEETING_TICK_MS, MEETING_VIDEO_PUBLISHERS, MEETING_VIDEO_SUBSCRIPTIONS,
    MeetingFlowBenchFixture, MeetingWorkProfile,
};
pub use observation::IncomingObservationBenchFixture;
pub use relay::{RELAY_MAILBOX_ATTEMPTS, RelayPressureBenchFixture};
pub use remote_gates::{REMOTE_GATE_RETRY_TURNS, RemoteGateRetryBenchFixture};
pub use scheduler::SchedulerBenchFixture;
pub use sinks::{PACKET_SINK_FANOUT_TURNS, PacketSinkFanoutBenchFixture};
pub use video::{
    KEYFRAME_COALESCING_REQUESTS, KeyframeCoalescingBenchFixture, RidReadinessBenchFixture,
    SELECTED_RID_DESTINATIONS,
};
#[cfg(feature = "internal-benchmarks")]
pub use worker::{
    WORKER_COMMAND_ROUNDTRIPS, WORKER_PACKET_COMMAND_MIX_PACKETS, WorkerLoopBenchFixture,
    WorkerPacketCommandMixBenchFixture,
};

pub use super::{
    consumer_egress::LocalRewriteBenchFixture,
    packet_loop::routing_miss::packet_fingerprint_for_benchmark as routing_miss_packet_fingerprint,
};
