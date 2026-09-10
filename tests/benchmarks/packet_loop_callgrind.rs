//! deterministic Callgrind coverage for packet-loop hot-path slices
//!
//! this suite measures fixed units of packet-loop work with `Ir` and
//! `EstimatedCycles`, which are the instruction-count and simulated cycle-cost
//! metrics reported by Callgrind
//! each benchmark builds the RTC-engine state outside the measured function,
//! then repeats one stable packet-loop operation with reusable buffers
//!
//! the value of this target is base-versus-head review, not throughput proof
//! it catches accidental instruction growth in production packet-loop helpers
//! before that growth becomes visible as lower room fanout, slower ingress
//! routing or extra route-control work under load
//!
//! the measured slices are deliberately narrower than the async worker loop
//! they cover packet observation, route planning, relay enqueue pressure, UDP
//! ingress demux, packet-sink fanout, selected-RID readiness, consumer gate
//! batches, RTP identity rewriting, local RTC sends, active-speaker policy and
//! keyframe-request coalescing without mixing socket waits into the instruction
//! count

#![expect(
    clippy::exit,
    clippy::must_use_candidate,
    clippy::needless_pass_by_value,
    reason = "Gungraun's generated harness owns setup values, returns measured outputs and exits with the runner status"
)]

use std::{hint::black_box, mem::drop};

use gungraun::{library_benchmark, library_benchmark_group, main};
use o_sfu_core::server::transport::benchmark_support::{
    ActiveSpeakerBenchFixture, ConsumerGateBatchBenchFixture, FanoutBenchTopology,
    IncomingObservationBenchFixture, IngressBurstBenchFixture, IngressRoutingBenchFixture,
    KeyframeCoalescingBenchFixture, LocalRewriteBenchFixture, LocalSendBenchFixture,
    PacketSinkFanoutBenchFixture, RelayDrainBenchFixture, RelayFanoutBenchFixture,
    RelayPressureBenchFixture, RemoteGateRetryBenchFixture, RidReadinessBenchFixture,
    SchedulerBenchFixture, SessionDrainBenchFixture, WorkerPacketCommandMixBenchFixture,
    routing_miss_packet_fingerprint,
};

#[path = "callgrind_config.rs"]
mod callgrind_config;

use callgrind_config::callgrind_config;

const ROUTING_MISS_FINGERPRINT_ATTEMPTS: usize = 4096;
fn fanout_topology(destination_count: usize) -> FanoutBenchTopology {
    FanoutBenchTopology::with_local_destinations(destination_count)
}

fn fingerprint_packet(packet_len: usize) -> Vec<u8> {
    let mut packet = Vec::with_capacity(packet_len);
    let sequence_number = 1_u16.to_be_bytes();
    let ssrc = 11_u32.to_be_bytes();
    packet.extend_from_slice(&[
        0x80,
        96,
        sequence_number[0],
        sequence_number[1],
        0,
        0,
        0,
        1,
        ssrc[0],
        ssrc[1],
        ssrc[2],
        ssrc[3],
    ]);
    for byte_index in packet.len()..packet_len {
        let mixed = byte_index
            .wrapping_mul(31)
            .wrapping_add(byte_index.rotate_left(5))
            .wrapping_add(17);
        packet.push(u8::try_from(mixed & 0xff).unwrap_or(0));
    }
    packet
}

// measures local fanout route planning for one producer and fixed local
// destination counts
//
// this protects the dense-room planner path where every extra destination is
// real work, so the useful info is if buffer reuse and route lookup stay
// proportional to the required fanout rather than adding allocator churn or
// unrelated scans
#[library_benchmark(config = callgrind_config(0.5))]
#[bench::fanout_1(args = (1usize), setup = fanout_topology)]
#[bench::fanout_8(args = (8usize), setup = fanout_topology)]
#[bench::fanout_32(args = (32usize), setup = fanout_topology)]
#[bench::fanout_64(args = (64usize), setup = fanout_topology)]
fn route_plan_1024(mut topology: FanoutBenchTopology) -> usize {
    black_box(topology.plan_route_turns())
}

fn validate_relay_gates(mut fixture: RelayFanoutBenchFixture) {
    fixture.assert_gate_selection();
}

#[library_benchmark(config = callgrind_config(0.5), teardown = validate_relay_gates)]
#[bench::mixed_gates(RelayFanoutBenchFixture::mixed_gates())]
fn relay_route_plan_1024(mut fixture: RelayFanoutBenchFixture) -> RelayFanoutBenchFixture {
    black_box(fixture.plan_route_turns());
    black_box(fixture)
}

// measures packet observation over a MID/RID packet followed by an SSRC-only
// packet that relies on learned producer identity, both with generic payloads
// and with a negotiated VP8 keyframe/interframe pair
//
// this protects the packet-loop phase that learns source metadata, updates
// active-speaker state, tracks RID liveness and records incoming bitrate before
// route planning starts
#[library_benchmark(config = callgrind_config(1.0))]
#[bench::mid_rid_then_ssrc(IncomingObservationBenchFixture::mid_rid_then_ssrc())]
#[bench::negotiated_vp8(IncomingObservationBenchFixture::negotiated_vp8())]
fn incoming_observation_512(mut fixture: IncomingObservationBenchFixture) -> usize {
    black_box(fixture.observe_turns())
}

// measures relay enqueue pressure at the production non-blocking mailbox
// boundary
//
// the open and overloaded cases have different expected outcomes but both are
// packet-loop work that can run for every relayed packet
// keeping them cheap preserves cross-worker forwarding under bursty rooms
#[library_benchmark(config = callgrind_config(1.0))]
#[bench::enqueue(RelayPressureBenchFixture::open_mailbox())]
#[bench::overloaded(RelayPressureBenchFixture::full_mailbox())]
fn relay_mailbox_256(fixture: RelayPressureBenchFixture) -> usize {
    black_box(fixture.run_attempts())
}

// measures UDP ingress demux for the indexed happy path and the defensive
// unknown-source miss path
//
// cached accepted routing protects the normal packet ingress path after a
// remote address has been learned
// repeated misses protect the defensive path that must stay bounded when noise
// or stale peers send datagrams that do not belong to a live RTC session
#[library_benchmark(config = callgrind_config(1.0))]
#[bench::cached_route(IngressRoutingBenchFixture::cached_accepted_route())]
#[bench::unknown_source(IngressRoutingBenchFixture::repeated_unknown_source_miss())]
#[bench::unknown_rtp_1200(IngressRoutingBenchFixture::repeated_large_unknown_source_miss())]
fn ingress_demux_256(mut fixture: IngressRoutingBenchFixture) -> usize {
    black_box(fixture.route_datagrams())
}

// measures the completed-datagram ingress boundary in front of demux
//
// this protects the path where the socket receive task obtains a reusable
// buffer, enqueues a completed datagram for the packet loop, then the packet
// loop drains the bounded queue before routing and recycling the packet buffer
#[library_benchmark(config = callgrind_config(1.0))]
#[bench::cached_route(IngressBurstBenchFixture::cached_accepted_route())]
#[bench::unknown_rtp_1200(IngressBurstBenchFixture::repeated_large_unknown_source_miss())]
fn ingress_completed_burst_256(mut fixture: IngressBurstBenchFixture) -> usize {
    black_box(fixture.route_completed_bursts())
}

// measures dirty-session scheduling and lazy stale-timeout cleanup
//
// this protects the packet-loop scheduler path from regressing back to full
// session scans or excessive heap churn while merging dirty sessions and due
// str0m timeouts
#[library_benchmark(config = callgrind_config(1.0), teardown = drop)]
#[bench::stale_timeouts(SchedulerBenchFixture::stale_timeouts())]
fn scheduler_churn_128(mut fixture: SchedulerBenchFixture) -> SchedulerBenchFixture {
    black_box(fixture.collect_ready_and_next_timeout());
    black_box(fixture)
}

// measures the routing-miss fingerprint helper directly with an RTP-shaped
// packet large enough to represent the normal media packet case
//
// this keeps the fingerprint cost visible next to the broader ingress-demux
// benchmark that includes recent-miss cache lookup and drop accounting
#[library_benchmark(config = callgrind_config(1.0))]
#[bench::rtp_1200(args = (1200usize), setup = fingerprint_packet)]
fn fingerprint_4096(packet: Vec<u8>) -> u64 {
    let mut fingerprint = 0_u64;
    for _ in 0..ROUTING_MISS_FINGERPRINT_ATTEMPTS {
        fingerprint = fingerprint.wrapping_add(routing_miss_packet_fingerprint(black_box(
            packet.as_slice(),
        )));
    }
    black_box(fingerprint)
}

// measures packet-sink fanout through production route planning and flush
// delivery
//
// recording sinks share the packet-loop origin side with media forwarding
// this benchmark keeps that adjacent path visible so recording support cannot
// quietly add per-packet cost to rooms that are already forwarding media
#[library_benchmark(config = callgrind_config(1.0))]
#[bench::recording(PacketSinkFanoutBenchFixture::recording_sink())]
fn packet_sink_512(mut fixture: PacketSinkFanoutBenchFixture) -> usize {
    black_box(fixture.route_sink_turns())
}

// measures selected-RID packet-gate batch updates for many consumers attached
// to one source
//
// this protects dense room policy changes from adding per-consumer lookup cost
// beyond the required destination validation and one aggregate source refresh
fn validate_route_gate_batch(fixture: ConsumerGateBatchBenchFixture) {
    assert!(fixture.updates_applied());
}

#[library_benchmark(config = callgrind_config(1.0), teardown = validate_route_gate_batch)]
#[bench::consumers_64(ConsumerGateBatchBenchFixture::consumers_64())]
#[bench::consumers_256(ConsumerGateBatchBenchFixture::consumers_256())]
fn route_gate_batch(fixture: ConsumerGateBatchBenchFixture) -> ConsumerGateBatchBenchFixture {
    black_box(fixture.apply_updates())
}

// measures remote packet-gate retry queue pressure while source-worker control
// mailboxes stay saturated
//
// this protects remote gate convergence from linear queue dedupe and front
// drain movement when relay pressure prevents immediate control delivery
#[library_benchmark(config = callgrind_config(1.0))]
#[bench::sources_64(RemoteGateRetryBenchFixture::sources_64())]
#[bench::sources_256(RemoteGateRetryBenchFixture::sources_256())]
fn remote_gate_retry(mut fixture: RemoteGateRetryBenchFixture) -> usize {
    black_box(fixture.retry_under_pressure())
}

// measures selected-RID readiness when one observed RID activates many pending
// route gates
//
// this protects video route-control updates from becoming proportional to
// repeated packets or duplicate readiness events instead of the unique source
// and destination work that must actually change
#[library_benchmark(config = callgrind_config(0.5))]
#[bench::selected(RidReadinessBenchFixture::pending_selected_rid())]
fn rid_readiness_256(mut fixture: RidReadinessBenchFixture) -> usize {
    black_box(fixture.activate_selected_rid())
}

// measures local RTP identity projection for steady and switching simulcast
// sources
//
// this protects the per-destination local egress rewrite cost paid before each
// forwarded packet is handed to str0m
#[library_benchmark(config = callgrind_config(0.5))]
#[bench::steady_ssrc(LocalRewriteBenchFixture::steady_ssrc())]
#[bench::switching_ssrc(LocalRewriteBenchFixture::switching_ssrc())]
fn local_rewrite_4096(mut fixture: LocalRewriteBenchFixture) -> u64 {
    black_box(fixture.project_packets())
}

// measures successful local RTC writes plus egress bitrate accounting
//
// this protects the destination-session lookup and counter update paid after
// str0m accepts each forwarded packet
fn validate_local_send(fixture: LocalSendBenchFixture) {
    assert!(fixture.accounting_matches());
}

#[library_benchmark(config = callgrind_config(1.0), teardown = validate_local_send)]
#[bench::successful(LocalSendBenchFixture::successful())]
fn local_send_512(mut fixture: LocalSendBenchFixture) -> LocalSendBenchFixture {
    fixture.send_packets();
    black_box(fixture)
}

// measures active-speaker audio observations plus snapshot and expiry queries
//
// this protects the packet-level audio policy used by room source-policy
// updates and diagnostics
#[library_benchmark(config = callgrind_config(1.0))]
#[bench::many_sources(ActiveSpeakerBenchFixture::many_sources())]
fn active_speaker_policy(mut fixture: ActiveSpeakerBenchFixture) -> usize {
    black_box(fixture.observe_sources())
}

// measures producer-side keyframe request coalescing for many consumer-local
// feedback requests
//
// coalescing keeps route-control feedback storms from turning into one remote
// source command per consumer
// this benchmark checks the route-scoped flush path that resolves current route
// state before collapsing many requests into one producer-side signal
#[library_benchmark(config = callgrind_config(5.0))]
#[bench::remote_source(KeyframeCoalescingBenchFixture::remote_source_requests())]
fn keyframe_coalesce_512(mut fixture: KeyframeCoalescingBenchFixture) -> usize {
    black_box(fixture.flush_requests())
}

// measures packet work interleaved with worker lifecycle commands
//
// the packet side reuses the deterministic fanout planner while the command
// side goes through the real worker mailbox
// this keeps the observation-lock cost visible in the regular base-versus-head
// Callgrind suite without adding fake peer negotiation to the measured window
#[library_benchmark(config = callgrind_config(1.0), teardown = drop)]
#[bench::packet_cmd_mix(WorkerPacketCommandMixBenchFixture::packet_command_mix_current_thread())]
fn interleaved_fanout(
    mut fixture: WorkerPacketCommandMixBenchFixture,
) -> WorkerPacketCommandMixBenchFixture {
    black_box(fixture.run_packet_command_mix());
    black_box(fixture)
}

// measures ready session output draining
#[library_benchmark(config = callgrind_config(1.0), teardown = drop)]
#[bench::drain(SessionDrainBenchFixture::new())]
fn session_drain_128(mut fixture: SessionDrainBenchFixture) -> SessionDrainBenchFixture {
    black_box(fixture.drain_sessions());
    black_box(fixture)
}

// measures relay channel packet draining
#[library_benchmark(config = callgrind_config(1.0))]
#[bench::drain(RelayDrainBenchFixture::new())]
fn relay_drain_256(mut fixture: RelayDrainBenchFixture) -> usize {
    black_box(fixture.drain_relay())
}

library_benchmark_group!(
    name = packet_loop_callgrind;
    benchmarks =
        route_plan_1024,
        relay_route_plan_1024,
        incoming_observation_512,
        relay_mailbox_256,
        ingress_demux_256,
        ingress_completed_burst_256,
        scheduler_churn_128,
        fingerprint_4096,
        packet_sink_512,
        route_gate_batch,
        remote_gate_retry,
        rid_readiness_256,
        local_rewrite_4096,
        local_send_512,
        active_speaker_policy,
        keyframe_coalesce_512,
        interleaved_fanout,
        session_drain_128,
        relay_drain_256
);

main!(library_benchmark_groups = packet_loop_callgrind);
