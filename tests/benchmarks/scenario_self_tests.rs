//! self-tests for the Callgrind scenario fixtures
//!
//! a scenario benchmark that silently stops doing the work it was built for keeps
//! reporting stable instruction counts, which reads as "no regression"
//! these tests run the scenarios outside Valgrind so the normal test job fails
//! when a scenario stops reaching the paths it exists to measure
//!
//! these scenarios live in this one target on purpose. a missing target fails the
//! gate, while a name filter that matches nothing exits zero, which would turn
//! the gate into the silent no-op it exists to prevent

#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "fixed benchmark fixtures must fail on invalid setup or missing coverage"
)]

#[path = "source_policy/mod.rs"]
mod source_policy;

use o_sfu_core::server::transport::benchmark_support::{
    IncomingObservationBenchFixture, InterleavedRelayActivityBenchFixture, MeetingFlowBenchFixture,
    RELAY_MAILBOX_ATTEMPTS, ROUTE_PLANNING_TURNS, RelayDrainBenchFixture, RelayFanoutBenchFixture,
    RelayPressureBenchFixture, RemoteGateRetryBenchFixture, RidReadinessBenchFixture,
    SchedulerBenchFixture, SessionDrainBenchFixture,
};
use source_policy::SourcePolicyFixture;

/// the room's video budget solver must keep reacting to receiver bandwidth
#[test]
fn source_policy_scenario_reacts_to_receiver_bandwidth() {
    let mut fixture = SourcePolicyFixture::new();
    let _ = fixture.run_policy_turns();
    fixture.assert_every_turn_planned();
    fixture.assert_budget_pressure_observed();
}

#[test]
fn source_policy_scenario_filters_foreign_and_inactive_speakers() {
    let mut fixture = SourcePolicyFixture::mixed_speakers();
    let _ = fixture.run_policy_turns();
    fixture.assert_every_turn_planned();
    fixture.assert_speaker_selection();
}

#[test]
fn interleaved_activity_controls_actual_relay_delivery() {
    let mut fixture = InterleavedRelayActivityBenchFixture::activity_gate();
    fixture.run();
    fixture.assert_coverage();
}

#[test]
fn relay_planning_scenario_applies_target_gates() {
    let mut fixture = RelayFanoutBenchFixture::mixed_gates();
    assert_eq!(fixture.plan_route_turns(), ROUTE_PLANNING_TURNS * 2);
    fixture.assert_gate_selection();
}

#[test]
fn incoming_observation_scenarios_learn_and_reuse_ssrc() {
    for mut fixture in [
        IncomingObservationBenchFixture::mid_rid_then_ssrc(),
        IncomingObservationBenchFixture::negotiated_vp8(),
    ] {
        let _ = fixture.observe_turns();
        fixture.assert_observation_coverage();
    }
}

#[test]
fn relay_mailbox_scenarios_reach_expected_pressure() {
    for fixture in [
        RelayPressureBenchFixture::open_mailbox(),
        RelayPressureBenchFixture::full_mailbox(),
    ] {
        assert_eq!(fixture.run_attempts(), RELAY_MAILBOX_ATTEMPTS);
    }
}

#[test]
fn relay_drain_scenario_consumes_only_queued_packets() {
    for (mut fixture, packet_count, warmed) in [
        (RelayDrainBenchFixture::new(), 256, false),
        (RelayDrainBenchFixture::warmed(256), 256, true),
        (RelayDrainBenchFixture::warmed(64), 64, true),
    ] {
        let capacity = fixture.staging_capacity();
        if warmed {
            assert!(capacity >= packet_count);
        }
        assert_eq!(fixture.drain_relay(), packet_count);
        fixture.assert_drained();
        if warmed {
            assert_eq!(fixture.staging_capacity(), capacity);
        }
        assert_eq!(fixture.drain_relay(), 0);
    }
}

#[test]
fn rid_readiness_scenario_activates_pending_gates_once() {
    let mut fixture = RidReadinessBenchFixture::pending_selected_rid();
    assert_eq!(fixture.activate_selected_rid(), 2);
    assert_eq!(fixture.activate_selected_rid(), 0);
}

/// the meeting scenario must keep exercising the branches it was built for
///
/// a run that silently stops forwarding, stops branching in the audio policy or
/// stops recording bandwidth estimates would still produce stable instruction
/// counts, which is exactly the failure mode the scenario replaces
#[test]
fn meeting_scenario_exercises_the_whole_packet_loop() {
    fn verify<const SAMPLED: bool>() {
        let mut fixture = MeetingFlowBenchFixture::<SAMPLED>::short_meeting();
        let total_work = fixture.run_meeting();
        assert!(total_work > 0, "meeting scenario produced no work");
        fixture.assert_packet_loop_coverage();
    }
    verify::<false>();
    verify::<true>();
}

/// saturated control mailboxes must leave every source's packet gate pending
#[test]
fn remote_gate_retry_scenario_keeps_saturated_sources_pending() {
    for (mut fixture, source_count) in [
        (RemoteGateRetryBenchFixture::sources_64(), 64),
        (RemoteGateRetryBenchFixture::sources_256(), 256),
    ] {
        assert_eq!(fixture.retry_under_pressure(), source_count);
    }
}

#[test]
fn session_drain_scenario_polls_every_ready_session() {
    let mut fixture = SessionDrainBenchFixture::new();
    fixture.drain_sessions();
    fixture.assert_drained();
}

#[test]
fn scheduler_scenario_collects_sessions_and_keeps_future_deadlines() {
    let mut fixture = SchedulerBenchFixture::stale_timeouts();
    for _ in 0..2 {
        assert_eq!(
            fixture.collect_ready_and_next_timeout(),
            129,
            "all 128 sessions must be ready with a future deadline remaining"
        );
    }
}
