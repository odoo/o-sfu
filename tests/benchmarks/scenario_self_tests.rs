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

#[path = "source_policy/mod.rs"]
mod source_policy;

use o_sfu_core::server::transport::benchmark_support::{
    IncomingObservationBenchFixture, MeetingFlowBenchFixture, RELAY_MAILBOX_ATTEMPTS,
    ROUTE_PLANNING_TURNS, RelayDrainBenchFixture, RelayFanoutBenchFixture,
    RelayPressureBenchFixture, RemoteGateRetryBenchFixture, RidReadinessBenchFixture,
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
    let mut fixture = RelayDrainBenchFixture::new();
    assert_eq!(fixture.drain_relay(), 256);
    assert_eq!(fixture.drain_relay(), 0);
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
    let mut fixture = MeetingFlowBenchFixture::short_meeting();
    let total_work = fixture.run_meeting();
    assert!(total_work > 0, "meeting scenario produced no work");
    fixture.assert_packet_loop_coverage();
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
