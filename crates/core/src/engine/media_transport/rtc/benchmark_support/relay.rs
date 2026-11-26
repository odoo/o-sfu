use o_sfu_model::UserId;
use tokio::sync::mpsc;

use super::super::{
    packet_loop::forwarded_packet::ForwardedPacket,
    state::{
        PacketLoopState,
        relay_registry::{RelayEnqueueOutcome, RelayPacketMailbox},
    },
    test_support::{resolve_sample_packet, sample_forwarded_packet, test_transport_session_key},
};
use crate::engine::media_transport::TransportMediaId;

pub const RELAY_MAILBOX_ATTEMPTS: usize = 256;

#[derive(Debug, Clone, Copy)]
enum RelayPressureMode {
    Open,
    Overloaded,
}

/// fixed relay-mailbox fixture for packet-loop relay enqueue benchmarks
///
/// the fixture keeps the receiver alive and exposes only fixed attempt-count
/// operations
/// this lets benchmarks measure the production non-blocking relay
/// send path for both open and overloaded mailbox states
pub struct RelayPressureBenchFixture {
    mode: RelayPressureMode,
    target: RelayPacketMailbox,
    packet: ForwardedPacket,
    state: PacketLoopState,
    _rx: mpsc::Receiver<ForwardedPacket>,
}

impl RelayPressureBenchFixture {
    #[must_use]
    pub fn open_mailbox() -> Self {
        Self::new(RELAY_MAILBOX_ATTEMPTS, RelayPressureMode::Open)
    }

    #[must_use]
    pub fn full_mailbox() -> Self {
        let fixture = Self::new(1, RelayPressureMode::Overloaded);
        let _ = fixture.count_matching_outcomes(1, RelayEnqueueOutcome::Enqueued);
        fixture
    }

    #[must_use]
    pub fn run_attempts(&self) -> usize {
        self.count_matching_outcomes(RELAY_MAILBOX_ATTEMPTS, self.expected_outcome())
    }

    fn new(capacity: usize, mode: RelayPressureMode) -> Self {
        let capacity = capacity.max(1);
        let source_session = test_transport_session_key(2, 0, 3, UserId::Integer(4));
        let mut packet = sample_forwarded_packet(source_session, "cam-up", b"payload");
        resolve_sample_packet(
            &mut packet,
            &PacketLoopState::default(),
            TransportMediaId::new(1),
        );
        let (tx, rx) = mpsc::channel(capacity);
        Self {
            mode,
            target: RelayPacketMailbox::new(tx),
            packet,
            state: PacketLoopState::default(),
            _rx: rx,
        }
    }

    fn count_matching_outcomes(&self, attempts: usize, expected: RelayEnqueueOutcome) -> usize {
        let mut matching_outcomes = 0;
        for _ in 0..attempts {
            if self
                .target
                .forward_packet(&self.state, &self.packet)
                .is_some_and(|report| report.outcome == expected)
            {
                matching_outcomes += 1;
            }
        }
        matching_outcomes
    }

    fn expected_outcome(&self) -> RelayEnqueueOutcome {
        match self.mode {
            RelayPressureMode::Open => RelayEnqueueOutcome::Enqueued,
            RelayPressureMode::Overloaded => RelayEnqueueOutcome::Overloaded,
        }
    }
}
