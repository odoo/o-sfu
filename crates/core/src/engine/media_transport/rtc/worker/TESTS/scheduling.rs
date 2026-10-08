use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex},
    time::Instant,
};

use str0m::net::Protocol;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::{
    packet_loop_config_for_test, sample_already_relayed_packet, test_transport_session_key,
};
use crate::{
    Bitrate,
    engine::{
        UserId,
        media_transport::{
            TransportMediaId,
            rtc::{
                bootstrap::test_support::ensure_session_rtc_state,
                commands::RtcWorkerCommand,
                packet_loop::{
                    IngressPacket, RtcIngress, forwarded_packet::ForwardedPacket,
                    routing_miss::DemuxRecoveryState,
                    udp::test_support::completed_datagram_channel,
                },
                state::{PacketLoopState, RtcSnapshotState, bitrate::BitrateRegistry},
                worker::{
                    input::{PacketLoopControlInput, PacketLoopInputReceivers},
                    loop_driver::{
                        PacketLoopApplyContext, PacketLoopConfig, PacketLoopTurn,
                        PacketLoopTurnInput, WaitPhaseSnapshot,
                    },
                },
            },
        },
    },
};

const CHECKPOINT_INPUTS: u64 = 32;
const COMMAND_QUEUE_CAPACITY: usize = 64;

struct SchedulingHarness {
    turn: PacketLoopTurn,
    state: PacketLoopState,
    bitrate_registry: Arc<Mutex<BitrateRegistry>>,
    snapshot_state: Arc<Mutex<RtcSnapshotState>>,
    config: PacketLoopConfig,
    demux: DemuxRecoveryState,
    inputs: PacketLoopInputReceivers,
    ingress: RtcIngress,
    datagram_tx: mpsc::Sender<IngressPacket>,
    command_tx: mpsc::Sender<RtcWorkerCommand>,
    relay_tx: mpsc::Sender<ForwardedPacket>,
    shutdown: CancellationToken,
}

impl SchedulingHarness {
    fn new() -> Self {
        let (command_tx, command_rx) = mpsc::channel(COMMAND_QUEUE_CAPACITY);
        let (relay_tx, relay_rx) = mpsc::channel(1);
        let shutdown = CancellationToken::new();
        let (datagram_tx, ingress) = completed_datagram_channel();
        Self {
            turn: PacketLoopTurn::new(Instant::now()),
            state: PacketLoopState::default(),
            bitrate_registry: Arc::new(Mutex::new(BitrateRegistry::default())),
            snapshot_state: Arc::new(Mutex::new(RtcSnapshotState::default())),
            config: packet_loop_config_for_test(),
            demux: DemuxRecoveryState::new(),
            inputs: PacketLoopInputReceivers::new(command_rx, relay_rx, shutdown.clone()),
            ingress,
            datagram_tx,
            command_tx,
            relay_tx,
            shutdown,
        }
    }

    fn queue_control(&self, id: u64) {
        let (response, _result) = oneshot::channel();
        assert!(
            self.command_tx
                .try_send(RtcWorkerCommand::ResolveMediaMid {
                    transport_media_id: TransportMediaId::new(id),
                    response,
                })
                .is_ok()
        );
    }

    fn queue_datagram(&self, payload: &[u8]) {
        let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        assert!(
            self.datagram_tx
                .try_send(IngressPacket {
                    source_addr: addr,
                    candidate_addr: addr,
                    received_at: Instant::now(),
                    packet: payload.to_vec(),
                    protocol: Protocol::Udp,
                })
                .is_ok()
        );
    }

    async fn next(&mut self, next_timeout: Option<Instant>) -> Option<PacketLoopTurnInput> {
        self.turn
            .wait_for_next_input(
                WaitPhaseSnapshot { next_timeout },
                &mut self.ingress,
                &mut self.inputs,
                &self.config.packet_loop_delay,
            )
            .await
    }

    fn apply(&mut self, input: PacketLoopTurnInput) {
        self.turn.apply_input(
            &mut PacketLoopApplyContext {
                packet_loop_state: &mut self.state,
                bitrate_registry: &self.bitrate_registry,
                snapshot_state: &self.snapshot_state,
                candidate_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
                config: &self.config,
                demux: &mut self.demux,
                ingress: &self.ingress,
            },
            input,
        );
        let _ = self.turn.pump(
            &mut self.state,
            &self.bitrate_registry,
            &self.snapshot_state,
            &self.config,
            &mut self.demux,
            &mut self.inputs,
        );
    }

    async fn apply_control(&mut self, id: u64) -> Result<(), &'static str> {
        let input = self
            .next(None)
            .await
            .ok_or("control input should be ready")?;
        assert_control(&input, id);
        self.apply(input);
        Ok(())
    }

    /// The driver must suspend before its queued input wins so the helper can run.
    async fn next_after_helper(
        &mut self,
        next_timeout: Option<Instant>,
    ) -> Result<PacketLoopTurnInput, &'static str> {
        let (entered_tx, entered_rx) = oneshot::channel();
        let (progress_tx, mut progress_rx) = oneshot::channel();
        let (input, ()) = tokio::join!(
            biased;
            async {
                assert!(entered_tx.send(()).is_ok());
                let input = self.next(next_timeout).await;
                assert!(progress_rx.try_recv().is_ok());
                input
            },
            async {
                assert!(entered_rx.await.is_ok());
                assert!(progress_tx.send(()).is_ok());
            }
        );
        input.ok_or("queued input should remain available after the checkpoint")
    }
}

fn assert_control(input: &PacketLoopTurnInput, expected: u64) {
    assert!(matches!(
        input,
        PacketLoopTurnInput::Control(PacketLoopControlInput::Command(
            RtcWorkerCommand::ResolveMediaMid { transport_media_id, .. }
        )) if *transport_media_id == TransportMediaId::new(expected)
    ));
}

fn assert_datagram(input: &PacketLoopTurnInput, expected: &[u8]) {
    assert!(matches!(
        input,
        PacketLoopTurnInput::Datagram(datagram) if datagram.packet == expected
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn sustained_control_yields_and_preserves_udp_and_command_order() -> Result<(), &'static str>
{
    let mut harness = SchedulingHarness::new();
    for id in 0..CHECKPOINT_INPUTS * 2 {
        harness.queue_control(id);
    }
    harness.queue_datagram(b"first");
    harness.queue_datagram(b"second");
    for id in 0..CHECKPOINT_INPUTS {
        harness.apply_control(id).await?;
    }
    let input = harness.next_after_helper(None).await?;
    assert_datagram(&input, b"first");
    harness.apply(input);
    for id in CHECKPOINT_INPUTS..CHECKPOINT_INPUTS * 2 - 1 {
        harness.apply_control(id).await?;
    }
    let input = harness.next_after_helper(None).await?;
    assert_datagram(&input, b"second");
    harness.apply(input);
    harness.apply_control(CHECKPOINT_INPUTS * 2 - 1).await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn mixed_control_and_timeout_flood_cannot_replenish_the_checkpoint()
-> Result<(), &'static str> {
    let mut harness = SchedulingHarness::new();
    let deadline = Instant::now();
    harness.queue_datagram(b"waiting");
    for id in 0..4 {
        harness.queue_control(id);
        harness.apply_control(id).await?;
        for _ in 0..7 {
            let input = harness
                .next(Some(deadline))
                .await
                .ok_or("due timeout should stay ready")?;
            assert!(matches!(input, PacketLoopTurnInput::Timeout));
            harness.apply(input);
        }
    }
    harness.queue_control(4);
    let input = harness.next_after_helper(Some(deadline)).await?;
    assert_datagram(&input, b"waiting");
    harness.apply(input);
    harness.apply_control(4).await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn checkpoint_datagram_consumes_the_first_slot_of_the_udp_burst() -> Result<(), &'static str>
{
    let mut harness = SchedulingHarness::new();
    for id in 0..CHECKPOINT_INPUTS {
        harness.queue_control(id);
    }
    for id in 0_u8..17 {
        harness.queue_datagram(&[id]);
    }
    for id in 0..CHECKPOINT_INPUTS {
        harness.apply_control(id).await?;
    }
    let input = harness.next_after_helper(None).await?;
    assert_datagram(&input, &[0]);
    harness.apply(input);
    for id in 1_u8..16 {
        let input = harness
            .next(None)
            .await
            .ok_or("burst datagram should be ready")?;
        assert_datagram(&input, &[id]);
        harness.apply(input);
    }
    let session = test_transport_session_key(1, 0, 2, UserId::Integer(3));
    assert!(
        harness
            .relay_tx
            .try_send(sample_already_relayed_packet(
                session,
                TransportMediaId::new(4),
                "audio",
                b"relay",
            ))
            .is_ok()
    );
    assert!(matches!(
        harness.next(None).await,
        Some(PacketLoopTurnInput::RelayPacket)
    ));
    let datagram = harness
        .ingress
        .try_recv()
        .ok_or("seventeenth datagram should remain queued")?;
    assert_eq!(datagram.packet, [16]);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn empty_ingress_checkpoint_yields_without_replacing_ready_control()
-> Result<(), &'static str> {
    let mut harness = SchedulingHarness::new();
    for id in 0..=CHECKPOINT_INPUTS {
        harness.queue_control(id);
    }
    for id in 0..CHECKPOINT_INPUTS {
        harness.apply_control(id).await?;
    }
    let input = harness.next_after_helper(None).await?;
    assert_control(&input, CHECKPOINT_INPUTS);
    harness.apply(input);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_precedes_an_exhausted_checkpoint() -> Result<(), &'static str> {
    let mut harness = SchedulingHarness::new();
    for id in 0..=CHECKPOINT_INPUTS {
        harness.queue_control(id);
    }
    harness.queue_datagram(b"waiting");
    for id in 0..CHECKPOINT_INPUTS {
        harness.apply_control(id).await?;
    }
    harness.shutdown.cancel();
    assert!(harness.next(None).await.is_none());
    assert!(harness.ingress.try_recv().is_some());
    assert!(harness.inputs.try_recv_control().is_some());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_during_checkpoint_yield_preserves_queued_inputs() -> Result<(), &'static str> {
    let mut harness = SchedulingHarness::new();
    for id in 0..=CHECKPOINT_INPUTS {
        harness.queue_control(id);
    }
    harness.queue_datagram(b"waiting");
    for id in 0..CHECKPOINT_INPUTS {
        harness.apply_control(id).await?;
    }
    let shutdown = harness.shutdown.clone();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (input, ()) = tokio::join!(
        biased;
        async {
            assert!(entered_tx.send(()).is_ok());
            harness.next(None).await
        },
        async {
            assert!(entered_rx.await.is_ok());
            shutdown.cancel();
        }
    );
    assert!(input.is_none());
    assert!(harness.ingress.try_recv().is_some());
    assert!(harness.inputs.try_recv_control().is_some());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn observation_command_preserves_unknown_source_cooldown() -> Result<(), &'static str> {
    use crate::engine::media_transport::rtc::packet_loop::routing_miss::PacketLoopRoutingMissKey;
    let mut harness = SchedulingHarness::new();
    let source_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 50_000));
    let candidate_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 50_001));
    let packet = [0x80, 0x60, 0, 1];
    let miss_key = PacketLoopRoutingMissKey::new(source_addr, candidate_addr, &packet);
    let now = Instant::now();
    for _ in 0..4 {
        harness
            .demux
            .record_miss(miss_key, &packet, source_addr, now);
    }
    assert!(harness.demux.is_source_blocked(source_addr, now));
    harness.queue_control(1);
    harness.apply_control(1).await?;
    assert!(harness.demux.is_source_blocked(source_addr, now));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn immediate_rtc_timeout_yields_to_queued_control_and_udp() -> Result<(), &'static str> {
    let mut harness = SchedulingHarness::new();
    let session = test_transport_session_key(7, 0, 8, UserId::Integer(9));
    let candidate_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 48_000));
    ensure_session_rtc_state(
        &mut harness.state.users,
        &session,
        candidate_addr,
        Bitrate::from_mbps(10),
    )
    .map_err(|_error| "RTC state should initialize")?;
    harness.state.mark_session_dirty(&session);
    harness.turn.force_immediate_timeout = true;
    harness.queue_control(1);
    harness.queue_datagram(b"waiting");

    let snapshot = harness.turn.pump(
        &mut harness.state,
        &harness.bitrate_registry,
        &harness.snapshot_state,
        &harness.config,
        &mut harness.demux,
        &mut harness.inputs,
    );
    assert!(
        snapshot
            .next_timeout
            .is_some_and(|deadline| deadline <= Instant::now())
    );
    assert!(!harness.state.has_dirty_sessions());

    let input = harness
        .next(snapshot.next_timeout)
        .await
        .ok_or("queued control should run")?;
    assert_control(&input, 1);
    harness.apply(input);
    for _ in 0..=CHECKPOINT_INPUTS {
        let input = harness
            .next(snapshot.next_timeout)
            .await
            .ok_or("queued UDP should run after bounded timeout wakeups")?;
        if matches!(input, PacketLoopTurnInput::Datagram(_)) {
            assert_datagram(&input, b"waiting");
            harness.apply(input);
            return Ok(());
        }
        assert!(matches!(input, PacketLoopTurnInput::Timeout));
        harness.apply(input);
    }
    Err("queued UDP starved after bounded timeout wakeups")
}
