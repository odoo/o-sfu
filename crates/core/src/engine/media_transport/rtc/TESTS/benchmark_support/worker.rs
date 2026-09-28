//! whole-worker fixtures for manual packet-loop profiling
//!
//! the fixtures in this file are heavier than the slice fixtures
//! they own a real current-thread `RtcWorker` so the benchmark can include
//! mailbox scheduling and worker command handling after setup
//!
//! callers should create fixtures in benchmark setup only
//! measured methods expose repeatable command work without allocating new
//! transport state or starting sockets

use std::sync::Arc;

use o_sfu_router::rtp::{MediaStream as RouterRtpParameters, StreamBinding as RouterRtpEncoding};
use str0m::media::MediaKind;
use tokio::runtime::{Builder, Runtime};

use super::{
    super::{
        RtcSessionOffer, RtcWorker, RtcWorkerCommand, RtpProfile,
        test_support::test_transport_session_key,
    },
    FanoutBenchTopology,
};
use crate::{
    MediaWorkerId, RtcPortRange,
    engine::{
        UserId,
        media_transport::{
            SourcePolicySignal, TransportResult, TransportSessionKey,
            test_support::{test_media_transport_config, test_media_transport_deps},
        },
    },
};

/// fixed command count for one worker investigation sample
///
/// this keeps the benchmark id and the measured work aligned so artifacts can
/// be compared across manual runs without reading the fixture code
pub const WORKER_COMMAND_ROUNDTRIPS: usize = 128;
pub const WORKER_PACKET_COMMAND_MIX_PACKETS: usize = 512;
const PACKETS_PER_LIFECYCLE_BURST: usize = 32;
const WORKER_PACKET_COMMAND_MIX_FANOUT: usize = 8;

#[expect(
    clippy::expect_used,
    reason = "benchmark setup must fail when its RTC profile or worker cannot start"
)]
fn benchmark_worker(rtc_port_range: RtcPortRange) -> RtcWorker {
    let config = test_media_transport_config(1, rtc_port_range);
    let profile = RtpProfile::compile(config.codec_flags, config.codec_preferences)
        .expect("benchmark RTP profile should compile");
    RtcWorker::start(
        &config,
        Arc::new(profile),
        rtc_port_range,
        &test_media_transport_deps(),
        SourcePolicySignal::default(),
        0,
        MediaWorkerId::from_raw(0),
    )
    .expect("benchmark RTC worker should start")
}

/// current-thread packet-loop fixture for whole-worker investigation benchmarks
///
/// setup builds a ready `RtcWorker` and warms one bootstrap session before the
/// measured function runs
/// the measured path sends read-only worker commands through the real mailbox
/// so Callgrind sees packet-loop scheduling, command drain and response
/// delivery without counting fixture construction
pub struct WorkerLoopBenchFixture {
    runtime: Runtime,
    worker: RtcWorker,
    session_key: TransportSessionKey,
}

impl WorkerLoopBenchFixture {
    /// builds and warms one command-driven current-thread worker fixture
    ///
    /// # Panics
    ///
    /// panics when the benchmark runtime cannot be created or when the worker
    /// cannot create its bootstrap offer
    #[must_use]
    #[expect(
        clippy::panic,
        reason = "benchmark setup must fail loudly when the current-thread worker cannot boot"
    )]
    pub fn command_driven_current_thread() -> Self {
        let Ok(runtime) = Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
        else {
            panic!("failed to build current-thread benchmark runtime")
        };
        let rtc_port_range = RtcPortRange::new(46_200, 46_220);
        let session_key = test_transport_session_key(91, 0, 92, UserId::Integer(93));
        let fixture = Self {
            runtime,
            worker: benchmark_worker(rtc_port_range),
            session_key,
        };
        fixture.bootstrap_worker();
        let _ = fixture.run_command_roundtrips();
        fixture
    }

    /// sends read-only commands through the worker mailbox
    ///
    /// the method is the measured body used by `packet_loop_worker_callgrind`
    /// it assumes `command_driven_current_thread` already booted and warmed the
    /// worker
    ///
    /// it blocks the fixture runtime until each mailbox response arrives, so
    /// callers should keep it inside benchmark code rather than production tests
    ///
    /// # Panics
    ///
    /// Panics if the benchmark worker stops before completing a read-only command.
    #[must_use]
    #[expect(
        clippy::expect_used,
        reason = "benchmark command failures must fail the measured fixture"
    )]
    pub fn run_command_roundtrips(&self) -> usize {
        self.runtime.block_on(async {
            let mut observed_sources = 0;
            for _ in 0..WORKER_COMMAND_ROUNDTRIPS {
                observed_sources += self
                    .worker
                    .active_speaker_source_snapshot()
                    .await
                    .expect("benchmark worker observation should complete")
                    .len();
            }
            observed_sources
        })
    }

    fn bootstrap_worker(&self) {
        let result = self.runtime.block_on(
            self.worker
                .create_initial_session_offer("test-room", &self.session_key),
        );
        assert!(
            result.is_ok(),
            "failed to bootstrap current-thread benchmark worker"
        );
    }
}

impl Drop for WorkerLoopBenchFixture {
    fn drop(&mut self) {
        let _ = self
            .runtime
            .block_on(self.worker.close_session(&self.session_key));
    }
}

/// current-thread fixture for packet sends interleaved with lifecycle commands
///
/// the measured work keeps a worker task alive with one base session, then runs
/// local fanout sends with a lifecycle burst every 32 sends
/// each lifecycle burst enters the real worker mailbox and executes the command
/// sequence affected by observation locking: session creation, receive-media
/// registration, media removal and session close
/// typed offer responses remain retained until teardown so their destruction is
/// outside the measured command mix
pub struct WorkerPacketCommandMixBenchFixture {
    runtime: Runtime,
    worker: RtcWorker,
    base_session_key: TransportSessionKey,
    fanout: FanoutBenchTopology,
    retained_offers: Vec<RtcSessionOffer>,
    next_session_id: u64,
}

impl WorkerPacketCommandMixBenchFixture {
    /// builds one current-thread worker and warms the mixed packet-command path
    ///
    /// # Panics
    ///
    /// panics when the benchmark runtime cannot be created or when a worker
    /// command unexpectedly fails
    #[must_use]
    #[expect(
        clippy::panic,
        reason = "benchmark setup must fail loudly when the current-thread worker cannot boot"
    )]
    pub fn packet_command_mix_current_thread() -> Self {
        let Ok(runtime) = Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
        else {
            panic!("failed to build current-thread benchmark runtime")
        };
        let rtc_port_range = RtcPortRange::new(46_200, 46_220);
        let mut fixture = Self {
            runtime,
            worker: benchmark_worker(rtc_port_range),
            base_session_key: benchmark_session_key(10_000),
            fanout: FanoutBenchTopology::with_local_destinations(WORKER_PACKET_COMMAND_MIX_FANOUT),
            retained_offers: Vec::with_capacity(
                WORKER_PACKET_COMMAND_MIX_PACKETS / PACKETS_PER_LIFECYCLE_BURST,
            ),
            next_session_id: 10_001,
        };
        fixture.bootstrap_base_session();
        let _ = fixture.run_packet_command_mix();
        fixture.retained_offers.clear();
        fixture
    }

    /// runs fixed fanout sends with realistic cold command spacing
    ///
    /// the packet side uses the same route-planning helper as the deterministic
    /// fanout benchmark
    /// the command side uses actual `RtcWorker` methods so the mailbox and
    /// lock-sensitive command handlers stay in the measured window
    #[must_use]
    pub fn run_packet_command_mix(&mut self) -> usize {
        let runtime = &self.runtime;
        let worker = &self.worker;
        let fanout = &mut self.fanout;
        let retained_offers = &mut self.retained_offers;
        let mut next_session_id = self.next_session_id;
        let observed = runtime.block_on(async {
            let mut observed = 0_usize;
            for packet_index in 0..WORKER_PACKET_COMMAND_MIX_PACKETS {
                observed = observed.saturating_add(fanout.plan_packet_send());
                if (packet_index + 1) % PACKETS_PER_LIFECYCLE_BURST == 0 {
                    let session_key = benchmark_session_key(next_session_id);
                    next_session_id = next_session_id.saturating_add(1);
                    let (lifecycle_work, offer) = run_lifecycle_burst(worker, &session_key).await;
                    retained_offers.push(offer);
                    observed = observed.saturating_add(lifecycle_work);
                }
            }
            observed
        });
        self.next_session_id = next_session_id;
        observed
    }

    fn bootstrap_base_session(&self) {
        let result = self.runtime.block_on(
            self.worker
                .create_initial_session_offer("test-room", &self.base_session_key),
        );
        assert!(result.is_ok(), "failed to bootstrap base worker session");
    }
}

impl Drop for WorkerPacketCommandMixBenchFixture {
    fn drop(&mut self) {
        let _ = self
            .runtime
            .block_on(self.worker.close_session(&self.base_session_key));
    }
}

async fn run_lifecycle_burst(
    worker: &RtcWorker,
    session_key: &TransportSessionKey,
) -> (usize, RtcSessionOffer) {
    let offer = require_ok(
        worker
            .request_worker(|response| RtcWorkerCommand::CreateInitialSessionOffer {
                room_id: Arc::from("test-room"),
                session_key: session_key.clone(),
                response,
            })
            .await,
        "temporary session offer failed",
    );
    let media_id = require_ok(
        worker
            .add_recv_media(
                session_key,
                MediaKind::Audio,
                &audio_rtp_parameters("mix-aud-up", 91_000),
            )
            .await,
        "temporary receive media registration failed",
    );
    require_ok(
        worker.remove_media(session_key, media_id).await,
        "temporary receive media removal failed",
    );
    require_ok(
        worker.close_session(session_key).await,
        "temporary session close failed",
    );
    (usize::try_from(media_id.as_u64()).unwrap_or(0), offer)
}

fn benchmark_session_key(connection_id: u64) -> TransportSessionKey {
    test_transport_session_key(
        120,
        0,
        connection_id,
        UserId::Integer(i64::try_from(connection_id).unwrap_or(i64::MAX)),
    )
}

fn audio_rtp_parameters(mid: &str, ssrc: u32) -> RouterRtpParameters {
    RouterRtpParameters::new(
        vec![],
        vec![],
        vec![RouterRtpEncoding::new().with_ssrc(ssrc)],
    )
    .with_mid(mid.to_owned())
}

#[expect(
    clippy::panic,
    reason = "benchmark setup and measurement should fail loudly when required worker commands fail"
)]
fn require_ok<T>(value: TransportResult<T>, context: &'static str) -> T {
    let Ok(value) = value else {
        panic!("{context}")
    };
    value
}
