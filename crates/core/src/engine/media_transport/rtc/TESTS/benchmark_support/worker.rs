//! whole-worker fixtures for manual packet-loop profiling
//!
//! this fixture is heavier than the slice fixtures
//! it calls a real `RtcWorker` from a current-thread runtime so benchmarks include
//! mailbox scheduling and worker command handling after setup
//!
//! callers should create fixtures in benchmark setup only
//! the measured method sends read-only commands without allocating transport state

use std::sync::Arc;

use tokio::runtime::{Builder, Runtime};

use super::super::{RtcWorker, RtpProfile, test_support::test_transport_session_key};
use crate::{
    MediaWorkerId, RtcPortRange,
    engine::{
        UserId,
        media_transport::{
            SourcePolicySignal, TransportSessionKey,
            test_support::{test_media_transport_config, test_media_transport_deps},
        },
    },
};

/// fixed command count for one worker investigation sample
///
/// this keeps the benchmark id and the measured work aligned so artifacts can
/// be compared across manual runs without reading the fixture code
pub const WORKER_COMMAND_ROUNDTRIPS: usize = 128;

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

/// current-thread caller fixture for whole-worker investigation benchmarks
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
    /// builds and warms a worker fixture with a current-thread caller runtime
    ///
    /// # Panics
    ///
    /// panics when the benchmark runtime cannot be created or when the worker
    /// cannot create its bootstrap offer
    #[must_use]
    pub fn command_driven_current_thread() -> Self {
        let Ok(runtime) = Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
        else {
            panic!("failed to build current-thread benchmark runtime")
        };
        let Ok(rtc_port_range) = RtcPortRange::try_new(46_200, 46_220) else {
            panic!("benchmark port range should be valid")
        };
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
        assert!(result.is_ok(), "failed to bootstrap benchmark worker");
    }
}

impl Drop for WorkerLoopBenchFixture {
    fn drop(&mut self) {
        let _ = self
            .runtime
            .block_on(self.worker.close_session(&self.session_key));
    }
}
