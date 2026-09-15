//! Room construction for rooms that are new to the runtime directory.
//!
//! `RoomManager` owns idempotent lookup, directory publication and creation
//! diagnostics. This module contains the cold-path allocation step used
//! after lookup misses, before the new room is visible to other runtime
//! entrypoints.
//!
//! A factory-created room receives fresh process-local placement, the immutable
//! runtime policy selected at boot and the shared runtime metrics catalog. It
//! does not register the room or emit creation events.
//!
//! Same-room worker placement is not decided here. The factory
//! gives a room its stable instance id and primary router id, while
//! `RoomManager::join_user` assigns workers from packet-loop heartbeat delays
//! when sessions arrive.

use std::sync::{Arc, Mutex};

use o_sfu_router::{RouterId, rtp::MediaCapabilities};
use secrecy::SecretString;

use super::{Room, RoomRuntimeContext};
use crate::{
    RoomMediaLimits, RoomWorkerPolicy, RuntimeFeatureFlags, VideoAdaptationTuning,
    engine::{RoomInstanceId, metrics::RuntimeMetrics, sync::lock_unpoisoned},
};

/// admission limits that stay fixed for one room lifetime
///
/// this is kept separate from the wider runtime policy because admission is a
/// narrow concern with its own tests and state checks
///
/// the policy is passed into `RoomState` at construction time and then treated
/// as immutable room configuration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoomAdmissionPolicy {
    /// maximum number of live users the room accepts at once
    ///
    /// replaced connections still consume this budget until the room transition
    /// finishes and the old live user has been removed
    pub max_sessions: usize,
}

impl RoomAdmissionPolicy {
    #[must_use]
    pub const fn new(max_sessions: usize) -> Self {
        Self { max_sessions }
    }
}

/// stable runtime policy bundle shared by the room and its state model
///
/// this groups the room rules that are fixed for the room lifetime and read by
/// more than one boundary during join, negotiation and observability work
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomRuntimePolicy {
    /// room-level admission limits enforced by room state
    pub admission_policy: RoomAdmissionPolicy,
    /// feature surface the room advertises to clients
    pub feature_flags: RuntimeFeatureFlags,
    /// router-native capability baseline used for negotiation and bootstrap
    pub router_rtp_capabilities: MediaCapabilities,
    /// same-room local worker-placement policy selected at runtime boot
    pub room_worker_policy: RoomWorkerPolicy,
    /// room media activation caps applied by source policy
    pub media_limits: RoomMediaLimits,
    /// receiver video adaptation knobs applied by source policy
    pub video_adaptation_tuning: VideoAdaptationTuning,
}

impl RoomRuntimePolicy {
    #[must_use]
    pub fn new(
        admission_policy: RoomAdmissionPolicy,
        feature_flags: RuntimeFeatureFlags,
        router_rtp_capabilities: MediaCapabilities,
    ) -> Self {
        Self {
            admission_policy,
            feature_flags,
            router_rtp_capabilities,
            room_worker_policy: RoomWorkerPolicy::strict_single_router(),
            media_limits: RoomMediaLimits::default(),
            video_adaptation_tuning: VideoAdaptationTuning::default(),
        }
    }

    /// return a room policy that uses the provided same-room worker policy
    #[must_use]
    pub fn with_room_worker_policy(mut self, room_worker_policy: RoomWorkerPolicy) -> Self {
        self.room_worker_policy = room_worker_policy;
        self
    }

    /// return a room policy that uses the provided media activation limits
    #[must_use]
    pub fn with_media_limits(mut self, media_limits: RoomMediaLimits) -> Self {
        self.media_limits = media_limits;
        self
    }

    #[must_use]
    pub fn with_video_adaptation_tuning(
        mut self,
        video_adaptation_tuning: VideoAdaptationTuning,
    ) -> Self {
        self.video_adaptation_tuning = video_adaptation_tuning;
        self
    }
}

/// external room config passed in from the http or runtime edge
///
/// this type keeps room identity separate from operator-facing knobs and
/// compatibility toggles that may be chosen per room at creation time
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomConfig {
    /// whether this room should expose WebRTC to clients at all
    pub web_rtc_enabled: bool,
    /// compatibility recording address from `/v1/channel`
    pub recording_address: Option<String>,
}

impl Default for RoomConfig {
    fn default() -> Self {
        Self {
            web_rtc_enabled: true,
            recording_address: None,
        }
    }
}

pub(super) struct RoomInit {
    /// runtime-local instance and primary placement for the room
    pub(super) runtime_context: RoomRuntimeContext,
    /// validated room policy copied from runtime startup
    pub(super) runtime_policy: RoomRuntimePolicy,
    /// compatibility-facing issuer captured at room creation
    pub(super) issuer: String,
    /// room key captured from the first create request
    pub(super) key: SecretString,
    /// room-level compatibility configuration
    pub(super) config: RoomConfig,
    /// process metric catalog used by room observers
    pub(super) metrics: Arc<RuntimeMetrics>,
}

/// Monotonic placement counters assigned by the current process.
///
/// Room instance ids and router ids are allocated under one lock so every
/// new room receives one coherent runtime placement. The counters are not a
/// distributed identity source and must not leak into the Odoo-facing room
/// contract.
#[derive(Debug)]
struct RoomRuntimeAllocator {
    next_room_instance_id: u64,
    /// Next router id to allocate for room-local topology.
    ///
    /// Room creation consumes one primary router id. Dynamic spillover
    /// placement consumes additional router ids when sessions join.
    next_router_id: u64,
}

/// Cold-path constructor for rooms that are new to the directory.
///
/// `RoomFactory` keeps runtime-wide creation dependencies behind the manager
/// so `RoomManager::serve_room` can focus on idempotent lookup and
/// publication. Each call to [`Self::create`] returns an unpublished
/// [`Room`] with fresh process-local placement. The caller must insert it in
/// the directory before exposing it to other runtime entrypoints.
#[derive(Debug)]
pub(crate) struct RoomFactory {
    /// Runtime-wide room rules cloned into each room.
    ///
    /// Keeping the policy here makes every room start from the validated
    /// boot-time policy while still letting the room own its copy.
    runtime_policy: RoomRuntimePolicy,
    /// Process metric catalog cloned into each room.
    metrics: Arc<RuntimeMetrics>,
    /// Serialized allocator for process-local placement ids.
    ///
    /// This keeps concurrent create requests from receiving the same runtime
    /// placement.
    allocator: Mutex<RoomRuntimeAllocator>,
}

impl RoomFactory {
    /// Builds the factory for one [`RoomManager`](super::RoomManager) lifetime.
    #[must_use]
    pub(crate) fn new(runtime_policy: RoomRuntimePolicy, metrics: Arc<RuntimeMetrics>) -> Self {
        Self {
            runtime_policy,
            metrics,
            allocator: Mutex::new(RoomRuntimeAllocator {
                next_room_instance_id: 0,
                next_router_id: 0,
            }),
        }
    }

    /// Creates an unpublished room from one manager lookup miss
    ///
    /// The room emits no creation diagnostics. `RoomManager` publishes it
    /// before emitting its creation event
    #[must_use]
    pub(crate) fn create(&self, issuer: &str, key: SecretString, config: &RoomConfig) -> Arc<Room> {
        Arc::new(Room::new(RoomInit {
            runtime_context: self.allocate_runtime_context(),
            runtime_policy: self.runtime_policy.clone(),
            issuer: issuer.to_owned(),
            key,
            config: config.clone(),
            metrics: Arc::clone(&self.metrics),
        }))
    }

    /// Reserves runtime-local placement for one new room.
    ///
    /// The primary router id is allocated here, but worker placement remains
    /// unset until the first session join assigns the room from live load data.
    ///
    /// The mutex is poisoned-tolerant because placement allocation has no
    /// partial side effect beyond the counters themselves. Recovering the inner
    /// value keeps later room creation possible after an unrelated panic.
    fn allocate_runtime_context(&self) -> RoomRuntimeContext {
        let (room_instance_id, primary_router_id) = {
            let mut allocator = lock_unpoisoned(&self.allocator);
            let room_instance_id = RoomInstanceId::allocate(&mut allocator.next_room_instance_id);
            let primary_router_id = RouterId(allocator.next_router_id);
            allocator.next_router_id = allocator.next_router_id.saturating_add(1);
            drop(allocator);
            (room_instance_id, primary_router_id)
        };
        RoomRuntimeContext::new_unassigned(room_instance_id, primary_router_id)
    }

    /// reserve a new process-unique identifier for a spillover router
    ///
    /// this provides the room engine with a thread-safe way to allocate new router
    /// identities on the fly when media load exceeds the primary worker capacity.
    /// the allocator lock is held only long enough to increment the counter, keeping
    /// the cold-path creation from blocking active request loops
    pub(super) fn allocate_spillover_router(&self) -> RouterId {
        let mut allocator = lock_unpoisoned(&self.allocator);
        let router_id = RouterId(allocator.next_router_id);
        allocator.next_router_id = allocator.next_router_id.saturating_add(1);
        router_id
    }
}
