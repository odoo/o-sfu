//! whole-tick packet-loop fixture for one realistic twelve-person meeting
//!
//! the other fixtures in this directory each isolate one packet-loop helper so a
//! base-versus-head comparison can attribute a regression to a single function
//! this fixture answers the opposite question: what does a second of a real room
//! actually cost when every phase of the packet loop runs against the same state
//!
//! this module is the single description of the scenario. the Callgrind driver
//! in `tests/benchmarks/meeting_flow_callgrind.rs` only picks the measured window
//!
//! # measured shape
//!
//! one worker owns twelve local RTC sessions in one room
//! every participant publishes audio, six publish a three-layer simulcast camera
//! and every participant receives the admitted audio sources plus one featured
//! and four thumbnail video layers
//! two relay targets stand in for receivers homed on another worker
//!
//! each synthetic 20 ms tick projects the room decision and stages that tick's
//! ingress, then drives the real [`PacketLoopTurn::pump`] so the production
//! phase order runs as-is instead of being replayed in fixture code that can
//! drift from the worker
//!
//! 1. rotate the audio floor and project the room decision as producer activity
//! 2. refresh the audio trace and mark every session dirty the way ingress does
//! 3. stage the tick's ingress packets with fresh sequence numbers, RTP
//!    timestamps, receive times, VP8 frames and audio metadata
//! 4. run the production turn: ready-session drain, bounded relay mailbox
//!    drain, keyframe request resolution, ingress observation, destination
//!    planning and forward flushing, all in `pump`'s order
//! 5. recycle the observed packets and consume relay mailboxes on behalf of the
//!    peer worker
//! 6. apply the periodic route-control work a room recomputation produces:
//!    receiver bandwidth estimates, featured-layer switches and coalesced
//!    keyframe feedback, then sample egress bitrate and active speakers
//!
//! # what this fixture cannot reach
//!
//! the scenario is socket-free, so no session completes ICE and DTLS and
//! `poll_output` yields no datagrams
//! the drain phase still runs the real ready-session scheduler, the `str0m` poll
//! loop and immediate-timeout feedback, and local egress is proven by the
//! periodic egress-bitrate samples instead
//!
//! the whole turn runs on the synthetic clock, drain included, so no phase can
//! cross a deadline just because the host was slow
//!
//! staged ingress carries the source shape of local ingress, a worker-local
//! session handle with origin fanout enabled, so source resolution pays the same
//! slot lookup production pays for a publisher's own packets
//!
//! it also carries the RTP and codec state of the streams it claims to be. every
//! simulcast layer is declared on its producer session, publishes under its own
//! SSRC and advances one sequence, and its packets are real VP8 frames sent at
//! a payload type the source negotiated. a fixture that skips any of these still
//! forwards packets, but it measures the wrong halves of three paths: consumer
//! projection never leaves its discontinuity branch, keyframe feedback for a
//! layer no producer stream exists for is dropped before it reaches the
//! publisher, and codec inspection returns an empty packet without parsing a
//! descriptor, detecting a keyframe or rewriting an identity
//! the header storage is the one part that cannot match: production keeps the
//! whole `str0m` packet for local ingress, and `RtpPacket` has private fields and
//! no public constructor, so only a session that completed ICE and DTLS can
//! produce one. the staged packets therefore use the relay-shaped header storage,
//! which costs one enum arm in `local_send_packet` and `rtp_header`
//!
//! # why the branches matter
//!
//! `observe_packet` dispatches on the VAD extension, and only the `None` arm
//! reaches `observe_audio_level`. covering the noise-floor, speech-threshold and
//! promotion branches therefore needs clients that negotiated the audio-level
//! extension but not VAD, which real browsers do, so the trace models three of
//! them alongside one client with no audio extension at all
//! the bandwidth trace dips and recovers, so receiver bandwidth estimates change
//! and wake source-policy recomputation instead of staying unset
//! this is the difference between measuring a room and measuring join
//! bookkeeping

#![allow(
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::panic,
    clippy::expect_used,
    clippy::too_many_lines,
    reason = "this benchmark-owned fixture is a readable fixed scenario that must fail loudly rather than a reusable public API"
)]

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use o_sfu_rfc::rtp::{CodecName, vp8};
use o_sfu_router::{
    MediaKind as RouterMediaKind,
    rtp::{MediaFormat, MediaStream as RouterRtpParameters, PayloadType},
};
use str0m::{
    Event,
    bwe::{Bitrate as Str0mBitrate, BweKind},
    media::{KeyframeRequestKind, MediaKind, Mid, Pt, Rid},
    rtp::{Ssrc, Vp8Descriptor},
};
use tokio::sync::mpsc;

use super::super::{
    RtcWorkerConfig, RtpProfile,
    bitrate::BitrateRegistry,
    bootstrap, codec,
    commands::WorkerMediaControlBatch,
    forwarded_packet::ForwardedPacket,
    media_registry::RegisteredMediaHandle,
    packet_loop::{
        BenchmarkTurnInput, PacketLoopConfig, PacketLoopDelaySnapshot, PacketLoopTurn,
        PendingKeyframeRequest, observe_rtc_event_for_benchmark,
    },
    relay_registry::{RelayPacketMailbox, RelayTargetId},
    route_control::PacketLayerGate,
    slots::SessionHandle,
    source_route::MediaRouteDestination,
    state::{PacketLoopState, RtcSnapshotState},
    test_support::{
        BenchmarkPacketStaging, BenchmarkStreamIdentity, restage_packet_for_benchmark,
        sample_local_forwarded_packet_for_benchmark, test_transport_session_key,
    },
    worker::{apply_media_control_batch, guarded_pkt_gate},
};
use crate::{
    Bitrate, CodecPreferences, MediaCodecFlags, SessionBitrateLimits, VideoBitrateLimits,
    engine::{
        UserId,
        media_transport::{
            ActiveSpeakerActivityReason, ActiveSpeakerSourceDiagnostic, ProducerActivity,
            ReceiverBweTargetUpdate, SourceActivityRevision, SourceActivityUpdate,
            SourcePolicySignal, SourcePolicyUpdateSubscription, TransportConsumerRoute,
            TransportMediaId, TransportSessionKey, TransportSourceKey,
            route_control::ProducerRouteControl,
        },
        metrics::{RtcMetricsRecorder, RuntimeMetrics},
        packet_sink_registry::RoomPacketSinkRegistry,
    },
};

/// synthetic media tick used by the scenario clock
pub const MEETING_TICK_MS: u64 = 20;
/// participants in the modelled room, all publishing audio
pub const MEETING_PARTICIPANTS: usize = 12;
/// participants publishing a three-layer simulcast camera
pub const MEETING_VIDEO_PUBLISHERS: usize = 6;
/// audio sources the room admits at the same time
pub const MEETING_ADMITTED_AUDIO_SOURCES: usize = 3;
/// video subscriptions each receiver holds: one featured plus four thumbnails
pub const MEETING_VIDEO_SUBSCRIPTIONS: usize = 5;
/// measured window for the pull-request scale case
pub const MEETING_SHORT_SECONDS: u64 = 2;
/// measured window for the full realistic case
pub const MEETING_LONG_SECONDS: u64 = 12;

const ROOM_ID: &str = "meeting-benchmark";
const ROOM_INSTANCE_ID: u64 = 500;
const WORKER_IDX: usize = 0;
const FIRST_CONNECTION_ID: u64 = 5_000;
const FIRST_USER_ID: i64 = 6_000;
const FIRST_CANDIDATE_PORT: u16 = 48_000;
const SESSION_MAX_BITRATE: u64 = 10;

const AUDIO_PAYLOAD_BYTES: usize = 80;
const AUDIO_RTP_TICK_STRIDE: u32 = 960;
const VIDEO_RTP_TICK_STRIDE: u32 = 3_000;

/// the Opus payload type every microphone in the room negotiated
const AUDIO_PAYLOAD_TYPE: u8 = 111;
/// the VP8 payload type every camera in the room negotiated
///
/// packet inspection is keyed by negotiated payload type, so a camera whose
/// packets carry a type the source never negotiated is never inspected at all
const VIDEO_PAYLOAD_TYPE: u8 = 96;
const VIDEO_CLOCK_RATE: u32 = 90_000;

/// frames one layer's prebuilt payload ring holds before it repeats
///
/// the ring starts on a keyframe, so this is also the layer's keyframe period in
/// frames. publishers start at different points in the ring, so the room's
/// keyframes spread across ticks instead of arriving as one synchronized burst
const VIDEO_FRAME_RING: usize = 10;
const VIDEO_FRAME_WIDTH: u16 = 640;
const VIDEO_FRAME_HEIGHT: u16 = 360;
/// RFC 6386 section 9.1 keyframe sync code, which a decodable keyframe carries
const VP8_KEYFRAME_SYNC_CODE: [u8; 3] = [0x9d, 0x01, 0x2a];

/// simulcast layer profile driving the per-tick video packet mix
///
/// `packets_per_frame` is how many packets the layer's frame is split into and
/// `frame_period` is how many ticks apart its frames are, so a layer's packet
/// rate is `packets_per_frame / frame_period` packets per 20 ms
///
/// `payload_bytes` is the layer's per-packet payload size. the frames are built
/// once per layer at setup and shared by every publisher through `Arc`, so the
/// ladder costs no per-tick allocation
#[derive(Debug, Clone, Copy)]
struct VideoLayerProfile {
    rid: &'static str,
    payload_bytes: usize,
    packets_per_frame: usize,
    frame_period: usize,
}

const VIDEO_LAYERS: [VideoLayerProfile; 3] = [
    VideoLayerProfile {
        rid: "hi",
        payload_bytes: 1_100,
        packets_per_frame: 3,
        frame_period: 1,
    },
    VideoLayerProfile {
        rid: "md",
        payload_bytes: 700,
        packets_per_frame: 2,
        frame_period: 2,
    },
    VideoLayerProfile {
        rid: "lo",
        payload_bytes: 300,
        packets_per_frame: 1,
        frame_period: 1,
    },
];

const FEATURED_RID: &str = "hi";
const THUMBNAIL_RID: &str = "lo";

/// layout slot meaning "this receiver holds no featured tile"
///
/// publisher indices stop below `MEETING_VIDEO_PUBLISHERS`, so the count itself is
/// free to stand for the empty slot
const NO_FEATURED_PUBLISHER: usize = MEETING_VIDEO_PUBLISHERS;

/// speaking turns last one second, the short end of a real conversational floor
const AUDIO_FLOOR_TICKS: usize = 50;
/// secondary admitted speakers interject on this cadence
const INTERJECTION_TICKS: usize = 25;
/// the featured publisher changes every two seconds
const FEATURED_SWITCH_TICKS: usize = 100;
/// receiver bandwidth estimates arrive twice a second
const BWE_TICKS: usize = 25;
/// ticks driven outside the measured window to observe a full audio-level cycle
const REASON_OBSERVATION_TICKS: usize = INTERJECTION_TICKS * 2 + 2;
/// consumer keyframe feedback bursts once a second
const KEYFRAME_FEEDBACK_TICKS: usize = 50;

const SPEECH_LEVELS_DBOV: [i8; 4] = [-24, -20, -28, -22];
const SECONDARY_SPEECH_DBOV: i8 = -30;
const NOISE_FLOOR_DBOV: i8 = -68;

/// participants whose clients negotiated the audio-level extension but not VAD
///
/// these are the only packets that reach `observe_audio_level`, so they are what
/// covers its noise-floor, speech-threshold and promotion-window branches
const AUDIO_LEVEL_ONLY_PARTICIPANTS: usize = 8;
/// one participant negotiated no audio extension at all
///
/// packets with neither field never create audio policy state, so this covers the
/// no-metadata early exit rather than an active-speaker decision
const EXTENSIONLESS_PARTICIPANT: usize = 11;
/// below the policy's noise floor
const BELOW_NOISE_FLOOR_DBOV: i8 = -64;
/// between the noise floor and the speech threshold
const BETWEEN_THRESHOLDS_DBOV: i8 = -52;
/// above the speech threshold, so repeated observations promote the source
const ABOVE_SPEECH_THRESHOLD_DBOV: i8 = -30;

const RELAY_TARGETS: usize = 2;
const RELAY_MAILBOX_CAPACITY: usize = 512;
const RECEIVER_BANDWIDTH_TRACE_BPS: [u64; 6] = [
    2_400_000, 2_200_000, 1_500_000, 700_000, 1_100_000, 2_000_000,
];
const DEFAULT_RECEIVER_BANDWIDTH_BPS: u64 = 1_000_000;

/// monotonic work counters used as the benchmark's anti-elimination anchor
///
/// every field only ever grows during a run, so the returned total is real work
/// the scenario performed rather than a snapshot of final room size
#[derive(Debug, Default, Clone, Copy)]
pub struct MeetingWorkProfile {
    pub observed_packets: usize,
    pub planned_forwards: usize,
    pub relay_packets_consumed: usize,
    pub egress_bitrate_samples: usize,
    /// most sources the audio policy ever held as active speakers at once
    pub max_active_speakers: usize,
    /// most audio sources the route table ever had forwarding at once
    pub max_active_audio_sources: usize,
    /// times the route table's set of forwarding audio sources actually changed
    ///
    /// this is what proves the audio floor moved. counting the producer-activity
    /// updates the fixture sent proves only that it tried: the batch result is
    /// dropped, and a stale revision is a successful no-op
    pub observed_audio_floor_moves: usize,
    /// times the route table's set of featured video gates actually changed
    pub observed_featured_switches: usize,
    /// samples where some receiver did not hold exactly one featured video tile
    ///
    /// this is a violation count rather than work, so it stays out of
    /// `total_work`: a healthy run leaves it at zero
    pub featured_layout_violations: usize,
    pub policy_wakeups: usize,
    pub producer_activity_updates: usize,
    pub consumer_gate_updates: usize,
    pub receiver_bwe_updates: usize,
    pub keyframe_flushes: usize,
    pub speech_observations: usize,
    pub silence_observations: usize,
    pub audio_level_fallback_observations: usize,
    pub extensionless_observations: usize,
}

impl MeetingWorkProfile {
    pub fn total_work(self) -> usize {
        [
            self.observed_packets,
            self.planned_forwards,
            self.relay_packets_consumed,
            self.egress_bitrate_samples,
            self.max_active_speakers,
            self.max_active_audio_sources,
            self.observed_audio_floor_moves,
            self.observed_featured_switches,
            self.policy_wakeups,
            self.producer_activity_updates,
            self.consumer_gate_updates,
            self.receiver_bwe_updates,
            self.keyframe_flushes,
            self.speech_observations,
            self.silence_observations,
            self.audio_level_fallback_observations,
            self.extensionless_observations,
        ]
        .into_iter()
        .fold(0, usize::saturating_add)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MeetingStreamKind {
    Audio,
    Video,
}

/// one producer stream: a participant's microphone or one layer of their camera
///
/// a layer is one stream on the wire, so its packets share an SSRC and carry one
/// monotonic sequence, and the packets of one frame share that frame's RTP
/// timestamp. staging packets that repeat a sequence number instead would leave
/// every consumer projection on its discontinuity path for the whole run
struct MeetingStream {
    /// reusable packet slots, one per packet of this stream's frame
    packets: Vec<Option<ForwardedPacket>>,
    /// prebuilt VP8 frames this stream walks, for video streams
    frames: Option<Arc<VideoLayerFrames>>,
    kind: MeetingStreamKind,
    participant: usize,
    frame_period: usize,
    frame_offset: usize,
    /// where in the payload ring this stream's next frame sits
    frame_cursor: usize,
    rtp_timestamp: u32,
    sequence_number: u64,
}

impl MeetingStream {
    fn emits_on(&self, tick: usize) -> bool {
        tick.wrapping_add(self.frame_offset)
            .is_multiple_of(self.frame_period)
    }
}

/// the prebuilt VP8 frames of one simulcast layer
///
/// frame 0 of the ring is a keyframe and the rest are interframes. every frame
/// carries its own picture id and TL0PICIDX, so the receiver-side VP8 projection
/// advances the way it does for a real encoder instead of restating one identity
struct VideoLayerFrames {
    /// the first packet of each frame, which carries the frame start bit
    starts: Vec<Arc<[u8]>>,
    /// the remaining packets of each frame, sharing their frame's picture id
    continuations: Vec<Arc<[u8]>>,
}

impl VideoLayerFrames {
    fn new(payload_bytes: usize) -> Self {
        Self {
            starts: (0..VIDEO_FRAME_RING)
                .map(|frame| vp8_payload(frame, true, payload_bytes))
                .collect(),
            continuations: (0..VIDEO_FRAME_RING)
                .map(|frame| vp8_payload(frame, false, payload_bytes))
                .collect(),
        }
    }

    /// the payload one packet of a ring frame carries
    fn payload(&self, frame: usize, packet: usize) -> &Arc<[u8]> {
        let ring = if packet == 0 {
            &self.starts
        } else {
            &self.continuations
        };
        ring.get(frame % VIDEO_FRAME_RING)
            .unwrap_or_else(|| panic!("meeting benchmark layer frame {frame} should be prebuilt"))
    }
}

/// one local video destination owned by a receiver
struct MeetingVideoDestination {
    route: TransportConsumerRoute,
    receiver: usize,
    /// index of this destination inside its source's route, for route-state reads
    dst_idx: usize,
    mid: Mid,
    featured: bool,
}

/// one video publisher plus the destinations subscribed to it
struct MeetingVideoPublisher {
    source: TransportSourceKey,
    destinations: Vec<MeetingVideoDestination>,
}

/// one participant session with its producer identities
struct MeetingParticipant {
    session_key: TransportSessionKey,
    audio_source: TransportSourceKey,
}

pub struct MeetingFlowBenchFixture {
    state: PacketLoopState,
    snapshot_state: Arc<Mutex<RtcSnapshotState>>,
    metrics: Arc<RuntimeMetrics>,
    rtc_metrics: Arc<RtcMetricsRecorder>,
    source_policy_signal: SourcePolicySignal,
    source_policy_updates: SourcePolicyUpdateSubscription,
    turn: PacketLoopTurn,
    packet_loop_config: PacketLoopConfig,
    relay_rx: mpsc::Receiver<ForwardedPacket>,
    bitrate_registry: Arc<Mutex<BitrateRegistry>>,
    participants: Vec<MeetingParticipant>,
    session_keys: Vec<TransportSessionKey>,
    video_publishers: Vec<MeetingVideoPublisher>,
    streams: Vec<MeetingStream>,
    /// the prebuilt frames of each simulcast layer, shared by every publisher
    video_frames: Vec<Arc<VideoLayerFrames>>,
    /// the stream slot each staged packet came from, in staging order
    staged_streams: Vec<(usize, usize)>,
    audio_plan: Vec<(Option<bool>, Option<i8>)>,
    relay_receivers: Vec<mpsc::Receiver<ForwardedPacket>>,
    activity_revision: SourceActivityRevision,
    admitted_audio: Vec<usize>,
    /// participants the route table was last seen forwarding audio for
    ///
    /// this is read back out of the route table, not projected from
    /// `admitted_audio`, so it is what detects the floor actually moving
    forwarding_audio_sources: u16,
    /// how far every receiver's featured slot has walked along its subscriptions
    featured_rotation: usize,
    /// the featured publisher each receiver was last seen holding in the route table
    featured_layout: [usize; MEETING_PARTICIPANTS],
    tick_cursor: usize,
    ticks: usize,
    now: Instant,
    profile: MeetingWorkProfile,
}

impl MeetingFlowBenchFixture {
    /// builds the two-second case used as a cheap pull-request gate
    pub fn short_meeting() -> Self {
        Self::twelve_person_meeting(MEETING_SHORT_SECONDS)
    }

    /// builds the full twelve-second case
    pub fn long_meeting() -> Self {
        Self::twelve_person_meeting(MEETING_LONG_SECONDS)
    }

    /// builds one meeting scenario and warms it to steady state
    ///
    /// warm-up runs a full second of ticks so first-ingress keyframe probes, RID
    /// readiness transitions and buffer capacity growth are all behind the
    /// measured window
    pub fn twelve_person_meeting(seconds: u64) -> Self {
        let mut fixture = Self::build(seconds);
        fixture.run_ticks(ticks_for_seconds(1));
        fixture.profile = MeetingWorkProfile::default();
        fixture
    }

    /// runs the whole measured meeting and returns its accumulated work
    ///
    /// this is the measured body of the scenario benchmark
    pub fn run_meeting(&mut self) -> usize {
        self.run_ticks(self.ticks);
        self.profile.total_work()
    }

    /// advances the scenario clock by a number of ticks
    ///
    /// the tick cursor is never reset, so the measured window continues the
    /// timeline warm-up left behind instead of replaying the room's first second
    fn run_ticks(&mut self, ticks: usize) {
        for _ in 0..ticks {
            let tick = self.tick_cursor;
            self.run_tick(tick);
            self.tick_cursor = self.tick_cursor.saturating_add(1);
        }
    }

    /// reports what the last run actually exercised
    ///
    /// the scenario self-test asserts on this instead of trusting that the flow
    /// still reaches the branches it was built for
    pub const fn work_profile(&self) -> MeetingWorkProfile {
        self.profile
    }

    /// reports which active-speaker decisions the audio policy actually reached
    ///
    /// this reads diagnostics outside the measured window. it is the only check
    /// that can tell the difference between a trace that carries varied audio
    /// metadata and one that hardcodes it: the reasons collapse to a single value
    /// the moment the trace stops branching
    pub fn observed_activity_reasons(&mut self) -> Vec<ActiveSpeakerActivityReason> {
        let source_ids = self
            .participants
            .iter()
            .map(|participant| participant.audio_source.transport_media_id())
            .collect::<Vec<_>>();
        let mut reasons = Vec::new();
        // a diagnostic is the state of one instant, and the audio-level clients move
        // across their thresholds over time, so one snapshot cannot see promotion and
        // demotion both. driving a full alternation period here keeps the check
        // independent of how many ticks the measured window happened to run
        for _ in 0..REASON_OBSERVATION_TICKS {
            let tick = self.tick_cursor;
            self.run_tick(tick);
            self.tick_cursor = self.tick_cursor.saturating_add(1);
            reasons.extend(
                self.state
                    .routes
                    .active_speaker_diagnostics(&source_ids, self.now)
                    .into_iter()
                    .map(ActiveSpeakerSourceDiagnostic::reason),
            );
        }
        reasons.sort_unstable_by_key(|reason| format!("{reason:?}"));
        reasons.dedup();
        reasons
    }

    /// reports the receiver bandwidth estimates the packet loop recorded
    pub fn receiver_bandwidth_estimates(&self) -> usize {
        self.snapshot_state
            .lock()
            .map(|snapshot_state| {
                snapshot_state
                    .receiver_bandwidth_snapshot(&self.session_keys)
                    .per_session
                    .len()
            })
            .unwrap_or_default()
    }

    fn run_tick(&mut self, tick: usize) {
        self.rotate_audio_floor(tick);
        self.refresh_audio_plan(tick);
        // every session is marked dirty up front the way an ingress packet would
        // mark its session, so the real ready-session drain polls the whole room
        // every tick instead of waiting for str0m timeouts
        for participant in &self.participants {
            self.state.mark_session_dirty(&participant.session_key);
        }
        let turn_input = BenchmarkTurnInput {
            packets: self.stage_tick_packets(tick),
            keyframe_requests: self.stage_keyframe_feedback(tick),
            now: self.now,
        };
        self.turn.pump_for_benchmark(
            &mut self.state,
            &self.bitrate_registry,
            &self.snapshot_state,
            &self.packet_loop_config,
            &mut self.relay_rx,
            turn_input,
        );
        self.profile.planned_forwards = self
            .profile
            .planned_forwards
            .saturating_add(self.turn.take_planned_forwards_for_benchmark());
        self.recycle_tick_packets();
        self.consume_relay_mailboxes();
        self.sample_egress_bitrate(tick);
        self.sample_active_speakers(tick);
        self.sample_admitted_audio(tick);
        self.sample_featured_layout(tick);
        self.observe_receiver_bandwidth(tick);
        self.switch_featured_layer(tick);
        self.collect_policy_wakeups();
        self.now += Duration::from_millis(MEETING_TICK_MS);
    }

    /// projects the room's audio floor decision as producer activity
    ///
    /// production pauses audio sources the room did not admit, so packets from
    /// those producers still reach ingress but stop before destination planning
    fn rotate_audio_floor(&mut self, tick: usize) {
        if !tick.is_multiple_of(AUDIO_FLOOR_TICKS) {
            return;
        }
        let admitted = admitted_audio_for_turn(tick / AUDIO_FLOOR_TICKS);
        // a speaking turn moves the floor rather than restating every source, so
        // only the sources whose admission actually changed are sent
        let changed = (0..MEETING_PARTICIPANTS)
            .filter(|participant| {
                admitted.contains(participant) != self.admitted_audio.contains(participant)
            })
            .collect::<Vec<_>>();
        self.apply_audio_admission(&admitted, &changed);
    }

    /// pauses every audio source the room did not admit, once, before the run
    ///
    /// the route table starts every source active and `rotate_audio_floor` only
    /// sends the difference between two speaking turns, so without this the room
    /// opens with all twelve audio sources forwarding and sheds the surplus three
    /// at a time. the two-second case would never reach three admitted sources at
    /// all, and both cases would spend their measured window on an audio fanout no
    /// real room produces
    fn seed_audio_admission(&mut self) {
        let admitted = admitted_audio_for_turn(0);
        let every_source = (0..MEETING_PARTICIPANTS).collect::<Vec<_>>();
        self.apply_audio_admission(&admitted, &every_source);
    }

    /// sends the admission decision for `changed` as one producer-activity batch
    fn apply_audio_admission(&mut self, admitted: &[usize], changed: &[usize]) {
        let mut updates = Vec::with_capacity(changed.len());
        self.activity_revision = self.activity_revision.next();
        for participant in changed.iter().copied() {
            let Some(source) = self
                .participants
                .get(participant)
                .map(|entry| entry.audio_source.clone())
            else {
                continue;
            };
            let activity = if admitted.contains(&participant) {
                ProducerActivity::Active
            } else {
                ProducerActivity::Inactive
            };
            updates.push((
                updates.len(),
                ProducerRouteControl {
                    source,
                    update: SourceActivityUpdate::new(activity, self.activity_revision),
                },
            ));
        }
        self.admitted_audio = admitted.to_vec();
        if updates.is_empty() {
            return;
        }
        self.profile.producer_activity_updates = self
            .profile
            .producer_activity_updates
            .saturating_add(updates.len());
        let _ = apply_media_control_batch(
            &mut self.state,
            &self.rtc_metrics,
            Bitrate::from_mbps(SESSION_MAX_BITRATE),
            self.now,
            WorkerMediaControlBatch::ProducerActivity(updates),
        );
    }

    fn refresh_audio_plan(&mut self, tick: usize) {
        let Self {
            admitted_audio,
            audio_plan,
            profile,
            ..
        } = self;
        for participant in 0..MEETING_PARTICIPANTS {
            let activity = audio_activity_for(tick, participant, admitted_audio);
            match activity {
                (None, None) => {
                    profile.extensionless_observations =
                        profile.extensionless_observations.saturating_add(1);
                }
                (None, Some(_)) => {
                    profile.audio_level_fallback_observations =
                        profile.audio_level_fallback_observations.saturating_add(1);
                }
                (Some(true), _) => {
                    profile.speech_observations = profile.speech_observations.saturating_add(1);
                }
                (Some(false), _) => {
                    profile.silence_observations = profile.silence_observations.saturating_add(1);
                }
            }
            if let Some(slot) = audio_plan.get_mut(participant) {
                *slot = activity;
            }
        }
    }

    fn stage_tick_packets(&mut self, tick: usize) -> Vec<ForwardedPacket> {
        let Self {
            streams,
            staged_streams,
            audio_plan,
            now,
            ..
        } = self;
        staged_streams.clear();
        let mut staged_packets = Vec::new();
        for stream_idx in 0..streams.len() {
            let Some(stream) = streams.get_mut(stream_idx) else {
                continue;
            };
            if !stream.emits_on(tick) {
                continue;
            }
            let (voice_activity, audio_level) = match stream.kind {
                MeetingStreamKind::Audio => audio_plan
                    .get(stream.participant)
                    .copied()
                    .unwrap_or((None, None)),
                MeetingStreamKind::Video => (None, None),
            };
            // one frame per emitting tick, so every packet the frame is split
            // into carries the frame's timestamp
            let frame_stride = match stream.kind {
                MeetingStreamKind::Audio => AUDIO_RTP_TICK_STRIDE,
                MeetingStreamKind::Video => VIDEO_RTP_TICK_STRIDE
                    .saturating_mul(u32::try_from(stream.frame_period).unwrap_or(1)),
            };
            stream.rtp_timestamp = stream.rtp_timestamp.wrapping_add(frame_stride);
            stream.frame_cursor = stream.frame_cursor.wrapping_add(1);
            let frame_cursor = stream.frame_cursor;
            let packets_per_frame = stream.packets.len();
            for slot in 0..packets_per_frame {
                let Some(mut packet) = stream.packets.get_mut(slot).and_then(Option::take) else {
                    panic!("meeting benchmark stream {stream_idx} lost reusable packet {slot}");
                };
                stream.sequence_number = stream.sequence_number.wrapping_add(1);
                restage_packet_for_benchmark(
                    &mut packet,
                    BenchmarkPacketStaging {
                        sequence_number: stream.sequence_number,
                        rtp_timestamp: stream.rtp_timestamp,
                        marker: slot + 1 == packets_per_frame,
                        voice_activity,
                        audio_level,
                    },
                    stream
                        .frames
                        .as_ref()
                        .map(|frames| frames.payload(frame_cursor, slot)),
                    *now,
                );
                staged_packets.push(packet);
                staged_streams.push((stream_idx, slot));
            }
        }
        self.profile.observed_packets = self
            .profile
            .observed_packets
            .saturating_add(self.staged_streams.len());
        staged_packets
    }

    /// returns this tick's packets to their stream slots for the next tick
    ///
    /// the slots are matched by position, so the turn has to hand the batch back
    /// in the order it was staged. the assertion pins that down: a turn that
    /// dropped, reordered or added a packet would otherwise hand streams the
    /// wrong reusable packet and quietly change what the next tick measures
    fn recycle_tick_packets(&mut self) {
        let Self {
            streams,
            staged_streams,
            turn,
            ..
        } = self;
        let observed_packets = turn.take_packets_for_benchmark();
        assert_eq!(
            observed_packets.len(),
            staged_streams.len(),
            "the turn must return exactly the packets this tick staged"
        );
        for ((stream_idx, slot), packet) in staged_streams.iter().copied().zip(observed_packets) {
            if let Some(reusable) = streams
                .get_mut(stream_idx)
                .and_then(|stream| stream.packets.get_mut(slot))
            {
                *reusable = Some(packet);
            }
        }
    }

    /// consumes relayed packets on behalf of the peer worker
    ///
    /// this fixture models one worker, so cross-worker packets leave the
    /// measured loop here instead of being forwarded a second time
    fn consume_relay_mailboxes(&mut self) {
        let mut consumed = 0_usize;
        for receiver in &mut self.relay_receivers {
            while receiver.try_recv().is_ok() {
                consumed = consumed.saturating_add(1);
            }
        }
        self.profile.relay_packets_consumed =
            self.profile.relay_packets_consumed.saturating_add(consumed);
    }

    /// stages consumer keyframe feedback for the real pump-phase coalescing
    ///
    /// the pump phase resolves these back to the producer source, so this only
    /// builds the requests a consumer session would have emitted during drain
    ///
    /// one publisher's consumers burst together, which is what makes the requests
    /// coalesce. each consumer asks for the layer it is actually receiving, so a
    /// thumbnail viewer does not request a layer it never decodes
    fn stage_keyframe_feedback(
        &mut self,
        tick: usize,
    ) -> Vec<(TransportSessionKey, PendingKeyframeRequest)> {
        if !tick.is_multiple_of(KEYFRAME_FEEDBACK_TICKS) {
            return Vec::new();
        }
        let bursting_publisher = self.featured_rotation % MEETING_VIDEO_PUBLISHERS;
        let Some(publisher) = self.video_publishers.get(bursting_publisher) else {
            return Vec::new();
        };
        let requests = publisher
            .destinations
            .iter()
            .map(|destination| {
                let rid = if destination.featured {
                    FEATURED_RID
                } else {
                    THUMBNAIL_RID
                };
                (
                    destination.route.consumer_session_key().clone(),
                    PendingKeyframeRequest::benchmark_request(
                        destination.mid,
                        Some(Rid::from(rid)),
                        KeyframeRequestKind::Pli,
                    ),
                )
            })
            .collect::<Vec<_>>();
        self.profile.keyframe_flushes = self.profile.keyframe_flushes.saturating_add(1);
        requests
    }

    /// samples how many audio sources the route table is really forwarding
    ///
    /// the fixture's own admission bookkeeping cannot prove this. the route table
    /// starts every source active, so an admission decision that never reaches it
    /// leaves the surplus sources fanning out to eleven receivers each while the
    /// scenario believes they are paused. reading the route state is what makes
    /// the audio fanout in these counts the fanout of a room that admits three
    /// speakers
    fn sample_admitted_audio(&mut self, tick: usize) {
        if !tick.is_multiple_of(BWE_TICKS) {
            return;
        }
        let mut forwarding = 0_u16;
        for (participant, entry) in self.participants.iter().enumerate() {
            if self
                .state
                .routes
                .source_is_active(entry.audio_source.transport_media_id())
            {
                forwarding |= 1_u16 << participant;
            }
        }
        let admitted = usize::try_from(forwarding.count_ones()).unwrap_or(0);
        if admitted > self.profile.max_active_audio_sources {
            self.profile.max_active_audio_sources = admitted;
        }
        if forwarding != self.forwarding_audio_sources {
            self.forwarding_audio_sources = forwarding;
            self.profile.observed_audio_floor_moves =
                self.profile.observed_audio_floor_moves.saturating_add(1);
        }
    }

    /// samples the featured video layout the route table is actually enforcing
    ///
    /// the gates are read back out of the route table rather than off the
    /// fixture's `featured` flags, so a switch that updated the fixture's copy
    /// without reaching the route would be caught. the layout must hold one
    /// featured tile per receiver at every sample: a room where some receiver has
    /// two, or none, is forwarding a mix of layers no client asked for
    fn sample_featured_layout(&mut self, tick: usize) {
        if !tick.is_multiple_of(BWE_TICKS) {
            return;
        }
        let featured_gate = PacketLayerGate::Rid(Rid::from(FEATURED_RID));
        let mut layout = [NO_FEATURED_PUBLISHER; MEETING_PARTICIPANTS];
        let mut doubled = false;
        for (publisher_idx, publisher) in self.video_publishers.iter().enumerate() {
            let Some(route) = self
                .state
                .routes
                .local_route(publisher.source.transport_media_id())
            else {
                continue;
            };
            for destination in &publisher.destinations {
                let Some(routed) = route.destinations.get(destination.dst_idx) else {
                    continue;
                };
                if routed.packet_gate != featured_gate {
                    continue;
                }
                let Some(slot) = layout.get_mut(destination.receiver) else {
                    continue;
                };
                doubled |= *slot != NO_FEATURED_PUBLISHER;
                *slot = publisher_idx;
            }
        }
        if doubled || layout.contains(&NO_FEATURED_PUBLISHER) {
            self.profile.featured_layout_violations =
                self.profile.featured_layout_violations.saturating_add(1);
        }
        if layout != self.featured_layout {
            self.featured_layout = layout;
            self.profile.observed_featured_switches =
                self.profile.observed_featured_switches.saturating_add(1);
        }
    }

    /// samples aggregate egress bitrate the way periodic media-quality reporting does
    ///
    /// this also proves local RTC egress reached receiver sessions, which a
    /// socket-free fixture cannot show through staged datagrams
    fn sample_egress_bitrate(&mut self, tick: usize) {
        if !tick.is_multiple_of(BWE_TICKS) {
            return;
        }
        if self
            .bitrate_registry
            .lock()
            .expect("meeting benchmark bitrate registry should lock")
            .egress_bitrate_snapshot_at(&self.session_keys, self.now)
            > Bitrate::zero()
        {
            self.profile.egress_bitrate_samples =
                self.profile.egress_bitrate_samples.saturating_add(1);
        }
    }

    /// samples the elected active speakers the way a room policy turn does
    ///
    /// production calls `active_speaker_source_snapshot` on every policy turn, so
    /// this is real per-turn work rather than benchmark-only observation
    ///
    /// the high-water mark is what makes the audio trace falsifiable: a producer
    /// whose packets all claim voice activity elects every publisher at once, which
    /// is exactly the distortion the old room benchmark shipped
    fn sample_active_speakers(&mut self, tick: usize) {
        if !tick.is_multiple_of(BWE_TICKS) {
            return;
        }
        let elected = self.state.routes.active_speaker_sources(self.now).len();
        if elected > self.profile.max_active_speakers {
            self.profile.max_active_speakers = elected;
        }
    }

    /// feeds receiver bandwidth estimates through the real event observation path
    ///
    /// each estimate that changes a session's value wakes source-policy
    /// recomputation, which is what makes the room's video budget solver run in
    /// production
    fn observe_receiver_bandwidth(&mut self, tick: usize) {
        if !tick.is_multiple_of(BWE_TICKS) {
            return;
        }
        let sample = tick / BWE_TICKS;
        for (participant, entry) in self.participants.iter().enumerate() {
            observe_rtc_event_for_benchmark(
                &self.snapshot_state,
                &self.metrics,
                &self.rtc_metrics,
                &self.source_policy_signal,
                ROOM_ID,
                &entry.session_key,
                &Event::EgressBitrateEstimate(BweKind::Twcc(Str0mBitrate::from(
                    receiver_bandwidth_bps(sample, participant),
                ))),
            );
        }
        let updates = self
            .participants
            .iter()
            .enumerate()
            .map(|(participant, entry)| {
                (
                    participant,
                    ReceiverBweTargetUpdate::new(
                        entry.session_key.clone(),
                        Bitrate::from_bps(receiver_bandwidth_bps(sample, participant)),
                    ),
                )
            })
            .collect::<Vec<_>>();
        self.profile.receiver_bwe_updates = self
            .profile
            .receiver_bwe_updates
            .saturating_add(updates.len());
        let _ = apply_media_control_batch(
            &mut self.state,
            &self.rtc_metrics,
            Bitrate::from_mbps(SESSION_MAX_BITRATE),
            self.now,
            WorkerMediaControlBatch::ReceiverBwe(updates),
        );
    }

    /// walks every receiver's featured slot to the next publisher it subscribes to
    ///
    /// `MEETING_VIDEO_SUBSCRIPTIONS` describes one featured tile plus four
    /// thumbnails per receiver, so a switch has to move each receiver's own slot.
    /// promoting one publisher for the whole room instead breaks that invariant
    /// two ways: receivers that do not subscribe to it lose their featured tile,
    /// and publishers the rotation has not reached yet keep the tiles the initial
    /// layout gave them, so the room drifts to more featured routes than it has
    /// receivers
    ///
    /// gates are applied per source, so each publisher's promotions and demotions
    /// travel as one source-scoped consumer gate batch
    fn switch_featured_layer(&mut self, tick: usize) {
        if !tick.is_multiple_of(FEATURED_SWITCH_TICKS) {
            return;
        }
        self.featured_rotation = self.featured_rotation.wrapping_add(1);
        let rotation = self.featured_rotation;
        for publisher_idx in 0..MEETING_VIDEO_PUBLISHERS {
            let Some(publisher) = self.video_publishers.get_mut(publisher_idx) else {
                continue;
            };
            let source = publisher.source.clone();
            let mut updates = Vec::with_capacity(publisher.destinations.len());
            for destination in &mut publisher.destinations {
                let featured =
                    featured_publisher_for(destination.receiver, rotation) == Some(publisher_idx);
                if destination.featured == featured {
                    continue;
                }
                destination.featured = featured;
                let gate = if featured {
                    PacketLayerGate::Rid(Rid::from(FEATURED_RID))
                } else {
                    PacketLayerGate::Rid(Rid::from(THUMBNAIL_RID))
                };
                updates.push((updates.len(), destination.route.clone(), gate));
            }
            if updates.is_empty() {
                continue;
            }
            self.profile.consumer_gate_updates = self
                .profile
                .consumer_gate_updates
                .saturating_add(updates.len());
            let _ = apply_media_control_batch(
                &mut self.state,
                &self.rtc_metrics,
                Bitrate::from_mbps(SESSION_MAX_BITRATE),
                self.now,
                WorkerMediaControlBatch::ConsumerGates { source, updates },
            );
        }
    }

    fn collect_policy_wakeups(&mut self) {
        let woken_rooms = self.source_policy_updates.take_pending_updates();
        self.profile.policy_wakeups = self
            .profile
            .policy_wakeups
            .saturating_add(woken_rooms.len());
    }

    fn build(seconds: u64) -> Self {
        let metrics = Arc::new(RuntimeMetrics::default());
        let route_metrics = metrics.register_rtc_worker();
        let packet_metrics = metrics.register_rtp_worker();
        let source_policy_signal = SourcePolicySignal::default();
        let source_policy_updates = source_policy_signal.subscribe();
        let packet_loop_config = PacketLoopConfig {
            worker: RtcWorkerConfig {
                bitrate_limits: SessionBitrateLimits::new(
                    Bitrate::from_mbps(8),
                    Bitrate::from_mbps(10),
                ),
                video_bitrate_limits: VideoBitrateLimits::default(),
                profile: Arc::new(
                    RtpProfile::compile(MediaCodecFlags::default(), CodecPreferences::default())
                        .unwrap_or_else(|_error| {
                            panic!("meeting benchmark RTP profile should compile")
                        }),
                ),
                media_quality_interval: None,
                media_id_base: 0,
            },
            packet_sink_registry: Arc::new(RoomPacketSinkRegistry::default()),
            source_policy_signal: source_policy_signal.clone(),
            metrics: Arc::clone(&metrics),
            rtp_metrics: Arc::clone(&packet_metrics),
            rtc_metrics: Arc::clone(&route_metrics),
            packet_loop_delay: Arc::new(PacketLoopDelaySnapshot::new(Instant::now())),
        };
        // this fixture models one worker, so no peer worker ever enqueues relay
        // packets into the pump-phase mailbox; per-target consumption below
        // stands in for the peer side of cross-worker fanout
        let (_relay_tx, relay_rx) = mpsc::channel(RELAY_MAILBOX_CAPACITY);
        let mut fixture = Self {
            state: PacketLoopState::default(),
            snapshot_state: Arc::new(Mutex::new(RtcSnapshotState::default())),
            metrics,
            rtc_metrics: route_metrics,
            source_policy_signal,
            source_policy_updates,
            turn: PacketLoopTurn::new(Instant::now()),
            packet_loop_config,
            relay_rx,
            bitrate_registry: Arc::new(Mutex::new(BitrateRegistry::default())),
            participants: Vec::with_capacity(MEETING_PARTICIPANTS),
            session_keys: Vec::with_capacity(MEETING_PARTICIPANTS),
            video_publishers: Vec::with_capacity(MEETING_VIDEO_PUBLISHERS),
            streams: Vec::new(),
            video_frames: build_video_layer_frames(),
            staged_streams: Vec::new(),
            audio_plan: vec![(None, None); MEETING_PARTICIPANTS],
            relay_receivers: Vec::with_capacity(RELAY_TARGETS),
            activity_revision: SourceActivityRevision::default(),
            admitted_audio: Vec::with_capacity(MEETING_ADMITTED_AUDIO_SOURCES),
            forwarding_audio_sources: 0,
            featured_rotation: 0,
            featured_layout: [NO_FEATURED_PUBLISHER; MEETING_PARTICIPANTS],
            tick_cursor: 0,
            ticks: ticks_for_seconds(seconds),
            now: Instant::now(),
            profile: MeetingWorkProfile::default(),
        };
        fixture.install_sessions();
        fixture.session_keys = fixture
            .participants
            .iter()
            .map(|participant| participant.session_key.clone())
            .collect();
        fixture.install_audio_routes();
        fixture.install_video_routes();
        // the sessions built above anchor their internal `str0m` deadlines to the
        // wall clock, so the synthetic clock has to start after topology setup
        // finished. starting it before would put every session's own start in the
        // synthetic future and let setup duration shift which tick a timeout lands
        // on
        //
        // the turn itself then runs on this same synthetic clock, which is what
        // keeps the measured counts off the wall clock. a turn that read
        // `Instant::now()` while its packets carried synthetic receive times would
        // resolve whichever deadlines the host happened to have crossed by then,
        // so a slower host measured strictly more work
        fixture.now = Instant::now();
        fixture.seed_audio_admission();
        fixture.install_relay_targets();
        fixture
    }

    /// bootstraps one RTC session per participant with its producer media
    fn install_sessions(&mut self) {
        for participant in 0..MEETING_PARTICIPANTS {
            let session_key = meeting_session_key(participant);
            let candidate_addr = SocketAddr::from((
                [127, 0, 0, 1],
                FIRST_CANDIDATE_PORT.saturating_add(u16::try_from(participant).unwrap_or(0)),
            ));
            bootstrap::ensure_session_rtc_state(
                &mut self.state.users,
                &session_key,
                candidate_addr,
                Bitrate::from_mbps(SESSION_MAX_BITRATE),
            )
            .expect("meeting benchmark session should enter RTC state");
            let audio_media = self.declare_producer(
                &session_key,
                audio_up_mid(participant),
                MediaKind::Audio,
                &[(audio_up_ssrc(participant), None)],
            );
            let audio_source = TransportSourceKey::new(session_key.clone(), audio_media);
            // packets a session publishes are staged against its worker-local handle,
            // the way local ingress stages them, so source resolution pays the same
            // slot lookup production pays per packet
            let session_handle = self
                .state
                .users
                .handle_for_key(&session_key)
                .expect("meeting benchmark session should have a worker-local handle");
            if participant < MEETING_VIDEO_PUBLISHERS {
                // every layer is declared, not just the featured one: keyframe
                // feedback for a RID with no producer stream is dropped before it
                // reaches the producer, so a thumbnail viewer's request would
                // never leave the room
                let layer_streams = VIDEO_LAYERS
                    .iter()
                    .enumerate()
                    .map(|(layer_idx, layer)| {
                        (
                            video_up_ssrc(participant, layer_idx),
                            Some(Rid::from(layer.rid)),
                        )
                    })
                    .collect::<Vec<_>>();
                let video_media = self.declare_producer(
                    &session_key,
                    video_up_mid(participant),
                    MediaKind::Video,
                    &layer_streams,
                );
                // without negotiated VP8 the source has no packet inspector, so
                // every camera packet skips descriptor parsing, keyframe detection
                // and the receiver-side VP8 rewrite
                self.state
                    .routes
                    .refresh_packet_inspector(video_media, &vp8_parameters());
                self.video_publishers.push(MeetingVideoPublisher {
                    source: TransportSourceKey::new(session_key.clone(), video_media),
                    destinations: Vec::with_capacity(MEETING_PARTICIPANTS),
                });
                self.install_video_streams(participant, session_handle);
            }
            self.install_audio_stream(participant, session_handle);
            self.participants.push(MeetingParticipant {
                session_key,
                audio_source,
            });
        }
    }

    /// declares one producer media on a session and registers its counters
    fn declare_producer(
        &mut self,
        session_key: &TransportSessionKey,
        mid: Mid,
        kind: MediaKind,
        streams: &[(u32, Option<Rid>)],
    ) -> TransportMediaId {
        let session = self
            .state
            .users
            .get_mut(session_key)
            .expect("meeting benchmark session should exist");
        let mut direct_api = session.rtc.direct_api();
        direct_api.declare_media(mid, kind);
        for (ssrc, rid) in streams.iter().copied() {
            direct_api.expect_stream_rx(Ssrc::from(ssrc), None, mid, rid);
        }
        let src_media = self
            .state
            .register_media_handle(RegisteredMediaHandle::Producer {
                session_key: session_key.clone(),
                mid,
            });
        let counter = self
            .bitrate_registry
            .lock()
            .expect("meeting benchmark bitrate registry should lock")
            .register_incoming_media(session_key, src_media, self.now);
        self.state
            .register_incoming_bitrate_counter(src_media, counter);
        src_media
    }

    /// installs the reusable audio packet slot for one publisher
    fn install_audio_stream(&mut self, participant: usize, session_handle: SessionHandle) {
        let packet = sample_local_forwarded_packet_for_benchmark(
            session_handle,
            audio_up_mid(participant).as_ref(),
            None,
            BenchmarkStreamIdentity {
                ssrc: audio_up_ssrc(participant),
                payload_type: AUDIO_PAYLOAD_TYPE,
            },
            Arc::from([0_u8; AUDIO_PAYLOAD_BYTES].as_slice()),
        );
        self.streams.push(MeetingStream {
            packets: vec![Some(packet)],
            frames: None,
            kind: MeetingStreamKind::Audio,
            participant,
            frame_period: 1,
            frame_offset: 0,
            frame_cursor: 0,
            rtp_timestamp: 0,
            sequence_number: 0,
        });
    }

    /// installs the reusable simulcast packet slots for one video publisher
    ///
    /// each layer is its own stream with its own SSRC, so the packet loop sees
    /// the three streams a simulcast camera really publishes rather than one
    /// stream whose packets contradict each other
    fn install_video_streams(&mut self, participant: usize, session_handle: SessionHandle) {
        let Self {
            streams,
            video_frames,
            ..
        } = self;
        for (layer_idx, layer) in VIDEO_LAYERS.iter().enumerate() {
            let Some(frames) = video_frames.get(layer_idx) else {
                continue;
            };
            let identity = BenchmarkStreamIdentity {
                ssrc: video_up_ssrc(participant, layer_idx),
                payload_type: VIDEO_PAYLOAD_TYPE,
            };
            let packets = (0..layer.packets_per_frame)
                .map(|packet| {
                    Some(sample_local_forwarded_packet_for_benchmark(
                        session_handle,
                        video_up_mid(participant).as_ref(),
                        Some(layer.rid),
                        identity,
                        Arc::clone(frames.payload(0, packet)),
                    ))
                })
                .collect();
            streams.push(MeetingStream {
                packets,
                frames: Some(Arc::clone(frames)),
                kind: MeetingStreamKind::Video,
                participant,
                frame_period: layer.frame_period,
                frame_offset: participant % layer.frame_period,
                // publishers enter the ring at different frames, so their
                // keyframes land on different ticks
                frame_cursor: participant % VIDEO_FRAME_RING,
                rtp_timestamp: 0,
                sequence_number: 0,
            });
        }
    }

    /// subscribes every participant to every other participant's audio
    ///
    /// all routes exist for the whole run because the room admits or pauses
    /// sources rather than tearing consumer routes down for a speaking turn
    fn install_audio_routes(&mut self) {
        for publisher in 0..MEETING_PARTICIPANTS {
            let Some(source) = self
                .participants
                .get(publisher)
                .map(|entry| entry.audio_source.clone())
            else {
                continue;
            };
            for receiver in 0..MEETING_PARTICIPANTS {
                if receiver == publisher {
                    continue;
                }
                let _ = self.install_local_destination(
                    receiver,
                    source.transport_media_id(),
                    audio_down_mid(receiver, publisher),
                    MediaKind::Audio,
                    audio_down_ssrc(receiver, publisher),
                    PacketLayerGate::Open,
                );
            }
        }
    }

    /// gives every receiver one featured publisher and four thumbnails
    fn install_video_routes(&mut self) {
        for receiver in 0..MEETING_PARTICIPANTS {
            for publisher in video_publishers_for(receiver) {
                // the initial layout is the rotation's first step, so a switch moves
                // an already-consistent layout rather than repairing one
                let featured = featured_publisher_for(receiver, 0) == Some(publisher);
                let Some(source) = self
                    .video_publishers
                    .get(publisher)
                    .map(|entry| entry.source.clone())
                else {
                    continue;
                };
                let rid = if featured {
                    FEATURED_RID
                } else {
                    THUMBNAIL_RID
                };
                let (consumer_media, dst_idx) = self.install_local_destination(
                    receiver,
                    source.transport_media_id(),
                    video_down_mid(receiver, publisher),
                    MediaKind::Video,
                    video_down_ssrc(receiver, publisher),
                    PacketLayerGate::Rid(Rid::from(rid)),
                );
                let Some(entry) = self.video_publishers.get_mut(publisher) else {
                    continue;
                };
                entry.destinations.push(MeetingVideoDestination {
                    route: TransportConsumerRoute::new(
                        meeting_session_key(receiver),
                        consumer_media,
                        source,
                    ),
                    receiver,
                    dst_idx,
                    mid: video_down_mid(receiver, publisher),
                    featured,
                });
            }
        }
    }

    /// installs one writable local RTC destination for a receiver
    ///
    /// the destination declares real `str0m` media and an outbound stream so the
    /// flush phase performs an actual RTC write instead of a lookup miss
    ///
    /// returns the consumer media id and the destination's index in its source
    /// route, which is what lets the layout checks read a gate back out of the
    /// route table instead of trusting the fixture's own copy
    fn install_local_destination(
        &mut self,
        receiver: usize,
        src_media: TransportMediaId,
        mid: Mid,
        kind: MediaKind,
        ssrc: u32,
        packet_gate: PacketLayerGate,
    ) -> (TransportMediaId, usize) {
        let session_key = meeting_session_key(receiver);
        let session = self
            .state
            .users
            .get_mut(&session_key)
            .expect("meeting benchmark receiver session should exist");
        let dest_stream = session.consumer_streams.allocate(mid);
        let egress_bitrate = Arc::clone(&session.egress_bitrate);
        let mut direct_api = session.rtc.direct_api();
        direct_api.declare_media(mid, kind);
        direct_api.declare_stream_tx(Ssrc::from(ssrc), None, mid, None);
        self.bitrate_registry
            .lock()
            .expect("meeting benchmark bitrate registry should lock")
            .register_session_egress(&session_key, egress_bitrate);
        let consumer_media = self
            .state
            .register_media_handle(RegisteredMediaHandle::Consumer {
                session_key: session_key.clone(),
                mid,
                src_media,
            });
        // a video consumer negotiated the same VP8 payload as its camera source,
        // so the destination starts with the same route state production
        // `register_consumer_route` builds: the effective gate stays blocked
        // until the selected RID produces the keyframe that makes the layer
        // decodable. installing a plain RID gate here would measure layer
        // switches that forward immediately instead of waiting for the selected
        // RID keyframe
        let consumer_rtp = match kind {
            MediaKind::Video => Some(vp8_parameters()),
            MediaKind::Audio => None,
        };
        let dest_payload_type = consumer_rtp.as_ref().and_then(codec::primary_payload_type);
        let requires_decoder_refresh = consumer_rtp.as_ref().is_some_and(|parameters| {
            codec::requires_decoder_refresh(parameters, dest_payload_type)
        });
        let (packet_gate, pending_gate) =
            guarded_pkt_gate(requires_decoder_refresh, src_media, packet_gate);
        let dst_idx = self.state.routes.add_consumer_route(
            src_media,
            MediaRouteDestination {
                dest_session: session_key.clone(),
                dest_transport_media_id: consumer_media,
                dest_stream,
                dest_mid: mid,
                dest_payload_type,
                repair_enabled: false,
                active: true,
                requires_decoder_refresh,
                delivery_generation: 0,
                packet_gate,
                pending_gate,
            },
        );
        self.state.set_consumer_dst_idx(
            &session_key,
            mid,
            consumer_media,
            src_media,
            Some(dst_idx),
        );
        (consumer_media, dst_idx)
    }

    /// registers cross-worker relay fanout for the busiest sources
    fn install_relay_targets(&mut self) {
        let relay_sources = self
            .video_publishers
            .iter()
            .map(|publisher| publisher.source.transport_media_id())
            .chain(
                self.participants
                    .iter()
                    .take(MEETING_ADMITTED_AUDIO_SOURCES)
                    .map(|participant| participant.audio_source.transport_media_id()),
            )
            .collect::<Vec<_>>();
        for target in 0..RELAY_TARGETS {
            let (sender, receiver) = mpsc::channel(RELAY_MAILBOX_CAPACITY);
            let target_id = RelayTargetId::new(u64::try_from(target).unwrap_or(0));
            for src_media in relay_sources.iter().copied() {
                self.state.routes.add_relay_target(
                    src_media,
                    target_id,
                    RelayPacketMailbox::new(sender.clone()),
                );
                self.state
                    .routes
                    .set_relay_target_active(src_media, target_id, true);
            }
            self.relay_receivers.push(receiver);
        }
    }
}

/// the VP8 media parameters every camera in the room negotiated
///
/// packet inspection is built from these, so they are what decides whether the
/// room's video packets are inspected at all
fn vp8_parameters() -> RouterRtpParameters {
    RouterRtpParameters::new(
        vec![MediaFormat::new(
            RouterMediaKind::Video,
            CodecName::Vp8,
            PayloadType::new(VIDEO_PAYLOAD_TYPE),
            VIDEO_CLOCK_RATE,
        )],
        vec![],
        vec![],
    )
}

/// builds the payload ring of every simulcast layer once for the whole room
///
/// # Panics
///
/// Panics when a built frame does not inspect as the frame it was built to be,
/// which is what keeps a silent descriptor mistake from turning the ladder back
/// into payloads the codec path skips
fn build_video_layer_frames() -> Vec<Arc<VideoLayerFrames>> {
    let inspector = codec::PacketInspector::from_parameters(&vp8_parameters());
    VIDEO_LAYERS
        .iter()
        .map(|layer| {
            let frames = VideoLayerFrames::new(layer.payload_bytes);
            assert!(
                video_frames_are_valid(&frames, &inspector),
                "meeting benchmark VP8 frames for layer {} must inspect as VP8",
                layer.rid
            );
            Arc::new(frames)
        })
        .collect()
}

/// proves one layer's ring carries the codec state the scenario claims
///
/// the ring must open on a decodable keyframe, keep the rest as interframes,
/// leave continuation packets out of keyframe detection and advance the picture
/// id per frame. a ring that fails any of these measures a codec path the room
/// would never take
fn video_frames_are_valid(frames: &VideoLayerFrames, inspector: &codec::PacketInspector) -> bool {
    let inspect = |payload: &Arc<[u8]>| {
        inspector
            .inspect(Pt::from(VIDEO_PAYLOAD_TYPE), payload, true)
            .decoder_refresh()
    };
    let picture_ids = (0..VIDEO_FRAME_RING)
        .map(|frame| {
            Vp8Descriptor::parse(frames.payload(frame, 0))
                .ok()?
                .picture_id()
        })
        .collect::<Option<Vec<_>>>();
    let advancing_picture_ids = picture_ids.is_some_and(|picture_ids| {
        picture_ids
            == (0..VIDEO_FRAME_RING)
                .map(|frame| u16::try_from(frame).unwrap_or(0))
                .collect::<Vec<_>>()
    });
    advancing_picture_ids
        && inspect(frames.payload(0, 0))
        && !inspect(frames.payload(0, 1))
        && (1..VIDEO_FRAME_RING).all(|frame| !inspect(frames.payload(frame, 0)))
}

/// builds one VP8 packet payload of a ring frame
///
/// `frame` is the ring index, which is also the frame's picture id and
/// TL0PICIDX, and frame 0 is the ring's keyframe. only the packet that starts a
/// frame carries the start bit, so a frame's later packets are not counted as
/// decoder refreshes
fn vp8_payload(frame: usize, frame_start: bool, payload_bytes: usize) -> Arc<[u8]> {
    let picture_id = u16::try_from(frame).unwrap_or(0);
    let tl0_pic_idx = u8::try_from(frame).unwrap_or(0);
    let mut payload = vec![
        vp8::X_BIT | if frame_start { vp8::S_BIT } else { 0 },
        vp8::I_BIT | vp8::L_BIT | vp8::T_BIT,
        vp8::LONG_PICTURE_ID_BIT | u8::try_from(picture_id >> 8).unwrap_or(0),
        u8::try_from(picture_id & 0xff).unwrap_or(0),
        tl0_pic_idx,
        0,
    ];
    if !frame_start {
        // a continuation packet carries frame data rather than a frame header
        payload.resize(payload_bytes.max(payload.len()), 0);
        return Arc::from(payload);
    }
    if frame.is_multiple_of(VIDEO_FRAME_RING) {
        payload.extend_from_slice(&[0, 0, 0]);
        payload.extend_from_slice(&VP8_KEYFRAME_SYNC_CODE);
        payload.extend_from_slice(&VIDEO_FRAME_WIDTH.to_le_bytes());
        payload.extend_from_slice(&VIDEO_FRAME_HEIGHT.to_le_bytes());
    } else {
        payload.extend_from_slice(&[vp8::INTERFRAME_BIT, 0, 0]);
    }
    payload.resize(payload_bytes.max(payload.len()), 0);
    Arc::from(payload)
}

/// the bandwidth one receiver reports for one sample of the trace
///
/// offsetting the trace by participant keeps receivers from dipping together, so
/// bandwidth-driven policy work spreads across the run instead of arriving as one
/// synchronized cliff
fn receiver_bandwidth_bps(sample: usize, participant: usize) -> u64 {
    let trace_index = (sample + participant) % RECEIVER_BANDWIDTH_TRACE_BPS.len();
    RECEIVER_BANDWIDTH_TRACE_BPS
        .get(trace_index)
        .copied()
        .unwrap_or(DEFAULT_RECEIVER_BANDWIDTH_BPS)
}

fn ticks_for_seconds(seconds: u64) -> usize {
    usize::try_from(seconds.saturating_mul(1_000) / MEETING_TICK_MS).unwrap_or(0)
}

/// the video publishers one receiver subscribes to, featured first
///
/// a receiver never subscribes to its own camera, so the self entry is filtered
/// out before the subscriptions are counted. skipping it afterwards instead would
/// cost the six publishers their featured slot, so the room would carry 54 video
/// routes with 6 featured instead of 60 with 12, and the featured-layer switch
/// would measure half the gate batch a real room produces
fn video_publishers_for(receiver: usize) -> Vec<usize> {
    (0..MEETING_VIDEO_PUBLISHERS)
        .map(|offset| (receiver + offset) % MEETING_VIDEO_PUBLISHERS)
        .filter(|publisher| *publisher != receiver)
        .take(MEETING_VIDEO_SUBSCRIPTIONS)
        .collect()
}

/// the publisher one receiver features at a given point in the rotation
///
/// the slot walks along that receiver's own subscription list, so every receiver
/// holds exactly one featured tile no matter where the rotation is
fn featured_publisher_for(receiver: usize, rotation: usize) -> Option<usize> {
    let publishers = video_publishers_for(receiver);
    let slot = rotation.checked_rem(publishers.len())?;
    publishers.get(slot).copied()
}

/// the audio sources the room admits for one speaking turn
///
/// the offsets stay distinct modulo the participant count, so every turn admits
/// exactly the configured number of sources
fn admitted_audio_for_turn(turn: usize) -> Vec<usize> {
    [0, 4, 9]
        .into_iter()
        .take(MEETING_ADMITTED_AUDIO_SOURCES)
        .map(|offset| (turn + offset) % MEETING_PARTICIPANTS)
        .collect()
}

/// the audio metadata one participant's packet carries on a given tick
///
/// the primary floor holds speech for a whole turn, secondary admitted speakers
/// interject in bursts and everyone else sits at the noise floor
fn audio_activity_for(
    tick: usize,
    participant: usize,
    admitted: &[usize],
) -> (Option<bool>, Option<i8>) {
    if participant == EXTENSIONLESS_PARTICIPANT {
        return (None, None);
    }
    if participant >= AUDIO_LEVEL_ONLY_PARTICIPANTS {
        return (None, Some(audio_level_only_dbov(tick, participant)));
    }
    let Some(seat) = admitted
        .iter()
        .position(|admitted| *admitted == participant)
    else {
        return (Some(false), Some(NOISE_FLOOR_DBOV));
    };
    if seat == 0 {
        let turn = tick / AUDIO_FLOOR_TICKS;
        let ramp = i8::try_from(tick % 5).unwrap_or(0);
        let level = SPEECH_LEVELS_DBOV
            .get(turn % SPEECH_LEVELS_DBOV.len())
            .copied()
            .unwrap_or(SECONDARY_SPEECH_DBOV);
        return (Some(true), Some(level.saturating_add(ramp)));
    }
    if (tick / INTERJECTION_TICKS) % MEETING_ADMITTED_AUDIO_SOURCES == seat {
        return (Some(true), Some(SECONDARY_SPEECH_DBOV));
    }
    (Some(false), Some(NOISE_FLOOR_DBOV))
}

/// the level an audio-level-only client reports for one tick
///
/// the three clients sit below the noise floor, between the two thresholds, and
/// alternating across the speech threshold, so the promotion window fills and
/// drains instead of resting on one branch
fn audio_level_only_dbov(tick: usize, participant: usize) -> i8 {
    match participant % 3 {
        0 => BELOW_NOISE_FLOOR_DBOV,
        1 => BETWEEN_THRESHOLDS_DBOV,
        _ => {
            if (tick / INTERJECTION_TICKS).is_multiple_of(2) {
                ABOVE_SPEECH_THRESHOLD_DBOV
            } else {
                BELOW_NOISE_FLOOR_DBOV
            }
        }
    }
}

fn meeting_session_key(participant: usize) -> TransportSessionKey {
    let offset = u64::try_from(participant).unwrap_or(0);
    test_transport_session_key(
        ROOM_INSTANCE_ID,
        WORKER_IDX,
        FIRST_CONNECTION_ID.saturating_add(offset),
        UserId::Integer(FIRST_USER_ID.saturating_add(i64::try_from(participant).unwrap_or(0))),
    )
}

fn audio_up_mid(participant: usize) -> Mid {
    Mid::from(format!("au{participant}").as_str())
}

fn video_up_mid(participant: usize) -> Mid {
    Mid::from(format!("vu{participant}").as_str())
}

fn audio_down_mid(receiver: usize, publisher: usize) -> Mid {
    Mid::from(format!("ad{receiver}-{publisher}").as_str())
}

fn video_down_mid(receiver: usize, publisher: usize) -> Mid {
    Mid::from(format!("vd{receiver}-{publisher}").as_str())
}

fn audio_up_ssrc(participant: usize) -> u32 {
    10_000 + u32::try_from(participant).unwrap_or(0)
}

/// the SSRC one publisher's simulcast layer publishes under
///
/// layers are distinct streams, so they never share an SSRC: a shared one would
/// make the receiver-side projection read three interleaved layers as one
fn video_up_ssrc(participant: usize, layer: usize) -> u32 {
    let stream = participant
        .saturating_mul(VIDEO_LAYERS.len())
        .saturating_add(layer);
    20_000 + u32::try_from(stream).unwrap_or(0)
}

fn audio_down_ssrc(receiver: usize, publisher: usize) -> u32 {
    30_000 + u32::try_from(receiver.saturating_mul(MEETING_PARTICIPANTS) + publisher).unwrap_or(0)
}

fn video_down_ssrc(receiver: usize, publisher: usize) -> u32 {
    40_000 + u32::try_from(receiver.saturating_mul(MEETING_PARTICIPANTS) + publisher).unwrap_or(0)
}

impl MeetingFlowBenchFixture {
    /// asserts the run reached every path the scenario exists to measure
    ///
    /// counters the fixture computed itself only prove intent, so the checks that
    /// matter read state back out of the packet loop: elected active speakers and
    /// the policy decisions the audio trace actually produced
    ///
    /// # Panics
    ///
    /// panics when the scenario stopped reaching one of those paths
    pub fn assert_packet_loop_coverage(&mut self) {
        let profile = self.profile;
        assert!(
            profile.observed_packets >= ticks_for_seconds(MEETING_SHORT_SECONDS),
            "every tick must observe at least one packet, got {}",
            profile.observed_packets
        );
        assert!(
            profile.planned_forwards > profile.observed_packets,
            "fanout must plan more destinations than observed packets, got {} for {}",
            profile.planned_forwards,
            profile.observed_packets
        );
        assert!(
            profile.relay_packets_consumed > 0,
            "cross-worker relay fanout never ran"
        );
        assert!(
            profile.egress_bitrate_samples > 0,
            "local RTC egress never reached a receiver session"
        );
        assert!(profile.policy_wakeups > 0, "source policy was never woken");
        assert!(
            profile.speech_observations > 0 && profile.silence_observations > 0,
            "audio policy must see both speech and silence, got {} speech and {} silence",
            profile.speech_observations,
            profile.silence_observations
        );
        assert!(
            profile.extensionless_observations > 0,
            "audio policy never saw a packet without the audio-level extension"
        );
        assert!(
            profile.audio_level_fallback_observations > 0,
            "no client reported an audio level without VAD, so observe_audio_level never ran"
        );
        // the counters above only prove the trace intended to carry speech and silence
        // this proves the packet loop acted on it: a trace whose packets all claim
        // voice activity elects every publisher at once, and one the policy ignores
        // entirely elects nobody
        assert!(
            profile.max_active_speakers > 0 && profile.max_active_speakers < MEETING_PARTICIPANTS,
            "audio policy elected {} of {MEETING_PARTICIPANTS} publishers as simultaneous active speakers, which is not a conversation",
            profile.max_active_speakers
        );
        // the decisions the policy actually reached, read outside the measured window
        // a hardcoded audio trace collapses these to one or two values
        let reasons = self.observed_activity_reasons();
        for expected in [
            ActiveSpeakerActivityReason::Vad,
            ActiveSpeakerActivityReason::VadFalse,
            ActiveSpeakerActivityReason::LowNoise,
            ActiveSpeakerActivityReason::BelowSpeechThreshold,
            ActiveSpeakerActivityReason::AudioLevel,
        ] {
            assert!(
                reasons.contains(&expected),
                "audio policy never reached {expected:?}, only {reasons:?}"
            );
        }
        // both audio-floor checks read the route table rather than the fixture's own
        // admission bookkeeping. counting the producer-activity updates the fixture
        // sent proves only that it tried: the batch result is dropped, and a stale
        // revision is a successful no-op, so those counters grow even when no source
        // ever changed state
        assert!(
            profile.observed_audio_floor_moves > 0,
            "the set of audio sources the route table forwards never changed, so the audio floor never moved"
        );
        assert_eq!(
            profile.max_active_audio_sources, MEETING_ADMITTED_AUDIO_SOURCES,
            "the route table forwarded {} audio sources at once, but the room admits {MEETING_ADMITTED_AUDIO_SOURCES}",
            profile.max_active_audio_sources
        );
        // the same rule as the audio floor: the gate batches the fixture sent only
        // prove intent, so both featured-layout checks read the route table
        assert!(
            profile.observed_featured_switches > 0,
            "the featured video gates the route table holds never changed, so the featured layer never switched"
        );
        assert_eq!(
            profile.featured_layout_violations, 0,
            "every receiver must hold exactly one featured tile at all times, but {} samples disagreed",
            profile.featured_layout_violations
        );
        assert!(profile.keyframe_flushes > 0, "keyframe feedback never ran");
        self.assert_video_publishers_carry_codec_state();
        assert_eq!(
            self.receiver_bandwidth_estimates(),
            MEETING_PARTICIPANTS,
            "every receiver must have a recorded bandwidth estimate"
        );
    }

    /// asserts the camera state the video half of the scenario depends on
    ///
    /// the counters above cannot see any of these: a room whose cameras declare
    /// one layer and negotiate no codec still forwards packets and still switches
    /// gates, it just measures a keyframe path that drops four of every five
    /// requests and a codec path that inspects nothing
    fn assert_video_publishers_carry_codec_state(&mut self) {
        let source_ids = self
            .video_publishers
            .iter()
            .map(|publisher| publisher.source.transport_media_id())
            .collect::<Vec<_>>();
        // the run itself has to have detected keyframes on every layer, which is
        // what proves the packets carried VP8 the source could inspect rather
        // than opaque bytes that parse as nothing
        for activity in self.state.routes.source_activity_snapshot(
            &source_ids,
            self.now,
            &self.state.incoming_bitrate_counters,
        ) {
            for layer in VIDEO_LAYERS {
                let observed = activity
                    .rids()
                    .iter()
                    .find(|rid_activity| rid_activity.rid() == layer.rid);
                assert!(
                    observed.is_some_and(|rid_activity| rid_activity.last_keyframe_age().is_some()),
                    "no keyframe was ever detected on layer {}, so camera packets were never inspected as VP8",
                    layer.rid
                );
            }
        }
        for publisher in &self.video_publishers {
            assert!(
                self.state
                    .routes
                    .decoder_refresh_is_observable(publisher.source.transport_media_id()),
                "video source {:?} negotiated no VP8, so its packets are never inspected",
                publisher.source
            );
        }
        for participant in 0..MEETING_VIDEO_PUBLISHERS {
            let mid = video_up_mid(participant);
            let session = self
                .state
                .users
                .get_mut(&meeting_session_key(participant))
                .expect("meeting benchmark video publisher session should exist");
            let mut direct_api = session.rtc.direct_api();
            for layer in VIDEO_LAYERS {
                assert!(
                    direct_api
                        .stream_rx_by_mid(mid, Some(Rid::from(layer.rid)))
                        .is_some(),
                    "publisher {participant} has no producer stream for layer {}, so its consumers' keyframe feedback is dropped before it reaches the producer",
                    layer.rid
                );
            }
        }
    }
}
