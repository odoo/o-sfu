//! destination RTP identity projection for local egress
//!
//! each consumer route maps to one receiver-visible RTP line
//! publisher identity can change when source selection or simulcast switches
//!
//! ```text
//! publisher SSRC/seq/timestamp/codec -> [`ConsumerStreamStore::project_identity`]
//!                                    -> receiver seq/timestamp/codec
//! ```
//!
//! state is scoped to the destination session so every consumer can rewrite the
//! same source packet without copying payload bytes

use core::hint::cold_path;
#[cfg(test)]
use std::sync::Arc;
use std::{
    collections::HashMap,
    mem,
    time::{Duration, Instant},
};

use str0m::{
    Rtc,
    media::{Frequency, Mid},
    rtp::{SeqNo, Ssrc},
};

use super::super::{
    codec,
    state::slots::{ConsumerStreamHandle, ConsumerStreamSlot, SlotStore},
};

// These mirror pinned str0m `Config::send_buffer_video`,
// `DEFAULT_RTX_CACHE_DURATION` and `DEFAULT_RTX_RATIO_CAP`. Recheck them on
// str0m upgrades so cache policy cannot drift.
pub const RTX_CACHE_MAX_PACKETS: usize = 1_000;
pub const RTX_CACHE_LIFETIME: Duration = Duration::from_secs(3);
const RTX_RATIO_CAP: Option<f32> = Some(0.15);
const TIMESTAMP_HALF_CYCLE: u32 = 1 << 31;

/// receiver identity state for one consumer route
///
/// all publisher SSRCs feeding the route are projected into one browser-facing
/// sequence, timestamp and encoded payload identity
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ConsumerStream {
    mid: Mid,
    delivery_generation: u64,
    primary_ssrc: Option<Ssrc>,
    queued_primary_writes: usize,
    stale_primary_writes: usize,
    rtx_cache_deadline: Option<Instant>,
    clock: Option<SourceClock>,
    rtp: RtpProjection,
    codec: codec::Projection,
}

/// generation-checked table of live consumer rewrite streams
///
/// route setup allocates a [`ConsumerStreamHandle`]
/// route removal releases it
/// stale handles make [`Self::project_identity`] return `None`
#[derive(Default)]
pub struct ConsumerStreamStore {
    streams: SlotStore<ConsumerStream, ConsumerStreamSlot>,
    repair_streams_by_primary: HashMap<Ssrc, ConsumerStreamHandle>,
    retired_repair_streams: HashMap<Ssrc, RetiredRepairStream>,
    next_rtx_deadline: Option<Instant>,
    #[cfg(test)]
    /// Accepted projected sequence and payload at the `StreamTx::write_rtp` queue boundary.
    pub last_local_write: Option<(SeqNo, Arc<[u8]>)>,
}

struct RetiredRepairStream {
    mid: Mid,
    queued_primary_writes: usize,
}

impl ConsumerStreamStore {
    /// creates the stream handle stored on one local route destination
    pub fn allocate(&mut self, mid: Mid) -> ConsumerStreamHandle {
        self.streams.insert(ConsumerStream {
            mid,
            ..ConsumerStream::default()
        })
    }

    /// releases a route destination handle
    pub fn release(&mut self, handle: ConsumerStreamHandle) {
        let Some(stream) = self.streams.remove(handle) else {
            return;
        };
        if let Some(ssrc) = stream.primary_ssrc
            && self.repair_streams_by_primary.get(&ssrc) == Some(&handle)
        {
            self.repair_streams_by_primary.remove(&ssrc);
            if stream.queued_primary_writes > 0 {
                let retired =
                    self.retired_repair_streams
                        .entry(ssrc)
                        .or_insert(RetiredRepairStream {
                            mid: stream.mid,
                            queued_primary_writes: 0,
                        });
                retired.mid = stream.mid;
                retired.queued_primary_writes = retired
                    .queued_primary_writes
                    .saturating_add(stream.queued_primary_writes);
            }
        }
    }

    pub(super) fn queue_repairable_write(&mut self, handle: ConsumerStreamHandle, ssrc: Ssrc) {
        let Some(stream) = self.streams.get_mut(handle) else {
            return;
        };
        if stream.primary_ssrc == Some(ssrc) {
            stream.queued_primary_writes = stream.queued_primary_writes.saturating_add(1);
            return;
        }
        let previous_ssrc = stream.primary_ssrc.replace(ssrc);
        let mid = stream.mid;
        let previous_writes = mem::take(&mut stream.queued_primary_writes);
        stream.stale_primary_writes = 0;

        if let Some(previous_ssrc) = previous_ssrc
            && self.repair_streams_by_primary.get(&previous_ssrc) == Some(&handle)
        {
            self.repair_streams_by_primary.remove(&previous_ssrc);
            if previous_writes > 0 {
                let retired = self.retired_repair_streams.entry(previous_ssrc).or_insert(
                    RetiredRepairStream {
                        mid,
                        queued_primary_writes: 0,
                    },
                );
                retired.mid = mid;
                retired.queued_primary_writes = retired
                    .queued_primary_writes
                    .saturating_add(previous_writes);
            }
        }
        if let Some(retired) = self.retired_repair_streams.remove(&ssrc) {
            stream.queued_primary_writes = retired.queued_primary_writes;
            stream.stale_primary_writes = retired.queued_primary_writes;
        }
        stream.queued_primary_writes = stream.queued_primary_writes.saturating_add(1);
        self.repair_streams_by_primary.insert(ssrc, handle);
    }

    pub fn note_repairable_transmit(&mut self, ssrc: Ssrc, now: Instant) -> Option<Ssrc> {
        if let Some(handle) = self.repair_streams_by_primary.get(&ssrc).copied() {
            let stream = self.streams.get_mut(handle)?;
            stream.queued_primary_writes = stream.queued_primary_writes.checked_sub(1)?;
            if stream.stale_primary_writes > 0 {
                stream.stale_primary_writes -= 1;
                stream.rtx_cache_deadline = None;
                return Some(ssrc);
            }
            let deadline = now + RTX_CACHE_LIFETIME;
            stream.rtx_cache_deadline = Some(deadline);
            self.next_rtx_deadline = Some(
                self.next_rtx_deadline
                    .map_or(deadline, |current| current.min(deadline)),
            );
            return None;
        }
        let retired = self.retired_repair_streams.get_mut(&ssrc)?;
        retired.queued_primary_writes = retired.queued_primary_writes.checked_sub(1)?;
        if retired.queued_primary_writes == 0 {
            self.retired_repair_streams.remove(&ssrc);
        }
        Some(ssrc)
    }

    pub fn invalidate_rtx_stream(&mut self, handle: ConsumerStreamHandle) -> Option<Ssrc> {
        let stream = self.streams.get_mut(handle)?;
        stream.rtx_cache_deadline = None;
        stream.stale_primary_writes = stream.queued_primary_writes;
        stream.primary_ssrc
    }

    pub fn purge_removed_rtx_streams(&mut self, rtc: &mut Rtc) {
        let mut api = rtc.direct_api();
        self.retired_repair_streams
            .retain(|ssrc, _| api.stream_tx(ssrc).is_some());
    }

    pub fn reset_rtx_streams(&mut self, mid: Mid) {
        for stream in self.streams.values_mut().filter(|stream| stream.mid == mid) {
            stream.queued_primary_writes = 0;
            stream.stale_primary_writes = 0;
            stream.rtx_cache_deadline = None;
        }
        self.retired_repair_streams
            .retain(|_, stream| stream.mid != mid);
    }

    pub fn expire_rtx_streams(&mut self, rtc: &mut Rtc, now: Instant) {
        if !matches!(self.next_rtx_deadline, Some(deadline) if deadline <= now) {
            return;
        }
        self.next_rtx_deadline = None;
        for stream in self.streams.values_mut() {
            let Some(deadline) = stream.rtx_cache_deadline else {
                continue;
            };
            if deadline <= now {
                if let Some(primary_ssrc) = stream.primary_ssrc {
                    rotate_rtx_cache(rtc, primary_ssrc);
                }
                stream.rtx_cache_deadline = None;
            } else {
                self.next_rtx_deadline = Some(
                    self.next_rtx_deadline
                        .map_or(deadline, |current| current.min(deadline)),
                );
            }
        }
    }

    #[cfg(test)]
    pub fn rtx_cache_is_armed(&self, handle: ConsumerStreamHandle) -> bool {
        self.streams
            .get(handle)
            .is_some_and(|stream| stream.rtx_cache_deadline.is_some())
    }

    #[cfg(test)]
    pub fn rtx_write_counts(&self, handle: ConsumerStreamHandle) -> Option<(usize, usize)> {
        self.streams
            .get(handle)
            .map(|stream| (stream.queued_primary_writes, stream.stale_primary_writes))
    }

    /// Projects source RTP and its clock pair into one receiver stream.
    ///
    /// # Projection rules
    ///
    /// Same-source packets preserve sequence and timestamp deltas. Gaps remain
    /// visible as loss. Source switches and delivery resumes use the next receiver
    /// sequence and advance timestamps by elapsed time at the source clock rate.
    /// A zero or sub-tick gap still advances one tick to separate switched frames.
    ///
    /// Example at 90 kHz with a 100 ms gap from the latest frame reference:
    ///
    /// ```text
    /// source packet                 receiver packet
    /// A: seq 10, ts 90000  ------->  seq 0, ts  90000
    /// A: seq 11, ts 93000  ------->  seq 1, ts  93000  (+3000 source ticks)
    ///                |
    ///                | switch after 100 ms: 90000 * 0.1 = 9000 ticks
    ///                v
    /// B: seq  1, ts  5000  ------->  seq 2, ts 102000  (+9000 elapsed ticks)
    /// B: seq  2, ts  8000  ------->  seq 3, ts 105000  (+3000 source ticks)
    /// ```
    ///
    /// A newer primary timestamp refreshes the arrival-based clock reference.
    /// Equal timestamps reuse its time. Older packets reconstruct a historical
    /// time without moving the reference. An ahead repair's inferred time is
    /// capped at arrival to keep str0m's clock in the past. That cap can shift
    /// the report estimate. Arrival jitter remains.
    ///
    /// Reordering means receiver sequences are not monotonic in arrival order.
    /// Repairs require an earlier sequence in the active SSRC and delivery
    /// generation window. Admission does not prove the packet was lost.
    ///
    /// Returns `None` for stale handles or generations, repairs outside that
    /// window, ambiguous half-cycle timestamps and
    /// unrepresentable sequence or clock arithmetic.
    pub(super) fn project_identity(
        &mut self,
        stream_handle: ConsumerStreamHandle,
        source: SourceRtpIdentity,
        codec_identity: codec::PacketIdentity,
    ) -> Option<ProjectedIdentity> {
        self.streams
            .get_mut(stream_handle)?
            .project(source, codec_identity)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SourceRtpIdentity {
    pub delivery_generation: u64,
    pub ssrc: Ssrc,
    pub seq_no: SeqNo,
    pub timestamp: u32,
    pub arrived_at: Instant,
    pub clock_rate: Frequency,
    pub was_repair: bool,
}

/// Source timestamp paired with the first arrival of its frame.
#[derive(Debug, Clone, Copy)]
struct SourceClock {
    timestamp: u32,
    at: Instant,
}

struct ProjectedClock {
    at: Instant,
    timestamp_anchor: u32,
    advances: bool,
}

impl ConsumerStream {
    fn project(
        &mut self,
        source: SourceRtpIdentity,
        codec_identity: codec::PacketIdentity,
    ) -> Option<ProjectedIdentity> {
        let reanchor = source.delivery_generation != self.delivery_generation;
        if reanchor
            && (source.was_repair
                || source
                    .delivery_generation
                    .wrapping_sub(self.delivery_generation)
                    > u64::MAX / 2)
        {
            return None;
        }
        // Prepare fallible clock arithmetic before committing RTP or codec identity.
        let clock = self.project_clock(&source, reanchor)?;
        let projected = self.rtp.project(
            source.ssrc,
            source.seq_no,
            source.timestamp,
            reanchor,
            source.was_repair,
            clock.timestamp_anchor,
        )?;
        let codec = match projected.outcome {
            RtpProjectionOutcome::Observed => {
                let mut codec = self.codec;
                codec.project(codec_identity, false)
            }
            RtpProjectionOutcome::Advanced if !reanchor => {
                self.codec.project(codec_identity, false)
            }
            RtpProjectionOutcome::Advanced | RtpProjectionOutcome::Switched { .. } => {
                self.codec.project(codec_identity, true)
            }
        };
        if clock.advances {
            self.clock = Some(SourceClock {
                timestamp: source.timestamp,
                at: clock.at,
            });
        }
        self.delivery_generation = source.delivery_generation;
        Some(ProjectedIdentity {
            seq_no: projected.seq_no,
            rtp_timestamp: projected.rtp_timestamp,
            wallclock: clock.at,
            codec,
            transition: match projected.outcome {
                RtpProjectionOutcome::Switched { previous_ssrc } => {
                    SourceTransition::Switched { previous_ssrc }
                }
                RtpProjectionOutcome::Observed | RtpProjectionOutcome::Advanced => {
                    SourceTransition::Unchanged
                }
            },
            resets_rtx_cache: reanchor,
        })
    }

    /// str0m replaces its report clock on every write. Reuse the frame reference
    /// or estimate a delayed packet's sampling time from it.
    /// New primary timestamps use arrival time, which retains network jitter
    /// and does not establish capture-time synchronization (RFC 3550 section 5.1).
    // Keep both inputs borrowed to avoid stack copies in the inlined packet path.
    fn project_clock(&self, source: &SourceRtpIdentity, reanchor: bool) -> Option<ProjectedClock> {
        let Some(clock) = self.clock.as_ref() else {
            return Some(ProjectedClock {
                at: source.arrived_at,
                timestamp_anchor: source.timestamp,
                advances: true,
            });
        };
        let RtpTimeline::Active {
            ssrc,
            src_timestamp_anchor,
            dst_timestamp_anchor,
            ..
        } = &self.rtp.timeline
        else {
            return None;
        };
        if *ssrc == source.ssrc && !reanchor {
            let delta = source.timestamp.wrapping_sub(clock.timestamp);
            let (at, advances) = if delta == 0 {
                (clock.at, false)
            } else if delta >= TIMESTAMP_HALF_CYCLE || source.was_repair {
                cold_path();
                // Timestamp order is defined within half an RTP clock cycle.
                if delta == TIMESTAMP_HALF_CYCLE {
                    return None;
                }
                let ahead = delta < TIMESTAMP_HALF_CYCLE;
                let ticks = if ahead {
                    delta
                } else {
                    clock.timestamp.wrapping_sub(source.timestamp)
                };
                // str0m extrapolates with whole microseconds. Round the offset up.
                let micros =
                    (u64::from(ticks) * 1_000_000).div_ceil(u64::from(source.clock_rate.get()));
                let offset = Duration::from_micros(micros);
                let at = if ahead {
                    // Decode order can put a repair ahead of the primary timestamp.
                    // str0m requires a past clock, so bound the estimate by arrival.
                    clock.at.checked_add(offset)?.min(source.arrived_at)
                } else {
                    clock.at.checked_sub(offset)?
                };
                (at, false)
            } else {
                (source.arrived_at, true)
            };
            return Some(ProjectedClock {
                at,
                timestamp_anchor: source.timestamp,
                advances,
            });
        }
        cold_path();
        let elapsed = source.arrived_at.saturating_duration_since(clock.at);
        let rate = u64::from(source.clock_rate.get());
        let ticks = elapsed
            .as_secs()
            .checked_mul(rate)?
            .checked_add(u64::from(elapsed.subsec_nanos()) * rate / 1_000_000_000)?;
        // A distinct switched frame still needs a tick at zero or sub-tick spacing.
        let advance = u32::try_from(ticks & u64::from(u32::MAX)).ok()?.max(1);
        let previous =
            dst_timestamp_anchor.wrapping_add(clock.timestamp.wrapping_sub(*src_timestamp_anchor));
        Some(ProjectedClock {
            at: source.arrived_at,
            timestamp_anchor: previous.wrapping_add(advance),
            advances: true,
        })
    }
}

/// Receiver RTP mapping for the active publisher source.
///
/// O-SFU preserves source gaps so receiver loss reports identify the same
/// missing source sequence numbers. RFC 4588 section 3 calls this sequence
/// number preservation. str0m restores the section 4 OSN before projection.
/// Resetting the source anchor compacts only packets deliberately filtered by
/// O-SFU.
///
/// References:
/// - <https://www.rfc-editor.org/rfc/rfc3550.html#section-5.1>
/// - <https://www.rfc-editor.org/rfc/rfc4588.html#section-3>
/// - <https://www.rfc-editor.org/rfc/rfc4588.html#section-4>
#[derive(Debug, Clone, Copy, Default)]
struct RtpProjection {
    next_seq_no: SeqNo,
    timeline: RtpTimeline,
}

impl RtpProjection {
    #[cfg(test)]
    const fn new(next_seq_no: SeqNo) -> Self {
        Self {
            next_seq_no,
            timeline: RtpTimeline::Empty,
        }
    }

    #[cfg(test)]
    fn next_source_seq(&self, source_ssrc: Ssrc) -> Option<SeqNo> {
        match self.timeline {
            RtpTimeline::Active {
                ssrc,
                highest_src_seq,
                ..
            } if ssrc == source_ssrc => Some((*highest_src_seq + 1).into()),
            RtpTimeline::Active { .. } => Some(SeqNo::default()),
            RtpTimeline::Empty => None,
        }
    }

    /// Projects one packet without changing the RTP mapping on rejection.
    ///
    /// Returns `None` for repairs outside the active SSRC sequence window,
    /// source deltas outside the representable receiver sequence range or
    /// advancement whose receiver successor would exceed `u64::MAX`.
    /// Reordered packets and accepted repairs remain projectable after exhaustion.
    /// The caller validates the delivery generation and prepares the timestamp
    /// anchor before requesting reanchoring.
    // Outlining this transition adds a call frame to every primary packet.
    #[inline]
    fn project(
        &mut self,
        source_ssrc: Ssrc,
        source_seq_no: SeqNo,
        source_timestamp: u32,
        reanchor: bool,
        was_repair: bool,
        timestamp_anchor: u32,
    ) -> Option<ProjectedRtp> {
        if was_repair && !self.accepts_repair(source_ssrc, source_seq_no) {
            return None;
        }
        match &mut self.timeline {
            RtpTimeline::Empty => {
                cold_path();
                let seq_no = self.next_seq_no;
                self.next_seq_no = (*seq_no).checked_add(1)?.into();
                self.timeline = RtpTimeline::Active {
                    ssrc: source_ssrc,
                    src_seq_anchor: source_seq_no,
                    dst_seq_anchor: seq_no,
                    highest_src_seq: source_seq_no,
                    src_timestamp_anchor: source_timestamp,
                    dst_timestamp_anchor: source_timestamp,
                };
                Some(ProjectedRtp {
                    seq_no,
                    rtp_timestamp: source_timestamp,
                    outcome: RtpProjectionOutcome::Advanced,
                })
            }
            RtpTimeline::Active {
                ssrc,
                highest_src_seq,
                src_timestamp_anchor,
                dst_timestamp_anchor,
                ..
            } => {
                if *ssrc == source_ssrc && !reanchor {
                    // The checked predecessor rejects extended-sequence wrap and lets
                    // x86 compare the high-water mark in memory without a separate load.
                    if source_seq_no.checked_sub(1) != Some(**highest_src_seq) {
                        cold_path();
                        return self.project_source_delta(source_seq_no, source_timestamp);
                    }
                    // A source gap can exhaust the successor before the next consecutive packet.
                    let seq_no = self.next_seq_no;
                    self.next_seq_no = (*seq_no).checked_add(1)?.into();
                    *highest_src_seq = source_seq_no;
                    let rtp_timestamp = dst_timestamp_anchor
                        .wrapping_add(source_timestamp.wrapping_sub(*src_timestamp_anchor));
                    return Some(ProjectedRtp {
                        seq_no,
                        rtp_timestamp,
                        outcome: RtpProjectionOutcome::Advanced,
                    });
                }
                let previous_ssrc = *ssrc;
                let seq_no = self.next_seq_no;
                self.next_seq_no = (*seq_no).checked_add(1)?.into();
                let rtp_timestamp = timestamp_anchor;
                self.timeline = RtpTimeline::Active {
                    ssrc: source_ssrc,
                    src_seq_anchor: source_seq_no,
                    dst_seq_anchor: seq_no,
                    highest_src_seq: source_seq_no,
                    src_timestamp_anchor: source_timestamp,
                    dst_timestamp_anchor: rtp_timestamp,
                };
                let outcome = if previous_ssrc == source_ssrc {
                    RtpProjectionOutcome::Advanced
                } else {
                    RtpProjectionOutcome::Switched { previous_ssrc }
                };
                Some(ProjectedRtp {
                    seq_no,
                    rtp_timestamp,
                    outcome,
                })
            }
        }
    }

    fn accepts_repair(&self, source_ssrc: Ssrc, source_seq_no: SeqNo) -> bool {
        // str0m restores the RFC 4588 OSN before this boundary. O-SFU admits an
        // earlier sequence in the active SSRC projection window without tracking
        // whether that primary packet was delivered.
        // https://www.rfc-editor.org/rfc/rfc4588.html#section-4
        matches!(
            self.timeline,
            RtpTimeline::Active {
                ssrc,
                src_seq_anchor,
                highest_src_seq,
                ..
            } if ssrc == source_ssrc
                && source_seq_no >= src_seq_anchor
                && source_seq_no < highest_src_seq
        )
    }

    #[expect(
        clippy::inline_always,
        reason = "outlining the delta path adds a call frame to every packet projection"
    )]
    #[inline(always)]
    fn project_source_delta(
        &mut self,
        source_seq_no: SeqNo,
        source_timestamp: u32,
    ) -> Option<ProjectedRtp> {
        let RtpTimeline::Active {
            src_seq_anchor,
            dst_seq_anchor,
            highest_src_seq,
            src_timestamp_anchor,
            dst_timestamp_anchor,
            ..
        } = &mut self.timeline
        else {
            return None;
        };
        let source_delta = source_seq_no.checked_sub(**src_seq_anchor)?;
        let seq_no: SeqNo = (**dst_seq_anchor).checked_add(source_delta)?.into();
        let rtp_timestamp =
            dst_timestamp_anchor.wrapping_add(source_timestamp.wrapping_sub(*src_timestamp_anchor));
        let outcome = if source_seq_no > *highest_src_seq {
            // Reject an unrepresentable successor before committing sequence state.
            let next_seq_no = (*seq_no).checked_add(1)?.into();
            *highest_src_seq = source_seq_no;
            self.next_seq_no = next_seq_no;
            RtpProjectionOutcome::Advanced
        } else {
            RtpProjectionOutcome::Observed
        };
        Some(ProjectedRtp {
            seq_no,
            rtp_timestamp,
            outcome,
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct ProjectedRtp {
    seq_no: SeqNo,
    rtp_timestamp: u32,
    outcome: RtpProjectionOutcome,
}

/// Accepted changes to receiver RTP identity.
///
/// A source switch always advances the receiver sequence high-water mark.
#[derive(Debug, Clone, Copy)]
enum RtpProjectionOutcome {
    /// Receiver sequence high-water mark advanced without changing publisher SSRC.
    Advanced,
    /// Packet identity was projected without advancing the receiver sequence high-water mark.
    Observed,
    /// Publisher SSRC changed and the receiver sequence high-water mark advanced.
    Switched { previous_ssrc: Ssrc },
}

/// source and receiver anchors for one projected RTP line
#[derive(Debug, Clone, Copy, Default)]
enum RtpTimeline {
    #[default]
    Empty,
    Active {
        ssrc: Ssrc,
        src_seq_anchor: SeqNo,
        dst_seq_anchor: SeqNo,
        highest_src_seq: SeqNo,
        src_timestamp_anchor: u32,
        dst_timestamp_anchor: u32,
    },
}

/// receiver-facing RTP identity for one successful local write
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectedIdentity {
    /// receiver-local RTP sequence number
    pub seq_no: SeqNo,
    /// receiver-local RTP timestamp after SSRC switch smoothing
    pub rtp_timestamp: u32,
    /// Time paired with this timestamp for str0m sender reports.
    pub wallclock: Instant,
    /// projected encoded payload identity used by the codec rewrite boundary
    pub codec: codec::ProjectedPacket,
    /// source switch observed by local forwarding
    pub transition: SourceTransition,
    pub resets_rtx_cache: bool,
}

/// source identity transition observed during projection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceTransition {
    /// packet continued the current projected source
    Unchanged,
    /// packet switched from the previous publisher SSRC
    Switched {
        /// publisher SSRC used by the previous projected packet
        previous_ssrc: Ssrc,
    },
}

pub fn rotate_rtx_cache(rtc: &mut Rtc, primary_ssrc: Ssrc) {
    let mut api = rtc.direct_api();
    // Primary-SSRC lookup avoids a MID scan for every cache expired in this drain.
    let Some(stream) = api.stream_tx(&primary_ssrc) else {
        return;
    };
    if stream.rtx().is_some() {
        stream.set_rtx_cache(RTX_CACHE_MAX_PACKETS, RTX_CACHE_LIFETIME, RTX_RATIO_CAP);
    }
}

#[cfg(kani)]
#[path = "PROOFS/streams.rs"]
mod proofs;

#[cfg(test)]
#[path = "TESTS/streams.rs"]
mod tests;

#[cfg(feature = "internal-benchmarks")]
#[path = "TESTS/benchmark_support.rs"]
mod benchmark_support;
#[cfg(feature = "internal-benchmarks")]
pub use self::benchmark_support::LocalRewriteBenchFixture;
