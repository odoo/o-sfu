//! str0m's Sans-I/O drain boundary for ready RTC sessions.
//!
//! A successful ready-session drain polls [`str0m::Rtc`] until
//! [`Output::Timeout`]. That timeout proves queued output is exhausted and
//! supplies the next host-driven deadline.
//!
//! [`PacketLoopState`] merges dirty marks with due deadlines so the worker does
//! not scan every session. Work that needs socket I/O or worker-wide route state
//! stays in [`PacketLoopBuffers`] until the mutable session borrow ends.

use std::{
    mem::take,
    sync::{Arc, Mutex},
    time::Instant,
};

use str0m::{Event, Input, Output};
use tracing::{trace, warn};

use super::{
    super::{
        control::{SessionCloseDisposition, worker_close_session},
        packet_loop::event_observation::{RtcEventContext, log_rtc_event, observe_rtc_event},
        recovery::PendingKeyframeRequest,
        state::{
            PacketLoopState, RtcSessionState, RtcSnapshotState, bitrate::BitrateRegistry,
            slots::SessionHandle,
        },
    },
    buffers::PacketLoopBuffers,
};
use crate::engine::{
    media_transport::{SourcePolicySignal, TransportSessionKey},
    metrics::{RtcMetricsRecorder, RtcOutputBudgetLimit, RuntimeMetrics},
};

// The limits admit hundreds of MTU-sized fragments from one large video
// keyframe while bounding one authenticated peer's work before the next input.
const SESSION_DRAIN_MAX_TRANSMITS: usize = 512;
const SESSION_DRAIN_MAX_PAYLOAD_BYTES: usize = 384 * 1024;

#[derive(Clone, Copy)]
struct SessionOutputLimits {
    transmits: usize,
    payload_bytes: usize,
}

const SESSION_OUTPUT_LIMITS: SessionOutputLimits = SessionOutputLimits {
    transmits: SESSION_DRAIN_MAX_TRANSMITS,
    payload_bytes: SESSION_DRAIN_MAX_PAYLOAD_BYTES,
};

struct SessionOutputBudget {
    remaining_transmits: usize,
    remaining_payload_bytes: usize,
}

impl SessionOutputBudget {
    const fn new(limits: SessionOutputLimits) -> Self {
        Self {
            remaining_transmits: limits.transmits,
            remaining_payload_bytes: limits.payload_bytes,
        }
    }

    fn try_charge(&mut self, payload_bytes: usize) -> Result<(), RtcOutputBudgetLimit> {
        let packets_exhausted = self.remaining_transmits == 0;
        let payload_bytes_exhausted = payload_bytes > self.remaining_payload_bytes;
        match (packets_exhausted, payload_bytes_exhausted) {
            (true, true) => Err(RtcOutputBudgetLimit::PacketsAndPayloadBytes),
            (true, false) => Err(RtcOutputBudgetLimit::Packets),
            (false, true) => Err(RtcOutputBudgetLimit::PayloadBytes),
            (false, false) => {
                self.remaining_transmits -= 1;
                self.remaining_payload_bytes -= payload_bytes;
                Ok(())
            }
        }
    }
}

enum SessionDrainOutcome {
    Drained(Option<Instant>),
    Exhausted(TransportSessionKey, RtcOutputBudgetLimit),
}

/// Worker services needed to observe output or tear down an exhausted session.
pub struct SessionDrainContext<'a> {
    snapshot_state: &'a Arc<Mutex<RtcSnapshotState>>,
    bitrate_registry: &'a Arc<Mutex<BitrateRegistry>>,
    metrics: &'a RuntimeMetrics,
    rtc_metrics: &'a RtcMetricsRecorder,
    source_policy_signal: &'a SourcePolicySignal,
    output_limits: SessionOutputLimits,
}

impl<'a> SessionDrainContext<'a> {
    #[must_use]
    pub const fn new(
        snapshot_state: &'a Arc<Mutex<RtcSnapshotState>>,
        bitrate_registry: &'a Arc<Mutex<BitrateRegistry>>,
        metrics: &'a RuntimeMetrics,
        rtc_metrics: &'a RtcMetricsRecorder,
        source_policy_signal: &'a SourcePolicySignal,
    ) -> Self {
        Self {
            snapshot_state,
            bitrate_registry,
            metrics,
            rtc_metrics,
            source_policy_signal,
            output_limits: SESSION_OUTPUT_LIMITS,
        }
    }
}

/// Drains each session selected by a dirty mark or due deadline once.
#[must_use]
pub fn drain_ready_sessions(
    state: &mut PacketLoopState,
    context: &SessionDrainContext<'_>,
    buffers: &mut PacketLoopBuffers,
    now: Instant,
) -> bool {
    // Resolve every due deadline against one turn clock. Resampling per session
    // would make iteration order and host speed change this turn's work.
    state.collect_ready_sessions(now, &mut buffers.ready_sessions);
    let mut ready_sessions = take(&mut buffers.ready_sessions);
    let mut topology_changed = false;
    for session_handle in ready_sessions.drain(..) {
        let checkpoint = buffers.checkpoint_session_drain();
        let outcome = {
            // The handle carries the slot generation, so a stale ready entry
            // cannot poll a later occupant.
            let Some((session_key, session_state)) =
                state.users.get_key_value_mut_by_handle(session_handle)
            else {
                continue;
            };
            // Keep a successful drain indivisible and on the turn's fixed `now`.
            // Returning before a future `Output::Timeout` would let the next
            // packet-loop mutation overtake queued str0m output.
            drain_single_session(
                session_handle,
                session_key,
                session_state,
                context,
                buffers,
                now,
            )
        };
        match outcome {
            SessionDrainOutcome::Drained(session_timeout) => {
                state.update_session_timeout_by_handle(session_handle, session_timeout);
            }
            SessionDrainOutcome::Exhausted(session_key, limit) => {
                // Budget failure aborts one session drain as a unit. No packet,
                // feedback or datagram staged by the offender may survive.
                buffers.rollback_session_drain(&checkpoint);
                context
                    .rtc_metrics
                    .record_rtc_output_budget_exhaustion(limit);
                worker_close_session(
                    state,
                    context.bitrate_registry,
                    context.snapshot_state,
                    &session_key,
                    SessionCloseDisposition::OutputBudgetExhausted,
                    context.metrics,
                );
                context.rtc_metrics.record_rtc_output_budget_session_close();
                topology_changed = true;
            }
        }
    }
    buffers.ready_sessions = ready_sessions;
    topology_changed
}

/// Drains one [`str0m::Rtc`] and stages its output.
///
/// Returns the next deadline or the exhausted session identity and limit.
fn drain_single_session(
    session_handle: SessionHandle,
    session_key: &TransportSessionKey,
    session_state: &mut RtcSessionState,
    context: &SessionDrainContext<'_>,
    buffers: &mut PacketLoopBuffers,
    now: Instant,
) -> SessionDrainOutcome {
    let defer_rtx_expiry = begin_rtx_cache_expiry(session_state, now);
    let mut output_budget = SessionOutputBudget::new(context.output_limits);
    loop {
        // The output budget bounds emitted datagrams. str0m may scan a
        // potentially long resend deque before yielding one. Accept that
        // upstream constraint until runtime evidence shows a material stall.
        match session_state.rtc.poll_output() {
            Ok(Output::Transmit(transmit)) => {
                if let Err(limit) = output_budget.try_charge(transmit.contents.len()) {
                    session_state.clear_ingress_context();
                    return SessionDrainOutcome::Exhausted(session_key.clone(), limit);
                }
                session_state.note_repairable_transmit(&transmit.contents, now);
                buffers.push_pending_transmit(
                    transmit.destination,
                    Vec::<u8>::from(transmit.contents),
                );
            }
            Ok(Output::Event(Event::RtpPacket(packet))) => {
                let was_repair = session_state.take_rtp_repair(packet.header.ssrc);
                if was_repair {
                    context
                        .rtc_metrics
                        .record_rtc_rtx_received_from_publisher(packet.payload.len());
                }
                buffers.pending_packets.push(
                    super::super::packet_loop::forwarded_packet::ForwardedPacket::from_rtp_packet(
                        session_handle,
                        packet,
                        was_repair,
                    ),
                );
            }
            Ok(Output::Event(Event::KeyframeRequest(request))) => {
                // Consumer MID/RID names the receiving leg. Preserve `session_key`
                // so route state can resolve its current producer after this borrow.
                buffers
                    .pending_keyframe_requests
                    .push((session_key.clone(), PendingKeyframeRequest::new(request)));
                trace!(
                    user_id = ?session_key.user_id(),
                    media_worker_id = session_key.media_worker_id().as_usize(),
                    mid = %request.mid,
                    rid = ?request.rid,
                    kind = ?request.kind,
                    "queued route-level keyframe request from rtc packet-loop event"
                );
            }
            Ok(Output::Event(event)) => {
                observe_rtc_event(RtcEventContext {
                    snapshot_state: context.snapshot_state,
                    metrics: context.metrics,
                    rtc_metrics: context.rtc_metrics,
                    nack_totals: &mut session_state.nack_totals,
                    source_policy_signal: context.source_policy_signal,
                    room_id: session_state.room_id.as_ref(),
                    session_key,
                    event: &event,
                });
                log_rtc_event(session_key, &event);
            }
            Ok(Output::Timeout(timeout_at)) => {
                // A future timeout can leave paced resend references inside
                // str0m. Rotating now deliberately lets them miss after the
                // original packet's finite buffering time.
                // https://www.rfc-editor.org/rfc/rfc4588.html#section-3
                finish_rtx_cache_expiry(session_state, now, defer_rtx_expiry);
                session_state.clear_ingress_context();
                // `Output::Timeout` marks current output exhausted. Elapsed host
                // time alone does not advance str0m's clock, so feed an already-due
                // deadline back and drain again.
                if timeout_at <= now {
                    if session_state.rtc.handle_input(Input::Timeout(now)).is_err() {
                        warn!(
                            user_id = ?session_key.user_id(),
                            media_worker_id = session_key.media_worker_id().as_usize(),
                            "failed to apply immediate rtc packet-loop timeout input"
                        );
                        return SessionDrainOutcome::Drained(None);
                    }
                    continue;
                }
                return SessionDrainOutcome::Drained(Some(timeout_at));
            }
            Err(error) => {
                finish_rtx_cache_expiry(session_state, now, defer_rtx_expiry);
                session_state.clear_ingress_context();
                // A failed poll yields no replacement deadline. Returning `None`
                // removes the schedule rather than retrying an unclassified `RtcError`.
                warn!(
                    user_id = ?session_key.user_id(),
                    media_worker_id = session_key.media_worker_id().as_usize(),
                    ?error,
                    "rtc packet loop failed while polling output"
                );
                return SessionDrainOutcome::Drained(None);
            }
        }
    }
}

fn begin_rtx_cache_expiry(session_state: &mut RtcSessionState, now: Instant) -> bool {
    // str0m queues NACK resend metadata rather than packet bytes during input.
    // RTCP admission already checked cache age at receive time, so a newer
    // drain clock must not rotate before output resolves that metadata.
    let defer_rtx_expiry = take(&mut session_state.defer_rtx_expiry);
    if !defer_rtx_expiry {
        session_state.expire_rtx_streams(now);
    }
    defer_rtx_expiry
}

fn finish_rtx_cache_expiry(
    session_state: &mut RtcSessionState,
    now: Instant,
    defer_rtx_expiry: bool,
) {
    if defer_rtx_expiry {
        session_state.expire_rtx_streams(now);
    }
}

#[cfg(test)]
#[path = "TESTS/session_drain.rs"]
mod tests;
