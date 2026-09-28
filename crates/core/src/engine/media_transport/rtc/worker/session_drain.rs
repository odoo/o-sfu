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

use str0m::{Event, Input, Output, RtcError};
use tracing::{trace, warn};

use super::{
    super::{
        control::{SessionCloseDisposition, worker_close_session},
        packet_loop::{
            event_observation::{RtcEventContext, log_rtc_event, observe_rtc_event},
            forwarded_packet::ForwardedPacket,
        },
        recovery::PendingKeyframeRequest,
        state::{
            PacketLoopState, RtcSessionState, RtcSnapshotState, bitrate::BitrateRegistry,
            demux::RemoteAddrDemux, media_registry::ProducerStreamBinding, slots::SessionHandle,
        },
    },
    buffers::PacketLoopBuffers,
};
use crate::engine::{
    media_transport::{SourcePolicySignal, TransportSessionKey},
    metrics::{RtcDrainFailureStage, RtcMetricsRecorder, RtcOutputBudgetLimit, RuntimeMetrics},
};

// The limits admit hundreds of MTU-sized fragments from one large video
// keyframe while bounding one authenticated peer's work before the next input.
const SESSION_DRAIN_MAX_TRANSMITS: usize = 512;
const SESSION_DRAIN_MAX_IMMEDIATE_TIMEOUTS: usize = 8;
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
    Failed(TransportSessionKey, RtcDrainFailureStage, RtcError),
}

/// Worker services needed to observe output or tear down an exhausted session.
pub struct SessionDrainContext<'a> {
    snapshot_state: &'a Arc<Mutex<RtcSnapshotState>>,
    bitrate_registry: &'a Arc<Mutex<BitrateRegistry>>,
    metrics: &'a RuntimeMetrics,
    rtc_metrics: &'a RtcMetricsRecorder,
    source_policy_signal: &'a SourcePolicySignal,
    output_limits: SessionOutputLimits,
    #[cfg(test)]
    forced_failure: Option<(TransportSessionKey, RtcDrainFailureStage)>,
    #[cfg(test)]
    pub(in crate::engine::media_transport::rtc) force_immediate_timeout: bool,
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
            #[cfg(test)]
            forced_failure: None,
            #[cfg(test)]
            force_immediate_timeout: false,
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
                &mut state.remote_addr_demux,
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
                    SessionCloseDisposition::TerminalFailure,
                    context.metrics,
                );
                context.rtc_metrics.record_rtc_output_budget_session_close();
                topology_changed = true;
            }
            SessionDrainOutcome::Failed(session_key, failure_stage, error) => {
                // A failed str0m drain can leave partial output in this turn.
                buffers.rollback_session_drain(&checkpoint);
                context.rtc_metrics.record_rtc_drain_failure(failure_stage);
                warn!(
                    user_id = ?session_key.user_id(),
                    media_worker_id = session_key.media_worker_id().as_usize(),
                    ?failure_stage,
                    ?error,
                    "retiring RTC session after terminal drain failure"
                );
                worker_close_session(
                    state,
                    context.bitrate_registry,
                    context.snapshot_state,
                    &session_key,
                    SessionCloseDisposition::TerminalFailure,
                    context.metrics,
                );
                topology_changed = true;
            }
        }
    }
    buffers.ready_sessions = ready_sessions;
    topology_changed
}

/// Drains one [`str0m::Rtc`] and stages its output.
///
/// Returns a future or due deadline after full output exhaustion. A budget or RTC
/// failure returns the session identity so the caller can discard staged output
/// and retire that session.
#[expect(
    clippy::too_many_lines,
    reason = "Moving Event by value into a helper raised session_drain_128 Callgrind instructions by 3.1%"
)]
fn drain_single_session(
    session_handle: SessionHandle,
    session_key: &TransportSessionKey,
    session_state: &mut RtcSessionState,
    demux: &mut RemoteAddrDemux,
    context: &SessionDrainContext<'_>,
    buffers: &mut PacketLoopBuffers,
    now: Instant,
) -> SessionDrainOutcome {
    let defer_rtx_expiry = begin_rtx_cache_expiry(session_state, now);
    let mut output_budget = SessionOutputBudget::new(context.output_limits);
    let mut immediate_timeouts = 0;
    #[cfg(test)]
    let mut polled_transmit = false;
    loop {
        // The output budget bounds emitted datagrams. str0m may scan a
        // potentially long resend deque before yielding one. Accept that
        // upstream constraint until runtime evidence shows a material stall.
        #[cfg(test)]
        let output = tests::substitute_poll_result(
            session_state.rtc.poll_output(),
            context,
            session_key,
            polled_transmit,
        );
        #[cfg(not(test))]
        let output = session_state.rtc.poll_output();
        match output {
            Ok(Output::Transmit(transmit)) => {
                if let Err(limit) = output_budget.try_charge(transmit.contents.len()) {
                    session_state.clear_ingress_context();
                    return SessionDrainOutcome::Exhausted(session_key.clone(), limit);
                }
                // ICE STUN output can target any candidate. DTLS and media output
                // uses str0m's nominated send address.
                if transmit
                    .contents
                    .first()
                    .is_some_and(|first| matches!(first, 20..=63 | 128..=191))
                {
                    demux.remember_selected_remote_addr(session_key, transmit.destination);
                }
                session_state.note_repairable_transmit(&transmit.contents, now);
                buffers.pending_transmits.push(transmit);
                #[cfg(test)]
                {
                    polled_transmit = true;
                }
            }
            Ok(Output::Event(Event::RtpPacket(packet))) => {
                let (mid, binding) = {
                    let mut api = session_state.rtc.direct_api();
                    let Some(stream) = api.stream_rx(&packet.header.ssrc) else {
                        // Admission requires the receive stream's MID, RID and SSRC bindings.
                        continue;
                    };
                    let binding = ProducerStreamBinding {
                        rid: stream.rid(),
                        primary: stream.ssrc(),
                        repair: stream.rtx(),
                    };
                    (stream.mid(), binding)
                };
                let was_repair = session_state.take_rtp_repair(packet.header.ssrc);
                if was_repair {
                    context
                        .rtc_metrics
                        .record_rtc_rtx_received_from_publisher(packet.payload.len());
                }
                buffers
                    .pending_packets
                    .push(ForwardedPacket::from_rtp_packet(
                        session_handle,
                        packet,
                        was_repair,
                        mid,
                        binding,
                    ));
            }
            Ok(Output::Event(Event::KeyframeRequest(request))) => {
                // Consumer MID/RID names the receiving leg. Preserve session_key
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
                if let Some(outcome) = advance_output_timeout(
                    session_key,
                    session_state,
                    now,
                    timeout_at,
                    &mut immediate_timeouts,
                    defer_rtx_expiry,
                    #[cfg(test)]
                    context,
                ) {
                    return outcome;
                }
            }
            Err(error) => {
                finish_rtx_cache_expiry(session_state, now, defer_rtx_expiry);
                session_state.clear_ingress_context();
                return SessionDrainOutcome::Failed(
                    session_key.clone(),
                    RtcDrainFailureStage::PollOutput,
                    error,
                );
            }
        }
    }
}

fn advance_output_timeout(
    session_key: &TransportSessionKey,
    session_state: &mut RtcSessionState,
    now: Instant,
    timeout_at: Instant,
    immediate_timeouts: &mut usize,
    defer_rtx_expiry: bool,
    #[cfg(test)] context: &SessionDrainContext<'_>,
) -> Option<SessionDrainOutcome> {
    #[cfg(test)]
    let timeout_at = tests::forced_timeout_at(context, session_key, now, timeout_at);
    // A future timeout can leave paced resend references inside str0m.
    // Rotating now lets them miss after the original packet's finite buffering time.
    // https://www.rfc-editor.org/rfc/rfc4588.html#section-3
    finish_rtx_cache_expiry(session_state, now, defer_rtx_expiry);
    session_state.clear_ingress_context();
    if timeout_at > now {
        return Some(SessionDrainOutcome::Drained(Some(timeout_at)));
    }
    // Output::Timeout marks current output exhausted. Elapsed host time alone
    // does not advance str0m's clock, so feed a due deadline back and drain again.
    if *immediate_timeouts == SESSION_DRAIN_MAX_IMMEDIATE_TIMEOUTS {
        return Some(SessionDrainOutcome::Drained(Some(now)));
    }
    #[cfg(test)]
    let input_result = tests::handle_timeout_input(session_state, context, session_key, now);
    #[cfg(not(test))]
    let input_result = session_state.rtc.handle_input(Input::Timeout(now));
    if let Err(error) = input_result {
        return Some(SessionDrainOutcome::Failed(
            session_key.clone(),
            RtcDrainFailureStage::TimeoutInput,
            error,
        ));
    }
    *immediate_timeouts += 1;
    None
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
