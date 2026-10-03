//! Publisher RTP-to-NTP interpolation within a shared CNAME clock reference.
//! Each encoding uses arrival fallback until its own usable sender report.

use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use str0m::{
    media::{Frequency, SenderFeedback},
    rtp::Ssrc,
};

use super::ProducerPacketTime;

// Bound interpolation and clock correction without rejecting media when an
// authenticated publisher supplies stale or inconsistent sender reports.
pub(super) const MAX_REPORT_AGE: Duration = Duration::from_secs(10);

/// Shared offset preserves relative publisher timing but includes unknown network delay.
/// Corrections only move earlier, so slow publisher clock drift can eventually
/// exceed [`MAX_REPORT_AGE`] and force arrival fallback.
#[derive(Debug, Clone, Copy)]
pub(super) struct ClockAnchor {
    pub remote: SystemTime,
    pub local: Instant,
}

impl ClockAnchor {
    pub fn project(self, remote: SystemTime) -> Option<Instant> {
        match remote.duration_since(self.remote) {
            Ok(elapsed) => self.local.checked_add(elapsed),
            Err(error) => self.local.checked_sub(error.duration()),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct SenderClock {
    pub cname: Arc<str>,
    primary: Ssrc,
    received_at: Instant,
    report_at: SystemTime,
    timestamp: u32,
    rate: Frequency,
    anchor: ClockAnchor,
    prepared: Option<PreparedReport>,
}

#[derive(Debug, Clone, Copy)]
struct PreparedReport {
    sampled_at: Instant,
    earliest_arrival: Instant,
    latest_arrival: Instant,
}

impl SenderClock {
    pub fn new(cname: Arc<str>, feedback: SenderFeedback, anchor: ClockAnchor) -> Self {
        let info = &feedback.sender_info;
        let [a, b, c, d, ..] = info.rtp_time.numer().to_le_bytes();
        let mut sender = Self {
            cname,
            primary: info.ssrc,
            received_at: feedback.received_at,
            report_at: info.ntp_time,
            timestamp: u32::from_le_bytes([a, b, c, d]),
            rate: info.rtp_time.frequency(),
            anchor,
            prepared: None,
        };
        sender.reanchor(anchor);
        sender
    }

    pub fn anchor(&self) -> ClockAnchor {
        self.anchor
    }

    pub fn rejects(&self, feedback: &SenderFeedback) -> bool {
        self.primary == feedback.sender_info.ssrc
            && (feedback.received_at < self.received_at
                || feedback.sender_info.ntp_time <= self.report_at)
    }

    pub fn reanchor(&mut self, anchor: ClockAnchor) {
        self.anchor = anchor;
        self.prepared = (|| {
            // Direct local interpolation must preserve remote overflow fallback too.
            // Prepare only when every allowed offset fits in both clock domains.
            self.report_at.checked_sub(MAX_REPORT_AGE)?;
            self.report_at.checked_add(MAX_REPORT_AGE)?;
            let sampled_at = anchor.project(self.report_at)?;
            sampled_at.checked_sub(MAX_REPORT_AGE)?;
            sampled_at.checked_add(MAX_REPORT_AGE)?;
            Some(PreparedReport {
                sampled_at,
                earliest_arrival: self.received_at.checked_sub(MAX_REPORT_AGE)?,
                latest_arrival: self.received_at.checked_add(MAX_REPORT_AGE)?,
            })
        })();
    }

    fn sample(&self, primary: Ssrc, time: ProducerPacketTime) -> Option<Instant> {
        if self.primary != primary
            || (self.rate != Frequency::SECONDS && self.rate != time.clock_rate)
        {
            return None;
        }
        if let Some(prepared) = &self.prepared {
            if time.received_at < prepared.earliest_arrival
                || time.received_at > prepared.latest_arrival
            {
                return None;
            }
        } else if time
            .received_at
            .saturating_duration_since(self.received_at)
            .max(self.received_at.saturating_duration_since(time.received_at))
            > MAX_REPORT_AGE
        {
            return None;
        }
        let delta = time.timestamp.wrapping_sub(self.timestamp);
        if delta == 1 << 31 {
            return None;
        }
        let ticks = i64::from(delta.cast_signed());
        let rate = u64::from(time.clock_rate.get());
        if ticks.unsigned_abs() > rate * MAX_REPORT_AGE.as_secs() {
            return None;
        }
        let offset = Duration::from_nanos(ticks.unsigned_abs() * 1_000_000_000 / rate);
        if let Some(prepared) = &self.prepared {
            return if ticks < 0 {
                prepared.sampled_at.checked_sub(offset)
            } else {
                prepared.sampled_at.checked_add(offset)
            };
        }
        let remote = if ticks < 0 {
            self.report_at.checked_sub(offset)?
        } else {
            self.report_at.checked_add(offset)?
        };
        self.anchor.project(remote)
    }
}

#[derive(Debug, Clone, Copy)]
struct FrameSample {
    timestamp: u32,
    rate: Frequency,
    sampled_at: Option<Instant>,
}

#[derive(Debug, Default, Clone)]
pub(super) struct PublisherClock {
    pub sender: Option<SenderClock>,
    frame: Option<FrameSample>,
}

impl PublisherClock {
    pub fn replace_primary(&mut self, primary: Ssrc) {
        self.frame = None;
        if self
            .sender
            .as_ref()
            .is_some_and(|sender| sender.primary != primary)
        {
            self.sender = None;
        }
    }

    /// Chooses and remembers a bounded sample, reporting any group-anchor correction.
    /// Equal-frame choices, including missing timing, survive later sender reports.
    pub fn prepare_packet(
        &mut self,
        primary: Ssrc,
        time: ProducerPacketTime,
    ) -> (Option<Instant>, bool) {
        let advances = match self.frame {
            Some(frame) if frame.rate == time.clock_rate => {
                let delta = time.timestamp.wrapping_sub(frame.timestamp);
                if delta == 0 {
                    return (frame.sampled_at.filter(|at| *at <= time.received_at), false);
                }
                delta.cast_signed() > 0
            }
            _ => true,
        };
        let remember = advances && !time.was_repair;
        let mut sampled_at = self
            .sender
            .as_ref()
            .and_then(|sender| sender.sample(primary, time));
        let mut rebased = false;
        if let Some(sample) = sampled_at.filter(|sample| *sample > time.received_at) {
            let correction = sample - time.received_at;
            sampled_at = None;
            if remember
                && correction <= MAX_REPORT_AGE
                && let Some(sender) = self.sender.as_mut()
                && let Some(local) = sender.anchor.local.checked_sub(correction)
            {
                sender.reanchor(ClockAnchor {
                    local,
                    ..sender.anchor
                });
                sampled_at = Some(time.received_at);
                rebased = true;
            }
        }
        if remember {
            self.frame = Some(FrameSample {
                timestamp: time.timestamp,
                rate: time.clock_rate,
                sampled_at,
            });
        }
        (sampled_at, rebased)
    }
}
