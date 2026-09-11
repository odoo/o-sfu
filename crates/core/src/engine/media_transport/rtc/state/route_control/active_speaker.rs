//! Packet-level active-speaker observation state.
//!
//! The RTC engine can observe RFC 6464 audio-level and VAD metadata while it
//! routes packets. This module turns those packet facts into a bounded
//! transport-owned active-speaker signal and a packet gate for silent audio
//! sources. Room policy later maps the active audio sources into room layout
//! intent.

use std::time::{Duration, Instant};

use super::packet_gate::PacketLayerGate;
use crate::engine::media_transport::{
    ActiveSpeakerActivityReason, ActiveSpeakerActivityState, ActiveSpeakerSource,
    ActiveSpeakerSourceDiagnostic, TransportMediaId,
};

const ACTIVE_SPEAKER_HOLD_WINDOW: Duration = Duration::from_millis(250);
const ACTIVE_SPEAKER_AUDIO_LEVEL_SPEECH_THRESHOLD_DBOV: i8 = -48;
const ACTIVE_SPEAKER_AUDIO_LEVEL_NOISE_FLOOR_DBOV: i8 = -56;
const AUDIO_LEVEL_PROMOTION_OBSERVATIONS: u8 = 2;
const AUDIO_LEVEL_PROMOTION_WINDOW: Duration = Duration::from_millis(120);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceAudioPolicyState {
    active_until: Option<Instant>,
    last_spoke_at: Option<Instant>,
    active_reason: Option<ActiveSpeakerActivityReason>,
    inactive_state: ActiveSpeakerActivityState,
    inactive_reason: ActiveSpeakerActivityReason,
    fallback_started_at: Option<Instant>,
    fallback_observations: u8,
    last_audio_level_dbov: Option<i8>,
    packet_gate: PacketLayerGate,
}

impl Default for SourceAudioPolicyState {
    fn default() -> Self {
        Self {
            active_until: None,
            last_spoke_at: None,
            active_reason: None,
            inactive_state: ActiveSpeakerActivityState::Idle,
            inactive_reason: ActiveSpeakerActivityReason::NoMetadata,
            fallback_started_at: None,
            fallback_observations: 0,
            last_audio_level_dbov: None,
            packet_gate: PacketLayerGate::default(),
        }
    }
}

impl SourceAudioPolicyState {
    pub fn observe_packet(
        &mut self,
        voice_activity: Option<bool>,
        audio_level_dbov: Option<i8>,
        now: Instant,
    ) {
        self.last_audio_level_dbov = audio_level_dbov;
        match voice_activity {
            Some(true) => self.promote(now, ActiveSpeakerActivityReason::Vad),
            Some(false) => self.observe_inactive(now, ActiveSpeakerActivityReason::VadFalse),
            None => self.observe_audio_level(audio_level_dbov, now),
        }
    }

    pub fn packet_gate(&self) -> PacketLayerGate {
        self.packet_gate
    }

    pub fn active_speaker_source(
        &self,
        transport_media_id: TransportMediaId,
        now: Instant,
    ) -> Option<ActiveSpeakerSource> {
        self.last_spoke_at
            .filter(|_| self.active_until.is_some_and(|deadline| now < deadline))
            .map(|observed_at| {
                ActiveSpeakerSource::with_audio_level(
                    transport_media_id,
                    observed_at,
                    self.last_audio_level_dbov,
                )
            })
    }

    pub fn active_deadline_after(&self, now: Instant) -> Option<Instant> {
        self.active_until.filter(|deadline| *deadline > now)
    }

    pub fn diagnostic(
        &self,
        transport_media_id: TransportMediaId,
        now: Instant,
    ) -> ActiveSpeakerSourceDiagnostic {
        if let Some(deadline) = self.active_until {
            if deadline > now {
                return ActiveSpeakerSourceDiagnostic::new(
                    transport_media_id,
                    ActiveSpeakerActivityState::Active,
                    self.active_reason
                        .unwrap_or(ActiveSpeakerActivityReason::AudioLevel),
                    self.last_audio_level_dbov,
                    self.fallback_observations,
                    Some(deadline.duration_since(now)),
                );
            }
            if self.last_spoke_at.is_some() {
                return ActiveSpeakerSourceDiagnostic::new(
                    transport_media_id,
                    ActiveSpeakerActivityState::RecentlyExpired,
                    ActiveSpeakerActivityReason::Expired,
                    self.last_audio_level_dbov,
                    self.fallback_observations,
                    None,
                );
            }
        }
        ActiveSpeakerSourceDiagnostic::new(
            transport_media_id,
            self.inactive_state,
            self.inactive_reason,
            self.last_audio_level_dbov,
            self.fallback_observations,
            None,
        )
    }

    fn observe_audio_level(&mut self, audio_level_dbov: Option<i8>, now: Instant) {
        let Some(audio_level_dbov) = audio_level_dbov else {
            self.observe_inactive(now, ActiveSpeakerActivityReason::MissingAudioMetadata);
            return;
        };
        if audio_level_dbov < ACTIVE_SPEAKER_AUDIO_LEVEL_NOISE_FLOOR_DBOV {
            self.observe_inactive(now, ActiveSpeakerActivityReason::LowNoise);
            return;
        }
        if audio_level_dbov < ACTIVE_SPEAKER_AUDIO_LEVEL_SPEECH_THRESHOLD_DBOV {
            self.observe_inactive(now, ActiveSpeakerActivityReason::BelowSpeechThreshold);
            return;
        }
        self.record_fallback_observation(now);
        self.packet_gate = PacketLayerGate::Open;
        if self.fallback_observations >= AUDIO_LEVEL_PROMOTION_OBSERVATIONS {
            self.promote(now, ActiveSpeakerActivityReason::AudioLevel);
            return;
        }
        if self.active_until.is_some_and(|deadline| deadline <= now) {
            self.active_until = None;
            self.active_reason = None;
        }
        self.inactive_state = ActiveSpeakerActivityState::Idle;
        self.inactive_reason = ActiveSpeakerActivityReason::AudioLevelWarmup;
    }

    fn observe_inactive(&mut self, now: Instant, reason: ActiveSpeakerActivityReason) {
        self.reset_fallback_observations();
        // during the active hold window keep the gate open so a brief pause does
        // not clip audio forwarding
        if self.active_until.is_some_and(|deadline| now < deadline) {
            self.packet_gate = PacketLayerGate::Open;
            return;
        }
        self.active_until = None;
        self.active_reason = None;
        self.inactive_state = ActiveSpeakerActivityState::Blocked;
        self.inactive_reason = reason;
        self.packet_gate = PacketLayerGate::Block;
    }

    fn promote(&mut self, now: Instant, reason: ActiveSpeakerActivityReason) {
        if reason == ActiveSpeakerActivityReason::Vad {
            self.reset_fallback_observations();
        }
        self.active_until = Some(now + ACTIVE_SPEAKER_HOLD_WINDOW);
        self.last_spoke_at = Some(now);
        self.active_reason = Some(reason);
        self.packet_gate = PacketLayerGate::Open;
    }

    fn record_fallback_observation(&mut self, now: Instant) {
        let extends_window = self
            .fallback_started_at
            .and_then(|started_at| now.checked_duration_since(started_at))
            .is_some_and(|elapsed| elapsed <= AUDIO_LEVEL_PROMOTION_WINDOW);
        if extends_window {
            self.fallback_observations = self
                .fallback_observations
                .saturating_add(1)
                .min(AUDIO_LEVEL_PROMOTION_OBSERVATIONS);
            return;
        }
        self.fallback_started_at = Some(now);
        self.fallback_observations = 1;
    }

    fn reset_fallback_observations(&mut self) {
        self.fallback_started_at = None;
        self.fallback_observations = 0;
    }
}
