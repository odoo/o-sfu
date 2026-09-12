//! Requested packet selection and the delivery currently safe for one decoder.

use str0m::media::Rid;

use super::PacketLayerGate;

#[cfg(any(test, feature = "internal-benchmarks"))]
#[path = "TESTS/decoder_delivery.rs"]
mod test_support;

/// Decoder readiness and receiver reanchoring generation for one destination.
///
/// A pending selection survives temporary fallback delivery. Only an observable
/// matching refresh activates that selection. Gate changes and pauses advance
/// the generation so receiver projection cannot reuse the previous delivery.
#[derive(Debug, Clone)]
pub(in crate::engine::media_transport::rtc) struct DecoderDelivery {
    requires_refresh: bool,
    effective_gate: PacketLayerGate,
    pending_gate: Option<PacketLayerGate>,
    generation: u64,
}

#[derive(Clone, Copy)]
pub(in crate::engine::media_transport::rtc) enum DeliveryTransition {
    Unchanged,
    IntentChanged,
    DeliveryChanged,
}

impl DeliveryTransition {
    pub(in crate::engine::media_transport::rtc) const fn changed(self) -> bool {
        !matches!(self, Self::Unchanged)
    }

    pub(in crate::engine::media_transport::rtc) const fn delivery_changed(self) -> bool {
        matches!(self, Self::DeliveryChanged)
    }
}

pub(in crate::engine::media_transport::rtc) enum SelectedRefresh {
    NotWaiting,
    Pending,
    Activated,
}

pub(in crate::engine::media_transport::rtc) enum DestinationKeyframeTarget {
    Current(Option<Rid>),
    Stale,
}

impl DecoderDelivery {
    /// Defers a refresh-sensitive selection and admits other codecs immediately.
    pub(in crate::engine::media_transport::rtc) const fn new(
        requires_refresh: bool,
        initial_gate: PacketLayerGate,
    ) -> Self {
        Self {
            requires_refresh,
            effective_gate: if requires_refresh {
                PacketLayerGate::Block
            } else {
                initial_gate
            },
            pending_gate: if requires_refresh {
                Some(initial_gate)
            } else {
                None
            },
            generation: 0,
        }
    }

    pub(in crate::engine::media_transport::rtc) const fn effective_gate(&self) -> PacketLayerGate {
        self.effective_gate
    }

    pub(in crate::engine::media_transport::rtc) const fn pending_gate(
        &self,
    ) -> Option<PacketLayerGate> {
        self.pending_gate
    }

    pub(in crate::engine::media_transport::rtc) const fn generation(&self) -> u64 {
        self.generation
    }

    /// Applies selection intent without discarding a usable matching fallback.
    ///
    /// Selecting the effective fallback consumes pending intent without changing
    /// receiver delivery. Repeating a pending selection changes nothing.
    pub(in crate::engine::media_transport::rtc) fn select(
        &mut self,
        gate: PacketLayerGate,
    ) -> DeliveryTransition {
        if self.effective_gate == gate {
            return if self.pending_gate.take().is_some() {
                DeliveryTransition::IntentChanged
            } else {
                DeliveryTransition::Unchanged
            };
        }
        if self.pending_gate == Some(gate) {
            return DeliveryTransition::Unchanged;
        }
        if self.requires_refresh {
            self.effective_gate = PacketLayerGate::Block;
            self.pending_gate = Some(gate);
        } else {
            self.effective_gate = gate;
            self.pending_gate = None;
        }
        self.advance_generation();
        DeliveryTransition::DeliveryChanged
    }

    /// Starts a new receiver delivery generation without changing selection.
    pub(in crate::engine::media_transport::rtc) fn advance_generation(&mut self) {
        self.generation = self.generation.wrapping_add(1);
    }

    /// Reanchors delivery and defers refresh-sensitive media until a matching refresh.
    pub(in crate::engine::media_transport::rtc) fn pause(&mut self) {
        self.advance_generation();
        if self.requires_refresh {
            self.pending_gate.get_or_insert(self.effective_gate);
            self.effective_gate = PacketLayerGate::Block;
        }
    }

    /// Suspends a selected RID that is absent from the current source liveness set.
    ///
    /// Pending selections and their fallback remain unchanged. Returns the stale
    /// selected RID whose decoder now requires recovery.
    pub(in crate::engine::media_transport::rtc) fn suspend_stale(
        &mut self,
        incoming_rid: Rid,
        ready_rids: &[Rid],
    ) -> Option<Rid> {
        if self.pending_gate.is_some() {
            return None;
        }
        let selected_rid = self.effective_gate.selected_rid()?;
        if selected_rid == incoming_rid || ready_rids.contains(&selected_rid) {
            return None;
        }
        self.pause();
        Some(selected_rid)
    }

    /// Activates a matching pending selection only when the packet proves refresh.
    ///
    /// Pending `Open` accepts a RID-less refresh. Explicit `Block` never activates
    /// from packet observation.
    pub(in crate::engine::media_transport::rtc) fn observe_selected_refresh(
        &mut self,
        incoming_rid: Option<Rid>,
        is_refresh: bool,
    ) -> SelectedRefresh {
        let Some(pending) = self.pending_gate else {
            return SelectedRefresh::NotWaiting;
        };
        match pending {
            PacketLayerGate::Rid(selected) if Some(selected) != incoming_rid => {
                return SelectedRefresh::NotWaiting;
            }
            PacketLayerGate::Block => return SelectedRefresh::NotWaiting,
            PacketLayerGate::Open | PacketLayerGate::Rid(_) => {}
        }
        if !is_refresh {
            return SelectedRefresh::Pending;
        }
        self.effective_gate = pending;
        self.pending_gate = None;
        self.advance_generation();
        SelectedRefresh::Activated
    }

    /// Admits a fallback refresh while retaining the different requested RID.
    ///
    /// The source owner calls this only after proving that no destination
    /// activated its requested selection for the current packet. Returns the
    /// selected RID that still requires refresh.
    pub(in crate::engine::media_transport::rtc) fn activate_fallback(
        &mut self,
        incoming_rid: Rid,
    ) -> Option<Rid> {
        let selected_rid = self.pending_gate.and_then(|gate| gate.selected_rid())?;
        if selected_rid == incoming_rid || self.effective_gate != PacketLayerGate::Block {
            return None;
        }
        self.effective_gate = PacketLayerGate::Rid(incoming_rid);
        self.advance_generation();
        Some(selected_rid)
    }

    pub(in crate::engine::media_transport::rtc) fn keyframe_target_rid(
        &self,
        open_rid: Option<Rid>,
    ) -> DestinationKeyframeTarget {
        if let Some(pending_gate) = self.pending_gate {
            return DestinationKeyframeTarget::Current(pending_gate.selected_rid());
        }
        let target_rid = match self.effective_gate {
            PacketLayerGate::Rid(rid) => Some(rid),
            PacketLayerGate::Block => return DestinationKeyframeTarget::Stale,
            PacketLayerGate::Open => open_rid,
        };
        DestinationKeyframeTarget::Current(target_rid)
    }

    /// Targets the intended RID while a temporary fallback is being forwarded.
    pub(in crate::engine::media_transport::rtc) fn requested_keyframe_rid(&self) -> Option<Rid> {
        self.pending_gate
            .and_then(|gate| gate.selected_rid())
            .or_else(|| self.effective_gate.selected_rid())
    }
}
