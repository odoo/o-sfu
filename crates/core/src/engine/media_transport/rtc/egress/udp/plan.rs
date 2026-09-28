//! Socket-independent mapping from ordered datagrams to submission messages.
//!
//! TODO(gro-gso-sendmmsg): group only contiguous compatible datagrams when GSO
//! is implemented. Keep their original boundaries for ordinary-send fallback.
//! Segmentation metadata belongs here while ancillary storage belongs to the
//! executor. Never pad protected payloads to create compatible sizes.

use std::num::NonZeroUsize;

use str0m::net::Transmit;

/// One kernel message covering the next nonempty group of window entries.
///
/// Ordinary sends cover one datagram. Submission validates that the counts
/// cover the entire window before using the plan.
pub(super) struct UdpMessage {
    pub datagrams: NonZeroUsize,
}

pub(super) fn plan_messages(transmits: &[Transmit], messages: &mut Vec<UdpMessage>) {
    messages.clear();
    messages.resize_with(transmits.len(), || UdpMessage {
        datagrams: NonZeroUsize::MIN,
    });
}

#[cfg(test)]
#[path = "TESTS/plan.rs"]
mod tests;
