use str0m::{
    bwe::Bitrate as Str0mBitrate,
    change::DirectApi,
    media::{Mid, Rid},
    rtp::Ssrc,
};
use tracing::debug;

use crate::Bitrate;

/// Selects the authoritative SSRC when str0m already has a `(mid, rid)` binding.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum StaleSsrcPolicy {
    /// Preserves str0m's primary SSRC while applying negotiated repair state.
    KeepExisting,
    /// Replaces a binding that disagrees with the pending receive identity after
    /// answer application.
    ReplaceStale,
}

#[derive(Clone, Copy)]
pub(super) struct RecvStreamRepair {
    pub ssrc: Option<Ssrc>,
    pub nack_enabled: bool,
}

/// Reconciles one receive binding and reapplies its inbound REMB cap.
///
/// Answer application can recreate `StreamRx`. Every retained or replaced
/// binding must therefore receive `max_bitrate_in` in the same pass. A changed
/// repair SSRC recreates `StreamRx` because [`DirectApi`] cannot replace RTX
/// identity in place. Generic NACK follows the negotiated repair mapping rather
/// than repair SSRC discovery because the first NACK can discover dynamic RTX.
pub(super) fn apply_recv_stream(
    api: &mut DirectApi<'_>,
    mid: Mid,
    rid: Option<Rid>,
    ssrc: Ssrc,
    repair: RecvStreamRepair,
    max_bitrate_in: Bitrate,
    stale_policy: StaleSsrcPolicy,
) {
    let repair_ssrc = repair.ssrc;
    let next_ssrc = if let Some(stream_rx) = api.stream_rx_by_mid(mid, rid) {
        let existing_ssrc = stream_rx.ssrc();
        let existing_repair_ssrc = stream_rx.rtx();
        let primary_matches =
            stale_policy == StaleSsrcPolicy::KeepExisting || existing_ssrc == ssrc;
        if primary_matches && existing_repair_ssrc == repair_ssrc {
            stream_rx.suppress_nack(!repair.nack_enabled);
            stream_rx.request_remb(Str0mBitrate::bps(max_bitrate_in.as_bps()));
            return;
        }
        let next_ssrc = if stale_policy == StaleSsrcPolicy::KeepExisting {
            existing_ssrc
        } else {
            ssrc
        };
        api.remove_stream_rx(existing_ssrc);
        debug!(
            ?mid,
            rid = ?rid,
            previous_ssrc = ?existing_ssrc,
            ?next_ssrc,
            previous_repair_ssrc = ?existing_repair_ssrc,
            next_repair_ssrc = ?repair_ssrc,
            "replaced stale recv stream while applying answer"
        );
        next_ssrc
    } else {
        ssrc
    };
    let stream_rx = api.expect_stream_rx(next_ssrc, repair_ssrc, mid, rid);
    stream_rx.suppress_nack(!repair.nack_enabled);
    stream_rx.request_remb(Str0mBitrate::bps(max_bitrate_in.as_bps()));
}

#[cfg(test)]
#[path = "TESTS/recv_stream.rs"]
mod tests;
