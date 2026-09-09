//! Session operations pairing receiver egress state with str0m.

use std::time::Instant;

use str0m::media::Mid;

use super::{
    super::{
        codec,
        consumer_egress::{LocalForwardedRtp, LocalPacketDestination, rotate_rtx_cache},
    },
    RtcSessionState, muxed_rtp_ssrc,
    slots::ConsumerStreamHandle,
};

impl RtcSessionState {
    /// Queues one receiver packet after validating its stream and delivery generation.
    ///
    /// Returns `None` when the str0m stream is absent or projection rejects the
    /// source identity. Successful writes retain their existing repair accounting.
    pub fn send_consumer_packet(
        &mut self,
        destination: &LocalPacketDestination,
        rtp: &LocalForwardedRtp<'_>,
        codec_packet: Option<&codec::Packet>,
    ) -> Option<usize> {
        destination.send(&mut self.consumer_streams, &mut self.rtc, rtp, codec_packet)
    }

    /// Invalidates cached repair before retiring queued writes for one route.
    pub fn release_consumer_stream(&mut self, handle: ConsumerStreamHandle) {
        self.invalidate_rtx_stream(handle);
        self.consumer_streams.release(handle);
    }

    pub fn note_repairable_transmit(&mut self, contents: &[u8], now: Instant) {
        let Some(ssrc) = muxed_rtp_ssrc(contents) else {
            return;
        };
        if let Some(primary_ssrc) = self.consumer_streams.note_repairable_transmit(ssrc, now) {
            rotate_rtx_cache(&mut self.rtc, primary_ssrc);
        }
    }

    pub fn invalidate_rtx_stream(&mut self, handle: ConsumerStreamHandle) {
        let Some(primary_ssrc) = self.consumer_streams.invalidate_rtx_stream(handle) else {
            return;
        };
        rotate_rtx_cache(&mut self.rtc, primary_ssrc);
    }

    pub fn expire_rtx_streams(&mut self, now: Instant) {
        self.consumer_streams.expire_rtx_streams(&mut self.rtc, now);
    }

    pub fn purge_removed_rtx_streams(&mut self) {
        self.consumer_streams
            .purge_removed_rtx_streams(&mut self.rtc);
    }

    pub fn reset_rtx_streams(&mut self, mid: Mid) {
        self.consumer_streams.reset_rtx_streams(mid);
    }

    /// Retires the destination `StreamTx` and host RTX state for one consumer MID.
    pub fn remove_consumer_stream_tx(&mut self, mid: Mid) {
        self.reset_rtx_streams(mid);
        {
            let mut api = self.rtc.direct_api();
            if let Some(ssrc) = api.stream_tx_by_mid(mid, None).map(|stream| stream.ssrc()) {
                api.remove_stream_tx(ssrc);
            }
        }
        self.purge_removed_rtx_streams();
    }
}
