//! Ordered UDP windows, announced-source validation and reusable message plans.
//!
//! Protocol dispatch leaves only UDP transmits in their original order. Each
//! window holds at most 64 datagrams and ends before a rejected source.
//! Planning borrows the staging drain directly. Submission takes payload ownership
//! without copying bytes or retaining work across packet-loop turns.

use std::{net::SocketAddr, sync::Arc, vec::Drain};

use str0m::net::Transmit;

use self::plan::{UdpMessage, plan_messages};
use super::{RtcUdpSocket, TransmitRejection, report_rejection};
use crate::engine::metrics::RtcMetricsRecorder;

mod plan;
mod submit;

const UDP_TX_WINDOW: usize = 64;

pub(super) struct UdpEgress {
    socket: RtcUdpSocket,
    candidate_addr: SocketAddr,
    rtc_metrics: Arc<RtcMetricsRecorder>,
    messages: Vec<UdpMessage>,
}

impl UdpEgress {
    pub fn new(
        socket: RtcUdpSocket,
        candidate_addr: SocketAddr,
        rtc_metrics: Arc<RtcMetricsRecorder>,
    ) -> Self {
        Self {
            socket,
            candidate_addr,
            rtc_metrics,
            messages: Vec::with_capacity(UDP_TX_WINDOW),
        }
    }

    /// Completes the UDP batch in bounded windows, preserving datagram order.
    ///
    /// The caller removes other protocols before submission. Source rejection
    /// finishes preceding UDP output and separates future GSO groups.
    pub async fn flush(&mut self, mut transmits: Drain<'_, Transmit>) {
        loop {
            let count = transmits
                .as_slice()
                .iter()
                .take(UDP_TX_WINDOW)
                .take_while(|transmit| transmit.source == self.candidate_addr)
                .count();
            if count == 0 {
                let Some(transmit) = transmits.next() else {
                    break;
                };
                report_rejection(&transmit, TransmitRejection::SourceMismatch);
                continue;
            }
            let (window, _) = transmits.as_slice().split_at(count);
            plan_messages(window, &mut self.messages);
            submit::submit(
                &self.socket,
                &self.rtc_metrics,
                &self.messages,
                transmits.by_ref().take(count),
            )
            .await;
        }
    }
}
