use tokio::sync::mpsc;

use super::super::ingress::{IngressPacket, RtcIngress};

/// Builds the production completed-datagram queue without a socket receive task.
pub(in crate::engine::media_transport::rtc) fn completed_datagram_channel()
-> (mpsc::Sender<IngressPacket>, RtcIngress) {
    let (rtc_ingress, packet_tx, _recycle_rx) = RtcIngress::new();
    (packet_tx, rtc_ingress)
}
