use super::{
    ForwardedPacket, PacketForwarder, PacketLoopState, RtcMetricsRecorder, RtpMetricsRecorder,
    SourcePolicySignal, finish_incoming_stats, record_incoming_packet,
};

/// Runs the existing observation-only verification window over staged packets.
pub(in crate::engine::media_transport::rtc) fn record_incoming_stats(
    state: &mut PacketLoopState,
    source_policy_signal: &SourcePolicySignal,
    control: &RtcMetricsRecorder,
    rtp: &RtpMetricsRecorder,
    forwarder: &mut PacketForwarder,
    packets: &mut [ForwardedPacket],
) {
    for packet in packets {
        let _ = record_incoming_packet(state, control, rtp, forwarder, packet);
    }
    finish_incoming_stats(state, source_policy_signal, control, forwarder);
}
