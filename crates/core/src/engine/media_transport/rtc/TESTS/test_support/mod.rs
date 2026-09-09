#[cfg(test)]
mod media_routes;
#[cfg(any(test, feature = "internal-benchmarks"))]
mod packets;
#[cfg(any(test, feature = "testing-transport"))]
mod probe;
#[cfg(any(test, feature = "internal-benchmarks"))]
mod route_graph;

#[cfg(test)]
use std::time::Instant;

#[cfg(test)]
pub(super) use media_routes::{
    add_source_rid_stream, assert_consumer_packet_gate, assert_remote_keyframe_command,
    assert_remote_packet_gate_command, drain_ready_sessions, install_video_route_with_gate,
    install_video_route_with_pending_gate, register_remote_source_control,
    register_saturated_remote_source, saturated_remote_control, test_consumer_session_key,
    test_consumer_session_key_on_worker, test_source_session_key,
};
#[cfg(any(test, feature = "testing-transport"))]
pub use probe::{DebugPacketGate, DebugRouteDestination, DebugRouteEntry};
#[cfg(any(test, feature = "testing-transport"))]
pub(super) use probe::{
    DebugProbe, DebugProbeRequest, ObserveAudioActivityProbe, ReceiverBweTargetProbe,
    RecordIncomingMediaProbe, RouteEntryByConsumerMidProbe, RouteEntryByMediaIdProbe,
    RouteEntryProbe, RtcWorkerDebugChannels, RtcWorkerDebugHandle, handle_debug_probe,
};
#[cfg(test)]
pub(super) use probe::{
    RememberRemoteAddrProbe, SessionStreamRxSsrcProbe, SessionStreamTxSsrcProbe,
};
#[cfg(test)]
pub(super) use route_graph::prepare_source_session_with_rid;
#[cfg(test)]
pub(super) use {
    self::packets::sample_rtp_packet,
    super::packet_loop::forwarded_packet::test_support::{
        sample_already_relayed_audio_packet_at, sample_already_relayed_packet,
        sample_forwarded_packet_with_audio_activity, sample_forwarded_packet_with_rid,
        sample_local_forwarded_packet, sample_local_repaired_packet,
    },
};
#[cfg(feature = "internal-benchmarks")]
pub(super) use {
    self::packets::sample_rtp_packet_with_len,
    super::packet_loop::forwarded_packet::test_support::{
        BenchmarkPacketStaging, BenchmarkStreamIdentity, reset_packet_resolution,
        restage_packet_for_benchmark, sample_forwarded_packet_with_rid_and_audio_activity,
        sample_local_forwarded_packet_for_benchmark,
    },
};
#[cfg(any(test, feature = "internal-benchmarks"))]
pub(super) use {
    self::{
        packets::serialize_stun_message,
        route_graph::{MediaWorkerScenario, prepare_source_session},
    },
    super::packet_loop::forwarded_packet::test_support::sample_forwarded_packet_without_mid,
};

#[cfg(any(test, feature = "internal-benchmarks"))]
pub use super::packet_loop::forwarded_packet::test_support::sample_forwarded_packet;
#[cfg(any(test, feature = "internal-benchmarks"))]
use crate::engine::{
    ConnectionId, MediaWorkerId, RoomInstanceId, UserId, media_transport::TransportSessionKey,
};

#[cfg(test)]
pub(super) fn collect_ready_session_keys(
    state: &mut super::state::PacketLoopState,
    now: Instant,
) -> Vec<TransportSessionKey> {
    let mut ready_handles = Vec::new();
    state.collect_ready_sessions(now, &mut ready_handles);
    ready_handles
        .into_iter()
        .filter_map(|session_handle| state.users.key_for_handle(session_handle).cloned())
        .collect()
}

#[cfg(any(test, feature = "internal-benchmarks"))]
#[must_use]
pub fn test_transport_session_key(
    room_instance_id: u64,
    media_worker_id: usize,
    connection_id: u64,
    user_id: UserId,
) -> TransportSessionKey {
    TransportSessionKey::new(
        RoomInstanceId::from_raw(room_instance_id),
        MediaWorkerId::from_raw(media_worker_id),
        ConnectionId::from_raw(connection_id),
        user_id,
    )
}
