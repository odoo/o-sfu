#![allow(
    clippy::panic,
    reason = "rtc media route test support fails loudly when mandatory fixture setup is impossible"
)]

use std::time::Instant;

use str0m::{
    media::{KeyframeRequestKind, Mid, Rid},
    rtp::Ssrc,
};
use tokio::sync::mpsc;

use super::{
    super::{
        commands::{RouteControlRequest, RtcWorkerCommand},
        state::{PacketLoopState, relay_registry::RelayTargetId, route_control::PacketLayerGate},
    },
    collect_ready_session_keys,
    route_graph::MediaWorkerScenario,
    test_transport_session_key,
};
use crate::engine::{
    UserId,
    media_transport::{TransportMediaId, TransportSessionKey},
};

pub fn drain_ready_sessions(state: &mut PacketLoopState) -> Vec<TransportSessionKey> {
    collect_ready_session_keys(state, Instant::now())
}

pub fn add_source_rid_stream(
    state: &mut PacketLoopState,
    src_key: &TransportSessionKey,
    src_mid: Mid,
    ssrc: u32,
    rid: Rid,
) {
    let Some(session) = state.users.get_mut(src_key) else {
        panic!("source session should exist before adding RID stream");
    };
    session
        .rtc
        .direct_api()
        .expect_stream_rx(Ssrc::from(ssrc), None, src_mid, Some(rid));
}

pub fn assert_consumer_packet_gate(
    state: &PacketLoopState,
    src_media: TransportMediaId,
    consumer_session: &TransportSessionKey,
    packet_gate: &PacketLayerGate,
    pending_gate: Option<&PacketLayerGate>,
) {
    assert!(state.routes.local_route(src_media).is_some_and(|route| {
        route.destinations.iter().any(|dst| {
            dst.dest_session == *consumer_session
                && &dst.packet_gate == packet_gate
                && dst.pending_gate.as_ref() == pending_gate
        })
    }));
}

pub fn install_video_route_with_gate(
    state: &mut PacketLoopState,
    src_media: TransportMediaId,
    dst_key: &TransportSessionKey,
    dst_mid: Mid,
    packet_gate: PacketLayerGate,
) -> TransportMediaId {
    let mut scenario = MediaWorkerScenario::new(state);
    scenario.destination_with_gate(src_media, dst_key.clone(), dst_mid, packet_gate)
}

pub fn assert_remote_keyframe_command(
    control_rx: &mut mpsc::Receiver<RtcWorkerCommand>,
    src_key: &TransportSessionKey,
    src_media: TransportMediaId,
    target_id: RelayTargetId,
    rid: Option<Rid>,
) {
    loop {
        match control_rx.try_recv().ok() {
            Some(RtcWorkerCommand::RouteControl {
                request: RouteControlRequest::SetRemoteSourcePacketGate { .. },
                response: None,
            }) => {}
            command => {
                assert!(matches!(
                    command,
                    Some(RtcWorkerCommand::RouteControl {
                        request: RouteControlRequest::RequestRemoteKeyframe {
                            source,
                            target_id: forwarded_target_id,
                            rid: forwarded_rid,
                            kind: KeyframeRequestKind::Pli,
                        },
                        response: None,
                    }) if source.session_key() == src_key
                        && source.transport_media_id() == src_media
                        && forwarded_target_id == target_id
                        && forwarded_rid == rid
                ));
                return;
            }
        }
    }
}

pub fn assert_remote_packet_gate_command(
    control_rx: &mut mpsc::Receiver<RtcWorkerCommand>,
    src_key: &TransportSessionKey,
    src_media: TransportMediaId,
    target_id: RelayTargetId,
    packet_gate: PacketLayerGate,
) {
    assert!(matches!(
        control_rx.try_recv().ok(),
        Some(RtcWorkerCommand::RouteControl {
            request: RouteControlRequest::SetRemoteSourcePacketGate {
                source,
                target_id: forwarded_target_id,
                packet_gate: forwarded_packet_gate,
            },
            response: None,
        }) if source.session_key() == src_key
            && source.transport_media_id() == src_media
            && forwarded_target_id == target_id
            && forwarded_packet_gate == packet_gate
    ));
}

pub fn test_source_session_key(seed: u64) -> TransportSessionKey {
    test_transport_session_key(seed, 0, seed + 1, test_user_id(seed + 2))
}

pub fn test_consumer_session_key(seed: u64) -> TransportSessionKey {
    test_consumer_session_key_on_worker(seed, 0)
}

pub fn test_consumer_session_key_on_worker(
    seed: u64,
    media_worker_id: usize,
) -> TransportSessionKey {
    test_transport_session_key(seed, media_worker_id, seed + 3, test_user_id(seed + 4))
}

fn test_user_id(value: u64) -> UserId {
    let Ok(value) = i64::try_from(value) else {
        panic!("test user id seed should fit in i64");
    };
    UserId::Integer(value)
}
