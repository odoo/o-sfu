use o_sfu_protocol::wire::UserPermissions;

use super::*;

fn recording_permissions() -> UserPermissions {
    UserPermissions {
        transcription: Some(true),
        audio_recording: Some(true),
        video_recording: Some(true),
    }
}

fn has_resolved_pending_request(
    resolutions: &[(RequestId, bool)],
    request_id: &RequestId,
    ok: bool,
) -> bool {
    resolutions
        .iter()
        .any(|(resolved_id, resolved_ok)| resolved_id == request_id && *resolved_ok == ok)
}

fn has_channel_info_update(updates: &[BundleUpdate]) -> bool {
    updates
        .iter()
        .any(|update| matches!(update, BundleUpdate::ChannelInfoChange(_)))
}

async fn drain_peer_until_pending_request_resolution(
    peer: &mut ProtocolHarnessPeer,
    ok: bool,
) -> Option<RequestId> {
    timeout(Duration::from_secs(1), async {
        loop {
            let Some(request) = peer.pending_request_starts.first() else {
                peer.read_server_frame().await?;
                continue;
            };
            if has_resolved_pending_request(
                &peer.pending_request_resolutions,
                &request.request_id,
                ok,
            ) {
                return Some(request.request_id.clone());
            }
            peer.read_server_frame().await?;
        }
    })
    .await
    .unwrap_or_default()
}

pub(crate) async fn connect_protocol_recording_peer(
    server: &TestServer,
    room: &Room,
) -> Option<ProtocolHarnessPeer> {
    let token = signed_connect_claims_with_permissions(
        TEST_ROOM_KEY,
        room.uuid(),
        UserId::Integer(63),
        Some(recording_permissions()),
    )?;
    let mut peer = ProtocolHarnessPeer::default();
    peer.connect_and_finish_handshake(&format!("ws://{}/", server.addr), &token, None)
        .await?;
    Some(peer)
}

pub(crate) async fn assert_recording_request_rejected(
    peer: &mut ProtocolHarnessPeer,
) -> Option<RequestId> {
    let request = peer.pending_request_starts.first()?;
    if peer.timers.get(&request.timeout_timer_id) != Some(&request.timeout_ms) {
        return None;
    }
    let request_id = drain_peer_until_pending_request_resolution(peer, false).await?;
    let request = peer.pending_request_starts.first()?;
    (!peer.timers.contains_key(&request.timeout_timer_id)
        && !has_channel_info_update(&peer.updates))
    .then_some(request_id)
}
