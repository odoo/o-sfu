use crate::engine::media_transport::{
    SourceActivityUpdate, TransportAdapterError, TransportSourceKey,
    rtc::{
        recovery::invalidate_source_repair,
        state::{
            PacketLoopState,
            relay_registry::{RelayPacketMailbox, RelayTargetId},
            route_control::PacketLayerGate,
        },
    },
};

pub(super) fn set_remote_src_pkt_gate(
    state: &mut PacketLoopState,
    source: &TransportSourceKey,
    target_id: RelayTargetId,
    packet_gate: PacketLayerGate,
) {
    let src_media = source.transport_media_id();
    if state
        .ensure_local_producer_mid(source.session_key(), src_media)
        .is_err()
    {
        return;
    }
    state
        .routes
        .set_relay_pkt_gate(src_media, target_id, packet_gate);
}

pub(super) fn worker_add_relay_target(
    state: &mut PacketLoopState,
    source: &TransportSourceKey,
    target_id: RelayTargetId,
    target: RelayPacketMailbox,
) -> Result<(), TransportAdapterError> {
    let src_media = source.transport_media_id();
    state.ensure_local_producer_mid(source.session_key(), src_media)?;
    state.routes.add_relay_target(src_media, target_id, target);
    Ok(())
}

pub(super) fn worker_remove_relay_target(
    state: &mut PacketLoopState,
    source: &TransportSourceKey,
    target_id: RelayTargetId,
) -> Result<(), TransportAdapterError> {
    let src_media = source.transport_media_id();
    match state.ensure_local_producer_mid(source.session_key(), src_media) {
        Ok(_) => {}
        Err(TransportAdapterError::TransportUnavailable) => return Ok(()),
        Err(error) => return Err(error),
    }
    state.routes.remove_relay_target(src_media, target_id);
    Ok(())
}

pub(super) fn worker_set_relay_target_active(
    state: &mut PacketLoopState,
    source: &TransportSourceKey,
    target_id: RelayTargetId,
    active: bool,
) -> Result<(), TransportAdapterError> {
    let src_media = source.transport_media_id();
    // only activation revalidates the producer, deactivation must still apply
    // after the source is torn down
    if active {
        state.ensure_local_producer_mid(source.session_key(), src_media)?;
    }
    state
        .routes
        .set_relay_target_active(src_media, target_id, active);
    Ok(())
}

pub(super) fn worker_apply_producer_activity(
    state: &mut PacketLoopState,
    source: &TransportSourceKey,
    update: SourceActivityUpdate,
) -> Result<bool, TransportAdapterError> {
    let src_media = source.transport_media_id();
    state.ensure_local_producer_mid(source.session_key(), src_media)?;
    let active = update.activity().is_active();
    let activity_changed = state.routes.source_is_active(src_media) != active;
    let accepted = state.routes.apply_source_activity(src_media, update)?;
    if accepted {
        state.apply_producer_nack_policy(source.session_key(), src_media);
    }
    if accepted && activity_changed {
        invalidate_source_repair(state, src_media);
    }
    Ok(accepted)
}

pub(super) fn worker_set_remote_source_activity(
    state: &mut PacketLoopState,
    source: &TransportSourceKey,
    update: SourceActivityUpdate,
) -> Result<(), TransportAdapterError> {
    let src_media = source.transport_media_id();
    match state.routes.remote_source(src_media) {
        Some(registration) if registration.source() == source => {}
        Some(_) => return Err(TransportAdapterError::InvalidInput),
        None => return Ok(()),
    }
    let activity_changed =
        state.routes.source_is_active(src_media) != update.activity().is_active();
    let accepted = state.routes.apply_source_activity(src_media, update)?;
    if accepted && activity_changed {
        invalidate_source_repair(state, src_media);
    }
    Ok(())
}
