use crate::engine::media_transport::{
    SourceActivityUpdate, TransportAdapterError, TransportConsumerRoute, TransportResult,
    TransportSourceKey,
    rtc::{
        recovery::invalidate_source_repair,
        state::{
            PacketLoopState,
            media_registry::RegisteredMediaHandle,
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

pub(super) fn worker_set_consumer_active(
    state: &mut PacketLoopState,
    route: &TransportConsumerRoute,
    active: bool,
) -> Result<(), TransportAdapterError> {
    update_consumer_route(state, route, ConsumerRouteMutation::Active(active)).map(|_| ())
}

pub(super) fn worker_set_consumer_pkt_gates(
    state: &mut PacketLoopState,
    source: &TransportSourceKey,
    updates: Vec<(usize, TransportConsumerRoute, PacketLayerGate)>,
) -> Vec<TransportResult<()>> {
    let src_media = source.transport_media_id();
    let mut changed = false;
    let mut results = Vec::with_capacity(updates.len());
    for (_, route, packet_gate) in updates {
        if route.source() != source {
            results.push(Err(TransportAdapterError::InvalidInput));
            continue;
        }
        let result = update_consumer_route(
            state,
            &route,
            ConsumerRouteMutation::PacketGate(packet_gate),
        );
        results.push(
            result
                .inspect(|route_changed| changed |= *route_changed)
                .map(|_| ()),
        );
    }
    if changed {
        state.routes.refresh_src_pkt_gate(src_media);
    }
    results
}

#[derive(Clone, Copy)]
enum ConsumerRouteMutation {
    Active(bool),
    PacketGate(PacketLayerGate),
}

fn update_consumer_route(
    state: &mut PacketLoopState,
    route: &TransportConsumerRoute,
    mutation: ConsumerRouteMutation,
) -> Result<bool, TransportAdapterError> {
    let consumer_key = route.consumer_session_key();
    let consumer_media = route.consumer_transport_media_id();
    let src_media = route.source_transport_media_id();
    state.ensure_existing_route_src(consumer_key, route.source())?;
    let RegisteredMediaHandle::Consumer {
        session_key,
        mid,
        src_media: consumer_src_media,
        ..
    } = state
        .media_handle(consumer_media)
        .ok_or(TransportAdapterError::TransportUnavailable)?
    else {
        return Err(TransportAdapterError::InvalidInput);
    };
    if session_key != consumer_key || *consumer_src_media != src_media {
        return Err(TransportAdapterError::InvalidInput);
    }
    let dst_idx = state
        .consumer_dst_idx(consumer_key, *mid, consumer_media, src_media)
        .ok_or(TransportAdapterError::TransportUnavailable)?;
    let update = match mutation {
        ConsumerRouteMutation::Active(active) => state.routes.set_consumer_active(
            src_media,
            dst_idx,
            consumer_key,
            consumer_media,
            active,
        ),
        ConsumerRouteMutation::PacketGate(packet_gate) => state.routes.set_consumer_pkt_gate(
            src_media,
            dst_idx,
            consumer_key,
            consumer_media,
            packet_gate,
        ),
    }?;
    if update.repair_delivery_changed {
        let (routes, users) = (&state.routes, &mut state.users);
        if let Some(destination) = routes
            .local_route(src_media)
            .and_then(|route| route.destinations.get(dst_idx))
            && let Some(session_state) = users.get_mut(consumer_key)
        {
            session_state.invalidate_rtx_stream(destination.dest_stream);
        }
    }
    Ok(update.route_changed)
}
