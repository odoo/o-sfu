//! Consumer routes coupled to session MID indexes and receiver repair state.

use o_sfu_router::rtp::MediaStream as RouterRtpParameters;
use str0m::media::Mid;

use super::{
    PacketLoopState, media_registry::RegisteredMediaHandle, route_control::PacketLayerGate,
    slots::ConsumerStreamHandle, source_route::MediaRouteDestination,
};
use crate::engine::media_transport::{
    TransportAdapterError, TransportConsumerRoute, TransportMediaId, TransportResult,
    TransportSessionKey, TransportSourceKey, rtc::codec,
};

/// Consumer stream and negotiated RTP identity committed as one local route.
#[derive(Clone, Copy)]
pub struct ConsumerRouteRegistration<'a> {
    pub consumer_key: &'a TransportSessionKey,
    pub consumer_stream: ConsumerStreamHandle,
    pub consumer_mid: Mid,
    pub src_media: TransportMediaId,
    pub consumer_rtp: &'a RouterRtpParameters,
    pub active: bool,
}

impl PacketLoopState {
    /// Registers a consumer identity, destination stream and its MID route index together.
    pub fn register_consumer_route(
        &mut self,
        registration: ConsumerRouteRegistration<'_>,
    ) -> TransportMediaId {
        let ConsumerRouteRegistration {
            consumer_key,
            consumer_stream,
            consumer_mid,
            src_media,
            consumer_rtp,
            active,
        } = registration;
        let consumer_media = self.register_media_handle(RegisteredMediaHandle::Consumer {
            session_key: consumer_key.clone(),
            mid: consumer_mid,
            src_media,
        });
        let dest_payload_type = codec::primary_payload_type(consumer_rtp);
        let repair_enabled = codec::repair_enabled(consumer_rtp);
        let requires_decoder_refresh =
            codec::requires_decoder_refresh(consumer_rtp, dest_payload_type);
        let (packet_gate, pending_gate) = MediaRouteDestination::guarded_packet_gate(
            requires_decoder_refresh,
            src_media,
            codec::initial_consumer_packet_gate(consumer_rtp),
        );
        let dst_idx = self.routes.add_consumer_route(
            src_media,
            MediaRouteDestination {
                dest_session: consumer_key.clone(),
                dest_transport_media_id: consumer_media,
                dest_stream: consumer_stream,
                dest_mid: consumer_mid,
                dest_payload_type,
                repair_enabled,
                active,
                requires_decoder_refresh,
                delivery_generation: 0,
                packet_gate,
                pending_gate,
            },
        );
        self.set_consumer_dst_idx(
            consumer_key,
            consumer_mid,
            consumer_media,
            src_media,
            Some(dst_idx),
        );
        consumer_media
    }

    /// Applies consumer activity and invalidates repair state before returning.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError::InvalidInput`] for mismatched source or
    /// consumer ownership or an incompatible media handle.
    /// Returns [`TransportAdapterError::TransportUnavailable`] when source registration,
    /// consumer media or its indexed destination is missing or stale.
    pub fn set_consumer_active(
        &mut self,
        route: &TransportConsumerRoute,
        active: bool,
    ) -> TransportResult<()> {
        self.update_consumer_route(route, ConsumerRouteMutation::Active(active))
            .map(|_| ())
    }

    /// Applies ordered consumer gates with repair invalidation and one source-gate refresh.
    ///
    /// Successful entries remain applied when another entry fails. Results retain
    /// input order. The aggregate gate is refreshed once if any route changed.
    ///
    /// # Errors
    ///
    /// Each entry can return the errors from [`Self::set_consumer_active`].
    /// A route for another source returns [`TransportAdapterError::InvalidInput`].
    pub fn set_consumer_packet_gates(
        &mut self,
        source: &TransportSourceKey,
        updates: impl ExactSizeIterator<Item = (TransportConsumerRoute, PacketLayerGate)>,
    ) -> Vec<TransportResult<()>> {
        let src_media = source.transport_media_id();
        let mut changed = false;
        let mut results = Vec::with_capacity(updates.len());
        for (route, packet_gate) in updates {
            if route.source() != source {
                results.push(Err(TransportAdapterError::InvalidInput));
                continue;
            }
            let result =
                self.update_consumer_route(&route, ConsumerRouteMutation::PacketGate(packet_gate));
            results.push(
                result
                    .inspect(|route_changed| changed |= *route_changed)
                    .map(|_| ()),
            );
        }
        if changed {
            self.routes.refresh_src_pkt_gate(src_media);
        }
        results
    }

    /// Removes a consumer route, repairs displaced MID indexes and retires its stream.
    pub fn remove_consumer_route(
        &mut self,
        consumer_key: &TransportSessionKey,
        consumer_media: TransportMediaId,
        src_media: TransportMediaId,
    ) {
        let Some(removed) =
            self.routes
                .remove_consumer_route(src_media, consumer_key, consumer_media)
        else {
            self.routes.prune_unrouted_remote_src(src_media);
            return;
        };
        self.set_consumer_dst_idx(
            &removed.destination.dest_session,
            removed.destination.dest_mid,
            removed.destination.dest_transport_media_id,
            src_media,
            None,
        );
        if let Some(moved) = &removed.moved {
            // Route removal uses `swap_remove`. Repair the moved destination's
            // lookup index before later control or keyframe lookup uses it.
            self.set_consumer_dst_idx(
                &moved.session_key,
                moved.mid,
                moved.media_id,
                src_media,
                Some(moved.dst_idx),
            );
        }
        self.release_destination_stream(&removed.destination);
    }

    /// Removes every destination and receiver stream retained by one source route.
    pub fn remove_source_route(&mut self, src_media: TransportMediaId) {
        let Some(route_entry) = self.routes.take_route(src_media) else {
            return;
        };
        for destination in route_entry.destinations {
            self.set_consumer_dst_idx(
                &destination.dest_session,
                destination.dest_mid,
                destination.dest_transport_media_id,
                src_media,
                None,
            );
            self.release_destination_stream(&destination);
        }
    }

    fn update_consumer_route(
        &mut self,
        route: &TransportConsumerRoute,
        mutation: ConsumerRouteMutation,
    ) -> TransportResult<bool> {
        let consumer_key = route.consumer_session_key();
        let consumer_media = route.consumer_transport_media_id();
        let src_media = route.source_transport_media_id();
        self.ensure_existing_route_src(consumer_key, route.source())?;
        let RegisteredMediaHandle::Consumer {
            session_key,
            mid,
            src_media: consumer_src_media,
            ..
        } = self
            .media_handle(consumer_media)
            .ok_or(TransportAdapterError::TransportUnavailable)?
        else {
            return Err(TransportAdapterError::InvalidInput);
        };
        if session_key != consumer_key || *consumer_src_media != src_media {
            return Err(TransportAdapterError::InvalidInput);
        }
        let dst_idx = self
            .consumer_dst_idx(consumer_key, *mid, consumer_media, src_media)
            .ok_or(TransportAdapterError::TransportUnavailable)?;
        let update = match mutation {
            ConsumerRouteMutation::Active(active) => self.routes.set_consumer_active(
                src_media,
                dst_idx,
                consumer_key,
                consumer_media,
                active,
            ),
            ConsumerRouteMutation::PacketGate(packet_gate) => self.routes.set_consumer_pkt_gate(
                src_media,
                dst_idx,
                consumer_key,
                consumer_media,
                packet_gate,
            ),
        }?;
        if update.repair_delivery_changed {
            let (routes, users) = (&self.routes, &mut self.users);
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

    fn release_destination_stream(&mut self, destination: &MediaRouteDestination) {
        if let Some(session_state) = self.users.get_mut(&destination.dest_session) {
            session_state.release_consumer_stream(destination.dest_stream);
        }
    }
}

#[derive(Clone, Copy)]
enum ConsumerRouteMutation {
    Active(bool),
    PacketGate(PacketLayerGate),
}
