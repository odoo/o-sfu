//! Consumer routes coupled to session media identity and receiver repair state.

use o_sfu_router::rtp::MediaStream as RouterRtpParameters;
use str0m::media::Mid;
use tracing::debug;

use super::{
    PacketLoopState,
    media_registry::RegisteredMediaHandle,
    route_control::PacketLayerGate,
    slots::ConsumerStreamHandle,
    source_route::{DecoderDelivery, MediaRouteDestination},
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
        let dest_payload_type = codec::primary_payload_type(consumer_rtp);
        let repair_enabled = codec::repair_enabled(consumer_rtp);
        let requires_decoder_refresh =
            codec::requires_decoder_refresh(consumer_rtp, dest_payload_type);
        let initial_gate = codec::initial_consumer_packet_gate(consumer_rtp);
        let delivery = DecoderDelivery::new(requires_decoder_refresh, initial_gate);
        if requires_decoder_refresh {
            debug!(
                source_transport_media_id = ?src_media,
                requested_packet_gate = ?initial_gate,
                "blocked video route until its decoder refresh arrives"
            );
        }
        self.register_consumer_destination(src_media, |consumer_media| MediaRouteDestination {
            dest_session: consumer_key.clone(),
            dest_transport_media_id: consumer_media,
            dest_stream: consumer_stream,
            dest_mid: consumer_mid,
            dest_payload_type,
            repair_enabled,
            active,
            delivery,
        })
    }

    /// Applies consumer activity and invalidates repair state before returning.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError::InvalidInput`] for mismatched source or
    /// consumer ownership or an incompatible media handle.
    /// Returns [`TransportAdapterError::TransportUnavailable`] when source registration,
    /// consumer media or its route destination is missing.
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

    /// Removes a consumer route and retires its stream.
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
        self.release_destination_stream(&removed.destination);
    }

    /// Removes every destination and receiver stream retained by one source route.
    pub fn remove_source_route(&mut self, src_media: TransportMediaId) {
        let Some(route_entry) = self.routes.take_route(src_media) else {
            return;
        };
        for destination in route_entry.destinations() {
            self.release_destination_stream(destination);
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
            src_media: consumer_src_media,
            destination,
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
        let destination = *destination;
        let update =
            match mutation {
                ConsumerRouteMutation::Active(active) => {
                    self.routes
                        .set_consumer_active(src_media, destination, consumer_media, active)
                }
                ConsumerRouteMutation::PacketGate(packet_gate) => self
                    .routes
                    .set_consumer_pkt_gate(src_media, destination, consumer_media, packet_gate),
            }?;
        if update.repair_delivery_changed {
            let (routes, users) = (&self.routes, &mut self.users);
            if let Some(destination) = routes
                .local_route(src_media)
                .and_then(|route| route.destination(destination, consumer_media))
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
