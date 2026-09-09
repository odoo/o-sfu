//! Media removal coupled to receive streams, routes, repair and session accounting.

use std::sync::Mutex;

use super::{PacketLoopState, bitrate::BitrateRegistry, media_registry::RegisteredMediaHandle};
use crate::engine::media_transport::{TransportAdapterError, TransportMediaId};

impl PacketLoopState {
    /// Removes media identity and every dependent route, stream and accounting entry.
    ///
    /// Callers stage the final MID's negotiated removal before committing teardown.
    /// Receive or transmit streams are retired before their NACK totals are cleared.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError::InvalidInput`] when the media is unregistered.
    pub fn unregister_media_handle(
        &mut self,
        bitrate_registry: &Mutex<BitrateRegistry>,
        transport_media_id: TransportMediaId,
    ) -> Result<(), TransportAdapterError> {
        let Some(registered) = self.media_handle(transport_media_id) else {
            return Err(TransportAdapterError::InvalidInput);
        };
        let session_key = registered.session_key().clone();
        let mid = registered.mid();
        let keep_mid = self.session_has_other_media_mid(&session_key, mid, transport_media_id);
        if matches!(registered, RegisteredMediaHandle::Producer { .. }) {
            let ssrcs = self
                .routes
                .producer_ssrcs(transport_media_id)
                .unwrap_or_default()
                .to_vec();
            if let Some(session_state) = self.users.get_mut(&session_key) {
                let mut api = session_state.rtc.direct_api();
                for ssrc in ssrcs {
                    api.remove_stream_rx(ssrc);
                }
            }
        }
        let Some(handle) = self.remove_media_handle(transport_media_id) else {
            return Err(TransportAdapterError::InvalidInput);
        };
        match handle {
            RegisteredMediaHandle::Producer {
                session_key: owner,
                mid,
            } => {
                if let Ok(mut bitrate) = bitrate_registry.lock() {
                    bitrate.remove_incoming_media(&owner, transport_media_id);
                }
                if !keep_mid && let Some(session_state) = self.users.get_mut(&owner) {
                    session_state
                        .sdp_negotiation
                        .negotiated_producer_parameters
                        .remove(&mid);
                }
                self.remove_source_route(transport_media_id);
            }
            RegisteredMediaHandle::Consumer {
                session_key: owner,
                src_media,
                ..
            } => {
                self.remove_consumer_route(&owner, transport_media_id, src_media);
                if let Some(session_state) = self.users.get_mut(&owner) {
                    if keep_mid {
                        session_state.purge_removed_rtx_streams();
                    } else {
                        session_state.remove_consumer_stream_tx(mid);
                    }
                }
            }
        }
        if !keep_mid && let Some(session_state) = self.users.get_mut(&session_key) {
            // A NACK baseline belongs to the MID lifetime. Retire its cumulative
            // StreamRx or StreamTx before clearing the totals.
            session_state.nack_totals.remove_mid(mid);
        }
        self.mark_session_dirty(&session_key);
        Ok(())
    }
}
