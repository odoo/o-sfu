//! Producer and route-source ownership validation.

use str0m::media::Mid;

use super::{
    super::commands::RemoteSourceControl, PacketLoopState, media_registry::RegisteredMediaHandle,
};
use crate::engine::media_transport::{
    TransportAdapterError, TransportMediaId, TransportSessionKey, TransportSourceKey,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteSourceKind {
    Local,
    Remote,
}

impl RouteSourceKind {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Remote => "remote",
        }
    }
}

impl PacketLoopState {
    /// validates source ownership for a route that is about to be created
    ///
    /// local sources must still be producer handles owned by the declared source
    /// session
    /// remote sources require `remote_source_control` because later route refreshes
    /// need a command path back to the producer worker
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError::InvalidInput`] when a source id belongs to another owner, when the
    /// media id names a consumer or when a remote source is missing its control path
    /// Returns [`TransportAdapterError::TransportUnavailable`] when the local source media id does not exist
    pub fn ensure_route_src_registered(
        &mut self,
        route_owner_session_key: &TransportSessionKey,
        source: &TransportSourceKey,
        remote_source_control: Option<RemoteSourceControl>,
    ) -> Result<RouteSourceKind, TransportAdapterError> {
        let src_key = source.session_key();
        let src_media = source.transport_media_id();
        if src_key.media_worker_id() == route_owner_session_key.media_worker_id() {
            self.ensure_local_producer_mid(src_key, src_media)?;
            return Ok(RouteSourceKind::Local);
        }
        let Some(remote_source_control) = remote_source_control else {
            return Err(TransportAdapterError::InvalidInput);
        };
        self.routes
            .register_remote_source(source, remote_source_control)?;
        Ok(RouteSourceKind::Remote)
    }

    /// Validates source ownership without creating remote-source state.
    ///
    /// Existing-route commands can lag teardown. They fail after registration
    /// disappears instead of recreating it from stale control input.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError::InvalidInput`] when the media id exists but belongs to a different
    /// source owner or is not a producer source for a local route
    /// Returns [`TransportAdapterError::TransportUnavailable`] when the expected local producer or remote
    /// source registration is gone
    pub fn ensure_existing_route_src(
        &self,
        route_owner_session_key: &TransportSessionKey,
        source: &TransportSourceKey,
    ) -> Result<RouteSourceKind, TransportAdapterError> {
        let src_key = source.session_key();
        let src_media = source.transport_media_id();
        if src_key.media_worker_id() == route_owner_session_key.media_worker_id() {
            self.ensure_local_producer_mid(src_key, src_media)?;
            return Ok(RouteSourceKind::Local);
        }
        match self.routes.remote_source(src_media) {
            Some(registration) if registration.source().session_key() == src_key => {
                Ok(RouteSourceKind::Remote)
            }
            Some(_) => Err(TransportAdapterError::InvalidInput),
            None => Err(TransportAdapterError::TransportUnavailable),
        }
    }

    /// returns the MID for a local producer after enforcing source ownership
    ///
    /// this is the strict form used by command paths that must reject stale or
    /// misaddressed producer effects
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError::InvalidInput`] when the media id exists without a live producer
    /// owned by `src_key`
    /// Returns [`TransportAdapterError::TransportUnavailable`] when the media id is not registered
    pub fn ensure_local_producer_mid(
        &self,
        src_key: &TransportSessionKey,
        src_media: TransportMediaId,
    ) -> Result<Mid, TransportAdapterError> {
        match self.media_handle(src_media) {
            Some(RegisteredMediaHandle::Producer { session_key, mid })
                if session_key == src_key && self.users.contains_key(src_key) =>
            {
                Ok(*mid)
            }
            Some(
                RegisteredMediaHandle::Producer { .. } | RegisteredMediaHandle::Consumer { .. },
            ) => Err(TransportAdapterError::InvalidInput),
            None => Err(TransportAdapterError::TransportUnavailable),
        }
    }
}
