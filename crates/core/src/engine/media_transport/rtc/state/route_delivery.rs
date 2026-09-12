//! Route delivery transitions coupled to session repair and producer NACK policy.

use std::time::{Duration, Instant};

use str0m::media::Rid;

use super::{
    PacketLoopState,
    route_table::{RidReadinessRouteUpdate, RidReadinessScratch},
};
use crate::engine::media_transport::{
    SourceActivityUpdate, TransportAdapterError, TransportMediaId, TransportSourceKey,
};

/// A selected RID must still produce packets within this freshness window.
const SELECTED_RID_READY_MAX_AGE: Duration = Duration::from_secs(2);

impl PacketLoopState {
    /// Applies producer activity with NACK policy and consumer repair invalidation.
    ///
    /// Accepted revisions that preserve activity keep existing receiver repair.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError::InvalidInput`] when the media belongs to
    /// another owner or is not a producer. Returns
    /// [`TransportAdapterError::TransportUnavailable`] when the producer or its
    /// route state is missing.
    pub fn apply_producer_activity(
        &mut self,
        source: &TransportSourceKey,
        update: SourceActivityUpdate,
    ) -> Result<bool, TransportAdapterError> {
        let src_media = source.transport_media_id();
        self.ensure_local_producer_mid(source.session_key(), src_media)?;
        let activity_changed =
            self.routes.source_is_active(src_media) != update.activity().is_active();
        let accepted = self.routes.apply_source_activity(src_media, update)?;
        if accepted {
            self.apply_producer_nack_policy(source.session_key(), src_media);
        }
        if accepted && activity_changed {
            self.invalidate_source_repair(src_media);
        }
        Ok(accepted)
    }

    /// Applies registered remote-source activity before retiring previous repair.
    ///
    /// Missing registrations and obsolete revisions are unchanged.
    ///
    /// # Errors
    ///
    /// Returns [`TransportAdapterError::InvalidInput`] when the source identity
    /// differs from the current remote registration.
    pub fn set_remote_source_activity(
        &mut self,
        source: &TransportSourceKey,
        update: SourceActivityUpdate,
    ) -> Result<(), TransportAdapterError> {
        let src_media = source.transport_media_id();
        match self.routes.remote_source(src_media) {
            Some(registration) if registration.source() == source => {}
            Some(_) => return Err(TransportAdapterError::InvalidInput),
            None => return Ok(()),
        }
        let activity_changed =
            self.routes.source_is_active(src_media) != update.activity().is_active();
        let accepted = self.routes.apply_source_activity(src_media, update)?;
        if accepted && activity_changed {
            self.invalidate_source_repair(src_media);
        }
        Ok(())
    }

    /// Commits decoder gates and invalidates affected repair before feedback dispatch.
    ///
    /// Source packet liveness must already include the current observation.
    /// Scratch vectors retain capacity and collect the RIDs requiring feedback.
    pub fn update_decoder_readiness(
        &mut self,
        src_media: TransportMediaId,
        incoming_rid: Option<Rid>,
        is_keyframe: bool,
        now: Instant,
        scratch: &mut RidReadinessScratch,
    ) -> RidReadinessRouteUpdate {
        self.routes.collect_ready_producer_rids(
            src_media,
            now,
            SELECTED_RID_READY_MAX_AGE,
            &mut scratch.ready,
        );
        let (routes, users) = (&mut self.routes, &mut self.users);
        routes.update_decoder_readiness(
            src_media,
            incoming_rid,
            is_keyframe,
            scratch,
            |destination| {
                if let Some(session_state) = users.get_mut(&destination.dest_session) {
                    session_state.invalidate_rtx_stream(destination.dest_stream);
                }
            },
        )
    }

    fn invalidate_source_repair(&mut self, src_media: TransportMediaId) {
        let (routes, users) = (&self.routes, &mut self.users);
        let Some(route) = routes.local_route(src_media) else {
            return;
        };
        for destination in &route.destinations {
            if destination.repair_enabled
                && let Some(session_state) = users.get_mut(&destination.dest_session)
            {
                session_state.invalidate_rtx_stream(destination.dest_stream);
            }
        }
    }
}
