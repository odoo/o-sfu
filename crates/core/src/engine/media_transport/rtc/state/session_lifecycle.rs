//! Session retirement across scheduling, identity, demux and route state.

use super::{PacketLoopState, RtcSessionState};
use crate::engine::media_transport::TransportSessionKey;

impl PacketLoopState {
    /// Retires a session and repairs every index retained by other sessions.
    ///
    /// Missing sessions still clear stale scheduling, demux and media state.
    /// Returns the removed session for lifetime accounting after route cleanup.
    pub fn remove_session(&mut self, session_key: &TransportSessionKey) -> Option<RtcSessionState> {
        self.clear_session_schedule(session_key);
        let removed_session = self.users.remove(session_key);
        self.remote_addr_demux.forget_user_remote_addrs(session_key);
        self.remote_addr_demux
            .forget_user_local_ice_ufrag(session_key);
        self.remote_addr_demux
            .forget_user_remote_candidates(session_key);
        for src_media in self.remove_session_media_handles(session_key) {
            self.remove_source_route(src_media);
        }
        let session_media = &mut self.session_media;
        self.routes
            .remove_dsts_for_session(session_key, |src_media, destination, dst_idx| {
                if let Some(consumer_lookup) = session_media.get_mut(&destination.dest_session) {
                    consumer_lookup.set_consumer_dst_idx(
                        destination.dest_mid,
                        destination.dest_transport_media_id,
                        src_media,
                        Some(dst_idx),
                    );
                }
            });
        let mid_registry = &self.mid_registry;
        self.routes
            .prune_unrouted_remote_srcs(|src_media| mid_registry.contains_key(src_media));
        removed_session
    }
}
