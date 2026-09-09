//! Worker-local indexes for UDP ingress routing.
//!
//! Learned source addresses provide fast-path session pins. Local ICE ufrags
//! and signaled candidate addresses narrow unknown-source recovery while
//! `Rtc::accepts()` remains the ownership authority. Mutations update forward
//! and reverse indexes together so session teardown cannot leave routing hints.

use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
};

use crate::engine::media_transport::TransportSessionKey;

/// Bidirectional demux indexes for worker-local UDP ingress recovery.
#[derive(Debug, Default)]
pub struct RemoteAddrDemux {
    /// learned UDP source tuple to session pin
    remote_addr_index: HashMap<SocketAddr, TransportSessionKey>,
    /// reverse lookup for learned UDP source tuple cleanup
    remote_addrs_by_session: BTreeMap<TransportSessionKey, Vec<SocketAddr>>,
    /// local ICE ufrag to session recovery hint
    local_ice_ufrag_index: HashMap<String, TransportSessionKey>,
    /// reverse lookup for replacing or removing a session local ICE ufrag
    local_ice_ufrag_by_session: BTreeMap<TransportSessionKey, String>,
    /// signaled remote candidate address to possible sessions
    remote_candidate_addr_index: HashMap<SocketAddr, Vec<TransportSessionKey>>,
    /// reverse lookup for candidate hint cleanup after renegotiation or teardown
    remote_candidate_addrs_by_session: BTreeMap<TransportSessionKey, Vec<SocketAddr>>,
}

impl RemoteAddrDemux {
    /// returns the session currently pinned to a UDP source tuple
    ///
    /// this is a hot-path hint for cached ingress routing
    /// callers must still re-check the packet with `Rtc::accepts()` before
    /// feeding it into a session because ICE state can move after the pin was
    /// learned
    #[must_use]
    pub fn session_key_for_remote_addr(
        &self,
        source_addr: SocketAddr,
    ) -> Option<&TransportSessionKey> {
        self.remote_addr_index.get(&source_addr)
    }

    /// pins a UDP source tuple to the session that just accepted traffic
    ///
    /// returns `true` when the visible mapping changed
    /// remapping a tuple also removes it from the previous session reverse
    /// index so session teardown can later clean every learned tuple with one
    /// key
    #[must_use]
    pub fn remember_remote_addr(
        &mut self,
        source_addr: SocketAddr,
        session_key: &TransportSessionKey,
    ) -> bool {
        if self
            .remote_addr_index
            .get(&source_addr)
            .is_some_and(|current_session| current_session == session_key)
        {
            return false;
        }
        let previous_session = self
            .remote_addr_index
            .insert(source_addr, session_key.clone());
        if let Some(previous_session) = previous_session {
            self.remove_remote_addr(&previous_session, source_addr);
        }
        let session_addrs = self
            .remote_addrs_by_session
            .entry(session_key.clone())
            .or_default();
        if !session_addrs.contains(&source_addr) {
            session_addrs.push(source_addr);
        }
        true
    }

    /// returns the session advertised by a local ICE ufrag
    ///
    /// this index is used to narrow STUN recovery when the USERNAME attribute
    /// names the local fragment
    /// the returned session is still only a candidate for `Rtc::accepts()`
    pub(in super::super) fn session_for_local_ufrag(
        &self,
        local_ice_ufrag: &str,
    ) -> Option<&TransportSessionKey> {
        self.local_ice_ufrag_index.get(local_ice_ufrag)
    }

    /// replaces the local ICE ufrag registered for a session
    ///
    /// each session owns at most one local ufrag
    /// each local ufrag maps to at most one session
    /// returning `false` means the existing mapping already expressed that
    /// contract
    pub(in super::super) fn remember_local_ice_ufrag(
        &mut self,
        local_ice_ufrag: &str,
        session_key: &TransportSessionKey,
    ) -> bool {
        if self
            .local_ice_ufrag_index
            .get(local_ice_ufrag)
            .is_some_and(|current_session| current_session == session_key)
        {
            return false;
        }
        let previous_ufrag = self
            .local_ice_ufrag_by_session
            .insert(session_key.clone(), local_ice_ufrag.to_owned());
        if let Some(previous_ufrag) = previous_ufrag {
            self.local_ice_ufrag_index.remove(&previous_ufrag);
        }
        let previous_session = self
            .local_ice_ufrag_index
            .insert(local_ice_ufrag.to_owned(), session_key.clone());
        if let Some(previous_session) = previous_session {
            self.local_ice_ufrag_by_session.remove(&previous_session);
        }
        true
    }

    /// Returns sessions whose signaled candidates match the observed source.
    ///
    /// Candidate addresses are weaker than learned source pins because every
    /// session advertising the address remains a candidate. `Rtc::accepts()`
    /// remains the ownership authority.
    #[must_use]
    pub fn candidates_for_src_addr(
        &self,
        source_addr: SocketAddr,
    ) -> Option<&[TransportSessionKey]> {
        self.remote_candidate_addr_index
            .get(&source_addr)
            .map(Vec::as_slice)
    }

    /// replaces all signaled remote candidate addresses for one session
    ///
    /// this is called after answer application when str0m has updated the
    /// candidate set
    /// old hints are removed first so recovery cannot keep probing a session
    /// through candidates that no longer belong to it
    pub fn replace_remote_candidates<I>(
        &mut self,
        session_key: &TransportSessionKey,
        candidate_addrs: I,
    ) where
        I: IntoIterator<Item = SocketAddr>,
    {
        self.forget_user_remote_candidates(session_key);
        let session_candidate_addrs = self
            .remote_candidate_addrs_by_session
            .entry(session_key.clone())
            .or_default();
        for candidate_addr in candidate_addrs {
            if session_candidate_addrs.contains(&candidate_addr) {
                continue;
            }
            session_candidate_addrs.push(candidate_addr);
            self.remote_candidate_addr_index
                .entry(candidate_addr)
                .or_default()
                .push(session_key.clone());
        }
        if session_candidate_addrs.is_empty() {
            self.remote_candidate_addrs_by_session.remove(session_key);
        }
    }

    /// removes one learned UDP source tuple pin after cached `Rtc::accepts()` rejection
    pub(in super::super) fn forget_remote_addr(&mut self, source_addr: SocketAddr) {
        let Some(session_key) = self.remote_addr_index.remove(&source_addr) else {
            return;
        };
        self.remove_remote_addr(&session_key, source_addr);
    }

    /// removes every learned UDP source tuple when its session is removed
    pub(in super::super) fn forget_user_remote_addrs(&mut self, session_key: &TransportSessionKey) {
        let Some(session_addrs) = self.remote_addrs_by_session.remove(session_key) else {
            return;
        };
        for source_addr in session_addrs {
            self.remote_addr_index.remove(&source_addr);
        }
    }

    /// removes the local ICE ufrag recovery hint for a session
    pub(in super::super) fn forget_user_local_ice_ufrag(
        &mut self,
        session_key: &TransportSessionKey,
    ) {
        let Some(local_ice_ufrag) = self.local_ice_ufrag_by_session.remove(session_key) else {
            return;
        };
        self.local_ice_ufrag_index.remove(&local_ice_ufrag);
    }

    /// removes all remote candidate recovery hints owned by a session
    ///
    /// candidate address indexes can contain several sessions for one address
    /// cleanup removes only the target session from each fanout list and drops
    /// empty address entries afterward
    pub(in super::super) fn forget_user_remote_candidates(
        &mut self,
        session_key: &TransportSessionKey,
    ) {
        let Some(candidate_addrs) = self.remote_candidate_addrs_by_session.remove(session_key)
        else {
            return;
        };
        for candidate_addr in candidate_addrs {
            let should_remove_index_entry = self
                .remote_candidate_addr_index
                .get_mut(&candidate_addr)
                .is_some_and(|session_keys| {
                    session_keys.retain(|candidate_session| candidate_session != session_key);
                    session_keys.is_empty()
                });
            if should_remove_index_entry {
                self.remote_candidate_addr_index.remove(&candidate_addr);
            }
        }
    }

    #[cfg(test)]
    pub(in super::super) fn session_addrs_for(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<&[SocketAddr]> {
        self.remote_addrs_by_session
            .get(session_key)
            .map(Vec::as_slice)
    }

    #[cfg(test)]
    pub(in super::super) fn local_ice_ufrag_for(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<&str> {
        self.local_ice_ufrag_by_session
            .get(session_key)
            .map(String::as_str)
    }

    #[cfg(test)]
    pub(in super::super) fn remote_candidate_addrs_for(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<&[SocketAddr]> {
        self.remote_candidate_addrs_by_session
            .get(session_key)
            .map(Vec::as_slice)
    }

    #[cfg(test)]
    pub(in super::super) fn is_empty(&self) -> bool {
        self.remote_addrs_by_session.is_empty()
            && self.local_ice_ufrag_by_session.is_empty()
            && self.remote_candidate_addrs_by_session.is_empty()
    }

    fn remove_remote_addr(&mut self, session_key: &TransportSessionKey, source_addr: SocketAddr) {
        let should_remove_session_entry = self
            .remote_addrs_by_session
            .get_mut(session_key)
            .is_some_and(|session_addrs| {
                if let Some(position) = session_addrs.iter().position(|addr| *addr == source_addr) {
                    session_addrs.swap_remove(position);
                }
                session_addrs.is_empty()
            });
        if should_remove_session_entry {
            self.remote_addrs_by_session.remove(session_key);
        }
    }
}

#[cfg(test)]
#[path = "../TESTS/demux.rs"]
mod tests;
