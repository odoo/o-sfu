use std::collections::{BTreeMap, HashMap};

use crate::engine::media_transport::TransportSessionKey;

#[derive(Debug, Default)]
pub struct UfragRegistry {
    /// local ICE ufrag to session recovery hint
    session_by_ufrag: HashMap<String, TransportSessionKey>,
    /// reverse lookup for replacing or removing a session local ICE ufrag
    ufrag_by_session: BTreeMap<TransportSessionKey, String>,
}
impl UfragRegistry {
    /// returns the session advertised by a local ICE ufrag
    ///
    /// this index is used to narrow STUN recovery when the USERNAME attribute
    /// names the local fragment
    /// the returned session is still only a candidate for `Rtc::accepts()`
    pub(in super::super) fn session_for(&self, ufrag: &str) -> Option<&TransportSessionKey> {
        self.session_by_ufrag.get(ufrag)
    }

    #[cfg(test)]
    pub(in super::super) fn ufrag_for(&self, session_key: &TransportSessionKey) -> Option<&str> {
        self.ufrag_by_session.get(session_key).map(String::as_str)
    }

    #[cfg(test)]
    pub(in super::super) fn is_empty(&self) -> bool {
        self.session_by_ufrag.is_empty() && self.ufrag_by_session.is_empty()
    }

    /// replaces the local ICE ufrag registered for a session
    ///
    /// each session owns at most one local ufrag
    /// each local ufrag maps to at most one session
    /// returning `false` means the existing mapping already expressed that
    /// contract
    pub(in super::super) fn remember(
        &mut self,
        ufrag: &str,
        session_key: &TransportSessionKey,
    ) -> bool {
        if self
            .session_by_ufrag
            .get(ufrag)
            .is_some_and(|current_session| current_session == session_key)
        {
            return false;
        }
        let previous_ufrag = self
            .ufrag_by_session
            .insert(session_key.clone(), ufrag.to_owned());
        if let Some(previous_ufrag) = previous_ufrag {
            self.session_by_ufrag.remove(&previous_ufrag);
        }
        let previous_session = self
            .session_by_ufrag
            .insert(ufrag.to_owned(), session_key.clone());
        if let Some(previous_session) = previous_session {
            self.ufrag_by_session.remove(&previous_session);
        }
        true
    }

    /// removes the local ICE ufrag recovery hint for a session
    pub(in super::super) fn forget_session(
        &mut self,
        session_key: &TransportSessionKey,
    ) -> Option<String> {
        let ufrag = self.ufrag_by_session.remove(session_key)?;
        self.session_by_ufrag.remove(&ufrag);
        Some(ufrag)
    }
}

#[cfg(test)]
#[path = "../TESTS/ufrag_registry.rs"]
mod tests;
