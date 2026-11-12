use o_sfu_model::UserId;

use crate::{
    engine::media_transport::rtc::{
        state::ufrag_registry::UfragRegistry, test_support::test_transport_session_key,
    },
    server::transport::TransportSessionKey,
};

impl UfragRegistry {
    pub(in crate::engine::media_transport::rtc) fn ufrag_for(
        &self,
        session_key: &TransportSessionKey,
    ) -> Option<&str> {
        self.ufrag_by_session.get(session_key).map(String::as_str)
    }

    pub(in crate::engine::media_transport::rtc) fn is_empty(&self) -> bool {
        self.session_by_ufrag.is_empty() && self.ufrag_by_session.is_empty()
    }
}

fn session_key(room_instance_id: u64, session_numeric_id: i64) -> TransportSessionKey {
    test_transport_session_key(0, 0, room_instance_id, UserId::Integer(session_numeric_id))
}

#[test]
fn remember_local_ice_ufrag_tracks_the_latest_session_mapping() {
    let mut registry = UfragRegistry::default();
    let first_session = session_key(9, 3);
    let second_session = session_key(9, 4);

    assert!(registry.remember("ufrag-a", &first_session));
    assert!(!registry.remember("ufrag-a", &first_session));
    assert!(registry.remember("ufrag-a", &second_session));

    assert_eq!(registry.session_for("ufrag-a"), Some(&second_session));
    assert_eq!(registry.ufrag_for(&first_session), None);
    assert_eq!(registry.ufrag_for(&second_session), Some("ufrag-a"));
}

#[test]
fn forget_session_removes_both_directions() {
    let mut registry = UfragRegistry::default();
    let first_session = session_key(9, 3);
    let second_session = session_key(9, 4);
    assert!(registry.remember("ufrag-a", &first_session));
    assert!(registry.remember("ufrag-b", &second_session));
    registry.forget_session(&first_session);
    assert_eq!(registry.session_for("ufrag-a"), None);
    assert_eq!(registry.ufrag_for(&first_session), None);
    assert_eq!(registry.session_for("ufrag-b"), Some(&second_session));
    assert_eq!(registry.ufrag_for(&second_session), Some("ufrag-b"));
    registry.forget_session(&second_session);
    assert!(registry.is_empty());
}
