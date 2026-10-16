use std::{collections::HashMap, time::Instant};

use super::*;

fn setup_speaker_sources(
    ranked_owners: &[(TransportMediaId, UserId)],
) -> (HashMap<TransportMediaId, UserId>, Vec<ActiveSpeakerSource>) {
    let now = Instant::now();
    let mut media_to_user_map = HashMap::new();
    let mut active_speaker_sources = Vec::new();
    for (media_id, user_id) in ranked_owners.iter().cloned() {
        media_to_user_map.insert(media_id, user_id);
        active_speaker_sources.push(ActiveSpeakerSource::new(media_id, now));
    }
    (media_to_user_map, active_speaker_sources)
}

#[test]
fn active_speaker_facts_features_top_ranked_owner() {
    let ranked_owners = [
        (TransportMediaId::new(102), UserId::from(2)),
        (TransportMediaId::new(103), UserId::from(3)),
    ];
    let (media_to_user_map, active_speaker_sources) = setup_speaker_sources(&ranked_owners);
    let active_speaker_facts = active_speaker_facts(
        |media_id| media_to_user_map.get(&media_id).cloned(),
        &active_speaker_sources,
    );
    assert_eq!(
        active_speaker_facts.desired_featured_user_id,
        Some(UserId::from(2))
    );
}

#[test]
fn active_speaker_facts_ranks_users_by_first_seen_source() {
    let ranked_owners = [
        (TransportMediaId::new(106), UserId::from(2)),
        (TransportMediaId::new(105), UserId::from(2)),
        (TransportMediaId::new(104), UserId::from(3)),
        (TransportMediaId::new(103), UserId::from(3)),
        (TransportMediaId::new(102), UserId::from(2)),
        (TransportMediaId::new(101), UserId::from(1)),
    ];
    let (media_to_user_map, active_speaker_sources) = setup_speaker_sources(&ranked_owners);
    let active_speaker_facts = active_speaker_facts(
        |media_id| media_to_user_map.get(&media_id).cloned(),
        &active_speaker_sources,
    );
    assert_eq!(
        active_speaker_facts.active_speaker_rank_by_user,
        BTreeMap::from([
            (UserId::from(2), 0),
            (UserId::from(3), 1),
            (UserId::from(1), 2),
        ])
    );
}

#[test]
fn active_speaker_facts_truncates_featured_users_at_clear_limit() {
    let mut ranked_owners = Vec::with_capacity(ACTIVE_SPEAKER_FEATURED_CLEAR_LIMIT + 1);
    let mut current_id = 0u64;
    while ranked_owners.len() < ACTIVE_SPEAKER_FEATURED_CLEAR_LIMIT {
        ranked_owners.push((TransportMediaId::new(current_id), UserId::from(1)));
        current_id += 1;
    }
    let overflow_media_id = TransportMediaId::new(999);
    let overflow_user_id = UserId::from(999);
    ranked_owners.push((overflow_media_id, overflow_user_id.clone()));
    let (media_to_user_map, active_speaker_sources) = setup_speaker_sources(&ranked_owners);
    let active_speaker_facts = active_speaker_facts(
        |media_id| media_to_user_map.get(&media_id).cloned(),
        &active_speaker_sources,
    );
    assert_eq!(
        active_speaker_facts.featured_source_user_ids,
        BTreeSet::from([UserId::from(1)])
    );
    assert!(
        active_speaker_facts
            .active_speaker_rank_by_user
            .contains_key(&overflow_user_id)
    );
}

#[test]
fn active_speaker_facts_skips_sources_without_owner() {
    let ranked_owners = [(TransportMediaId::new(2), UserId::from(2))];
    let (media_to_user_map, mut active_speaker_sources) = setup_speaker_sources(&ranked_owners);
    active_speaker_sources.push(ActiveSpeakerSource::new(
        TransportMediaId::new(999),
        Instant::now(),
    ));
    let active_speaker_facts = active_speaker_facts(
        |media_id| media_to_user_map.get(&media_id).cloned(),
        &active_speaker_sources,
    );
    assert_eq!(
        active_speaker_facts.active_speaker_rank_by_user,
        BTreeMap::from([(UserId::from(2), 0)])
    );
}

#[test]
fn active_speaker_facts_skips_initial_ownerless_source_for_desired_featured_user() {
    let ranked_owners = [(TransportMediaId::new(2), UserId::from(2))];
    let (media_to_user_map, mut active_speaker_sources) = setup_speaker_sources(&ranked_owners);
    active_speaker_sources.insert(
        0,
        ActiveSpeakerSource::new(TransportMediaId::new(100), Instant::now()),
    );
    let active_speaker_facts = active_speaker_facts(
        |media_id| media_to_user_map.get(&media_id).cloned(),
        &active_speaker_sources,
    );
    assert_eq!(
        active_speaker_facts.desired_featured_user_id,
        Some(UserId::from(2))
    );
}
