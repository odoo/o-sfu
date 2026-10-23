use serde_json::{Value, json};

use super::*;

const RTP: &[u8] = &[0x80, 96, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1];
const RTCP: &[u8] = &[0x80, 200, 0, 1, 0, 0, 0, 1];
const STUN: &[u8] = &[
    0, 1, 0, 0, 0x21, 0x12, 0xa4, 0x42, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];
const DTLS: &[u8] = &[22, 0xfe, 0xfd, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

fn rule(packet: &str, effects: &Value) -> Value {
    json!({"direction": "client_to_server", "packet": packet, "effects": effects})
}

fn config(rules: &[Value]) -> crate::Result<Config> {
    let config: Config = serde_json::from_value(json!({"rules": rules}))?;
    config.validate()?;
    Ok(config)
}

#[test]
fn schema_defaults_and_rejects_ambiguous_input() -> crate::Result<()> {
    let empty: Config = serde_json::from_str("{}")?;
    assert!(empty.rules.is_empty());
    let selected = config(&[rule("rtp", &json!(["drop"]))])?;
    let mut policy = Policy::new(&selected);
    assert_eq!(policy.apply(Direction::ClientToServer, RTP).matched, [0]);
    let defaults = config(&[rule("rtp", &json!([{"duplicate":{"copies":1}}]))])?;
    assert_eq!(
        Policy::new(&defaults)
            .apply(Direction::ClientToServer, RTP)
            .delays,
        [Duration::ZERO; 2]
    );
    for invalid in [
        json!({"extra": true}),
        json!({"rules": [{"direction":"client_to_server", "packet":"rtp", "effects":[{"duplicate":{"copies":0}}]}]}),
        json!({"rules": [{"direction":"client_to_server", "effects":["drop"]}]}),
        json!({"rules": [{"direction":"client_to_server", "packet":"any", "effects":["drop"]}]}),
        json!({"rules": [{"direction":"client_to_server", "packet":"rtp", "every":0,"effects":["drop"]}]}),
        json!({"rules": [{"direction":"client_to_server", "packet":"rtp", "limit":0,"effects":["drop"]}]}),
        json!({"rules": [{"direction":"client_to_server", "packet":"rtp", "effects":[{"delay":{"ms":1,"extra":true}}]}]}),
    ] {
        assert!(serde_json::from_value::<Config>(invalid).is_err());
    }
    Ok(())
}

#[test]
fn effects_compose_across_rules_and_each_current_copy() -> crate::Result<()> {
    let config = config(&[
        rule(
            "rtp",
            &json!([{"delay":{"ms":10}}, {"duplicate":{"copies":1,"spacing_ms":20}}]),
        ),
        rule(
            "rtp",
            &json!([{"duplicate":{"copies":1,"spacing_ms":5}}, {"delay":{"ms":7}}]),
        ),
    ])?;
    let delivery = Policy::new(&config).apply(Direction::ClientToServer, RTP);
    assert_eq!(delivery.matched, [0, 1]);
    assert_eq!(delivery.delays, [17, 22, 37, 42].map(Duration::from_millis));
    Ok(())
}

#[test]
fn drop_preserves_later_rule_selection_and_consumption() -> crate::Result<()> {
    let mut drop = rule("rtp", &json!(["drop"]));
    drop["limit"] = json!(1);
    let mut duplicate = rule("rtp", &json!([{"duplicate":{"copies":1,"spacing_ms":0}}]));
    duplicate["limit"] = json!(1);
    let mut policy = Policy::new(&config(&[drop, duplicate])?);
    let first = policy.apply(Direction::ClientToServer, RTP);
    assert_eq!(first.matched, [0, 1]);
    assert!(first.delays.is_empty());
    let second = policy.apply(Direction::ClientToServer, RTP);
    assert!(second.matched.is_empty());
    assert_eq!(second.delays, [Duration::ZERO]);
    Ok(())
}

#[test]
fn selectors_count_only_eligible_packets_and_are_per_flow() -> crate::Result<()> {
    let mut selected = rule("rtp", &json!(["drop"]));
    selected["skip"] = json!(1);
    selected["every"] = json!(2);
    selected["limit"] = json!(2);
    let config = config(&[selected])?;
    let mut policy = Policy::new(&config);
    let mut second_flow = Policy::new(&config);
    for ordinal in 1..=7 {
        assert!(
            policy
                .apply(Direction::ServerToClient, RTP)
                .matched
                .is_empty()
        );
        assert!(
            policy
                .apply(Direction::ClientToServer, RTCP)
                .matched
                .is_empty()
        );
        assert!(
            policy
                .apply(Direction::ClientToServer, &[0x80])
                .matched
                .is_empty()
        );
        let delivery = policy.apply(Direction::ClientToServer, RTP);
        assert_eq!(!delivery.matched.is_empty(), matches!(ordinal, 3 | 5));
    }
    assert!(
        second_flow
            .apply(Direction::ClientToServer, RTP)
            .matched
            .is_empty()
    );
    assert!(
        second_flow
            .apply(Direction::ClientToServer, RTP)
            .matched
            .is_empty()
    );
    assert_eq!(
        second_flow.apply(Direction::ClientToServer, RTP).matched,
        [0]
    );
    Ok(())
}

#[test]
fn earlier_rules_do_not_share_ordinal_state() -> crate::Result<()> {
    let mut early = rule("rtp", &json!(["drop"]));
    early["every"] = json!(2);
    let mut later = rule("rtp", &json!([{"delay":{"ms":1}}]));
    later["every"] = json!(3);
    let mut policy = Policy::new(&config(&[early, later])?);
    for ordinal in 1..=6 {
        let delivery = policy.apply(Direction::ClientToServer, RTP);
        let expected: Vec<usize> = [(0, 2), (1, 3)]
            .into_iter()
            .filter_map(|(index, every)| (ordinal % every == 0).then_some(index))
            .collect();
        assert_eq!(delivery.matched, expected);
    }
    Ok(())
}

#[test]
fn classification_preserves_control_traffic_for_media_rules() -> crate::Result<()> {
    let mut selected = rule("rtp", &json!(["drop"]));
    selected["skip"] = json!(1);
    let mut policy = Policy::new(&config(&[selected])?);
    for packet in [RTCP, STUN, DTLS, &[], &[64, 0], &[0x80], &[0x8f, 96, 0, 0]] {
        let delivery = policy.apply(Direction::ClientToServer, packet);
        assert!(delivery.matched.is_empty());
        assert_eq!(delivery.delays, [Duration::ZERO]);
    }
    let first = policy.apply(Direction::ClientToServer, RTP);
    assert!(first.matched.is_empty());
    assert_eq!(first.delays, [Duration::ZERO]);
    let second = policy.apply(Direction::ClientToServer, RTP);
    assert_eq!(second.matched, [0]);
    assert!(second.delays.is_empty());
    Ok(())
}

#[test]
fn validates_composition_limits_and_independent_packet_classes() {
    let duplicate = rule("rtp", &json!([{"duplicate":{"copies":1,"spacing_ms":0}}]));
    assert!(config(&vec![duplicate.clone(); 3]).is_ok());
    assert!(config(&vec![duplicate; 4]).is_err());
    assert!(config(&vec![rule("rtp", &json!([{"delay":{"ms":2500}}])); 2]).is_ok());
    assert!(config(&vec![rule("rtp", &json!([{"delay":{"ms":2501}}])); 2]).is_err());
    assert!(
        config(&[
            rule("rtp", &json!([{"delay":{"ms":5000}}])),
            rule("rtcp", &json!([{"delay":{"ms":5000}}])),
        ])
        .is_ok()
    );
    assert!(
        config(&[
            rule("rtp", &json!([{"delay":{"ms":3000}}])),
            rule("rtp", &json!(["drop"])),
            rule("rtp", &json!([{"delay":{"ms":3000}}])),
        ])
        .is_err()
    );
    for effects in [
        json!([]),
        json!(vec!["drop"; 9]),
        json!([{"delay":{"ms":u64::MAX}}]),
        json!([{"duplicate":{"copies":u64::MAX,"spacing_ms":u64::MAX}}]),
        json!([{"duplicate":{"copies":7,"spacing_ms":715}}]),
    ] {
        assert!(config(&[rule("rtp", &effects)]).is_err());
    }
    assert!(config(&vec![rule("rtp", &json!(["drop"])); 32]).is_ok());
    assert!(config(&vec![rule("rtp", &json!(["drop"])); 33]).is_err());
}

#[test]
fn selector_countdowns_handle_maximum_configured_values() -> crate::Result<()> {
    for field in ["skip", "every"] {
        let mut selected = rule("rtp", &json!(["drop"]));
        selected[field] = json!(u64::MAX);
        let mut policy = Policy::new(&config(&[selected])?);
        for _ in 0..3 {
            assert!(
                policy
                    .apply(Direction::ClientToServer, RTP)
                    .matched
                    .is_empty()
            );
        }
    }
    Ok(())
}
