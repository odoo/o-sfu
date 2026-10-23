//! Per-flow selectors and ordered delivery effects without transport state.
//!
//! Rules require a direction and packet kind. After `skip` eligible datagrams,
//! `every` selects each Nth datagram and `limit` caps selected applications.
//! Effects compose left to right across selected rules. Duplicate `copies` are
//! additional deliveries of each current delivery, spaced by `spacing_ms`.
//! Drop removes all deliveries without preventing later selectors from advancing.

use std::{num::NonZeroU64, time::Duration};

use o_sfu_rfc::webrtc::{self as rfc_webrtc, DatagramKind};
use serde::Deserialize;

use crate::Direction;

const MAX_RULES: usize = 32;
const MAX_EFFECTS: usize = 8;
const MAX_DELIVERIES: u64 = 8;
const MAX_DELAY_MS: u64 = 5_000;

#[derive(Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub rules: Vec<Rule>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    direction: Direction,
    packet: DatagramKind,
    #[serde(default)]
    skip: u64,
    #[serde(default = "one")]
    every: NonZeroU64,
    limit: Option<NonZeroU64>,
    effects: Vec<Effect>,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum Effect {
    Drop,
    Delay {
        ms: u64,
    },
    Duplicate {
        copies: NonZeroU64,
        #[serde(default)]
        spacing_ms: u64,
    },
}

impl Config {
    /// Returns `BoxError` for excessive rules, empty or excessive effect lists,
    /// more than eight expanded deliveries or composed delays over five seconds.
    pub fn validate(&self) -> crate::Result<()> {
        if self.rules.len() > MAX_RULES {
            return Err(format!("UDP policy allows at most {MAX_RULES} rules").into());
        }
        for rule in &self.rules {
            if rule.effects.is_empty() || rule.effects.len() > MAX_EFFECTS {
                return Err(
                    format!("UDP rules require between 1 and {MAX_EFFECTS} effects").into(),
                );
            }
            for effect in &rule.effects {
                match effect {
                    Effect::Delay { ms } if *ms > MAX_DELAY_MS => {
                        return Err(format!("UDP delay exceeds {MAX_DELAY_MS} milliseconds").into());
                    }
                    Effect::Duplicate { copies, spacing_ms }
                        if copies.get() >= MAX_DELIVERIES || *spacing_ms > MAX_DELAY_MS =>
                    {
                        return Err("UDP duplication exceeds delivery or delay bounds".into());
                    }
                    _ => {}
                }
            }
        }
        for direction in [Direction::ClientToServer, Direction::ServerToClient] {
            for packet in [
                DatagramKind::Rtp,
                DatagramKind::Rtcp,
                DatagramKind::Stun,
                DatagramKind::Dtls,
            ] {
                let mut count = 1;
                let mut delay = 0;
                for rule in self
                    .rules
                    .iter()
                    .filter(|rule| rule.direction == direction && rule.packet == packet)
                {
                    let mut selected_count = count;
                    let mut selected_delay = delay;
                    // Raw operands are bounded above and each intermediate
                    // result is checked before another multiplication or addition.
                    for effect in &rule.effects {
                        match effect {
                            Effect::Drop => {
                                selected_count = 0;
                                selected_delay = 0;
                            }
                            Effect::Delay { ms } if selected_count != 0 => {
                                selected_delay += ms;
                            }
                            Effect::Duplicate { copies, spacing_ms } if selected_count != 0 => {
                                selected_count *= copies.get() + 1;
                                selected_delay += copies.get() * spacing_ms;
                            }
                            _ => {}
                        }
                        if selected_count > MAX_DELIVERIES || selected_delay > MAX_DELAY_MS {
                            return Err(
                                format!(
                                    "composed UDP effects exceed {MAX_DELIVERIES} deliveries or {MAX_DELAY_MS} milliseconds"
                                ).into(),
                            );
                        }
                    }
                    // Selectors can skip any rule, including a drop. Preserve
                    // the bypass path when bounding subsequent compositions.
                    count = count.max(selected_count);
                    delay = delay.max(selected_delay);
                }
            }
        }
        Ok(())
    }
}

fn one() -> NonZeroU64 {
    NonZeroU64::MIN
}

pub struct Delivery {
    /// Delays relative to receipt, ordered from earliest to latest.
    pub delays: Vec<Duration>,
    /// Selected rule indexes, including rules applied after a drop.
    pub matched: Vec<usize>,
}

/// One browser UDP flow, including both directions. Never share between flows.
pub struct Policy {
    rules: Vec<Selector>,
}

struct Selector {
    rule: Rule,
    skip: u64,
    until_next: u64,
    remaining: Option<u64>,
}

impl Policy {
    /// Requires a configuration accepted by [`Config::validate`].
    pub fn new(config: &Config) -> Self {
        Self {
            rules: config
                .rules
                .iter()
                .map(|rule| Selector {
                    rule: rule.clone(),
                    skip: rule.skip,
                    until_next: rule.every.get(),
                    remaining: rule.limit.map(NonZeroU64::get),
                })
                .collect(),
        }
    }

    pub fn apply(&mut self, direction: Direction, packet: &[u8]) -> Delivery {
        let mut delivery = Delivery {
            delays: vec![Duration::ZERO],
            matched: Vec::new(),
        };
        let Some(packet) = rfc_webrtc::classify_datagram(packet) else {
            return delivery;
        };
        for (index, selector) in self.rules.iter_mut().enumerate() {
            if selector.rule.direction != direction
                || selector.rule.packet != packet
                || !selector.select()
            {
                continue;
            }
            delivery.matched.push(index);
            for effect in &selector.rule.effects {
                match effect {
                    Effect::Drop => delivery.delays.clear(),
                    Effect::Delay { ms } => {
                        for delay in &mut delivery.delays {
                            *delay += Duration::from_millis(*ms);
                        }
                    }
                    Effect::Duplicate { copies, spacing_ms } => {
                        let originals = delivery.delays.len();
                        for delivery_index in 0..originals {
                            if let Some(delay) = delivery.delays.get(delivery_index).copied() {
                                for copy in 1..=copies.get() {
                                    delivery
                                        .delays
                                        .push(delay + Duration::from_millis(copy * spacing_ms));
                                }
                            }
                        }
                    }
                }
            }
        }
        delivery.delays.sort_unstable();
        delivery
    }
}

impl Selector {
    fn select(&mut self) -> bool {
        if self.skip != 0 {
            self.skip -= 1;
            return false;
        }
        self.until_next -= 1;
        if self.until_next != 0 {
            return false;
        }
        self.until_next = self.rule.every.get();
        if let Some(remaining) = &mut self.remaining {
            if *remaining == 0 {
                return false;
            }
            *remaining -= 1;
        }
        true
    }
}

#[cfg(test)]
#[path = "TESTS/policy.rs"]
mod tests;
