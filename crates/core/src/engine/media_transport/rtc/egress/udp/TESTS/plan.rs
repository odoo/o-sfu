use str0m::net::{Protocol, Transmit};

use super::plan_messages;

#[test]
fn ordinary_plan_preserves_datagram_count_and_storage() {
    let mut transmits: Vec<_> = [0, 3, 7]
        .into_iter()
        .map(|length| Transmit {
            proto: Protocol::Udp,
            source: ([127, 0, 0, 1], 40_000).into(),
            destination: ([127, 0, 0, 1], 40_001).into(),
            contents: vec![42; length].into(),
        })
        .collect();
    let mut messages = Vec::with_capacity(3);
    let capacity = messages.capacity();
    plan_messages(&transmits, &mut messages);
    let counts: Vec<_> = messages
        .iter()
        .map(|message| message.datagrams.get())
        .collect();
    assert_eq!(counts, vec![1, 1, 1]);
    transmits.truncate(1);
    plan_messages(&transmits, &mut messages);
    assert_eq!(messages.len(), 1);
    assert_eq!(messages.capacity(), capacity);
    plan_messages(&[], &mut messages);
    assert!(messages.is_empty());
    assert_eq!(messages.capacity(), capacity);
}
