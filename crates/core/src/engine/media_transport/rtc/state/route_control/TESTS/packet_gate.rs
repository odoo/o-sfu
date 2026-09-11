use super::{PacketLayerGate, aggregate_packet_gates, intersect_packet_gates};

#[test]
fn packet_gate_only_forwards_the_selected_rid() {
    let gate = PacketLayerGate::Rid("hi".into());

    assert!(gate.permits(Some("hi".into())));
    assert!(!gate.permits(Some("lo".into())));
    assert!(!gate.permits(None));
}

#[test]
fn aggregate_packet_gates_prefers_a_shared_selected_rid() {
    assert_eq!(
        aggregate_packet_gates([
            &PacketLayerGate::Rid("hi".into()),
            &PacketLayerGate::Rid("hi".into()),
            &PacketLayerGate::Block,
        ]),
        Some(PacketLayerGate::Rid("hi".into()))
    );
}

#[test]
fn aggregate_packet_gates_reopens_when_routes_disagree() {
    assert_eq!(
        aggregate_packet_gates([
            &PacketLayerGate::Rid("hi".into()),
            &PacketLayerGate::Rid("lo".into()),
        ]),
        Some(PacketLayerGate::Open)
    );
    assert_eq!(
        aggregate_packet_gates([&PacketLayerGate::Rid("hi".into()), &PacketLayerGate::Open]),
        Some(PacketLayerGate::Open)
    );
}

#[test]
fn intersect_packet_gates_preserves_gate_algebra() {
    let hi = Some(PacketLayerGate::Rid("hi".into()));
    let lo = Some(PacketLayerGate::Rid("lo".into()));
    let open = Some(PacketLayerGate::Open);
    let block = Some(PacketLayerGate::Block);

    for (first, second, expected) in [
        (None, None, None),
        (None, hi, hi),
        (hi, None, hi),
        (open, hi, hi),
        (hi, open, hi),
        (block, hi, block),
        (hi, block, block),
        (hi, hi, hi),
        (hi, lo, block),
        (lo, hi, block),
    ] {
        assert_eq!(intersect_packet_gates(first, second), expected);
    }
}
