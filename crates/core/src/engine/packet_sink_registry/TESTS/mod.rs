use std::{sync::Arc, time::Instant};

use crate::engine::{
    RoomInstanceId,
    media_transport::{TransportMediaId, TransportSessionKey},
    metrics::RtpForwardDestinationKind,
    packet_sink_registry::{
        PacketSink, PacketSinkKind, PacketSinkRouteCache, RoomPacketSinkRegistry,
    },
};

struct IgnoringSink;

impl PacketSink for IgnoringSink {
    fn record_packet(
        &self,
        _session_key: &TransportSessionKey,
        _transport_media_id: TransportMediaId,
        _received_at: Instant,
        _payload: &[u8],
    ) {
    }
}

#[test]
fn packet_sink_registry_exposes_the_active_room_sink_for_forwarding_destinations() {
    let registry = RoomPacketSinkRegistry::default();
    let sink = Arc::new(IgnoringSink);
    let mut cache = PacketSinkRouteCache::default();

    cache.refresh_from(&registry);
    assert!(cache.sink_for_room(RoomInstanceId::from_raw(10)).is_none());
    for (kind, expected) in [
        (
            PacketSinkKind::Recording,
            RtpForwardDestinationKind::Recording,
        ),
        (
            PacketSinkKind::IntraNodeRelay,
            RtpForwardDestinationKind::IntraNodeRelay,
        ),
    ] {
        registry.register_room(
            RoomInstanceId::from_raw(10),
            Arc::<IgnoringSink>::clone(&sink),
            kind,
        );
        cache.refresh_from(&registry);
        assert_eq!(
            cache
                .sink_for_room(RoomInstanceId::from_raw(10))
                .map(|sink| sink.forward_destination_kind()),
            Some(expected),
        );
    }
    assert!(cache.sink_for_room(RoomInstanceId::from_raw(11)).is_none());
}

#[test]
fn packet_sink_route_cache_refreshes_after_registry_changes() {
    let registry = RoomPacketSinkRegistry::default();
    let sink = Arc::new(IgnoringSink);
    let mut cache = PacketSinkRouteCache::default();
    let room_id = RoomInstanceId::from_raw(13);

    cache.refresh_from(&registry);
    assert!(cache.sink_for_room(room_id).is_none());

    registry.register_room(room_id, sink, PacketSinkKind::Recording);

    assert!(cache.sink_for_room(room_id).is_none());
    cache.refresh_from(&registry);
    assert!(cache.sink_for_room(room_id).is_some());

    registry.unregister_room(room_id);

    assert!(cache.sink_for_room(room_id).is_some());
    cache.refresh_from(&registry);
    assert!(cache.sink_for_room(room_id).is_none());
}
