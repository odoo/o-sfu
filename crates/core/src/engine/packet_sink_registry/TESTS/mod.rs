use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use crate::engine::{
    RoomInstanceId, UserId,
    media_transport::{
        TransportMediaId, TransportSessionKey,
        test_support::{ForwardedPacket, sample_forwarded_packet, test_transport_session_key},
    },
    metrics::RtpForwardDestinationKind,
    packet_sink_registry::{PacketSink, PacketSinkRouteCache, RoomPacketSinkRegistry},
};

struct CountingSink {
    frames: AtomicUsize,
}

impl CountingSink {
    fn new() -> Self {
        Self {
            frames: AtomicUsize::new(0),
        }
    }
}

impl PacketSink for CountingSink {
    fn record_packet(
        &self,
        _session_key: &TransportSessionKey,
        _transport_media_id: TransportMediaId,
        _received_at: Instant,
        _payload: &[u8],
    ) {
        self.frames.fetch_add(1, Ordering::Relaxed);
    }
}

fn register_recording_sink<T>(
    registry: &RoomPacketSinkRegistry,
    room_instance_id: RoomInstanceId,
    sink: Arc<T>,
) where
    T: PacketSink + 'static,
{
    registry.register_room(room_instance_id, sink, RtpForwardDestinationKind::Recording);
}

fn write_packet(
    registry: &RoomPacketSinkRegistry,
    packet: &ForwardedPacket,
    transport_media_id: TransportMediaId,
) {
    let Some(src_key) = packet.stable_src_key() else {
        return;
    };
    let Some(sink) = registry.sink_for_room(src_key.room_instance_id()) else {
        return;
    };
    sink.record_packet(
        src_key,
        transport_media_id,
        packet.received_at(),
        packet.payload(),
    );
}

#[test]
fn packet_sink_registry_exposes_the_active_room_sink_for_forwarding_destinations() {
    let registry = RoomPacketSinkRegistry::default();
    let sink = Arc::new(CountingSink::new());

    assert!(
        registry
            .sink_for_room(RoomInstanceId::from_raw(10))
            .is_none()
    );
    register_recording_sink(
        &registry,
        RoomInstanceId::from_raw(10),
        Arc::<CountingSink>::clone(&sink),
    );

    assert!(
        registry
            .sink_for_room(RoomInstanceId::from_raw(10))
            .is_some()
    );
    assert!(
        registry
            .sink_for_room(RoomInstanceId::from_raw(11))
            .is_none()
    );
}

#[test]
fn packet_sink_route_cache_refreshes_after_registry_changes() {
    let registry = RoomPacketSinkRegistry::default();
    let sink = Arc::new(CountingSink::new());
    let mut cache = PacketSinkRouteCache::default();
    let room_id = RoomInstanceId::from_raw(13);

    cache.refresh_from(&registry);
    assert!(cache.sink_for_room(room_id).is_none());

    register_recording_sink(&registry, room_id, Arc::<CountingSink>::clone(&sink));

    assert!(cache.sink_for_room(room_id).is_none());
    cache.refresh_from(&registry);
    assert!(cache.sink_for_room(room_id).is_some());

    registry.unregister_room(room_id);

    assert!(cache.sink_for_room(room_id).is_some());
    cache.refresh_from(&registry);
    assert!(cache.sink_for_room(room_id).is_none());
}

#[test]
fn packet_sink_registry_keeps_multiple_rooms_active_at_once() {
    let registry = RoomPacketSinkRegistry::default();
    let first_sink = Arc::new(CountingSink::new());
    let second_sink = Arc::new(CountingSink::new());
    let first_packet = sample_forwarded_packet(
        test_transport_session_key(10, 0, 1, UserId::Integer(1)),
        "aud-up",
        b"first",
    );
    let second_packet = sample_forwarded_packet(
        test_transport_session_key(11, 0, 1, UserId::Integer(2)),
        "aud-up",
        b"second",
    );

    register_recording_sink(
        &registry,
        RoomInstanceId::from_raw(10),
        Arc::<CountingSink>::clone(&first_sink),
    );
    register_recording_sink(
        &registry,
        RoomInstanceId::from_raw(11),
        Arc::<CountingSink>::clone(&second_sink),
    );
    write_packet(&registry, &first_packet, TransportMediaId::new(3));
    write_packet(&registry, &second_packet, TransportMediaId::new(4));

    assert_eq!(first_sink.frames.load(Ordering::Relaxed), 1);
    assert_eq!(second_sink.frames.load(Ordering::Relaxed), 1);
}
