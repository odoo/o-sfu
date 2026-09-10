use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};

use super::{
    RoomInstanceId,
    media_transport::{TransportMediaId, TransportSessionKey},
    metrics::RtpForwardDestinationKind,
    sync::{read_unpoisoned, write_unpoisoned},
};

#[cfg(test)]
#[expect(non_snake_case, reason = "test modules map to local TESTS directories")]
#[path = "packet_sink_registry/TESTS/mod.rs"]
mod TESTS;

/// Observes origin-side RTP payloads routed to a room packet sink.
///
/// Packet gates do not filter this source-side stream. Relayed packets are not
/// observed again.
pub trait PacketSink: Send + Sync {
    /// Records one source RTP payload.
    ///
    /// `session_key` and `transport_media_id` identify the source transport
    /// media. The RTC engine invokes this synchronously on a packet worker.
    /// Different workers may invoke the same sink concurrently, so
    /// implementations must return promptly and must not perform blocking I/O.
    fn record_packet(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        received_at: Instant,
        payload: &[u8],
    );
}

#[derive(Clone)]
pub struct RegisteredPacketSink {
    sink: Arc<dyn PacketSink>,
    forward_destination_kind: RtpForwardDestinationKind,
}

impl RegisteredPacketSink {
    pub fn new(
        sink: Arc<dyn PacketSink>,
        forward_destination_kind: RtpForwardDestinationKind,
    ) -> Self {
        Self {
            sink,
            forward_destination_kind,
        }
    }

    pub fn record_packet(
        &self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        received_at: Instant,
        payload: &[u8],
    ) {
        self.sink
            .record_packet(session_key, transport_media_id, received_at, payload);
    }

    #[must_use]
    pub const fn forward_destination_kind(&self) -> RtpForwardDestinationKind {
        self.forward_destination_kind
    }
}

impl fmt::Debug for RegisteredPacketSink {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredPacketSink")
            .field("forward_destination_kind", &self.forward_destination_kind)
            .finish_non_exhaustive()
    }
}

pub struct RoomPacketSinkRegistry {
    any_active: AtomicBool,
    generation: AtomicU64,
    active_rooms: RwLock<HashMap<RoomInstanceId, RegisteredPacketSink>>,
}

impl Default for RoomPacketSinkRegistry {
    fn default() -> Self {
        Self {
            any_active: AtomicBool::new(false),
            generation: AtomicU64::new(0),
            active_rooms: RwLock::new(HashMap::new()),
        }
    }
}

#[derive(Default)]
pub struct PacketSinkRouteCache {
    generation: u64,
    active_rooms: HashMap<RoomInstanceId, RegisteredPacketSink>,
}

impl PacketSinkRouteCache {
    /// Refreshes the cached room routes to one registry generation.
    ///
    /// Later registry changes remain invisible through [`Self::sink_for_room`]
    /// until `refresh_from` runs again.
    pub fn refresh_from(&mut self, registry: &RoomPacketSinkRegistry) {
        let generation = registry.generation();
        if self.generation == generation {
            return;
        }
        let snapshot = registry.snapshot();
        self.generation = snapshot.generation;
        self.active_rooms = snapshot.active_rooms;
    }

    #[inline]
    pub fn sink_for_room(&self, room_instance_id: RoomInstanceId) -> Option<RegisteredPacketSink> {
        self.active_rooms.get(&room_instance_id).cloned()
    }
}

struct PacketSinkRegistrySnapshot {
    generation: u64,
    active_rooms: HashMap<RoomInstanceId, RegisteredPacketSink>,
}

impl RoomPacketSinkRegistry {
    #[inline]
    pub fn sink_for_room(&self, room_instance_id: RoomInstanceId) -> Option<RegisteredPacketSink> {
        if !self.any_active.load(Ordering::Acquire) {
            return None;
        }
        read_unpoisoned(&self.active_rooms)
            .get(&room_instance_id)
            .cloned()
    }

    fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    fn snapshot(&self) -> PacketSinkRegistrySnapshot {
        // Both writers advance the generation before releasing this map's guard.
        let active_rooms = read_unpoisoned(&self.active_rooms);
        PacketSinkRegistrySnapshot {
            generation: self.generation(),
            active_rooms: active_rooms.clone(),
        }
    }

    /// Registers `sink` for `room_instance_id`, replacing the current entry.
    ///
    /// Previously cloned [`RegisteredPacketSink`] handles are not revoked.
    pub fn register_room(
        &self,
        room_instance_id: RoomInstanceId,
        sink: Arc<dyn PacketSink>,
        forward_destination_kind: RtpForwardDestinationKind,
    ) {
        let mut active_rooms = write_unpoisoned(&self.active_rooms);
        active_rooms.insert(
            room_instance_id,
            RegisteredPacketSink::new(sink, forward_destination_kind),
        );
        self.any_active.store(true, Ordering::Release);
        self.generation.fetch_add(1, Ordering::AcqRel);
        drop(active_rooms);
    }

    /// Removes the current entry for `room_instance_id`.
    ///
    /// Cached or cloned sink handles may still receive packets after
    /// `unregister_room` returns.
    pub fn unregister_room(&self, room_instance_id: RoomInstanceId) {
        let mut active_rooms = write_unpoisoned(&self.active_rooms);
        active_rooms.remove(&room_instance_id);
        self.any_active
            .store(!active_rooms.is_empty(), Ordering::Release);
        self.generation.fetch_add(1, Ordering::AcqRel);
        drop(active_rooms);
    }

    fn active_room_count(&self) -> usize {
        read_unpoisoned(&self.active_rooms).len()
    }
}

impl fmt::Debug for RoomPacketSinkRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RoomPacketSinkRegistry")
            .field("any_active", &self.any_active.load(Ordering::Relaxed))
            .field("active_room_count", &self.active_room_count())
            .finish_non_exhaustive()
    }
}
