//! worker-local session media ownership indexes.
//!
//! the registry owns session-scoped producer and consumer media lookup
//! source route entries, remote-source registrations
//! and decoder-refresh classifiers live in `source_route`

use std::{
    collections::{BTreeMap, BTreeSet},
    mem,
    time::Instant,
};

use o_sfu_router::rtp::MediaStream;
use str0m::{
    media::{Mid, Rid},
    rtp::Ssrc,
};

use super::{
    super::{codec, packet_loop::forwarded_packet::ForwardedPacketSource},
    PacketLoopState,
    route_table::RouteTable,
    slots::SessionHandle,
    source_route::DestinationKeyframeTarget,
};
use crate::engine::{
    RoomInstanceId,
    media_transport::{TransportMediaId, TransportSessionKey},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in super::super) enum RegisteredMediaHandle {
    Producer {
        session_key: TransportSessionKey,
        mid: Mid,
    },
    Consumer {
        session_key: TransportSessionKey,
        mid: Mid,
        src_media: TransportMediaId,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in super::super) struct ConsumerKeyframeTarget {
    pub(in super::super) src_media: TransportMediaId,
    pub(in super::super) rid: Option<Rid>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ConsumerMidBinding {
    consumer_media: TransportMediaId,
    src_media: TransportMediaId,
    dst_idx: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProducerSsrcBinding {
    transport_media_id: TransportMediaId,
    rid: Option<Rid>,
    role: ProducerSsrcRole,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProducerSsrcRole {
    Primary,
    Repair,
}

/// One authenticated str0m receive stream after RTX normalization.
///
/// The exact stream selected by `Event::RtpPacket` supplies both SSRCs. RID
/// resolves against the current negotiated binding before admission because
/// str0m can retain an earlier SDP-declared RID for the same primary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerStreamBinding {
    pub rid: Option<Rid>,
    pub primary: Ssrc,
    pub repair: Option<Ssrc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerSsrcUpdate {
    Unchanged,
    Learned,
    Replaced,
    Rejected,
}

#[derive(Debug, Clone)]
struct ProducerEncoding {
    rid: Option<Rid>,
    primary: Option<Ssrc>,
    repair: Option<Ssrc>,
    previous_primary: Option<Ssrc>,
}

impl RegisteredMediaHandle {
    pub(in super::super) fn session_key(&self) -> &TransportSessionKey {
        match self {
            Self::Producer { session_key, .. } | Self::Consumer { session_key, .. } => session_key,
        }
    }

    pub(in super::super) fn mid(&self) -> Mid {
        match self {
            Self::Producer { mid, .. } | Self::Consumer { mid, .. } => *mid,
        }
    }
}

/// Registration and receive-stream state for one media id.
#[derive(Debug, Clone)]
pub(in super::super) struct RegisteredMedia {
    handle: RegisteredMediaHandle,
    producer_encodings: Vec<ProducerEncoding>,
}

impl RegisteredMedia {
    fn is_producer_for(&self, session_key: &TransportSessionKey) -> bool {
        matches!(
            &self.handle,
            RegisteredMediaHandle::Producer { session_key: owner, .. } if owner == session_key
        )
    }
}

pub(in super::super) type MediaStore = BTreeMap<TransportMediaId, RegisteredMedia>;
pub(in super::super) type SessionMediaRegistry = BTreeMap<TransportSessionKey, SessionMediaLookup>;

#[derive(Debug, Default, Clone)]
pub(in super::super) struct SessionMediaLookup {
    owned_media: Vec<TransportMediaId>,
    producer_mids: TinyLookup<Mid, TransportMediaId>,
    producer_ssrcs: TinyLookup<Ssrc, ProducerSsrcBinding>,
    consumer_mids: TinyLookup<Mid, ConsumerMidBinding>,
}

impl SessionMediaLookup {
    /// Repairs a destination slot only while the MID still names the same route.
    pub(in super::super) fn set_consumer_dst_idx(
        &mut self,
        consumer_mid: Mid,
        consumer_media: TransportMediaId,
        src_media: TransportMediaId,
        dst_idx: Option<usize>,
    ) {
        let Some(binding) = self.consumer_mids.get_mut(&consumer_mid) else {
            return;
        };
        if binding.consumer_media == consumer_media && binding.src_media == src_media {
            binding.dst_idx = dst_idx;
        }
    }

    fn insert_owned_media(&mut self, transport_media_id: TransportMediaId) {
        if !self.owned_media.contains(&transport_media_id) {
            self.owned_media.push(transport_media_id);
        }
    }

    fn remove_owned_media(&mut self, transport_media_id: TransportMediaId) {
        if let Some(position) = self
            .owned_media
            .iter()
            .position(|id| *id == transport_media_id)
        {
            self.owned_media.swap_remove(position);
        }
    }

    fn is_empty(&self) -> bool {
        self.owned_media.is_empty()
            && self.producer_mids.is_empty()
            && self.producer_ssrcs.is_empty()
            && self.consumer_mids.is_empty()
    }
}

fn bind_producer_stream(
    mid_registry: &mut MediaStore,
    lookup: &mut SessionMediaLookup,
    routes: &mut RouteTable,
    session_key: &TransportSessionKey,
    transport_media_id: TransportMediaId,
    binding: ProducerStreamBinding,
) -> ProducerSsrcUpdate {
    let Some(registered) = mid_registry
        .get_mut(&transport_media_id)
        .filter(|registered| registered.is_producer_for(session_key))
    else {
        return ProducerSsrcUpdate::Rejected;
    };
    if binding.repair == Some(binding.primary) {
        return ProducerSsrcUpdate::Rejected;
    }
    let encodings = &mut registered.producer_encodings;
    let Some(encoding) = encodings
        .iter_mut()
        .find(|encoding| encoding.rid == binding.rid)
    else {
        return ProducerSsrcUpdate::Rejected;
    };
    if encoding.primary == Some(binding.primary) && encoding.repair == binding.repair {
        return ProducerSsrcUpdate::Unchanged;
    }
    // str0m 0.23.1 can recreate a refused preceding SSRC in
    // map_dynamic_finish. Keep its intended anti-flap rule even when the
    // delayed packet is emitted as a second authenticated receive stream.
    // https://docs.rs/str0m/0.23.1/src/str0m/streams/mod.rs.html
    if encoding.previous_primary == Some(binding.primary) {
        return ProducerSsrcUpdate::Rejected;
    }
    for (ssrc, role) in [
        (Some(binding.primary), ProducerSsrcRole::Primary),
        (binding.repair, ProducerSsrcRole::Repair),
    ] {
        if let Some(previous) = ssrc.and_then(|ssrc| lookup.producer_ssrcs.get(&ssrc))
            && previous
                != (ProducerSsrcBinding {
                    transport_media_id,
                    rid: binding.rid,
                    role,
                })
        {
            return ProducerSsrcUpdate::Rejected;
        }
    }
    let outcome = match (encoding.primary, encoding.repair) {
        (Some(primary), _) if primary != binding.primary => ProducerSsrcUpdate::Replaced,
        (_, Some(_)) => ProducerSsrcUpdate::Replaced,
        _ => ProducerSsrcUpdate::Learned,
    };
    for previous in [encoding.primary, encoding.repair].into_iter().flatten() {
        lookup.producer_ssrcs.remove(&previous);
    }
    if encoding.primary != Some(binding.primary) {
        encoding.previous_primary = encoding.primary;
    }
    encoding.primary = Some(binding.primary);
    encoding.repair = binding.repair;
    for (ssrc, role) in [
        (Some(binding.primary), ProducerSsrcRole::Primary),
        (binding.repair, ProducerSsrcRole::Repair),
    ] {
        if let Some(ssrc) = ssrc {
            lookup.producer_ssrcs.insert(
                ssrc,
                ProducerSsrcBinding {
                    transport_media_id,
                    rid: binding.rid,
                    role,
                },
            );
        }
    }
    routes.replace_producer_ssrcs(
        transport_media_id,
        encodings
            .iter()
            .flat_map(|encoding| [encoding.primary, encoding.repair])
            .flatten(),
    );
    outcome
}

#[derive(Debug, Clone)]
struct TinyLookup<K, V> {
    entries: Vec<(K, V)>,
}

impl<K, V> Default for TinyLookup<K, V> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl<K: Eq, V: Copy> TinyLookup<K, V> {
    fn insert(&mut self, key: K, value: V) -> Option<V> {
        if let Some((_entry_key, entry_value)) = self
            .entries
            .iter_mut()
            .find(|(entry_key, _value)| entry_key == &key)
        {
            return Some(mem::replace(entry_value, value));
        }
        self.entries.push((key, value));
        None
    }

    fn remove(&mut self, key: &K) {
        if let Some(position) = self
            .entries
            .iter()
            .position(|(entry_key, _value)| entry_key == key)
        {
            self.entries.swap_remove(position);
        }
    }

    fn get(&self, key: &K) -> Option<V> {
        self.entries
            .iter()
            .find_map(|(entry_key, value)| (entry_key == key).then_some(*value))
    }

    fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.entries
            .iter_mut()
            .find_map(|(entry_key, value)| (entry_key == key).then_some(value))
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl PacketLoopState {
    pub(in super::super) fn register_media_handle(
        &mut self,
        handle: RegisteredMediaHandle,
    ) -> TransportMediaId {
        let transport_media_id = TransportMediaId::new(self.next_media_id);
        self.next_media_id = self.next_media_id.saturating_add(1);
        if let RegisteredMediaHandle::Producer { session_key, mid } = &handle {
            let session_lookup = self.session_media.entry(session_key.clone()).or_default();
            session_lookup.insert_owned_media(transport_media_id);
            session_lookup
                .producer_mids
                .insert(*mid, transport_media_id);
            self.routes.register_local_source(transport_media_id);
        } else if let RegisteredMediaHandle::Consumer {
            session_key,
            mid,
            src_media,
        } = &handle
        {
            let session_lookup = self.session_media.entry(session_key.clone()).or_default();
            session_lookup.insert_owned_media(transport_media_id);
            session_lookup.consumer_mids.insert(
                *mid,
                ConsumerMidBinding {
                    consumer_media: transport_media_id,
                    src_media: *src_media,
                    dst_idx: None,
                },
            );
        }
        self.mid_registry.insert(
            transport_media_id,
            RegisteredMedia {
                handle,
                producer_encodings: Vec::new(),
            },
        );
        transport_media_id
    }

    pub(in super::super) fn resolve_mid(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<Mid> {
        self.mid_registry
            .get(&transport_media_id)
            .map(|registered| registered.handle.mid())
    }

    pub(in super::super) fn media_handle(
        &self,
        transport_media_id: TransportMediaId,
    ) -> Option<&RegisteredMediaHandle> {
        self.mid_registry
            .get(&transport_media_id)
            .map(|registered| &registered.handle)
    }

    pub(in super::super) fn producer_media_snapshot(
        &self,
        session_key: &TransportSessionKey,
    ) -> Vec<(TransportMediaId, Mid)> {
        self.session_media
            .get(session_key)
            .map_or_else(Vec::new, |session_lookup| {
                session_lookup
                    .producer_mids
                    .entries
                    .iter()
                    .map(|(mid, transport_media_id)| (*transport_media_id, *mid))
                    .collect()
            })
    }

    pub(in super::super) fn consumer_media_snapshot(
        &self,
        session_key: &TransportSessionKey,
    ) -> Vec<(TransportMediaId, Mid, TransportMediaId)> {
        self.session_media
            .get(session_key)
            .map_or_else(Vec::new, |session_lookup| {
                session_lookup
                    .consumer_mids
                    .entries
                    .iter()
                    .map(|(mid, binding)| (binding.consumer_media, *mid, binding.src_media))
                    .collect()
            })
    }

    pub(in super::super) fn mid_is_shared(
        &self,
        session_key: &TransportSessionKey,
        mid: Mid,
        excluded_transport_media_id: TransportMediaId,
    ) -> bool {
        self.session_media
            .get(session_key)
            .is_some_and(|session_lookup| {
                session_lookup
                    .owned_media
                    .iter()
                    .copied()
                    .any(|transport_media_id| {
                        transport_media_id != excluded_transport_media_id
                            && self
                                .mid_registry
                                .get(&transport_media_id)
                                .is_some_and(|registered| registered.handle.mid() == mid)
                    })
            })
    }

    /// remove one media handle and every dependent reverse index owned by it
    ///
    /// producer removal clears source packet policy, incoming bitrate counters,
    /// decoder-refresh metadata, live RID state and SSRC lookups
    /// Consumer removal clears its MID lookup. Complete route and stream teardown
    /// belongs to [`Self::unregister_media_handle`].
    pub(super) fn remove_media_handle(
        &mut self,
        transport_media_id: TransportMediaId,
    ) -> Option<RegisteredMediaHandle> {
        let handle = self.media_handle(transport_media_id)?.clone();
        let owner_session_key = handle.session_key().clone();
        match &handle {
            RegisteredMediaHandle::Producer { session_key, mid } => {
                if let Some(session_lookup) = self.session_media.get_mut(session_key) {
                    session_lookup.remove_owned_media(transport_media_id);
                    session_lookup.producer_mids.remove(mid);
                }
                self.clear_producer_ssrcs(session_key, transport_media_id);
                self.routes.unregister_local_source(transport_media_id);
                self.remove_incoming_bitrate_counter(transport_media_id);
            }
            RegisteredMediaHandle::Consumer {
                session_key, mid, ..
            } => {
                if let Some(session_lookup) = self.session_media.get_mut(session_key) {
                    session_lookup.remove_owned_media(transport_media_id);
                    session_lookup.consumer_mids.remove(mid);
                }
            }
        }
        self.mid_registry.remove(&transport_media_id);
        self.prune_empty_session_media(&owner_session_key);
        Some(handle)
    }

    pub(in super::super) fn session_has_registered_media(
        &self,
        session_key: &TransportSessionKey,
    ) -> bool {
        self.session_media
            .get(session_key)
            .is_some_and(|session_lookup| !session_lookup.owned_media.is_empty())
    }

    fn prune_empty_session_media(&mut self, session_key: &TransportSessionKey) {
        if self
            .session_media
            .get(session_key)
            .is_some_and(SessionMediaLookup::is_empty)
        {
            self.session_media.remove(session_key);
        }
    }

    pub(in super::super) fn take_expired_speaker_rooms(
        &mut self,
        now: Instant,
    ) -> BTreeSet<RoomInstanceId> {
        self.routes
            .take_expired_speakers(now)
            .into_iter()
            .filter_map(|src_media| self.source_room_instance_id(src_media))
            .collect()
    }

    pub(in super::super) fn src_media_for_mid(
        &self,
        src_key: &TransportSessionKey,
        source_mid: Mid,
    ) -> Option<TransportMediaId> {
        self.session_media
            .get(src_key)
            .and_then(|source_lookup| source_lookup.producer_mids.get(&source_mid))
    }

    pub(in super::super) fn src_media_for_ssrc(
        &self,
        src_key: &TransportSessionKey,
        source_ssrc: Ssrc,
    ) -> Option<TransportMediaId> {
        self.producer_binding_for_ssrc(src_key, source_ssrc)
            .map(|(media, _rid)| media)
    }

    pub fn producer_binding_for_ssrc(
        &self,
        src_key: &TransportSessionKey,
        source_ssrc: Ssrc,
    ) -> Option<(TransportMediaId, Option<Rid>)> {
        self.session_media
            .get(src_key)
            .and_then(|source_lookup| source_lookup.producer_ssrcs.get(&source_ssrc))
            .map(|binding| (binding.transport_media_id, binding.rid))
    }

    pub(in super::super) fn source_rid_for_ssrc(
        &self,
        src_key: &TransportSessionKey,
        source_ssrc: Ssrc,
    ) -> Option<Rid> {
        self.producer_binding_for_ssrc(src_key, source_ssrc)
            .and_then(|(_media, rid)| rid)
    }

    /// Resolves and admits one authenticated local packet in the session's media index.
    ///
    /// Cached media precedes MID and SSRC. An indexed binding for that media
    /// supplies the current RID, including RID-less after renegotiation.
    pub(in super::super) fn bind_producer_packet(
        &mut self,
        session_handle: SessionHandle,
        cached_media: Option<TransportMediaId>,
        mid: Option<Mid>,
        binding: &mut ProducerStreamBinding,
    ) -> Option<(TransportMediaId, RoomInstanceId, ProducerSsrcUpdate)> {
        let session_key = self.users.key_for_handle(session_handle)?;
        let Some(lookup) = self.session_media.get_mut(session_key) else {
            return cached_media.map(|media| {
                (
                    media,
                    session_key.room_instance_id(),
                    ProducerSsrcUpdate::Rejected,
                )
            });
        };
        let indexed = lookup.producer_ssrcs.get(&binding.primary);
        let media = cached_media
            .or_else(|| lookup.producer_mids.get(&mid?))
            .or_else(|| indexed.map(|indexed| indexed.transport_media_id))?;
        // str0m can retain an earlier SDP-declared RID for the same primary.
        if let Some(indexed) = indexed.filter(|indexed| indexed.transport_media_id == media) {
            binding.rid = indexed.rid;
        }
        let update = bind_producer_stream(
            &mut self.mid_registry,
            lookup,
            &mut self.routes,
            session_key,
            media,
            *binding,
        );
        Some((media, session_key.room_instance_id(), update))
    }

    /// Commits the current primary and repair identities for one negotiated encoding.
    ///
    /// Both indexes retain at most two SSRCs per encoding. The previous primary
    /// is only a bounded rejection tombstone, never a demultiplexing binding.
    /// A collision, unknown RID or preceding primary returns Rejected without
    /// changing either index. Callers must not forward rejected packets.
    pub fn bind_producer_stream(
        &mut self,
        source: &ForwardedPacketSource,
        transport_media_id: TransportMediaId,
        binding: ProducerStreamBinding,
    ) -> ProducerSsrcUpdate {
        let session_key = match source {
            ForwardedPacketSource::Local { session_handle, .. } => {
                let Some(key) = self.users.key_for_handle(*session_handle) else {
                    return ProducerSsrcUpdate::Rejected;
                };
                key
            }
            ForwardedPacketSource::Relayed(key) => key,
        };
        let Some(lookup) = self.session_media.get_mut(session_key) else {
            return ProducerSsrcUpdate::Rejected;
        };
        bind_producer_stream(
            &mut self.mid_registry,
            lookup,
            &mut self.routes,
            session_key,
            transport_media_id,
            binding,
        )
    }

    #[cfg(any(test, feature = "testing-transport"))]
    pub(in super::super) fn consumer_src_media_for_mid(
        &self,
        consumer_key: &TransportSessionKey,
        consumer_mid: Mid,
    ) -> Option<TransportMediaId> {
        self.session_media
            .get(consumer_key)
            .and_then(|consumer_lookup| consumer_lookup.consumer_mids.get(&consumer_mid))
            .map(|binding| binding.src_media)
    }

    /// resolve consumer RTCP feedback to the currently active producer target
    ///
    /// this is the packet-loop feedback path
    /// missing indexes, removed routes and inactive routes are treated as stale
    /// feedback because those can race with teardown after `str0m` emits a
    /// request
    ///
    /// selected destination gates override the feedback RID so RID-less browser
    /// PLI stays scoped to the routed simulcast layer
    #[inline]
    pub(in super::super) fn active_consumer_kf_target(
        &self,
        consumer_key: &TransportSessionKey,
        consumer_mid: Mid,
        feedback_rid: Option<Rid>,
    ) -> Option<ConsumerKeyframeTarget> {
        let binding = self
            .session_media
            .get(consumer_key)?
            .consumer_mids
            .get(&consumer_mid)?;
        let (route_entry, source_active) =
            self.routes.local_route_and_activity(binding.src_media)?;
        // a miss means feedback raced with route teardown or index repair
        let destination = route_entry.destinations.get(binding.dst_idx?)?;
        debug_assert_eq!(&destination.dest_session, consumer_key);
        debug_assert_eq!(destination.dest_transport_media_id, binding.consumer_media);
        if !source_active || !destination.active {
            return None;
        }
        let DestinationKeyframeTarget::Current(rid) =
            destination.delivery.keyframe_target_rid(feedback_rid)
        else {
            return None;
        };
        Some(ConsumerKeyframeTarget {
            src_media: binding.src_media,
            rid,
        })
    }

    /// updates the cached destination slot for one consumer MID binding
    ///
    /// callers pass both sides of the route identity so a late repair from an
    /// old route cannot relink a MID to a different source
    pub(in super::super) fn set_consumer_dst_idx(
        &mut self,
        consumer_key: &TransportSessionKey,
        consumer_mid: Mid,
        consumer_media: TransportMediaId,
        src_media: TransportMediaId,
        dst_idx: Option<usize>,
    ) {
        if let Some(consumer_lookup) = self.session_media.get_mut(consumer_key) {
            consumer_lookup.set_consumer_dst_idx(consumer_mid, consumer_media, src_media, dst_idx);
        }
    }

    pub(in super::super) fn consumer_dst_idx(
        &self,
        consumer_key: &TransportSessionKey,
        consumer_mid: Mid,
        consumer_media: TransportMediaId,
        src_media: TransportMediaId,
    ) -> Option<usize> {
        let binding = self
            .session_media
            .get(consumer_key)?
            .consumer_mids
            .get(&consumer_mid)?;
        (binding.consumer_media == consumer_media && binding.src_media == src_media)
            .then_some(binding.dst_idx?)
    }

    fn source_room_instance_id(&self, src_media: TransportMediaId) -> Option<RoomInstanceId> {
        self.media_handle(src_media)
            .map(|handle| handle.session_key().room_instance_id())
            .or_else(|| {
                self.routes
                    .remote_source(src_media)
                    .map(|registration| registration.source().session_key().room_instance_id())
            })
    }

    pub(in super::super) fn remove_session_media_handles(
        &mut self,
        session_key: &TransportSessionKey,
    ) -> Vec<TransportMediaId> {
        let mut removed_ids =
            self.session_media.get(session_key).map_or_else(
                || {
                    debug_assert!(
                        self.mid_registry.iter().all(
                            |(_transport_media_id, registered)| registered.handle.session_key()
                                != session_key
                        ),
                        "session media index missing handles for session"
                    );
                    Vec::new()
                },
                |session_lookup| session_lookup.owned_media.clone(),
            );
        removed_ids
            .retain(|transport_media_id| self.remove_media_handle(*transport_media_id).is_some());
        removed_ids
    }

    pub(in super::super) fn refresh_answer_producer_ssrcs(
        &mut self,
        session_key: &TransportSessionKey,
        producer_mids: &[Mid],
        refreshed_parameters: &[(Mid, MediaStream)],
    ) {
        let previous_encodings = producer_mids
            .iter()
            .filter_map(|mid| {
                let lookup = self.session_media.get(session_key)?;
                let media = lookup.producer_mids.get(mid)?;
                let registered = self
                    .mid_registry
                    .get(&media)
                    .filter(|registered| registered.is_producer_for(session_key))?;
                Some((*mid, media, registered.producer_encodings.clone()))
            })
            .collect::<Vec<_>>();
        for mid in producer_mids {
            self.clear_producer_ssrcs_for_mid(session_key, *mid);
        }
        for (mid, parameters) in refreshed_parameters {
            if let Some((_, media, previous)) = previous_encodings
                .iter()
                .find(|(previous_mid, _, _)| previous_mid == mid)
            {
                self.refresh_producer_ssrcs_with_previous(
                    session_key,
                    *media,
                    parameters,
                    previous,
                );
            }
        }
    }

    fn refresh_producer_ssrcs_with_previous(
        &mut self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
        parameters: &MediaStream,
        previous_encodings: &[ProducerEncoding],
    ) {
        if !self
            .mid_registry
            .get(&transport_media_id)
            .is_some_and(|registered| registered.is_producer_for(session_key))
        {
            return;
        }
        self.routes
            .refresh_packet_inspector(transport_media_id, parameters);
        let mut encodings = Vec::new();
        for binding in parameters.bindings().take(codec::MAX_SEND_STREAMS) {
            let rid = binding.rid().map(Rid::from);
            if encodings
                .iter()
                .any(|encoding: &ProducerEncoding| encoding.rid == rid)
            {
                continue;
            }
            encodings.push(ProducerEncoding {
                rid,
                primary: None,
                repair: None,
                previous_primary: None,
            });
        }
        if let Some(registered) = self.mid_registry.get_mut(&transport_media_id) {
            registered.producer_encodings = encodings;
        }
        for binding in parameters.bindings().take(codec::MAX_SEND_STREAMS) {
            let Some(primary) = binding.ssrc() else {
                continue;
            };
            let Some(lookup) = self.session_media.get_mut(session_key) else {
                continue;
            };
            let _outcome = bind_producer_stream(
                &mut self.mid_registry,
                lookup,
                &mut self.routes,
                session_key,
                transport_media_id,
                ProducerStreamBinding {
                    rid: binding.rid().map(Rid::from),
                    primary: primary.into(),
                    repair: binding.repair_ssrc().map(Into::into),
                },
            );
        }
        if let Some(registered) = self.mid_registry.get_mut(&transport_media_id) {
            for encoding in &mut registered.producer_encodings {
                if let Some(previous) = previous_encodings
                    .iter()
                    .find(|previous| previous.rid == encoding.rid)
                    && encoding.primary.is_some()
                {
                    encoding.previous_primary = if previous.primary == encoding.primary {
                        previous.previous_primary
                    } else {
                        previous.primary
                    };
                }
            }
        }
        self.apply_producer_nack_policy(session_key, transport_media_id);
    }

    /// Reapplies NACK suppression because SDP answers can recreate `StreamRx`.
    pub(in super::super) fn apply_producer_nack_policy(
        &mut self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) {
        let Some(mid) = self.resolve_mid(transport_media_id) else {
            return;
        };
        let active = self.routes.source_is_active(transport_media_id);
        let (routes, session_media, users) = (&self.routes, &self.session_media, &mut self.users);
        let Some(ssrcs) = routes.producer_ssrcs(transport_media_id) else {
            return;
        };
        let Some(session_lookup) = session_media.get(session_key) else {
            return;
        };
        // Primary and repair bindings share one receive stream per negotiated RID.
        let mut rids = Vec::new();
        for ssrc in ssrcs {
            let rid = session_lookup
                .producer_ssrcs
                .get(ssrc)
                .and_then(|binding| binding.rid);
            if !rids.contains(&rid) {
                rids.push(rid);
            }
        }
        let Some(session_state) = users.get_mut(session_key) else {
            return;
        };
        let repair_enabled = session_state
            .sdp_negotiation
            .negotiated_producer_parameters
            .get(&mid)
            .is_some_and(codec::repair_enabled);
        let mut api = session_state.rtc.direct_api();
        for rid in rids {
            // `stream_rx_by_mid` invalidates str0m's aggregate NACK cache before mutation.
            let Some(stream_rx) = api.stream_rx_by_mid(mid, rid) else {
                continue;
            };
            stream_rx.suppress_nack(!active || !repair_enabled);
        }
    }

    pub(in super::super) fn clear_producer_ssrcs_for_mid(
        &mut self,
        session_key: &TransportSessionKey,
        mid: Mid,
    ) {
        let Some(transport_media_id) = self
            .session_media
            .get(session_key)
            .and_then(|producer_lookup| producer_lookup.producer_mids.get(&mid))
        else {
            return;
        };
        if !self
            .mid_registry
            .get(&transport_media_id)
            .is_some_and(|registered| registered.is_producer_for(session_key))
        {
            return;
        }
        self.clear_producer_ssrcs(session_key, transport_media_id);
        self.routes.clear_packet_inspector(transport_media_id);
        self.routes.replace_producer_ssrcs(transport_media_id, []);
    }

    fn clear_producer_ssrcs(
        &mut self,
        session_key: &TransportSessionKey,
        transport_media_id: TransportMediaId,
    ) {
        let Some(registered) = self
            .mid_registry
            .get_mut(&transport_media_id)
            .filter(|registered| registered.is_producer_for(session_key))
        else {
            return;
        };
        let Some(ssrcs) = self.routes.clear_producer_ssrcs(transport_media_id) else {
            return;
        };
        registered.producer_encodings.clear();
        let Some(session_lookup) = self.session_media.get_mut(session_key) else {
            return;
        };
        for ssrc in ssrcs {
            if session_lookup
                .producer_ssrcs
                .get(&ssrc)
                .is_some_and(|binding| binding.transport_media_id == transport_media_id)
            {
                session_lookup.producer_ssrcs.remove(&ssrc);
            }
        }
    }
}

#[cfg(test)]
#[path = "../TESTS/media_registry.rs"]
mod tests;
