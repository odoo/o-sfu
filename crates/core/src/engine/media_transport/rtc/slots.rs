//! worker-local slots for packet-loop identities
//!
//! slots solve the mismatch between stable transport ids and packet-loop work
//!
//! ```text
//! room commands, diagnostics and teardown
//!   use stable public keys such as TransportSessionKey or TransportMediaId
//!   those keys are meaningful outside one media worker
//!
//! packet-loop queues, timeout heaps and route destinations
//!   use tiny copy handles
//!   those handles are only meaningful inside the store that created them
//! ```
//!
//! the worker must accept that some work is already queued when a session,
//! media entry or consumer route is removed
//! a bare index would let that delayed work reach a replacement occupant after
//! the index is recycled
//! [`SlotHandle`] prevents that by pairing the index with the generation that was
//! current when the handle was created
//! removal advances the generation before reuse, so stale dirty-session marks,
//! timeout heap entries and route destinations become ordinary no-ops instead
//! of touching new state
//!
//! the tag parameter is part of the contract
//! it keeps session handles, media handles and consumer stream handles in
//! separate type namespaces even though every handle is represented by the same
//! compact index plus generation pair

use std::{collections::BTreeMap, marker::PhantomData};

use super::state::RtcSessionState;
use crate::engine::media_transport::TransportSessionKey;

/// handle namespace for live `RtcSessionState` entries
///
/// session handles are allowed to live in scheduler queues after the public
/// session key has been removed
/// the store validates the generation before the packet loop polls a session
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct SessionSlot;

/// handle namespace for downstream RTP rewrite state
///
/// route destinations carry these handles so per-packet local forwarding can
/// reach receiver-local rewrite state without rebuilding a lookup key
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct ConsumerStreamSlot;

/// generation-checked session handle used by packet-loop scheduler queues
pub(super) type SessionHandle = SlotHandle<SessionSlot>;

/// generation-checked handle stored on route destinations for local RTP rewrite state
pub(super) type ConsumerStreamHandle = SlotHandle<ConsumerStreamSlot>;

/// session table keyed by the public session identity at the worker boundary
///
/// commands enter through [`TransportSessionKey`]
/// the packet loop converts that key into [`SessionHandle`] only for queued work
pub(super) type SessionStore = KeyedSlotStore<TransportSessionKey, RtcSessionState, SessionSlot>;

/// copy identity for one reusable worker-local slot
///
/// callers may queue, copy and compare handles freely because the value is only
/// an access token
/// every read, write or removal must still go through the owning store so stale
/// generations are rejected at the point where state would be touched
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct SlotHandle<Tag> {
    index: usize,
    generation: u64,
    _tag: PhantomData<fn() -> Tag>,
}

impl<Tag> Clone for SlotHandle<Tag> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Tag> Copy for SlotHandle<Tag> {}

impl<Tag> Default for SlotHandle<Tag> {
    /// create an invalid handle that cannot match a live generation
    fn default() -> Self {
        Self {
            index: usize::MAX,
            generation: 0,
            _tag: PhantomData,
        }
    }
}

/// dense reusable storage for one worker-local identity class
///
/// `SlotStore` provides O(1) generational array access for packet-loop hot paths.
/// Callers hold lightweight `Copy` access tokens ([`SlotHandle`]) while the store
/// manages entry allocation, free-list recycling, and ABA safety.
///
/// # Generational Indexing & ABA Safety
///
/// In an asynchronous packet loop, scheduler queues, timeout heaps, and route tables
/// may hold references to an occupant after it has been removed. A bare array index
/// would allow delayed work for a removed occupant to mistakenly corrupt a new occupant
/// that recycled the same index (the classic ABA problem).
///
/// `SlotStore` uses these checks during one slot's generation cycle:
/// 1. Each slot tracks an independent `generation: u64` counter alongside `value: Option<T>`.
/// 2. `insert` assigns the slot's current generation to the returned `SlotHandle`.
/// 3. `get` / `get_mut` validate `handle.generation == entry.generation` before granting access.
/// 4. `remove` takes the value, advances the slot's generation, and returns the index to the LIFO `free` list.
/// 5. Any delayed work attempting access with an older `SlotHandle` fails the generation check
///    and evaluates to `None`, safely turning stale queue items into no-ops.
///
/// A generation value may be shared by separate slot indices. For one slot index, a
/// generation can repeat only after its `u64` counter wraps.
///
/// # Type-Safe Tagging
///
/// The `Tag` marker parameter ([`SessionSlot`], [`ConsumerStreamSlot`]) ensures
/// distinct identity-class handle types at compile time with zero memory overhead
/// (`PhantomData<fn() -> Tag>`). It does not identify a particular store instance, so callers
/// must use a handle only with the store that created it.
///
/// ```text
/// 1. Initial State (2 active sessions, 1 free slot):
///    entries:
///      [0] generation: 1 | value: Some(Session A) <--- SlotHandle { idx: 0, gen: 1 } (active)
///      [1] generation: 3 | value: Some(Session B) <--- SlotHandle { idx: 1, gen: 3 } (active)
///      [2] generation: 2 | value: None
///    free stack: [ 2 ]
///
/// 2. Session A is Removed (`remove(handle)`):
///    - value taken -> None
///    - generation advanced -> 2
///    - index 0 pushed to free stack
///    entries:
///      [0] generation: 2 | value: None
///      [1] generation: 3 | value: Some(Session B)
///      [2] generation: 2 | value: None
///    free stack: [ 2, 0 ]
///
/// 3. Session C is Inserted (`insert(Session C)`):
///    - index 0 popped from free stack
///    - value set -> Some(Session C)
///    - generation retained -> 2
///    entries:
///      [0] generation: 2 | value: Some(Session C) <--- SlotHandle { idx: 0, gen: 2 } (new handle)
///      [1] generation: 3 | value: Some(Session B)
///      [2] generation: 2 | value: None
///    free stack: [ 2 ]
///
/// 4. Delayed Stale Queue Item Executes:
///    - queue item holds stale SlotHandle { idx: 0, gen: 1 } (from Session A)
///    - check: handle.gen (1) == entry.gen (2) ?
///                       |
///                       v  (mismatch)
///    - access returns `None` --> stale work safely discarded with 0 side-effects
/// ```
pub(super) struct SlotStore<T, Tag> {
    entries: Vec<SlotEntry<T>>,
    free: Vec<usize>,
    _tag: PhantomData<fn() -> Tag>,
}

impl<T, Tag> Default for SlotStore<T, Tag> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            free: Vec::new(),
            _tag: PhantomData,
        }
    }
}

struct SlotEntry<T> {
    generation: u64,
    value: Option<T>,
}

impl<T, Tag> SlotStore<T, Tag> {
    /// allocate or reuse a slot and return the generation that names this value
    pub(super) fn insert(&mut self, value: T) -> SlotHandle<Tag> {
        let index = self.free.pop().unwrap_or_else(|| {
            let index = self.entries.len();
            self.entries.push(SlotEntry {
                generation: 1,
                value: None,
            });
            index
        });
        let Some(entry) = self.entries.get_mut(index) else {
            return SlotHandle::default();
        };
        entry.value = Some(value);
        SlotHandle {
            index,
            generation: entry.generation,
            _tag: PhantomData,
        }
    }

    /// return the value only when the handle still names the current generation
    pub(super) fn get(&self, handle: SlotHandle<Tag>) -> Option<&T> {
        let entry = self.entries.get(handle.index)?;
        (entry.generation == handle.generation)
            .then_some(entry.value.as_ref())
            .flatten()
    }

    /// return the mutable value only when the handle still names the current generation
    pub(super) fn get_mut(&mut self, handle: SlotHandle<Tag>) -> Option<&mut T> {
        let entry = self.entries.get_mut(handle.index)?;
        (entry.generation == handle.generation)
            .then_some(entry.value.as_mut())
            .flatten()
    }

    pub(super) fn values_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.entries
            .iter_mut()
            .filter_map(|entry| entry.value.as_mut())
    }

    /// remove the value only when the handle still names the current generation
    ///
    /// successful removal invalidates every copied handle for the old occupant
    pub(super) fn remove(&mut self, handle: SlotHandle<Tag>) -> Option<T> {
        let entry = self.entries.get_mut(handle.index)?;
        if entry.generation != handle.generation {
            return None;
        }
        let value = entry.value.take()?;
        entry.generation = next_generation(entry.generation);
        self.free.push(handle.index);
        Some(value)
    }
}

/// public-key index backed by generation slots
///
/// `KeyedSlotStore` bridges public identity at worker boundaries ([`TransportSessionKey`])
/// with worker-internal generational slots ([`SlotHandle`]).
///
/// ```text
/// Public Boundary (O(log N) tree):
///   TransportSessionKey("user-1", conn_id) ---> BTreeMap lookup ---> SlotHandle { idx: 0, gen: 2 }
///                                                                          |
/// Packet-Loop Hot Path (O(1) direct slot):                                 |
///   SlotStore entries[0] --------------------------------------------------+
///     - KeyedSlot { key: TransportSessionKey(...), value: RtcSessionState }
/// ```
///
/// - **Worker Boundary**: Callers query by public key (`get(&key)`). `handle_for_key`
///   resolves that key to its current slot handle through the `BTreeMap`.
/// - **Packet Loop**: Queues and heaps store lightweight `SlotHandle` copies, avoiding
///   per-packet map lookups.
/// - **Reverse Translation**: `key_for_handle` translates a live handle back to its public
///   key in O(1) time by reading `KeyedSlot.key`.
pub(super) struct KeyedSlotStore<K, V, Tag> {
    by_key: BTreeMap<K, SlotHandle<Tag>>,
    slots: SlotStore<KeyedSlot<K, V>, Tag>,
}

struct KeyedSlot<K, V> {
    key: K,
    value: V,
}

impl<K, V, Tag> Default for KeyedSlotStore<K, V, Tag> {
    fn default() -> Self {
        Self {
            by_key: BTreeMap::new(),
            slots: SlotStore::default(),
        }
    }
}

impl<K: Ord + Clone, V, Tag> KeyedSlotStore<K, V, Tag> {
    pub(super) fn contains_key(&self, key: &K) -> bool {
        self.by_key.contains_key(key)
    }

    /// read by public key after validating the current slot generation
    pub(super) fn get(&self, key: &K) -> Option<&V> {
        self.get_by_handle(self.handle_for_key(key)?)
    }

    /// mutably read by public key after validating the current slot generation
    pub(super) fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.get_mut_by_handle(self.handle_for_key(key)?)
    }

    /// replace the value for a public key and invalidate any old handle
    pub(super) fn insert(&mut self, key: K, value: V) -> Option<V> {
        let previous = self
            .by_key
            .remove(&key)
            .and_then(|handle| self.slots.remove(handle))
            .map(|entry| entry.value);
        let handle = self.slots.insert(KeyedSlot {
            key: key.clone(),
            value,
        });
        self.by_key.insert(key, handle);
        previous
    }

    /// remove the value for a public key and invalidate its handle
    pub(super) fn remove(&mut self, key: &K) -> Option<V> {
        let handle = self.by_key.remove(key)?;
        self.slots.remove(handle).map(|entry| entry.value)
    }

    pub(super) fn len(&self) -> usize {
        self.by_key.len()
    }

    pub(super) fn keys(&self) -> impl Iterator<Item = &K> {
        self.by_key.keys()
    }

    /// translate a public key to the handle used by packet-loop queues
    pub(super) fn handle_for_key(&self, key: &K) -> Option<SlotHandle<Tag>> {
        self.by_key.get(key).copied()
    }

    /// translate a live handle back to its public key
    ///
    /// `None` means the handle is stale or invalid for this store
    pub(super) fn key_for_handle(&self, handle: SlotHandle<Tag>) -> Option<&K> {
        self.slots.get(handle).map(|entry| &entry.key)
    }

    /// read by handle after validating the current slot generation
    pub(super) fn get_by_handle(&self, handle: SlotHandle<Tag>) -> Option<&V> {
        self.slots.get(handle).map(|entry| &entry.value)
    }

    /// mutably read by handle after validating the current slot generation
    pub(super) fn get_mut_by_handle(&mut self, handle: SlotHandle<Tag>) -> Option<&mut V> {
        self.slots.get_mut(handle).map(|entry| &mut entry.value)
    }

    /// mutably read by handle while borrowing the matching public key
    pub(super) fn get_key_value_mut_by_handle(
        &mut self,
        handle: SlotHandle<Tag>,
    ) -> Option<(&K, &mut V)> {
        self.slots
            .get_mut(handle)
            .map(|entry| (&entry.key, &mut entry.value))
    }
}

fn next_generation(generation: u64) -> u64 {
    // keep generation 0 reserved for invalid handles after wraparound
    generation.wrapping_add(1).max(1)
}
