//! Bounded per-user room output.
//!
//! [`UserOutboundSender::send`] enqueues without waiting and signals
//! [`UserOutboundEvent::Overflow`] when message-count or byte capacity is
//! exhausted. [`UserOutboundReceiver::recv_event`] prioritizes that signal over
//! queued output. User-session loops must stop normal draining after overflow.

use std::{
    collections::BTreeMap,
    future::pending,
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use tokio::sync::{
    mpsc::{self, error::TrySendError},
    watch,
};

use super::UserCloseReason;
use crate::engine::{
    JsonPayload, RecordingStateUpdate, UserId, UserInfo, metrics::RuntimeMetrics,
    source_model::UserStreamId, sync::lock_unpoisoned,
};

pub const MAX_BROADCAST_PAYLOAD_BYTES: usize = 16 * 1024;

const ROOM_EVENT_QUEUE_BYTES: usize = 1024;
const BROADCAST_QUEUE_OVERHEAD_BYTES: usize = 256;
const TRACK_PROJECTION_QUEUE_OVERHEAD_BYTES: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BroadcastPayload {
    message: Arc<JsonPayload>,
    byte_len: usize,
}

impl BroadcastPayload {
    /// Creates a broadcast payload and records its serialized byte length.
    ///
    /// # Errors
    ///
    /// Returns [`BroadcastPayloadError::TooLarge`] when the serialized JSON
    /// exceeds [`MAX_BROADCAST_PAYLOAD_BYTES`]. Returns
    /// [`BroadcastPayloadError::JsonSerialization`] when JSON serialization
    /// fails.
    pub fn try_new(message: JsonPayload) -> Result<Self, BroadcastPayloadError> {
        let byte_len = serialized_json_len(&message)?;
        if byte_len > MAX_BROADCAST_PAYLOAD_BYTES {
            return Err(BroadcastPayloadError::TooLarge {
                actual: byte_len,
                limit: MAX_BROADCAST_PAYLOAD_BYTES,
            });
        }
        Ok(Self {
            message: Arc::new(message),
            byte_len,
        })
    }

    #[must_use]
    pub const fn byte_len(&self) -> usize {
        self.byte_len
    }

    #[must_use]
    pub fn to_json(&self) -> JsonPayload {
        self.message.as_ref().clone()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BroadcastPayloadError {
    TooLarge { actual: usize, limit: usize },
    JsonSerialization,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoomEventMessage {
    Broadcast {
        sender_id: UserId,
        message: BroadcastPayload,
    },
    UserJoined {
        user_id: UserId,
        info: UserInfo,
    },
    UserDeparted {
        user_id: UserId,
    },
    UserInfoChanged(BTreeMap<UserId, UserInfo>),
    RecordingStateChanged(RecordingStateUpdate),
}

impl RoomEventMessage {
    #[must_use]
    pub(super) fn queued_bytes(&self) -> usize {
        match self {
            Self::Broadcast { message, .. } => message
                .byte_len()
                .saturating_add(BROADCAST_QUEUE_OVERHEAD_BYTES),
            Self::UserInfoChanged(snapshot) => {
                ROOM_EVENT_QUEUE_BYTES.saturating_mul(snapshot.len())
            }
            Self::UserJoined { .. }
            | Self::UserDeparted { .. }
            | Self::RecordingStateChanged(_) => ROOM_EVENT_QUEUE_BYTES,
        }
    }
}

#[derive(Debug, Default)]
struct JsonByteCounter {
    len: usize,
}

impl io::Write for JsonByteCounter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.len = self.len.saturating_add(buf.len());
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn serialized_json_len(value: &JsonPayload) -> Result<usize, BroadcastPayloadError> {
    let mut counter = JsonByteCounter::default();
    serde_json::to_writer(&mut counter, value)
        .map_err(|_error| BroadcastPayloadError::JsonSerialization)?;
    Ok(counter.len)
}

pub const DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY: usize = 128;
pub const DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY: usize =
    DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY * MAX_BROADCAST_PAYLOAD_BYTES;

pub(super) type OutboundSender = UserOutboundSender;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteTrackProjection {
    pub consumer_mid: String,
    pub user_id: UserId,
    pub stream_id: UserStreamId,
    pub producer_active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteTrackSnapshot {
    pub tracks: Vec<RemoteTrackProjection>,
    pub requires_negotiation: bool,
}

impl RemoteTrackSnapshot {
    fn queued_bytes(&self) -> usize {
        self.tracks
            .iter()
            .fold(ROOM_EVENT_QUEUE_BYTES, |bytes, track| {
                let user_id_bytes = match &track.user_id {
                    UserId::Integer(_) => 0,
                    UserId::String(value) => value.len(),
                };
                bytes
                    .saturating_add(TRACK_PROJECTION_QUEUE_OVERHEAD_BYTES)
                    .saturating_add(track.consumer_mid.len())
                    .saturating_add(user_id_bytes)
                    .saturating_add(track.stream_id.as_str().len())
            })
    }
}

#[derive(Debug, Clone)]
pub(in crate::engine::room) struct VersionedRemoteTrackSnapshot {
    pub(in crate::engine::room) snapshot: RemoteTrackSnapshot,
    pub(in crate::engine::room) revision: u64,
}

/// room output that belongs to one connected user
#[derive(Debug, Clone)]
pub enum UserOutbound {
    Message(RoomEventMessage),
    RemoteTracks(RemoteTrackSnapshot),
    Close(UserCloseReason),
}

impl UserOutbound {
    #[must_use]
    pub(super) fn queued_bytes(&self) -> usize {
        match self {
            Self::Message(message) => message.queued_bytes(),
            Self::RemoteTracks(snapshot) => snapshot.queued_bytes(),
            Self::Close(_) => ROOM_EVENT_QUEUE_BYTES,
        }
    }
}

/// queue overflow details captured when a user cannot accept more outbound work
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UserOutboundOverflow {
    kind: UserOutboundOverflowKind,
    message_capacity: usize,
    byte_capacity: usize,
    queued_bytes: usize,
    message_bytes: usize,
}

/// outbound queue limit that rejected a message
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserOutboundOverflowKind {
    MessageCount,
    QueuedBytes,
}

impl UserOutboundOverflow {
    const fn new(
        kind: UserOutboundOverflowKind,
        message_capacity: usize,
        byte_capacity: usize,
        queued_bytes: usize,
        message_bytes: usize,
    ) -> Self {
        Self {
            kind,
            message_capacity,
            byte_capacity,
            queued_bytes,
            message_bytes,
        }
    }

    #[must_use]
    pub const fn capacity(self) -> usize {
        self.message_capacity
    }

    #[must_use]
    pub const fn kind(self) -> UserOutboundOverflowKind {
        self.kind
    }

    #[must_use]
    pub const fn byte_capacity(self) -> usize {
        self.byte_capacity
    }

    #[must_use]
    pub const fn queued_bytes(self) -> usize {
        self.queued_bytes
    }

    #[must_use]
    pub const fn message_bytes(self) -> usize {
        self.message_bytes
    }
}

/// non-blocking outbound send failure
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserOutboundSendError {
    Full(UserOutboundOverflow),
    Closed,
}

/// receiver-side queue event for user-session loops
#[derive(Debug)]
pub enum UserOutboundEvent {
    Message(UserOutbound),
    Overflow(UserOutboundOverflow),
    Closed,
}

/// message and byte capacity for one user outbound queue
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UserOutboundQueueLimits {
    message_capacity: usize,
    byte_capacity: usize,
}

impl UserOutboundQueueLimits {
    #[must_use]
    pub fn new(message_capacity: usize, byte_capacity: usize) -> Self {
        Self {
            message_capacity: message_capacity.max(1),
            byte_capacity: byte_capacity.max(1),
        }
    }

    #[must_use]
    pub const fn message_capacity(self) -> usize {
        self.message_capacity
    }

    #[must_use]
    pub const fn byte_capacity(self) -> usize {
        self.byte_capacity
    }
}

impl Default for UserOutboundQueueLimits {
    fn default() -> Self {
        Self::new(
            DEFAULT_USER_OUTBOUND_QUEUE_CAPACITY,
            DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY,
        )
    }
}

#[derive(Debug)]
struct QueuedUserOutbound {
    outbound: UserOutbound,
    bytes: usize,
}

#[derive(Debug, Clone)]
pub struct UserOutboundSender {
    messages: mpsc::Sender<QueuedUserOutbound>,
    overflow: watch::Sender<Option<UserOutboundOverflow>>,
    metrics: Arc<RuntimeMetrics>,
    limits: UserOutboundQueueLimits,
    queued_bytes: Arc<AtomicUsize>,
    latest_track_snapshot: Arc<Mutex<Option<VersionedRemoteTrackSnapshot>>>,
}

#[derive(Debug)]
pub struct UserOutboundReceiver {
    messages: mpsc::Receiver<QueuedUserOutbound>,
    overflow: watch::Receiver<Option<UserOutboundOverflow>>,
    metrics: Arc<RuntimeMetrics>,
    queued_bytes: Arc<AtomicUsize>,
}

impl UserOutboundSender {
    #[must_use]
    pub fn channel(capacity: usize, metrics: Arc<RuntimeMetrics>) -> (Self, UserOutboundReceiver) {
        Self::channel_with_limits(
            UserOutboundQueueLimits::new(capacity, DEFAULT_USER_OUTBOUND_QUEUE_BYTE_CAPACITY),
            metrics,
        )
    }

    #[must_use]
    pub fn channel_with_limits(
        limits: UserOutboundQueueLimits,
        metrics: Arc<RuntimeMetrics>,
    ) -> (Self, UserOutboundReceiver) {
        let (messages_tx, messages_rx) = mpsc::channel(limits.message_capacity());
        let (overflow_tx, overflow_rx) = watch::channel(None);
        let queued_bytes = Arc::new(AtomicUsize::new(0));
        (
            Self {
                messages: messages_tx,
                overflow: overflow_tx,
                metrics: Arc::clone(&metrics),
                limits,
                queued_bytes: Arc::clone(&queued_bytes),
                latest_track_snapshot: Arc::new(Mutex::new(None)),
            },
            UserOutboundReceiver {
                messages: messages_rx,
                overflow: overflow_rx,
                metrics,
                queued_bytes,
            },
        )
    }

    /// Enqueues `outbound` without waiting for capacity.
    ///
    /// [`UserOutboundSendError::Full`] also signals an overflow event for
    /// [`UserOutboundReceiver::recv_event`].
    ///
    /// # Errors
    ///
    /// Returns [`UserOutboundSendError::Full`] when message-count or byte
    /// capacity is exhausted. Returns [`UserOutboundSendError::Closed`] when the
    /// receiver has been dropped.
    pub fn send(&self, outbound: UserOutbound) -> Result<(), UserOutboundSendError> {
        self.enqueue(outbound)
    }

    /// Suppresses older track state while carrying every late negotiation edge
    /// forward on the latest snapshot.
    pub(in crate::engine::room) fn send_remote_tracks(
        &self,
        snapshot: VersionedRemoteTrackSnapshot,
    ) -> Result<(), UserOutboundSendError> {
        let revision = snapshot.revision;
        {
            let mut latest = lock_unpoisoned(&self.latest_track_snapshot);
            if self.messages.is_closed() {
                return Err(UserOutboundSendError::Closed);
            }
            if latest
                .as_ref()
                .is_none_or(|current| revision > current.revision)
            {
                self.enqueue(UserOutbound::RemoteTracks(snapshot.snapshot.clone()))?;
                *latest = Some(snapshot);
                return Ok(());
            }
            if let Some(current) = latest.as_mut()
                && revision < current.revision
                && snapshot.snapshot.requires_negotiation
            {
                current.snapshot.requires_negotiation = true;
                self.enqueue(UserOutbound::RemoteTracks(current.snapshot.clone()))?;
            }
        }
        Ok(())
    }

    fn enqueue(&self, outbound: UserOutbound) -> Result<(), UserOutboundSendError> {
        let bytes = outbound.queued_bytes();
        self.reserve_bytes(bytes)?;
        match self
            .messages
            .try_send(QueuedUserOutbound { outbound, bytes })
        {
            Ok(()) => {
                self.metrics.add_ws_outbound_queued_messages(1);
                Ok(())
            }
            Err(TrySendError::Full(_outbound)) => {
                self.release_bytes(bytes);
                let overflow = self.mark_overflow(
                    UserOutboundOverflowKind::MessageCount,
                    self.queued_bytes.load(Ordering::Acquire),
                    bytes,
                );
                Err(UserOutboundSendError::Full(overflow))
            }
            Err(TrySendError::Closed(_outbound)) => {
                self.release_bytes(bytes);
                Err(UserOutboundSendError::Closed)
            }
        }
    }

    fn reserve_bytes(&self, bytes: usize) -> Result<(), UserOutboundSendError> {
        let byte_capacity = self.limits.byte_capacity();
        let mut queued = self.queued_bytes.load(Ordering::Acquire);
        loop {
            let Some(next) = queued
                .checked_add(bytes)
                .filter(|next| *next <= byte_capacity)
            else {
                let overflow =
                    self.mark_overflow(UserOutboundOverflowKind::QueuedBytes, queued, bytes);
                return Err(UserOutboundSendError::Full(overflow));
            };
            match self.queued_bytes.compare_exchange_weak(
                queued,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_previous) => return Ok(()),
                Err(current) => queued = current,
            }
        }
    }

    fn release_bytes(&self, bytes: usize) {
        self.queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
    }

    fn mark_overflow(
        &self,
        kind: UserOutboundOverflowKind,
        queued_bytes: usize,
        message_bytes: usize,
    ) -> UserOutboundOverflow {
        let overflow = UserOutboundOverflow::new(
            kind,
            self.limits.message_capacity(),
            self.limits.byte_capacity(),
            queued_bytes,
            message_bytes,
        );
        self.metrics.record_ws_outbound_queue_overflow();
        let _ = self.overflow.send(Some(overflow));
        overflow
    }
}

impl UserOutboundReceiver {
    #[must_use]
    pub fn has_overflowed(&self) -> bool {
        self.overflow.borrow().is_some()
    }

    /// Receives queued output without observing overflow.
    ///
    /// User-session loops should use [`Self::recv_event`].
    pub async fn recv(&mut self) -> Option<UserOutbound> {
        self.messages
            .recv()
            .await
            .map(|message| self.record_received(message))
    }

    /// Attempts to receive queued output without observing overflow.
    ///
    /// User-session loops should use [`Self::recv_event`].
    ///
    /// # Errors
    ///
    /// Returns [`mpsc::error::TryRecvError::Empty`] when no output is queued and
    /// [`mpsc::error::TryRecvError::Disconnected`] when every sender is dropped.
    pub fn try_recv(&mut self) -> Result<UserOutbound, mpsc::error::TryRecvError> {
        self.messages
            .try_recv()
            .map(|message| self.record_received(message))
    }

    /// Receives the next queue event with overflow prioritized over queued output.
    pub async fn recv_event(&mut self) -> UserOutboundEvent {
        if let Some(overflow) = *self.overflow.borrow_and_update() {
            return UserOutboundEvent::Overflow(overflow);
        }
        tokio::select! {
            biased;
            overflow = wait_for_overflow(&mut self.overflow) => {
                UserOutboundEvent::Overflow(overflow)
            }
            message = self.messages.recv() => {
                message.map_or(UserOutboundEvent::Closed, |message| {
                    UserOutboundEvent::Message(self.record_received(message))
                })
            }
        }
    }

    fn record_received(&self, message: QueuedUserOutbound) -> UserOutbound {
        self.queued_bytes.fetch_sub(message.bytes, Ordering::AcqRel);
        self.metrics.add_ws_outbound_queued_messages(-1);
        message.outbound
    }
}

impl Drop for UserOutboundReceiver {
    fn drop(&mut self) {
        let mut pending = 0_i64;
        let mut bytes = 0_usize;
        while let Ok(message) = self.messages.try_recv() {
            pending = pending.saturating_add(1);
            bytes = bytes.saturating_add(message.bytes);
        }
        if pending > 0 {
            self.metrics.add_ws_outbound_queued_messages(-pending);
        }
        if bytes > 0 {
            self.queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
        }
    }
}

async fn wait_for_overflow(
    overflow: &mut watch::Receiver<Option<UserOutboundOverflow>>,
) -> UserOutboundOverflow {
    loop {
        if let Some(overflow) = *overflow.borrow_and_update() {
            return overflow;
        }
        if overflow.changed().await.is_err() {
            // A dropped overflow sender parks this arm forever, avoiding a busy
            // loop and letting the message arm of the select resolve.
            pending::<()>().await;
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct MessageFanout {
    recipients: Vec<OutboundSender>,
    message: RoomEventMessage,
}

impl MessageFanout {
    pub(super) fn emit(self) {
        for recipient in self.recipients {
            let _ = recipient.send(UserOutbound::Message(self.message.clone()));
        }
    }
}

pub(super) fn fanout_all(
    recipients: impl IntoIterator<Item = OutboundSender>,
    message: RoomEventMessage,
) -> MessageFanout {
    MessageFanout {
        recipients: recipients.into_iter().collect(),
        message,
    }
}
