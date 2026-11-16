//! Send m-sections of one session, by the `a=msid` stream they belong to.
//!
//! str0m keeps the MID and the stream of a section for the session and, as
//! offerer, only appends sections. A removed consumer therefore leaves its
//! section behind. [`SendStreams`] gives that MID to a later consumer, so the
//! SDP stops growing with every publisher that leaves.
//!
//! Sync rule: a receiver plays the tracks of one stream in sync, so one sync
//! group sends on a stream at a time. A consumer takes a free MID of its kind
//! from the stream of its group, else from a stream without consumers.
//! Otherwise it gets a new section, in the stream of its group or in a new
//! one. A consumer without a group takes a stream only when it fills it, so it
//! never keeps a publisher from a free MID.
//!
//! The stream of a section is fixed, so a free MID serves only a consumer that
//! fits its stream: a publisher's camera needs the stream of its microphone
//! and a consumer without a group needs a stream of one section. A session
//! can therefore hold more sections than its peak of consumers per kind.
//!
//! ```text
//! stream sfu-7   publisher A: microphone on mid 3, camera on mid 4
//!   A leaves  -> mid 3 and mid 4 are free and the stream has no group
//!   B joins   -> B's microphone takes mid 3, B's camera takes mid 4
//! ```
use str0m::{
    Rtc,
    media::{MediaKind, Mid},
};

use crate::{
    ConnectionId, RoomInstanceId,
    engine::{
        media_transport::{TransportMediaId, TransportSessionKey},
        source_model::SourceSyncPolicy,
    },
};

/// Publisher connection whose microphone and camera a receiver plays in sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::engine::media_transport::rtc) struct SyncGroup {
    room_instance: RoomInstanceId,
    connection: ConnectionId,
}

impl SyncGroup {
    /// Returns the group of a consumer of `source`. `None` syncs it with no
    /// other consumer.
    pub(in crate::engine::media_transport::rtc) const fn of(
        source: &TransportSessionKey,
        sync: SourceSyncPolicy,
    ) -> Option<Self> {
        // Browsers synchronize only one audio/video pair per SDP stream. Sources
        // outside that pair need a stream of their own so they cannot displace
        // its video.
        // https://webrtc.googlesource.com/src/+/refs/heads/main/call/call.cc
        match sync {
            SourceSyncPolicy::Independent => None,
            SourceSyncPolicy::Publisher => Some(Self {
                room_instance: source.room_instance_id(),
                connection: source.connection_id(),
            }),
        }
    }
}

/// Stream and MID of a send addition. Without a free MID the offer adds a
/// section to `stream`.
pub(super) struct Placement {
    pub(super) stream: String,
    pub(super) free_mid: Option<Mid>,
}

struct SendStream {
    id: String,
    /// Group of the consumers on the stream. An independent consumer has none
    /// and stays alone on its stream.
    sync_group: Option<SyncGroup>,
    /// Consumers on the stream. At zero another group may take it.
    consumers: usize,
    /// Sections whose consumer was removed.
    free_mids: Vec<(Mid, MediaKind)>,
}

#[derive(Default)]
pub(super) struct SendStreams(Vec<SendStream>);

impl SendStreams {
    /// Puts the consumer `media` on a stream by the sync rule and takes a free
    /// MID of its kind there.
    pub(super) fn place(
        &mut self,
        media: TransportMediaId,
        kind: MediaKind,
        sync_group: Option<SyncGroup>,
    ) -> Placement {
        let has_free_mid =
            |stream: &SendStream| stream.free_mids.iter().any(|(_, free)| *free == kind);
        let position = self
            .0
            .iter()
            .position(|stream| {
                stream.consumers > 0 && sync_group.is_some() && stream.sync_group == sync_group
            })
            .or_else(|| {
                self.0.iter().position(|stream| {
                    stream.consumers == 0
                        && has_free_mid(stream)
                        // An independent consumer keeps other consumers off its
                        // stream, so it takes one it fills and leaves a stream
                        // with two sections to a publisher.
                        && (sync_group.is_some() || stream.free_mids.len() == 1)
                })
            });
        let Some(stream) = position.and_then(|position| self.0.get_mut(position)) else {
            // A media id is unique in the worker, so the name is unique in the SDP.
            let stream = format!("sfu-{}", media.as_u64());
            self.0.push(SendStream {
                id: stream.clone(),
                sync_group,
                consumers: 1,
                free_mids: Vec::new(),
            });
            return Placement {
                stream,
                free_mid: None,
            };
        };
        stream.sync_group = sync_group;
        stream.consumers += 1;
        let free_mid = stream
            .free_mids
            .iter()
            .position(|(_, free)| *free == kind)
            .map(|position| stream.free_mids.swap_remove(position).0);
        Placement {
            stream: stream.id.clone(),
            free_mid,
        }
    }

    /// Takes the consumer of `mid` off its stream. With `reuse` the MID is free
    /// for another consumer when its section still sends.
    ///
    /// A MID outside these streams, such as a producer's, is left alone.
    pub(super) fn release(&mut self, rtc: &Rtc, mid: Mid, reuse: bool) {
        let Some(media) = rtc.media(mid) else {
            return;
        };
        let Some(stream) = self
            .0
            .iter_mut()
            .find(|stream| stream.id == media.stream_id())
        else {
            return;
        };
        stream.consumers = stream.consumers.saturating_sub(1);
        // A section the answer declined or rejected stays out. Its client would
        // decline the next consumer too.
        if reuse && media.direction().is_sending() && !media.disabled() {
            stream.free_mids.push((mid, media.kind()));
        }
        self.0
            .retain(|stream| stream.consumers > 0 || !stream.free_mids.is_empty());
    }
}
