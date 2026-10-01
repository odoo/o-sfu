//! Caller-facing `o-sfu-core` API.
//!
//! This facade groups configuration, source intent, [`SfuCore`] and
//! [`MediaSession`]. Process integration types remain under [`crate::server`].

pub use crate::{
    Bitrate, ConnectionId, RoomInstanceId,
    engine::{
        media_transport::TransportSessionHealth,
        source_model::{
            ActiveSpeakerGroup, ActiveSpeakerPolicy, ActiveSpeakerSourceRole,
            SourceAdaptationPolicy, SourceDeactivateIntent, SourceLayoutPolicy, SourcePolicy,
            SourcePublishIntent, SourceRoomPolicySelector, SourceRoutePriority,
            SourceSubscriptionIntent, UploadLayerPolicyRole, UserStreamId,
        },
    },
    options::{
        AudioCodecPreference, CodecPreferences, MediaCodecFlags, RoomMediaLimits,
        RoomMediaLimitsError, RoomWorkerPolicy, RtcPortRange, RtcPortRangeError, RtcUdpIoBackend,
        RuntimeFeatureFlags, SessionBitrateLimits, VideoAdaptationTuning,
        VideoAdaptationTuningError, VideoBitrateLimits, VideoCodecPreference,
    },
    sfu::{
        MediaSession, NegotiationOffer, SessionError, SfuCore, SfuCoreError, UploadEncoding,
        UploadSlot,
    },
};
