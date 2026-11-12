use std::fmt::{self, Display, Formatter};

use o_sfu_model::UserId;
use serde::{Deserialize, Serialize};

/// User-scoped stream identity supplied with a publish intent.
///
/// The room treats this value as an opaque slot name scoped by the publishing
/// user. Callers allocate the slot before entering core and pass media kind
/// plus room-policy metadata through the publish intent.
///
/// # Invariant
///
/// `UserStreamId` is not globally unique. It is unique only together with the
/// publishing user id. Room indexes that need publication identity must pair it
/// with the publishing user or use [`PublishedSourceId`] after a publish
/// commits.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct UserStreamId(String);

impl UserStreamId {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for UserStreamId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl From<&str> for UserStreamId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for UserStreamId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

/// Stable room-domain identity for one logical published source.
///
/// A source id identifies the publication itself, not the SDP media section,
/// negotiated RID, RTP SSRC or transport media handle currently realizing it.
/// Room state should allocate it when a publish becomes a room-domain source
/// and keep using it across renegotiation or transport reattachment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PublishedSourceId(u64);

impl PublishedSourceId {
    #[must_use]
    pub fn allocate(next_source_id: &mut u64) -> Self {
        let source_id = Self(*next_source_id);
        *next_source_id = next_source_id.saturating_add(1);
        source_id
    }

    #[cfg(any(test, feature = "testing-transport"))]
    #[must_use]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl Display for PublishedSourceId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "source-{}", self.0)
    }
}

/// Stable room-domain identity for one advertised source encoding
///
/// The id names an advertised encoding of a source such as a simulcast `lo` or
/// `hi` layer. It does not encode a RID string or worker-local route so
/// selectors can keep pointing at the same encoding after transport details are
/// refreshed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourceEncodingId(u64);

impl SourceEncodingId {
    #[must_use]
    pub fn allocate(next_encoding_id: &mut u64) -> Self {
        let encoding_id = Self(*next_encoding_id);
        *next_encoding_id = next_encoding_id.saturating_add(1);
        encoding_id
    }

    #[cfg(any(test, feature = "testing-transport"))]
    #[must_use]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl Display for SourceEncodingId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "encoding-{}", self.0)
    }
}

/// Logical publishing user attached to a source descriptor.
///
/// The user identifies the logical owner visible to room policy. Connection
/// freshness is tracked by producer and transport indexes, because source
/// descriptors are room-domain metadata rather than async commit guards.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedSourceOwner {
    user_id: UserId,
}

impl PublishedSourceOwner {
    #[must_use]
    pub fn new(user_id: UserId) -> Self {
        Self { user_id }
    }

    #[must_use]
    pub fn user_id(&self) -> &UserId {
        &self.user_id
    }
}
