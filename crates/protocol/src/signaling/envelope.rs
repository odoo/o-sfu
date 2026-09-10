use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use serde_json::Value;

pub type EnvelopeBatch = Vec<Envelope>;

pub const MAX_ENVELOPE_BATCH_LEN: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EnvelopeBatchDecodeError {
    #[error("invalid JSON envelope batch")]
    InvalidJson,
    #[error("envelope batch contains {actual} entries, exceeding the limit of {limit}")]
    BatchTooLarge { actual: usize, limit: usize },
    #[error("invalid envelope routing metadata")]
    InvalidRoutingMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RequestId(String);

impl RequestId {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum EnvelopeRoute {
    Message,
    Request(RequestId),
    Response(RequestId),
}

impl EnvelopeRoute {
    fn from_wire(request_id: Option<RequestId>, response_to: Option<RequestId>) -> Option<Self> {
        match (request_id, response_to) {
            (None, None) => Some(Self::Message),
            (Some(request_id), None) => Some(Self::Request(request_id)),
            (None, Some(response_to)) => Some(Self::Response(response_to)),
            (Some(_), Some(_)) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    tag: String,
    payload: Option<Value>,
    route: EnvelopeRoute,
}

#[derive(Deserialize)]
struct WireEnvelope {
    #[serde(rename = "t")]
    tag: String,
    #[serde(rename = "p")]
    payload: Option<Value>,
    #[serde(rename = "q")]
    request_id: Option<RequestId>,
    #[serde(rename = "r")]
    response_to: Option<RequestId>,
}

#[derive(Serialize)]
struct WireEnvelopeRef<'a> {
    #[serde(rename = "t")]
    tag: &'a str,
    #[serde(rename = "p", skip_serializing_if = "Option::is_none")]
    payload: Option<&'a Value>,
    #[serde(rename = "q", skip_serializing_if = "Option::is_none")]
    request_id: Option<&'a RequestId>,
    #[serde(rename = "r", skip_serializing_if = "Option::is_none")]
    response_to: Option<&'a RequestId>,
}

impl Envelope {
    #[must_use]
    pub fn message(tag: &str, payload: Option<Value>) -> Self {
        Self {
            tag: tag.to_owned(),
            payload,
            route: EnvelopeRoute::Message,
        }
    }

    #[must_use]
    pub fn request(tag: &str, request_id: RequestId, payload: Option<Value>) -> Self {
        Self {
            tag: tag.to_owned(),
            payload,
            route: EnvelopeRoute::Request(request_id),
        }
    }

    #[must_use]
    pub fn response(tag: &str, response_to: RequestId, payload: Option<Value>) -> Self {
        Self {
            tag: tag.to_owned(),
            payload,
            route: EnvelopeRoute::Response(response_to),
        }
    }

    pub(super) fn into_parts(self) -> (String, Option<Value>, EnvelopeRoute) {
        (self.tag, self.payload, self.route)
    }
}

impl WireEnvelope {
    fn into_envelope(self) -> Option<Envelope> {
        let route = EnvelopeRoute::from_wire(self.request_id, self.response_to)?;

        Some(Envelope {
            tag: self.tag,
            payload: self.payload,
            route,
        })
    }
}

/// Decode a websocket envelope batch while preserving route validation errors
/// and checking a caller-provided batch limit before route conversion.
///
/// # Errors
///
/// Returns `InvalidJson` when the payload cannot be decoded as the envelope
/// wire shape. Returns `BatchTooLarge` when the decoded batch exceeds `limit`.
/// Returns `InvalidRoutingMetadata` when an envelope contains both a request id
/// and response id.
pub fn decode_envelope_batch(
    payload: &str,
    limit: usize,
) -> Result<EnvelopeBatch, EnvelopeBatchDecodeError> {
    let wire_batch = serde_json::from_str::<Vec<WireEnvelope>>(payload)
        .map_err(|_error| EnvelopeBatchDecodeError::InvalidJson)?;
    if wire_batch.len() > limit {
        return Err(EnvelopeBatchDecodeError::BatchTooLarge {
            actual: wire_batch.len(),
            limit,
        });
    }

    wire_batch
        .into_iter()
        .map(WireEnvelope::into_envelope)
        .collect::<Option<EnvelopeBatch>>()
        .ok_or(EnvelopeBatchDecodeError::InvalidRoutingMetadata)
}

impl Serialize for Envelope {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let (request_id, response_to) = match &self.route {
            EnvelopeRoute::Message => (None, None),
            EnvelopeRoute::Request(request_id) => (Some(request_id), None),
            EnvelopeRoute::Response(response_to) => (None, Some(response_to)),
        };

        WireEnvelopeRef {
            tag: self.tag.as_str(),
            payload: self.payload.as_ref(),
            request_id,
            response_to,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Envelope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        WireEnvelope::deserialize(deserializer)?
            .into_envelope()
            .ok_or_else(|| de::Error::custom("envelope cannot be both request and response"))
    }
}
