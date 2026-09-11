//! Native signaling protocol surface and wire codec.

#[cfg(test)]
#[expect(non_snake_case, reason = "test modules map to local TESTS directories")]
mod TESTS;
mod codec;
mod envelope;
mod messages;
mod payloads;

pub use o_sfu_model::{PeerSnapshot, RecordingOptions, WebSocketCloseCode};
pub use payloads::{
    AuthPayload, ClientBroadcastPayload, NegotiationUploadEncoding, NegotiationUploadSlot,
    PeerInfoPayload, PeerLeftPayload, RecordingActionResult, ServerBroadcastPayload,
    SessionDescriptionPayload, SourceDescriptor, SourceEncodingDescriptor, StreamIntentPayload,
    SubscribePayload, TrackBinding, UploadLayerPolicyRole, WelcomePayload,
};

pub use self::{
    codec::{ClientEnvelope, ServerEnvelope},
    envelope::{
        Envelope, EnvelopeBatch, EnvelopeBatchDecodeError, MAX_ENVELOPE_BATCH_LEN, RequestId,
        decode_envelope_batch,
    },
    messages::{
        ClientMessage, ClientRequest, ClientResponse, EnvelopeDecodeError, ServerMessage,
        ServerRequest, ServerResponse,
    },
};
