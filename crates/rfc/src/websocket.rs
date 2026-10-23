//! WebSocket limits from RFC 6455:
//! - [Control frames, section 5.5](https://www.rfc-editor.org/rfc/rfc6455.html#section-5.5)
//! - [Close payload, section 5.5.1](https://www.rfc-editor.org/rfc/rfc6455.html#section-5.5.1)

/// Maximum payload of a WebSocket control frame, in bytes.
const MAX_CONTROL_PAYLOAD_BYTES: usize = 125;

/// Status-code prefix of a nonempty Close frame, in bytes.
const CLOSE_STATUS_CODE_BYTES: usize = 2;

/// Maximum UTF-8 reason length after the Close frame's status code.
pub const MAX_CLOSE_REASON_BYTES: usize = MAX_CONTROL_PAYLOAD_BYTES - CLOSE_STATUS_CODE_BYTES;
