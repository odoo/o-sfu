//! browser and native signaling protocol for `o-sfu`
//!
//! `o-sfu-protocol` keeps client-side protocol state pure
//! the core state machine accepts host events and returns ordered command
//! vectors that the embedding host executes through its own WebSocket,
//! `RTCPeerConnection` and timer APIs
//!
//! the crate has 3 public facades:
//!
//! - `host` is the state-machine surface for browser, native and test hosts
//! - `wire` contains JSON envelope and signaling payload types
//! - `bundle` preserves the browser bundle compatibility API used by Odoo
//!
//! Hosts execute every command in each returned vector before reporting the
//! resulting socket and peer-connection events to the same
//! [`host::ProtocolCore`]. The host creates the peer connection when applying
//! an initial offer. While that negotiation is pending, the host
//! submits its correlated answer then reports readiness after the peer
//! connection becomes usable.
//!
//! ```
//! use o_sfu_protocol::host::{Command, ConnectionState, NegotiationKind, ProtocolCore};
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut core = ProtocolCore::new();
//! let connect = core.connect("wss://sfu.test/ws", "signed-token", None);
//! assert!(matches!(
//!     connect.as_slice(),
//!     [
//!         Command::SetAvailableFeatures { .. },
//!         Command::SetRecordingState { .. },
//!         Command::EmitStateChange { state: ConnectionState::Connecting, .. },
//!         Command::Connect { .. },
//!     ]
//! ));
//!
//! let auth = core.on_ws_open();
//! assert!(matches!(auth.as_slice(), [Command::SendWebSocket { .. }]));
//! # let welcome = concat!(
//! #     r#"[{"t":"welcome","p":{"features":{"rtc":true,"transcription":false,"#,
//! #     r#""audioRecording":false,"videoRecording":false},"recording":{},"peers":[]}}]"#,
//! # );
//! let welcome = core.on_ws_message(welcome);
//! assert!(matches!(welcome.get(2),
//!     Some(Command::EmitStateChange { state: ConnectionState::Authenticated, .. })
//! ));
//!
//! let offer = r#"[{"t":"offer","q":"offer-1","p":{"sdp":"v=0\r\n","uploadSlots":[]}}]"#;
//! let negotiation = core.on_ws_message(offer);
//! let [Command::ApplyNegotiation { request_id, kind, .. }] = negotiation.as_slice()
//! else {
//!     return Err("unexpected negotiation command order".into());
//! };
//!
//! assert!(core.on_transport_ready().is_empty());
//!
//! let answer = core.submit_negotiation_answer(request_id, *kind, "v=0\r\ns=answer\r\n");
//! assert_eq!(*kind, NegotiationKind::Offer);
//! assert!(matches!(answer.as_slice(), [Command::SendWebSocket { .. }]));
//!
//! // The host sends `answer` then waits until the peer connection is usable.
//! let ready = core.on_transport_ready();
//! assert_eq!(
//!     ready.as_slice(),
//!     &[Command::EmitStateChange { state: ConnectionState::Connected, cause: None }]
//! );
//! # Ok(())
//! # }
//! ```

mod bundle_api;
mod core;
mod host_bridge;
mod shared;
mod signaling;
#[cfg(target_arch = "wasm32")]
pub mod wasm;

/// host-owned protocol state machine and side-effect commands
///
/// hosts drive `ProtocolCore` by reporting WebSocket, timer and peer-connection
/// events
/// every transition returns ordered commands, so side effects stay explicit at
/// the host boundary
pub mod host {
    pub use crate::{core::*, host_bridge::*};
}

/// JSON wire envelopes and signaling payloads
///
/// this facade contains the serialized protocol contract exchanged over the
/// WebSocket
/// browser and native hosts should share these types instead of duplicating
/// envelope names or request payload shapes
///
/// String user identities are limited to 256 decoded UTF-8 bytes by
/// [`crate::wire::UserId::MAX_STRING_BYTES`]. This limit applies to native clients as well
/// as browser clients, including authentication claims and subscription
/// targets. Numeric strings retain their wire representation until runtime
/// normalization, so the limit applies before that normalization.
pub mod wire {
    pub use crate::{shared::*, signaling::*};
}

/// browser bundle compatibility API
pub mod bundle {
    pub use crate::{bundle_api::*, shared::*};
}
