//! Worker-local route control for declared media.
//!
//! This module exists because consumer-route mutation touches several pieces of
//! state that must stay consistent:
//!
//! - `RouteTable` records source routes and remote-source registrations
//! - relay targets track cross-worker packet fanout
//! - packet gates keep effective local, relay and source policy decisions
//!
//! [`lifecycle`](super::lifecycle) handles media declaration and teardown against
//! [`RtcSessionState`](crate::engine::media_transport::rtc::state::RtcSessionState).
//! Once a producer or consumer handle exists, this module
//! contains the routing-side bookkeeping that validates sources, registers or
//! removes consumer routes and recomputes packet-gate state.
//!
//! Small ownership graph:
//!
//! ```text
//! lifecycle.rs
//!   |-- declare/remove str0m media
//!   |-- register/remove media handles
//!   `-- call control/ when route ownership changes
//!
//! control/
//!   |-- validate source ownership (local vs remote)
//!   |-- mutate RouteTable
//!   |-- refresh route-control packet gates
//!   `-- keep remote-source packet gates executable
//!
//! keyframe.rs
//!   `-- reads the same source-ownership rules for feedback routing
//! ```

mod responses;
mod routes;
mod selected_rid;

pub use responses::{apply_media_control_batch, apply_route_control_request};
pub use routes::remove_source_route;
pub(in crate::engine::media_transport::rtc) use routes::{
    ConsumerRouteRegistration, ensure_existing_route_src, ensure_local_producer_mid,
    ensure_route_src_registered, register_consumer_route, remove_consumer_route,
};
pub use selected_rid::apply_src_decoder_ready;
#[cfg(feature = "internal-benchmarks")]
pub(in crate::engine::media_transport::rtc) use selected_rid::guarded_pkt_gate;
#[cfg(test)]
pub use selected_rid::observe_src_rid_ready;
