#![allow(
    clippy::panic,
    reason = "integration tests use panic-based assertions for clear failures"
)]

#[path = "full_stack/audio_policy.rs"]
mod audio_policy;
#[path = "full_stack/download_controls.rs"]
mod download_controls;
#[path = "full_stack/large_room_spillover.rs"]
mod large_room_spillover;
#[path = "full_stack/metrics.rs"]
mod metrics;
#[path = "full_stack/nack_recovery.rs"]
mod nack_recovery;
#[path = "full_stack/protocol_flow.rs"]
mod protocol_flow;
#[path = "full_stack/relay_spillover.rs"]
mod relay_spillover;
#[path = "full_stack/replacement_flow.rs"]
mod replacement_flow;
#[path = "full_stack/room_lifecycle.rs"]
mod room_lifecycle;
#[path = "full_stack/support.rs"]
mod support;
#[path = "full_stack/video_routing.rs"]
mod video_routing;
