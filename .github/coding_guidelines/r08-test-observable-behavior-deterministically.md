# R8. Make tests prove real behavior

Test each contract through the narrowest production interface that exposes it.
Assertions must observe behavior that requires the operation under test to
work, rather than values computed by the fixture or results a no-op could
produce.

Make asynchronous progress explicit by waiting for readiness, controlling time
and synchronizing concurrent steps. Use timeouts only to bound hangs or check
expected absence, never as a substitute for readiness. Tests must remain
independent of test execution order and shared process state.

Compatibility tests must exercise real Rust and TypeScript producers and
consumers across production boundaries. Check the built Odoo bundle against
fixed expectations written independently of its implementation.

> [!NOTE]
> Further reading: **[async testing with paused time in Tokio](https://tokio.rs/tokio/topics/testing)**.

**Example:** `websocket_rejects_batches_over_protocol_envelope_limit` drives the
real WebSocket boundary then observes its close code and metrics.

**Avoid**

```rust
send_text_frame(&mut websocket, oversized_batch, "batch should send").await;
// This delay guesses when processing finished and can still race.
sleep(Duration::from_millis(50)).await;
assert_eq!(server.state.metrics.snapshot().ws_bus_parse_failures(), 1);
```

**Prefer**

```rust
send_text_frame(&mut websocket, oversized_batch, "batch should send").await;
// The peer-observed close frame is the readiness signal.
assert_eq!(
    read_close_code_promptly(&mut websocket).await,
    Some(CloseCode::Protocol),
);
assert_eq!(server.state.metrics.snapshot().ws_bus_parse_failures(), 1);
```

**Rationale:** A useful test fails on broken behavior, not scheduler timing or
private implementation changes.
