# R4. Do not hide failures

Report failures with enough context for operators to understand what went wrong
and investigate the cause.

Use [existing metrics](../../crates/telemetry/) for repeated failures and
structured logs when details help. Keep metric labels to a fixed vocabulary
and never record credentials, packet contents or raw signaling payloads.

> [!NOTE]
> Further reading: **[the OWASP Logging Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html)** and **[Prometheus instrumentation practices](https://prometheus.io/docs/practices/instrumentation/)**.
>
> Related lints: [dbg_macro](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#dbg_macro),
> [map_err_ignore](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#map_err_ignore),
> [print_stderr](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#print_stderr),
> [print_stdout](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#print_stdout)
> and [unused_result_ok](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#unused_result_ok).

**Example:** `handshake::reject` records why a WebSocket connection was rejected
before trying to close it.

**Avoid**

```rust
let code = WebSocketCloseCode::AuthFailed;
// The rejected connection is closed, but operators cannot tell why.
close_writer_bounded(writer, code).await;
```

**Prefer**

```rust
// Preserve the reason even if the close attempt fails.
state.metrics.record_ws_handshake_rejection(Some(code));
info!(
    event = telemetry_event::WS_HANDSHAKE_REJECTED,
    close_code = u16::from(code),
    remote_address,
    "{message}"
);
close_writer_bounded(writer, code).await;
```

**Rationale:** Silent failures leave operators without the evidence needed to
diagnose or resolve a problem.
