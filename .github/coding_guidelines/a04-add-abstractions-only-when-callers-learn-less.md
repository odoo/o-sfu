# A4. Add abstractions only when they simplify callers

An abstraction earns its place by removing caller decisions, enforcing an
invariant, isolating an external system or capturing a reused contract. Prefer
concrete APIs until actual implementations reveal that contract. Future
flexibility, file organization and mocking alone do not justify a new boundary.

Keep generic APIs similarly focused by placing trait bounds on the smallest
function or `impl` that needs them. Use argument-position `impl Trait` for
incidental type parameters used only once and remove the structure an
abstraction replaces when refactoring.

> [!NOTE]
> Further reading: **[avoiding unnecessary interfaces in Google's Go Style Guide](https://google.github.io/styleguide/go/best-practices.html#avoid-unnecessary-interfaces)**.
>
> Related lints: [complexity::extra_unused_type_parameters](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#extra_unused_type_parameters).

**Example:** `MediaTransport::publish_media` hides worker selection, command
construction and channel dispatch behind one domain call.

**Avoid**

```rust
// Callers must coordinate worker selection, command creation and dispatch.
let worker_id = transport.select_worker(&session_key)?;
let command = TransportCommand::Publish(media_kind, rtp_parameters);
let media = transport.send_command(worker_id, command).await?;
```

**Prefer**

```rust
// One domain call encapsulates worker routing and internal messaging.
let media = transport
    .publish_media(&session_key, media_kind, &rtp_parameters)
    .await?;
```

**Rationale:** A smaller interface is useful only if callers can also forget
the details behind it.
