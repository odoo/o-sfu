# R1. Preserve compatibility between versions

Clients and servers may deploy at different times, so preserve compatibility
through mixed-version upgrades. This contract covers published
APIs, wire (sfu_client -> server) messages, documented Odoo-facing behavior.

Prefer additive changes that old peers can tolerate: give absent new fields
defaults, ignore unknown optional data where permitted and negotiate tags that
old readers would reject. Accept retained legacy input at the edge and
normalize it before domain use.

Remove compatibility code only when its documented removal condition is met
and tests with mixed versions cover the remaining upgrades and rollbacks. An
unavoidable wire break requires an explicit migration coordinated across the
server, client and compiled Odoo bundle.

(you should probably talk to the internal team when doing deployment-critical changes)

> [!NOTE]
> Further reading: **[SemVer compatibility in The Cargo Book](https://doc.rust-lang.org/cargo/reference/semver.html)** and **[protocol extension guidance in RFC 6709](https://www.rfc-editor.org/rfc/rfc6709.html)**.

**Example:** `SfuClient` keeps `updateUpload` as an alias for older Odoo code.
`ProtocolCore` accepts the legacy `sources` message from older servers.

```typescript
/** @deprecated Odoo compatibility alias. Use `publish()` for new code. */
updateUpload(type: StreamType, track: MediaStreamTrack | null | undefined): void {
    // Older Odoo callers still reach the validated publish path.
    this.publish(type, track);
}
```

```rust
// Ignore the retired snapshot without rejecting the frame. Rolled-back servers
// can still emit it.
ServerMessage::Sources(_) => Vec::new(),
```

**Rationale:** Components change at different times. Compatibility prevents a
version change from breaking calls.
