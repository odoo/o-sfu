# E2. Convert external formats at adapter boundaries

Represent transport-specific, versioned or loosely constrained data with
private wire types, then convert it to O-SFU domain types at adapter
boundaries. Direct serialization of domain types is appropriate only when the
format is their documented contract and needs no adapter validation. Keep
HTTP extractors, WebSocket frames and external-library types inside adapters
so domain code works with validated representations. Normalize compatibility
forms before storage or indexing and use `serde_json::Value` only when the
contract permits arbitrary JSON.

> [!NOTE]
> Further reading: **[`TryFrom` in the standard library](https://doc.rust-lang.org/std/convert/trait.TryFrom.html)**.

**Example:** `decode_client_batch` converts WebSocket input into
`ClientEnvelope`, giving domain code a typed enum after rejecting unknown
tags and malformed payloads.

**Avoid**

```rust
pub async fn apply_client_envelope(
    &mut self,
    // Domain code must interpret wire tags and validate untyped payloads.
    tag: String,
    payload: Option<serde_json::Value>,
) -> Result<UserOutput, UserError>
```

**Prefer**

```rust
pub async fn apply_client_envelope(
    &mut self,
    // Unknown tags and invalid payloads were rejected at the WebSocket boundary.
    envelope: ClientEnvelope,
) -> Result<UserOutput, UserError>
```

**Rationale:** Domain code can rely on validated values without interpreting
transport details.
