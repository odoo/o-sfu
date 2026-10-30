# M6. Qualify functions and constants with their module

Keep the origin visible when using a free function or constant from another
module or crate. Import the containing module and use `module::function` or
`module::CONSTANT`. Use the crate name for items exposed at its root. Apply
the same rule to RFC constants and to functions passed as values.

Names defined in the current module or supplied by the Rust prelude can remain
unqualified. Types and traits keep their normal imports as described in
[M2](m02-make-names-imports-and-visibility-reveal-ownership.md).

> [!NOTE]
> Further reading: **[module qualification in Google's Rust guide](https://google.github.io/styleguide/rustguide.html#qualify-calls-to-top-level-functions-with-a-module-name)**.

**Example:** The [RTP module](../../crates/rfc/src/rtp.rs) defines payload-type
constants and classification functions. Keep `rtp` visible at each use.

**Avoid**

```rust
use o_sfu_rfc::rtp::{RTP_DYNAMIC_PAYLOAD_TYPE_START, is_dynamic_payload_type};

let payload_type = requested_payload_type.unwrap_or(RTP_DYNAMIC_PAYLOAD_TYPE_START);
let is_dynamic = is_dynamic_payload_type(payload_type);
```

**Prefer**

```rust
use o_sfu_rfc::rtp;

let payload_type = requested_payload_type.unwrap_or(rtp::RTP_DYNAMIC_PAYLOAD_TYPE_START);
let is_dynamic = rtp::is_dynamic_payload_type(payload_type);
```

**Rationale:** The module name identifies the source of a helper or protocol
value at the use site. Readers can distinguish protocol definitions from local
policy without looking up each import.
