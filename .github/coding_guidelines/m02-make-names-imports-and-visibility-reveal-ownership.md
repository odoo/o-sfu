# M2. Use consistent names, explicit imports and narrow visibility

Use one term per concept, naming values by role and operations by action. Types
and modules provide context, so short names work when their scope makes the
meaning clear. If a precise name requires a sentence, simplify the boundary it
describes.

Conversion names communicate both cost and ownership. Following the Rust API
Guidelines' [conversion
conventions](https://rust-lang.github.io/api-guidelines/naming.html#c-conv), use
`as_` for cheap views of the existing representation, `to_` for conversions that
retain the source and `into_` for conversions that consume it.

Make dependencies visible through explicit production imports and qualify
generic names where context helps, as in `h264::Profile` or `fmt::Result`. Keep
items private until callers need access, then grant only the required visibility.

> [!NOTE]
> Further reading: **[visibility and privacy in the Rust Reference](https://doc.rust-lang.org/reference/visibility-and-privacy.html)**.
>
> Related lints: [absolute_paths](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#absolute_paths),
> [pedantic::enum_glob_use](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#enum_glob_use),
> [pedantic::similar_names](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#similar_names),
> [pedantic::struct_field_names](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#struct_field_names),
> [pedantic::wildcard_imports](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#wildcard_imports)
> and [style::wrong_self_convention](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#wrong_self_convention).

**Example:** `PublishedSources` gives its short field names meaning, while
explicit imports and narrow visibility reveal the module boundary.

**Avoid**

```rust
use std::collections::*;
use super::*;
use crate::engine::source_model::*;

pub struct PublishedSources {
    // The type name is repeated instead of naming each field's role.
    pub published_source_records_by_published_source_id:
        BTreeMap<PublishedSourceId, PublishedSource>,
    pub published_source_id_by_source_key:
        BTreeMap<SourceKey, PublishedSourceId>,
}
```

**Prefer**

```rust
// Explicit imports expose this module's concrete dependencies.
use std::collections::BTreeMap;

use super::{PublishedSource, SourceKey};
use crate::engine::source_model::PublishedSourceId;

pub(super) struct PublishedSources {
    // The type supplies context while visibility keeps the index in media_graph.
    records: BTreeMap<PublishedSourceId, PublishedSource>,
    id_by_key: BTreeMap<SourceKey, PublishedSourceId>,
}
```

**Rationale:** Readers can follow and search consistent vocabulary without
tracing every name to its definition. Narrow visibility keeps internal details
from becoming dependencies.
