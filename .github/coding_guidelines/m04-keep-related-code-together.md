# M4. Keep related code together

Organize modules around responsibilities, keeping each responsibility's state,
behavior, errors, constants and small helpers together. Extract a module when it
has a clear responsibility of its own. Separating item kinds or shortening a
file is not sufficient reason.

Within a module, lead with the main types and entry points before introducing
private helpers. Keep inherent `impl`s beside their types unless a large owner
needs to be split by responsibility and expose one coherent interface at the
module root.

**Example:** `SourceModelError` belongs beside `PublishedSourceDescriptor`
because its meaning comes from the constructor that returns it.

**Avoid**

```rust
// The error has no responsibility apart from descriptor construction.
mod descriptor;
mod errors;

pub use descriptor::PublishedSourceDescriptor;
pub use errors::SourceModelError;
```

**Prefer**

```rust
mod descriptor;

// Descriptor construction and SourceModelError remain one module responsibility.
pub use descriptor::{PublishedSourceDescriptor, SourceModelError};
```

**Rationale:** A maintainer should be able to understand and change one
responsibility without navigating artificial boundaries.
