# R7. Keep infrastructure out of domain crates

Keep domain crates independent of runtime, network, browser and transport
implementations so their rules can be understood without infrastructure.
Integration belongs in `o-sfu-core` or the root server crate, while shared
contracts belong in the lowest suitable crate. Reuse must respect this
dependency direction rather than introducing loops in the dependency graph.

> [!NOTE]
> Further reading: **[the dependency inversion principle](https://en.wikipedia.org/wiki/Dependency_inversion_principle)**.

**Example:** `o-sfu-router` depends on `o-sfu-model` and `o-sfu-rfc` while
`str0m` integration remains in `o-sfu-core`.

**Avoid**

```toml
# crates/router/Cargo.toml
[dependencies]
# Pulling runtime dependencies inward couples policy to infrastructure.
o-sfu-core.workspace = true
str0m.workspace = true
tokio.workspace = true
```

**Prefer**

```toml
[dependencies]
# Router stays independent of runtime infrastructure.
o-sfu-model.workspace = true
o-sfu-rfc.workspace = true
thiserror.workspace = true
```

**Rationale:** One-way dependencies prevent cycles and isolate domain logic.
