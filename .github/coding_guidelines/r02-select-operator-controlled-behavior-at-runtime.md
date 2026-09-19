# R2. Configure operator choices at runtime

Keep operator choices such as codecs, media policy and limits in runtime
configuration so one build can serve different deployments. Define variables
under [`src/config`](../../src/config/), load them through `Env::var` in
`Config::from_env` and document them in
[`DEPLOYMENT.md`](../../DEPLOYMENT.md). Validate defaults and supplied values,
rejecting unsupported, conflicting or retired settings. When renaming a
variable, define which name takes precedence or reject configurations that
supply both.

only use Cargo features for optional infrastructure or verification
capabilities. Because Cargo can unify features across dependencies, document
and test every combination enabled by CI or deployment manifests. See the
[Cargo feature-unification
guidance](https://doc.rust-lang.org/cargo/reference/features.html#feature-unification).

**Example:** `load_media_codec_flags` loads each codec choice through `Env`.

**Avoid**

```rust
// A Cargo feature bakes operator policy into the binary.
#[cfg(feature = "h264")]
const H264_ENABLED: bool = true;
```

**Prefer**

```rust
// Env validates the deployment-specific choice at startup.
.with_h264(env.var("CODEC_H264").default(defaults.h264_enabled())?)
```

**Rationale:** Runtime configuration keeps operator choices validated and lets
one build serve different deployments.
