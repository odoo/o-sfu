# A5. Use the simplest construction API

Successful construction must establish the type's invariants. Use `new` when
inputs are already valid and `try_new` when construction needs fallible
validation. Reserve `Default` for one unsurprising canonical value and keep
invariant-bearing fields private to preserve the guarantees of construction.
Plain records may expose independent, unconstrained fields.

Use a builder when it clarifies many or optional arguments, accumulated
compound input, shared terminal-operation configuration or construction side
effects. A small fixed input set does not justify a builder. See the Rust API
Guidelines on [builders for complex
construction](https://rust-lang.github.io/api-guidelines/type-safety.html#builders-enable-construction-of-complex-values-c-builder).

> [!NOTE]
> Related lints: [pedantic::unnecessary_wraps](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#unnecessary_wraps),
> [style::new_ret_no_self](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#new_ret_no_self),
> [style::new_without_default](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#new_without_default)
> and [style::self_named_constructors](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#self_named_constructors).

**Example:** `RoomMediaLimits::try_new` establishes the limits at construction
time and private fields preserve them afterward.

**Avoid**

```rust
// Public fields allow callers to construct invalid states such as zero limits.
pub struct RoomMediaLimits {
    pub max_active_audio_speakers: usize,
    pub max_video_downloads_per_receiver: usize,
}
```

**Prefer**

```rust
pub struct RoomMediaLimits {
    max_active_audio_speakers: usize,
    max_video_downloads_per_receiver: usize,
}

impl RoomMediaLimits {
    // Successful construction proves both room media limits are non-zero.
    pub const fn try_new(
        max_active_audio_speakers: usize,
        max_video_downloads_per_receiver: usize,
    ) -> Result<Self, RoomMediaLimitsError> {
        if max_active_audio_speakers == 0 {
            return Err(RoomMediaLimitsError::MaxActiveAudioSpeakersZero);
        }
        if max_video_downloads_per_receiver == 0 {
            return Err(RoomMediaLimitsError::MaxVideoDownloadsPerReceiverZero);
        }
        Ok(Self {
            max_active_audio_speakers,
            max_video_downloads_per_receiver,
        })
    }
}
```

**Rationale:** Callers can trust a constructed value without learning how its
invariants are enforced. Private fields also leave room to change the internal
representation.
