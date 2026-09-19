# O2. Release state guards before async effects

Never hold a blocking lock guard across `.await` and release room-state guards
before I/O or async effects. Capture what the effect needs while reading or
updating the state, then leave the guard's scope before executing it. If I/O must
succeed before a state change can commit, run it without the guard then
reacquire the guard to revalidate and commit.

An async ordering guard, such as `Room::source_policy_turn`, may span `.await`
only to prevent effects from overtaking one another. The awaited code must not
acquire that guard, either directly or through another call.

> [!NOTE]
> Further reading: **[Tokio's shared-state guidance](https://tokio.rs/tokio/tutorial/shared-state)** and **[the async `Mutex` contract](https://docs.rs/tokio/latest/tokio/sync/struct.Mutex.html)**.
>
> Related lints: [significant_drop_tightening](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#significant_drop_tightening),
> [suspicious::await_holding_lock](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#await_holding_lock)
> and [suspicious::await_holding_refcell_ref](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#await_holding_refcell_ref).

The [publication lifecycle test](../../crates/core/src/engine/room/TESTS/producer_tests/publish_lifecycle.rs)
checks that a competing transition waits for the ordered turn to finish.

**Example:** `Room::update_user_info` returns the commit from the state block
and executes its effects after the guard is released.

**Avoid**

```rust
let mut state = self.state.write().await;
if let Some(commit) = state.apply_presence_update(...) {
    // The room-state guard remains held while the effect waits.
    RoomEffects::from_presence(commit).execute(...).await;
}
```

**Prefer**

```rust
let commit = {
    let mut state = self.state.write().await;
    state.apply_presence_update(...)
};
// The block released the room-state guard before the effect can suspend.
if let Some(commit) = commit {
    RoomEffects::from_presence(commit).execute(...).await;
}
```

**Rationale:** State guards protect a mutation while ordering guards protect an
effect sequence. Separating those duties limits how long state access waits and
avoids lock-order cycles without allowing effects to overtake one another.
