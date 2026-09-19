# S1. Avoid unnecessary mutability

Prefer immutable values that are fully constructed before they are exposed.
When mutation is necessary, keep it within the smallest scope. In **Rust**,
compute final values directly and use `mut` only where reassignment or mutable
borrowing requires it. In **TypeScript**, prefer `const` and `readonly` unless
the API contract requires mutation.

Borrow values for local use and clone only when an independent value or shared
ownership is required. To move a value out of a mutable location, use
`mem::take` or `mem::replace` to leave a valid replacement without cloning.

> [!NOTE]
> Further reading: **[aliasing in the Rustonomicon](https://doc.rust-lang.org/nomicon/aliasing.html)** and **[ownership and borrowing in The Rust Book](https://doc.rust-lang.org/book/ch04-01-what-is-ownership.html)**.
>
> Related lints: [needless_pass_by_ref_mut](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#needless_pass_by_ref_mut),
> [redundant_clone](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#redundant_clone),
> [complexity::clone_on_copy](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#clone_on_copy)
> and [style::unnecessary_mut_passed](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#unnecessary_mut_passed).

**Example:** Return computed values from helper functions instead of passing a
mutable struct through multiple modification steps.

**Avoid**

```rust
// Passing `&mut` across helpers obscures which functions modify which fields.
fn setup_session(session: &mut Session, auth: &AuthPayload) {
    authenticate_user(session, auth);
    assign_permissions(session, auth);
}

fn authenticate_user(session: &mut Session, auth: &AuthPayload) {
    session.user_id = Some(auth.user_id);
    session.authenticated = true;
}
```

**Prefer**

```rust
// Functions take immutable inputs and return values for explicit construction.
fn create_session(auth: &AuthPayload) -> Session {
    let user_id = authenticate_user(auth);
    let permissions = evaluate_permissions(auth);
    Session::new(user_id, permissions)
}
```

**Rationale:** Keeping mutation local makes state transitions and ownership easier to audit/understand.
