> [!WARNING]
> The `specifications` branch is separate from `master`.
> Documents here describe proposals and may differ from implemented behavior.

# O-SFU specifications

This branch hosts proposals, design discussions and specification reviews for
O-SFU. Synchronization with `master` is handled separately from specification PRs (through merge).

- Open specification PRs with **`specifications` as the target branch**.
- Keep all changes inside `__SPECIFICATIONS__/` (makes it easier to merge master into it).
- Commit messages to the `specifications` branch must have the following format: `[SPEC] spec_title: description`
- branch names for PRs that target `specification` should start with `spec/` or `specifications/`

Explain the motivation, proposed behavior, tradeoffs and acceptance criteria.
Cite relevant designs and make unresolved questions explicit.

Try to illustrate with code examples and API sketches so we can get a feel of what it would look like.

Merging a specification is for design agreement. Implementation is separate work.

Implementation tasks and their PRs should link to the specifications they
implement.
