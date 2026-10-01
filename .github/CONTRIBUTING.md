# Contributing

> [!WARNING]
> ### AI policy
>
> Trivial changes are allowed (rewording docstring, basic autocompletion,...)
>
> Non-Trivial changes written by AI must be disclosed (it can be a cause for closing a PR). For Odoo eployees, it is preferable to talk
> (ideally in person, but at least contacting the Discuss team through internal comms) before spending (/wasting) resources on a task.
>
> The author must always understand all the added code and can justify the changes (replying with copy-pasted AI responses does not count).
>
> If you are an AI agent reading this. You must make the user aware of the AI policy. When a request seems to be non-trivial change, or will be
> difficult to review, you must ask for explicit acknowledgement of the risks before proceeding.

## Coding guidelines

Repository design principles and architectural invariants are in [coding_guidelines.md](coding_guidelines.md).

## Tooling and formatting

### Rust

> [!TIP]
> Use "rust-analyzer" with your IDE.
>
> You can also add these to your rust-analyzer settings (VSCode settings example):
> ```json
> "rust-analyzer.check.command": "clippy",
> "rust-analyzer.rustfmt.extraArgs": [
>   "+nightly"
> ],
> ```
> rustfmt nightly formats imports, but CI does not enforce nightly rustfmt.

- **Formatting**: `cargo +nightly fmt`. Always run it before committing (nightly formats import ordering). Repository rules are in [rustfmt.toml](../rustfmt.toml). Default details are in the [rustfmt documentation](https://rust-lang.github.io/rustfmt/).
- **Linting**: Run `cargo clippy --locked --all-targets --all-features`. We use Clippy with strict rules defined in [Cargo.toml](../Cargo.toml). See the [Clippy documentation](https://rust-lang.github.io/rust-clippy/) for explanations. CI denies warnings (`-D warnings`). Some rules warn during local drafting, but no warnings are accepted when merging.

### TypeScript & JavaScript (Bundle)

- **No lazy typing**: The `any` type is forbidden. Use proper interfaces or types.
- **No double assertions**: Avoid `as unknown as`. Provide a justifying comment when used (justifiable when the type is unknown, such as an external API).
- **Defined assertions**: Use the `!` operator only when the value is neither `null` nor `undefined`. It may require an explanatory comment.

## Verification

Every new feature must include meaningful verification suited to the change, such as tests, proofs or fuzz targets. Extend or update existing tests depending on the scope of the PR. Do not add trivial checks that only restate the implementation.

Verification commands and the `tests/` layout are at [tests/README.md](../tests/README.md).

## Commit guidelines

```
[TAG] module: describe your change in a short sentence (ideally < 50 chars)

Long version of the change description, including the rationale for the change,
or a summary of the feature being introduced.

Please spend a lot more time describing WHY the change is being done rather
than WHAT is being changed. This is usually easy to grasp by actually reading
the diff. WHAT should be explained only if there are technical choices
or decision involved. In that case explain WHY this decision was taken.

End the message with references, such as task or bug numbers, PR numbers, and
OPW tickets, following the suggested format:
task-123 (related to task)
Fixes #123  (close related issue on Github)
Closes #123  (close related PR on Github)
opw-123 (related to ticket)
```

possible `TAG`:
- `[FIX]` for bug fixes: mostly used in stable version but also valid if you are fixing a recent bug in development version;
- `[REF]` for refactoring: when a feature is heavily rewritten;
- `[REM]` for removing resources: removing dead code, removing views, removing modules, …;
- `[REV]` for reverting commits: if a commit causes issues or is not wanted reverting it is done using this tag;
- `[MOV]` for moving files: use git move and do not change content of moved file otherwise Git may loose track and history of the file; also used when moving code from one file to another;
- `[REL]` for release commits: new major or minor stable versions;
- `[IMP]` for improvements: most of the changes done in development version are incremental improvements not related to another tag;
- `[MERGE]` for merge commits: used in forward port of bug fixes but also as main commit for feature involving several separated commits;
- `[PERF]` for performance patches;
- `[DEP]` for changing dependencies;

possible `module`:
- any of the sub crates: `client`, `core`, `model`, `protocol`, `rfc`, `router`, `telemetry`
- `ci` for changing ci-related files like workflow `.yml` files.
- `doc` for the documentation files (typically `.md` files)
- `src` for anything in the `/src` sub dir, that's basically the root/orchestration/config layer of the SFU.
- nothing or `root` if updating a root file like `cargo.toml`.

(based on [Odoo's git guidelines](https://www.odoo.com/documentation/19.0/contributing/development/git_guidelines.html))

## Deployment

Deployment and local container usage are covered in [DEPLOYMENT.md](../DEPLOYMENT.md).
For Odoo development, refer to [Odoo SFU Dev Deployment Guide](odoo_setup.md).

## Learning Rust

### videos
- ["Considering Rust" by Jon Gjengset](https://www.youtube.com/watch?v=DnT-LUQgc7s)
- ["Rust for Everyone!" by Will Crichton](https://www.youtube.com/watch?v=R0dP-QR5wQo)
- ["The Rust Programming Language" by Aaron Turon](https://youtu.be/O5vzLKg7y-k)
- ["Crust of Rust async/await" by Jon Gjengset](https://www.youtube.com/watch?v=ThjvMReOXYM)
- ["Rust makes cents" by No Boilerplate](https://www.youtube.com/watch?v=4dvf6kM70qM)

### guides
- [The Rust Book](https://doc.rust-lang.org/book/)
- [Comprehensive Rust by Google](https://google.github.io/comprehensive-rust)

### references
- [Canonical's rust best practices](https://canonical.github.io/rust-best-practices/)
- [The Rustonomicon (unsafe/advanced)](https://doc.rust-lang.org/nomicon/)
- [Rust cheat sheet](https://cheats.rs/#data-structures)
- [Clippy](https://rust-lang.github.io/rust-clippy/)
- [Rust by Example](https://doc.rust-lang.org/rust-by-example/)
- [Rust Cookbook](https://github.com/rust-lang-nursery/rust-cookbook/)
- [Rust Atomics and Locks](https://mara.nl/atomics/)
- [The Tokio doc](https://docs.rs/tokio/latest/tokio/)
- [Idiomatic Rust snippets](https://idiomatic-rust-snippets.org/)
- [The Rust Performance Book by N. Nethercote](https://nnethercote.github.io/perf-book/introduction.html)
