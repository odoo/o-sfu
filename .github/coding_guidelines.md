# Guidelines

The Rust compiler, Clippy and rustfmt enforce many local correctness and
formatting rules. These guidelines cover design choices they cannot enforce
such as API boundaries, ownership, side effects, compatibility and
verification.

Some rules may be a matter of opinion and some design philosophies may recomend otherwise, but gaving consistent patterns and expectations in a codebase is better than having contradictory patterns that coexist without a clear direction.

## Coding and design practices

### C. Correctness and safety

Correctness means that accepted values, state transitions and outcomes
preserve the system's domain invariants. Expressing those invariants through
types, owners and explicit proof obligations moves defect detection into
compilation and narrow boundaries where failures are cheaper to diagnose and
the remaining risk is easier to audit.

- [C1. Let the compiler enforce correctness](coding_guidelines/c01-let-the-compiler-enforce-correctness.md)
- [C2. Handle behavior-changing variants and fields explicitly](coding_guidelines/c02-make-domain-evolution-compiler-visible.md)
- [C3. Make units, overflow and clock choice explicit](coding_guidelines/c03-make-units-arithmetic-and-time-semantics-explicit.md)
- [C4. Enforce invariants where the state lives](coding_guidelines/c04-put-each-invariant-in-its-lowest-owner.md)
- [C5. Do not discard meaningful outcomes](coding_guidelines/c05-preserve-meaningful-outcomes.md)

### P. Program flow and effects

Software is easier to reason about when decisions are transformations of
explicit inputs and interactions with state or external systems remain
visible. Small contracts and clear separation between decisions and effects
make behavior easier to test, review and change without reconstructing hidden
control flow.

- [P1. Keep function inputs and outputs minimal](coding_guidelines/p01-request-and-return-only-what-a-function-needs.md)
- [P2. Pass dependencies and expose side effects](coding_guidelines/p02-make-dependencies-and-side-effects-explicit.md)
- [P3. Keep each function focused and at one level of detail](coding_guidelines/p03-keep-functions-focused-and-at-one-level.md)
- [P4. Keep decision logic pure](coding_guidelines/p04-keep-decision-logic-pure.md)
- [P5. Use combinators for queries and transformations](coding_guidelines/p05-use-combinators-for-queries-and-transformations.md)

### E. Errors and external boundaries

External boundaries separate trusted domain values from representations and
contracts the system cannot control. Early conversion and validation contain
malformed input while structured error categories, preserved causes and
documented dependency assumptions support correct handling, predictable
upgrades and diagnosis without exposing internal details.

- [E1. Preserve error categories and context](coding_guidelines/e01-make-errors-easy-to-handle-and-diagnose.md)
- [E2. Convert external formats at adapter boundaries](coding_guidelines/e02-keep-external-representations-at-the-edge.md)
- [E3. Validate external input before use](coding_guidelines/e03-treat-external-input-as-fallible.md)
- [E4. Rely only on documented dependency behavior](coding_guidelines/e04-depend-only-on-documented-behavior.md)

### S. State and synchronization

Shared state is correct only when every observer sees a valid combination of
values and every transition preserves its invariants. Clear ownership,
explicit validity rules for derived state and synchronization at the invariant
boundary prevent partial observations and stale work from becoming
behavior-changing races.

- [S1. Avoid unnecessary mutability](coding_guidelines/s01-avoid-unnecessary-mutability.md)
- [S2. Choose the right synchronzation primitives](coding_guidelines/s02-match-locks-and-atomics-to-the-state.md)
- [S3. Define how derived state stays valid](coding_guidelines/s03-define-how-derived-state-stays-valid.md)
- [S4. Separate intent from realization](coding_guidelines/s04-separate-intent-from-current-realization.md)

### O. Async ordering and lifecycle

Async concurrency separates the moments when an operation changes state,
performs external effects and reports completion to its caller. Explicit
boundaries and ordering prevent other tasks from observing incomplete
transitions and let shutdown distinguish accepted work from work that has
actually finished.

- [O1. Never block an async executor](coding_guidelines/o01-keep-blocking-work-off-async-executors.md)
- [O2. Release state guards before async effects](coding_guidelines/o02-make-asynchronous-commit-boundaries-explicit.md)
- [O3. Own spawned work through shutdown](coding_guidelines/o03-give-spawned-work-a-lifecycle-policy.md)
- [O4. Make cancellation behavior explicit](coding_guidelines/o04-make-cancellation-behavior-explicit.md)
- [O5. Match async primitives to delivery semantics](coding_guidelines/o05-choose-async-primitives-by-delivery-semantics.md)

### A. APIs and abstractions

APIs determine how much of a component's complexity every caller must
understand and coordinate. Boundaries that expose complete operations through
clear types and familiar conventions reduce caller decisions, prevent partial
use and keep invariants enforceable as implementations evolve.

- [A1. Use booleans only for clear facts](coding_guidelines/a01-make-mode-choices-explicit.md)
- [A2. Hide ordered steps behind one operation](coding_guidelines/a02-expose-complete-operations-not-partial-steps.md)
- [A3. Prefer existing APIs](coding_guidelines/a03-reuse-established-apis-and-crates.md)
- [A4. Add abstractions only when they simplify callers](coding_guidelines/a04-add-abstractions-only-when-callers-learn-less.md)
- [A5. Use the simplest construction API](coding_guidelines/a05-use-simple-construction-apis.md)

### M. Maintainability

Code is read and reviewed far more often than it is written. Lowering
cognitive overhead reduces reviewer burden, prevents regressions during
maintenance and sustains development velocity as the codebase grows.

- [M1. Comment contracts, not mechanics](coding_guidelines/m01-comment-contracts-not-mechanics.md)
- [M2. Use consistent names, explicit imports and narrow visibility](coding_guidelines/m02-make-names-imports-and-visibility-reveal-ownership.md)
- [M3. Choose the simplest clear design](coding_guidelines/m03-choose-the-simplest-clear-design.md)
- [M4. Keep related code together](coding_guidelines/m04-keep-related-code-together.md)
- [M5. Define shared decisions once](coding_guidelines/m05-define-shared-decisions-once.md)

## Repository guidelines

Changes that are correct in isolation can still fail at release and operations
boundaries. Repository-level contracts preserve interoperability between
versions, separate deployment policy from builds, isolate domain logic from
infrastructure, contain externally driven work, make failures visible and keep
tests representative of production behavior.

- [R1. Preserve compatibility between versions](coding_guidelines/r01-preserve-compatibility-boundaries.md)
- [R2. Configure operator choices at runtime](coding_guidelines/r02-select-operator-controlled-behavior-at-runtime.md)
- [R3. Keep tests and proofs out of production files](coding_guidelines/r03-keep-tests-and-proofs-out-of-production-files.md)
- [R4. Do not hide failures](coding_guidelines/r04-do-not-hide-failures.md)
- [R5. Keep packet and frame processing cheap](coding_guidelines/r05-keep-media-hot-paths-cheap.md)
- [R6. Limit work triggered by external input](coding_guidelines/r06-bound-externally-driven-work.md)
- [R7. Keep infrastructure out of domain crates](coding_guidelines/r07-preserve-crate-dependency-direction.md)
- [R8. Make tests prove real behavior](coding_guidelines/r08-test-observable-behavior-deterministically.md)
- [R9. Prefer `expect` for lint exceptions](coding_guidelines/r09-prefer-expect-for-lint-exceptions.md)


---

For formatting and contributor workflow, see [CONTRIBUTING.md](CONTRIBUTING.md).
For verification commands, see [`tests/README.md`](../tests/README.md).
