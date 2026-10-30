# R5. Keep packet and frame processing cheap

Keep packet, frame and per-destination work bounded and free of steady-state
allocation. Reuse buffers, resolve policy and handles before repeated calls
and compute shared facts once. Avoid payload copies, formatting, metric
lookup, whole-room scans, blocking and contended locks.

Measure the production path under the same workload and compiler settings,
excluding setup and confirming that the workload reaches the operation being
measured. Choose evidence that answers the performance question: allocation
profiles for allocations, load tests for latency and instruction counts for
small synchronous operations.

> [!NOTE]
> Further reading: **[heap allocation costs](https://nnethercote.github.io/perf-book/heap-allocations.html)** and **[benchmark design](https://nnethercote.github.io/perf-book/benchmarking.html)**.
>
> Related lints: [assigning_clones](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#assigning_clones),
> [redundant_clone](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#redundant_clone),
> [pedantic::format_collect](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#format_collect),
> [pedantic::inefficient_to_string](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#inefficient_to_string),
> [pedantic::large_types_passed_by_value](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#large_types_passed_by_value),
> [perf::iter_overeager_cloned](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#iter_overeager_cloned),
> [perf::manual_memcpy](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#manual_memcpy),
> [perf::regex_creation_in_loops](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#regex_creation_in_loops)
> and [perf::unnecessary_to_owned](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#unnecessary_to_owned).

**Example:** `PacketLoopRoutingMissRecord::overwrite` reuses the retained packet
buffer when replacing an evicted cache entry. No allocation is needed when the
replacement packet fits its capacity.

**Avoid**

```rust
// This allocates a new buffer on every overwrite.
self.packet = packet.to_vec();
```

**Prefer**

```rust
// The retained capacity is reused whenever the next packet fits.
self.packet.clear();
self.packet.extend_from_slice(packet);
```

**Rationale:** Small costs multiply at media rate and reduce throughput.
