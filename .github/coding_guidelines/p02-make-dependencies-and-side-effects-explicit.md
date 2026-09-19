# P2. Pass dependencies and expose side effects

Give domain functions their dependencies as explicit inputs, including time,
configuration, external data and services. Construct those services in
orchestration code and keep environment, clock, network and storage access in
adapters or orchestration so callers can see where external effects begin.

Make mutation, resource creation, I/O, retries and caching apparent in the
operation's name and signature.

**Example:** Formatting a report should not hide a
[file write](https://doc.rust-lang.org/std/fs/fn.write.html). Let the caller
decide whether and where to save the result.

**Avoid**

```rust
fn format_report(total: u32) -> std::io::Result<String> {
    let report = format!("Total: {total}\n");
    std::fs::write("report.txt", &report)?;
    Ok(report)
}
```

**Prefer**

```rust
fn format_report(total: u32) -> String {
    format!("Total: {total}\n")
}

let report = format_report(total);
std::fs::write(path, &report)?;
```

**Rationale:** Explicit inputs and effects make behavior predictable and
testable.
