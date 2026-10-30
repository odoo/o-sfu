# S4. Separate intent from realization

Keep the requested intent separate from the resource that fulfills it, so
resource loss, replacement or renegotiation cannot erase the request. Only the
domain operation responsible for that intent may remove it.

Give each pending realization an identity and accept its completion only while
that identity still matches the pending realization for the current request.

> [!NOTE]
> Further reading: **[the Kubernetes controller pattern](https://kubernetes.io/docs/concepts/architecture/controller/)** and **[optimistic concurrency control in Google Cloud](https://docs.cloud.google.com/java/docs/occ)**.

**Example:** `RouteGraph` keeps `Subscription::intent` when its current
publication detaches. `ConsumerRealization::Pending` carries a
`RouteReservationId` that rejects stale setup completion.

**Avoid**

```rust
struct Subscription {
    // Detaching the current publication would erase receiver intent.
    current: Option<CurrentPublication>,
}
```

**Prefer**

```rust
struct Subscription {
    // Intent survives publication detach or receiver replacement.
    intent: SourceSubscriptionIntent,
    current: Option<CurrentPublication>,
}

struct CurrentPublication {
    source_id: PublishedSourceId,
    selection: ConsumerSourceSelection,
    realization: ConsumerRealization,
}

#[derive(Debug, Default)]
enum ConsumerRealization {
    #[default]
    Absent,
    // The reservation ID rejects stale completions from older setup attempts.
    Pending(RouteReservationId, Option<RouteRelay>),
    Committed(
        TransportConsumerRoute,
        String,
        RoutedConsumerId,
        Option<RouteRelay>,
    ),
}
```

**Rationale:** A request may outlive several resources or setup attempts.
Separating their identities prevents a stale completion from modifying or
erasing a replacement.
