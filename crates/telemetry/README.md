In-repository telemetry crate for `o-sfu`.

This crate contains the runtime telemetry contract: tracing setup, event and field
schema, diagnostics DTOs and store, runtime metrics and Prometheus rendering.
JSON logs separate event fields from their parent span fields. Diagnostics expose
room, user, source and worker state without dashboard-specific projections.
