# o-sfu deployment

This document covers deploying `o-sfu` and [integrating it](#integration) with
an application. Odoo Discuss uses the same HTTP and browser APIs described here.

(for Odoo development, refer to [Odoo SFU Dev Deployment Guide](/.github/odoo_setup.md))

## typical traffic model

```text
HTTPS and WSS -> public reverse proxy -> o-sfu HTTP listener
WebRTC UDP    -> public VM IP       -> o-sfu RTC UDP range
```

the reverse proxy handles only HTTP and WebSocket traffic

media UDP must reach the VM public address directly because `o-sfu` advertises `ANNOUNCED_IP` in ICE-lite SDP

## Odoo binding

A single `o-sfu` deployment can serve many independent tenants, such as
different Odoo databases. Configure each database with the same public SFU URL
and shared server key shown below. Calls use separate, independently
authenticated rooms. The shared server key belongs only to trusted Odoo
backends. See [Multi-Tenant Segregation](/SECURITY.md#multi-tenant-segregation)
for the security model.

> [!WARNING]
> If tenants can see the o-sfu key, you should configure your SFU to be behind a reverse-proxy
> that whitelists the tenant server IP addresses for the control routes (`v1/channel` and `v1/disconnect`).
> Without doing that, a tenant may leak the key and be able to use the service outside of their Odoo instance.

on `o-sfu`:

```env
AUTH_KEY=<base64-auth-key>
ANNOUNCED_IP=<vm-public-ip>
```

or use the file option for the `AUTH_KEY` (recommended for extra security).
The file must be readable by the `o-sfu` process and contain only the base64 key (e.g. Docker secrets):

```env
AUTH_KEY_FILE=/run/secrets/o_sfu_auth_key
ANNOUNCED_IP=<vm-public-ip>
```

on Odoo Discuss settings:

```text
RTC Server URL = https://<sfu-domain>
RTC server KEY = <same value as AUTH_KEY>
```

## basic production environment

```env
PROXY=true
TRUSTED_PROXIES=127.0.0.1/32,::1/128
ANNOUNCED_IP=<vm-public-ip>
AUTH_KEY=<base64-auth-key>
DIAGNOSTICS_AUTH_TOKEN=<diagnostics-token>
RTC_MIN_PORT=40000
RTC_MAX_PORT=40099
TELEMETRY_LOG_FORMAT=json
TELEMETRY_DEPLOYMENT_ENVIRONMENT=production
```

`RTC_MIN_PORT` and `RTC_MAX_PORT` must match the cloud firewall, host firewall and container or service binding

`TELEMETRY_DEPLOYMENT_ENVIRONMENT=production` setting it to `production` makes the tracing switch to ratio-based sampling so only a subset of traces is captured to reduce load on the tracing system

`ROOM_MAX_LOCAL_ROUTERS` must be less than or equal to `RTC_MEDIA_WORKER_COUNT`

## security

see [SECURITY.md](SECURITY.md) for privacy and vulnerability reporting

### credentials

`AUTH_KEY` must match the application backend's signing key and be valid base64
that decodes to at least 32 cryptographically random bytes. For Odoo, configure
the same value as the RTC server key. Generate it and `DIAGNOSTICS_AUTH_TOKEN`
independently by running this command for each:

```bash
openssl rand -base64 32
```

### network and proxy trust

keep port `8070` unreachable from untrusted networks. Bind it to loopback for a
host proxy or expose it only on an isolated same-host container network

set `PROXY=true` with `TRUSTED_PROXIES` listing the proxy TCP peers as
comma-separated IP CIDRs. The list must be nonempty. The trusted public proxy
must strip or overwrite client-supplied forwarded headers. Use
`$proxy_add_x_forwarded_for` only when a trusted upstream has already stripped
client input

`/v1/stats`, `/metrics` and `/internal/diagnostics/...` require
the configured diagnostics token on every request. Without one, the
actual listener must use loopback. A same-host reverse proxy can reach that
fallback, so configure the token and block these routes at every public edge

port `8070` serves plaintext HTTP. Keep bearer traffic on loopback or an
isolated same-host network limited to trusted services. Otherwise use a
TLS-terminating proxy or authenticated encrypted overlay. A firewall alone
does not protect plaintext credentials

### io_uring

Docker's [default seccomp profile](https://docs.docker.com/engine/security/seccomp/)
blocks the `io_uring` system calls. Keep the default `tokio` backend unless the
container has a reviewed seccomp policy. `seccomp=unconfined` permits those
calls by removing the default syscall filter:

```yaml
security_opt:
  - seccomp=unconfined
```

### release integrity

promote only non-prerelease release-tag images and assets after completing the
verification steps under [image](#image) and [release assets](#release-assets)

## image

pull a CI-built image on the VM:

```text
ghcr.io/<owner>/o-sfu:<tag> // pattern
ghcr.io/odoo/o-sfu:v0.11.4 // a specific version
ghcr.io/odoo/o-sfu:latest // the latest release
ghcr.io/odoo/o-sfu:master // the last commit on master
ghcr.io/odoo/o-sfu:sha-dcd449bd27e15ddae3e786250c717a498c02a54e // a specifc commit
```

prefer release tags such as `v0.3.1` for production

do not promote suffixed tags such as `v0.3.1-rc.1` or
`v0.3.1-test.20260605` to production because they are published as prereleases
and are not marked as the latest GitHub release

use `sha-<commit>` tags for staging and test infrastructure that must track a
specific commit

use `master` only for staging flows that explicitly track GitHub Actions status

for a fixed production version:

```yaml
services:
  o-sfu:
    image: ghcr.io/<owner>/o-sfu:v0.3.1
```

or keep the version in the compose `.env` file:

```env
OSFU_VERSION=v0.3.1
```

```yaml
services:
  o-sfu:
    image: ghcr.io/<owner>/o-sfu:${OSFU_VERSION}
```

after changing the version, pull the configured image and recreate the service:

```bash
docker compose pull o-sfu
docker compose up -d o-sfu
```

`docker compose pull o-sfu` pulls the image tag configured for the `o-sfu`
service. the tag is selected by the `image` value in the compose file after
environment interpolation, not by the service name.

only release-tag image builds carry Docker provenance, SBOM and GitHub image
attestations. `master` and commit-addressable `sha-<commit>` images are
intentionally not attested.

you can verify a release image before updating production:

```bash
docker login ghcr.io
gh attestation verify oci://ghcr.io/<owner>/o-sfu:v0.3.1 -R <owner>/o-sfu
```

## release assets

tag pushes matching `v*` create a GitHub release with:

- `o-sfu-server-<tag>-linux-amd64.tar.gz`
- `o-sfu-client-<tag>.js`
- `o-sfu-client-<tag>.d.ts`
- `o-sfu-image-<tag>.sbom.json`
- `SHA256SUMS`

suffixed tags are generated validation builds with the same assets,
attestations and version-tag image, while the GitHub release is marked as a
prerelease and is not marked latest

the server asset contains the release Linux `o-sfu` binary

the client JavaScript asset is the Odoo-compatible `odoo_sfu.js` bundle with
embedded WASM. the same-basename declaration asset carries its public types and
API documentation for TypeScript and editor tooling. see the
[client API reference](crates/client/API.md)

the SBOM asset is extracted from the version-tag container image SBOM

release artifacts are covered by GitHub artifact attestations. after
downloading an asset:

```bash
gh attestation verify <asset> -R <owner>/o-sfu
sha256sum -c SHA256SUMS
```

prefer release assets for production rollout

keep `ghcr.io/<owner>/o-sfu:sha-<commit>` images for staging and test
infrastructure that tracks commit-level container packages

## runtime binding

for Docker Compose, publish the same UDP range as the RTC env range:

```yaml
x-logging: &bounded-logs
  driver: json-file
  options:
    max-size: "20m"
    max-file: "5"
    labels: "com.odoo.sfu.component"

services:
  o-sfu:
    image: ghcr.io/<owner>/o-sfu:<tag>
    restart: unless-stopped
    labels:
      com.odoo.sfu.component: server
    logging: *bounded-logs
    env_file: /etc/o-sfu/o-sfu.env
    expose:
      - "8070"
    ports:
      - "40000-40099:40000-40099/udp"
```

the service litsens on `8070` inside the compose network

expose that port to other containers, but only publish it to the host when the
reverse proxy also runs on the host

### host NGINX

if NGINX runs on the host, publish the HTTP listener on loopback with a local
compose overlay:

```yaml
services:
  o-sfu:
    ports:
      - "127.0.0.1:8070:8070/tcp"
```

this keeps the SFU HTTP listener off the public interface while still letting
host NGINX proxy to `http://127.0.0.1:8070`

### containerized NGINX

containerized NGINX should instead join the compose network and proxy to
`http://o-sfu:8070`

do not publish `8070/tcp` on the host for this layout

only the reverse proxy should expose public HTTP andTLS

### Docker log ingestion

the `logging` block bounds the container stdout and stderr log store at the
source with the Docker `json-file` driver

the `com.odoo.sfu.component=server` label is copied into Docker log records
because the logging options include `labels: "com.odoo.sfu.component"`

the reference `o-sfu-telemetry` VPS profile uses that label to ingest only SFU
container logs from Docker's rotated `json-file` log store

use `TELEMETRY_LOG_FORMAT=json` for structured `o-sfu` log bodies

with that setting, `o-sfu` writes one JSON object per stdout or stderr line

the Docker `json-file` driver wraps each line in its own record

Collectors parse the outer Docker record first, then parse its `log` string
as the `o-sfu` JSON payload:

```json
{
  "log": "{\"timestamp\":\"2026-07-09T10:12:34.567890123Z\",\"level\":\"INFO\",\"target\":\"o_sfu::runtime::http_server::server\",\"service.name\":\"o-sfu\",\"service.version\":\"0.7.0\",\"service.instance.id\":\"pid-1\",\"deployment.environment\":\"production\",\"fields\":{\"event\":\"http.listener.ready\",\"message\":\"booted HTTP and WebSocket listener\",\"bind_address\":\"0.0.0.0:8070\",\"local_address\":\"0.0.0.0:8070\",\"trust_proxy_headers\":true},\"spans\":[]}\n",
  "stream": "stdout",
  "time": "2026-07-09T10:12:34.568000000Z"
}
```

The decoded payload separates event fields from span context:

```json
{
  "timestamp": "2026-07-09T10:12:34.567890123Z",
  "level": "INFO",
  "target": "o_sfu::runtime::http_server::server",
  "service.name": "o-sfu",
  "service.version": "0.7.0",
  "service.instance.id": "pid-1",
  "deployment.environment": "production",
  "fields": {
    "event": "http.listener.ready",
    "message": "booted HTTP and WebSocket listener",
    "bind_address": "0.0.0.0:8070",
    "local_address": "0.0.0.0:8070",
    "trust_proxy_headers": true
  },
  "spans": []
}
```

| field | type | value |
| --- | --- | --- |
| `timestamp` | string | RFC 3339 UTC timestamp generated when the event is formatted |
| `level` | string | tracing level such as `INFO`, `WARN` or `ERROR` |
| `target` | string | Rust tracing target that emitted the event |
| `service.name` | string | `TELEMETRY_SERVICE_NAME` defaulting to `o-sfu` |
| `service.version` | string | compiled `o-sfu` crate version |
| `service.instance.id` | string | `TELEMETRY_SERVICE_INSTANCE_ID` defaulting to `pid-<pid>` |
| `deployment.environment` | string | `TELEMETRY_DEPLOYMENT_ENVIRONMENT` defaulting to `local` |
| `trace_id` | string | optional derived trace ID from the event's tracing context |
| `fields` | object | values recorded on the event, including `event` and `message` when supplied |
| `spans` | array | event parent scope from root to leaf, or an empty array outside a span |

Each span is `{ "name": "<span name>", "fields": {} }`. Its fields retain all
recorded values and later `Span::record` updates. Numbers recorded as numbers
remain JSON numbers. Event and span fields remain separate.

Consumers that need flattened correlation IDs first read the event's `fields`,
then search span `fields` from the nearest parent toward the root. This applies
to IDs such as `room_id`, `user_id` and `connection_id`. Treat `fields.event` as
an optional discriminator. For the first `transport.health.changed` transition,
`fields.from` is absent because no previous state exists.

The event and field catalog is in
`crates/telemetry/src/schema.rs` and the formatter is in
`crates/telemetry/src/setup.rs`.

`source_policy.route_changed` reports a receiver video route transition only
after the media transport and current room topology accept it. `outcome` is
`degraded`, `paused` or `resumed`. A pause reports the applied policy `reason`.
A resume reports the policy `reason` that was cleared. Route identity, receiver
bandwidth, selected budget and selected encoding fields provide the route-level
context. `planned_active_video_route_count` and
`planned_selected_video_bitrate_bps` report the final post-dwell solver
snapshot. They can include a sibling route displaced while transport work was
in flight.

`osfu_budget_solver_outcomes_total` counts the same committed transitions.
Pending dwell, pause reason replacements, budget-only updates, initial
selector resolution and rejected stale work do not increment it.

## NGINX public edge

The example limits each client IP to 64 connections and 10 requests per second.
Tune both for shared NATs. Apply these limits at the public edge before proxying.
NGINX counts connections only after complete headers, so set its header timeout too.

```nginx
limit_conn_zone $binary_remote_addr zone=sfu_connections:10m;
limit_req_zone $binary_remote_addr zone=sfu_requests:10m rate=10r/s;

map $http_upgrade $connection_upgrade {
    default upgrade;
    "" close;
}

server {
    listen 443 ssl http2;
    server_name <sfu-domain>;
    client_header_timeout 10s;

    ssl_certificate <certificate-path>;
    ssl_certificate_key <certificate-key-path>;

    location = /metrics {
        return 404;
    }

    location = /v1/stats {
        return 404;
    }

    location ^~ /internal/diagnostics/ {
        return 404;
    }

    location / {
        limit_conn sfu_connections 64;
        limit_req zone=sfu_requests burst=20 nodelay;
        proxy_pass http://127.0.0.1:8070;
        proxy_http_version 1.1;
        proxy_read_timeout 75s;

        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $remote_addr;
        proxy_set_header X-Forwarded-Host $host;
        proxy_set_header X-Forwarded-Proto $scheme;

        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection $connection_upgrade;
    }
}
```

## private observability

use the telemetry reference for exact queries and response shapes:

- [Prometheus metrics](https://odoo.github.io/o-sfu/o_sfu/http/telemetry/metrics/index.html)
- [HTTP diagnostics](https://odoo.github.io/o-sfu/o_sfu/http/telemetry/diagnostics/index.html)

remote Prometheus scrape through a private TLS endpoint when
a diagnostics token is configured:

```yaml
scrape_configs:
  - job_name: o-sfu
    scheme: https
    metrics_path: /metrics
    authorization:
      type: Bearer
      credentials_file: /run/secrets/o_sfu_diagnostics_token
    tls_config:
      ca_file: /run/secrets/o_sfu_observability_ca
      server_name: o-sfu-observability.internal
    static_configs:
      - targets: ["o-sfu-observability.internal:443"]
```

the private endpoint proxies operator routes to `http://127.0.0.1:8070`

private stats and diagnostics access:

```bash
curl -H 'Authorization: Bearer <diagnostics-token>' \
  https://o-sfu-observability.internal/v1/stats
curl -H 'Authorization: Bearer <diagnostics-token>' \
  https://o-sfu-observability.internal/internal/diagnostics/summary
```

## rollout validation

```bash
curl -i https://<sfu-domain>/v1/noop
curl -i https://<sfu-domain>/v1/stats
curl -i https://<sfu-domain>/metrics
curl -i https://<sfu-domain>/internal/diagnostics/summary
```

expected:

```text
/v1/noop -> 200 with {"result":"ok"}
/v1/stats -> 404
/metrics -> 404
/internal/diagnostics/summary -> 404
```

confirm direct port `8070` is unreachable from untrusted networks and authorized
private operator requests return `200`. Then validate a real browser join
from Odoo because HTTP health does not validate the UDP media path

## deployment checklist

network:

- cloud firewall allows `443/tcp`
- cloud firewall allows the configured RTC UDP range
- Google Cloud firewall rule targets match the VM network tags when tags are used
- the VM has the `sfu-server` network tag when the SFU firewall rule targets it
- host firewall such as UFW allows the configured RTC UDP range
- Docker or systemd exposes the same UDP range as `RTC_MIN_PORT` and `RTC_MAX_PORT`
- host NGINX deployments publish `o-sfu` HTTP only on `127.0.0.1:8070`

proxy:

- NGINX terminates TLS for `<sfu-domain>`
- NGINX proxies to the actual `HTTP_INTERFACE`
- NGINX uses HTTP/1.1 upstream for WebSocket upgrade support
- NGINX forwards `Upgrade` and `Connection`
- NGINX overwrites `X-Forwarded-For`, `X-Forwarded-Host`, `X-Forwarded-Proto`, `X-Real-IP` and `Host`
- `/v1/stats`, `/metrics` and `/internal/diagnostics/...` are not public

runtime:

- `ANNOUNCED_IP` is the VM public IP
- `ANNOUNCED_IP` is not the NGINX domain
- `ANNOUNCED_IP` is not `0.0.0.0`
- Docker Compose logging uses explicit `max-size` and `max-file` limits
- Docker Compose logging uses `json-file` when `o-sfu-telemetry` ingests Docker logs
- `o-sfu` has the `com.odoo.sfu.component=server` Docker label
- `PROXY=true` requires `TRUSTED_PROXIES` matching the NGINX peer address
- `AUTH_KEY` matches the Odoo caller configuration
- `AUTH_KEY` decodes to at least 32 bytes generated with cryptographically safe randomness
- `DIAGNOSTICS_AUTH_TOKEN` is generated independently from at least 32 random bytes
- `RTC_MEDIA_WORKER_COUNT` fits the VM capacity
- `ROOM_MAX_LOCAL_ROUTERS` does not exceed `RTC_MEDIA_WORKER_COUNT`

validation:

- `GET /v1/noop` succeeds through HTTPS
- public `/v1/stats`, `/metrics` and `/internal/diagnostics/summary` return `404`
- private operator requests send `DIAGNOSTICS_AUTH_TOKEN` over a confidential transport
- browser join through Odoo succeeds
- if HTTP succeeds but media fails, check UDP firewalls and the `sfu-server` tag first

## environment variables

required:

| variable | default | description |
| --- | --- | --- |
| `ANNOUNCED_IP` | required | concrete advertised IP address used in ICE-lite SDP |
| `AUTH_KEY` | required | base64 key with at least 32 decoded bytes used to sign and verify SFU JWTs |

HTTP and operator access:

| variable | default | description |
| --- | --- | --- |
| `HTTP_INTERFACE` | `0.0.0.0:8070` | HTTP and WebSocket listening address |
| `MAX_HTTP_CONNECTIONS` | `4096` | concurrent accepted sockets per process, including authenticated WebSockets until they close |
| `HEADER_READ_TIMEOUT` | `10` | seconds from acceptance to first headers and for subsequent HTTP/1 headers, including keep-alive idle time, from `1` to `86400` |
| `PROXY` | `false` | when `true`, requires nonempty `TRUSTED_PROXIES` IP CIDRs and trusts forwarded metadata only from matching TCP peers |
| `DIAGNOSTICS_AUTH_TOKEN` | unset | bearer token of at least 32 bytes after trimming whitespace for `/v1/stats`, `/metrics` and `/internal/diagnostics/...`. Tokenless access requires the actual listener to use loopback |
| `SHUTDOWN_TIMEOUT_MS` | `10000` | total deadline in milliseconds for listener, WebSocket session, background task and RTC worker drainage, from `1` to `86400000` |

Set `RLIMIT_NOFILE` above `MAX_HTTP_CONNECTIONS` with headroom for UDP sockets,
logs and other process descriptors. The HTTP cap also limits concurrent WebSocket users.

authentication and websocket admission:

| variable | default | description |
| --- | --- | --- |
| `AUTHENTICATION_TIMEOUT_MS` | `10000` | first authenticated WebSocket frame timeout in milliseconds, from `1` to `86400000` |
| `MAX_PRE_AUTH_WEBSOCKET_SESSIONS` | `512` | process-wide cap for upgraded WebSockets waiting for authentication |
| `MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN` | `16` | per IPv4 address or IPv6 /64 cap for upgraded WebSockets waiting for authentication |

room and user limits:

| variable | default | description |
| --- | --- | --- |
| `ROOM_SIZE` | `100` | maximum concurrent users per room |
| `USER_TIMEOUT_MS` | `10000` | idle user timeout in milliseconds, from `1` to `86400000` |
| `PING_INTERVAL_MS` | `60000` | signaling ping interval in milliseconds, from `1` to `86400000` |
| `USER_OUTBOUND_QUEUE_CAPACITY` | `128` | per-user WebSocket room-event queue depth |
| `USER_OUTBOUND_QUEUE_BYTE_CAPACITY` | `2097152` | per-user WebSocket queued-byte budget |
| `ROOM_RESERVATION_TTL` | `60` | time-to-live for unjoined rooms in seconds |
| `ROOM_DEPARTURE_GRACE` | `60` | empty room grace period before removal (seconds) |

RTC transport:

| variable | default | description |
| --- | --- | --- |
| `RTC_MIN_PORT` | `40000` | lower bound for the RTC UDP port range, must be greater than zero |
| `RTC_MAX_PORT` | `49999` | upper bound for the RTC UDP port range |
| `RTC_UDP_IO_BACKEND` | `tokio` | UDP socket backend for RTC workers, either `tokio` or Linux-only `io_uring` |
| `RTC_MEDIA_WORKER_COUNT` | available parallelism | number of RTC media workers, falling back to `1` when the host cannot report available parallelism |
| `MAX_BITRATE_IN` | `8000000` | maximum incoming bitrate in bps per user |
| `MAX_BITRATE_OUT` | `10000000` | receiver-side BWE ceiling in bps per user |
| `MAX_VIDEO_BITRATE` | `4000000` | maximum bitrate in bps for the highest default simulcast video layer metadata |

room worker placement:

| variable | default | description |
| --- | --- | --- |
| `ROOM_MAX_LOCAL_ROUTERS` | `1` | maximum workers a room may use, with `1` disabling spillover |
| `ROOM_SPILLOVER_PACKET_LOOP_DELAY_MS` | `20` | packet-loop service delay that marks an assigned worker unhealthy after two consecutive observations |

Rooms remain on an assigned healthy worker. A join attaches an unused healthy
worker only when every assigned worker is unhealthy and the router cap permits
it. A missed heartbeat is unhealthy after one full interval. The `300 ms`
grace applies only before the first heartbeat.

media policy and codecs:

| variable | default | description |
| --- | --- | --- |
| `ROOM_MAX_ACTIVE_AUDIO_SPEAKERS` | `4` | maximum active audio speakers forwarded by room media policy |
| `ROOM_MAX_VIDEO_DOWNLOADS_PER_RECEIVER` | `10` | maximum active video source downloads per receiver |
| `CODEC_OPUS` | `true` | enables Opus audio |
| `CODEC_PCMU` | `false` | enables G.711 mu-law audio |
| `CODEC_PCMA` | `false` | enables G.711 a-law audio |
| `CODEC_VP8` | `true` | enables VP8 video |
| `CODEC_H264` | `false` | enables H.264 video |
| `CODEC_H265` | `false` | enables H.265 video |
| `CODEC_VP9` | `false` | enables VP9 video |
| `CODEC_AV1` | `false` | enables AV1 video |
| `CODEC_AUDIO_PREFERENCE` | `opus,PCMU,PCMA` | optional comma-separated audio codec preference order |
| `CODEC_VIDEO_PREFERENCE` | `VP8,H264,H265,VP9,AV1` | optional comma-separated video codec preference order. The first enabled entry selects layered upload eligibility |

receiver video adaptation tuning:

| variable | default | description |
| --- | --- | --- |
| `ROOM_MULTIPARTY_SCALABLE_VIDEO_THRESHOLD` | `3` | receiver count at or above which scalable video is layer-selected per receiver instead of forwarded at full quality |
| `ROOM_THUMBNAIL_BUDGET_DIVISOR` | `2` | divisor applied to the per-source budget when a source is shown as a thumbnail |
| `ROOM_SOFT_PAUSE_DWELL_MS` | `750` | positive duration of continuous receiver pressure before soft policy pauses |
| `ROOM_UPGRADE_DWELL_MS` | `750` | positive duration of continuous eligibility for the exact post-fit upgrade or soft-resume target |
| `ROOM_RECEIVER_BUDGET_HEADROOM_PERCENT` | `0` | percent of the receiver bandwidth estimate held back from the video budget for RTP, RTX and FEC overhead, from `0` to `100` |
| `ROOM_AUDIO_RESERVE_PER_SPEAKER_BPS` | `0` | fixed bitrate in bps held back from each receiver's video budget for every admitted audio speaker that receiver consumes; a receiver with audio disabled reserves nothing; `0` disables audio reservation |

Eligible layer downsteps are immediate. Soft pauses may keep the selected video
bitrate above the receiver budget until `ROOM_SOFT_PAUSE_DWELL_MS` expires.
Hard media limits remain immediate. Both dwells are bounded to
`3153600000000` ms to keep deadline addition within the portable `Instant`
range.

telemetry:

| variable | default | description |
| --- | --- | --- |
| `RUST_LOG` | `info` | `tracing-subscriber` env filter |
| `TELEMETRY_LOG_FORMAT` | `compact` | log output format, either `compact` or `json` |
| `TELEMETRY_SERVICE_NAME` | `o-sfu` | service name in telemetry resource metadata |
| `TELEMETRY_DEPLOYMENT_ENVIRONMENT` | `local` | deployment environment in telemetry resource metadata |
| `TELEMETRY_SERVICE_INSTANCE_ID` | `pid-<pid>` | stable service instance id override |
| `TELEMETRY_MEDIA_QUALITY_INTERVAL_MS` | `5000` | sampled media-quality telemetry interval in milliseconds, from `0` to `86400000`, with `0` disabling sampling |
| `TELEMETRY_OTLP_ENDPOINT` | disabled | optional OTLP HTTP traces endpoint, normalized to `/v1/traces` |

feature flags:

| variable | default | description |
| --- | --- | --- |
| `FEATURE_TRANSCRIPTION` | `false` | enables transcription intent flags, currently WIP |
| `FEATURE_AUDIO_RECORDING` | `false` | enables audio recording intent flags, currently WIP |
| `FEATURE_VIDEO_RECORDING` | `false` | enables video recording intent flags, currently WIP |

## integration

`o-sfu` can provide audio and video calls for any application that uses its
HTTP API and browser client. It does not necessarily require an Odoo backend (even though Odoo is the main target).
Your application authenticates users, decides who may join each call and issues
their SFU credentials.

After deployment, give your backend the public SFU address, such as
`https://sfu.example.com`, and the configured `AUTH_KEY`. The proxy must route
`GET /v1/channel` to the SFU and accept WebSocket upgrades at `/`. The browser
must also be able to reach the advertised media address and ports described
under [typical traffic model](#typical-traffic-model).

```text
Application backend -- AUTH_KEY-signed JWT --> GET /v1/channel
Application backend <-- room UUID and URL -- o-sfu
Application browser <-- room URL and user JWT -- application backend
Application browser -- SfuClient / WebSocket / WebRTC --> o-sfu
```

Keep `AUTH_KEY`, key seeds, room signing keys and provisioning JWTs on the backend.
The browser receives only its participant JWT and connection details. A JWT
is signed rather than encrypted, so its claims are readable by its holder.

### provision a room

Choose a distinct `iss` string for each application call, including a tenant
namespace when several tenants share the SFU. For example,
`my-platform:tenant-42:call-123` identifies one call rather than every call in
tenant 42. Prefer `keySeed` for provisioning. Generate 32 cryptographically
random bytes once per call, base64-encode them and retain that seed on the
backend. Reuse it for subsequent provisioning requests for the same call.

Use a JWT library to sign the following payload with `HS256` and the
**base64-decoded bytes of `AUTH_KEY`**. Do not use the base64 text as the HMAC
key. Set the header to `{"alg":"HS256","typ":"JWT"}`. Construct the claims
on your backend immediately before signing and sending the request. This
JavaScript example gives the token a 60-second lifetime:

```js
const provisioningClaims = {
    iss: "my-platform:tenant-42:call-123",
    exp: Math.floor(Date.now() / 1000) + 60,
    keySeed: "<base64-encoded per-call seed>",
}
```

| Provisioning claim | Requirement | Meaning |
| --- | --- | --- |
| `iss` | Required string | Identifies the current room for this application call |
| `exp` | Required number | Expiration in Unix seconds, strictly after the SFU's current time |
| `keySeed` | Required unless `key` is supplied | Recommended. Nonempty base64 seed used to derive the room signing key |
| `key` | Optional alternative to `keySeed` | Explicit base64 room signing key with at least 32 decoded bytes |
| `nbf` | Optional number | Earliest acceptance time in Unix seconds |
| `iat` | Optional number | Issue time in Unix seconds, at most 60 seconds ahead of the SFU clock |
| `sub`, `jti` | Optional strings | Not used to authorize room creation |
| `aud` | Optional string or string array | Not used to authorize room creation |

For `keySeed`, both your backend and the SFU derive the same raw signing bytes:

```text
room_key = HMAC-SHA256(Base64Decode(AUTH_KEY), Base64Decode(keySeed))
```

`keySeed` takes precedence when both claims are present. Supply one key form
and retain it for subsequent requests for the same call. Key and seed values
accept standard or URL-safe base64, with or without padding. JWT segments use
base64url without padding.

Send the token in the `Authorization` header. This endpoint uses `GET` with
query parameters and no request body. It does not accept the token in the URL.
The legacy `Authorization: jwt <token>` form is also accepted.

```bash
curl --fail-with-body \
  --header "Authorization: Bearer ${PROVISIONING_JWT}" \
  'https://sfu.example.com/v1/channel?webRTC=true'
```

| Query parameter | Requirement | Meaning |
| --- | --- | --- |
| `webRTC` | Optional, defaults to `true` | Boolean `true` or `false`. Keep it enabled for browser audio and video |
| `recordingAddress` | Optional string | Compatibility parameter. Recording is currently unavailable, so supplying it does not enable recording |

A successful request returns `200 OK` and JSON:

```json
{
  "uuid": "8e15fb78-b6b8-4b13-9bc4-92f47688691b",
  "url": "https://sfu.example.com"
}
```

Store the returned `uuid` alongside the call's issuer and seed or explicit
room key. The UUID identifies the SFU room and is distinct from your `iss`.
The returned `url` comes from the request host and scheme, including trusted
proxy headers.
Call the endpoint through the public SFU address and configure proxy forwarding
so this URL is reachable by browsers.

Repeating the request with the same `iss`, decoded room key and query
configuration returns the current room. A different key or configuration for
that issuer returns `409 Conflict`. Reuse the call's seed or explicit room key
for every participant. Rooms reside in memory and can disappear after an
empty-room timeout or server restart. Provision again when needed and use the new UUID
in newly issued participant tokens. Until the first participant joins,
`ROOM_RESERVATION_TTL` limits the reservation lifetime, which defaults to
60 seconds. This room deadline is separate from JWT expiry. Provision close to
the join attempt or renew the reservation with a matching request.
After a successful join, the reservation deadline ends.
The last participant's normal departure starts `ROOM_DEPARTURE_GRACE`.

| HTTP status | Cause |
| --- | --- |
| `400 Bad Request` | Invalid query, no key claim or invalid room key or seed |
| `401 Unauthorized` | Missing or invalid authorization, unsupported algorithm, bad signature or invalid time claims |
| `403 Forbidden` | Verified provisioning JWT has no `iss` |
| `409 Conflict` | Current room for `iss` has a different key or configuration |

### issue a JWT for each participant

After your backend authorizes a user to join the call, sign a participant JWT
with `HS256` using the **raw room signing bytes** derived from `keySeed` by
the HMAC above. If provisioning used an explicit `key`, decode its base64
value instead. Do not sign participant JWTs with `AUTH_KEY` or the seed itself.

Use the returned room UUID as `room_id` and a participant identifier unique
within that room as `user_id`. Neither identifier needs to come from Odoo.
Issue the token immediately before the browser connects. This backend example
also gives it a 60-second admission lifetime:

```js
const participantClaims = {
    room_id: "8e15fb78-b6b8-4b13-9bc4-92f47688691b",
    user_id: "participant-7",
    exp: Math.floor(Date.now() / 1000) + 60,
}
```

| Participant claim | Requirement | Meaning |
| --- | --- | --- |
| `exp` | Required number | Expiration in Unix seconds |
| `user_id` | Required unless `session_id` is supplied | Participant identity as an integer or string |
| `session_id` | Optional alternative to `user_id` | Takes precedence when both are present, for example a connection identity separate from an account ID |
| `room_id` | Required unless the client supplies `channelUUID` | SFU room UUID. `sfu_channel_uuid` is an accepted alias |
| `label` | Optional string | Accepted but currently unused by room admission |
| `permissions` | Optional object | Accepts `transcription`, `audioRecording` and `videoRecording` booleans. Currently not enforced and does not enable recording |
| `nbf`, `iat` | Optional numbers | Same time validation as provisioning JWTs |
| `iss`, `sub`, `jti` | Optional strings | Not used to authorize participant admission |
| `aud` | Optional string or string array | Not used to authorize participant admission |

Include `room_id` in new integrations. If both it and `channelUUID` are
supplied, they must identify the same room. A string participant identity can
contain at most 256 UTF-8 bytes. Numeric strings normalize to integers, so
`"7"` and `7` identify the same participant. A new connection with the same
identity replaces the existing connection. Use separate identities for
simultaneous tabs or devices that should remain connected.

Both token types require `exp` and accept only `HS256`. `nbf` cannot be in the
future and `iat` can be omitted. Tokens are limited to 16 KiB. Expiry is checked
when authenticating, including reconnections. It does not end an already
authenticated call. Obtain fresh credentials from your backend when an expired
token prevents reconnection.

Return connection details to the authorized browser through an endpoint in
your application. The browser example below assumes that endpoint returns:

```json
{
  "url": "https://sfu.example.com",
  "uuid": "8e15fb78-b6b8-4b13-9bc4-92f47688691b",
  "jwt": "<participant JWT>"
}
```

### serve the browser bundle

Download `o-sfu-client-<tag>.js`, `o-sfu-client-<tag>.d.ts` and `SHA256SUMS` from the
[GitHub release](https://github.com/odoo/o-sfu/releases) matching your deployed
server version. Set `SFU_TAG` to that release tag and verify the downloaded
client assets before renaming them:

```bash
gh attestation verify "o-sfu-client-${SFU_TAG}.js" -R odoo/o-sfu
gh attestation verify "o-sfu-client-${SFU_TAG}.d.ts" -R odoo/o-sfu
sha256sum --check --ignore-missing SHA256SUMS
```

`--ignore-missing` skips server and SBOM files when only the client was
downloaded. Both client files must be present and report `OK`. See
[release assets](#release-assets) for the complete release contents.

Serve the JavaScript file with your application's static assets. If you rename
it to `o-sfu-client.js`, rename the declaration to `o-sfu-client.d.ts` and keep
the files together for TypeScript and editor support.

The bundle is a browser ES module with embedded WASM. It does not require an
Odoo JavaScript runtime or a separate `.wasm` download. Import it from a module
script or your application's browser build, just sererve it along the rest of your front-end.
Its content security policy must allow the bundle, WebAssembly execution and a connection to your SFU's `wss:` address.

### connect and exchange media

The following module assumes a call page with `#join` and `#leave` buttons and
a `#remote-media` container. `/api/calls/123/join` belongs to your application,
not to `o-sfu`. It authenticates the caller, checks call access and returns the
connection details above. Use your application's normal request and CSRF
handling for that endpoint.

```js
import { CLIENT_UPDATE, SfuClient } from "/static/o-sfu-client.js"

const sfu = new SfuClient()
const remoteMedia = new Map()
const joinButton = document.querySelector("#join")
const leaveButton = document.querySelector("#leave")
leaveButton.disabled = true
let localStream

function releaseMedia() {
    localStream?.getTracks().forEach((track) => track.stop())
    localStream = undefined
    for (const { element } of remoteMedia.values()) {
        element.srcObject = null
        element.remove()
    }
    remoteMedia.clear()
    joinButton.disabled = false
    leaveButton.disabled = true
}

sfu.addEventListener("stateChange", ({ detail }) => {
    console.log("SFU state:", detail.state)
    if (detail.state === "closed" || detail.state === "disconnected") {
        releaseMedia()
    }
})
sfu.addEventListener("handledError", ({ detail }) => {
    console.error(detail.error)
})
sfu.addEventListener("update", ({ detail }) => {
    if (detail.name === CLIENT_UPDATE.TRACK) {
        const { sessionId, type, track, active } = detail.payload
        const key = JSON.stringify([sessionId, type])
        let entry = remoteMedia.get(key)
        if (!entry) {
            const element = document.createElement(type === "audio" ? "audio" : "video")
            element.autoplay = true
            element.controls = true
            element.playsInline = true
            document.querySelector("#remote-media").append(element)
            entry = { sessionId, element }
            remoteMedia.set(key, entry)
        }
        entry.element.srcObject = active ? new MediaStream([track]) : null
    } else if (detail.name === CLIENT_UPDATE.DISCONNECT) {
        for (const [key, entry] of remoteMedia) {
            if (entry.sessionId === detail.payload.sessionId) {
                entry.element.remove()
                remoteMedia.delete(key)
            }
        }
    }
})

joinButton.addEventListener("click", async () => {
    joinButton.disabled = true
    try {
        localStream = await navigator.mediaDevices.getUserMedia({ audio: true, video: true })
        const response = await fetch("/api/calls/123/join", { method: "POST" })
        if (!response.ok) throw new Error(`Join failed: ${response.status}`)
        const { url, uuid, jwt } = await response.json()
        sfu.connect(url, jwt, { channelUUID: uuid })
        sfu.publish("audio", localStream.getAudioTracks()[0])
        sfu.publish("camera", localStream.getVideoTracks()[0])
        leaveButton.disabled = false
    } catch (error) {
        console.error(error)
        sfu.disconnect()
        releaseMedia()
    }
})
leaveButton.addEventListener("click", () => {
    sfu.disconnect()
    releaseMedia()
})
```

The example obtains microphone and camera permission before requesting the
participant token so time spent in the permission prompt does not consume its
admission lifetime.

`connect()` returns before authentication and media negotiation finish. Listen
for `stateChange` and `handledError` rather than awaiting it. The client sends
the authentication frame and handles SDP negotiation. `https:` addresses are
converted to `wss:`. `channelUUID` is optional when the JWT includes `room_id`.
Pass `iceServers` in the same options object if your deployment provides STUN
or TURN services.

Remote publications are received by default and arrive through
`CLIENT_UPDATE.TRACK`. Browser autoplay policy can require the user to start
playback with the media controls. Use
`sfu.subscribe(remoteParticipantId, { audio: true, camera: true, screen: false })`
to change what a participant receives. Publish a screen capture with
`sfu.publish("screen", screenTrack)` and pause a publication with
`sfu.publish("camera", null)`.

The example stops capture and clears remote media on leave or terminal
disconnection. `sfu.disconnect()` alone does not release camera or microphone
tracks captured by your application. Keep those tracks during `recovering`
so the client can replay publication intent. Each new join requests fresh
credentials. See the [client API reference](crates/client/API.md) for connection
states, subscription options, screen sharing and event payloads.
