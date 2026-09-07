# o-sfu deployment

This document is the operator contract for running `o-sfu` as the Odoo Discuss SFU.

(for Odoo development, refer to [Odoo SFU Dev Deployment Guide](/.github/odoo_setup.md))

## typical traffic model

```text
HTTPS and WSS -> public reverse proxy -> o-sfu HTTP listener
WebRTC UDP    -> public VM IP       -> o-sfu RTC UDP range
```

the reverse proxy handles only HTTP and WebSocket traffic

media UDP must reach the VM public address directly because `o-sfu` advertises `ANNOUNCED_IP` in ICE-lite SDP

## Odoo binding

use the same public SFU URL and shared key on both sides

on `o-sfu`:

```env
AUTH_KEY=<base64-auth-key>
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

`AUTH_KEY` must match Odoo and be valid base64 that decodes to at least 32
cryptographically random bytes. Generate it and `DIAGNOSTICS_AUTH_TOKEN`
independently by running this command for each:

```bash
openssl rand -base64 32
```

### network and proxy trust

keep port `8070` unreachable from untrusted networks. Bind it to loopback for a
host proxy or expose it only on an isolated same-host container network

set `PROXY=true` only when the trusted public proxy strips or overwrites
client-supplied forwarded headers. Use `$proxy_add_x_forwarded_for` only when a
trusted upstream has already stripped client input

`/v1/stats`, `/metrics` and `/internal/diagnostics/...` require
`DIAGNOSTICS_AUTH_TOKEN` on every request when configured. Without it, the
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
attestations. `master`, commit-addressable `sha-<commit>` images and pull
request smoke-test images are intentionally not attested.

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

collectors that read Docker log files must parse the outter Docker record first,
then parse the inner `log` string as the `o-sfu` JSON payload

the outer Docker record looks like this:

```json
{
  "log": "{\"timestamp\":\"2026-07-09T10:12:34.567890123Z\",\"level\":\"INFO\",\"target\":\"o_sfu::runtime::http_server::controller\",\"event\":\"http.listener.ready\",\"message\":\"booted HTTP and WebSocket listener\",\"service.name\":\"o-sfu\",\"service.version\":\"0.7.0\",\"service.instance.id\":\"pid-1\",\"deployment.environment\":\"production\",\"bind_address\":\"0.0.0.0:8070\",\"local_address\":\"0.0.0.0:8070\",\"trust_proxy_headers\":true}\n",
  "stream": "stdout",
  "time": "2026-07-09T10:12:34.568000000Z"
}
```

after decoding the outer `log` field, parse the resulting string as the inner
`o-sfu` payload

that payload is not the default nested `tracing-subscriber` JSON
shape because rutnime event fields are flattened at the top level

```json
{
  "timestamp": "2026-07-09T10:12:34.567890123Z",
  "level": "INFO",
  "target": "o_sfu::runtime::http_server::controller",
  "event": "http.listener.ready",
  "message": "booted HTTP and WebSocket listener",
  "service.name": "o-sfu",
  "service.version": "0.7.0",
  "service.instance.id": "pid-1",
  "deployment.environment": "production",
  "bind_address": "0.0.0.0:8070",
  "local_address": "0.0.0.0:8070",
  "trust_proxy_headers": true
}
```

common fields are:

| field | type | value |
| --- | --- | --- |
| `timestamp` | string | RFC 3339 UTC timestamp generated when the event is formatted |
| `level` | string | tracing level such as `INFO`, `WARN` or `ERROR` |
| `target` | string | Rust tracing target that emitted the event |
| `event` | string | stable `o-sfu` event name or `runtime.log` when the call site has no explicit event |
| `message` | string | optional human log message |
| `service.name` | string | `TELEMETRY_SERVICE_NAME` defaulting to `o-sfu` |
| `service.version` | string | compiled `o-sfu` crate version |
| `service.instance.id` | string | `TELEMETRY_SERVICE_INSTANCE_ID` defaulting to `pid-<pid>` |
| `deployment.environment` | string | `TELEMETRY_DEPLOYMENT_ENVIRONMENT` defaulting to `local` |
| `trace_id` | string | optional active trace id when tracing context exists |

event-specific fields are also top-level keys

examples include `room_id`, `user_id`, `connection_id`, `remote_address`,
`operation`, `outcome`, `reason`, `close_code`, `error_kind`, `duration_ms`,
`transport_media_id` and `media_worker_id`

operator tooling should use `event` as the discriminator, require only the
common fields it needs and tolerate unknown extra keys

the reviewed event and field catalog lives in `crates/telemetry/src/schema.rs`
the formatter is in `crates/telemetry/src/setup.rs`

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

```nginx
map $http_upgrade $connection_upgrade {
    default upgrade;
    "" close;
}

server {
    listen 443 ssl http2;
    server_name <sfu-domain>;

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
`DIAGNOSTICS_AUTH_TOKEN` is configured:

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
- NGINX proxies to the actual `BIND_ADDRESS`
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
- `PROXY=true` is set only behind the trusted NGINX edge
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
| `BIND_ADDRESS` | `0.0.0.0:8070` | HTTP and WebSocket listening address |
| `PROXY` | `false` | trusts proxy-provided request metadata when `true` |
| `DIAGNOSTICS_AUTH_TOKEN` | unset | bearer token for `/v1/stats`, `/metrics` and `/internal/diagnostics/...`. Tokenless access requires the actual listener to use loopback |
| `SHUTDOWN_TIMEOUT_MS` | `10000` | positive total deadline in milliseconds for listener, WebSocket session, background task and RTC worker drainage |

authentication and websocket admission:

| variable | default | description |
| --- | --- | --- |
| `AUTHENTICATION_TIMEOUT_MS` | `10000` | first authenticated WebSocket frame timeout in milliseconds |
| `MAX_PRE_AUTH_WEBSOCKET_SESSIONS` | `512` | process-wide cap for upgraded WebSockets waiting for authentication |
| `MAX_PRE_AUTH_WEBSOCKET_SESSIONS_PER_ORIGIN` | `16` | per-origin cap for upgraded WebSockets waiting for authentication |

room and user limits:

| variable | default | description |
| --- | --- | --- |
| `ROOM_SIZE` | `100` | maximum concurrent users per room |
| `USER_TIMEOUT_MS` | `10000` | idle user timeout in milliseconds |
| `PING_INTERVAL_MS` | `60000` | signaling ping interval in milliseconds |
| `USER_OUTBOUND_QUEUE_CAPACITY` | `128` | per-user WebSocket room-event queue depth |
| `USER_OUTBOUND_QUEUE_BYTE_CAPACITY` | `2097152` | per-user WebSocket queued-byte budget |
| `ROOM_RESERVATION_TTL` | `60` | time-to-live for unjoined rooms in seconds |

RTC transport:

| variable | default | description |
| --- | --- | --- |
| `RTC_MIN_PORT` | `40000` | lower bound for the RTC UDP port range |
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
range. The legacy
`ROOM_DOWNSWITCH_PRESSURE_OBSERVATIONS` and
`ROOM_UPSWITCH_STABLE_OBSERVATIONS` variables now reject startup with an error
naming the corresponding duration variable. Replace them before upgrading.

telemetry:

| variable | default | description |
| --- | --- | --- |
| `RUST_LOG` | `info` | `tracing-subscriber` env filter |
| `TELEMETRY_LOG_FORMAT` | `compact` | log output format, either `compact` or `json` |
| `TELEMETRY_SERVICE_NAME` | `o-sfu` | service name in telemetry resource metadata |
| `TELEMETRY_DEPLOYMENT_ENVIRONMENT` | `local` | deployment environment in telemetry resource metadata |
| `TELEMETRY_SERVICE_INSTANCE_ID` | `pid-<pid>` | stable service instance id override |
| `TELEMETRY_MEDIA_QUALITY_INTERVAL_MS` | `5000` | sampled media-quality telemetry interval, with `0` disabling sampling |
| `TELEMETRY_OTLP_ENDPOINT` | disabled | optional OTLP HTTP traces endpoint, normalized to `/v1/traces` |

feature flags:

| variable | default | description |
| --- | --- | --- |
| `FEATURE_TRANSCRIPTION` | `false` | enables transcription intent flags, currently WIP |
| `FEATURE_AUDIO_RECORDING` | `false` | enables audio recording intent flags, currently WIP |
| `FEATURE_VIDEO_RECORDING` | `false` | enables video recording intent flags, currently WIP |
