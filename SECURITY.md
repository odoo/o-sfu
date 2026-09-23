# Security & Privacy

<div align="center">

[![Fuzzing](https://github.com/odoo/o-sfu/actions/workflows/fuzzing.yml/badge.svg)](https://github.com/odoo/o-sfu/actions/workflows/fuzzing.yml)
[![Cargo Deny](https://github.com/odoo/o-sfu/actions/workflows/cargo-deny.yml/badge.svg)](https://github.com/odoo/o-sfu/actions/workflows/cargo-deny.yml)
[![Dependency Review](https://github.com/odoo/o-sfu/actions/workflows/dependency-review.yml/badge.svg)](https://github.com/odoo/o-sfu/actions/workflows/dependency-review.yml)
[![CodeQL](https://github.com/odoo/o-sfu/actions/workflows/codeql.yml/badge.svg)](https://github.com/odoo/o-sfu/actions/workflows/codeql.yml)
[![OSV-Scanner](https://github.com/odoo/o-sfu/actions/workflows/osv-scanner.yml/badge.svg)](https://github.com/odoo/o-sfu/actions/workflows/osv-scanner.yml)
[![DevSkim](https://github.com/odoo/o-sfu/actions/workflows/devskim.yml/badge.svg)](https://github.com/odoo/o-sfu/actions/workflows/devskim.yml)

</div>

### Vulnerability Reporting & Contact

Please do **not** open public issues or discussions for security vulnerabilities. Instead, use the contact information provided below:

https://www.odoo.com/security-report

---

## Security Policy

### Supported Versions

Only latest. Version support is at the Odoo layer.

### Authentication Secrets

`o-sfu` uses secret containers for server and room keys, key seeds and JWTs.
These containers redact their contents from debug output and overwrite them
when dropped, reducing accidental disclosure and secret data left in memory.

`AUTH_KEY_FILE` supports loading the server key from a mounted secret,
keeping its value out of the process environment. Leave `AUTH_KEY` unset
when using this option. See [Deployment](DEPLOYMENT.md) for configuration.

### Security Tooling & Verification
(see badges above for status)

- **Dynamic Analysis & Sanitizers**:
    - **AddressSanitizer (ASan)**: Validates runtime execution, protocol handling, and media packet loops to detect memory corruption, buffer overflows, and use-after-free issues.
    - **Miri (Undefined Behavior Detection)**: Analyzes unsafe blocks, pointer provenance, uninitialized memory, SIMD versus scalar operations, and cross-target endianness.
    - **Fuzzing (`cargo-fuzz` / `libFuzzer`)**: Continuously stresses ingress attack surfaces against malformed input, taregts are: WebSocket protocol decoders, HTTP authentication payloads, SDP negotiation, and RTP packet demuxing.
- **Static Analysis**:
    - **CodeQL**: Semantic code analysis for common vulnerabilities, taint tracking, and memory safety flaws ([@GitHub/codeql](https://github.com/github/codeql)).
    - **DevSkim**: Static analysis for security anti-patterns and insecure API usage ([@Microsoft/devskim](https://github.com/microsoft/devskim)).
- **Supply Chain Security**:
    - **Cargo Deny (`cargo-deny`)**: checks license compliance, bans duplicate dependencies, and blocks vulnerable crates reported in the [RustSec Advisory Database](https://rustsec.org/)
    - **OSV-Scanner & Dependency Review**: scans dependencies against the OSV database ([@Google/osv-scanner](https://github.com/google/osv-scanner)) on pull requests and scheduled runs.

### Releases

#### The release includes:

- A build provenance summary listing all the tools and pinned versions used to generate the binary.
- SLSA/Sigstore attestations and a SHA256 checksum for every generated artifact
- A SBOM in SPDX (ISO/IEC 5962:2021) format

see: https://github.com/odoo/o-sfu/releases

---

## Privacy & Data Handling

`o-sfu` only operates in-memory, there is no persistent storage.

### 1. Data Processed

| Category                   | Data Processed                                      | Purpose & Scope                                                                                    |
| -------------------------- | --------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| **Network & IP Addresses** | Client IP addresses                                 | Real-time WebRTC media routing, connection rate-limiting (anti-abuse/DoS), and diagnostic logging. |
| **User & Room Identity**   | Ephemeral user IDs and room IDs                     | Authenticating connections and routing media to the correct call participants.                     |
| **Call Presence**          | Mute state, camera/screen status, speaking activity | Relayed in real time only to active participants within the same room.                             |
| **Media Streams**          | Audio, video, and screen sharing                    | Encrypted in transit (DTLS-SRTP), routed in volatile memory, and never stored.                     |

### 2. Media Confidentiality & Storage

- **In-Memory Forwarding**: Media streams are forwarded in volatile memory only. `o-sfu` does not record, transcode, inspect content, or write media payloads to disk.
- **Transport Encryption**: All WebRTC media streams are encrypted in transit over UDP using DTLS-SRTP (the crypto backend is [AWS libcrypto](https://github.com/aws/aws-lc-rs)).
- **Zero Local Persistence**: `o-sfu` has no database or file storage. When a call ends or a participant leaves, all associated routing and session data are immediately erased from memory.

### 3. Logging & Observability

- **Operational Logs**: Server logs (stdout/stderr) record connection lifecycle events, IP addresses, and user IDs for debugging, performance monitoring, and abuse detection. Media content and secret keys are never logged.
- **Statistics**: `/v1/stats` exposes room UUIDs, remote addresses and participant media counts to authorized operators.
- **Metrics**: Aggregate Prometheus metrics (`/metrics`) contain only high-level operational counters and never expose IP addresses, user IDs or room names.
- **Diagnostics**: Detailed internal runtime diagnostics expose current room and user facts to authorized operators.
- **Observation Access**: `/v1/stats`, `/metrics` and diagnostics require the configured bearer token on every listener. Without one, the actual listener must be loopback.

### 4. Operator Privacy Responsibilities

Operators hosting `o-sfu` control their deployment environment and should ensure compliance with applicable data protection regulations (such as GDPR):

- **Log Retention**: Configure appropriate log rotation and retention limits on host or container logging systems to manage IP and identifier storage.
- **Transport Security**: Deploy `o-sfu` behind a trusted reverse proxy with TLS/WSS enabled for signaling traffic.
- **Access Control**: Keep observation routes private and securely manage their bearer token plus shared authentication keys.
- **Observation Transport**: Send the observation bearer token only over same-host loopback, an isolated same-host container network, TLS or an authenticated encrypted transport. Only trusted telemetry services may join the container network. The `o-sfu` HTTP listener does not terminate TLS.
