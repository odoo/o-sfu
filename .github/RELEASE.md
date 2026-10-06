## release workflow

the `Release` workflow runs only for `v*` tags. it builds the locked release
server binary, Odoo client bundle and version-tag GHCR image from the tagged
source. it extracts the image SBOM into a release asset, writes `SHA256SUMS`,
creates GitHub artifact attestations from that checksum manifest, creates
GitHub image attestation for the version-tag image, then publishes the release.

this does not replace the container image workflow. commit-addressable GHCR
images remain the testing-infrastructure package path for `master` and specific
commits.

the `Release` workflow also publishes the version-tag container image. only
those version-tag image builds get Docker provenance, SBOM and GitHub image
attestations. `master` and commit-addressable `sha-<commit>` images are
intentionally not attested.

suffixed tags, such as `v0.3.1-rc.1` or `v0.3.1-test.20260605`, are
published as GitHub prereleases and are explicitly not marked as the latest
release

the GitHub release includes the server tarball, Odoo client bundle, image SBOM
and checksum manifest. the release notes include the pullable GHCR image
reference and verification commands generated from `GITHUB_REPOSITORY` and
`GITHUB_REF_NAME`, so the owner and version are not hardcoded in the published
release text. the deployment guide link uses `GITHUB_SERVER_URL`,
`GITHUB_REPOSITORY` and `GITHUB_SHA` so it targets the source of that release.

the public release body also records the version tag, source commit, successful
Cargo base-version and default-branch ancestry checks, runner OS generation and
fixed build inputs. Docker base references are read from the tagged
`Dockerfile`.

## build inputs

the preflight summary records the reviewed Buildx, BuildKit, Node.js, wasm-pack
and Docker base-image references from the tagged workflow. the tagged
`rust-toolchain.toml` selects the host Rust compiler. the release job records
the version reported by the selected compiler in the release notes. Docker
bases keep readable tags and immutable multi-platform index digests. the runtime
copies the CA bundle from the pinned builder, so the image build performs no
operating-system package download.

Dependabot checks the root Dockerfile each week and proposes base tag or digest
updates as pull requests. review and verify those changes before merging them.
GitHub Actions updates remain separate Dependabot pull requests. update the
exact Buildx, Node.js or wasm-pack selector in `release.yml` through the same
reviewed pull-request process when a tool update is intentional. BuildKit
updates must change its readable tag and multi-platform index digest together
against the published `moby/buildkit` image.

`ubuntu-24.04` fixes the release runner OS generation. GitHub still updates the
hosted image contents and does not expose a content-digest runner selector. the
exact runner image used by a release remains available in that workflow run's
setup log.

## updating Rust versions

1. update [`rust-toolchain.toml`](/rust-toolchain.toml) for developer builds,
   normal CI and releases.
2. update the Rust builder tag and immutable digest in
   [`Dockerfile`](/Dockerfile) in the same pull request.
3. update Cargo `rust-version` in [`Cargo.toml`](/Cargo.toml) only when the
   supported compiler floor changes.

the container build rejects a compiler that differs from
[`rust-toolchain.toml`](/rust-toolchain.toml). dated nightly and tool-specific
toolchains remain independent.

## updating the release lockfile

for a release-only version bump, update the root Cargo version and refresh only
the local package entries in the shared workspace lockfile:

```bash
cargo update --workspace
```

`cargo update --workspace` keeps already locked third-party dependencies in
place and updates only packages defined by the current workspace unless Cargo
must add a missing package. this is the right default after changing the version
in `Cargo.toml`. Omit it if you update dependencies.

the opt-in fuzz package is part of the root workspace and uses the root
`Cargo.lock`. no separate fuzz lockfile refresh is needed.

after the lockfile is refreshed, check that locked metadata does not need to
rewrite it:

```bash
cargo metadata --locked --format-version 1 >/dev/null
```

if the release intentionally includes a full dependency refresh, update the
shared lockfile without `--workspace`:

```bash
cargo update
```

review third-party dependency changes before merging a full refresh. tooling
dependencies can also affect CI, so keep workflow-installed tool versions in
sync with any locked tool crate updates.

## creating a release tag

merge the release changes through a pull request first. after the PR is merged,
create the tag on the updated `master` branch and push only the tag:

```bash
git checkout master
git pull
git tag -a v0.11.3 -m "v0.11.3"
git push origin v0.11.3
```

`git tag -a` creates an annotated tag that points at the current `HEAD` commit.
it does not create a code commit and does not move `master`.

when GitHub receives the pushed `v*` tag, it starts the `Release` workflow on
the commit the tag points to. avoid creating the GitHub release manually from
the web UI for this flow because the workflow creates the release and uploads
the assets itself.

## formatting

tags must start with the Cargo base version and use Docker-tag-safe suffix
characters because the release tag is also used as the GHCR image tag

`v{major}.{minor}.{patch}`
example: `v0.3.1`

or the same base version with any suffix that starts with `.`, `_` or `-`

`v{major}.{minor}.{patch}<suffix>`
examples: `v0.3.1-rc.1`, `v0.3.1-test.20260605`, `v0.3.1_build42`

only the base `major.minor.patch` is compared to Cargo, so generated suffixes
do not require Cargo version bumps
