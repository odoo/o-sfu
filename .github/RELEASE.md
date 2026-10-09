# Release

1. Set `[workspace.package].version` in [Cargo.toml](../Cargo.toml) to the
   release version, for example `0.11.5`.
2. Refresh the workspace lockfile and check it:

   ```bash
   cargo update --workspace
   cargo metadata --locked --format-version 1 >/dev/null
   ```

3. Run the [local checks](../tests/README.md#local-checks), review the diff
   and merge the release PR into `master` after CI passes.
4. Fetch `master`, create an annotated tag on the merged commit and push only
   that tag. Replace `v0.11.5` with the version from step 1:

   ```bash
   git fetch origin master
   git tag -a v0.11.5 origin/master -m "v0.11.5"
   git push origin v0.11.5
   ```

5. Open the `Release` run in GitHub Actions, review the preflight summary
   and approve the `release` environment if prompted. Wait for all jobs to pass.
6. Confirm the GitHub release has its assets and
   `ghcr.io/odoo/o-sfu:v0.11.5` is available. `latest` follows the latest
   non-prerelease release. Follow [deployment verification](../DEPLOYMENT.md#release-integrity)
   before rollout.

The workflow creates the GitHub release. Do not create it manually.

For a prerelease, use a suffix such as `v0.11.5-rc.1`. The base version must
match Cargo. Prereleases do not update `latest`.
