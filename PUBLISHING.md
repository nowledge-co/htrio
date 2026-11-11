# Publishing

All seven workspace packages publish to crates.io. The public client is `htrio`;
its HTTP/3 and QUIC dependencies retain their `htrio-` package names, upstream
licenses, and provenance. The dependency versions identify the upstream source
baselines and evolve independently of the client version.

## Prepare a release

Use current stable Cargo for workspace publishing. The library's minimum
supported Rust version remains 1.89.

1. Update the versions of changed packages and their sibling dependency
   requirements. Keep each `path` together with a matching `version`: local and
   Git consumers use the workspace source, while crates.io consumers use the
   registry package.
2. Update `Cargo.lock` and the README's dependency examples when needed.
3. Commit the release changes and require CI to pass for that exact commit.
   CI includes package verification, native regression suites, feature profiles,
   and the minimum supported Rust version.
4. Inspect the package contents and run a publication dry run:

   ```sh
   cargo package --workspace --list
   cargo publish --workspace --dry-run --locked -j 2
   ```

The dry run builds extracted packages with registry-style dependencies, including
workspace packages that have not been published yet. It does not upload crates
and does not require a crates.io token. Package verification is required even if
workspace builds already pass.

## Publish

Authenticate with `cargo login` or an existing Cargo credential provider. Never
commit tokens. After the exact release commit passes CI, publish the workspace:

```sh
cargo publish --workspace --locked -j 2
```

Cargo orders uploads by dependency. For later releases, select only changed,
unpublished packages with one or more `--package` arguments instead of
`--workspace`. The initial release contains:

| Package | Version | Workspace dependencies |
| --- | --- | --- |
| `htrio-quinn-udp` | `0.5.16` | None |
| `htrio-quinn-proto` | `0.11.19` | None |
| `htrio-quinn` | `0.11.12` | `htrio-quinn-proto`, `htrio-quinn-udp` |
| `htrio-h3` | `0.0.8` | Quinn packages for development only |
| `htrio-h3-datagram` | `0.0.2` | `htrio-h3` |
| `htrio-h3-quinn` | `0.0.10` | `htrio-h3`, `htrio-h3-datagram` (optional), `htrio-quinn` |
| `htrio` | `0.1.0` | `htrio-h3`, `htrio-h3-quinn`, `htrio-quinn` |

Published versions cannot be overwritten. If an upload is interrupted, inspect
crates.io before retrying and use `--package` to select only unpublished packages.
Do not bump versions merely to retry an upload whose outcome is unknown.

After publication, verify `htrio = "0.1.0"` from a new project outside this
workspace, including the default HTTP/3 dependencies. Tag the release commit as
`v0.1.0` and push the tag to preserve its source identity.
