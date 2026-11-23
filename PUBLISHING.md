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

## Configure trusted publishing

The `Release` GitHub Actions environment is the only automated publisher. For
each of the seven crates, configure a crates.io trusted publisher with:

- repository owner `nowledge-co` and repository `htrio`;
- workflow file `release.yml`;
- environment `release`.

Protect that GitHub environment with required reviewers. After all publishers
are configured, enable crates.io's trusted-publishing-only mode for every crate.
The workflow exchanges GitHub's OIDC identity for a short-lived token; no
long-lived registry secret is stored in GitHub.

## Publish

After the exact release commit passes CI, dispatch the `Release` workflow with
the existing immutable tag. The script checks that the tag matches the `htrio`
version, skips versions already visible on crates.io, publishes the remaining
packages in dependency order, and creates the GitHub release only after registry
read-back succeeds:

```sh
gh workflow run release.yml -f tag=v0.5.0
```

For an emergency manual release, authenticate with `cargo login` or a Cargo
credential provider and run `scripts/publish-release.sh` from the exact tag.
Never commit tokens.

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

For v0.2.0, publish only the three changed packages:

| Package | Version | Updated dependency requirement |
| --- | --- | --- |
| `htrio-quinn-proto` | `0.11.20` | None |
| `htrio-quinn` | `0.11.13` | `htrio-quinn-proto >= 0.11.20` |
| `htrio` | `0.2.0` | `htrio-quinn >= 0.11.13` |

```sh
cargo publish --locked -j 2 --package htrio-quinn-proto --package htrio-quinn --package htrio
```

For v0.3.0, publish the following packages in the same dependency order:

| Package | Version | Updated dependency requirement |
| --- | --- | --- |
| `htrio-quinn-proto` | `0.11.21` | None |
| `htrio-quinn` | `0.11.14` | `htrio-quinn-proto >= 0.11.21` |
| `htrio` | `0.3.0` | `htrio-quinn >= 0.11.14` |

The h3 and UDP packages retain their existing versions. Only test coverage
changed in the h3 packages; their runtime implementation and public API are
unchanged since the previous publication.

For v0.4.0, publish the following packages in the same dependency order:

| Package | Version | Updated dependency requirement |
| --- | --- | --- |
| `htrio-quinn-proto` | `0.11.22` | None |
| `htrio-quinn` | `0.11.15` | `htrio-quinn-proto >= 0.11.22` |
| `htrio` | `0.4.0` | `htrio-quinn >= 0.11.15` |

The h3 and UDP packages retain their existing versions. BBRv3 changes only the
three packages listed above.

For v0.5.0, publish the following packages in dependency order:

| Package | Version | Updated dependency requirement |
| --- | --- | --- |
| `htrio-quinn-proto` | `0.11.23` | None |
| `htrio-quinn` | `0.11.16` | `htrio-quinn-proto >= 0.11.23` |
| `htrio-h3` | `0.0.9` | Quinn packages for development only |
| `htrio-h3-datagram` | `0.0.3` | `htrio-h3 = 0.0.9` |
| `htrio-h3-quinn` | `0.0.11` | `htrio-h3 = 0.0.9`, `htrio-h3-datagram = 0.0.3` (optional), `htrio-quinn >= 0.11.16` |
| `htrio` | `0.5.0` | `htrio-h3 = 0.0.9`, `htrio-h3-quinn = 0.0.11`, `htrio-quinn >= 0.11.16` |

The H3 packages include timeout classification helpers and source maintenance
from PR #12. Publish the protocol and its adapters together so registry consumers
share the same H3 types. The unchanged UDP package remains 0.5.16.

Published versions cannot be overwritten. The release script is safe to retry:
it inspects crates.io before each upload and waits for registry visibility before
publishing dependants. Do not bump versions merely to retry an upload whose
outcome is unknown.

After publication, verify `htrio = "0.5.0"` from a new project outside this
workspace, including the default HTTP/3 dependencies. Create `v0.5.0` only after
the release commit passes CI, then dispatch the release workflow with that exact
tag. Never move or recreate an existing release tag.
