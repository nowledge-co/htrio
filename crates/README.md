# Local dependency crates

Modified dependencies live in this Cargo workspace and are committed and pushed
with htrio. No separate repository or submodule is required.

| Crate | Upstream | Local purpose |
| --- | --- | --- |
| `htrio-h3` | hyperium/h3 0.0.8 | Keep the HTTP/3 protocol source and regression suite in this workspace. |
| `htrio-h3-quinn` | hyperium/h3 0.0.10 | Use the same local h3 protocol types and preserve upstream h3's adapter-backed regression tests. |
| `htrio-h3-datagram` | hyperium/h3 0.0.2 | Preserve type identity for the adapter's optional native DATAGRAM extension. |

Each crate retains its upstream license and revision metadata. htrio-h3-quinn's
implementation is unchanged; its h3 and optional datagram dependencies use sibling paths. Path
dependencies preserve type identity when htrio is consumed from another
workspace. The root package and local dependencies are not configured for
crates.io publication; release naming/versioning must be decided first.

Keep protocol fixes narrow, add regression coverage, and document deviations
from upstream in each crate's `UPSTREAM.md`.

The optional DATAGRAM extension preserves the adapter's feature compatibility;
the htrio client does not enable native QUIC DATAGRAM forwarding.
