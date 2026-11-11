# Local dependency crates

Modified dependencies live in this Cargo workspace and are committed and pushed
with htrio. No separate repository or submodule is required.

| Crate | Upstream | Local purpose |
| --- | --- | --- |
| `htrio-h3` | hyperium/h3 0.0.8 | Keep the HTTP/3 protocol source and regression suite in this workspace. |
| `htrio-h3-quinn` | hyperium/h3 0.0.10 | Use the same local h3 protocol types and preserve upstream h3's adapter-backed regression tests. |
| `htrio-h3-datagram` | hyperium/h3 0.0.2 | Preserve type identity for the adapter's optional native DATAGRAM extension. |

| `htrio-quinn` | quinn-rs/quinn 0.11.12 | Own Tokio QUIC integration and cancellation-safe chunk polling. |
| `htrio-quinn-proto` | quinn-rs/quinn 0.11.19 | Own the transport state machine, recovery, and congestion implementation. |
| `htrio-quinn-udp` | quinn-rs/quinn 0.5.16 | Own platform UDP/GSO/GRO integration. |

Each crate retains its upstream license and revision metadata. The adapter uses
owned receive streams and propagates peer upload cancellation. Path dependencies preserve type identity when htrio is consumed from another
workspace. All seven workspace packages are publishable to crates.io. Cargo
uses the version requirement instead of the sibling path in published packages,
preserving the same dependency graph for registry consumers. Inherited dependency
versions identify their upstream source baselines; these are separately maintained
forks. See [the publishing guide](../PUBLISHING.md) for release order and checks.

Keep protocol fixes narrow, add regression coverage, and document deviations
from upstream in each crate's `UPSTREAM.md`.

The optional DATAGRAM extension preserves the adapter's feature compatibility;
the htrio client does not enable native QUIC DATAGRAM forwarding.
