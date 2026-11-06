# Upstream provenance

Vendored from hyperium/h3, package `h3` version `0.0.8`.
The original MIT license is preserved. Upstream source metadata:

```json
{
  "git": {
    "sha1": "22c1aa3f44d1463cd7644c8f654fffc9a6da305c"
  },
  "path_in_vcs": "h3"
}
```

Local changes are tracked in this repository and shipped with htrio.

## Local changes

- Rename the Cargo package to `htrio-h3`, retaining the `h3` library name.
- Point adapter-backed upstream tests at the sibling `htrio-h3-quinn` source.
  No QUIC transport implementation is vendored into this crate.

- Bound decoded field sections and buffered frames; skip unknown payloads incrementally.
- Expose connection availability after GOAWAY and peer upload-stop polling.
- Keep request stream receive ownership intact across pending polls.

Run the inherited protocol suite with `cargo test -p htrio-h3 --all-features
--lib -- --test-threads=2`. The inherited tests use the optional backend
inspection feature; htrio's runtime dependency does not require it.
