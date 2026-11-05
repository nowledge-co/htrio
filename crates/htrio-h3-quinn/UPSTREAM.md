# Upstream provenance

Vendored from hyperium/h3, package `h3-quinn` version `0.0.10`.
The original MIT license is preserved. Upstream source metadata:

```json
{
  "git": {
    "sha1": "2dc3412bdf6083451920d5bfd7a9484d054c1859"
  },
  "path_in_vcs": "h3-quinn"
}
```

Local changes are tracked in this repository and shipped with htrio.

## Local changes

- Rename the Cargo package to `htrio-h3-quinn`, retaining `h3_quinn` as its
  library name.
- Depend on sibling `htrio-h3` by package alias and path, so workspace and
  downstream Git/path consumers share the same protocol types.
- Point the optional `datagram` feature at sibling `htrio-h3-datagram` to avoid
  mixing protocol types with a second, registry-sourced h3 package.

The adapter implementation is unchanged. Quinn remains an upstream dependency.
