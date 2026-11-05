# Upstream provenance

Vendored from hyperium/h3, package `h3-datagram` version `0.0.2`.
The original MIT license is preserved. Upstream source metadata:

```json
{
  "git": {
    "sha1": "230744a80ad49aa14ae68c4b9698fb6260992dbd"
  },
  "path_in_vcs": "h3-datagram"
}
```

## Local changes

Rename the Cargo package to `htrio-h3-datagram`, retaining the `h3_datagram`
library name. The protocol dependency uses sibling `htrio-h3` by package alias
and path. This preserves protocol type identity for the Quinn adapter's optional
`datagram` feature. The extension implementation is unchanged.

The htrio client does not enable this optional native QUIC DATAGRAM extension.
