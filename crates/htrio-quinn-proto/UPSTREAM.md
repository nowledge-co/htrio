# Upstream provenance

Derived from quinn-rs/quinn, package `quinn-proto` version `0.11.19`.
The upstream MIT and Apache-2.0 licenses are preserved.

```json
{
  "git": {
    "sha1": "8192ed399a26a6e8f0aba43d10ec8badaa5b905e"
  },
  "path_in_vcs": "quinn-proto"
}
```

## Local maintenance

The package is named `htrio-quinn-proto`; its Rust library name is unchanged.
Sibling path dependencies keep protocol and UDP types consistent for downstream
Git/path consumers. Changes are maintained and pushed with htrio.

- Enforce the ACK Frequency draft-14 delay bound on received and generated
  frames, and reject ACK_FREQUENCY and IMMEDIATE_ACK in 0-RTT packets. The
  frame types and wire encoding remain unchanged.
