# Upstream provenance

The initial native HTTP client implementation, examples, and regression tests
are derived from [seanmonstar/reqwest](https://github.com/seanmonstar/reqwest),
version **0.13.4**, commit
[`11489b34eda6d32b15ad4033e62beba2ee401350`](https://github.com/seanmonstar/reqwest/commit/11489b34eda6d32b15ad4033e62beba2ee401350).

Upstream is dual-licensed under MIT OR Apache-2.0. The original license files
and Sean McArthur's copyright are included unchanged.

## Local changes

- Rename the package and Rust crate to htrio. Native implementation only;
  remove WASM source/dependencies and custom HTTP/3 compiler-flag requirements.
- Enable the common native serialization, cookies, compression, streaming,
  HTTP/2, and HTTP/3 Cargo features by default. Keep `rustls-tls` as an alias for
  the spelling used by Mem's reqwest 0.12 client.
- Unify TCP and QUIC dispatch below retry, cookie, redirect, and decompression
  middleware, so each redirect hop selects a transport for its own origin.
- Add bounded RFC 7838 header discovery, cache controls, freshness calculation,
  network-change invalidation, broken-endpoint backoff, `Alt-Used`, and 421
  invalidation/replay handling.
- Separate the origin's TLS server name from the alternative network authority;
  key HTTP/3 pools by both origin and destination. Bind QUIC endpoints lazily
  and bound idle HTTP/3 pool entries.
- Add local certificate/HTTP/QUIC tests and explicitly ignore external network
  tests. Preserve the upstream native regression suites.
- Implement SOCKS5 UDP ASSOCIATE with custom DNS routing, authenticated proxy
  configuration, and proxy-scoped connection pools.
- Classify retries using typed protocol errors: HTTP/3 REQUEST_REJECTED and
  HTTP/2 safe NACKs can be retried; ambiguous network timeouts cannot.

## Maintenance

Compare this pinned upstream revision with future reqwest releases when taking
security or compatibility fixes. Keep transport selection below the shared
middleware. Preserve header/body streaming and never infer safe replay from a
transport error after dispatch. Keep protocol-specific code in the transport
and HTTP/3 modules; avoid diverging from Hyper without a concrete failing case.

Quinn and rustls provide the QUIC transport and TLS implementation. quiche and
BoringSSL are deferred; neither is a dependency or an implemented backend.

HTTP/3 uses the local `htrio-h3` and `htrio-h3-quinn` workspace packages under
`crates/`, derived from h3 0.0.8 and h3-quinn 0.0.10. The protocol implementation
is unchanged; the Quinn adapter uses the sibling protocol crate to preserve
type identity. Both retain their upstream licenses and revision metadata and
are committed with htrio. Quinn itself remains an upstream dependency.

The adapter's optional native DATAGRAM feature uses sibling `htrio-h3-datagram`,
derived from h3-datagram 0.0.2, to share the same protocol types. The client does
not enable this extension.
