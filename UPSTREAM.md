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
- Add bounded RFC 7838 header/HTTP/2 frame discovery, cache controls, freshness calculation,
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
- Race TCP/QUIC preparation without consuming bodies; qualify failed routes with
  bounded, request-free probes only after TCP succeeds. Discover HTTPS records
  with the client's resolver and retain origin authentication when applying ECH.
- Add replay-gated 0-RTT, bounded dynamic QPACK, RFC 9218 priority, opt-in
  authenticated origin coalescing, connection diagnostics, and qlog hooks.
- Expose the same native HTTP/3 configuration and network maintenance in async
  and blocking clients; register rebound sockets with their owning runtime.

## Maintenance

Compare this pinned upstream revision with future reqwest releases when taking
security or compatibility fixes. Keep transport selection below the shared
middleware. Preserve header/body streaming and never infer safe replay from a
transport error after dispatch. Keep protocol-specific code in the transport
and HTTP/3 modules; avoid diverging from Hyper without a concrete failing case.

Quinn and rustls provide the QUIC transport and TLS implementation. quiche and
BoringSSL are deferred; neither is a dependency or an implemented backend.

HTTP/3 uses the local `htrio-h3` and `htrio-h3-quinn` workspace packages under
`crates/`, derived from h3 0.0.8 and h3-quinn 0.0.10. The protocol adds bounded frame parsing, incremental unknown-frame skipping,
connection availability checks, and upload cancellation notification. The Quinn
adapter owns receive streams during pending reads. All local crates retain
upstream licenses and revision metadata and are committed with htrio.

The transport is vendored as `htrio-quinn` 0.11.12, `htrio-quinn-proto` 0.11.19,
and `htrio-quinn-udp` 0.5.16. Each package records its exact upstream revision in
`UPSTREAM.md`; all internal dependencies use sibling paths. Quinn exposes a
cancellation-safe chunk polling method for the HTTP/3 adapter.

The adapter's optional native DATAGRAM feature uses sibling `htrio-h3-datagram`,
derived from h3-datagram 0.0.2, to share the same protocol types. The client does
not enable this extension.

All maintained workspace crates require Rust 1.89. Their inherited versions
identify the upstream source baseline, rather than promising the upstream MSRV.
