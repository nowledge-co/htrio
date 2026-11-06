# htrio

[![CI](https://github.com/nowledge-co/htrio/actions/workflows/ci.yml/badge.svg)](https://github.com/nowledge-co/htrio/actions/workflows/ci.yml)

A native Rust HTTP client with a reqwest-style API, HTTP/1.0, HTTP/1.1,
HTTP/2, and HTTP/3.

htrio is a source fork of reqwest 0.13.4. HTTP/1 and HTTP/2 use Hyper;
HTTP/3 uses Quinn, rustls, and the local `htrio-h3` protocol crate. The client
shares request, cookie, redirect, retry, and decompression handling across
transports. Requires **Rust 1.89 or later**. Native targets only.

## Quick start

Use a Git dependency while crates.io publication is not configured:

```toml
[dependencies]
htrio = { git = "https://github.com/nowledge-co/htrio" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust,no_run
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), htrio::Error> {
    let client = htrio::Client::builder()
        .user_agent("my-app/1.0")
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(30))
        .redirect(htrio::redirect::Policy::limited(10))
        .cookie_store(true)
        .build()?;

    let response = client
        .post("https://example.com/api")
        .bearer_auth("token")
        .json(&[("message", "hello")])
        .send()
        .await?
        .error_for_status()?;

    println!("{:?}: {}", response.version(), response.text().await?);
    Ok(())
}
```

Reuse a client across requests. Clones share connection pools, the cookie store,
and the Alt-Svc cache. Cookie storage requires `cookie_store(true)` or an
explicit store; enabling the Cargo feature alone does not enable storage.

## Capabilities

- Plain and byte bodies, JSON, URL queries, URL-encoded forms, multipart,
  streaming uploads, `chunk()`, and `bytes_stream()`.
- Custom redirect and retry policies, cross-origin credential handling,
  total/connect/read timeouts, and connection pooling.
- Shared `cookie::Jar` and custom `CookieStore` implementations.
- Custom `dns::Resolve`, per-host DNS overrides, and optional Hickory DNS.
- HTTP/HTTPS and SOCKS proxies, proxy authentication, and `NO_PROXY` rules.
  HTTP/3 proxying uses SOCKS5 UDP ASSOCIATE.
- System trust, custom CA roots, client identities, and TLS version bounds.
- gzip, Brotli, deflate, and Zstandard response decompression.
- An optional `blocking` client using the same transport selection.

Default features include `rustls`, `http2`, `http3`, `charset`, `system-proxy`,
`json`, `query`, `form`, `multipart`, `cookies`, `stream`, `gzip`, `brotli`,
`deflate`, and `zstd`. `blocking`, `socks`, `native-tls`, and `hickory-dns`
are opt-in. For a smaller build, disable defaults and select features explicitly:

```toml
htrio = { git = "https://github.com/nowledge-co/htrio", default-features = false, features = ["rustls", "http2", "json", "stream"] }
```

## HTTP/3 and Alt-Svc

By default, HTTPS starts on HTTP/1.1 or HTTP/2. An authenticated response such
as `Alt-Svc: h3=":443"; ma=3600` can select HTTP/3 for later requests to the same
origin. Each redirect hop makes its own transport decision.

- Routing can use an alternative host, but TLS authentication, the URL, and
  HTTP authority retain the original origin. Alternative requests send `Alt-Used`.
- Connection preparation has a one-second per-alternative budget, capped by
  `connect_timeout`. Change it with `http3_connect_timeout`. After 250 ms, TCP
  connection establishment races QUIC. Only the winning transport receives the
  request. Available alternatives and resolved addresses are rotated/raced.
- Route backoff is scoped to the client and proxy credentials and confirmed
  after the same TCP route succeeds. A TCP winner starts a bounded, request-free
  QUIC probe to qualify silent failures or warm a usable connection.
  `Client::network_changed()` resets network
  state and future endpoints while active streams retain their transport.
- After dispatch, ambiguous transport failures do not trigger TCP replay.
  Safe protocol NACKs follow the retry policy. A 421 invalidates the alternative;
  the origin is retried only when the body can be cloned.
- The process-local cache holds 256 origins and eight alternatives per origin
  by default. It handles header lists, `ma`, `persist`, `clear`, freshness/age,
  expiry, and broken-endpoint backoff. Applications detecting network changes
  call `AltSvcCache::network_changed()`; no OS watcher is installed.
- HTTP/1-only and HTTP/2-only policies prevent automatic upgrades, as do disabled
  SNI or certificate/hostname verification. Preconfigured TLS verifiers must
  enforce origin authentication themselves.

For a known QUIC endpoint, select HTTP/3 explicitly:

```rust,no_run
# async fn request() -> Result<(), htrio::Error> {
let client = htrio::Client::builder().http3_prior_knowledge().build()?;
let response = client.get("https://example.com").send().await?;
assert_eq!(response.version(), htrio::Version::HTTP_3);
# Ok(())
# }
```

A per-request `.version(htrio::Version::HTTP_3)` also selects HTTP/3. Explicit
HTTP/3 requires rustls with TLS 1.3 and never falls back to TCP. No custom
`RUSTFLAGS` are needed. `.alt_svc(false)` disables discovery while retaining
explicit HTTP/3 support.

Discovery uses response headers, authenticated HTTP/2 ALTSVC frames, and the
optional `dns::Resolve::resolve_service` metadata method. ALTSVC frames are
accepted only for the TLS connection's origin or its issued request streams;
cross-origin advertisements are ignored. Hickory DNS also queries HTTPS records,
follows bounded alias chains, and recognizes ALPN, ports, address hints,
mandatory keys, and ECH. `AltSvcCache::snapshot()` and `restore()` allow trusted,
application-controlled persistence; htrio never writes cache files itself.

HTTP/3 preserves informational responses and trailers, checks message lengths,
and bounds encoded/control frames and decoded header sections. Dropping a
request or response cancels pending upload source reads. `preconnect()` opens
connections without an HTTP request; `http3_keep_alive_interval()` controls
keepalive. `http3_rebind()` migrates direct QUIC endpoints only.
Network changes call the resolver's optional `clear_cache()` hook. Applications
can rebind before the first request and outside Tokio; socket registration uses
the endpoint's runtime. Blocking clients expose the same HTTP/3 configuration,
priority, preconnection, pool, and network maintenance APIs.

Response extensions expose `http3::ConnectionInfo` with setup time, current RTT,
loss/congestion/MTU statistics, connection reuse, and pool availability.
`http3::TransportInfo` records per-hop time to headers, protocol, route selection,
and confirmed recovery reasons, including TCP race/fallback. Pool snapshots
include bounded route probes and confirmed failure backoff. `remote_addr()` reports
direct peers; SOCKS virtual addresses stay private. `tls_info(true)` includes the
peer certificate. `http3_transport_config()` accepts native QUIC settings,
including application-owned qlog streams with the `http3-qlog` feature.

Dynamic QPACK uses a 4096-byte table and at most 16 blocked streams by default.
`http3_qpack()` changes these limits or disables dynamic compression. Tables,
instruction queues, outstanding sections, and decoded field sizes are bounded.
Authorization, Cookie, API keys, and explicitly sensitive fields use stateless sections.

`RequestBuilder::priority()` sends RFC 9218 urgency and incremental hints and
maps urgency to QUIC scheduling. The response's `http3::RequestPriority` extension
can enqueue bounded, coalesced PRIORITY_UPDATE messages.

DNS ECH configurations are applied to origin-authenticated QUIC handshakes.
ECH-selected routes fail closed instead of racing a TCP connection with clear
SNI. Preconfigured rustls backends use `http3_certificate_verifier()` to supply their
matching verification policy for ECH rebuilding; normal TLS configurations
retain it automatically.

`tls_early_data(true)` explicitly enables 0-RTT for empty GET/HEAD requests
without authentication, cookies, API keys, or sensitive fields. The client
remembers authenticated SETTINGS, uses the known route, waits for acceptance
before exposing a response, and retries rejection/425 once with a confirmed
handshake. Uploads and credentialed requests wait for confirmation. Default TLS
resumption caches are route/proxy scoped; preconfigured rustls session stores
retain the application's policy. Session counters contain no ticket contents.

`http3_coalescing(true)` permits cross-origin reuse only within the same client,
proxy credentials, network generation, and alternative endpoint. The certificate
is verified again for the new origin. Client-certificate and ECH routes remain
isolated. A 421 disables coalescing for the rejected route.

## SOCKS5 UDP and DNS

Enable `socks` to carry HTTP/3 through a SOCKS5 proxy with UDP ASSOCIATE:

```rust,no_run
# async fn request() -> Result<(), htrio::Error> {
let client = htrio::Client::builder()
    .proxy(htrio::Proxy::all("socks5h://127.0.0.1:1080")?)
    .http3_prior_knowledge()
    .build()?;
let response = client.get("https://example.com").send().await?;
# Ok(())
# }
```

| Route | Target hostname resolution | Proxy/relay hostname resolution |
| --- | --- | --- |
| Direct HTTP/3 | Client resolver | Not applicable |
| `socks5://` | Client resolver | Client resolver |
| `socks5h://` | Proxy | Client resolver |

`dns_resolver`, `resolve`, and `resolve_to_addrs` apply to client-side lookups,
including alternative hosts. An explicit URI port overrides resolver-provided
ports. Resolution changes routing, never the origin's TLS identity.

The UDP association supports RFC 1929 username/password authentication and
stays tied to its TCP control connection. Fragmented SOCKS UDP frames are
ignored. A rejected UDP setup can fall back through the same SOCKS proxy before
dispatch. Ordinary HTTP CONNECT proxies use TCP; explicit HTTP/3 through an
unsupported proxy returns an error. Failed proxies are never bypassed directly.

## TLS

The default rustls backend supports TLS 1.2 and TLS 1.3. HTTP/3 always requires
TLS 1.3. Use `tls_version_min` and `tls_version_max` to constrain negotiation.
Legacy TLS 1.0/1.1 requires the optional `native-tls` backend and
`tls_backend_native()`; availability depends on the platform's TLS policy.
Native TLS connections use HTTP/1 or HTTP/2.

## Migrating reqwest users

The API baseline is reqwest 0.13.4. Applications can retain their import name:

```toml
reqwest = { package = "htrio", git = "https://github.com/nowledge-co/htrio", features = ["json", "stream", "query", "multipart"] }
```

`rustls-tls` is an alias for `rustls`, retained for existing Mem configurations.
With `default-features = false`, explicitly enable serialization and protocol
features used by the application.

htrio and reqwest have different nominal Rust types. This alias migrates direct
application calls; third-party libraries accepting a concrete `reqwest::Client`
need an adapter or migration too. It is not a `[patch.crates-io]` substitution
for the reqwest package. Local Mem API contract tests cover thread export,
event streams, and multipart upload; full Mem integration remains separate.

## Development

Protocol behavior and configuration are documented in source rustdoc. Build
local API documentation with:

```sh
cargo doc --workspace --locked -j 2 --no-deps --features blocking,socks,hickory-dns
```

Run tests with bounded concurrency:

```sh
cargo fmt --all -- --check
find src tests examples -name '*.rs' -print0 | xargs -0 rustfmt --edition 2021 --check
cargo test --locked -j 2 --features blocking,socks,hickory-dns,native-tls -- --test-threads=2
cargo test -p htrio-h3 --locked -j 2 --all-features --lib -- --test-threads=2
```

Tests use local HTTPS/QUIC servers and SOCKS5 relays. External service tests are
explicitly ignored. [CI](.github/workflows/ci.yml) defines Linux, macOS, Windows,
MSRV, native TLS, and feature-profile checks.

The [local dependency crates](crates/README.md) are committed in this workspace.
Quinn, its protocol engine, and its UDP adapter are sibling workspace crates. See [UPSTREAM.md](UPSTREAM.md) for pinned
source revisions, licenses, and maintenance boundaries.

## License

MIT OR Apache-2.0. Derived code retains its upstream license and copyright.
