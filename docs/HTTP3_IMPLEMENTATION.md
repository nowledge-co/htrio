# HTTP/3 implementation work

## Scope and invariants

Complete the Chromium comparison gaps on top of the reqwest-style client.
Changes are delivered directly to `main` in this repository. Modified protocol,
transport, and UDP crates are sibling workspace packages with explicit path
aliases, preserved upstream licenses, and recorded upstream revisions.

- Keep SOCKS5 UDP and custom DNS support. MASQUE remains out of scope.
- Authenticate the original origin and retain proxy/credential isolation.
- Race connection preparation only. Dispatch each request body once.
- Keep early data, cross-origin pooling, persistence, and migration policy
  explicit where they affect replay, authentication, or application isolation.
- Retain the existing `dns::Resolve` interface; extend endpoint metadata without
  breaking existing resolvers.
- Use bounded parsers, queues, connection attempts, and header compression state.

## Delivery sequence

### 1. Local transport ownership and correctness

- [x] Import pinned Quinn 0.11.12, quinn-proto 0.11.19, and quinn-udp 0.5.16.
- [x] Verify one local protocol/transport type graph, licenses, MSRV, and CI.
- [x] Consume informational responses until final response; reject HTTP/3 101.
- [x] Retire GOAWAY connections from reuse and safely reconnect before dispatch.
- [x] Cancel upload tasks and source reads on request/response cancellation.
- [x] Preserve request and response trailers and validate the final frame sequence.
- [x] Enforce content lengths with HEAD/status exceptions and remaining size hints.
- [x] Bound encoded/decoded headers and control frames; skip unknown frames incrementally.
- [x] Add integration coverage for each boundary and run exact-commit CI.

### 2. Connection establishment and recovery

- [x] Race H3/TCP connection readiness without consuming or duplicating bodies.
- [x] Rotate alternative services and race/continue resolved target candidates.
- [x] Scope broken-route recovery by failure kind and network/proxy context.
- [x] Discover HTTPS/SVCB ALPN/service endpoints and apply ECH configuration.
- [x] Support custom service-endpoint metadata and compatible address overrides.
- [x] Add network-change/rebinding APIs and preserve tunnels during recovery.

### 3. HTTP/3 protocol performance and reuse

- [x] Implement opt-in 0-RTT with remembered SETTINGS, rejection handling, and replay policy.
- [x] Integrate bounded dynamic QPACK encoder/decoder streams and acknowledgements.
- [x] Map request priority to HTTP priority updates and QUIC stream scheduling.
- [x] Add opt-in authenticated cross-origin connection coalescing.
- [x] Add preconnection, keepalive, and application-controlled persistence hooks.

### 4. Observability and qualification

- [x] Expose connection timing, RTT/loss/congestion and qlog hooks.
- [x] Qualify per-hop selection/recovery/reuse metadata and background route probes.
- [x] Expose HTTP/3 TLS and network peer metadata without virtual proxy addresses.
- [x] Document native APIs, defaults, operational bounds, and remaining limitations.
- [x] Qualify default, native TLS, HTTP/3-disabled, and plain HTTP profiles in CI.
- [x] Qualify inherited h3/Quinn/protocol/UDP tests with bounded concurrency.
- [x] Qualify authenticated HTTP/2 ALTSVC frame discovery and blocking maintenance APIs.

## Acceptance

Each capability needs an executable regression covering both success and its
failure/cancellation boundary. New commits must qualify independently; previous
CI results do not qualify changed code. Chromium parity claims are limited to
implemented, tested contracts rather than browser-only features or field-trial
defaults. Full Mem application integration is a separate qualification.

## Qualification log

- `9e1ad31`: local Quinn dependency graph and compile checks qualified. The
  inherited h3 and Quinn CI jobs passed. Native semantic regressions identified
  response/Client lifetime ownership, a missing STOP_SENDING forwarding layer,
  and a GOAWAY test synchronization gap. Follow-up fixes retain the sender in
  response metadata, forward cancellation through buffered streams, expose a
  typed pre-dispatch error, and synchronize the GOAWAY regression on pool state.
- Connection/recovery batch: add TCP/QUIC preparation racing, alternative
  rotation, bounded address racing, proxy-scoped route health, network generation
  invalidation, direct endpoint rebinding, preconnect/keepalive, cache snapshots,
  and connection statistics. Qualification remains pending until its exact CI.

- `e58d72f`: MSRV, optional qlog/feature profiles, and owned Quinn CI passed.
  Existing h3 callers expected their original error variants; the recovery-aware
  entry point is now separate. STOP_SENDING regressions now assert immediate
  source cancellation rather than delivering a late source frame. Full native
  semantic qualification continues on the next commit.
- Add request priority headers/control updates/QUIC scheduling and compatible
  HTTPS service metadata. Hickory alias resolution and ECH route rebuilding are
  bounded; ECH routes do not downgrade to clear-SNI TCP. Dynamic QPACK, 0-RTT,
  and authenticated coalescing remain in progress.

- `e62ca17`: all seven CI jobs passed, including the three native platforms,
  service metadata/ECH rejection, priority, cancellation, route racing, migration,
  and inherited protocol/transport regressions.
- Dynamic QPACK batch: integrate bounded tables/instruction streams/queues,
  blocked-section wakeups, ACK/cancellation, sensitive-section bypass, and size
  limits. Keep encoded response headers across cancelled receive futures. Add
  fragmented instruction, prefix wrap, limits, and real header/trailer tests.

- Coalescing batch: opt-in reuse verifies the new origin's chain/name with the
  configured verifier, preserves proxy and destination identity, and excludes
  client certificates/ECH. Rejected origins cannot immediately coalesce again.
  Pool locks now carry a network generation so cancelled old attempts cannot
  remove or populate a new network's pool. Tests cover opted-out reuse, accepted
  multi-SAN reuse, rejected SANs, and an old/new generation lock race.

- `23542a7`: all seven exact-commit CI jobs passed, including dynamic QPACK,
  coalescing isolation, frame/header bounds, and network generation regressions.
- Early-data batch: route-scoped session caches and remembered peer SETTINGS,
  actual Quinn 0-RTT opening, acceptance/deadline barriers, credential/body policy,
  a single confirmed rejection/425 retry, and reduced-SETTINGS rejection. A local
  delayed UDP relay exercises data dispatched before confirmation, TLS rejection,
  HTTP 425, unsafe request gating, and changed server SETTINGS.
- `8a6ecf0`: all seven exact-commit CI jobs passed, including replay-gated 0-RTT,
  rejection/425 recovery, and reduced-SETTINGS rejection.
- Final qualification batch: authenticated HTTP/2 ALTSVC observation without a
  Hyper fork; compatible DNS cache invalidation; ECH ClientHello wire regression;
  per-hop selection/recovery/reuse diagnostics; bounded post-TCP QUIC probes;
  runtime-independent socket rebinding; blocking API parity; persistence bounds.
  All seven exact-commit CI jobs passed at `ab56f30`:
  [qualification run](https://github.com/nowledge-co/htrio/actions/runs/42172939161).
- Follow-up hardening retains ECH privacy on the connection after DNS records
  expire and pins selected ECH requirements across setup/retries. Expired service
  metadata cannot authorize a clear-SNI handshake. It also avoids invoking
  application DNS hooks under pool/health locks and
  rejects stale network notifications. Regressions cover hook reentrancy and
  generation ordering/wrap. Every new main commit must pass its own CI;
  historical qualification never substitutes for the current commit's result.

## Deliberate boundaries

Chromium's browser profile, OS network detection, disk cache, and field-trial
defaults are application policy. Early data and origin coalescing are opt-in;
network notification and cache persistence are explicit APIs. ECH ClientHello
encryption and rejection/privacy boundaries have executable coverage; successful
interoperation with a separate production ECH server is not yet qualified.
MASQUE, quiche/BoringSSL, WebTransport, and WASM are outside this client's scope.
