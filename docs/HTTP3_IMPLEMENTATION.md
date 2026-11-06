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
- [ ] Verify one local protocol/transport type graph, licenses, MSRV, and CI.
- [ ] Consume informational responses until final response; reject HTTP/3 101.
- [ ] Retire GOAWAY connections from reuse and safely reconnect before dispatch.
- [ ] Cancel upload tasks and source reads on request/response cancellation.
- [ ] Preserve request and response trailers and validate the final frame sequence.
- [ ] Enforce content lengths with HEAD/status exceptions and remaining size hints.
- [ ] Bound encoded/decoded headers and control frames; skip unknown frames incrementally.
- [ ] Add integration coverage for each boundary and run exact-commit CI.

### 2. Connection establishment and recovery

- [ ] Race H3/TCP connection readiness without consuming or duplicating bodies.
- [ ] Rotate alternative services and race/continue resolved target candidates.
- [ ] Scope broken-route recovery by failure kind and network/proxy context.
- [ ] Discover HTTPS/SVCB ALPN/service endpoints and apply ECH configuration.
- [ ] Support custom service-endpoint metadata and compatible address overrides.
- [ ] Add network-change/rebinding APIs and preserve tunnels during recovery.

### 3. HTTP/3 protocol performance and reuse

- [ ] Implement opt-in 0-RTT with remembered SETTINGS, rejection handling, and replay policy.
- [ ] Integrate bounded dynamic QPACK encoder/decoder streams and acknowledgements.
- [ ] Map request priority to HTTP priority updates and QUIC stream scheduling.
- [ ] Add opt-in authenticated cross-origin connection coalescing.
- [ ] Add preconnection, keepalive, and application-controlled persistence hooks.

### 4. Observability and qualification

- [ ] Expose connection timing, RTT/loss/congestion, pool/fallback reasons, and qlog hooks.
- [ ] Expose HTTP/3 TLS and network peer metadata without virtual proxy addresses.
- [ ] Document native APIs, defaults, operational bounds, and remaining limitations.
- [ ] Qualify default, native TLS, HTTP/3-disabled, and plain HTTP profiles in CI.
- [ ] Qualify inherited h3/Quinn/protocol/UDP tests with bounded concurrency.

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
