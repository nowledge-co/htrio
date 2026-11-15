# Changelog

## 0.3.0 - 2026-11-16

### Fixes

- Apply the configured retry policy to automatic HTTP/3 421 recovery, including
  disabled retries, host scopes, classifiers, request limits and budgets.
  Rejected alternatives are still evicted when replay is refused. ([#6])
- Reject RETIRE_CONNECTION_ID sequence numbers at or beyond the next unissued
  CID with PROTOCOL_VIOLATION, including the first invalid sequence immediately
  after handshake or subsequent issuance. ([#8])

### Regression coverage

- Verify HTTP/3 priority propagation to the Quinn send scheduler and the peer's
  control stream, including queue saturation and coalescing. ([#7], [#8])
- Cover asymmetric zero-length CIDs, peer and local issuance limits, repeated
  port/IP migrations, and duplicate CID retirements. ([#7], [#8])

### Dependencies

- Require `htrio-quinn >= 0.11.14`, which requires
  `htrio-quinn-proto >= 0.11.21`, so registry consumers receive the CID fix.
- The minimum supported Rust version remains 1.89.

## 0.2.0 - 2026-11-14

### Performance

- Prune only the requested Alt-Svc origin during candidate lookup, avoiding a
  scan of the entire cache while holding its mutex. Full expiry cleanup remains
  on insertion and cache observation operations. ([#2])
- Skip HTTP/3 coalescing candidate scans and route-health locking when
  cross-origin reuse is disabled or the selected route requires ECH. Existing
  certificate and connection-isolation checks remain in place. ([#3])
- Add reproducible Alt-Svc cache and HTTP/3 connection-preparation benchmarks.
  Measurements, raw samples, and limitations are recorded in [#2] and [#3].

### Fixes

- Reject ACK Frequency delays at or above the exclusive 2^14 millisecond bound,
  clamp generated delays below that bound, and reject ACK_FREQUENCY and
  IMMEDIATE_ACK frames in 0-RTT packets. ([#1])
- Grow QUIC receive buffers when rebinding to a socket with greater GRO
  capacity, and retain the high-water capacity while previous sockets can still
  deliver aggregated datagrams. ([#1])

### Dependencies

- Require `htrio-quinn >= 0.11.13`, which requires
  `htrio-quinn-proto >= 0.11.20`, to include the transport fixes for registry
  consumers. Other local dependency packages retain their existing versions.
- The minimum supported Rust version remains 1.89.

[#1]: https://github.com/nowledge-co/htrio/pull/1
[#2]: https://github.com/nowledge-co/htrio/pull/2
[#3]: https://github.com/nowledge-co/htrio/pull/3
[#6]: https://github.com/nowledge-co/htrio/pull/6
[#7]: https://github.com/nowledge-co/htrio/pull/7
[#8]: https://github.com/nowledge-co/htrio/pull/8

## 0.1.0 - 2026-11-11

- Publish the native HTTP/1, HTTP/2, and HTTP/3 client and its six local protocol
  and transport dependencies on crates.io.
