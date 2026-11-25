# BBR qualification lab

This opt-in lab compares the existing CUBIC, BBRv1, and BBRv3 implementations.
It does not change production algorithms or defaults. Precautionary BBRv3 probing
is a future candidate; this first stage establishes reproducible baselines and
regression coverage before changing its state machine.

## Deterministic model

The shared-bottleneck model uses two continuously backlogged senders, 1200-byte
packets, per-packet feedback, fixed distinct RNG seeds, and production ACK/RTT
callback ordering. Each run lasts 30 simulated seconds; the first 10 seconds are
excluded from measurement. The experiment sweeps:

| Profile | Link rate | Base RTT | Queue time limit |
| --- | --- | --- | --- |
| baseline | 10 Mbit/s | 40 ms | 80 ms |
| shallow | 10 Mbit/s | 40 ms | 20 ms |
| long-rtt | 10 Mbit/s | 160 ms | 80 ms |
| slow-link | 2 Mbit/s | 40 ms | 80 ms |

Every profile compares BBRv3/BBRv3, BBRv3/BBRv1, and BBRv3/CUBIC, with mixed
pairs in both orders and three seed pairs. Output includes delivered/lost bytes,
Jain fairness, utilization, and per-flow p95 packet ACK RTT. It records outcomes
without lowering existing qualification floors to fit a result.

```sh
cargo test -p htrio-quinn-proto --locked -j 2 --lib \
  congestion::qualification::network_profile_matrix -- --ignored --nocapture
```

Rows starting with `model,` are CSV. This model omits delayed ACKs, ACK aggregation,
transport retransmissions, reverse-path contention, and host CPU effects. Its
per-packet scheduler uses controller pacing rates when present and otherwise
`cwnd / RTT`; it does not reproduce Quinn's token-bucket burst budget. Treat
its metrics as model outcomes; validate algorithm candidates through real QUIC
connections and multiple networks before promoting them.

## Real-network upload measurements

Use two machines when measuring a network path. On the server, build the same
revision and start the authenticated upload sink:

```sh
HTRIO_LAB_REVISION=$(git rev-parse HEAD) cargo build --locked -j 2 --release \
  --example bbr_lab --no-default-features --features http3,json
export HTRIO_LAB_TOKEN=replaceWithTheSameLongRandomTokenOnBothHosts
./target/release/examples/bbr_lab serve 0.0.0.0:4433 lab.example.test /tmp/lab-ca.pem
```

The certificate name must match the client's URL hostname or IP. Transfer the
certificate to the client through a trusted channel. The server creates an
in-memory private key, refuses to overwrite the certificate file, limits each
upload to 64 MiB, bounds connections, and uses a 120-second request timeout.
Allow UDP to the chosen port; stop the server when the experiment ends.

On the client, build the same command above and run:

```sh
export HTRIO_LAB_TOKEN=replaceWithTheSameLongRandomTokenOnBothHosts
./target/release/examples/bbr_lab run https://lab.example.test:4433/ \
  ./lab-ca.pem home-wifi 8388608 5 > home-wifi.jsonl
python3 scripts/summarize-bbr-lab.py home-wifi.jsonl
```

Arguments after the network label are upload bytes and repetitions per algorithm.
For loopback validation, use `127.0.0.1:4433` for the bind address and `127.0.0.1`
for the certificate name and URL host. The CA certificate is pinned, proxies and
redirects are disabled, and HTTP/3 plus connection reuse are checked explicitly.

Each algorithm gets a fresh connection and an unmeasured `/ready` exchange.
The upload measurement uses that same connection and verifies the server's byte
receipt. Algorithm order rotates across repetitions. The client records every
attempt, including errors, and returns nonzero if any attempt fails. Keep those
records when comparing networks or revisions.

The upload sender's congestion controller is under test. Merely selecting BBRv3
on a client download would mainly measure the server's sending algorithm.
The real-network tool currently measures single-flow uploads; Jain fairness is
reported only by the shared-bottleneck model, not inferred from sequential runs.

Receipts include revision, version, OS/architecture, network label, trial/order,
peer, verified application bytes, elapsed time, Mbit/s, pre-upload RTT, sampled
p95 smoothed RTT, sent/lost packets, lost bytes, congestion events, cwnd, and MTU.
The p95 value is a quantile of smoothed RTT samples every 25 ms, not per-packet
latency. Short uploads have few samples. Loss counters are sender-reported
observations at completion, not a configured loss probability. Warmup traffic is
excluded from counter deltas and elapsed time; congestion-window history from
warmup remains, so this is not a cold-start measurement.

## Collection protocol

For each network, record its access type, approximate location pair, wired/Wi-Fi
status, VPN/tunnel/proxy conditions, and whether traffic shaping is active. Keep
payload size, peer, revision, and repetition count fixed. Start with five runs,
then use at least ten for comparisons; retain all raw JSONL files. Run tests
sequentially and avoid other heavy transfers on either endpoint.

Compare completion/failure rate first, then median and lower-tail throughput,
sampled RTT and sender-reported loss. Compare an algorithm candidate against the
same baseline revision under paired network conditions. Loopback only verifies
the harness. It does not establish WAN performance, fairness, or an improvement.

Before implementing precautionary probing, add state-transition regressions for
early stop at the inflight bound, one-round probe feedback across DOWN/CRUISE,
fast reprobe only after safe feedback, and application-limited/ProbeRTT paths.
Keep any candidate opt-in until throughput, latency, loss, and coexistence data
support it; do not change the default controller based on a single network.
