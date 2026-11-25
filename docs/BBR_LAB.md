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

## Portable result bundles

After building the native example, use the Python 3 standard-library collector
instead of redirecting JSONL manually. Keep the same server running while
switching the client's access network:

```sh
export HTRIO_LAB_TOKEN=replaceWithTheSameLongRandomTokenOnBothHosts
python3 scripts/run-bbr-lab.py collect https://lab.example.test:4433/ \
  ./lab-ca.pem home-wifi --bytes 8388608 --runs 10 \
  --note "Home Wi-Fi to the fixed lab server; no VPN or competing uploads"
```

The final `Send back:` line identifies a `.tar.gz` archive with a SHA256 sidecar.
It contains raw attempts, success/failure summaries, stderr, timestamps,
source/binary identities, OS/architecture, parameters and your note. Peer
addresses are present in successful receipts. The collector does not include
the shared token, private keys, or an environment-variable dump. Nonzero exits
and interrupted runs still produce a bundle; an incomplete receipt sequence is
marked incomplete rather than treated as a successful run. Use a new label and
output directory for each network. `--binary` selects a separately built example.

## Isolated Docker/netem experiment

This command builds the Linux example and collects all four network profiles:

```sh
python3 scripts/run-bbr-lab.py netem --runs 3
```

It requires Docker and Python 3 on the host. No host ports or host directories
are mounted into the experiment. Two temporary containers use an internal bridge;
only their own network interfaces receive `NET_ADMIN` shaping. Each container
has a two-CPU quota and 512 MiB memory limit. The collector removes its own
containers and bridge after completion or interruption. Existing Docker resources
are untouched.

| Profile | Nominal RTT | Rate in each direction | Jitter per direction | Random loss per direction | Queue limit per direction |
| --- | --- | --- | --- | --- | --- |
| clean | 40 ms | 10 Mbit/s | 0 ms | 0% | 128 packets |
| long-rtt | 160 ms | 10 Mbit/s | 0 ms | 0% | 256 packets |
| lossy | 40 ms | 10 Mbit/s | 5 ms | 1% | 128 packets |
| slow | 80 ms | 2 Mbit/s | 5 ms | 0.1% | 64 packets |

The `tc netem` qdisc combines rate, delay and loss on each endpoint's egress.
The queue limit includes packets held for simulated propagation and rate
serialization; it is not an additional queue-time budget. Seed 7 is used on
the client and seed 8 on the server. Fixed seeds do not give different
algorithms identical loss traces because their packet timings differ.
GSO, GRO and UDP segmentation are disabled and checked before impairment;
otherwise a single emulated loss might drop an entire offloaded batch.

The bundle includes the exact image ID, Docker/kernel details, profile parameters,
offload state, qdisc configuration/counters before and after uploads, RTT probes,
server/client logs and per-attempt results. The collector fails if shaping or
offload configuration fails, and checks that RTT probes confirm the imposed delay.
Linux scheduling and timer granularity can affect `netem` rates and burstiness;
Docker Desktop also adds its VM and CPU effects. These are emulated single-flow
baselines, not physical-network or concurrent-flow fairness measurements.
See the [netem manual](https://man7.org/linux/man-pages/man8/tc-netem.8.html).

For a quick smoke run, select profiles and reduce the payload/repetitions:

```sh
python3 scripts/run-bbr-lab.py netem --profiles clean lossy --runs 1 --bytes 262144
```

`--skip-build --image IMAGE` reuses an existing lab image and records its ID;
otherwise the current source is built with two Cargo jobs. The manual
`Congestion qualification` GitHub Actions workflow runs this same experiment and
uploads the archive even on failure. PR checks run the deterministic model and
collector regression tests; the Docker experiment is opt-in.

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
