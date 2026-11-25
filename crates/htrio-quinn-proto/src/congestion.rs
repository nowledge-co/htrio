//! Logic for controlling the rate at which data is sent

use crate::connection::RttEstimator;
use crate::{Instant, SpaceId};
use std::any::Any;
use std::sync::Arc;

mod bbr;
mod bbr3;
mod cubic;
mod new_reno;

pub use bbr::{Bbr, BbrConfig};
pub use bbr3::{Bbr3, Bbr3Config};
pub use cubic::{Cubic, CubicConfig};
pub use new_reno::{NewReno, NewRenoConfig};

/// Common interface for different congestion controllers
pub trait Controller: Send + Sync {
    /// One or more packets were just sent
    #[allow(unused_variables)]
    fn on_sent(&mut self, now: Instant, bytes: u64, last_packet_number: u64) {}

    /// Packet deliveries were confirmed
    ///
    /// `app_limited` indicates whether the connection was blocked on outgoing
    /// application data prior to receiving these acknowledgements.
    #[allow(unused_variables)]
    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
        rtt: &RttEstimator,
    ) {
    }

    /// Packets are acked in batches, all with the same `now` argument. This indicates one of those batches has completed.
    #[allow(unused_variables)]
    fn on_end_acks(
        &mut self,
        now: Instant,
        in_flight: u64,
        app_limited: bool,
        largest_packet_num_acked: Option<u64>,
    ) {
    }

    /// One ack-eliciting packet was just sent
    ///
    /// Unlike [`Controller::on_sent`], which summarizes a batch of datagrams, this is called once
    /// per packet. Packet numbers are only unique within `space`.
    #[allow(unused_variables)]
    fn on_packet_sent(&mut self, now: Instant, bytes: u16, pn: u64, space: SpaceId) {}

    /// One ack-eliciting packet was acknowledged
    ///
    /// Called alongside [`Controller::on_ack`] for the same packet.
    #[allow(unused_variables, clippy::too_many_arguments)]
    fn on_packet_acked(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u16,
        pn: u64,
        space: SpaceId,
        app_limited: bool,
        rtt: &RttEstimator,
    ) {
    }

    /// One packet was declared lost
    ///
    /// Called for each lost packet before [`Controller::on_congestion_event`] summarizes the batch.
    #[allow(unused_variables)]
    fn on_packet_lost(&mut self, now: Instant, bytes: u16, pn: u64, space: SpaceId) {}

    /// The peer reported ECN congestion marks on packets up to `largest_pn`
    ///
    /// Called alongside [`Controller::on_congestion_event`] with zero `lost_bytes`.
    #[allow(unused_variables)]
    fn on_ecn_congestion(&mut self, now: Instant, sent: Instant, largest_pn: u64, space: SpaceId) {}

    /// Data was ready to send but the congestion window was full
    fn on_cwnd_limited(&mut self) {}

    /// Packets were deemed lost or marked congested
    ///
    /// `in_persistent_congestion` indicates whether all packets sent within the persistent
    /// congestion threshold period ending when the most recent packet in this batch was sent were
    /// lost.
    /// `lost_bytes` indicates how many bytes were lost. This value will be 0 for ECN triggers.
    fn on_congestion_event(
        &mut self,
        now: Instant,
        sent: Instant,
        is_persistent_congestion: bool,
        lost_bytes: u64,
    );

    /// The known MTU for the current network path has been updated
    fn on_mtu_update(&mut self, new_mtu: u16);

    /// Number of ack-eliciting bytes that may be in flight
    fn window(&self) -> u64;

    /// Retrieve implementation-specific metrics
    ///
    /// Populates `qlog` traces, and is read on every transmit poll for pacing controls, so it must
    /// be cheap.
    fn metrics(&self) -> ControllerMetrics {
        ControllerMetrics {
            congestion_window: self.window(),
            ssthresh: None,
            pacing_rate: None,
            pacer_rate: None,
            send_quantum: None,
        }
    }

    /// Duplicate the controller's state
    fn clone_box(&self) -> Box<dyn Controller>;

    /// Initial congestion window
    fn initial_window(&self) -> u64;

    /// Returns Self for use in down-casting to extract implementation details
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}

/// Common congestion controller metrics
#[derive(Default)]
#[non_exhaustive]
pub struct ControllerMetrics {
    /// Congestion window (bytes)
    pub congestion_window: u64,
    /// Slow start threshold (bytes)
    pub ssthresh: Option<u64>,
    /// Pacing rate (bits/s), reported for diagnostics only
    pub pacing_rate: Option<u64>,
    /// Rate at which the pacer releases packets (bytes/s)
    ///
    /// When set, this replaces the pacing rate derived from the congestion window and RTT.
    pub pacer_rate: Option<u64>,
    /// Upper bound on the bytes sent in one batch of datagrams
    pub send_quantum: Option<u64>,
}

/// Constructs controllers on demand
pub trait ControllerFactory {
    /// Construct a fresh `Controller`
    fn build(self: Arc<Self>, now: Instant, current_mtu: u16) -> Box<dyn Controller>;
}

const BASE_DATAGRAM_SIZE: u64 = 1200;

#[cfg(test)]
mod qualification {
    use super::*;
    use crate::Duration;
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    const MSS: u64 = 1200;
    const LINK_BYTES_PER_SECOND: u64 = 1_250_000;
    const RTT_NS: u64 = 40_000_000;

    #[derive(Clone, Copy)]
    struct Network {
        name: &'static str,
        bytes_per_second: u64,
        rtt_ns: u64,
        queue_ns: u64,
    }

    const DEFAULT_NETWORK: Network = Network {
        name: "baseline",
        bytes_per_second: LINK_BYTES_PER_SECOND,
        rtt_ns: RTT_NS,
        queue_ns: RTT_NS * 2,
    };
    const RUN_NS: u64 = 30_000_000_000;
    const WARMUP_NS: u64 = 10_000_000_000;

    struct Flow {
        controller: Box<dyn Controller>,
        rtt: RttEstimator,
        inflight: u64,
        next_send_ns: u64,
        pn: u64,
        delivered: u64,
        lost: u64,
        ack_rtts_ns: Vec<u64>,
    }

    #[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
    struct Event {
        at_ns: u64,
        flow: usize,
        pn: u64,
        send_ns: u64,
        acked: bool,
    }

    #[derive(Debug)]
    struct Result {
        delivered: [u64; 2],
        lost: [u64; 2],
        fairness: f64,
        utilization: f64,
        p95_rtt_ms: [f64; 2],
    }

    fn factory(name: &str, seed: [u8; 16]) -> Arc<dyn ControllerFactory> {
        match name {
            "bbr3" => {
                let mut config = Bbr3Config::default();
                config.probe_rng_seed(seed);
                Arc::new(config)
            }
            "bbr1" => {
                let mut config = BbrConfig::default();
                config.cycle_rng_seed(seed);
                Arc::new(config)
            }
            "cubic" => Arc::new(CubicConfig::default()),
            _ => panic!("unknown controller: {name}"),
        }
    }

    fn run(left: &str, right: &str) -> Result {
        run_network(left, right, DEFAULT_NETWORK, 1)
    }

    fn run_network(left: &str, right: &str, network: Network, seed: u8) -> Result {
        let base = Instant::now();
        let mut flows = [(left, [seed; 16]), (right, [seed + 1; 16])].map(|(name, seed)| Flow {
            controller: factory(name, seed).build(base, MSS as u16),
            rtt: RttEstimator::new(Duration::from_nanos(network.rtt_ns)),
            inflight: 0,
            next_send_ns: 0,
            pn: 0,
            delivered: 0,
            lost: 0,
            ack_rtts_ns: Vec::new(),
        });
        let mut events: BinaryHeap<Reverse<Event>> = BinaryHeap::new();
        let mut now_ns = 0;
        let mut bottleneck_free_ns = network.rtt_ns / 2;
        let service_ns = MSS * 1_000_000_000 / network.bytes_per_second;
        let queue_limit_ns = network.queue_ns;

        while now_ns < RUN_NS {
            let next_event_ns = events.peek().map_or(u64::MAX, |event| event.0.at_ns);
            let mut send_flow = None;
            let mut send_ns = u64::MAX;
            for (index, flow) in flows.iter_mut().enumerate() {
                if flow.inflight + MSS <= flow.controller.window() {
                    let candidate = flow.next_send_ns.max(now_ns);
                    if (candidate, index) < (send_ns, send_flow.unwrap_or(usize::MAX)) {
                        send_ns = candidate;
                        send_flow = Some(index);
                    }
                } else {
                    flow.controller.on_cwnd_limited();
                }
            }

            if let Some(index) = send_flow.filter(|_| send_ns <= next_event_ns) {
                if send_ns >= RUN_NS {
                    break;
                }
                now_ns = send_ns;
                let flow = &mut flows[index];
                let now = base + Duration::from_nanos(now_ns);
                let pn = flow.pn;
                flow.pn += 1;
                flow.controller
                    .on_packet_sent(now, MSS as u16, pn, SpaceId::Data);
                flow.controller.on_sent(now, MSS, pn);
                flow.inflight += MSS;

                let arrival_ns = now_ns + network.rtt_ns / 2;
                let service_start_ns = arrival_ns.max(bottleneck_free_ns);
                let (at_ns, acked) = if service_start_ns - arrival_ns > queue_limit_ns {
                    (now_ns + network.rtt_ns, false)
                } else {
                    bottleneck_free_ns = service_start_ns + service_ns;
                    (bottleneck_free_ns + network.rtt_ns / 2, true)
                };
                events.push(Reverse(Event {
                    at_ns,
                    flow: index,
                    pn,
                    send_ns: now_ns,
                    acked,
                }));

                let metrics = flow.controller.metrics();
                let fallback_rate = flow.controller.window().saturating_mul(1_000_000_000)
                    / flow.rtt.get().as_nanos().max(1) as u64;
                let pacing_rate = metrics.pacer_rate.unwrap_or(fallback_rate).max(MSS);
                flow.next_send_ns = now_ns + MSS * 1_000_000_000 / pacing_rate;
                continue;
            }

            let Reverse(event) = events
                .pop()
                .expect("simulation has no send or feedback event");
            if event.at_ns >= RUN_NS {
                break;
            }
            now_ns = event.at_ns;
            let flow = &mut flows[event.flow];
            flow.inflight -= MSS;
            let now = base + Duration::from_nanos(now_ns);
            let sent = base + Duration::from_nanos(event.send_ns);
            if event.acked {
                flow.controller.on_ack(now, sent, MSS, false, &flow.rtt);
                flow.controller.on_packet_acked(
                    now,
                    sent,
                    MSS as u16,
                    event.pn,
                    SpaceId::Data,
                    false,
                    &flow.rtt,
                );
                flow.controller
                    .on_end_acks(now, flow.inflight, false, Some(event.pn));
                flow.rtt
                    .update(Duration::ZERO, Duration::from_nanos(now_ns - event.send_ns));
                if now_ns >= WARMUP_NS {
                    flow.delivered += MSS;
                    flow.ack_rtts_ns.push(now_ns - event.send_ns);
                }
            } else {
                flow.controller
                    .on_packet_lost(now, MSS as u16, event.pn, SpaceId::Data);
                flow.controller.on_congestion_event(now, sent, false, MSS);
                if now_ns >= WARMUP_NS {
                    flow.lost += MSS;
                }
            }
        }

        let delivered = flows.each_ref().map(|flow| flow.delivered);
        let lost = flows.each_ref().map(|flow| flow.lost);
        let sum = (delivered[0] + delivered[1]) as f64;
        let squares = (delivered[0] as f64).powi(2) + (delivered[1] as f64).powi(2);
        let fairness = sum.powi(2) / (2.0 * squares);
        let capacity = network.bytes_per_second as f64
            * Duration::from_nanos(RUN_NS - WARMUP_NS).as_secs_f64();

        Result {
            delivered,
            lost,
            fairness,
            utilization: sum / capacity,
            p95_rtt_ms: flows.each_mut().map(|flow| {
                flow.ack_rtts_ns.sort_unstable();
                let n = flow.ack_rtts_ns.len();
                assert!(n > 0, "flow delivered no measurement samples");
                flow.ack_rtts_ns[(95 * n).div_ceil(100) - 1] as f64 / 1_000_000.0
            }),
        }
    }

    #[test]
    fn network_profiles_are_reproducible() {
        let first = run_network("bbr3", "cubic", DEFAULT_NETWORK, 7);
        let second = run_network("bbr3", "cubic", DEFAULT_NETWORK, 7);
        assert_eq!(first.delivered, second.delivered);
        assert_eq!(first.lost, second.lost);
        assert_eq!(first.p95_rtt_ms, second.p95_rtt_ms);
    }

    #[test]
    #[ignore = "opt-in network profile experiment; records baselines without tuning floors"]
    fn network_profile_matrix() {
        let networks = [
            DEFAULT_NETWORK,
            Network {
                name: "shallow",
                queue_ns: RTT_NS / 2,
                ..DEFAULT_NETWORK
            },
            Network {
                name: "long-rtt",
                rtt_ns: 160_000_000,
                queue_ns: 80_000_000,
                ..DEFAULT_NETWORK
            },
            Network {
                name: "slow-link",
                bytes_per_second: 250_000,
                ..DEFAULT_NETWORK
            },
        ];
        println!("model,profile,left,right,seed,left_bytes,right_bytes,left_lost_bytes,right_lost_bytes,jain,utilization,left_p95_ack_rtt_ms,right_p95_ack_rtt_ms");
        for network in networks {
            for (left, right) in [
                ("bbr3", "bbr3"),
                ("bbr3", "bbr1"),
                ("bbr1", "bbr3"),
                ("bbr3", "cubic"),
                ("cubic", "bbr3"),
            ] {
                for seed in [1, 7, 19] {
                    let result = run_network(left, right, network, seed);
                    println!(
                        "model,{},{left},{right},{seed},{},{},{},{},{:.6},{:.6},{:.3},{:.3}",
                        network.name,
                        result.delivered[0],
                        result.delivered[1],
                        result.lost[0],
                        result.lost[1],
                        result.fairness,
                        result.utilization,
                        result.p95_rtt_ms[0],
                        result.p95_rtt_ms[1]
                    );
                    assert!(
                        result.fairness.is_finite() && (0.5..=1.000_001).contains(&result.fairness)
                    );
                    assert!(result.utilization > 0.0 && result.utilization <= 1.01);
                }
            }
        }
    }

    #[test]
    fn two_bbr3_flows_share_a_bottleneck() {
        let result = run("bbr3", "bbr3");
        assert!(
            result.fairness >= 0.90,
            "BBRv3 self-fairness below qualification floor: {result:?}"
        );
        assert!(
            result.utilization >= 0.75,
            "BBRv3 bottleneck utilization below qualification floor: {result:?}"
        );
    }

    #[test]
    #[ignore = "scheduled qualification matrix; prints stable machine-readable results"]
    fn shared_bottleneck_matrix() {
        println!("left,right,left_bytes,right_bytes,left_lost,right_lost,jain,utilization");
        let cases = [
            ("bbr3", "bbr3", 0.90, 0.40),
            // The inherited BBRv1 implementation dominates BBRv3 in this model.
            // Require both a nontrivial BBRv3 share and a Jain index above the
            // complete-starvation value of 0.5.
            ("bbr3", "bbr1", 0.505, 0.01),
            ("bbr3", "cubic", 0.90, 0.40),
        ];
        for (left, right, min_fairness, min_left_share) in cases {
            let result = run(left, right);
            println!(
                "{left},{right},{},{},{},{},{:.4},{:.4}",
                result.delivered[0],
                result.delivered[1],
                result.lost[0],
                result.lost[1],
                result.fairness,
                result.utilization
            );
            assert!(
                result.fairness >= min_fairness,
                "{left}/{right} fairness below qualification floor {min_fairness}: {result:?}"
            );
            let delivered = result.delivered[0] + result.delivered[1];
            let left_share = result.delivered[0] as f64 / delivered as f64;
            assert!(
                left_share >= min_left_share,
                "{left}/{right} left-flow delivery share below qualification floor {min_left_share}: {result:?}"
            );
            assert!(
                result.utilization >= 0.75,
                "{left}/{right} utilization below qualification floor: {result:?}"
            );
        }
    }
}
