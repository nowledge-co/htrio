//! Measure the production cache lookup without exposing a benchmark-only API.

#[allow(dead_code)]
#[path = "../src/alt_svc.rs"]
mod alt_svc;

use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use alt_svc::AltSvcCache;
use http::{HeaderMap, StatusCode, Uri};

struct Case {
    name: &'static str,
    origins: usize,
    alternatives: usize,
    hit: bool,
    threads: usize,
}

impl Case {
    fn cache(&self) -> (AltSvcCache, Uri) {
        let cache = AltSvcCache::with_capacity(self.origins);
        let mut headers = HeaderMap::new();
        let alternatives = (0..self.alternatives)
            .map(|index| format!("h3=\":{}\";ma=3600", 8443 + index))
            .collect::<Vec<_>>()
            .join(", ");
        headers.insert("alt-svc", alternatives.parse().unwrap());
        for index in 0..self.origins {
            cache.observe(
                &format!("https://origin{index}.bench.invalid/")
                    .parse()
                    .unwrap(),
                StatusCode::OK,
                &headers,
                Duration::ZERO,
            );
        }
        let origin = if self.hit {
            "https://origin0.bench.invalid/"
        } else {
            "https://missing.bench.invalid/"
        }
        .parse()
        .unwrap();
        assert_eq!(cache.len(), self.origins);
        assert_eq!(
            cache.candidates(&origin).len(),
            if self.hit { self.alternatives } else { 0 }
        );
        (cache, origin)
    }

    fn sample(&self, cache: &AltSvcCache, origin: &Uri, iterations: usize) -> f64 {
        let elapsed = if self.threads == 1 {
            let started = Instant::now();
            lookups(cache, origin, iterations);
            started.elapsed()
        } else {
            std::thread::scope(|scope| {
                let ready = Arc::new(Barrier::new(self.threads + 1));
                let start = Arc::new(Barrier::new(self.threads + 1));
                let workers: Vec<_> = (0..self.threads)
                    .map(|_| {
                        let ready = ready.clone();
                        let start = start.clone();
                        scope.spawn(move || {
                            ready.wait();
                            start.wait();
                            lookups(cache, origin, iterations);
                        })
                    })
                    .collect();
                ready.wait();
                let started = Instant::now();
                start.wait();
                for worker in workers {
                    worker.join().unwrap();
                }
                started.elapsed()
            })
        };
        elapsed.as_secs_f64() * 1e9 / (iterations * self.threads) as f64
    }
}

fn lookups(cache: &AltSvcCache, origin: &Uri, iterations: usize) {
    for _ in 0..iterations {
        black_box(cache.candidates(black_box(origin)));
    }
}

fn main() {
    let mut iterations = 50_000;
    let mut samples = 7;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--iterations" => {
                iterations = args
                    .next()
                    .expect("missing iteration count")
                    .parse()
                    .unwrap()
            }
            "--samples" => samples = args.next().expect("missing sample count").parse().unwrap(),
            "--bench" => {}
            _ => panic!("unknown argument: {arg}"),
        }
    }
    assert!(iterations > 0 && samples > 0);

    let cases = [
        Case {
            name: "empty_miss",
            origins: 0,
            alternatives: 0,
            hit: false,
            threads: 1,
        },
        Case {
            name: "single_hit",
            origins: 1,
            alternatives: 1,
            hit: true,
            threads: 1,
        },
        Case {
            name: "64_origins_hit",
            origins: 64,
            alternatives: 1,
            hit: true,
            threads: 1,
        },
        Case {
            name: "256_origins_hit",
            origins: 256,
            alternatives: 1,
            hit: true,
            threads: 1,
        },
        Case {
            name: "full_hit",
            origins: 256,
            alternatives: 8,
            hit: true,
            threads: 1,
        },
        Case {
            name: "full_miss",
            origins: 256,
            alternatives: 8,
            hit: false,
            threads: 1,
        },
        Case {
            name: "shared_full_hit",
            origins: 256,
            alternatives: 8,
            hit: true,
            threads: 4,
        },
        Case {
            name: "shared_full_miss",
            origins: 256,
            alternatives: 8,
            hit: false,
            threads: 4,
        },
    ];
    for case in cases {
        let (cache, origin) = case.cache();
        lookups(&cache, &origin, 1_000);
        let mut times: Vec<_> = (0..samples)
            .map(|_| case.sample(&cache, &origin, iterations))
            .collect();
        times.sort_by(f64::total_cmp);
        println!(
            "{{\"case\":\"{}\",\"origins\":{},\"alternatives\":{},\"threads\":{},\"iterations_per_thread\":{},\"samples\":{},\"median_ns_per_lookup\":{:.2},\"min_ns_per_lookup\":{:.2},\"max_ns_per_lookup\":{:.2},\"ns_per_lookup_samples\":{:?}}}",
            case.name, case.origins, case.alternatives, case.threads, iterations,
            samples, times[samples / 2], times[0], times[samples - 1], times
        );
    }
}
