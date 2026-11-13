//! Measure connection preparation with real pooled QUIC connections and local DNS.

#[path = "../tests/support/local_tls.rs"]
mod local_tls;

use std::hint::black_box;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use htrio::dns::{Addrs, Name, Resolve, Resolving};
use htrio::{Client, Url};
use local_tls::LocalServer;

struct Resolver {
    address: SocketAddr,
    failures: AtomicUsize,
}

impl Resolve for Resolver {
    fn resolve(&self, name: Name) -> Resolving {
        if name.as_str() == "missing.bench.invalid" {
            self.failures.fetch_add(1, Ordering::Relaxed);
            Box::pin(std::future::ready(Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "benchmark DNS miss",
            )
            .into())))
        } else {
            let addresses: Addrs = Box::new(std::iter::once(self.address));
            Box::pin(std::future::ready(Ok(addresses)))
        }
    }
}

struct Case {
    name: &'static str,
    origins: usize,
    coalescing: bool,
    hit: bool,
}

impl Case {
    async fn run(&self, server: &LocalServer, iterations: usize, samples: usize) {
        let resolver = Arc::new(Resolver {
            address: server.quic_addr,
            failures: AtomicUsize::new(0),
        });
        let client = server
            .client_builder()
            .http3_prior_knowledge()
            .http3_coalescing(self.coalescing)
            .pool_idle_timeout(None)
            .dns_resolver(resolver.clone())
            .build()
            .unwrap();
        for index in 0..self.origins {
            client
                .preconnect(Url::parse(&format!("https://origin{index}.bench.invalid/")).unwrap())
                .await
                .unwrap();
        }
        assert_eq!(client.http3_pool_stats().available, self.origins);
        let origin = Url::parse(if self.hit {
            "https://origin0.bench.invalid/"
        } else {
            "https://missing.bench.invalid/"
        })
        .unwrap();
        prepare(&client, &origin, self.hit, 1_000).await;
        let failures = resolver.failures.load(Ordering::Relaxed);
        let mut times = Vec::with_capacity(samples);
        for _ in 0..samples {
            let started = Instant::now();
            prepare(&client, &origin, self.hit, iterations).await;
            times.push(started.elapsed().as_secs_f64() * 1e9 / iterations as f64);
        }
        assert_eq!(client.http3_pool_stats().available, self.origins);
        assert_eq!(client.http3_pool_stats().connecting, 0);
        assert_eq!(
            resolver.failures.load(Ordering::Relaxed) - failures,
            if self.hit { 0 } else { iterations * samples }
        );
        times.sort_by(f64::total_cmp);
        println!(
            "{{\"case\":\"{}\",\"origins\":{},\"coalescing\":{},\"hit\":{},\"iterations\":{},\"samples\":{},\"median_ns_per_prepare\":{:.2},\"ns_per_prepare_samples\":{:?}}}",
            self.name, self.origins, self.coalescing, self.hit, iterations,
            samples, times[samples / 2], times
        );
    }
}

async fn prepare(client: &Client, origin: &Url, hit: bool, iterations: usize) {
    for _ in 0..iterations {
        let result = black_box(client.preconnect(black_box(origin.clone())).await);
        assert_eq!(result.is_ok(), hit);
    }
}

fn main() {
    let mut iterations = 10_000;
    let mut samples = 9;
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
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let names: Vec<_> = (0..256)
            .map(|index| format!("origin{index}.bench.invalid"))
            .collect();
        let names: Vec<_> = names.iter().map(String::as_str).collect();
        let server = LocalServer::with_names(&names, |_, _| http::Response::default()).await;
        for (name, origins, coalescing, hit) in [
            ("empty_miss", 0, false, false),
            ("single_miss", 1, false, false),
            ("64_origins_miss", 64, false, false),
            ("256_origins_miss", 256, false, false),
            ("enabled_256_origins_miss", 256, true, false),
            ("pooled_hit", 256, false, true),
            ("enabled_pooled_hit", 256, true, true),
        ] {
            Case {
                name,
                origins,
                coalescing,
                hit,
            }
            .run(&server, iterations, samples)
            .await;
            tokio::task::yield_now().await;
        }
    });
}
