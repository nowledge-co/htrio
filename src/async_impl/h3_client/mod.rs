#![cfg(feature = "http3")]
//! HTTP/3 connection preparation and pooled request streams.
//!
//! The connector chooses direct UDP or SOCKS5 UDP without changing the
//! origin used for TLS and HTTP authority. Pools distinguish origins, network
//! destinations, and proxy credentials. Drivers retain their Quinn endpoints
//! and proxy tunnels for the lifetime of the connection.

pub(crate) mod connect;
pub(crate) mod dns;
mod pool;
pub(crate) mod session;
#[cfg(feature = "socks")]
mod socks;

use crate::error::BoxError;
use connect::H3Connector;
use http::uri::Authority;
use http::Uri;
use log::trace;
use pool::Pool;
use std::time::Duration;

pub(crate) use pool::{can_send_early, PoolClient};

/// Destination and handshake policy retained for one request hop and its retries.
#[derive(Clone)]
pub(crate) struct Route {
    pub(crate) authority: Authority,
    pub(crate) requires_ech: bool,
}
impl From<&crate::alt_svc::Alternative> for Route {
    fn from(alternative: &crate::alt_svc::Alternative) -> Self {
        Self {
            authority: alternative.authority.clone(),
            requires_ech: alternative.requires_ech,
        }
    }
}

#[derive(Clone)]
pub(crate) struct H3Client {
    pool: Pool,
    connector: H3Connector,
    health: std::sync::Arc<std::sync::Mutex<RouteHealth>>,
}

#[derive(Default)]
struct RouteHealth {
    generation: u64,
    coalescing_rejected: std::collections::HashSet<pool::Key>,
    probes: std::collections::HashSet<pool::Key>,
    failed: std::collections::HashMap<pool::Key, (u32, std::time::Instant)>,
}

pub(crate) struct RouteProbe {
    health: std::sync::Arc<std::sync::Mutex<RouteHealth>>,
    key: pool::Key,
    generation: u64,
}
impl Drop for RouteProbe {
    fn drop(&mut self) {
        let mut health = self.health.lock().unwrap();
        if health.generation == self.generation {
            health.probes.remove(&self.key);
        }
    }
}

impl H3Client {
    pub fn new(connector: H3Connector, pool_timeout: Option<Duration>) -> Self {
        Self {
            pool: Pool::new(pool_timeout),
            connector,
            health: Default::default(),
        }
    }

    // Establishing a connection cannot consume the request body. The transport
    // may fall back to TCP after this step fails, including for streaming POSTs.
    pub(crate) async fn prepare(
        &mut self,
        origin: &Uri,
        alternative: Option<Route>,
        proxy: Option<&crate::proxy::Intercepted>,
        allow_early: bool,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<PoolClient, BoxError> {
        let alternative = if alternative.is_none() {
            self.discover(origin, proxy)
                .await
                .into_iter()
                .next()
                .as_ref()
                .map(Route::from)
        } else {
            alternative
        };
        let requires_ech = alternative
            .as_ref()
            .is_some_and(|alternative| alternative.requires_ech);
        let mut key = pool::extract_domain(origin)?;
        if let Some(alternative) = alternative {
            key.2 = alternative.authority;
        }
        key.3 = proxy.map(pool::ProxyKey::new);
        if let Some(mut client) = self.pool.try_pool(&key) {
            if (!requires_ech || client.info.uses_ech())
                && (allow_early || client.info.early.accepted().await)
            {
                trace!("getting HTTP/3 client from pool with key {key:?}");
                return Ok(client);
            }
            self.pool.remove(&key);
        }
        let may_coalesce = !requires_ech && self.connector.coalescing_enabled() && {
            let health = self.health.lock().unwrap();
            health.coalescing_rejected.len() < 256 && !health.coalescing_rejected.contains(&key)
        };
        if may_coalesce {
            for (previous, client) in self.pool.coalescing_candidates(&key) {
                if self.connector.can_coalesce(origin, &key.2, &client.info) {
                    if let Some(client) = self.pool.alias(key.clone(), &previous) {
                        return Ok(client);
                    }
                }
            }
        }
        let lock = match self.pool.connecting(&key) {
            pool::Connecting::InProgress(waiter) => {
                let client = waiter
                    .receive()
                    .await
                    .ok_or("failed to establish HTTP/3 connection")?;
                if requires_ech && !client.info.uses_ech() {
                    return Err("selected ECH route requires a new encrypted handshake".into());
                }
                return Ok(client);
            }
            pool::Connecting::Acquired(lock) => lock,
        };
        let started = std::time::Instant::now();
        let generation = self.health.lock().unwrap().generation;
        let direct = proxy.is_none();
        let destination = pool::domain_as_uri(key.clone());
        let established = self
            .connector
            .connect(
                destination,
                Route {
                    authority: key.2.clone(),
                    requires_ech,
                },
                proxy,
                allow_early,
                deadline,
            )
            .await?;
        {
            let mut health = self.health.lock().unwrap();
            if health.generation == generation {
                health.failed.remove(&key);
            }
        }
        Ok(self.pool.new_connection(
            lock,
            established,
            direct,
            started.elapsed(),
            self.connector.tls_info(),
        ))
    }

    pub(crate) async fn discover(
        &self,
        origin: &Uri,
        proxy: Option<&crate::proxy::Intercepted>,
    ) -> Vec<crate::alt_svc::Alternative> {
        if proxy.is_some_and(|proxy| proxy.uri().scheme_str() == Some("socks5h")) {
            return Vec::new();
        }
        let Ok(Ok(endpoints)) =
            tokio::time::timeout(Duration::from_millis(200), self.connector.discover(origin)).await
        else {
            return Vec::new();
        };
        endpoints
            .into_iter()
            .filter_map(|endpoint| {
                format!("{}:{}", endpoint.target, endpoint.port)
                    .parse()
                    .ok()
                    .map(|authority| {
                        crate::alt_svc::Alternative::service(
                            authority,
                            endpoint.ttl,
                            endpoint.ech_config_list.is_some(),
                        )
                    })
            })
            .collect()
    }

    pub(crate) fn network_generation(&mut self, generation: u64) -> bool {
        let is_newer = |current: u64| {
            let distance = generation.wrapping_sub(current);
            distance != 0 && distance < (1_u64 << 63)
        };
        if !is_newer(self.health.lock().unwrap().generation) {
            return false;
        }
        // Resolver hooks are application code and may inspect client statistics.
        // Never invoke them while holding a pool or route-health lock.
        self.connector.clear_dns_cache();
        let mut health = self.health.lock().unwrap();
        // A concurrent notification may have already installed a newer network.
        if is_newer(health.generation) {
            health.generation = generation;
            health.failed.clear();
            health.coalescing_rejected.clear();
            health.probes.clear();
            self.pool.clear();
            self.connector.network_changed();
            return true;
        }
        false
    }

    pub(crate) fn is_broken(
        &self,
        origin: &Uri,
        alternative: &Authority,
        proxy: Option<&crate::proxy::Intercepted>,
    ) -> bool {
        let Ok(mut key) = pool::extract_domain(origin) else {
            return false;
        };
        key.2 = alternative.clone();
        key.3 = proxy.map(pool::ProxyKey::new);
        self.health
            .lock()
            .unwrap()
            .failed
            .get(&key)
            .is_some_and(|(_, until)| *until > std::time::Instant::now())
    }

    // Call only after the same origin is reachable through the selected TCP
    // route. An offline network must not poison otherwise healthy QUIC routes.
    pub(crate) fn confirm_broken(
        &self,
        origin: &Uri,
        alternative: Authority,
        proxy: Option<&crate::proxy::Intercepted>,
    ) {
        let Ok(mut key) = pool::extract_domain(origin) else {
            return;
        };
        key.2 = alternative;
        key.3 = proxy.map(pool::ProxyKey::new);
        let mut health = self.health.lock().unwrap();
        if health.failed.len() >= 256 {
            health
                .failed
                .retain(|_, (_, until)| *until > std::time::Instant::now());
        }
        if health.failed.len() >= 256 {
            return;
        }
        let failures = health
            .failed
            .get(&key)
            .map_or(1, |(count, _)| count.saturating_add(1));
        let delay = Duration::from_secs((30_u64 << failures.min(7)).min(3600));
        health
            .failed
            .insert(key, (failures, std::time::Instant::now() + delay));
    }

    pub(crate) fn pool_stats(&self) -> crate::http3::PoolStats {
        let mut stats = self.pool.stats();
        let health = self.health.lock().unwrap();
        stats.probing = health.probes.len();
        stats.broken_routes = health
            .failed
            .values()
            .filter(|(_, until)| *until > std::time::Instant::now())
            .count();
        stats
    }

    pub(crate) fn rebind(&self, socket: std::net::UdpSocket) -> Result<(), BoxError> {
        self.connector.rebind(socket)
    }

    pub(crate) fn probe(
        &self,
        origin: &Uri,
        alternative: Authority,
        proxy: Option<&crate::proxy::Intercepted>,
        generation: u64,
    ) -> Option<RouteProbe> {
        let mut key = pool::extract_domain(origin).ok()?;
        key.2 = alternative;
        key.3 = proxy.map(pool::ProxyKey::new);
        let mut health = self.health.lock().unwrap();
        if health.generation != generation
            || health.probes.len() >= 64
            || !health.probes.insert(key.clone())
        {
            return None;
        }
        Some(RouteProbe {
            health: self.health.clone(),
            key,
            generation,
        })
    }

    pub(crate) fn retire_connection(&self, connection: &PoolClient) {
        self.pool.remove(&connection.key);
    }

    pub(crate) fn reject_connection(&self, connection: &PoolClient) {
        let mut health = self.health.lock().unwrap();
        if health.coalescing_rejected.len() < 256 {
            health.coalescing_rejected.insert(connection.key.clone());
        }
        self.pool.remove(&connection.key);
    }

    pub(crate) fn reject(
        &self,
        origin: &Uri,
        alternative: Authority,
        proxy: Option<&crate::proxy::Intercepted>,
    ) {
        if let Ok(mut key) = pool::extract_domain(origin) {
            key.2 = alternative.clone();
            key.3 = proxy.map(pool::ProxyKey::new);
            let mut health = self.health.lock().unwrap();
            if health.coalescing_rejected.len() < 256 {
                health.coalescing_rejected.insert(key);
            }
        }
        self.remove(origin, alternative, proxy);
    }

    pub(crate) fn remove(
        &self,
        origin: &Uri,
        alternative: Authority,
        proxy: Option<&crate::proxy::Intercepted>,
    ) {
        if let Ok(mut key) = pool::extract_domain(origin) {
            key.2 = alternative;
            key.3 = proxy.map(pool::ProxyKey::new);
            self.pool.remove(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn stale_network_notifications_cannot_restore_an_old_generation() {
        struct Resolver(Arc<AtomicUsize>);
        impl crate::dns::Resolve for Resolver {
            fn resolve(&self, _: crate::dns::Name) -> crate::dns::Resolving {
                unreachable!("network invalidation does not resolve addresses")
            }
            fn clear_cache(&self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();
        let connector = H3Connector::new(
            crate::dns::DynResolver::new(Arc::new(Resolver(calls.clone()))),
            tls,
            None,
            Default::default(),
            Default::default(),
        )
        .unwrap();
        let mut client = H3Client::new(connector, None);
        assert!(client.network_generation(2));
        assert!(!client.network_generation(1));
        assert!(!client.network_generation(2));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        client.health.lock().unwrap().generation = u64::MAX;
        assert!(client.network_generation(0));
        assert!(!client.network_generation(u64::MAX));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
