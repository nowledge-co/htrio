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
#[cfg(feature = "socks")]
mod socks;

use crate::error::BoxError;
use connect::H3Connector;
use http::uri::Authority;
use http::Uri;
use log::trace;
use pool::Pool;
use std::time::Duration;

pub(crate) use pool::PoolClient;

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
    failed: std::collections::HashMap<pool::Key, (u32, std::time::Instant)>,
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
        alternative: Option<Authority>,
        proxy: Option<&crate::proxy::Intercepted>,
    ) -> Result<PoolClient, BoxError> {
        let mut key = pool::extract_domain(origin)?;
        if let Some(alternative) = alternative {
            key.2 = alternative;
        }
        key.3 = proxy.map(pool::ProxyKey::new);
        if let Some(client) = self.pool.try_pool(&key) {
            trace!("getting HTTP/3 client from pool with key {key:?}");
            return Ok(client);
        }
        let may_coalesce = {
            let health = self.health.lock().unwrap();
            health.coalescing_rejected.len() < 256 && !health.coalescing_rejected.contains(&key)
        };
        if may_coalesce {
            for (previous, client) in self.pool.coalescing_candidates(&key) {
                if self.connector.can_coalesce(
                    origin,
                    &pool::domain_as_uri(previous.clone()),
                    &key.2,
                    &client.info.connection,
                ) {
                    if let Some(client) = self.pool.alias(key.clone(), &previous) {
                        return Ok(client);
                    }
                }
            }
        }
        let lock = match self.pool.connecting(&key) {
            pool::Connecting::InProgress(waiter) => {
                return waiter
                    .receive()
                    .await
                    .ok_or_else(|| "failed to establish HTTP/3 connection".into());
            }
            pool::Connecting::Acquired(lock) => lock,
        };
        let started = std::time::Instant::now();
        let direct = proxy.is_none();
        let destination = pool::domain_as_uri(key.clone());
        let (driver, tx, endpoint, connection) =
            self.connector.connect(destination, key.2, proxy).await?;
        Ok(self.pool.new_connection(
            lock,
            driver,
            tx,
            endpoint,
            connection,
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
        let mut health = self.health.lock().unwrap();
        if health.generation != generation {
            health.generation = generation;
            health.failed.clear();
            health.coalescing_rejected.clear();
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
        self.pool.stats()
    }

    pub(crate) fn rebind(&self, socket: std::net::UdpSocket) -> Result<(), BoxError> {
        self.connector.rebind(socket)
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
