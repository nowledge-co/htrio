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
}

impl H3Client {
    pub fn new(connector: H3Connector, pool_timeout: Option<Duration>) -> Self {
        Self {
            pool: Pool::new(pool_timeout),
            connector,
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
        let lock = match self.pool.connecting(&key) {
            pool::Connecting::InProgress(waiter) => {
                return waiter
                    .receive()
                    .await
                    .ok_or_else(|| "failed to establish HTTP/3 connection".into());
            }
            pool::Connecting::Acquired(lock) => lock,
        };
        let destination = pool::domain_as_uri(key.clone());
        let (driver, tx, endpoint) = self.connector.connect(destination, key.2, proxy).await?;
        Ok(self.pool.new_connection(lock, driver, tx, endpoint))
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
