//! HTTP/3 transport metadata and native QUIC configuration.
//!
//! Statistics are cumulative per connection. Response extensions contain a
//! [`ConnectionInfo`] handle; snapshots never inspect request or response data.

use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;

pub use quinn::{ConnectionStats, TransportConfig};
#[cfg(feature = "http3-qlog")]
pub use quinn::{QlogConfig, QlogStream};

/// A live HTTP/3 connection handle available in response extensions.
#[derive(Clone)]
pub struct ConnectionInfo {
    pub(crate) connection: quinn::Connection,
    pub(crate) sender: h3::client::SendRequest<h3_quinn::OpenStreams, bytes::Bytes>,
    pub(crate) direct: bool,
    pub(crate) connect_time: Duration,
    pub(crate) tls_info: bool,
}

impl ConnectionInfo {
    /// A process-local identifier stable across connection migration.
    pub fn id(&self) -> usize {
        self.connection.stable_id()
    }
    /// Whether this connection accepts new requests (false after GOAWAY).
    pub fn is_available(&self) -> bool {
        self.sender.is_available() && self.connection.close_reason().is_none()
    }
    /// DNS, proxy negotiation, QUIC, and HTTP/3 setup duration.
    pub fn connect_time(&self) -> Duration {
        self.connect_time
    }
    /// Current round-trip-time estimate.
    pub fn rtt(&self) -> Duration {
        self.connection.rtt()
    }
    /// Packet, byte, congestion, loss, and path MTU counters.
    pub fn stats(&self) -> ConnectionStats {
        self.connection.stats()
    }
    /// Actual network peer for direct connections; proxy virtual addresses are omitted.
    pub fn remote_addr(&self) -> Option<SocketAddr> {
        self.direct.then(|| self.connection.remote_address())
    }
}

impl fmt::Debug for ConnectionInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionInfo")
            .field("id", &self.id())
            .field("available", &self.is_available())
            .field("connect_time", &self.connect_time)
            .field("rtt", &self.rtt())
            .finish_non_exhaustive()
    }
}

/// A snapshot of the client's bounded HTTP/3 route pool.
#[derive(Clone, Copy, Debug, Default)]
pub struct PoolStats {
    /// Routes currently establishing a connection.
    pub connecting: usize,
    /// Connections eligible for new requests.
    pub available: usize,
    /// Connections retained by the pool but no longer eligible for reuse.
    pub retiring: usize,
}

/// Request urgency and incremental response preference (RFC 9218).
pub use h3::priority::Priority;

/// Reprioritize a response through its HTTP/3 control stream.
#[derive(Clone, Debug)]
pub struct RequestPriority {
    pub(crate) handle: h3::priority::Handle,
    pub(crate) connection: quinn::Connection,
}
impl RequestPriority {
    /// Update HTTP priority and the upload stream's QUIC scheduler hint.
    /// Returns false if the connection is closing or its bounded queue is full.
    pub fn update(&self, priority: Priority) -> bool {
        if let Ok(stream) = quinn::VarInt::from_u64(self.handle.stream_id()) {
            let _ = self
                .connection
                .set_stream_priority(stream.into(), priority.quic_priority());
        }
        self.handle.update(priority)
    }
}
