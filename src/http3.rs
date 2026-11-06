//! HTTP/3 transport metadata and native QUIC configuration.
//!
//! Statistics are cumulative per connection. Response extensions contain a
//! [`ConnectionInfo`] handle; snapshots never inspect request or response data.

use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;

/// Header compression counters.
pub use h3::QpackStats;
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
    pub(crate) coalesced: bool,
    pub(crate) early: crate::async_impl::h3_client::session::EarlyData,
    pub(crate) early_data_used: bool,
    pub(crate) dispatched: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pub(crate) reused: bool,
    // Retain handshake privacy independently of the service record's lifetime.
    pub(crate) ech: bool,
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
    /// DNS, proxy negotiation, and setup time until the route became usable.
    /// With opted-in early data, transport keys can precede TLS confirmation.
    pub fn connect_time(&self) -> Duration {
        self.connect_time
    }
    /// Whether the connection had already dispatched another request.
    pub fn is_reused(&self) -> bool {
        self.reused
    }
    /// Whether this route reuses a connection authenticated for another origin.
    pub fn is_coalesced(&self) -> bool {
        self.coalesced
    }
    /// Whether this connection's handshake required encrypted ClientHello.
    /// Retained after DNS service records expire; such connections stay isolated.
    pub fn uses_ech(&self) -> bool {
        self.ech
    }
    /// Current round-trip-time estimate.
    pub fn rtt(&self) -> Duration {
        self.connection.rtt()
    }
    /// Packet, byte, congestion, loss, and path MTU counters.
    pub fn stats(&self) -> ConnectionStats {
        self.connection.stats()
    }
    /// Whether early data was attempted and accepted on this connection.
    pub fn early_data_status(&self) -> EarlyDataStatus {
        self.early.status()
    }
    /// Whether this request was dispatched before TLS confirmation.
    pub fn used_early_data(&self) -> bool {
        self.early_data_used
    }
    /// Content-free counters for the route's owned TLS session cache.
    pub fn session_stats(&self) -> SessionStats {
        self.early.stats()
    }
    /// Dynamic QPACK insertion and blocked-section counters.
    pub fn qpack_stats(&self) -> h3::QpackStats {
        self.sender.qpack_stats()
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
    /// Bounded request-free route probes after a TCP race succeeds.
    pub probing: usize,
    /// Routes currently in confirmed failure backoff.
    pub broken_routes: usize,
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

/// Certificate verification policy used when rebuilding preconfigured TLS for ECH.
pub use rustls::client::danger::ServerCertVerifier as CertificateVerifier;

/// TLS early-data acceptance state for a QUIC connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EarlyDataStatus {
    /// This connection completed a regular TLS handshake.
    NotAttempted,
    /// An early connection is awaiting server confirmation.
    Pending,
    /// The server accepted the early connection.
    Accepted,
    /// The server rejected early data; a fresh confirmed connection is required.
    Rejected,
}

/// Cumulative route-scoped resumption counters; no session secrets are exposed.
#[derive(Clone, Copy, Debug, Default)]
pub struct SessionStats {
    /// Tickets in htrio's owned cache (preconfigured external stores are not counted).
    pub cached_tickets: usize,
    /// Whether authenticated peer SETTINGS are available for resumption.
    pub settings_cached: bool,
    /// Connections that actually obtained 0-RTT transport keys.
    pub early_attempts: usize,
    /// Early connections rejected by the peer or terminated during establishment.
    pub early_rejections: usize,
}

/// Transport choice and timing for a single request hop, in response extensions.
#[derive(Clone, Copy, Debug)]
pub struct TransportInfo {
    /// Negotiated protocol for this response.
    pub protocol: http::Version,
    /// Time from transport selection until final response headers.
    pub time_to_headers: Duration,
    /// Why this transport handled the request.
    pub selection: TransportSelection,
    /// A safe retry performed within this hop, if any.
    pub recovery: Option<RecoveryReason>,
}

/// Content-free transport selection reasons.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportSelection {
    /// The ordinary TCP origin route, without an alternative attempt.
    Origin,
    /// An explicitly selected HTTP/3 route.
    ExplicitHttp3,
    /// An authenticated Alt-Svc header or ALTSVC frame.
    AltSvc,
    /// HTTPS/SVCB service metadata.
    HttpsRecord,
    /// TCP preparation won a race against an in-progress QUIC attempt.
    TcpRace,
    /// Alternative preparation failed before dispatch and TCP succeeded.
    ConnectionFallback,
    /// A cloneable request used the origin route after HTTP 421.
    MisdirectedFallback,
}

/// Proof used to authorize a bounded retry before exposing a response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryReason {
    /// A retiring connection did not dispatch any request bytes.
    ConnectionRetired,
    /// An eligible early request was rejected or received HTTP 425.
    EarlyDataRejected,
}
