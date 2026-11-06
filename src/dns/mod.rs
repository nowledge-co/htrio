//! DNS resolution

pub use resolve::{Addrs, Name, Resolve, Resolving};
pub(crate) use resolve::{DnsResolverWithOverrides, DynResolver};

#[cfg(docsrs)]
pub use resolve::IntoResolve;

pub(crate) mod gai;
#[cfg(feature = "hickory-dns")]
pub(crate) mod hickory;
pub(crate) mod resolve;

/// Future returning HTTPS/SVCB endpoint metadata.
pub type ServiceResolving = std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<Vec<ServiceEndpoint>, crate::error::BoxError>>
            + Send,
    >,
>;

/// A resolved HTTPS ServiceMode endpoint. The origin remains the TLS identity.
#[derive(Clone, Debug)]
pub struct ServiceEndpoint {
    /// Service target hostname (without a trailing dot).
    pub target: String,
    /// Service port; zero is invalid.
    pub port: u16,
    /// ALPN identifiers, such as `h3` or `h2`.
    pub alpn: Vec<Vec<u8>>,
    /// DNS address hints; normal target lookups take precedence.
    pub addresses: Vec<std::net::SocketAddr>,
    /// Binary ECHConfigList, if advertised.
    pub ech_config_list: Option<Vec<u8>>,
    /// Service priority, where lower nonzero values are preferred.
    pub priority: u16,
    /// Remaining record validity.
    pub ttl: std::time::Duration,
}

impl ServiceEndpoint {
    /// Construct an HTTP/3 endpoint with a 60-second validity budget.
    pub fn http3(target: impl Into<String>, port: u16) -> Self {
        Self {
            target: target.into(),
            port,
            alpn: vec![b"h3".to_vec()],
            addresses: Vec::new(),
            ech_config_list: None,
            priority: 1,
            ttl: std::time::Duration::from_secs(60),
        }
    }
}
