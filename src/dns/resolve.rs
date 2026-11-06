use hyper_util::client::legacy::connect::dns::Name as HyperName;
use tower_service::Service;

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::error::BoxError;

/// Alias for an `Iterator` trait object over `SocketAddr`.
pub type Addrs = Box<dyn Iterator<Item = SocketAddr> + Send>;

/// Alias for the `Future` type returned by a DNS resolver.
pub type Resolving = Pin<Box<dyn Future<Output = Result<Addrs, BoxError>> + Send>>;

/// Trait for customizing DNS resolution in htrio.
///
/// Shared by the TCP and HTTP/3 connectors, including Alt-Svc destinations and
/// proxy hostnames. `socks5://` resolves targets locally; `socks5h://` delegates
/// target resolution to the proxy. Resolving an alternative host does
/// not change the origin's TLS name or HTTP authority.
pub trait Resolve: Send + Sync {
    /// Performs DNS resolution on a `Name`.
    /// The return type is a future containing an iterator of `SocketAddr`.
    ///
    /// It differs from `tower_service::Service<Name>` in several ways:
    ///  * It is assumed that `resolve` will always be ready to poll.
    ///  * It does not need a mutable reference to `self`.
    ///  * Since trait objects cannot make use of associated types, it requires
    ///    wrapping the returned `Future` and its contained `Iterator` with `Box`.
    ///
    /// Explicitly specified port in the URL will override any port in the resolved `SocketAddr`s.
    /// Otherwise, port `0` will be replaced by the conventional port for the given scheme (e.g. 80 for http).
    fn resolve(&self, name: Name) -> Resolving;

    /// Resolve HTTPS/SVCB service metadata for an HTTPS origin.
    ///
    /// The default preserves existing address-only resolvers. Return resolved
    /// ServiceMode records in priority order; address overrides still apply to
    /// target lookups. Remote-DNS SOCKS routes do not call this method.
    fn resolve_service(&self, _name: Name, _port: u16) -> super::ServiceResolving {
        Box::pin(std::future::ready(Ok(Vec::new())))
    }
}

/// A name that must be resolved to addresses.
#[derive(Debug)]
pub struct Name(pub(super) HyperName);

/// A more general trait implemented for types implementing `Resolve`.
///
/// Unnameable, only exported to aid seeing what implements this.
pub trait IntoResolve {
    #[doc(hidden)]
    fn into_resolve(self) -> Arc<dyn Resolve>;
}

impl Name {
    /// View the name as a string.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl FromStr for Name {
    type Err = sealed::InvalidNameError;

    fn from_str(host: &str) -> Result<Self, Self::Err> {
        HyperName::from_str(host)
            .map(Name)
            .map_err(|_| sealed::InvalidNameError { _ext: () })
    }
}

#[derive(Clone)]
pub(crate) struct DynResolver {
    resolver: Arc<dyn Resolve>,
}

impl DynResolver {
    pub(crate) fn new(resolver: Arc<dyn Resolve>) -> Self {
        Self { resolver }
    }

    #[cfg(feature = "socks")]
    pub(crate) fn gai() -> Self {
        Self::new(Arc::new(super::gai::GaiResolver::new()))
    }

    #[cfg(feature = "http3")]
    pub(crate) async fn resolve_service(
        &self,
        host: &str,
        port: u16,
    ) -> Result<Vec<super::ServiceEndpoint>, BoxError> {
        self.resolver.resolve_service(host.parse()?, port).await
    }

    /// Resolve an HTTP host and port, not just a domain name.
    ///
    /// This does the same thing that hyper-util's HttpConnector does, before
    /// calling out to its underlying DNS resolver.
    #[cfg(feature = "socks")]
    pub(crate) async fn http_resolve(
        &self,
        target: &http::Uri,
    ) -> Result<impl Iterator<Item = std::net::SocketAddr>, BoxError> {
        let host = target.host().ok_or("missing host")?;
        let port = target
            .port_u16()
            .unwrap_or_else(|| match target.scheme_str() {
                Some("https") => 443,
                Some("socks4") | Some("socks4a") | Some("socks5") | Some("socks5h") => 1080,
                _ => 80,
            });

        let explicit_port = target.port().is_some();

        let addrs = self.resolver.resolve(host.parse()?).await?;

        Ok(addrs.map(move |mut addr| {
            if explicit_port || addr.port() == 0 {
                addr.set_port(port);
            }
            addr
        }))
    }
}

impl Service<HyperName> for DynResolver {
    type Response = Addrs;
    type Error = BoxError;
    type Future = Resolving;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, name: HyperName) -> Self::Future {
        self.resolver.resolve(Name(name))
    }
}

pub(crate) struct DnsResolverWithOverrides {
    dns_resolver: Arc<dyn Resolve>,
    overrides: Arc<HashMap<String, Vec<SocketAddr>>>,
}

impl DnsResolverWithOverrides {
    pub(crate) fn new(
        dns_resolver: Arc<dyn Resolve>,
        overrides: HashMap<String, Vec<SocketAddr>>,
    ) -> Self {
        DnsResolverWithOverrides {
            dns_resolver,
            overrides: Arc::new(overrides),
        }
    }
}

impl Resolve for DnsResolverWithOverrides {
    fn resolve(&self, name: Name) -> Resolving {
        match self.overrides.get(name.as_str()) {
            Some(dest) => {
                let addrs: Addrs = Box::new(dest.clone().into_iter());
                Box::pin(std::future::ready(Ok(addrs)))
            }
            None => self.dns_resolver.resolve(name),
        }
    }
    fn resolve_service(&self, name: Name, port: u16) -> super::ServiceResolving {
        let future = self.dns_resolver.resolve_service(name, port);
        let overrides = self.overrides.clone();
        Box::pin(async move {
            let mut endpoints = future.await?;
            for endpoint in &mut endpoints {
                if overrides.contains_key(endpoint.target.trim_end_matches('.')) {
                    endpoint.addresses.clear();
                }
            }
            Ok(endpoints)
        })
    }
}

impl IntoResolve for Arc<dyn Resolve> {
    fn into_resolve(self) -> Arc<dyn Resolve> {
        self
    }
}

impl<R> IntoResolve for Arc<R>
where
    R: Resolve + 'static,
{
    fn into_resolve(self) -> Arc<dyn Resolve> {
        self
    }
}

impl<R> IntoResolve for R
where
    R: Resolve + 'static,
{
    fn into_resolve(self) -> Arc<dyn Resolve> {
        Arc::new(self)
    }
}

mod sealed {
    use std::fmt;

    #[derive(Debug)]
    pub struct InvalidNameError {
        pub(super) _ext: (),
    }

    impl fmt::Display for InvalidNameError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("invalid DNS name")
        }
    }

    impl std::error::Error for InvalidNameError {}
}
