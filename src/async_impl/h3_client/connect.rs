//! Establish QUIC and HTTP/3 without sending an origin request body.
//!
//! The destination authority controls routing; the origin controls SNI and
//! certificate verification. Connection budgets are applied by the transport
//! to include DNS, SOCKS negotiation, and the QUIC/TLS handshake.

use crate::async_impl::h3_client::dns::resolve_host;
use crate::dns::DynResolver;
use crate::error::BoxError;
use bytes::Bytes;
use h3::client::SendRequest;
use h3_quinn::{Connection, OpenStreams};
use http::Uri;
use quinn::crypto::rustls::QuicClientConfig;
use quinn::{ClientConfig, Endpoint, TransportConfig};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub(super) struct H3Connection {
    pub(super) driver: h3::client::Connection<Connection, Bytes>,
    pub(super) sender: SendRequest<OpenStreams, Bytes>,
    pub(super) endpoint: Endpoint,
    pub(super) connection: quinn::Connection,
    pub(super) early: super::session::EarlyData,
    pub(super) ech: bool,
}

const HAPPY_EYEBALLS_DELAY: Duration = Duration::from_millis(250);

#[derive(Default)]
struct DirectEndpoint {
    active: Option<(Endpoint, tokio::runtime::Handle)>,
    pending_socket: Option<std::net::UdpSocket>,
}

/// HTTP/3 settings applied to direct and tunneled connections.
#[derive(Clone)]
pub(crate) struct H3ClientConfig {
    /// Maximum uncompressed field section size advertised in HTTP/3 SETTINGS.
    ///
    /// See [header size constraints] section of the specification for details.
    ///
    /// [header size constraints]: https://www.rfc-editor.org/rfc/rfc9114.html#name-header-size-constraints
    ///
    pub(crate) max_field_section_size: Option<u64>,
    pub(crate) max_frame_size: Option<u64>,
    pub(crate) tls_info: bool,
    pub(crate) verifier: Option<Arc<dyn rustls::client::danger::ServerCertVerifier>>,

    /// Send reserved protocol values to exercise peers' extension handling.
    pub(crate) send_grease: Option<bool>,
    pub(crate) qpack: (u64, u64),
    pub(crate) coalescing: bool,
    pub(crate) partition_sessions: bool,
}

impl Default for H3ClientConfig {
    fn default() -> Self {
        Self {
            max_field_section_size: None,
            max_frame_size: None,
            tls_info: false,
            verifier: None,
            send_grease: None,
            qpack: (4096, 16),
            coalescing: false,
            partition_sessions: true,
        }
    }
}

#[derive(Clone)]
pub(crate) struct H3Connector {
    resolver: DynResolver,
    endpoint: Arc<Mutex<DirectEndpoint>>,
    quic_config: ClientConfig,
    tls: Arc<rustls::ClientConfig>,
    transport: Arc<TransportConfig>,
    client_config: H3ClientConfig,
    local_addr: Option<IpAddr>,
    network_epoch: Arc<std::sync::atomic::AtomicU64>,
    routes:
        Arc<Mutex<std::collections::HashMap<super::pool::Key, Arc<super::session::RouteState>>>>,
    services: Arc<
        Mutex<
            std::collections::HashMap<
                String,
                (std::time::Instant, Vec<crate::dns::ServiceEndpoint>),
            >,
        >,
    >,
}

impl H3Connector {
    pub fn new(
        resolver: DynResolver,
        tls: rustls::ClientConfig,
        local_addr: Option<IpAddr>,
        transport_config: TransportConfig,
        client_config: H3ClientConfig,
    ) -> Result<H3Connector, BoxError> {
        let tls = Arc::new(tls);
        let quic_client_config = Arc::new(QuicClientConfig::try_from(tls.clone())?);
        let transport = Arc::new(transport_config);
        let mut config = ClientConfig::new(quic_client_config);
        config.transport_config(transport.clone());

        Ok(Self {
            resolver,
            endpoint: Default::default(),
            quic_config: config,
            tls,
            transport,
            client_config,
            local_addr,
            services: Default::default(),
            routes: Default::default(),
            network_epoch: Default::default(),
        })
    }

    fn origin_key(origin: &Uri) -> String {
        format!(
            "{}:{}",
            origin.host().unwrap_or(""),
            origin.port_u16().unwrap_or(443)
        )
    }

    pub(crate) async fn discover(
        &self,
        origin: &Uri,
    ) -> Result<Vec<crate::dns::ServiceEndpoint>, BoxError> {
        let key = Self::origin_key(origin);
        if let Some((_, endpoints)) = self
            .services
            .lock()
            .unwrap()
            .get(&key)
            .filter(|(expiry, _)| *expiry > std::time::Instant::now())
        {
            return Ok(endpoints.clone());
        }
        let host = origin.host().ok_or("HTTPS origin has no host")?;
        if host.parse::<IpAddr>().is_ok() || host.starts_with('[') {
            return Ok(Vec::new());
        }
        let epoch = self
            .network_epoch
            .load(std::sync::atomic::Ordering::Relaxed);
        let mut endpoints = self
            .resolver
            .resolve_service(host, origin.port_u16().unwrap_or(443))
            .await?;
        if epoch
            != self
                .network_epoch
                .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Ok(Vec::new());
        }
        endpoints.truncate(16);
        for endpoint in &mut endpoints {
            endpoint.addresses.truncate(16);
            // Keep ECH present on unusable records so selection fails closed.
            if endpoint
                .ech_config_list
                .as_ref()
                .is_some_and(|ech| ech.len() > 16 * 1024)
            {
                endpoint.ech_config_list = Some(Vec::new());
            }
        }
        endpoints.retain(|endpoint| {
            endpoint.priority != 0
                && endpoint.port != 0
                && !endpoint.ttl.is_zero()
                && endpoint.alpn.iter().any(|alpn| alpn == b"h3")
                && !endpoint.target.contains('@')
                && format!("{}:{}", endpoint.target, endpoint.port)
                    .parse::<http::uri::Authority>()
                    .is_ok()
        });
        if endpoints
            .iter()
            .any(|endpoint| endpoint.ech_config_list.is_some())
        {
            endpoints.retain(|endpoint| endpoint.ech_config_list.is_some());
        }
        endpoints.sort_by_key(|endpoint| endpoint.priority);
        endpoints.truncate(8);
        let ttl = endpoints
            .iter()
            .map(|endpoint| endpoint.ttl)
            .min()
            .unwrap_or(Duration::from_secs(60))
            .min(Duration::from_secs(86_400));
        let mut cache = self.services.lock().unwrap();
        if epoch
            != self
                .network_epoch
                .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Ok(Vec::new());
        }
        cache.retain(|_, (expiry, _)| *expiry > std::time::Instant::now());
        if cache.len() >= 256 {
            if let Some(oldest) = cache.keys().next().cloned() {
                cache.remove(&oldest);
            }
        }
        cache.insert(key, (std::time::Instant::now() + ttl, endpoints.clone()));
        Ok(endpoints)
    }

    fn service(
        &self,
        origin: &Uri,
        alternative: &http::uri::Authority,
    ) -> Option<crate::dns::ServiceEndpoint> {
        self.services
            .lock()
            .unwrap()
            .get(&Self::origin_key(origin))
            .filter(|(expiry, _)| *expiry > std::time::Instant::now())?
            .1
            .iter()
            .find(|endpoint| {
                endpoint.target.eq_ignore_ascii_case(alternative.host())
                    && endpoint.port == alternative.port_u16().unwrap_or(443)
            })
            .cloned()
    }

    pub(super) fn coalescing_enabled(&self) -> bool {
        self.client_config.coalescing
    }

    pub(crate) fn can_coalesce(
        &self,
        origin: &Uri,
        destination: &http::uri::Authority,
        info: &crate::http3::ConnectionInfo,
    ) -> bool {
        let connection = &info.connection;
        if connection.is_handshaking()
            || info.uses_ech()
            || !self.coalescing_enabled()
            || self.tls.client_auth_cert_resolver.has_certs()
            || self
                .service(origin, destination)
                .is_some_and(|service| service.ech_config_list.is_some())
        {
            return false;
        }
        let Some(verifier) = self.client_config.verifier.as_ref() else {
            return false;
        };
        let Some(host) = origin.host() else {
            return false;
        };
        let Ok(name) =
            rustls::pki_types::ServerName::try_from(host.trim_matches(['[', ']']).to_owned())
        else {
            return false;
        };
        let Some(identity) = connection.peer_identity() else {
            return false;
        };
        let Ok(chain) = identity.downcast::<Vec<rustls::pki_types::CertificateDer<'static>>>()
        else {
            return false;
        };
        let Some(leaf) = chain.first() else {
            return false;
        };
        verifier
            .verify_server_cert(
                leaf,
                &chain[1..],
                &name,
                &[],
                rustls::pki_types::UnixTime::now(),
            )
            .is_ok()
    }

    fn route(
        &self,
        origin: &Uri,
        alternative: &http::uri::Authority,
        proxy: Option<&crate::proxy::Intercepted>,
    ) -> Result<Arc<super::session::RouteState>, BoxError> {
        let mut key = super::pool::extract_domain(origin)?;
        key.2 = alternative.clone();
        key.3 = proxy.map(super::pool::ProxyKey::new);
        let mut routes = self.routes.lock().unwrap();
        if let Some(route) = routes.get(&key) {
            return Ok(route.clone());
        }
        if routes.len() >= 256 {
            if let Some(oldest) = routes.keys().next().cloned() {
                routes.remove(&oldest);
            }
        }
        let route = Arc::new(super::session::RouteState::new(
            &self.tls,
            self.client_config.partition_sessions,
        ));
        routes.insert(key, route.clone());
        Ok(route)
    }

    fn route_tls(
        &self,
        service: Option<&crate::dns::ServiceEndpoint>,
        route: &super::session::RouteState,
    ) -> Result<Arc<rustls::ClientConfig>, BoxError> {
        let Some(ech) = service.and_then(|service| service.ech_config_list.as_ref()) else {
            return Ok(route.tls.clone());
        };
        let verifier = self
            .client_config
            .verifier
            .clone()
            .ok_or("DNS ECH requires the certificate verifier used by the TLS configuration")?;
        let ech = rustls::client::EchConfig::new(
            rustls::pki_types::EchConfigListBytes::from(ech.as_slice()),
            rustls::crypto::aws_lc_rs::hpke::ALL_SUPPORTED_SUITES,
        )?;
        let mut tls =
            rustls::ClientConfig::builder_with_provider(self.tls.crypto_provider().clone())
                .with_ech(ech.into())?
                .dangerous()
                .with_custom_certificate_verifier(verifier)
                .with_client_cert_resolver(self.tls.client_auth_cert_resolver.clone());
        tls.alpn_protocols = vec![b"h3".to_vec()];
        tls.resumption = route.tls.resumption.clone();
        tls.key_log = self.tls.key_log.clone();
        tls.enable_early_data = self.tls.enable_early_data;
        tls.max_fragment_size = self.tls.max_fragment_size;
        Ok(Arc::new(tls))
    }

    fn route_config(
        &self,
        service: Option<&crate::dns::ServiceEndpoint>,
        route: &super::session::RouteState,
    ) -> Result<ClientConfig, BoxError> {
        let key = service.and_then(|service| service.ech_config_list.clone());
        let mut cached = route.quic.lock().unwrap();
        if let Some((_, config)) = cached.as_ref().filter(|(previous, _)| previous == &key) {
            return Ok(config.clone());
        }
        let mut config = ClientConfig::new(Arc::new(QuicClientConfig::try_from(
            self.route_tls(service, route)?,
        )?));
        config.transport_config(self.transport.clone());
        *cached = Some((key, config.clone()));
        Ok(config)
    }

    pub(super) async fn connect(
        &mut self,
        origin: Uri,
        route: super::Route,
        proxy: Option<&crate::proxy::Intercepted>,
        allow_early: bool,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<H3Connection, BoxError> {
        let super::Route {
            authority: alternative,
            requires_ech,
        } = route;
        let server_name = origin
            .host()
            .ok_or("origin must have a host")?
            .trim_start_matches('[')
            .trim_end_matches(']');
        let host = alternative
            .host()
            .trim_start_matches('[')
            .trim_end_matches(']');
        let service = self.service(&origin, &alternative);
        let ech = service
            .as_ref()
            .is_some_and(|service| service.ech_config_list.is_some());
        if requires_ech && !ech {
            return Err("selected ECH route no longer has usable service metadata".into());
        }
        let route = self.route(&origin, &alternative, proxy)?;
        let quic_config = self.route_config(service.as_ref(), &route)?;
        if let Some(proxy) = proxy {
            #[cfg(feature = "socks")]
            return self
                .connect_socks(
                    proxy,
                    host,
                    alternative.port_u16(),
                    server_name,
                    quic_config,
                    route,
                    allow_early,
                    deadline,
                )
                .await
                .map(|mut established| {
                    established.ech = ech;
                    established
                });
            #[cfg(not(feature = "socks"))]
            {
                let _ = proxy;
                return Err("HTTP/3 SOCKS5 proxying requires the socks Cargo feature".into());
            }
        }
        let addrs = match resolve_host(&mut self.resolver, host, alternative.port_u16(), 443).await
        {
            Ok(addresses) if !addresses.is_empty() => addresses,
            result => match service {
                Some(service) if !service.addresses.is_empty() => service
                    .addresses
                    .into_iter()
                    .take(16)
                    .map(|mut address| {
                        address.set_port(service.port);
                        address
                    })
                    .collect(),
                _ => result?,
            },
        };

        // The destination comes from Alt-Svc; TLS still authenticates the origin.
        self.remote_connect(
            addrs,
            server_name,
            quic_config,
            route,
            allow_early,
            deadline,
        )
        .await
        .map(|mut established| {
            established.ech = ech;
            established
        })
    }

    pub(crate) fn tls_info(&self) -> bool {
        self.client_config.tls_info
    }

    pub(crate) fn network_changed(&mut self) {
        // Existing streams retain their endpoint and tunnel. Future connections
        // bind on the new network without migrating a SOCKS association.
        self.network_epoch
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        *self.endpoint.lock().unwrap() = DirectEndpoint::default();
        self.services.lock().unwrap().clear();
        for route in self.routes.lock().unwrap().values() {
            route.quic.lock().unwrap().take();
        }
    }

    pub(super) fn clear_dns_cache(&self) {
        self.resolver.clear_cache();
    }

    fn endpoint(&self) -> Result<Endpoint, BoxError> {
        let mut shared = self.endpoint.lock().unwrap();
        if let Some((endpoint, _)) = shared.active.as_ref() {
            return Ok(endpoint.clone());
        }
        // Bind lazily inside the request's runtime, so Client::new() also works
        // before entering Tokio or on the blocking client's worker thread.
        let socket_addr = match self.local_addr {
            Some(ip) => SocketAddr::new(ip, 0),
            None => "[::]:0".parse::<SocketAddr>().unwrap(),
        };
        let runtime = tokio::runtime::Handle::try_current()?;
        let mut endpoint = match shared.pending_socket.take() {
            Some(socket) => Endpoint::new(
                quinn::EndpointConfig::default(),
                None,
                socket,
                Arc::new(quinn::TokioRuntime),
            )?,
            None => Endpoint::client(socket_addr)?,
        };
        endpoint.set_default_client_config(self.quic_config.clone());
        shared.active = Some((endpoint.clone(), runtime));
        Ok(endpoint)
    }

    pub(crate) fn rebind(&self, socket: std::net::UdpSocket) -> Result<(), BoxError> {
        socket.set_nonblocking(true)?;
        let mut shared = self.endpoint.lock().unwrap();
        if let Some((endpoint, runtime)) = shared.active.as_ref() {
            // Register with the endpoint's runtime even when the caller is on
            // an application thread, or currently inside a different runtime.
            let _enter = runtime.enter();
            endpoint.rebind(socket)?;
        } else {
            shared.pending_socket = Some(socket);
        }
        Ok(())
    }

    #[cfg(feature = "socks")]
    async fn connect_socks(
        &mut self,
        proxy: &crate::proxy::Intercepted,
        host: &str,
        port: Option<u16>,
        server_name: &str,
        quic_config: ClientConfig,
        route: Arc<super::session::RouteState>,
        allow_early: bool,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<H3Connection, BoxError> {
        use super::socks::Target;
        let targets =
            if proxy.uri().scheme_str() == Some("socks5h") && host.parse::<IpAddr>().is_err() {
                vec![Target::Domain(host.to_owned(), port.unwrap_or(443))]
            } else {
                resolve_host(&mut self.resolver, host, port, 443)
                    .await?
                    .into_iter()
                    .map(Target::Ip)
                    .collect()
            };
        let mut attempts = tokio::task::JoinSet::new();
        for (index, target) in targets.into_iter().take(16).enumerate() {
            let connector = self.clone();
            let proxy = proxy.clone();
            let quic_config = quic_config.clone();
            let route = route.clone();
            let server_name = server_name.to_owned();
            attempts.spawn(async move {
                tokio::time::sleep(HAPPY_EYEBALLS_DELAY * index as u32).await;
                connector
                    .connect_socks_target(
                        &proxy,
                        target,
                        &server_name,
                        quic_config,
                        route,
                        allow_early,
                        deadline,
                    )
                    .await
            });
        }
        let mut last: BoxError = "HTTP/3 target resolved no addresses".into();
        while let Some(result) = attempts.join_next().await {
            match result {
                Ok(Ok(connection)) => return Ok(connection),
                Ok(Err(error)) => last = error,
                Err(error) => last = error.into(),
            }
        }
        Err(last)
    }

    #[cfg(feature = "socks")]
    async fn connect_socks_target(
        &self,
        proxy: &crate::proxy::Intercepted,
        target: super::socks::Target,
        server_name: &str,
        quic_config: ClientConfig,
        route: Arc<super::session::RouteState>,
        allow_early: bool,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<H3Connection, BoxError> {
        let (socket, peer) = super::socks::SocksSocket::connect(
            proxy,
            target,
            &mut self.resolver.clone(),
            self.local_addr,
        )
        .await?;
        let mut endpoint = Endpoint::new_with_abstract_socket(
            quinn::EndpointConfig::default(),
            None,
            socket.clone(),
            Arc::new(quinn::TokioRuntime),
        )?;
        endpoint.set_default_client_config(quic_config);
        let (connection, early) = tokio::select! {
            result = super::session::EarlyData::connect(endpoint.connect(peer, server_name)?, route, allow_early, deadline) => result?,
            _ = socket.closed() => return Err("SOCKS5 UDP association closed during handshake".into()),
        };
        let monitored = connection.clone();
        let result = Self::build_h3(&endpoint, connection, &self.client_config, early).await;
        if result.is_err() {
            monitored.close(0_u32.into(), b"HTTP/3 setup failed");
            return result;
        }
        tokio::spawn(async move {
            tokio::select! {
                _ = monitored.closed() => {},
                _ = socket.closed() => monitored.close(0_u32.into(), b"SOCKS5 association closed"),
            }
        });
        result
    }

    async fn remote_connect(
        &mut self,
        addrs: Vec<SocketAddr>,
        server_name: &str,
        quic_config: ClientConfig,
        route: Arc<super::session::RouteState>,
        allow_early: bool,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<H3Connection, BoxError> {
        let endpoint = self.endpoint()?;
        let mut ipv6 = addrs.iter().copied().filter(|addr| addr.is_ipv6());
        let mut ipv4 = addrs.iter().copied().filter(|addr| addr.is_ipv4());
        let prefer_v6 = addrs.first().is_some_and(SocketAddr::is_ipv6);
        let mut ordered = Vec::new();
        loop {
            let (first, second) = if prefer_v6 {
                (ipv6.next(), ipv4.next())
            } else {
                (ipv4.next(), ipv6.next())
            };
            if first.is_none() && second.is_none() {
                break;
            }
            ordered.extend(first);
            ordered.extend(second);
        }
        let mut attempts = tokio::task::JoinSet::new();
        for (index, address) in ordered
            .into_iter()
            .filter(|addr| {
                self.local_addr
                    .is_none_or(|ip| ip.is_ipv4() == addr.is_ipv4())
            })
            .take(16)
            .enumerate()
        {
            let endpoint = endpoint.clone();
            let config = self.client_config.clone();
            let quic_config = quic_config.clone();
            let route = route.clone();
            let server_name = server_name.to_owned();
            attempts.spawn(async move {
                tokio::time::sleep(HAPPY_EYEBALLS_DELAY * index as u32).await;
                let connecting = endpoint.connect_with(quic_config, address, &server_name)?;
                let (connection, early) =
                    super::session::EarlyData::connect(connecting, route, allow_early, deadline)
                        .await?;
                Self::build_h3(&endpoint, connection, &config, early).await
            });
        }
        let mut last: BoxError = "no compatible addresses to connect to".into();
        while let Some(result) = attempts.join_next().await {
            match result {
                Ok(Ok(connection)) => return Ok(connection),
                Ok(Err(error)) => last = error,
                Err(error) => last = error.into(),
            }
        }
        Err(last)
    }

    async fn build_h3(
        endpoint: &Endpoint,
        connection: quinn::Connection,
        config: &H3ClientConfig,
        early: super::session::EarlyData,
    ) -> Result<H3Connection, BoxError> {
        let mut builder = h3::client::builder();
        builder.enable_extended_connect(true);
        if early.attempted {
            let remembered = *early.route.settings.lock().unwrap();
            if let Some(settings) = remembered {
                builder.remembered_settings(settings);
            }
        }
        builder.qpack(config.qpack.0, config.qpack.1);
        if let Some(size) = config.max_field_section_size {
            builder.max_field_section_size(size);
        }
        if let Some(size) = config.max_frame_size {
            builder.max_frame_size(size);
        }
        if let Some(grease) = config.send_grease {
            builder.send_grease(grease);
        }
        let (driver, sender) = builder.build(Connection::new(connection.clone())).await?;
        Ok(H3Connection {
            driver,
            sender,
            endpoint: endpoint.clone(),
            connection,
            early,
            ech: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn discovered_ech_encrypts_the_origin_name_in_the_quic_client_hello() {
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let identity =
            rcgen::generate_simple_self_signed(vec!["private-origin.test".into()]).unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(identity.cert.der().clone()).unwrap();
        let verifier = rustls::client::WebPkiServerVerifier::builder_with_provider(
            Arc::new(roots),
            provider.clone(),
        )
        .build()
        .unwrap();
        let tls = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .dangerous()
            .with_custom_certificate_verifier(verifier.clone())
            .with_no_client_auth();
        let config = H3ClientConfig {
            verifier: Some(verifier),
            ..Default::default()
        };
        let connector = H3Connector::new(
            DynResolver::new(Arc::new(crate::dns::gai::GaiResolver::new())),
            tls,
            None,
            TransportConfig::default(),
            config,
        )
        .unwrap();
        let route = super::super::session::RouteState::new(&connector.tls, true);
        let mut contents = vec![0, 0, 0x20, 0, 32];
        contents.extend_from_slice(&[9]);
        contents.extend_from_slice(&[0; 31]);
        contents.extend_from_slice(&[0, 4, 0, 1, 0, 1, 0, 11]);
        contents.extend_from_slice(b"public.test");
        contents.extend_from_slice(&[0, 0]);
        let mut config = vec![0xfe, 0x0d];
        config.extend_from_slice(&(contents.len() as u16).to_be_bytes());
        config.extend_from_slice(&contents);
        let mut list = (config.len() as u16).to_be_bytes().to_vec();
        list.extend_from_slice(&config);
        let mut endpoint = crate::dns::ServiceEndpoint::http3("service.test", 443);
        endpoint.ech_config_list = Some(list);
        let tls = connector.route_tls(Some(&endpoint), &route).unwrap();
        let name = rustls::pki_types::ServerName::try_from("private-origin.test").unwrap();
        let mut session =
            rustls::quic::ClientConnection::new(tls, rustls::quic::Version::V1, name, Vec::new())
                .unwrap();
        let mut hello = Vec::new();
        session.write_hs(&mut hello);
        assert!(hello
            .windows(b"public.test".len())
            .any(|bytes| bytes == b"public.test"));
        assert!(!hello
            .windows(b"private-origin.test".len())
            .any(|bytes| bytes == b"private-origin.test"));
    }
}
