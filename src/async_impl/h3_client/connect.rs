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

type H3Connection = (
    h3::client::Connection<Connection, Bytes>,
    SendRequest<OpenStreams, Bytes>,
    Endpoint,
    quinn::Connection,
);

const HAPPY_EYEBALLS_DELAY: Duration = Duration::from_millis(250);

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

    /// Send reserved protocol values to exercise peers' extension handling.
    pub(crate) send_grease: Option<bool>,
}

impl Default for H3ClientConfig {
    fn default() -> Self {
        Self {
            max_field_section_size: None,
            max_frame_size: None,
            tls_info: false,
            send_grease: None,
        }
    }
}

#[derive(Clone)]
pub(crate) struct H3Connector {
    resolver: DynResolver,
    endpoint: Arc<Mutex<Option<Endpoint>>>,
    quic_config: ClientConfig,
    client_config: H3ClientConfig,
    local_addr: Option<IpAddr>,
}

impl H3Connector {
    pub fn new(
        resolver: DynResolver,
        tls: rustls::ClientConfig,
        local_addr: Option<IpAddr>,
        transport_config: TransportConfig,
        client_config: H3ClientConfig,
    ) -> Result<H3Connector, BoxError> {
        let quic_client_config = Arc::new(QuicClientConfig::try_from(tls)?);
        let mut config = ClientConfig::new(quic_client_config);
        config.transport_config(Arc::new(transport_config));

        Ok(Self {
            resolver,
            endpoint: Arc::new(Mutex::new(None)),
            quic_config: config,
            client_config,
            local_addr,
        })
    }

    pub async fn connect(
        &mut self,
        origin: Uri,
        alternative: http::uri::Authority,
        proxy: Option<&crate::proxy::Intercepted>,
    ) -> Result<H3Connection, BoxError> {
        let server_name = origin
            .host()
            .ok_or("origin must have a host")?
            .trim_start_matches('[')
            .trim_end_matches(']');
        let host = alternative
            .host()
            .trim_start_matches('[')
            .trim_end_matches(']');
        if let Some(proxy) = proxy {
            #[cfg(feature = "socks")]
            return self
                .connect_socks(proxy, host, alternative.port_u16(), server_name)
                .await;
            #[cfg(not(feature = "socks"))]
            {
                let _ = proxy;
                return Err("HTTP/3 SOCKS5 proxying requires the socks Cargo feature".into());
            }
        }
        let addrs = resolve_host(&mut self.resolver, host, alternative.port_u16(), 443).await?;

        // The destination comes from Alt-Svc; TLS still authenticates the origin.
        self.remote_connect(addrs, server_name).await
    }

    pub(crate) fn tls_info(&self) -> bool {
        self.client_config.tls_info
    }

    pub(crate) fn network_changed(&mut self) {
        // Existing streams retain their endpoint and tunnel. Future connections
        // bind on the new network without migrating a SOCKS association.
        self.endpoint.lock().unwrap().take();
    }

    fn endpoint(&self) -> Result<Endpoint, BoxError> {
        let mut shared = self.endpoint.lock().unwrap();
        if let Some(endpoint) = shared.as_ref() {
            return Ok(endpoint.clone());
        }
        // Bind lazily inside the request's runtime, so Client::new() also works
        // before entering Tokio or on the blocking client's worker thread.
        let socket_addr = match self.local_addr {
            Some(ip) => SocketAddr::new(ip, 0),
            None => "[::]:0".parse::<SocketAddr>().unwrap(),
        };
        let mut endpoint = Endpoint::client(socket_addr)?;
        endpoint.set_default_client_config(self.quic_config.clone());
        *shared = Some(endpoint.clone());
        Ok(endpoint)
    }

    pub(crate) fn rebind(&self, socket: std::net::UdpSocket) -> Result<(), BoxError> {
        socket.set_nonblocking(true)?;
        let mut shared = self.endpoint.lock().unwrap();
        if let Some(endpoint) = shared.as_ref() {
            endpoint.rebind(socket)?;
        } else {
            let mut endpoint = Endpoint::new(
                quinn::EndpointConfig::default(),
                None,
                socket,
                Arc::new(quinn::TokioRuntime),
            )?;
            endpoint.set_default_client_config(self.quic_config.clone());
            *shared = Some(endpoint);
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
            let server_name = server_name.to_owned();
            attempts.spawn(async move {
                tokio::time::sleep(HAPPY_EYEBALLS_DELAY * index as u32).await;
                connector
                    .connect_socks_target(&proxy, target, &server_name)
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
        endpoint.set_default_client_config(self.quic_config.clone());
        let connection = tokio::select! {
            result = endpoint.connect(peer, server_name)? => result?,
            _ = socket.closed() => return Err("SOCKS5 UDP association closed during handshake".into()),
        };
        let monitored = connection.clone();
        let result = Self::build_h3(&endpoint, connection, &self.client_config).await;
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
            let server_name = server_name.to_owned();
            attempts.spawn(async move {
                tokio::time::sleep(HAPPY_EYEBALLS_DELAY * index as u32).await;
                let connection = endpoint.connect(address, &server_name)?.await?;
                Self::build_h3(&endpoint, connection, &config).await
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
    ) -> Result<H3Connection, BoxError> {
        let mut builder = h3::client::builder();
        builder.enable_extended_connect(true);
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
        Ok((driver, sender, endpoint.clone(), connection))
    }
}
