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
use std::sync::{Arc, OnceLock};
use std::time::Duration;

type H3Connection = (
    h3::client::Connection<Connection, Bytes>,
    SendRequest<OpenStreams, Bytes>,
    Endpoint,
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

    /// Send reserved protocol values to exercise peers' extension handling.
    pub(crate) send_grease: Option<bool>,
}

impl Default for H3ClientConfig {
    fn default() -> Self {
        Self {
            max_field_section_size: None,
            send_grease: None,
        }
    }
}

#[derive(Clone)]
pub(crate) struct H3Connector {
    resolver: DynResolver,
    endpoint: Arc<OnceLock<Endpoint>>,
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
            endpoint: Arc::new(OnceLock::new()),
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

    fn endpoint(&self) -> Result<Endpoint, BoxError> {
        if let Some(endpoint) = self.endpoint.get() {
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
        let _ = self.endpoint.set(endpoint);
        Ok(self
            .endpoint
            .get()
            .expect("endpoint was initialized")
            .clone())
    }

    #[cfg(feature = "socks")]
    async fn connect_socks(
        &mut self,
        proxy: &crate::proxy::Intercepted,
        host: &str,
        port: Option<u16>,
        server_name: &str,
    ) -> Result<H3Connection, BoxError> {
        use super::socks::{SocksSocket, Target};
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
        let mut last_error: BoxError = "HTTP/3 target resolved no addresses".into();
        for target in targets {
            let (socket, peer) =
                SocksSocket::connect(proxy, target, &mut self.resolver, self.local_addr).await?;
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
            match Self::build_h3(&endpoint, connection, &self.client_config).await {
                Ok(connection) => {
                    tokio::spawn(async move {
                        tokio::select! {
                            _ = monitored.closed() => {},
                            _ = socket.closed() => monitored.close(0_u32.into(), b"SOCKS5 association closed"),
                        }
                    });
                    return Ok(connection);
                }
                Err(error) => {
                    monitored.close(0_u32.into(), b"HTTP/3 setup failed");
                    last_error = error;
                }
            }
        }
        Err(last_error)
    }

    async fn remote_connect(
        &mut self,
        addrs: Vec<SocketAddr>,
        server_name: &str,
    ) -> Result<H3Connection, BoxError> {
        if addrs.is_empty() {
            return Err("no addresses to connect to".into());
        }
        let endpoint = self.endpoint()?;

        let (mut ipv6_addrs, mut ipv4_addrs): (Vec<SocketAddr>, Vec<SocketAddr>) =
            addrs.into_iter().partition(|addr| addr.is_ipv6());

        if let Some(local_ip) = self.local_addr {
            if local_ip.is_ipv6() {
                ipv4_addrs.clear();
            } else {
                ipv6_addrs.clear();
            }
        }

        if ipv6_addrs.is_empty() {
            return Self::try_addresses_static(
                &endpoint,
                &ipv4_addrs,
                server_name,
                &self.client_config,
            )
            .await;
        }
        if ipv4_addrs.is_empty() {
            return Self::try_addresses_static(
                &endpoint,
                &ipv6_addrs,
                server_name,
                &self.client_config,
            )
            .await;
        }

        let client_config = self.client_config.clone();

        if self.local_addr.is_some() {
            return match Self::try_addresses_static(
                &endpoint,
                &ipv6_addrs,
                server_name,
                &client_config,
            )
            .await
            {
                Ok(conn) => Ok(conn),
                Err(_) => {
                    Self::try_addresses_static(&endpoint, &ipv4_addrs, server_name, &client_config)
                        .await
                }
            };
        }

        Self::try_addresses_happy_eyeballs(
            &endpoint,
            &ipv6_addrs,
            &ipv4_addrs,
            server_name,
            &client_config,
        )
        .await
    }

    async fn try_addresses_static(
        endpoint: &Endpoint,
        addrs: &[SocketAddr],
        server_name: &str,
        client_config: &H3ClientConfig,
    ) -> Result<H3Connection, BoxError> {
        let mut last_err: Option<BoxError> = None;

        for addr in addrs {
            match endpoint.connect(*addr, server_name) {
                Ok(connecting) => match connecting.await {
                    Ok(new_conn) => {
                        return Self::build_h3(endpoint, new_conn, client_config).await;
                    }
                    Err(e) => {
                        last_err = Some(Box::new(e) as BoxError);
                    }
                },
                Err(e) => {
                    last_err = Some(Box::new(e) as BoxError);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| "no addresses available".into()))
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
        if let Some(grease) = config.send_grease {
            builder.send_grease(grease);
        }
        let (driver, sender) = builder.build(Connection::new(connection)).await?;
        Ok((driver, sender, endpoint.clone()))
    }

    async fn try_addresses_happy_eyeballs(
        endpoint: &Endpoint,
        ipv6_addrs: &[SocketAddr],
        ipv4_addrs: &[SocketAddr],
        server_name: &str,
        client_config: &H3ClientConfig,
    ) -> Result<H3Connection, BoxError> {
        let ipv6_connect =
            Self::try_addresses_static(endpoint, ipv6_addrs, server_name, client_config);
        tokio::pin!(ipv6_connect);

        let delay = tokio::time::sleep(HAPPY_EYEBALLS_DELAY);
        tokio::pin!(delay);

        tokio::select! {
            result = &mut ipv6_connect => {
                return match result {
                    Ok(conn) => Ok(conn),
                    Err(_) => {
                        Self::try_addresses_static(endpoint, ipv4_addrs, server_name, client_config).await
                    }
                };
            }
            _ = &mut delay => {}
        }

        let ipv4_connect =
            Self::try_addresses_static(endpoint, ipv4_addrs, server_name, client_config);
        tokio::pin!(ipv4_connect);

        let wait_for_ipv6 = tokio::select! {
            result = &mut ipv6_connect => {
                match result {
                    Ok(conn) => return Ok(conn),
                    Err(_) => false,
                }
            }
            result = &mut ipv4_connect => {
                match result {
                    Ok(conn) => return Ok(conn),
                    Err(_) => true,
                }
            }
        };

        if wait_for_ipv6 {
            ipv6_connect.await
        } else {
            ipv4_connect.await
        }
    }
}
