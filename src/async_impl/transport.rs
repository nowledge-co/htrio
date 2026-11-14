//! Select a transport for each hop below shared redirect, cookie, and retry middleware.
//!
//! Preparing QUIC must not consume the body: failures before dispatch may use
//! the origin route, preserving the selected proxy. After dispatch, only the
//! retry policy may authorize replay, including after a 421 response.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::body::ResponseBody;
use sync_wrapper::SyncWrapper;

pub(super) struct ResponseFuture {
    inner: SyncWrapper<
        Pin<Box<dyn Future<Output = crate::Result<http::Response<ResponseBody>>> + Send>>,
    >,
}

impl ResponseFuture {
    pub(super) fn new(
        future: impl Future<Output = crate::Result<http::Response<ResponseBody>>> + Send + 'static,
    ) -> Self {
        Self {
            inner: SyncWrapper::new(Box::pin(future)),
        }
    }
}

impl Future for ResponseFuture {
    type Output = crate::Result<http::Response<ResponseBody>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.get_mut().as_mut().poll(cx)
    }
}

#[cfg(feature = "http3")]
pub(super) use http3::Http3Transport;

#[cfg(feature = "http3")]
mod http3 {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use http::{Request, Response, StatusCode, Uri, Version};

    use super::super::body::{boxed, ResponseBody};
    use super::super::client::HyperClient;
    use super::super::h3_client::{H3Client, PoolClient, Route};
    use crate::alt_svc::AltSvcCache;
    use crate::error::{self, BoxError};
    use crate::http3::{RecoveryReason, TransportInfo, TransportSelection};
    use crate::proxy::Matcher;
    use crate::Body;

    #[derive(Clone)]
    pub(crate) struct Http3Transport {
        pub(crate) client: Option<H3Client>,
        pub(crate) cache: Arc<AltSvcCache>,
        pub(crate) tcp_preconnect: crate::connect::TcpPreconnect,
        pub(crate) discover: bool,
        pub(crate) prior_knowledge: bool,
        pub(crate) proxies: Arc<Vec<Matcher>>,
        pub(crate) connect_timeout: Option<Duration>,
        pub(crate) alternative_timeout: Duration,
    }

    enum Prepared {
        Http3(PoolClient, crate::alt_svc::Alternative),
        Tcp(crate::connect::sealed::Conn, TransportSelection),
    }

    impl Http3Transport {
        pub(crate) fn network_changed(&mut self) {
            if let Some(client) = self.client.as_mut() {
                client.network_generation(self.cache.generation());
            }
            self.tcp_preconnect.ready.clear();
        }

        pub(crate) fn rebind(&self, socket: std::net::UdpSocket) -> crate::Result<()> {
            self.client
                .as_ref()
                .ok_or_else(|| error::request("HTTP/3 is not configured"))?
                .rebind(socket)
                .map_err(error::request)
        }

        async fn candidates(
            &self,
            origin: &Uri,
            proxy: Option<&crate::proxy::Intercepted>,
        ) -> crate::Result<(Vec<crate::alt_svc::Alternative>, bool)> {
            let mut alternatives = self
                .cache
                .candidates(origin)
                .into_iter()
                .filter(|alternative| {
                    !self
                        .client
                        .as_ref()
                        .unwrap()
                        .is_broken(origin, &alternative.authority, proxy)
                })
                .collect::<Vec<_>>();
            let services = self.client.as_ref().unwrap().discover(origin, proxy).await;
            let requires_ech = services.iter().any(|service| service.requires_ech);
            if requires_ech {
                alternatives.clear();
            }
            for service in services {
                if (!requires_ech || service.requires_ech)
                    && !alternatives
                        .iter()
                        .any(|alternative| alternative.authority == service.authority)
                    && !self
                        .client
                        .as_ref()
                        .unwrap()
                        .is_broken(origin, &service.authority, proxy)
                {
                    alternatives.push(service);
                }
            }
            if requires_ech && alternatives.is_empty() {
                return Err(error::request("all ECH service routes are unavailable"));
            }
            Ok((alternatives, requires_ech))
        }

        pub(crate) async fn preconnect(&mut self, origin: Uri) -> crate::Result<()> {
            if let Some(client) = self.client.as_mut() {
                if client.network_generation(self.cache.generation()) {
                    self.tcp_preconnect.ready.clear();
                }
            }
            let proxy = self
                .proxies
                .iter()
                .find_map(|proxy| proxy.intercept(&origin));
            if self.prior_knowledge {
                self.prepare_with_early(&origin, None, proxy.as_ref(), false, false)
                    .await?;
            } else if self.discover
                && self.client.is_some()
                && origin.scheme_str() == Some("https")
                && proxy.as_ref().is_none_or(|proxy| {
                    cfg!(feature = "socks")
                        && matches!(proxy.uri().scheme_str(), Some("socks5" | "socks5h"))
                })
            {
                let (alternatives, requires_ech) = self.candidates(&origin, proxy.as_ref()).await?;
                let mut failed = Vec::new();
                match self
                    .prepare_candidates(&origin, alternatives, proxy.as_ref(), &mut failed, false)
                    .await
                {
                    Ok(_) => return Ok(()),
                    Err(error) if requires_ech => return Err(error::request(error)),
                    Err(_) => {}
                }
                let connection = self
                    .tcp_preconnect
                    .connect(origin.clone())
                    .await
                    .map_err(error::request)?;
                self.tcp_preconnect.ready.insert(&origin, connection);
            } else {
                let connection = self
                    .tcp_preconnect
                    .connect(origin.clone())
                    .await
                    .map_err(error::request)?;
                self.tcp_preconnect.ready.insert(&origin, connection);
            }
            Ok(())
        }

        pub(crate) async fn send(
            mut self,
            tcp: HyperClient,
            mut request: Request<Body>,
        ) -> crate::Result<Response<ResponseBody>> {
            let origin = request.uri().clone();
            let origin_retry = request
                .extensions_mut()
                .remove::<crate::retry::MisdirectedRetry>();
            let started = origin_retry.map_or_else(Instant::now, |retry| retry.started);
            let tcp_selection = if origin_retry.is_some() {
                TransportSelection::MisdirectedFallback
            } else {
                TransportSelection::Origin
            };
            let proxy = self
                .proxies
                .iter()
                .find_map(|proxy| proxy.intercept(&origin));
            let generation = self.cache.generation();
            if let Some(client) = self.client.as_mut() {
                if client.network_generation(generation) {
                    self.tcp_preconnect.ready.clear();
                }
            }
            let forced = self.prior_knowledge || request.version() == Version::HTTP_3;
            let supports_http3 = proxy.as_ref().is_none_or(|proxy| {
                cfg!(feature = "socks")
                    && matches!(proxy.uri().scheme_str(), Some("socks5" | "socks5h"))
            });
            // Alt-Used describes this particular transport hop. In particular,
            // it must not leak through redirects or through a TCP fallback.
            request.headers_mut().remove("alt-used");
            // SOCKS authenticates during tunnel setup. HTTP forward proxies
            // still need their authorization header on the TCP request.
            if proxy.is_some() && supports_http3 {
                request
                    .headers_mut()
                    .remove(http::header::PROXY_AUTHORIZATION);
            }

            if forced && !supports_http3 {
                return Err(error::request("HTTP/3 proxying requires SOCKS5 UDP"));
            }

            let allow_early = super::super::h3_client::can_send_early(&request);

            if self.discover
                && origin_retry.is_none()
                && supports_http3
                && self.client.is_some()
                && request.version() == Version::HTTP_11
                && !forced
            {
                let (alternatives, requires_ech) = self.candidates(&origin, proxy.as_ref()).await?;
                if !alternatives.is_empty() {
                    let mut failed = Vec::new();
                    let tcp_prepare = self.tcp_preconnect.clone();
                    let probe_candidates = alternatives
                        .iter()
                        .map(|alternative| alternative.authority.clone())
                        .collect::<Vec<_>>();
                    let prepared = {
                        let h3 = self.prepare_candidates(
                            &origin,
                            alternatives,
                            proxy.as_ref(),
                            &mut failed,
                            allow_early,
                        );
                        let tcp_connect = async {
                            tokio::time::sleep(Duration::from_millis(250)).await;
                            tcp_prepare.connect(origin.clone()).await
                        };
                        tokio::pin!(h3, tcp_connect);
                        if requires_ech {
                            let (connection, alternative) = h3.await.map_err(error::request)?;
                            Prepared::Http3(connection, alternative)
                        } else {
                            tokio::select! {
                                result = &mut h3 => match result {
                                    Ok((connection, alternative)) => Prepared::Http3(connection, alternative),
                                    Err(_) => Prepared::Tcp(tcp_connect.await.map_err(error::request)?, TransportSelection::ConnectionFallback),
                                },
                                result = &mut tcp_connect => match result {
                                    Ok(connection) => Prepared::Tcp(connection, TransportSelection::TcpRace),
                                    Err(tcp_error) => match h3.await {
                                        Ok((connection, alternative)) => Prepared::Http3(connection, alternative),
                                        Err(_) => return Err(error::request(tcp_error)),
                                    },
                                },
                            }
                        }
                    };
                    match prepared {
                        Prepared::Http3(connection, alternative) => {
                            *request.version_mut() = Version::HTTP_3;
                            if !alternative.from_dns {
                                request.headers_mut().insert(
                                    "alt-used",
                                    alternative
                                        .authority
                                        .as_str()
                                        .parse()
                                        .expect("valid authority"),
                                );
                            }
                            match self
                                .dispatch(connection, request, &origin, proxy.as_ref(), true)
                                .await
                            {
                                Ok(mut response)
                                    if response.status() == StatusCode::MISDIRECTED_REQUEST =>
                                {
                                    self.annotate(
                                        &mut response,
                                        started,
                                        if alternative.from_dns {
                                            TransportSelection::HttpsRecord
                                        } else {
                                            TransportSelection::AltSvc
                                        },
                                    );
                                    self.cache.remove(&origin, &alternative);
                                    self.client.as_ref().unwrap().reject(
                                        &origin,
                                        alternative.authority,
                                        proxy.as_ref(),
                                    );
                                    // Evict the route even when replay is forbidden. The
                                    // shared policy owns cloning, scope, limits and budget.
                                    // ECH-required routes must never fall back to clear SNI.
                                    if !alternative.requires_ech {
                                        response.extensions_mut().insert(
                                            crate::retry::MisdirectedRetry { started },
                                        );
                                    }
                                    return Ok(response);
                                }
                                Ok(mut response) => {
                                    self.annotate(
                                        &mut response,
                                        started,
                                        if alternative.from_dns {
                                            TransportSelection::HttpsRecord
                                        } else {
                                            TransportSelection::AltSvc
                                        },
                                    );
                                    self.observe(&origin, &response, started);
                                    return Ok(response);
                                }
                                Err(error) => return Err(error::request(error)),
                            }
                        }
                        Prepared::Tcp(connection, selection) => {
                            self.tcp_preconnect.ready.insert(&origin, connection);
                            let mut response = tcp
                                .request(request)
                                .await
                                .map_err(error::request)?
                                .map(boxed);
                            self.annotate(&mut response, started, selection);
                            if selection == TransportSelection::TcpRace {
                                if let Some(alternative) = probe_candidates.get(failed.len()) {
                                    self.probe_after_tcp(
                                        &origin,
                                        alternative.clone(),
                                        proxy.as_ref(),
                                        generation,
                                    );
                                }
                            }
                            if generation == self.cache.generation() {
                                for alternative in failed {
                                    self.client.as_ref().unwrap().confirm_broken(
                                        &origin,
                                        alternative,
                                        proxy.as_ref(),
                                    );
                                }
                            }
                            self.observe(&origin, &response, started);
                            return Ok(response);
                        }
                    }
                }
            }

            if forced {
                let connection = self
                    .prepare_with_early(&origin, None, proxy.as_ref(), false, allow_early)
                    .await?;
                *request.version_mut() = Version::HTTP_3;
                let mut response = self
                    .dispatch(connection, request, &origin, proxy.as_ref(), false)
                    .await
                    .map_err(error::request)?;
                self.annotate(&mut response, started, TransportSelection::ExplicitHttp3);
                self.observe(&origin, &response, started);
                return Ok(response);
            }

            let mut response = tcp
                .request(request)
                .await
                .map_err(error::request)?
                .map(boxed);
            self.annotate(&mut response, started, tcp_selection);
            self.observe(&origin, &response, started);
            Ok(response)
        }

        async fn prepare_candidates(
            &mut self,
            origin: &Uri,
            alternatives: Vec<crate::alt_svc::Alternative>,
            proxy: Option<&crate::proxy::Intercepted>,
            failed: &mut Vec<http::uri::Authority>,
            allow_early: bool,
        ) -> Result<(PoolClient, crate::alt_svc::Alternative), BoxError> {
            let mut last: BoxError = "no available HTTP/3 alternative".into();
            for alternative in alternatives {
                match self
                    .prepare_with_early(
                        origin,
                        Some(Route::from(&alternative)),
                        proxy,
                        true,
                        allow_early,
                    )
                    .await
                {
                    Ok(connection) => return Ok((connection, alternative)),
                    Err(error) => {
                        failed.push(alternative.authority);
                        last = error.into();
                    }
                }
            }
            Err(last)
        }

        async fn dispatch(
            &mut self,
            mut connection: PoolClient,
            request: Request<Body>,
            origin: &Uri,
            proxy: Option<&crate::proxy::Intercepted>,
            automatic: bool,
        ) -> Result<Response<ResponseBody>, BoxError> {
            // Retain this hop's privacy requirement across confirmed retries.
            // A record expiring between attempts cannot authorize clear SNI.
            let alternative = connection.route();
            let mut pending = Some(request);
            let mut recovery = None;
            for attempt in 0..2 {
                let mut result = connection.send_request(&mut pending).await;
                if let (Some(reason), Ok(response)) = (recovery, result.as_mut()) {
                    response.extensions_mut().insert(reason);
                }
                if result
                    .as_ref()
                    .is_ok_and(|response| response.status() == StatusCode::MISDIRECTED_REQUEST)
                {
                    self.client.as_ref().unwrap().reject_connection(&connection);
                }
                if result.is_ok() || pending.is_none() || attempt == 1 {
                    return result;
                }
                recovery = Some(
                    if result.as_ref().err().is_some_and(|error| {
                        error.is::<super::super::h3_client::session::EarlyDataRejected>()
                    }) {
                        RecoveryReason::EarlyDataRejected
                    } else {
                        RecoveryReason::ConnectionRetired
                    },
                );
                self.client.as_ref().unwrap().retire_connection(&connection);
                connection = self
                    .prepare_with_early(origin, Some(alternative.clone()), proxy, automatic, false)
                    .await
                    .map_err(|error| Box::new(error) as BoxError)?;
            }
            unreachable!("bounded reconnect returns a result")
        }

        async fn prepare_with_early(
            &mut self,
            origin: &Uri,
            alternative: Option<Route>,
            proxy: Option<&crate::proxy::Intercepted>,
            automatic: bool,
            allow_early: bool,
        ) -> crate::Result<PoolClient> {
            let client = self.client.as_mut().ok_or_else(|| {
                error::request("HTTP/3 requires a rustls configuration with TLS 1.3 support")
            })?;
            let timeout = if automatic {
                Some(
                    self.connect_timeout
                        .map_or(self.alternative_timeout, |timeout| {
                            timeout.min(self.alternative_timeout)
                        }),
                )
            } else {
                self.connect_timeout
            };
            let deadline =
                timeout.and_then(|timeout| tokio::time::Instant::now().checked_add(timeout));
            let connect = client.prepare(origin, alternative, proxy, allow_early, deadline);
            let result = match timeout {
                Some(timeout) => tokio::time::timeout(timeout, connect)
                    .await
                    .map_err(|_| error::request(error::TimedOut))?,
                None => connect.await,
            };
            result.map_err(|error: BoxError| error::request(error))
        }

        fn probe_after_tcp(
            &self,
            origin: &Uri,
            alternative: http::uri::Authority,
            proxy: Option<&crate::proxy::Intercepted>,
            generation: u64,
        ) {
            let Some(client) = self.client.as_ref() else {
                return;
            };
            let Some(guard) = client.probe(origin, alternative.clone(), proxy, generation) else {
                return;
            };
            let mut transport = self.clone();
            let origin = origin.clone();
            let proxy = proxy.cloned();
            tokio::spawn(async move {
                let _guard = guard;
                let result = transport
                    .prepare_with_early(
                        &origin,
                        Some(Route {
                            authority: alternative.clone(),
                            requires_ech: false,
                        }),
                        proxy.as_ref(),
                        true,
                        false,
                    )
                    .await;
                if result.is_err() && transport.cache.generation() == generation {
                    if let Some(client) = transport.client.as_ref() {
                        client.confirm_broken(&origin, alternative, proxy.as_ref());
                    }
                }
            });
        }

        fn annotate(
            &self,
            response: &mut Response<ResponseBody>,
            started: Instant,
            selection: TransportSelection,
        ) {
            let info = TransportInfo {
                protocol: response.version(),
                time_to_headers: started.elapsed(),
                selection,
                recovery: response.extensions().get::<RecoveryReason>().copied(),
            };
            response.extensions_mut().insert(info);
        }

        fn observe(&self, origin: &Uri, response: &Response<ResponseBody>, started: Instant) {
            if self.discover {
                self.cache.observe(
                    origin,
                    response.status(),
                    response.headers(),
                    started.elapsed(),
                );
            }
        }
    }
}
