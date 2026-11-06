//! Select a transport for each hop below shared redirect, cookie, and retry middleware.
//!
//! Preparing QUIC must not consume the body: failures before dispatch may use
//! the origin route, preserving the selected proxy. After dispatch, only the
//! retry policy may authorize replay, except for a 421 with a cloneable body.

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
    use super::super::h3_client::{H3Client, PoolClient};
    use crate::alt_svc::AltSvcCache;
    use crate::error::{self, BoxError};
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
        Tcp(crate::connect::sealed::Conn),
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

        pub(crate) async fn preconnect(&mut self, origin: Uri) -> crate::Result<()> {
            let proxy = self
                .proxies
                .iter()
                .find_map(|proxy| proxy.intercept(&origin));
            if self.prior_knowledge {
                self.prepare(&origin, None, proxy.as_ref(), false).await?;
            } else if self.discover && origin.scheme_str() == Some("https") {
                let alternatives = self
                    .cache
                    .candidates(&origin)
                    .into_iter()
                    .filter(|alternative| {
                        self.client.as_ref().is_some_and(|client| {
                            !client.is_broken(&origin, &alternative.authority, proxy.as_ref())
                        })
                    })
                    .collect::<Vec<_>>();
                let mut failed = Vec::new();
                if self
                    .prepare_candidates(&origin, alternatives, proxy.as_ref(), &mut failed)
                    .await
                    .is_ok()
                {
                    return Ok(());
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
            let started = Instant::now();
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

            if self.discover
                && supports_http3
                && self.client.is_some()
                && request.version() == Version::HTTP_11
                && !forced
            {
                let mut alternatives = self
                    .cache
                    .candidates(&origin)
                    .into_iter()
                    .filter(|alternative| {
                        !self.client.as_ref().unwrap().is_broken(
                            &origin,
                            &alternative.authority,
                            proxy.as_ref(),
                        )
                    })
                    .collect::<Vec<_>>();
                let services = self
                    .client
                    .as_ref()
                    .unwrap()
                    .discover(&origin, proxy.as_ref())
                    .await;
                let requires_ech = services.iter().any(|service| service.requires_ech);
                if requires_ech {
                    alternatives.clear();
                }
                for service in services {
                    if (!requires_ech || service.requires_ech)
                        && !alternatives
                            .iter()
                            .any(|alternative| alternative.authority == service.authority)
                        && !self.client.as_ref().unwrap().is_broken(
                            &origin,
                            &service.authority,
                            proxy.as_ref(),
                        )
                    {
                        alternatives.push(service);
                    }
                }
                if requires_ech && alternatives.is_empty() {
                    return Err(error::request("all ECH service routes are unavailable"));
                }
                if !alternatives.is_empty() {
                    let mut failed = Vec::new();
                    let tcp_prepare = self.tcp_preconnect.clone();
                    let prepared = {
                        let h3 = self.prepare_candidates(
                            &origin,
                            alternatives,
                            proxy.as_ref(),
                            &mut failed,
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
                                    Err(_) => Prepared::Tcp(tcp_connect.await.map_err(error::request)?),
                                },
                                result = &mut tcp_connect => match result {
                                    Ok(connection) => Prepared::Tcp(connection),
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
                            let replay = if alternative.requires_ech {
                                None
                            } else {
                                clone_request(&request)
                            };
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
                                .dispatch(
                                    connection,
                                    request,
                                    &origin,
                                    Some(alternative.authority.clone()),
                                    proxy.as_ref(),
                                    true,
                                )
                                .await
                            {
                                Ok(response)
                                    if response.status() == StatusCode::MISDIRECTED_REQUEST =>
                                {
                                    self.cache.remove(&origin, &alternative);
                                    self.client.as_ref().unwrap().reject(
                                        &origin,
                                        alternative.authority,
                                        proxy.as_ref(),
                                    );
                                    if let Some(replay) = replay {
                                        request = replay;
                                    } else {
                                        return Ok(response);
                                    }
                                }
                                Ok(response) => {
                                    self.observe(&origin, &response, started);
                                    return Ok(response);
                                }
                                Err(error) => return Err(error::request(error)),
                            }
                        }
                        Prepared::Tcp(connection) => {
                            self.tcp_preconnect.ready.insert(&origin, connection);
                            let response = tcp
                                .request(request)
                                .await
                                .map_err(error::request)?
                                .map(boxed);
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
                let connection = self.prepare(&origin, None, proxy.as_ref(), false).await?;
                *request.version_mut() = Version::HTTP_3;
                let response = self
                    .dispatch(connection, request, &origin, None, proxy.as_ref(), false)
                    .await
                    .map_err(error::request)?;
                self.observe(&origin, &response, started);
                return Ok(response);
            }

            let response = tcp
                .request(request)
                .await
                .map_err(error::request)?
                .map(boxed);
            self.observe(&origin, &response, started);
            Ok(response)
        }

        async fn prepare_candidates(
            &mut self,
            origin: &Uri,
            alternatives: Vec<crate::alt_svc::Alternative>,
            proxy: Option<&crate::proxy::Intercepted>,
            failed: &mut Vec<http::uri::Authority>,
        ) -> Result<(PoolClient, crate::alt_svc::Alternative), BoxError> {
            let mut last: BoxError = "no available HTTP/3 alternative".into();
            for alternative in alternatives {
                match self
                    .prepare(origin, Some(alternative.authority.clone()), proxy, true)
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
            alternative: Option<http::uri::Authority>,
            proxy: Option<&crate::proxy::Intercepted>,
            automatic: bool,
        ) -> Result<Response<ResponseBody>, BoxError> {
            let mut pending = Some(request);
            for attempt in 0..2 {
                let result = connection.send_request(&mut pending).await;
                if result.is_ok() || pending.is_none() || attempt == 1 {
                    return result;
                }
                self.client.as_ref().unwrap().remove(
                    origin,
                    alternative
                        .clone()
                        .unwrap_or_else(|| origin.authority().unwrap().clone()),
                    proxy,
                );
                connection = self
                    .prepare(origin, alternative.clone(), proxy, automatic)
                    .await
                    .map_err(|error| Box::new(error) as BoxError)?;
            }
            unreachable!("bounded reconnect returns a result")
        }

        async fn prepare(
            &mut self,
            origin: &Uri,
            alternative: Option<http::uri::Authority>,
            proxy: Option<&crate::proxy::Intercepted>,
            automatic: bool,
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
            let connect = client.prepare(origin, alternative, proxy);
            let result = match timeout {
                Some(timeout) => tokio::time::timeout(timeout, connect)
                    .await
                    .map_err(|_| error::request(error::TimedOut))?,
                None => connect.await,
            };
            result.map_err(|error: BoxError| error::request(error))
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

    fn clone_request(request: &Request<Body>) -> Option<Request<Body>> {
        let mut replay = Request::new(request.body().try_clone()?);
        *replay.method_mut() = request.method().clone();
        *replay.uri_mut() = request.uri().clone();
        *replay.headers_mut() = request.headers().clone();
        *replay.version_mut() = request.version();
        *replay.extensions_mut() = request.extensions().clone();
        Some(replay)
    }
}
