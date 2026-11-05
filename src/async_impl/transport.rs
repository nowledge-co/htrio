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
        pub(crate) discover: bool,
        pub(crate) prior_knowledge: bool,
        pub(crate) proxies: Arc<Vec<Matcher>>,
        pub(crate) connect_timeout: Option<Duration>,
        pub(crate) alternative_timeout: Duration,
    }

    impl Http3Transport {
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
                if let Some(alternative) = self.cache.get(&origin) {
                    let prepared = self
                        .prepare(
                            &origin,
                            Some(alternative.authority.clone()),
                            proxy.as_ref(),
                            true,
                        )
                        .await;
                    match prepared {
                        Ok(mut connection) => {
                            let replay = clone_request(&request);
                            *request.version_mut() = Version::HTTP_3;
                            request.headers_mut().insert(
                                "alt-used",
                                alternative
                                    .authority
                                    .as_str()
                                    .parse()
                                    .expect("valid authority"),
                            );
                            match connection.send_request(request).await {
                                Ok(response)
                                    if response.status() == StatusCode::MISDIRECTED_REQUEST =>
                                {
                                    self.cache.remove(&origin, &alternative);
                                    self.client.as_ref().unwrap().remove(
                                        &origin,
                                        alternative.authority,
                                        proxy.as_ref(),
                                    );
                                    // RFC 7838 permits replay after 421 even for
                                    // non-idempotent methods, but never replay a
                                    // consumed streaming body.
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
                                Err(error) => {
                                    let error = error::request(error);
                                    if !crate::retry::is_retryable_error(&error) {
                                        self.cache.mark_broken(&origin, &alternative);
                                    }
                                    // Dispatch has started. A transport failure
                                    // does not prove the server did not execute
                                    // a request, so leave retries to the policy.
                                    return Err(error);
                                }
                            }
                        }
                        Err(error) => {
                            log::debug!("HTTP/3 alternative connection failed: {error}");
                            self.cache.mark_broken(&origin, &alternative);
                        }
                    }
                }
            }

            if forced {
                let mut connection = self.prepare(&origin, None, proxy.as_ref(), false).await?;
                *request.version_mut() = Version::HTTP_3;
                let response = connection
                    .send_request(request)
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
