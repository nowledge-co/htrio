//! Share multiplexed HTTP/3 connections within an origin and proxy identity.
//!
//! A connecting lock coalesces concurrent handshakes for the same route. Its
//! drop releases waiters on cancellation or failure. A driver owns the endpoint;
//! pooled senders and active streams keep the connection usable independently.

use bytes::Bytes;
use std::collections::HashMap;
use std::future::{self, Future};
use std::pin::Pin;
use std::sync::mpsc::{Receiver, TryRecvError};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{oneshot, watch};
use tokio::time::Instant;

use crate::async_impl::body::ResponseBody;
use crate::error::{BoxError, Error, Kind};
use crate::Body;
use bytes::Buf;
use h3::client::SendRequest;
use h3_quinn::{Connection, OpenStreams};
use http::uri::{Authority, Scheme};
use http::{Request, Response, Uri};
use log::trace;

// Include both authorities: an alternative can serve several origins, and an
// origin can move between alternatives without reusing a stale connection.
pub(super) type Key = (Scheme, Authority, Authority, Option<ProxyKey>);

#[derive(Clone, PartialEq, Eq, Hash)]
pub(super) struct ProxyKey {
    uri: Uri,
    credentials: Option<(String, String)>,
}

impl ProxyKey {
    pub(super) fn new(proxy: &crate::proxy::Intercepted) -> Self {
        Self {
            uri: proxy.uri().clone(),
            #[cfg(feature = "socks")]
            credentials: proxy
                .raw_auth()
                .map(|(user, password)| (user.to_owned(), password.to_owned())),
            #[cfg(not(feature = "socks"))]
            credentials: None,
        }
    }
}

impl std::fmt::Debug for ProxyKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyKey")
            .field("scheme", &self.uri.scheme_str())
            .field("host", &self.uri.host())
            .field("port", &self.uri.port_u16())
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct Pool {
    inner: Arc<Mutex<PoolInner>>,
}

struct ConnectingLockInner {
    key: Key,
    pool: Arc<Mutex<PoolInner>>,
    generation: u64,
}

/// A lock that ensures only one HTTP/3 connection is established per route at a
/// time. The lock is automatically released when dropped.
pub struct ConnectingLock(Option<ConnectingLockInner>);

/// A waiter that allows subscribers to receive updates when a new connection is
/// established or when the connection attempt fails. For example, when
/// connection lock is dropped due to an error.
pub struct ConnectingWaiter {
    receiver: watch::Receiver<Option<PoolClient>>,
}

pub enum Connecting {
    /// A connection attempt is already in progress.
    /// You must subscribe to updates instead of initiating a new connection.
    InProgress(ConnectingWaiter),
    /// The connection lock has been acquired, allowing you to initiate a
    /// new connection.
    Acquired(ConnectingLock),
}

impl ConnectingLock {
    fn new(key: Key, pool: Arc<Mutex<PoolInner>>, generation: u64) -> Self {
        Self(Some(ConnectingLockInner {
            key,
            pool,
            generation,
        }))
    }

    /// Forget the lock and return corresponding Key
    fn forget(mut self) -> (Key, u64) {
        // Unwrap is safe because the Option can be None only after dropping the
        // lock
        let lock = self.0.take().unwrap();
        (lock.key, lock.generation)
    }
}

impl Drop for ConnectingLock {
    fn drop(&mut self) {
        if let Some(ConnectingLockInner {
            key,
            pool,
            generation,
        }) = self.0.take()
        {
            let mut pool = pool.lock().unwrap();
            if pool.generation == generation {
                pool.connecting.remove(&key);
            }
            trace!("HTTP/3 connecting lock for {:?} is dropped", key);
        }
    }
}

impl ConnectingWaiter {
    pub async fn receive(mut self) -> Option<PoolClient> {
        match self.receiver.wait_for(Option::is_some).await {
            // unwrap because we already checked that option is Some
            Ok(ok) => Some(ok.as_ref().unwrap().to_owned()),
            Err(_) => None,
        }
    }
}

impl Pool {
    pub fn new(timeout: Option<Duration>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(PoolInner {
                generation: 0,
                connecting: HashMap::new(),
                idle_conns: HashMap::new(),
                timeout,
            })),
        }
    }

    /// Coalesce handshakes for the same origin, destination, and proxy identity.
    pub fn connecting(&self, key: &Key) -> Connecting {
        let mut inner = self.inner.lock().unwrap();

        if let Some(sender) = inner.connecting.get(key) {
            Connecting::InProgress(ConnectingWaiter {
                receiver: sender.subscribe(),
            })
        } else {
            let (tx, _) = watch::channel(None);
            inner.connecting.insert(key.clone(), tx);
            Connecting::Acquired(ConnectingLock::new(
                key.clone(),
                Arc::clone(&self.inner),
                inner.generation,
            ))
        }
    }

    pub fn try_pool(&self, key: &Key) -> Option<PoolClient> {
        let mut inner = self.inner.lock().unwrap();
        let timeout = inner.timeout;
        if let Some(conn) = inner.idle_conns.get(&key) {
            // We check first if the connection still valid
            // and if not, we remove it from the pool.
            if conn.is_invalid() {
                trace!("pooled HTTP/3 connection is invalid so removing it...");
                inner.idle_conns.remove(&key);
                return None;
            }

            if let Some(duration) = timeout {
                if Instant::now().saturating_duration_since(conn.idle_timeout) > duration {
                    trace!("pooled connection expired");
                    inner.idle_conns.remove(&key);
                    return None;
                }
            }
        }

        inner
            .idle_conns
            .get_mut(&key)
            .and_then(|conn| Some(conn.pool()))
    }

    pub fn coalescing_candidates(&self, key: &Key) -> Vec<(Key, PoolClient)> {
        let inner = self.inner.lock().unwrap();
        inner
            .idle_conns
            .iter()
            .filter(|(candidate, connection)| {
                candidate.0 == key.0
                    && candidate.1 != key.1
                    && candidate.2 == key.2
                    && candidate.3 == key.3
                    && !connection.is_invalid()
                    && inner
                        .timeout
                        .is_none_or(|timeout| connection.idle_timeout.elapsed() <= timeout)
            })
            .map(|(key, connection)| (key.clone(), connection.client.clone()))
            .collect()
    }

    pub fn alias(&self, key: Key, existing: &Key) -> Option<PoolClient> {
        let mut inner = self.inner.lock().unwrap();
        let mut connection = inner.idle_conns.get(existing)?.clone();
        if connection.is_invalid() {
            return None;
        }
        connection.idle_timeout = Instant::now();
        connection.client.info.coalesced = true;
        connection.client.key = key.clone();
        let client = connection.client.clone();
        inner.insert(key, connection);
        Some(client)
    }

    pub fn stats(&self) -> crate::http3::PoolStats {
        let inner = self.inner.lock().unwrap();
        let available = inner
            .idle_conns
            .values()
            .filter(|connection| !connection.is_invalid())
            .count();
        crate::http3::PoolStats {
            connecting: inner.connecting.len(),
            available,
            retiring: inner.idle_conns.len() - available,
            ..Default::default()
        }
    }

    pub fn clear(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.generation = inner.generation.wrapping_add(1);
        inner.idle_conns.clear();
        inner.connecting.clear();
    }

    pub fn remove(&self, key: &Key) {
        self.inner.lock().unwrap().idle_conns.remove(key);
    }

    pub fn new_connection(
        &mut self,
        lock: ConnectingLock,
        mut driver: h3::client::Connection<Connection, Bytes>,
        tx: SendRequest<OpenStreams, Bytes>,
        endpoint: quinn::Endpoint,
        connection: quinn::Connection,
        direct: bool,
        connect_time: Duration,
        tls_info: bool,
        early: super::session::EarlyData,
    ) -> PoolClient {
        let (close_tx, close_rx) = std::sync::mpsc::channel();
        let mut confirmation = early.clone();
        let transport = connection.clone();
        tokio::spawn(async move {
            let _endpoint = endpoint;
            // Rejected early streams have no reusable HTTP/3 state. Wait for
            // acceptance before interpreting fresh SETTINGS against remembered limits.
            if !confirmation.accepted().await {
                transport.close(0_u32.into(), b"early data rejected");
                return;
            }
            *confirmation.route.last_peer.lock().unwrap() = Some(transport.remote_address());
            let error = future::poll_fn(|cx| {
                let result = driver.poll_close(cx);
                if let Some(settings) = driver.peer_settings() {
                    *confirmation.route.settings.lock().unwrap() = Some(settings);
                }
                result
            })
            .await;
            trace!("HTTP/3 driver closed: {error:?}");
            close_tx.send(error).ok();
        });

        let mut inner = self.inner.lock().unwrap();

        // We clean up "connecting" here so we don't have to acquire the lock again.
        let (key, generation) = lock.forget();
        let client = PoolClient::new(
            tx,
            connection,
            direct,
            connect_time,
            tls_info,
            early,
            key.clone(),
        );
        if generation != inner.generation {
            return client;
        }
        let Some(notifier) = inner.connecting.remove(&key) else {
            unreachable!("there should be one connecting lock at a time");
        };
        // Send the client to all our awaiters
        let pool_client = if let Err(watch::error::SendError(Some(unsent_client))) =
            notifier.send(Some(client.clone()))
        {
            // If there are no awaiters, the client is returned to us. As a
            // micro optimisation, let's reuse it and avoid cloning.
            unsent_client
        } else {
            client.clone()
        };

        let conn = PoolConnection::new(pool_client, close_rx);
        inner.insert(key, conn);

        client
    }
}

struct PoolInner {
    generation: u64,
    connecting: HashMap<Key, watch::Sender<Option<PoolClient>>>,
    idle_conns: HashMap<Key, PoolConnection>,
    timeout: Option<Duration>,
}

impl PoolInner {
    fn insert(&mut self, key: Key, conn: PoolConnection) {
        let now = tokio::time::Instant::now();
        let timeout = self.timeout;
        self.idle_conns.retain(|_, connection| {
            !connection.is_invalid()
                && timeout.is_none_or(|timeout| {
                    now.saturating_duration_since(connection.idle_timeout) <= timeout
                })
        });
        // Bound idle origins as well as the alternative service cache. Active
        // streams own their sender independently and survive idle eviction.
        if self.idle_conns.len() >= 256 {
            if let Some(oldest) = self
                .idle_conns
                .iter()
                .min_by_key(|(_, connection)| connection.idle_timeout)
                .map(|(key, _)| key.clone())
            {
                self.idle_conns.remove(&oldest);
            }
        }
        if self.idle_conns.contains_key(&key) {
            trace!("connection already exists for key {key:?}");
        }

        self.idle_conns.insert(key, conn);
    }
}

#[derive(Clone)]
pub struct PoolClient {
    pub(super) key: Key,
    inner: SendRequest<OpenStreams, Bytes>,
    pub(super) info: crate::http3::ConnectionInfo,
}

impl PoolClient {
    fn new(
        tx: SendRequest<OpenStreams, Bytes>,
        connection: quinn::Connection,
        direct: bool,
        connect_time: Duration,
        tls_info: bool,
        early: super::session::EarlyData,
        key: Key,
    ) -> Self {
        let info = crate::http3::ConnectionInfo {
            connection,
            sender: tx.clone(),
            direct,
            connect_time,
            tls_info,
            coalesced: false,
            early,
            early_data_used: false,
            dispatched: Default::default(),
            reused: false,
        };
        Self {
            inner: tx,
            info,
            key,
        }
    }

    pub async fn send_request(
        &mut self,
        request: &mut Option<Request<Body>>,
    ) -> Result<Response<ResponseBody>, BoxError> {
        let safe = can_send_early(request.as_ref().expect("request is pending"));
        if self.info.early.attempted
            && (!safe || self.info.early.rejected())
            && !self.info.early.accepted().await
        {
            return Err(super::session::EarlyDataRejected.into());
        }
        let early = safe && self.info.early.pending() && self.info.connection.is_handshaking();
        let replay =
            early.then(|| clone_empty_request(request.as_ref().expect("request is pending")));
        if early {
            request
                .as_mut()
                .unwrap()
                .headers_mut()
                .insert("early-data", http::HeaderValue::from_static("1"));
        }
        let mut result = self.send_request_inner(request).await;
        if self.info.early.attempted && !self.info.early.accepted().await {
            if let Some(replay) = replay {
                *request = Some(replay);
            }
            return Err(super::session::EarlyDataRejected.into());
        }
        if early {
            self.inner.wait_peer_settings().await?;
        }
        if early
            && result
                .as_ref()
                .is_ok_and(|response| response.status() == http::StatusCode::TOO_EARLY)
        {
            *request = replay;
            return Err(super::session::EarlyDataRejected.into());
        }
        if let Ok(response) = result.as_mut() {
            if let Some(info) = response
                .extensions_mut()
                .get_mut::<crate::http3::ConnectionInfo>()
            {
                info.early_data_used = early;
            }
        }
        result
    }

    async fn send_request_inner(
        &mut self,
        request: &mut Option<Request<Body>>,
    ) -> Result<Response<ResponseBody>, BoxError> {
        use hyper::body::Body as _;

        let (mut head, req_body) = request
            .take()
            .expect("request has not been dispatched")
            .into_parts();
        let declared = content_length(&head.headers)?;
        if let Some(exact) = req_body.size_hint().exact() {
            if declared.is_some_and(|length| length != exact) {
                return Err("request Content-Length does not match the body".into());
            }
            if exact > 0 || declared.is_some() {
                head.headers
                    .insert(http::header::CONTENT_LENGTH, exact.into());
            }
        }
        let expected_upload = content_length(&head.headers)?;
        let head_request = head.method == http::Method::HEAD;
        let mut wire = Request::new(());
        *wire.method_mut() = head.method.clone();
        *wire.uri_mut() = head.uri.clone();
        *wire.version_mut() = head.version;
        *wire.headers_mut() = head.headers.clone();
        *wire.extensions_mut() = head.extensions.clone();
        let stream = match self.inner.send_request_with_recovery(wire).await {
            Ok(stream) => stream,
            Err(error) if error.is_request_not_sent() => {
                // This error is raised before opening a stream or consuming the
                // source body. Return ownership for a bounded reconnect attempt.
                *request = Some(Request::from_parts(head, req_body));
                return Err(error.into());
            }
            Err(error) => return Err(error.into()),
        };
        let reused = self
            .info
            .dispatched
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            != 0;
        let priority = crate::http3::RequestPriority {
            handle: stream.priority_handle(),
            connection: self.info.connection.clone(),
        };
        let (send, mut recv) = stream.split();
        let upload = SendGuard {
            stream: send,
            finished: false,
        };
        let (tx, mut rx) = oneshot::channel::<Result<(), BoxError>>();
        let task = tokio::spawn(async move {
            let result = send_body(upload, req_body, expected_upload).await;
            let _ = tx.send(result);
        });
        let upload = UploadTask(task.abort_handle());
        let mut upload_done = false;
        let mut informational = 0;
        let mut response = loop {
            tokio::select! {
                result = &mut rx, if !upload_done => {
                    upload_done = true;
                    match result {
                        Ok(Ok(())) => {},
                        Ok(Err(error)) => return Err(error),
                        Err(_) => return Err("HTTP/3 upload task terminated".into()),
                    }
                }
                response = recv.recv_response() => {
                    let response = response?;
                    if response.status() == http::StatusCode::SWITCHING_PROTOCOLS {
                        recv.stop_sending(h3::error::Code::H3_MESSAGE_ERROR);
                        return Err("HTTP/3 forbids a 101 response".into());
                    }
                    if response.status().is_informational() {
                        informational += 1;
                        if informational > 64 {
                            recv.stop_sending(h3::error::Code::H3_EXCESSIVE_LOAD);
                            return Err("too many HTTP/3 informational responses".into());
                        }
                        continue;
                    }
                    break response;
                }
            }
        };
        let mut info = self.info.clone();
        info.reused = reused;
        response.extensions_mut().insert(info);
        response.extensions_mut().insert(priority);
        if self.info.tls_info {
            if let Some(identity) = self.info.connection.peer_identity() {
                if let Ok(chain) =
                    identity.downcast::<Vec<rustls::pki_types::CertificateDer<'static>>>()
                {
                    response.extensions_mut().insert(crate::tls::TlsInfo {
                        peer_certificate: chain.first().map(|certificate| certificate.to_vec()),
                    });
                }
            }
        }
        let no_body = head_request || matches!(response.status().as_u16(), 204 | 205 | 304);
        let declared = content_length(response.headers())?;
        let remaining = if no_body { Some(0) } else { declared };
        let body = Incoming {
            inner: recv,
            remaining,
            send_rx: rx,
            send_done: upload_done,
            reading_trailers: false,
            done: false,
            _upload: upload,
            _connection: self.inner.clone(),
        };
        Ok(response.map(|_| crate::async_impl::body::boxed(body)))
    }
}

pub(crate) fn can_send_early(request: &Request<Body>) -> bool {
    use http_body::Body as _;
    matches!(*request.method(), http::Method::GET | http::Method::HEAD)
        && request.body().is_end_stream()
        && !request.headers().iter().any(|(name, value)| {
            value.is_sensitive()
                || matches!(
                    name.as_str(),
                    "authorization" | "proxy-authorization" | "cookie" | "x-api-key"
                )
        })
}

fn clone_empty_request(request: &Request<Body>) -> Request<Body> {
    let mut replay = Request::new(Body::empty());
    *replay.method_mut() = request.method().clone();
    *replay.uri_mut() = request.uri().clone();
    *replay.version_mut() = request.version();
    *replay.headers_mut() = request.headers().clone();
    *replay.extensions_mut() = request.extensions().clone();
    replay
}

/// Cancelling a request or dropping its response also cancels source-body reads.
struct UploadTask(tokio::task::AbortHandle);

impl Drop for UploadTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct SendGuard {
    stream: h3::client::RequestStream<h3_quinn::SendStream<Bytes>, Bytes>,
    finished: bool,
}

impl Drop for SendGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.stream
                .stop_stream(h3::error::Code::H3_REQUEST_CANCELLED);
        }
    }
}

async fn send_body(mut send: SendGuard, body: Body, expected: Option<u64>) -> Result<(), BoxError> {
    use http_body::Body as _;
    let mut body = Box::pin(body);
    let mut remaining = expected;
    let mut trailers = None;
    loop {
        let frame = tokio::select! {
            stopped = std::future::poll_fn(|cx| send.stream.poll_stopped(cx)) => {
                match stopped {
                    Ok(()) => {},
                    Err(error) if is_stop_sending(&error) => {},
                    Err(error) => return Err(error.into()),
                }
                send.finished = true;
                return Ok(());
            }
            frame = std::future::poll_fn(|cx| body.as_mut().poll_frame(cx)) => frame,
        };
        let Some(frame) = frame else {
            break;
        };
        match frame?.into_data() {
            Ok(data) => {
                if trailers.is_some() {
                    return Err("request DATA after trailers".into());
                }
                if let Some(length) = remaining.as_mut() {
                    *length = length
                        .checked_sub(data.len() as u64)
                        .ok_or("request body exceeds Content-Length")?;
                }
                if let Err(error) = send.stream.send_data(data).await {
                    if is_stop_sending(&error) {
                        send.finished = true;
                        return Ok(());
                    }
                    return Err(error.into());
                }
            }
            Err(frame) => {
                if let Ok(headers) = frame.into_trailers() {
                    validate_trailers(&headers)?;
                    if trailers.replace(headers).is_some() {
                        return Err("multiple request trailer sections".into());
                    }
                }
            }
        }
    }
    if remaining.is_some_and(|length| length != 0) {
        return Err("request body shorter than Content-Length".into());
    }
    if let Some(trailers) = trailers {
        send.stream.send_trailers(trailers).await?;
    }
    if let Err(error) = send.stream.finish().await {
        if !is_stop_sending(&error) {
            return Err(error.into());
        }
    }
    send.finished = true;
    Ok(())
}

fn content_length(headers: &http::HeaderMap) -> Result<Option<u64>, BoxError> {
    let mut expected = None;
    for value in headers.get_all(http::header::CONTENT_LENGTH) {
        for part in value.to_str()?.split(',') {
            let part = part.trim();
            if part.is_empty() || !part.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err("invalid Content-Length".into());
            }
            let length = part.parse::<u64>()?;
            if expected.is_some_and(|expected| expected != length) {
                return Err("conflicting Content-Length values".into());
            }
            expected = Some(length);
        }
    }
    Ok(expected)
}

fn validate_trailers(headers: &http::HeaderMap) -> Result<(), BoxError> {
    if headers.contains_key(http::header::CONTENT_LENGTH)
        || headers.contains_key(http::header::TRANSFER_ENCODING)
    {
        return Err("framing headers are forbidden in trailers".into());
    }
    Ok(())
}

#[derive(Clone)]
pub struct PoolConnection {
    // This receives errors from polling h3 driver.
    close_rx: Arc<Mutex<Receiver<h3::error::ConnectionError>>>,
    client: PoolClient,
    idle_timeout: Instant,
}

impl PoolConnection {
    pub fn new(client: PoolClient, close_rx: Receiver<h3::error::ConnectionError>) -> Self {
        Self {
            close_rx: Arc::new(Mutex::new(close_rx)),
            client,
            idle_timeout: Instant::now(),
        }
    }

    pub fn pool(&mut self) -> PoolClient {
        self.idle_timeout = Instant::now();
        self.client.clone()
    }

    pub fn is_invalid(&self) -> bool {
        if self.client.info.early.rejected() || !self.client.inner.is_available() {
            return true;
        }
        match self.close_rx.lock().unwrap().try_recv() {
            Err(TryRecvError::Empty) => false,
            Err(TryRecvError::Disconnected) => true,
            Ok(_) => true,
        }
    }
}

struct Incoming<S: h3::quic::RecvStream, B: Buf> {
    inner: h3::client::RequestStream<S, B>,
    remaining: Option<u64>,
    send_rx: oneshot::Receiver<Result<(), BoxError>>,
    send_done: bool,
    reading_trailers: bool,
    done: bool,
    _upload: UploadTask,
    _connection: SendRequest<OpenStreams, Bytes>,
}

impl<S: h3::quic::RecvStream, B: Buf> Drop for Incoming<S, B> {
    fn drop(&mut self) {
        if !self.done {
            self.inner
                .stop_sending(h3::error::Code::H3_REQUEST_CANCELLED);
        }
    }
}

impl<S: h3::quic::RecvStream, B: Buf> Incoming<S, B> {
    fn fail(
        &mut self,
        error: impl Into<BoxError>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, crate::Error>>> {
        self.done = true;
        self.inner.stop_sending(h3::error::Code::H3_MESSAGE_ERROR);
        Poll::Ready(Some(Err(crate::error::body(error))))
    }
}

impl<S, B> http_body::Body for Incoming<S, B>
where
    S: h3::quic::RecvStream,
    B: Buf,
{
    type Data = Bytes;
    type Error = crate::error::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        if !this.send_done {
            match Pin::new(&mut this.send_rx).poll(cx) {
                Poll::Ready(Ok(Err(error))) => return this.fail(error),
                Poll::Ready(Err(_)) => return this.fail("HTTP/3 upload task terminated"),
                Poll::Ready(Ok(Ok(()))) => this.send_done = true,
                Poll::Pending => {}
            }
        }
        if !this.reading_trailers {
            match futures_core::ready!(this.inner.poll_recv_data(cx)) {
                Ok(Some(mut data)) => {
                    if let Some(remaining) = this.remaining.as_mut() {
                        let Some(next) = remaining.checked_sub(data.remaining() as u64) else {
                            return this.fail("response body exceeds Content-Length");
                        };
                        *remaining = next;
                    }
                    return Poll::Ready(Some(Ok(http_body::Frame::data(
                        data.copy_to_bytes(data.remaining()),
                    ))));
                }
                Ok(None) => {
                    if this.remaining.is_some_and(|remaining| remaining != 0) {
                        return this.fail("response body shorter than Content-Length");
                    }
                    this.reading_trailers = true;
                }
                Err(error) => return this.fail(error),
            }
        }
        match futures_core::ready!(this.inner.poll_recv_trailers(cx)) {
            Ok(trailers) => {
                this.done = true;
                match trailers {
                    Some(trailers) => match validate_trailers(&trailers) {
                        Ok(()) => Poll::Ready(Some(Ok(http_body::Frame::trailers(trailers)))),
                        Err(error) => this.fail(error),
                    },
                    None => Poll::Ready(None),
                }
            }
            Err(error) => this.fail(error),
        }
    }

    fn is_end_stream(&self) -> bool {
        self.done
    }

    fn size_hint(&self) -> http_body::SizeHint {
        if self.done {
            return http_body::SizeHint::with_exact(0);
        }
        self.remaining
            .map(http_body::SizeHint::with_exact)
            .unwrap_or_default()
    }
}

pub(crate) fn extract_domain(uri: &Uri) -> Result<Key, Error> {
    let uri_clone = uri.clone();
    match (uri_clone.scheme(), uri_clone.authority()) {
        (Some(scheme), Some(auth)) => {
            let scheme_str = scheme.as_str();
            if scheme_str != "https" && scheme_str != "h3" {
                return Err(Error::new(
                    Kind::Request,
                    Some(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "HTTP/3 only supports 'https' or 'h3' schemes, got: {}",
                            scheme_str
                        ),
                    ))),
                ));
            }
            Ok((scheme.clone(), auth.clone(), auth.clone(), None))
        }
        _ => Err(Error::new(Kind::Request, None::<Error>)),
    }
}

pub(crate) fn domain_as_uri((scheme, auth, _, _): Key) -> Uri {
    http::uri::Builder::new()
        .scheme(scheme)
        .authority(auth)
        .path_and_query("/")
        .build()
        .expect("domain is valid Uri")
}

/// Indicates the remote requested the peer to stop sending data without error.
fn is_stop_sending(e: &h3::error::StreamError) -> bool {
    matches!(
        e,
        h3::error::StreamError::RemoteTerminate {
            code: h3::error::Code::H3_NO_ERROR,
            ..
        }
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn early_data_policy_excludes_bodies_and_credentials() {
        let mut request = Request::new(Body::empty());
        assert!(can_send_early(&request));
        for name in [
            "authorization",
            "cookie",
            "proxy-authorization",
            "x-api-key",
        ] {
            request
                .headers_mut()
                .insert(name, http::HeaderValue::from_static("secret"));
            assert!(!can_send_early(&request));
            request.headers_mut().remove(name);
        }
        let mut sensitive = http::HeaderValue::from_static("secret");
        sensitive.set_sensitive(true);
        request.headers_mut().insert("x-custom", sensitive);
        assert!(!can_send_early(&request));
        request.headers_mut().clear();
        *request.method_mut() = http::Method::POST;
        assert!(!can_send_early(&request));
        *request.method_mut() = http::Method::GET;
        *request.body_mut() = Body::from("payload");
        assert!(!can_send_early(&request));
    }

    #[test]
    fn an_old_connecting_lock_cannot_release_a_new_network_attempt() {
        let pool = Pool::new(None);
        let key = extract_domain(&"https://origin.test/".parse().unwrap()).unwrap();
        let Connecting::Acquired(old) = pool.connecting(&key) else {
            panic!("first attempt must own the lock");
        };
        pool.clear();
        let Connecting::Acquired(current) = pool.connecting(&key) else {
            panic!("network change must release old waiters");
        };
        drop(old);
        assert!(matches!(pool.connecting(&key), Connecting::InProgress(_)));
        drop(current);
        assert!(matches!(pool.connecting(&key), Connecting::Acquired(_)));
    }
}
