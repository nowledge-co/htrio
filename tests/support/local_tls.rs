#![allow(dead_code)]

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{Buf, Bytes};
use http::{Request, Response};
use http_body_util::{BodyExt, Full};
use hyper_util::rt::{TokioExecutor, TokioIo};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::task::{JoinHandle, JoinSet};

type Handler = Arc<dyn Fn(Request<Bytes>, u16) -> Response<Bytes> + Send + Sync>;

pub struct LocalServer {
    pub addr: SocketAddr,
    pub quic_addr: SocketAddr,
    pub cert: CertificateDer<'static>,
    pub abort_quic: Arc<AtomicBool>,
    pub reject_once: Arc<AtomicBool>,
    seen: Arc<Mutex<Vec<Request<Bytes>>>>,
    endpoint: quinn::Endpoint,
    tasks: Vec<JoinHandle<()>>,
}

impl LocalServer {
    pub async fn new(
        handler: impl Fn(Request<Bytes>, u16) -> Response<Bytes> + Send + Sync + 'static,
    ) -> Self {
        Self::with_names(&["origin.test"], handler).await
    }

    pub async fn with_names(
        names: &[&str],
        handler: impl Fn(Request<Bytes>, u16) -> Response<Bytes> + Send + Sync + 'static,
    ) -> Self {
        Self::with_versions(
            names,
            &[&rustls::version::TLS13, &rustls::version::TLS12],
            handler,
        )
        .await
    }

    pub async fn with_versions(
        names: &[&str],
        versions: &[&'static rustls::SupportedProtocolVersion],
        handler: impl Fn(Request<Bytes>, u16) -> Response<Bytes> + Send + Sync + 'static,
    ) -> Self {
        let identity = rcgen::generate_simple_self_signed(
            names
                .iter()
                .map(|name| name.to_string())
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let cert = identity.cert.der().clone();
        let key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(
            identity.signing_key.serialize_der(),
        ));
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let config = |versions: &[&'static rustls::SupportedProtocolVersion]| {
            rustls::ServerConfig::builder_with_provider(provider.clone())
                .with_protocol_versions(versions)
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(vec![cert.clone()], key.clone_key())
                .unwrap()
        };
        let mut quic_tls = config(&[&rustls::version::TLS13]);
        quic_tls.alpn_protocols = vec![b"h3".to_vec()];
        let quic_config = quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(quic_tls).unwrap(),
        ));
        let endpoint =
            quinn::Endpoint::server(quic_config, "127.0.0.1:0".parse().unwrap()).unwrap();
        let quic_addr = endpoint.local_addr().unwrap();

        let mut tcp_tls = config(versions);
        tcp_tls.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(tcp_tls));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handler: Handler = Arc::new(handler);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let abort_quic = Arc::new(AtomicBool::new(false));
        let reject_once = Arc::new(AtomicBool::new(false));
        let tcp_task = {
            let handler = handler.clone();
            let seen = seen.clone();
            tokio::spawn(async move {
                let mut connections = JoinSet::new();
                loop {
                    tokio::select! {
                        accepted = listener.accept() => {
                            let (stream, _) = accepted.unwrap();
                            let acceptor = acceptor.clone();
                            let handler = handler.clone();
                            let seen = seen.clone();
                            connections.spawn(async move {
                                let Ok(stream) = acceptor.accept(stream).await else { return; };
                                let service = hyper::service::service_fn(move |request: Request<hyper::body::Incoming>| {
                                    let handler = handler.clone();
                                    let seen = seen.clone();
                                    async move {
                                        let (parts, body) = request.into_parts();
                                        let body = body.collect().await.unwrap().to_bytes();
                                        let request = Request::from_parts(parts, body);
                                        seen.lock().unwrap().push(request.clone());
                                        Ok::<_, Infallible>(handler(request, quic_addr.port()).map(Full::new))
                                    }
                                });
                                let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                                    .serve_connection(TokioIo::new(stream), service).await;
                            });
                        }
                        Some(result) = connections.join_next() => { result.unwrap(); }
                    }
                }
            })
        };
        let quic_task = {
            let endpoint = endpoint.clone();
            let seen = seen.clone();
            let abort_quic = abort_quic.clone();
            let reject_once = reject_once.clone();
            tokio::spawn(async move {
                let mut connections = JoinSet::new();
                loop {
                    tokio::select! {
                        Some(incoming) = endpoint.accept() => {
                            let handler = handler.clone();
                            let seen = seen.clone();
                            let abort_quic = abort_quic.clone();
                            let reject_once = reject_once.clone();
                            connections.spawn(async move {
                                let Ok(connection) = incoming.await else { return; };
                                let Ok(mut connection) = h3::server::Connection::new(h3_quinn::Connection::new(connection)).await else { return; };
                                let mut streams = JoinSet::new();
                                loop {
                                    tokio::select! {
                                        request = connection.accept() => {
                                            let Ok(Some(resolver)) = request else { break; };
                                            let handler = handler.clone();
                                            let seen = seen.clone();
                                            let abort_quic = abort_quic.clone();
                                            let reject_once = reject_once.clone();
                                            streams.spawn(async move {
                                                let Ok((request, mut stream)) = resolver.resolve_request().await else { return; };
                                                let mut body = Vec::new();
                                                while let Ok(Some(mut data)) = stream.recv_data().await {
                                                    body.extend_from_slice(&data.copy_to_bytes(data.remaining()));
                                                }
                                                let request = request.map(|()| Bytes::from(body));
                                                seen.lock().unwrap().push(request.clone());
                                                if reject_once.swap(false, Ordering::SeqCst) {
                                                    stream.stop_stream(h3::error::Code::H3_REQUEST_REJECTED);
                                                    return;
                                                }
                                                if abort_quic.load(Ordering::SeqCst) {
                                                    stream.stop_stream(h3::error::Code::H3_INTERNAL_ERROR);
                                                    return;
                                                }
                                                let (parts, body) = handler(request, quic_addr.port()).into_parts();
                                                if stream.send_response(Response::from_parts(parts, ())).await.is_err() { return; }
                                                if !body.is_empty() && stream.send_data(body).await.is_err() { return; }
                                                let _ = stream.finish().await;
                                            });
                                        }
                                        Some(result) = streams.join_next() => { result.unwrap(); }
                                    }
                                }
                            });
                        }
                        Some(result) = connections.join_next() => { result.unwrap(); }
                    }
                }
            })
        };
        Self {
            addr,
            quic_addr,
            cert,
            abort_quic,
            reject_once,
            seen,
            endpoint,
            tasks: vec![tcp_task, quic_task],
        }
    }

    pub fn url(&self, path: &str) -> String {
        format!("https://origin.test:{}{path}", self.addr.port())
    }

    pub fn certificate(&self) -> htrio::Certificate {
        htrio::Certificate::from_der(self.cert.as_ref()).unwrap()
    }

    pub fn client_builder(&self) -> htrio::ClientBuilder {
        htrio::Client::builder()
            .no_proxy()
            .tls_certs_only([self.certificate()])
            .resolve("origin.test", self.addr)
            .resolve("alternative.test", self.quic_addr)
            .local_address(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST))
            .timeout(Duration::from_secs(5))
    }

    pub fn requests(&self) -> Vec<Request<Bytes>> {
        self.seen.lock().unwrap().clone()
    }
}

impl Drop for LocalServer {
    fn drop(&mut self) {
        self.endpoint.close(0_u32.into(), b"test complete");
        for task in &self.tasks {
            task.abort();
        }
    }
}

pub fn advertise(request: Request<Bytes>, port: u16) -> Response<Bytes> {
    Response::builder()
        .header(
            "alt-svc",
            format!("h3=\"alternative.test:{port}\"; ma=3600"),
        )
        .body(request.into_body())
        .unwrap()
}
