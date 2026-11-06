#![allow(dead_code)]

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::Request;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinHandle, JoinSet};

pub type Stream = h3::server::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>;
type Handler =
    Arc<dyn Fn(Request<()>, Stream) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

pub struct Server {
    pub address: std::net::SocketAddr,
    pub certificate: CertificateDer<'static>,
    pub connections: Arc<AtomicUsize>,
    endpoint: quinn::Endpoint,
    task: JoinHandle<()>,
    goaway: mpsc::UnboundedSender<oneshot::Sender<()>>,
}

impl Server {
    pub async fn new<F, Fut>(handler: F) -> Self
    where
        F: Fn(Request<()>, Stream) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let identity = rcgen::generate_simple_self_signed(vec!["origin.test".into()]).unwrap();
        let certificate = identity.cert.der().clone();
        let key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(
            identity.signing_key.serialize_der(),
        ));
        let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![certificate.clone()], key)
        .unwrap();
        tls.alpn_protocols = vec![b"h3".to_vec()];
        let configuration = quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap(),
        ));
        let endpoint =
            quinn::Endpoint::server(configuration, "127.0.0.1:0".parse().unwrap()).unwrap();
        let address = endpoint.local_addr().unwrap();
        let listener = endpoint.clone();
        let connections = Arc::new(AtomicUsize::new(0));
        let accepted = connections.clone();
        let handler: Handler = Arc::new(move |request, stream| Box::pin(handler(request, stream)));
        let (goaway, goaway_rx) = mpsc::unbounded_channel::<oneshot::Sender<()>>();
        let task = tokio::spawn(async move {
            // The first connection accepts the explicit GOAWAY control command.
            let mut control = Some(goaway_rx);
            let mut sessions = JoinSet::new();
            while let Some(incoming) = listener.accept().await {
                let handler = handler.clone();
                let accepted = accepted.clone();
                let mut control = control.take();
                sessions.spawn(async move {
                    let connection = incoming.await.unwrap();
                    accepted.fetch_add(1, Ordering::SeqCst);
                    let mut server = h3::server::builder().build(h3_quinn::Connection::new(connection)).await.unwrap();
                    let mut streams = JoinSet::new();
                    loop {
                        tokio::select! {
                            request = server.accept() => {
                                let Ok(Some(resolver)) = request else { break; };
                                let handler = handler.clone();
                                streams.spawn(async move {
                                    if let Ok((request, stream)) = resolver.resolve_request().await {
                                        handler(request, stream).await;
                                    }
                                });
                            }
                            command = async {
                                match control.as_mut() {
                                    Some(receiver) => receiver.recv().await,
                                    None => std::future::pending().await,
                                }
                            } => {
                                if let Some(complete) = command {
                                    server.shutdown(0).await.unwrap();
                                    let _ = complete.send(());
                                } else { control = None; }
                            }
                            Some(result) = streams.join_next() => { result.unwrap(); }
                        }
                    }
                });
            }
        });
        Self {
            address,
            certificate,
            connections,
            endpoint,
            task,
            goaway,
        }
    }

    pub fn client_builder(&self) -> htrio::ClientBuilder {
        htrio::Client::builder()
            .no_proxy()
            .http3_prior_knowledge()
            .local_address(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST))
            .tls_certs_only([htrio::Certificate::from_der(self.certificate.as_ref()).unwrap()])
            .resolve("origin.test", self.address)
            .timeout(Duration::from_secs(5))
    }

    pub fn url(&self, path: &str) -> String {
        format!("https://origin.test:{}{path}", self.address.port())
    }

    pub async fn goaway(&self) {
        let (tx, rx) = oneshot::channel();
        self.goaway.send(tx).unwrap();
        tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .unwrap()
            .unwrap();
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.endpoint.close(0u32.into(), b"test finished");
        self.task.abort();
    }
}
