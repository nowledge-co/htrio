#![cfg(all(feature = "__native-tls", feature = "__rustls"))]

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn native_tls12_uses_custom_roots_dns_and_version_bounds() {
    let certificate = rcgen::generate_simple_self_signed(vec!["origin.test".into()]).unwrap();
    let key = rustls::pki_types::PrivateKeyDer::from(rustls::pki_types::PrivatePkcs8KeyDer::from(
        certificate.signing_key.serialize_der(),
    ));
    let tls = rustls::ServerConfig::builder_with_provider(std::sync::Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS12])
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![certificate.cert.der().clone()], key)
    .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(tls));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut stream = acceptor.accept(socket).await.unwrap();
        let mut request = Vec::new();
        while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
            let mut bytes = [0; 1024];
            let length = stream.read(&mut bytes).await.unwrap();
            assert!(length > 0);
            request.extend_from_slice(&bytes[..length]);
        }
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\nnative")
            .await
            .unwrap();
    });
    let client = htrio::Client::builder()
        .no_proxy()
        .tls_backend_native()
        .tls_certs_only([htrio::Certificate::from_der(certificate.cert.der().as_ref()).unwrap()])
        .tls_version_min(htrio::tls::Version::TLS_1_2)
        .tls_version_max(htrio::tls::Version::TLS_1_2)
        .resolve("origin.test", address)
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    let response = client
        .get(format!("https://origin.test:{}/tls12", address.port()))
        .send()
        .await
        .unwrap();
    assert_eq!(response.version(), htrio::Version::HTTP_11);
    assert_eq!(response.text().await.unwrap(), "native");
    server.await.unwrap();
}
