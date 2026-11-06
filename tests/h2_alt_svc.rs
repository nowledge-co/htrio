#![cfg(all(feature = "http3", feature = "http2"))]

#[path = "support/h3_server.rs"]
mod h3_server;

use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
    let mut wire = vec![
        ((payload.len() >> 16) & 255) as u8,
        ((payload.len() >> 8) & 255) as u8,
        (payload.len() & 255) as u8,
        kind,
        flags,
    ];
    wire.extend_from_slice(&stream.to_be_bytes());
    wire.extend_from_slice(payload);
    wire
}

#[tokio::test]
async fn authenticated_http2_altsvc_frame_upgrades_a_later_request() {
    let server = h3_server::Server::new(|request, mut stream| async move {
        assert_eq!(request.uri().host(), Some("origin.test"));
        assert!(request.headers().contains_key("alt-used"));
        stream.send_response(http::Response::new(())).await.unwrap();
        stream
            .send_data(Bytes::from_static(b"via h3"))
            .await
            .unwrap();
        stream.finish().await.unwrap();
    })
    .await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let origin = format!("https://origin.test:{}", address.port());
    let mut tls = server.tls.clone();
    tls.alpn_protocols = vec![b"h2".to_vec()];
    tls.max_early_data_size = 0;
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(tls));
    let advertised = format!("h3=\"alternative.test:{}\";ma=60", server.address.port());
    let advertised_origin = origin.clone();
    let task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut socket = acceptor.accept(socket).await.unwrap();
        let mut preface = [0; 24];
        socket.read_exact(&mut preface).await.unwrap();
        assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
        socket.write_all(&frame(4, 0, 0, &[])).await.unwrap();
        socket.flush().await.unwrap();
        loop {
            let mut header = [0; 9];
            socket.read_exact(&mut header).await.unwrap();
            let length = (usize::from(header[0]) << 16)
                | (usize::from(header[1]) << 8)
                | usize::from(header[2]);
            assert!(length <= 16384);
            let mut payload = vec![0; length];
            socket.read_exact(&mut payload).await.unwrap();
            let stream = u32::from_be_bytes(header[5..9].try_into().unwrap()) & 0x7fff_ffff;
            if header[3] == 4 && header[4] & 1 == 0 {
                socket.write_all(&frame(4, 1, 0, &[])).await.unwrap();
            }
            if header[3] == 1 {
                let mut alternative = (advertised_origin.len() as u16).to_be_bytes().to_vec();
                alternative.extend_from_slice(advertised_origin.as_bytes());
                alternative.extend_from_slice(advertised.as_bytes());
                socket
                    .write_all(&frame(0x0a, 0, 0, &alternative))
                    .await
                    .unwrap();
                socket
                    .write_all(&frame(1, 5, stream, &[0x88]))
                    .await
                    .unwrap();
                socket.flush().await.unwrap();
                break;
            }
        }
        let _ = tokio::io::copy(&mut socket, &mut tokio::io::sink()).await;
    });
    let cache = Arc::new(htrio::alt_svc::AltSvcCache::new());
    let client = htrio::Client::builder()
        .no_proxy()
        .local_address(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST))
        .tls_certs_only([htrio::Certificate::from_der(server.certificate.as_ref()).unwrap()])
        .resolve("origin.test", address)
        .resolve("alternative.test", server.address)
        .alt_svc_cache(cache.clone())
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let response = client.get(format!("{origin}/seed")).send().await.unwrap();
    assert_eq!(response.version(), http::Version::HTTP_2);
    assert!(!response.headers().contains_key("alt-svc"));
    response.bytes().await.unwrap();
    assert_eq!(cache.len(), 1);
    let response = client
        .get(format!("{origin}/upgraded"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.version(), http::Version::HTTP_3);
    assert_eq!(response.text().await.unwrap(), "via h3");
    task.abort();
}
