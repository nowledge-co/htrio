#![cfg(feature = "http3")]

#[path = "support/h3_server.rs"]
mod h3_server;

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Buf, Bytes};
use h3_server::Server;
use http::{HeaderMap, HeaderValue, Response, StatusCode};
use http_body_util::{BodyExt, StreamBody};
use tokio::sync::oneshot;

#[tokio::test]
async fn informational_responses_precede_final_response() {
    let server = Server::new(|_, mut stream| async move {
        for status in [100, 103, 200] {
            stream
                .send_response(Response::builder().status(status).body(()).unwrap())
                .await
                .unwrap();
        }
        stream
            .send_data(Bytes::from_static(b"final"))
            .await
            .unwrap();
        stream.finish().await.unwrap();
    })
    .await;
    let response = server
        .client_builder()
        .build()
        .unwrap()
        .get(server.url("/"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await.unwrap(), "final");
}

#[tokio::test]
async fn switching_protocols_is_forbidden() {
    let server = Server::new(|_, mut stream| async move {
        let _ = stream
            .send_response(Response::builder().status(101).body(()).unwrap())
            .await;
        let _ = stream.finish().await;
    })
    .await;
    assert!(server
        .client_builder()
        .build()
        .unwrap()
        .get(server.url("/"))
        .send()
        .await
        .is_err());
}

#[tokio::test]
async fn request_and_response_trailers_survive_body_adaptation() {
    let server = Server::new(|_, mut stream| async move {
        let mut bytes = Vec::new();
        while let Some(mut data) = stream.recv_data().await.unwrap() {
            bytes.extend_from_slice(&data.copy_to_bytes(data.remaining()));
        }
        assert_eq!(bytes, b"payload");
        let trailers = stream.recv_trailers().await.unwrap().unwrap();
        assert_eq!(trailers["upload-checksum"], "verified");
        stream.send_response(Response::new(())).await.unwrap();
        stream
            .send_data(Bytes::from_static(b"response"))
            .await
            .unwrap();
        let mut trailers = HeaderMap::new();
        trailers.insert("grpc-status", HeaderValue::from_static("0"));
        stream.send_trailers(trailers).await.unwrap();
        stream.finish().await.unwrap();
    })
    .await;
    let mut trailers = HeaderMap::new();
    trailers.insert("upload-checksum", HeaderValue::from_static("verified"));
    let frames = futures_util::stream::iter([
        Ok::<_, std::io::Error>(http_body::Frame::data(Bytes::from_static(b"payload"))),
        Ok(http_body::Frame::trailers(trailers)),
    ]);
    let response = server
        .client_builder()
        .build()
        .unwrap()
        .post(server.url("/"))
        .body(htrio::Body::wrap(StreamBody::new(frames)))
        .send()
        .await
        .unwrap();
    let response: http::Response<htrio::Body> = response.into();
    let collected = response.into_body().collect().await.unwrap();
    assert_eq!(collected.trailers().unwrap()["grpc-status"], "0");
    assert_eq!(collected.to_bytes(), "response");
}

#[tokio::test]
async fn short_and_long_response_bodies_fail_content_length_validation() {
    for body in [b"abc".as_slice(), b"abcdef".as_slice()] {
        let body = Bytes::copy_from_slice(body);
        let server = Server::new(move |_, mut stream| {
            let body = body.clone();
            async move {
                stream
                    .send_response(
                        Response::builder()
                            .header("content-length", "5")
                            .body(())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                let _ = stream.send_data(body).await;
                let _ = stream.finish().await;
            }
        })
        .await;
        let response = server
            .client_builder()
            .build()
            .unwrap()
            .get(server.url("/"))
            .send()
            .await
            .unwrap();
        assert!(response.bytes().await.is_err());
    }
}

#[tokio::test]
async fn head_preserves_nonzero_content_length_without_requiring_a_body() {
    let server = Server::new(|_, mut stream| async move {
        stream
            .send_response(
                Response::builder()
                    .header("content-length", "123")
                    .body(())
                    .unwrap(),
            )
            .await
            .unwrap();
        stream.finish().await.unwrap();
    })
    .await;
    let response = server
        .client_builder()
        .build()
        .unwrap()
        .head(server.url("/"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.headers()["content-length"], "123");
    assert!(response.bytes().await.unwrap().is_empty());
}

struct PendingBody(Option<oneshot::Sender<()>>);

impl Drop for PendingBody {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}

impl http_body::Body for PendingBody {
    type Data = Bytes;
    type Error = std::io::Error;
    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        Poll::Pending
    }
}

#[tokio::test]
async fn request_timeout_cancels_a_pending_upload_source() {
    let server = Server::new(|_, mut stream| async move {
        let _ = stream.recv_data().await;
    })
    .await;
    let (tx, rx) = oneshot::channel();
    let response = server
        .client_builder()
        .build()
        .unwrap()
        .post(server.url("/"))
        .timeout(Duration::from_millis(100))
        .body(htrio::Body::wrap(PendingBody(Some(tx))))
        .send()
        .await;
    assert!(response.unwrap_err().is_timeout());
    tokio::time::timeout(Duration::from_secs(2), rx)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn dropping_a_response_cancels_pending_upload_and_receive_halves() {
    let observed = Arc::new(Mutex::new(None));
    let (tx, rx) = oneshot::channel();
    *observed.lock().unwrap() = Some(tx);
    let server = Server::new(move |_, mut stream| {
        let observed = observed.clone();
        async move {
            stream.send_response(Response::new(())).await.unwrap();
            let error = match stream.recv_data().await {
                Err(error) => error,
                Ok(_) => panic!("cancelled upload unexpectedly ended successfully"),
            };
            if let Some(tx) = observed.lock().unwrap().take() {
                let _ = tx.send(format!("{error:?}"));
            }
        }
    })
    .await;
    let (tx, body_dropped) = oneshot::channel();
    let client = server.client_builder().build().unwrap();
    let response = client
        .post(server.url("/"))
        .body(htrio::Body::wrap(PendingBody(Some(tx))))
        .send()
        .await
        .unwrap();
    drop(response);
    tokio::time::timeout(Duration::from_secs(2), body_dropped)
        .await
        .unwrap()
        .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(2), rx)
        .await
        .unwrap()
        .unwrap();
    assert!(error.contains("H3_REQUEST_CANCELLED"), "{error}");
}

#[tokio::test]
async fn peer_stop_sending_releases_a_pending_source_before_response_eof() {
    let (release, wait) = tokio::sync::watch::channel(false);
    let server = Server::new(move |_, mut stream| {
        let mut wait = wait.clone();
        async move {
            stream.stop_sending(h3::error::Code::H3_NO_ERROR);
            stream.send_response(Response::new(())).await.unwrap();
            wait.wait_for(|released| *released).await.unwrap();
            stream.finish().await.unwrap();
        }
    })
    .await;
    let (tx, dropped) = oneshot::channel();
    let response = server
        .client_builder()
        .build()
        .unwrap()
        .post(server.url("/"))
        .body(htrio::Body::wrap(PendingBody(Some(tx))))
        .send()
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), dropped)
        .await
        .unwrap()
        .unwrap();
    release.send(true).unwrap();
    assert!(response.bytes().await.unwrap().is_empty());
}

#[tokio::test]
async fn goaway_retires_the_connection_before_reusing_it() {
    use std::sync::atomic::Ordering;
    let server = Server::new(|_, mut stream| async move {
        while stream.recv_data().await.unwrap().is_some() {}
        stream.send_response(Response::new(())).await.unwrap();
        stream.finish().await.unwrap();
    })
    .await;
    let client = server.client_builder().build().unwrap();
    client
        .get(server.url("/first"))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    server.goaway().await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while client.http3_pool_stats().available != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let body = futures_util::stream::once(async {
        Ok::<_, std::io::Error>(Bytes::from_static(b"single dispatch"))
    });
    client
        .post(server.url("/after-goaway"))
        .body(htrio::Body::wrap_stream(body))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(server.connections.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn informational_response_count_is_bounded() {
    let server = Server::new(|_, mut stream| async move {
        for _ in 0..65 {
            if stream
                .send_response(Response::builder().status(103).body(()).unwrap())
                .await
                .is_err()
            {
                return;
            }
        }
        let _ = stream.finish().await;
    })
    .await;
    let error = server
        .client_builder()
        .build()
        .unwrap()
        .get(server.url("/"))
        .send()
        .await
        .unwrap_err();
    assert!(format!("{error:?}").contains("too many HTTP/3 informational"));
}

#[tokio::test]
async fn preconnect_rebind_and_network_change_preserve_protocol_boundaries() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let server = Server::new(move |_, mut stream| {
        let calls = observed.clone();
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            stream.send_response(Response::new(())).await.unwrap();
            stream.finish().await.unwrap();
        }
    })
    .await;
    let client = server.client_builder().tls_info(true).build().unwrap();
    client.preconnect(server.url("/")).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(client.http3_pool_stats().available, 1);
    let response = client.get(server.url("/first")).send().await.unwrap();
    assert_eq!(response.remote_addr(), Some(server.address));
    assert!(response
        .extensions()
        .get::<htrio::tls::TlsInfo>()
        .unwrap()
        .peer_certificate()
        .is_some());
    let info = response
        .extensions()
        .get::<htrio::http3::ConnectionInfo>()
        .unwrap()
        .clone();
    assert!(info.stats().udp_rx.datagrams > 0);
    response.bytes().await.unwrap();
    client
        .http3_rebind(std::net::UdpSocket::bind("127.0.0.1:0").unwrap())
        .unwrap();
    let response = client.get(server.url("/migrated")).send().await.unwrap();
    assert_eq!(
        response
            .extensions()
            .get::<htrio::http3::ConnectionInfo>()
            .unwrap()
            .id(),
        info.id()
    );
    response.bytes().await.unwrap();
    client.network_changed();
    assert_eq!(client.http3_pool_stats().available, 0);
    let response = client.get(server.url("/new-network")).send().await.unwrap();
    assert_ne!(
        response
            .extensions()
            .get::<htrio::http3::ConnectionInfo>()
            .unwrap()
            .id(),
        info.id()
    );
    response.bytes().await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn resolved_addresses_race_past_a_silent_first_candidate() {
    let server = Server::new(|_, mut stream| async move {
        stream.send_response(Response::new(())).await.unwrap();
        stream.finish().await.unwrap();
    })
    .await;
    let silent = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let client = server
        .client_builder()
        .resolve_to_addrs(
            "origin.test",
            &[silent.local_addr().unwrap(), server.address],
        )
        .build()
        .unwrap();
    let response = tokio::time::timeout(
        Duration::from_secs(2),
        client.get("https://origin.test/").send(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(response.remote_addr(), Some(server.address));
    response.bytes().await.unwrap();
}

#[tokio::test]
async fn priority_headers_and_live_updates_keep_the_response_stream_usable() {
    let server = Server::new(|request, mut stream| async move {
        assert_eq!(request.headers()["priority"], "u=1, i");
        stream.send_response(Response::new(())).await.unwrap();
        stream
            .send_data(Bytes::from_static(b"priority"))
            .await
            .unwrap();
        stream.finish().await.unwrap();
    })
    .await;
    let client = server.client_builder().build().unwrap();
    let response = client
        .get(server.url("/"))
        .priority(htrio::http3::Priority::new(1, true).unwrap())
        .send()
        .await
        .unwrap();
    let handle = response
        .extensions()
        .get::<htrio::http3::RequestPriority>()
        .unwrap();
    assert!(handle.update(htrio::http3::Priority::new(0, false).unwrap()));
    assert_eq!(response.text().await.unwrap(), "priority");
}

struct ServiceResolver {
    address: std::net::SocketAddr,
    ech: bool,
}
impl htrio::dns::Resolve for ServiceResolver {
    fn resolve(&self, _: htrio::dns::Name) -> htrio::dns::Resolving {
        let addresses: htrio::dns::Addrs = Box::new([self.address].into_iter());
        Box::pin(std::future::ready(Ok(addresses)))
    }
    fn resolve_service(&self, name: htrio::dns::Name, _: u16) -> htrio::dns::ServiceResolving {
        assert_eq!(name.as_str(), "origin.test");
        let mut endpoint = htrio::dns::ServiceEndpoint::http3("service.test", self.address.port());
        if self.ech {
            endpoint.ech_config_list = Some(vec![0, 0]);
        }
        Box::pin(std::future::ready(Ok(vec![endpoint])))
    }
}

#[tokio::test]
async fn custom_https_metadata_selects_h3_without_alt_svc_and_keeps_origin_identity() {
    let server = Server::new(|request, mut stream| async move {
        assert_eq!(request.uri().host(), Some("origin.test"));
        assert!(!request.headers().contains_key("alt-used"));
        stream.send_response(Response::new(())).await.unwrap();
        stream.finish().await.unwrap();
    })
    .await;
    let client = htrio::Client::builder()
        .no_proxy()
        .tls_certs_only([htrio::Certificate::from_der(server.certificate.as_ref()).unwrap()])
        .local_address(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST))
        .dns_resolver(Arc::new(ServiceResolver {
            address: server.address,
            ech: false,
        }))
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let response = client.get("https://origin.test/").send().await.unwrap();
    assert_eq!(response.version(), http::Version::HTTP_3);
    assert_eq!(response.remote_addr(), Some(server.address));
    response.bytes().await.unwrap();
}

#[tokio::test]
async fn invalid_dns_ech_never_falls_back_to_a_plaintext_sni_connection() {
    let server =
        Server::new(|_, _| async { panic!("invalid ECH must not dispatch a request") }).await;
    let client = htrio::Client::builder()
        .no_proxy()
        .tls_certs_only([htrio::Certificate::from_der(server.certificate.as_ref()).unwrap()])
        .local_address(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST))
        .dns_resolver(Arc::new(ServiceResolver {
            address: server.address,
            ech: true,
        }))
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let error = client.get("https://origin.test/").send().await.unwrap_err();
    assert!(format!("{error:?}").contains("EncryptedClientHello"));
    assert_eq!(
        server.connections.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
}

#[tokio::test]
async fn dynamic_qpack_headers_and_trailers_share_bounded_connection_state() {
    let server = Server::new(|request, mut stream| async move {
        assert_eq!(
            request.headers()["x-compressible"],
            "repeated-request-metadata"
        );
        stream
            .send_response(
                Response::builder()
                    .header("x-compressible", "repeated-response-metadata")
                    .body(())
                    .unwrap(),
            )
            .await
            .unwrap();
        let mut trailers = HeaderMap::new();
        trailers.insert(
            "x-compressible-trailer",
            HeaderValue::from_static("repeated-trailer-metadata"),
        );
        stream.send_trailers(trailers).await.unwrap();
        stream.finish().await.unwrap();
    })
    .await;
    let client = server
        .client_builder()
        .http3_qpack(4096, 16)
        .build()
        .unwrap();
    let mut info = None;
    for _ in 0..4 {
        let response = client
            .get(server.url("/qpack"))
            .header("x-compressible", "repeated-request-metadata")
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.headers()["x-compressible"],
            "repeated-response-metadata"
        );
        info = response
            .extensions()
            .get::<htrio::http3::ConnectionInfo>()
            .cloned();
        let response: http::Response<htrio::Body> = response.into();
        let collected = response.into_body().collect().await.unwrap();
        assert_eq!(
            collected.trailers().unwrap()["x-compressible-trailer"],
            "repeated-trailer-metadata"
        );
    }
    let stats = info.unwrap().qpack_stats();
    assert!(stats.encoded_inserts > 0);
    assert!(stats.decoded_inserts > 0);
    assert_eq!(stats.blocked_sections, 0);
}
