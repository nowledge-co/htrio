#![cfg(all(
    feature = "http3",
    feature = "http2",
    feature = "cookies",
    feature = "stream"
))]

#[path = "support/local_tls.rs"]
mod local_tls;

use std::net::Ipv4Addr;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use htrio::alt_svc::AltSvcCache;
use http::{Request, Response, StatusCode, Version};
use local_tls::{advertise, LocalServer};

#[derive(Default)]
struct Resolver {
    names: Mutex<Vec<String>>,
}

impl htrio::dns::Resolve for Resolver {
    fn resolve(&self, name: htrio::dns::Name) -> htrio::dns::Resolving {
        self.names.lock().unwrap().push(name.as_str().to_owned());
        let addresses: htrio::dns::Addrs =
            Box::new([std::net::SocketAddr::from((Ipv4Addr::LOCALHOST, 0))].into_iter());
        Box::pin(std::future::ready(Ok(addresses)))
    }
}

#[tokio::test]
async fn upgrades_with_origin_authority_sni_custom_dns_and_cookies() {
    let server = LocalServer::new(|request, port| {
        let mut response = advertise(request, port);
        response.headers_mut().insert(
            "set-cookie",
            "session=origin; Secure; Path=/".parse().unwrap(),
        );
        response
    })
    .await;
    // The certificate covers only origin.test, not alternative.test. Success
    // proves that changing the network host does not change TLS authentication.
    let resolver = Arc::new(Resolver::default());
    let client = htrio::Client::builder()
        .no_proxy()
        .tls_certs_only([server.certificate()])
        .dns_resolver(resolver.clone())
        .cookie_store(true)
        .local_address(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST))
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let first = client.get(server.url("/seed")).send().await.unwrap();
    assert_eq!(first.version(), Version::HTTP_2);
    first.bytes().await.unwrap();
    let response = client
        .post(server.url("/echo"))
        .body("hello")
        .send()
        .await
        .unwrap();
    assert_eq!(response.version(), Version::HTTP_3);
    assert_eq!(response.url().as_str(), server.url("/echo"));
    assert_eq!(response.text().await.unwrap(), "hello");
    let requests = server.requests();
    assert_eq!(
        requests[1].uri().authority().unwrap().as_str(),
        format!("origin.test:{}", server.addr.port())
    );
    assert_eq!(
        requests[1].headers()["alt-used"],
        format!("alternative.test:{}", server.quic_addr.port())
    );
    assert_eq!(requests[1].headers()["cookie"], "session=origin");
    assert_eq!(
        *resolver.names.lock().unwrap(),
        ["origin.test", "alternative.test"]
    );
}

#[tokio::test]
async fn handshake_timeout_falls_back_before_consuming_streaming_post() {
    let blackhole = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = blackhole.local_addr().unwrap().port();
    let server = LocalServer::new(move |request, _| advertise(request, port)).await;
    let client = server
        .client_builder()
        .http3_connect_timeout(Duration::from_millis(50))
        .build()
        .unwrap();
    client
        .get(server.url("/seed"))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let body = htrio::Body::wrap_stream(futures_util::stream::iter([
        Ok::<_, std::io::Error>(Bytes::from_static(b"one")),
        Ok(Bytes::from_static(b"two")),
    ]));
    let response = client
        .post(server.url("/echo"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.version(), Version::HTTP_2);
    assert_eq!(response.text().await.unwrap(), "onetwo");
    let requests = server.requests();
    assert_eq!(
        requests.iter().filter(|req| req.method() == "POST").count(),
        1
    );
    assert!(requests.iter().all(|req| req.version() != Version::HTTP_3));
}

#[tokio::test]
async fn alternative_certificate_must_cover_origin() {
    let alternate = LocalServer::with_names(&["alternative.test"], advertise).await;
    let port = alternate.quic_addr.port();
    let origin = LocalServer::new(move |request, _| advertise(request, port)).await;
    let client = origin
        .client_builder()
        .add_root_certificate(alternate.certificate())
        .build()
        .unwrap();
    client
        .get(origin.url("/seed"))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let response = client
        .post(origin.url("/echo"))
        .body("once")
        .send()
        .await
        .unwrap();
    assert_eq!(response.version(), Version::HTTP_2);
    assert_eq!(response.text().await.unwrap(), "once");
    assert!(alternate.requests().is_empty());
    assert_eq!(origin.requests().len(), 2);
}

fn misdirected(request: Request<Bytes>, port: u16) -> Response<Bytes> {
    if request.uri().path() == "/seed" {
        advertise(request, port)
    } else if request.version() == Version::HTTP_3 {
        Response::builder()
            .status(StatusCode::MISDIRECTED_REQUEST)
            .header("alt-svc", "h3=\":9999\"")
            .body(Bytes::new())
            .unwrap()
    } else {
        Response::new(request.into_body())
    }
}

#[tokio::test]
async fn misdirected_replayable_post_evicts_alternative_and_retries_origin_once() {
    let server = LocalServer::new(misdirected).await;
    let cache = Arc::new(AltSvcCache::new());
    let client = server
        .client_builder()
        .alt_svc_cache(cache.clone())
        .retry(htrio::retry::never())
        .build()
        .unwrap();
    client
        .get(server.url("/seed"))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let response = client
        .post(server.url("/echo"))
        .body("replayable")
        .send()
        .await
        .unwrap();
    assert_eq!(response.version(), Version::HTTP_2);
    assert_eq!(response.text().await.unwrap(), "replayable");
    assert!(cache.is_empty());
    let requests = server.requests();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[1].version(), Version::HTTP_3);
    assert_eq!(requests[2].version(), Version::HTTP_2);
    assert!(!requests[2].headers().contains_key("alt-used"));
    assert_eq!(requests[1].body(), requests[2].body());
}

#[tokio::test]
async fn misdirected_streaming_post_is_not_replayed() {
    let server = LocalServer::new(misdirected).await;
    let cache = Arc::new(AltSvcCache::new());
    let client = server
        .client_builder()
        .alt_svc_cache(cache.clone())
        .build()
        .unwrap();
    client
        .get(server.url("/seed"))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let body = htrio::Body::wrap_stream(futures_util::stream::iter([Ok::<_, std::io::Error>(
        "stream",
    )]));
    let response = client
        .post(server.url("/echo"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::MISDIRECTED_REQUEST);
    assert!(cache.is_empty());
    assert_eq!(server.requests().len(), 2);
}

#[tokio::test]
async fn post_dispatch_failure_never_causes_tcp_replay() {
    let server = LocalServer::new(advertise).await;
    let client = server.client_builder().build().unwrap();
    client
        .get(server.url("/seed"))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    server.abort_quic.store(true, Ordering::SeqCst);
    let error = client
        .post(server.url("/post"))
        .body("side effect")
        .send()
        .await
        .unwrap_err();
    assert!(error.is_request());
    let requests = server.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[1].version(), Version::HTTP_3);
    assert_eq!(requests[1].body(), "side effect");
}

#[tokio::test]
async fn redirects_select_each_origin_and_remove_cross_origin_credentials() {
    let destination = LocalServer::with_names(&["destination.test"], |request, port| {
        let mut response = advertise(request, port);
        // Both names resolve to the local server, but the advertised host is
        // always alternative.test and must not replace destination.test in TLS.
        response.headers_mut().insert(
            "set-cookie",
            "destination=1; Secure; Path=/".parse().unwrap(),
        );
        response
    })
    .await;
    let destination_url = format!("https://destination.test:{}/done", destination.addr.port());
    let origin = LocalServer::new(move |request, port| {
        if request.uri().path() == "/redirect" {
            Response::builder()
                .status(StatusCode::FOUND)
                .header("location", &destination_url)
                .body(Bytes::new())
                .unwrap()
        } else {
            let mut response = advertise(request, port);
            response
                .headers_mut()
                .insert("set-cookie", "source=1; Secure; Path=/".parse().unwrap());
            response
        }
    })
    .await;
    let client = origin
        .client_builder()
        .add_root_certificate(destination.certificate())
        .resolve("destination.test", destination.addr)
        .cookie_store(true)
        .build()
        .unwrap();
    client
        .get(format!(
            "https://destination.test:{}/seed",
            destination.addr.port()
        ))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    client
        .get(origin.url("/seed"))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let response = client
        .get(origin.url("/redirect"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap();
    assert_eq!(response.version(), Version::HTTP_3);
    assert_eq!(response.url().host_str(), Some("destination.test"));
    response.bytes().await.unwrap();
    let source = origin.requests();
    assert_eq!(source[1].version(), Version::HTTP_3);
    let requests = destination.requests();
    let final_request = requests.last().unwrap();
    assert_eq!(final_request.uri().path(), "/done");
    assert_eq!(final_request.version(), Version::HTTP_3);
    assert!(!final_request.headers().contains_key("authorization"));
    assert_eq!(final_request.headers()["cookie"], "destination=1");
    assert_eq!(
        final_request.headers()["alt-used"],
        format!("alternative.test:{}", destination.quic_addr.port())
    );
}

#[tokio::test]
async fn clear_on_http3_response_returns_next_request_to_tcp() {
    let server = LocalServer::new(|request, port| {
        if request.uri().path() == "/seed" {
            advertise(request, port)
        } else {
            Response::builder()
                .header("alt-svc", "clear")
                .body(Bytes::new())
                .unwrap()
        }
    })
    .await;
    let client = server.client_builder().build().unwrap();
    for (path, version) in [
        ("/seed", Version::HTTP_2),
        ("/clear", Version::HTTP_3),
        ("/again", Version::HTTP_2),
    ] {
        let response = client.get(server.url(path)).send().await.unwrap();
        assert_eq!(response.version(), version);
        response.bytes().await.unwrap();
    }
}

#[tokio::test]
async fn opt_out_and_disabled_verification_prevent_automatic_upgrade() {
    let server = LocalServer::new(advertise).await;
    let builders = [
        server.client_builder().alt_svc(false),
        server
            .client_builder()
            .tls_danger_accept_invalid_certs(true),
        server.client_builder().tls_sni(false),
        server.client_builder().http2_prior_knowledge(),
    ];
    for builder in builders {
        let client = builder.build().unwrap();
        for _ in 0..2 {
            let response = client.get(server.url("/seed")).send().await.unwrap();
            assert_eq!(response.version(), Version::HTTP_2);
            response.bytes().await.unwrap();
        }
    }
}

#[tokio::test]
async fn proxy_configuration_is_never_bypassed_by_http3() {
    let server = LocalServer::new(advertise).await;
    let cache = Arc::new(AltSvcCache::new());
    let direct = server
        .client_builder()
        .alt_svc_cache(cache.clone())
        .build()
        .unwrap();
    direct
        .get(server.url("/seed"))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let proxy_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy =
        htrio::Proxy::https(format!("http://{}", proxy_listener.local_addr().unwrap())).unwrap();
    let client = server
        .client_builder()
        .alt_svc_cache(cache)
        .proxy(proxy)
        .build()
        .unwrap();
    let request = client.get(server.url("/proxied")).send();
    let check_proxy = async {
        let (mut stream, _) = proxy_listener.accept().await.unwrap();
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut bytes = [0; 1024];
        let n = stream.read(&mut bytes).await.unwrap();
        assert!(std::str::from_utf8(&bytes[..n])
            .unwrap()
            .starts_with("CONNECT origin.test:"));
        stream
            .write_all(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
    };
    let (response, ()) = tokio::join!(request, check_proxy);
    assert!(response.is_err());
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn known_http3_server_needs_no_per_request_version_override() {
    let server = LocalServer::new(advertise).await;
    let client = server
        .client_builder()
        .http3_prior_knowledge()
        .build()
        .unwrap();
    let response = client
        .get(format!(
            "https://origin.test:{}/direct",
            server.quic_addr.port()
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.version(), Version::HTTP_3);
}

#[tokio::test]
async fn explicit_http3_honors_custom_dns_ports_and_reports_dns_failures() {
    let server = LocalServer::new(advertise).await;
    let client = server
        .client_builder()
        .http3_prior_knowledge()
        .resolve("origin.test", server.quic_addr)
        .build()
        .unwrap();
    // With no URL port, the resolver's nonzero port must be preserved.
    let response = client
        .get("https://origin.test/resolver-port")
        .send()
        .await
        .unwrap();
    assert_eq!(response.version(), Version::HTTP_3);
    response.bytes().await.unwrap();

    struct FailingResolver;
    impl htrio::dns::Resolve for FailingResolver {
        fn resolve(&self, _: htrio::dns::Name) -> htrio::dns::Resolving {
            Box::pin(std::future::ready(Err("fixture HTTP/3 DNS failure".into())))
        }
    }
    let url = "https://unresolvable.invalid/dns";
    let client = htrio::Client::builder()
        .no_proxy()
        .http3_prior_knowledge()
        .dns_resolver(Arc::new(FailingResolver))
        .build()
        .unwrap();
    let error = client.get(url).send().await.unwrap_err();
    assert!(error.is_request());
    assert_eq!(error.url().unwrap().as_str(), url);
    assert!(std::error::Error::source(&error)
        .unwrap()
        .to_string()
        .contains("fixture HTTP/3 DNS failure"));
}

#[tokio::test]
async fn tls_versions_and_http_versions_negotiate_locally() {
    for tls_version in [htrio::tls::Version::TLS_1_2, htrio::tls::Version::TLS_1_3] {
        let supported = if tls_version == htrio::tls::Version::TLS_1_2 {
            &rustls::version::TLS12
        } else {
            &rustls::version::TLS13
        };
        let server = LocalServer::with_versions(&["origin.test"], &[supported], advertise).await;
        for version in [Version::HTTP_10, Version::HTTP_11, Version::HTTP_2] {
            let builder = server
                .client_builder()
                .tls_version_min(tls_version)
                .tls_version_max(tls_version);
            let builder = if version == Version::HTTP_2 {
                builder.http2_prior_knowledge()
            } else {
                builder.http1_only()
            };
            let response = builder
                .build()
                .unwrap()
                .get(server.url("/version"))
                .version(version)
                .send()
                .await
                .unwrap();
            assert_eq!(response.version(), version);
            response.bytes().await.unwrap();
        }
    }
    assert!(htrio::Client::builder()
        .http3_prior_knowledge()
        .tls_version_max(htrio::tls::Version::TLS_1_2)
        .build()
        .unwrap_err()
        .is_builder());
}

#[tokio::test]
#[cfg(feature = "gzip")]
async fn decompression_remains_enabled_after_automatic_upgrade() {
    use std::io::Write;
    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gzip.write_all(b"decoded http3 response").unwrap();
    let gzip = Bytes::from(gzip.finish().unwrap());
    let server = LocalServer::new(move |request, port| {
        if request.uri().path() == "/seed" {
            advertise(request, port)
        } else {
            Response::builder()
                .header("content-encoding", "gzip")
                .header("content-length", gzip.len())
                .body(gzip.clone())
                .unwrap()
        }
    })
    .await;
    let client = server.client_builder().build().unwrap();
    client
        .get(server.url("/seed"))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let response = client.get(server.url("/compressed")).send().await.unwrap();
    assert_eq!(response.version(), Version::HTTP_3);
    assert!(!response.headers().contains_key("content-encoding"));
    assert_eq!(response.text().await.unwrap(), "decoded http3 response");
}

#[test]
fn client_can_be_built_before_entering_tokio() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let server = runtime.block_on(LocalServer::new(advertise));
    let client = server
        .client_builder()
        .http3_prior_knowledge()
        .build()
        .unwrap();
    runtime.block_on(async {
        let response = client
            .get(format!(
                "https://origin.test:{}/direct",
                server.quic_addr.port()
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(response.version(), Version::HTTP_3);
    });
}

#[tokio::test]
async fn request_rejected_is_a_safe_nack_and_does_not_poison_alternative() {
    let server = LocalServer::new(advertise).await;
    let client = server.client_builder().build().unwrap();
    client
        .get(server.url("/seed"))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    server.reject_once.store(true, Ordering::SeqCst);
    let response = client
        .post(server.url("/retry"))
        .body("safe rejection")
        .send()
        .await
        .unwrap();
    assert_eq!(response.version(), Version::HTTP_3);
    assert_eq!(response.text().await.unwrap(), "safe rejection");
    let requests = server.requests();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[1].version(), Version::HTTP_3);
    assert_eq!(requests[2].version(), Version::HTTP_3);
    assert_eq!(requests[1].body(), requests[2].body());
}

#[cfg(feature = "__native-tls-alpn")]
#[tokio::test]
async fn native_tls_alpn_still_negotiates_http2_when_http3_is_compiled() {
    let server = LocalServer::new(advertise).await;
    let client = server
        .client_builder()
        .tls_backend_native()
        .alt_svc(false)
        .build()
        .unwrap();
    let response = client
        .get(server.url("/native-http2"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.version(), Version::HTTP_2);
    response.bytes().await.unwrap();
}

#[tokio::test]
async fn rotates_failed_alternatives_without_dispatching_a_second_body() {
    let closed = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let bad_port = closed.local_addr().unwrap().port();
    drop(closed);
    let server = LocalServer::new(move |request, port| {
        if request.uri().path() == "/seed" {
            Response::builder()
                .header(
                    "alt-svc",
                    format!("h3=\"alternative.test:{bad_port}\", h3=\"alternative.test:{port}\""),
                )
                .body(Bytes::new())
                .unwrap()
        } else {
            Response::new(request.into_body())
        }
    })
    .await;
    let client = server
        .client_builder()
        .http3_connect_timeout(Duration::from_millis(100))
        .build()
        .unwrap();
    client
        .get(server.url("/seed"))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let response = client
        .post(server.url("/echo"))
        .body(htrio::Body::wrap_stream(futures_util::stream::iter([
            Ok::<_, std::io::Error>(Bytes::from_static(b"once")),
        ])))
        .send()
        .await
        .unwrap();
    assert_eq!(response.version(), Version::HTTP_3);
    assert_eq!(response.text().await.unwrap(), "once");
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|request| request.method() == "POST")
            .count(),
        1
    );
}

#[tokio::test]
async fn tcp_connection_races_a_blackholed_quic_handshake() {
    let blackhole = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = blackhole.local_addr().unwrap().port();
    let server = LocalServer::new(move |request, _| advertise(request, port)).await;
    let client = server
        .client_builder()
        .http3_connect_timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    client
        .get(server.url("/seed"))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let response = tokio::time::timeout(
        Duration::from_secs(2),
        client.post(server.url("/echo")).body("one dispatch").send(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(response.version(), Version::HTTP_2);
    assert_eq!(response.text().await.unwrap(), "one dispatch");
    assert_eq!(server.requests().len(), 2);
}

#[tokio::test]
async fn cross_origin_coalescing_is_opt_in_and_rechecks_certificate_authority() {
    for enabled in [false, true] {
        let server = LocalServer::with_names(&["origin.test", "second.test"], advertise).await;
        let cache = Arc::new(AltSvcCache::new());
        let client = server
            .client_builder()
            .resolve("second.test", server.addr)
            .resolve("uncovered.test", server.addr)
            .http3_coalescing(enabled)
            .alt_svc_cache(cache.clone())
            .build()
            .unwrap();
        let second = format!("https://second.test:{}/", server.addr.port());
        for url in [server.url("/seed"), format!("{second}seed")] {
            client.get(url).send().await.unwrap().bytes().await.unwrap();
        }
        let response = client.get(server.url("/first")).send().await.unwrap();
        let first = response
            .extensions()
            .get::<htrio::http3::ConnectionInfo>()
            .unwrap()
            .id();
        response.bytes().await.unwrap();
        let response = client.get(format!("{second}second")).send().await.unwrap();
        let info = response
            .extensions()
            .get::<htrio::http3::ConnectionInfo>()
            .unwrap();
        assert_eq!(info.is_coalesced(), enabled);
        assert_eq!(info.id() == first, enabled);
        response.bytes().await.unwrap();
        cache.restore([htrio::alt_svc::Record {
            origin: format!("https://uncovered.test:{}", server.addr.port()),
            authority: format!("alternative.test:{}", server.quic_addr.port()),
            expires_at: std::time::SystemTime::now() + Duration::from_secs(60),
            persist: false,
        }]);
        assert!(client
            .get(format!(
                "https://uncovered.test:{}/must-not-dispatch",
                server.addr.port()
            ))
            .send()
            .await
            .is_err());
        assert!(server
            .requests()
            .iter()
            .all(|request| request.uri().host() != Some("uncovered.test")));
        let response = client
            .get(server.url("/still-healthy"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            response
                .extensions()
                .get::<htrio::http3::ConnectionInfo>()
                .unwrap()
                .id(),
            first
        );
        response.bytes().await.unwrap();
    }
}
