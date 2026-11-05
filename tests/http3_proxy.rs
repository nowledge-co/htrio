#![cfg(all(
    feature = "http3",
    feature = "socks",
    feature = "http2",
    feature = "stream"
))]

#[path = "support/local_tls.rs"]
mod local_tls;
#[path = "support/socks_proxy.rs"]
mod socks_proxy;

use local_tls::{advertise, LocalServer};
use socks_proxy::SocksProxy;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default)]
struct Resolver {
    names: Mutex<Vec<String>>,
    remote: bool,
}
impl htrio::dns::Resolve for Resolver {
    fn resolve(&self, name: htrio::dns::Name) -> htrio::dns::Resolving {
        self.names.lock().unwrap().push(name.as_str().to_owned());
        if self.remote && name.as_str() != "proxy.test" {
            return Box::pin(std::future::ready(Err(
                "target DNS must stay at proxy".into()
            )));
        }
        let addresses: htrio::dns::Addrs = Box::new(["127.0.0.1:0".parse().unwrap()].into_iter());
        Box::pin(std::future::ready(Ok(addresses)))
    }
}

#[tokio::test]
async fn socks5_udp_uses_local_custom_dns_and_socks5h_does_not_leak_target_dns() {
    let server = LocalServer::new(advertise).await;
    let proxy = SocksProxy::new(None, false).await;
    for remote in [false, true] {
        let resolver = Arc::new(Resolver {
            remote,
            ..Resolver::default()
        });
        let scheme = if remote { "socks5h" } else { "socks5" };
        let client = htrio::Client::builder()
            .no_proxy()
            .tls_certs_only([server.certificate()])
            .dns_resolver(resolver.clone())
            .http3_prior_knowledge()
            .proxy(
                htrio::Proxy::all(format!("{scheme}://proxy.test:{}", proxy.addr.port())).unwrap(),
            )
            .connect_timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let response = client
            .post(format!(
                "https://origin.test:{}/echo",
                server.quic_addr.port()
            ))
            .body("via proxy")
            .send()
            .await
            .unwrap();
        assert_eq!(response.version(), htrio::Version::HTTP_3);
        assert_eq!(response.text().await.unwrap(), "via proxy");
        let names = resolver.names.lock().unwrap();
        assert!(names.iter().any(|name| name == "proxy.test"));
        assert_eq!(names.iter().any(|name| name == "origin.test"), !remote);
        let destinations = proxy.destinations.lock().unwrap();
        assert_eq!(destinations.last().unwrap().domain, remote);
    }
    assert_eq!(proxy.associations.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn automatic_upgrade_and_streaming_requests_use_udp_associate() {
    let server = LocalServer::new(advertise).await;
    let proxy = SocksProxy::new(Some(("fixture-user", "fixture-pass")), false).await;
    let client = server
        .client_builder()
        .proxy(
            htrio::Proxy::all(format!("socks5://{}", proxy.addr))
                .unwrap()
                .basic_auth("fixture-user", "fixture-pass"),
        )
        .connect_timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let first = client.get(server.url("/seed")).send().await.unwrap();
    assert_eq!(first.version(), htrio::Version::HTTP_2);
    first.bytes().await.unwrap();
    let body = htrio::Body::wrap_stream(futures_util::stream::iter([Ok::<_, std::io::Error>(
        "streamed through UDP",
    )]));
    let response = client
        .post(server.url("/echo"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.version(), htrio::Version::HTTP_3);
    assert_eq!(response.text().await.unwrap(), "streamed through UDP");
    assert_eq!(proxy.associations.load(Ordering::SeqCst), 1);
    assert!(!server
        .requests()
        .last()
        .unwrap()
        .headers()
        .contains_key("proxy-authorization"));
}

#[tokio::test]
async fn rejected_udp_falls_back_through_the_same_socks_proxy() {
    let server = LocalServer::new(advertise).await;
    let proxy = SocksProxy::new(None, true).await;
    let client = server
        .client_builder()
        .proxy(htrio::Proxy::all(format!("socks5://{}", proxy.addr)).unwrap())
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
        .post(server.url("/post"))
        .body("once")
        .send()
        .await
        .unwrap();
    assert_eq!(response.version(), htrio::Version::HTTP_2);
    assert_eq!(response.text().await.unwrap(), "once");
    assert_eq!(proxy.associations.load(Ordering::SeqCst), 0);
    assert!(proxy
        .destinations
        .lock()
        .unwrap()
        .iter()
        .any(|destination| destination.port == server.addr.port()));
    assert_eq!(server.requests().len(), 2);
}

#[tokio::test]
async fn proxy_credentials_partition_the_http3_pool() {
    let server = LocalServer::new(advertise).await;
    let proxy = SocksProxy::new(Some(("alice", "fixture-secret")), false).await;
    let wrong_credentials = Arc::new(AtomicBool::new(false));
    let choose = wrong_credentials.clone();
    let address = proxy.addr;
    let client = server
        .client_builder()
        .http3_prior_knowledge()
        .connect_timeout(Duration::from_secs(2))
        .proxy(htrio::Proxy::custom(move |_| {
            let user = if choose.load(Ordering::SeqCst) {
                "wrong"
            } else {
                "alice"
            };
            Some(format!("socks5://{user}:fixture-secret@{address}"))
        }))
        .build()
        .unwrap();
    let url = format!("https://origin.test:{}/echo", server.quic_addr.port());
    client
        .get(&url)
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    wrong_credentials.store(true, Ordering::SeqCst);
    let error = client.get(&url).send().await.unwrap_err();
    assert!(error.is_request());
    assert!(!format!("{error:?}").contains("fixture-secret"));
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn no_proxy_rules_preserve_direct_http3() {
    let server = LocalServer::new(advertise).await;
    let proxy = SocksProxy::new(None, false).await;
    let client = server
        .client_builder()
        .http3_prior_knowledge()
        .proxy(
            htrio::Proxy::all(format!("socks5://{}", proxy.addr))
                .unwrap()
                .no_proxy(htrio::NoProxy::from_string("origin.test")),
        )
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
    assert_eq!(response.version(), htrio::Version::HTTP_3);
    assert_eq!(proxy.associations.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn closing_control_connection_invalidates_the_association() {
    let server = LocalServer::new(advertise).await;
    let proxy = SocksProxy::new(None, false).await;
    let client = server
        .client_builder()
        .http3_prior_knowledge()
        .connect_timeout(Duration::from_secs(1))
        .http3_max_idle_timeout(Duration::from_millis(100))
        .proxy(htrio::Proxy::all(format!("socks5://{}", proxy.addr)).unwrap())
        .build()
        .unwrap();
    let url = format!("https://origin.test:{}/echo", server.quic_addr.port());
    client
        .get(&url)
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    proxy.close_associations();
    tokio::time::timeout(Duration::from_secs(1), async {
        while proxy.active.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let response = client.get(&url).send().await.unwrap();
    assert_eq!(response.version(), htrio::Version::HTTP_3);
    assert_eq!(proxy.associations.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn ipv6_targets_work_through_ipv4_socks_relays() {
    #[path = "support/server.rs"]
    #[allow(dead_code)]
    mod server;
    let origin = server::Http3::new()
        .build(|_| async { http::Response::new("ipv6 over ipv4 proxy".into()) });
    let proxy = SocksProxy::new(None, false).await;
    let client = htrio::Client::builder()
        .no_proxy()
        .http3_prior_knowledge()
        .tls_danger_accept_invalid_certs(true)
        .timeout(Duration::from_secs(3))
        .proxy(htrio::Proxy::all(format!("socks5://{}", proxy.addr)).unwrap())
        .build()
        .unwrap();
    let response = client
        .get(format!("https://{}/ipv6", origin.addr()))
        .send()
        .await
        .unwrap();
    assert_eq!(response.version(), htrio::Version::HTTP_3);
    assert_eq!(response.text().await.unwrap(), "ipv6 over ipv4 proxy");
    assert!(proxy
        .destinations
        .lock()
        .unwrap()
        .iter()
        .any(|destination| destination.host == "::1"));
}
