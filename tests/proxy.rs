#![cfg(not(target_arch = "wasm32"))]
#![cfg(not(feature = "rustls-no-provider"))]
mod support;
use support::server;

use std::env;

use std::sync::LazyLock;
use tokio::sync::Mutex;

// serialize tests that read from / write to environment variables
static HTTP_PROXY_ENV_MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

struct ProxyEnvGuard(Vec<(&'static str, Option<std::ffi::OsString>)>);

impl ProxyEnvGuard {
    fn new(proxy: String) -> Self {
        let keys = [
            "HTTP_PROXY",
            "http_proxy",
            "ALL_PROXY",
            "all_proxy",
            "NO_PROXY",
            "no_proxy",
        ];
        let saved = keys
            .into_iter()
            .map(|key| (key, env::var_os(key)))
            .collect();
        for key in keys {
            if matches!(key, "HTTP_PROXY" | "http_proxy") {
                env::set_var(key, &proxy);
            } else {
                env::remove_var(key);
            }
        }
        Self(saved)
    }
}

impl Drop for ProxyEnvGuard {
    fn drop(&mut self) {
        for (key, value) in &self.0 {
            match value {
                Some(value) => env::set_var(key, value),
                None => env::remove_var(key),
            }
        }
    }
}

#[tokio::test]
async fn http_proxy() {
    let url = "http://hyper.rs.local/prox";
    let server = server::http(move |req| {
        assert_eq!(req.method(), "GET");
        assert_eq!(req.uri(), url);
        assert_eq!(req.headers()["host"], "hyper.rs.local");

        async { http::Response::default() }
    });

    let proxy = format!("http://{}", server.addr());

    let res = htrio::Client::builder()
        .proxy(htrio::Proxy::http(&proxy).unwrap())
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap();

    assert_eq!(res.url().as_str(), url);
    assert_eq!(res.status(), htrio::StatusCode::OK);
}

#[tokio::test]
async fn http_proxy_basic_auth() {
    let url = "http://hyper.rs.local/prox";
    let server = server::http(move |req| {
        assert_eq!(req.method(), "GET");
        assert_eq!(req.uri(), url);
        assert_eq!(req.headers()["host"], "hyper.rs.local");
        assert_eq!(
            req.headers()["proxy-authorization"],
            "Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ=="
        );

        async { http::Response::default() }
    });

    let proxy = format!("http://{}", server.addr());

    let res = htrio::Client::builder()
        .proxy(
            htrio::Proxy::http(&proxy)
                .unwrap()
                .basic_auth("Aladdin", "open sesame"),
        )
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap();

    assert_eq!(res.url().as_str(), url);
    assert_eq!(res.status(), htrio::StatusCode::OK);
}

#[tokio::test]
async fn http_proxy_basic_auth_parsed() {
    let url = "http://hyper.rs.local/prox";
    let server = server::http(move |req| {
        assert_eq!(req.method(), "GET");
        assert_eq!(req.uri(), url);
        assert_eq!(req.headers()["host"], "hyper.rs.local");
        assert_eq!(
            req.headers()["proxy-authorization"],
            "Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ=="
        );

        async { http::Response::default() }
    });

    let proxy = format!("http://Aladdin:open sesame@{}", server.addr());

    let res = htrio::Client::builder()
        .proxy(htrio::Proxy::http(&proxy).unwrap())
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap();

    assert_eq!(res.url().as_str(), url);
    assert_eq!(res.status(), htrio::StatusCode::OK);
}

#[tokio::test]
async fn system_http_proxy_basic_auth_parsed() {
    let url = "http://hyper.rs.local/prox";
    let server = server::http(move |req| {
        assert_eq!(req.method(), "GET");
        assert_eq!(req.uri(), url);
        assert_eq!(req.headers()["host"], "hyper.rs.local");
        assert_eq!(
            req.headers()["proxy-authorization"],
            "Basic QWxhZGRpbjpvcGVuc2VzYW1l"
        );

        async { http::Response::default() }
    });

    // avoid races with other tests that change "http_proxy"
    let _env_lock = HTTP_PROXY_ENV_MUTEX.lock().await;

    let _environment = ProxyEnvGuard::new(format!("http://Aladdin:opensesame@{}", server.addr()));

    let res = htrio::Client::builder()
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap();

    assert_eq!(res.url().as_str(), url);
    assert_eq!(res.status(), htrio::StatusCode::OK);
}

#[tokio::test]
async fn test_no_proxy() {
    let server = server::http(move |req| {
        assert_eq!(req.method(), "GET");
        assert_eq!(req.uri(), "/4");

        async { http::Response::default() }
    });
    let proxy = format!("http://{}", server.addr());
    let url = format!("http://{}/4", server.addr());

    // set up proxy and use no_proxy to clear up client builder proxies.
    let res = htrio::Client::builder()
        .proxy(htrio::Proxy::http(&proxy).unwrap())
        .no_proxy()
        .build()
        .unwrap()
        .get(&url)
        .send()
        .await
        .unwrap();

    assert_eq!(res.url().as_str(), &url);
    assert_eq!(res.status(), htrio::StatusCode::OK);
}

#[tokio::test]
async fn test_custom_headers() {
    let url = "http://hyper.rs.local/prox";
    let server = server::http(move |req| {
        assert_eq!(req.method(), "GET");
        assert_eq!(req.uri(), url);
        assert_eq!(req.headers()["host"], "hyper.rs.local");
        assert_eq!(
            req.headers()["proxy-authorization"],
            "Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ=="
        );
        async { http::Response::default() }
    });

    let proxy = format!("http://{}", server.addr());
    let mut headers = htrio::header::HeaderMap::new();
    headers.insert(
        // htrio::header::HeaderName::from_static("Proxy-Authorization"),
        htrio::header::PROXY_AUTHORIZATION,
        "Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ==".parse().unwrap(),
    );

    let res = htrio::Client::builder()
        .proxy(htrio::Proxy::http(&proxy).unwrap().headers(headers))
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap();

    assert_eq!(res.url().as_str(), url);
    assert_eq!(res.status(), htrio::StatusCode::OK);
}

#[tokio::test]
async fn test_using_system_proxy() {
    let url = "http://not.a.real.sub.hyper.rs.local/prox";
    let server = server::http(move |req| {
        assert_eq!(req.method(), "GET");
        assert_eq!(req.uri(), url);
        assert_eq!(req.headers()["host"], "not.a.real.sub.hyper.rs.local");

        async { http::Response::default() }
    });

    // avoid races with other tests that change "http_proxy"
    let _env_lock = HTTP_PROXY_ENV_MUTEX.lock().await;

    let _environment = ProxyEnvGuard::new(format!("http://{}", server.addr()));

    // system proxy is used by default
    let res = htrio::get(url).await.unwrap();

    assert_eq!(res.url().as_str(), url);
    assert_eq!(res.status(), htrio::StatusCode::OK);
}

#[tokio::test]
async fn http_over_http() {
    let url = "http://hyper.rs.local/prox";

    let server = server::http(move |req| {
        assert_eq!(req.method(), "GET");
        assert_eq!(req.uri(), url);
        assert_eq!(req.headers()["host"], "hyper.rs.local");

        async { http::Response::default() }
    });

    let proxy = format!("http://{}", server.addr());

    let res = htrio::Client::builder()
        .proxy(htrio::Proxy::http(&proxy).unwrap())
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap();

    assert_eq!(res.url().as_str(), url);
    assert_eq!(res.status(), htrio::StatusCode::OK);
}

#[cfg(feature = "__tls")]
#[tokio::test]
async fn tunnel_detects_auth_required() {
    let url = "https://hyper.rs.local/prox";

    let server = server::http(move |req| {
        assert_eq!(req.method(), "CONNECT");
        assert_eq!(req.uri(), "hyper.rs.local:443");
        assert!(!req
            .headers()
            .contains_key(http::header::PROXY_AUTHORIZATION));

        async {
            let mut res = http::Response::default();
            *res.status_mut() = http::StatusCode::PROXY_AUTHENTICATION_REQUIRED;
            res
        }
    });

    let proxy = format!("http://{}", server.addr());

    let err = htrio::Client::builder()
        .proxy(htrio::Proxy::https(&proxy).unwrap())
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap_err();

    let err = support::error::inspect(err).pop().unwrap();
    assert!(
        err.contains("auth"),
        "proxy auth err expected, got: {:?}",
        err
    );
}

#[cfg(feature = "__tls")]
#[tokio::test]
async fn tunnel_includes_proxy_auth() {
    let url = "https://hyper.rs.local/prox";

    let server = server::http(move |req| {
        assert_eq!(req.method(), "CONNECT");
        assert_eq!(req.uri(), "hyper.rs.local:443");
        assert_eq!(
            req.headers()["proxy-authorization"],
            "Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ=="
        );

        async {
            // return 400 to not actually deal with TLS tunneling
            let mut res = http::Response::default();
            *res.status_mut() = http::StatusCode::BAD_REQUEST;
            res
        }
    });

    let proxy = format!("http://Aladdin:open%20sesame@{}", server.addr());

    let err = htrio::Client::builder()
        .proxy(htrio::Proxy::https(&proxy).unwrap())
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap_err();

    let err = support::error::inspect(err).pop().unwrap();
    assert!(
        err.contains("unsuccessful"),
        "tunnel unsuccessful expected, got: {:?}",
        err
    );
}

#[cfg(feature = "__tls")]
#[tokio::test]
async fn tunnel_includes_user_agent() {
    let url = "https://hyper.rs.local/prox";

    let server = server::http(move |req| {
        assert_eq!(req.method(), "CONNECT");
        assert_eq!(req.uri(), "hyper.rs.local:443");
        assert_eq!(req.headers()["user-agent"], "htrio-test");

        async {
            // return 400 to not actually deal with TLS tunneling
            let mut res = http::Response::default();
            *res.status_mut() = http::StatusCode::BAD_REQUEST;
            res
        }
    });

    let proxy = format!("http://{}", server.addr());

    let err = htrio::Client::builder()
        .proxy(htrio::Proxy::https(&proxy).unwrap())
        .user_agent("htrio-test")
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap_err();

    let err = support::error::inspect(err).pop().unwrap();
    assert!(
        err.contains("unsuccessful"),
        "tunnel unsuccessful expected, got: {:?}",
        err
    );
}

#[tokio::test]
async fn tunnel_includes_proxy_auth_with_multiple_proxies() {
    let url = "http://hyper.rs.local/prox";
    let server1 = server::http(move |req| {
        assert_eq!(req.method(), "GET");
        assert_eq!(req.uri(), url);
        assert_eq!(req.headers()["host"], "hyper.rs.local");
        assert_eq!(
            req.headers()["proxy-authorization"],
            "Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ=="
        );
        assert_eq!(req.headers()["proxy-header"], "proxy2");
        async { http::Response::default() }
    });

    let proxy_url = format!("http://Aladdin:open%20sesame@{}", server1.addr());

    let mut headers1 = htrio::header::HeaderMap::new();
    headers1.insert("proxy-header", "proxy1".parse().unwrap());

    let mut headers2 = htrio::header::HeaderMap::new();
    headers2.insert("proxy-header", "proxy2".parse().unwrap());

    let client = htrio::Client::builder()
        // When processing proxy headers, the first one is iterated,
        // and if the current URL does not match, the proxy is skipped
        .proxy(
            htrio::Proxy::https(&proxy_url)
                .unwrap()
                .headers(headers1.clone()),
        )
        // When processing proxy headers, the second one is iterated,
        // and for the current URL matching, the proxy will be used
        .proxy(
            htrio::Proxy::http(&proxy_url)
                .unwrap()
                .headers(headers2.clone()),
        )
        .build()
        .unwrap();

    let res = client.get(url).send().await.unwrap();

    assert_eq!(res.url().as_str(), url);
    assert_eq!(res.status(), htrio::StatusCode::OK);

    let client = htrio::Client::builder()
        // When processing proxy headers, the first one is iterated,
        // and for the current URL matching, the proxy will be used
        .proxy(htrio::Proxy::http(&proxy_url).unwrap().headers(headers2))
        // When processing proxy headers, the second one is iterated,
        // and if the current URL does not match, the proxy is skipped
        .proxy(htrio::Proxy::https(&proxy_url).unwrap().headers(headers1))
        .build()
        .unwrap();

    let res = client.get(url).send().await.unwrap();

    assert_eq!(res.url().as_str(), url);
    assert_eq!(res.status(), htrio::StatusCode::OK);
}
