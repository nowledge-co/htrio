#![cfg(feature = "http3")]

use std::path::Path;

#[tokio::test]
#[ignore = "requires an external HTTP/3 server; run through interop.yml"]
async fn caddy_http3_interoperability() {
    let url = std::env::var("HTRIO_INTEROP_URL").expect("HTRIO_INTEROP_URL is required");
    let ca_path = std::env::var("HTRIO_INTEROP_CA").expect("HTRIO_INTEROP_CA is required");
    let ca = std::fs::read(Path::new(&ca_path)).expect("read interop CA");
    let certificate = htrio::Certificate::from_pem(&ca).expect("parse interop CA");

    let response = htrio::Client::builder()
        .add_root_certificate(certificate)
        .http3_prior_knowledge()
        .build()
        .expect("build HTTP/3 client")
        .get(url)
        .send()
        .await
        .expect("request Caddy over HTTP/3");

    assert_eq!(response.version(), http::Version::HTTP_3);
    assert_eq!(response.text().await.unwrap(), "htrio-caddy-http3");
}
