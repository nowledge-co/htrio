#![cfg(all(
    feature = "json",
    feature = "query",
    feature = "multipart",
    feature = "stream"
))]

// Public API contracts adapted from Mem's thread_export_client,
// desktop_event_stream, and ai_now_assets clients. No Tauri runtime is needed.
use htrio as reqwest;

mod support;
use futures_util::StreamExt;
use http_body_util::BodyExt;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use std::time::Duration;
use support::server;

fn client() -> reqwest::Client {
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static("Bearer fixture-token"),
    );
    headers.insert("x-nmem-api-key", HeaderValue::from_static("fixture-token"));
    headers.insert("x-request-id", HeaderValue::from_static("fixture-request"));
    reqwest::Client::builder()
        .no_proxy()
        .default_headers(headers)
        .connect_timeout(Duration::from_secs(15))
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap()
}

#[tokio::test]
async fn exact_thread_export_preserves_path_query_and_receipt() {
    let server = server::http(|request| async move {
        assert_eq!(request.uri().path(), "/threads/stored%2Frow/export");
        let url = reqwest::Url::parse(&format!("http://localhost{}", request.uri())).unwrap();
        let query: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(query["format"], "json");
        assert_eq!(query["include_metadata"], "true");
        assert_eq!(query["space_id"], "space-one");
        assert_eq!(request.headers()[AUTHORIZATION], "Bearer fixture-token");
        assert_eq!(request.headers()["x-request-id"], "fixture-request");
        http::Response::builder()
            .header("x-nmem-thread-reference", "fixture-reference")
            .body("{\"messages\":[]}".into())
            .unwrap()
    });
    let mut url = reqwest::Url::parse(&format!("http://{}/threads/", server.addr())).unwrap();
    url.path_segments_mut()
        .unwrap()
        .pop_if_empty()
        .push("stored/row")
        .push("export");
    let response = client()
        .get(url)
        .query(&[("format", "json"), ("include_metadata", "true")])
        .query(&[("space_id", "space-one")])
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    assert_eq!(
        response.headers()["x-nmem-thread-reference"],
        "fixture-reference"
    );
    assert_eq!(
        response.json::<serde_json::Value>().await.unwrap()["messages"],
        serde_json::json!([])
    );
}

#[tokio::test]
async fn desktop_event_stream_keeps_auth_headers_and_streaming_api() {
    let server = server::http(|request| async move {
        assert_eq!(request.headers()["accept"], "text/event-stream");
        assert_eq!(request.headers()["cache-control"], "no-cache");
        assert_eq!(request.headers()["x-nmem-api-key"], "fixture-token");
        let body = reqwest::Body::wrap_stream(futures_util::stream::iter([
            Ok::<_, std::io::Error>("event: message\ndata: {\"kind\":"),
            Ok("\"changed\"}\n\n"),
        ]));
        http::Response::builder()
            .header(CONTENT_TYPE, "text/event-stream")
            .body(body)
            .unwrap()
    });
    let response = client()
        .get(format!("http://{}/events", server.addr()))
        .header("accept", "text/event-stream")
        .header("cache-control", "no-cache")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert_eq!(response.headers()[CONTENT_TYPE], "text/event-stream");
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    while let Some(chunk) = stream.next().await {
        body.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(body, b"event: message\ndata: {\"kind\":\"changed\"}\n\n");
}

#[tokio::test]
async fn attachment_upload_uses_streaming_multipart_and_dual_auth() {
    let server = server::http(|request| async move {
        assert_eq!(request.headers()[AUTHORIZATION], "Bearer fixture-token");
        assert_eq!(request.headers()["x-nmem-api-key"], "fixture-token");
        assert!(request.headers()[CONTENT_TYPE]
            .to_str()
            .unwrap()
            .starts_with("multipart/form-data; boundary="));
        let body = request.collect().await.unwrap().to_bytes();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("name=\"manifest\""));
        assert!(body.contains("name=\"files\"; filename=\"attachment.txt\""));
        assert!(body.contains("streamed attachment"));
        http::Response::new("{\"ok\":true}".into())
    });
    let part =
        reqwest::multipart::Part::stream(reqwest::Body::wrap_stream(futures_util::stream::iter([
            Ok::<_, std::io::Error>("streamed attachment"),
        ])))
        .file_name("attachment.txt")
        .mime_str("text/plain")
        .unwrap();
    let form = reqwest::multipart::Form::new()
        .text("manifest", "{\"files\":1}")
        .part("files", part);
    let mut response = client()
        .post(format!("http://{}/assets", server.addr()))
        .multipart(form)
        .send()
        .await
        .unwrap();
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.unwrap() {
        body.extend_from_slice(&chunk);
    }
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["ok"],
        true
    );
}
