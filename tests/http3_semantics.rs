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
    let response = server
        .client_builder()
        .build()
        .unwrap()
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
