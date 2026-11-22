use bytes::Buf;
use futures_util::future;
use http::{HeaderMap, Response};
use quic::{SendStream as _, StreamId};
#[cfg(feature = "tracing")]
use tracing::instrument;

use crate::{
    connection::{self},
    error::{
        connection_error_creators::{CloseStream, HandleFrameStreamErrorOnRequestStream},
        internal_error::InternalConnectionError,
        Code, StreamError,
    },
    proto::{frame::Frame, headers::Header},
    qpack,
    quic::{self},
    shared_state::{ConnectionState, SharedState},
};
use std::{
    convert::TryFrom,
    task::{Context, Poll},
};

/// Transfer a request body and receive response headers, data, and trailers.
///
/// Once a request has been sent via [`crate::client::SendRequest::send_request()`], a response can be awaited by calling
/// [`RequestStream::recv_response()`]. A body for this request can be sent with [`RequestStream::send_data()`], then the request
/// must be completed with [`RequestStream::finish()`], including after
/// [`RequestStream::send_trailers()`].
///
/// After receiving final response headers, its body can be read by [`RequestStream::recv_data()`] until it returns
/// `None`. Then the trailers will eventually be available via [`RequestStream::recv_trailers()`].
///
/// To cancel both directions, reset the upload with [`RequestStream::stop_stream()`]
/// and stop the response with [`RequestStream::stop_sending()`].
///
/// # Examples
///
/// ```rust
/// # use h3::{quic, client::*};
/// # use http::{Request, Response};
/// # use bytes::Buf;
/// # use tokio::io::AsyncWriteExt;
/// # async fn doc<T,B>(mut req_stream: RequestStream<T, B>) -> Result<(), Box<dyn std::error::Error>>
/// # where
/// #     T: quic::RecvStream,
/// # {
/// // Prepare the HTTP request to send to the server
/// let request = Request::get("https://www.example.com/").body(())?;
///
/// // Receive the response
/// let response = req_stream.recv_response().await?;
/// // Receive the body
/// while let Some(mut chunk) = req_stream.recv_data().await? {
///     let mut out = tokio::io::stdout();
///     out.write_all_buf(&mut chunk).await?;
///     out.flush().await?;
/// }
/// # Ok(())
/// # }
/// # pub fn main() {}
/// ```
///
/// [`send_request()`]: struct.SendRequest.html#method.send_request
/// [`recv_response()`]: #method.recv_response
/// [`recv_data()`]: #method.recv_data
/// [`send_data()`]: #method.send_data
/// [`send_trailers()`]: #method.send_trailers
/// [`recv_trailers()`]: #method.recv_trailers
/// [`finish()`]: #method.finish
/// [`stop_sending()`]: #method.stop_sending
pub struct RequestStream<S, B> {
    pub(super) pending_response: Option<bytes::Bytes>,
    pub(super) inner: connection::RequestStream<S, B>,
}

impl<S, B> ConnectionState for RequestStream<S, B> {
    fn shared_state(&self) -> &SharedState {
        &self.inner.conn_state
    }
}

impl<S, B> CloseStream for RequestStream<S, B> {}

impl<S, B> RequestStream<S, B>
where
    S: quic::RecvStream,
{
    /// Receive the HTTP/3 response
    ///
    /// This should be called before trying to receive any data with [`recv_data()`].
    ///
    /// [`recv_data()`]: #method.recv_data
    #[cfg_attr(feature = "tracing", instrument(skip_all, level = "trace"))]
    pub async fn recv_response(&mut self) -> Result<Response<()>, StreamError> {
        if self.pending_response.is_none() {
            let frame = future::poll_fn(|cx| self.inner.stream.poll_next(cx))
                .await
                .map_err(|error| self.handle_frame_stream_error_on_request_stream(error))?;
            match frame {
                Some(Frame::Headers(encoded)) => self.pending_response = Some(encoded),
                _ => {
                    return Err(self.handle_connection_error_on_stream(
                        InternalConnectionError::new(
                            Code::H3_FRAME_UNEXPECTED,
                            "expected response headers".into(),
                        ),
                    ))
                }
            }
        }
        let encoded = self
            .pending_response
            .as_ref()
            .expect("response headers retained")
            .clone();
        let state = self.inner.conn_state.qpack.clone();
        let decoded = future::poll_fn(|cx| {
            state.poll_decode(
                self.inner.stream.id().into_inner(),
                &encoded,
                self.inner.max_field_section_size,
                cx,
            )
        })
        .await;
        let decoded = match decoded {
            Ok(decoded) => decoded,
            Err(qpack::DecoderError::HeaderTooLong(actual_size)) => {
                self.inner.stop_sending(Code::H3_REQUEST_CANCELLED);
                return Err(StreamError::HeaderTooBig {
                    actual_size,
                    max_size: self.inner.max_field_section_size,
                });
            }
            Err(error) => {
                return Err(
                    self.handle_connection_error_on_stream(InternalConnectionError::new(
                        Code::QPACK_DECOMPRESSION_FAILED,
                        error.to_string(),
                    )),
                )
            }
        };
        self.pending_response = None;

        let qpack::Decoded { fields, .. } = decoded;

        let (status, headers) = Header::try_from(fields)
            .map_err(|_e| {
                self.inner.stream.stop_sending(Code::H3_REQUEST_CANCELLED);
                StreamError::StreamError {
                    code: Code::H3_MESSAGE_ERROR,
                    reason: "Received malformed header".to_string(),
                }
            })?
            .into_response_parts()
            .map_err(|_e| {
                self.inner.stream.stop_sending(Code::H3_REQUEST_CANCELLED);
                StreamError::StreamError {
                    code: Code::H3_MESSAGE_ERROR,
                    reason: "Received malformed header".to_string(),
                }
            })?;
        let mut resp = Response::new(());
        *resp.status_mut() = status;
        *resp.headers_mut() = headers;
        *resp.version_mut() = http::Version::HTTP_3;

        Ok(resp)
    }

    /// Receive response data after the final response headers.
    #[cfg_attr(feature = "tracing", instrument(skip_all, level = "trace"))]
    pub async fn recv_data(&mut self) -> Result<Option<impl Buf>, StreamError> {
        future::poll_fn(|cx| self.poll_recv_data(cx)).await
    }

    /// Poll for response data after the final response headers.
    pub fn poll_recv_data(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<impl Buf>, StreamError>> {
        self.inner.poll_recv_data(cx)
    }

    /// Receive an optional set of trailers for the response.
    #[cfg_attr(feature = "tracing", instrument(skip_all, level = "trace"))]
    pub async fn recv_trailers(&mut self) -> Result<Option<HeaderMap>, StreamError> {
        future::poll_fn(|cx| self.poll_recv_trailers(cx)).await
    }

    /// Poll receive an optional set of trailers for the response.
    #[cfg_attr(feature = "tracing", instrument(skip_all, level = "trace"))]
    pub fn poll_recv_trailers(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<HeaderMap>, StreamError>> {
        let res = self.inner.poll_recv_trailers(cx);
        if let Poll::Ready(Err(StreamError::HeaderTooBig { .. })) = &res {
            self.inner.stream.stop_sending(Code::H3_REQUEST_CANCELLED);
        }
        res
    }

    /// Tell the peer to stop sending into the underlying QUIC stream
    #[cfg_attr(feature = "tracing", instrument(skip_all, level = "trace"))]
    pub fn stop_sending(&mut self, error_code: Code) {
        // TODO take by value to prevent any further call as this request is cancelled
        // rename `cancel()` ?
        self.inner.stream.stop_sending(error_code)
    }

    /// Returns the underlying stream id
    pub fn id(&self) -> StreamId {
        self.inner.stream.id()
    }
}

impl<S, B> RequestStream<S, B>
where
    S: quic::SendStream<B>,
    B: Buf,
{
    /// Obtain a handle for control-stream priority updates.
    pub fn priority_handle(&self) -> crate::priority::Handle {
        crate::priority::Handle {
            shared: self.inner.conn_state.clone(),
            id: self.inner.stream.send_id().into_inner(),
        }
    }

    /// Observe peer cancellation while an upload source is pending.
    pub fn poll_stopped(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), StreamError>> {
        self.inner
            .stream
            .poll_stopped(cx)
            .map_err(|error| self.handle_quic_stream_error(error))
    }

    /// Send some data on the request body.
    #[cfg_attr(feature = "tracing", instrument(skip_all, level = "trace"))]
    pub async fn send_data(&mut self, buf: B) -> Result<(), StreamError> {
        self.inner.send_data(buf).await
    }

    /// Stop a stream with an error code
    ///
    /// The code can be [`Code::H3_NO_ERROR`].
    pub fn stop_stream(&mut self, error_code: Code) {
        self.inner.stop_stream(error_code);
    }

    /// Send the request's trailer section.
    ///
    /// [`RequestStream::finish()`] must be called to finalize a request.
    #[cfg_attr(feature = "tracing", instrument(skip_all, level = "trace"))]
    pub async fn send_trailers(&mut self, trailers: HeaderMap) -> Result<(), StreamError> {
        self.inner.send_trailers(trailers).await
    }

    /// Close the request's send direction, with or without trailers.
    ///
    /// [`RequestStream::finish()`] must be called to finalize a request.
    #[cfg_attr(feature = "tracing", instrument(skip_all, level = "trace"))]
    pub async fn finish(&mut self) -> Result<(), StreamError> {
        self.inner.finish().await
    }

    //= https://www.rfc-editor.org/rfc/rfc9114#section-4.1.1
    //= type=TODO
    //# Implementations SHOULD cancel requests by abruptly terminating any
    //# directions of a stream that are still open.  To do so, an
    //# implementation resets the sending parts of streams and aborts reading
    //# on the receiving parts of streams; see Section 2.4 of
    //# [QUIC-TRANSPORT].
}

impl<S, B> RequestStream<S, B>
where
    S: quic::BidiStream<B>,
    B: Buf,
{
    /// Split this stream into two halves that can be driven independently.
    pub fn split(
        self,
    ) -> (
        RequestStream<S::SendStream, B>,
        RequestStream<S::RecvStream, B>,
    ) {
        let (send, recv) = self.inner.split();
        (
            RequestStream {
                inner: send,
                pending_response: None,
            },
            RequestStream {
                inner: recv,
                pending_response: self.pending_response,
            },
        )
    }
}

#[cfg(test)]
mod priority_tests {
    use http::{Request, Response};

    use crate::{client, priority::Priority, server, tests::Pair};

    #[tokio::test]
    async fn request_priority_reaches_quinn_send_scheduler() {
        let mut pair = Pair::default();
        let mut server = pair.server();
        let (finished, received) = tokio::sync::oneshot::channel();
        let client = async {
            let (_driver, mut sender) = client::new(pair.client().await).await.unwrap();
            for (urgency, expected) in [(None, 0), (Some(0), 7), (Some(3), 4), (Some(7), 0)] {
                let mut request = Request::post("https://localhost/priority")
                    .body(())
                    .unwrap();
                if let Some(urgency) = urgency {
                    request
                        .extensions_mut()
                        .insert(Priority::new(urgency, false).unwrap());
                }
                let mut stream = sender.send_request(request).await.unwrap();
                assert_eq!(
                    stream.inner.stream.stream.inner().priority().unwrap(),
                    expected
                );
                stream.finish().await.unwrap();
                assert_eq!(stream.recv_response().await.unwrap().status(), 200);
            }
            finished.send(()).unwrap();
        };
        let server = async {
            let mut incoming = server::Connection::new(server.next().await).await.unwrap();
            for _ in 0..4 {
                let (_, mut stream) = incoming
                    .accept()
                    .await
                    .unwrap()
                    .unwrap()
                    .resolve_request()
                    .await
                    .unwrap();
                stream.send_response(Response::<()>::new(())).await.unwrap();
                stream.finish().await.unwrap();
            }
            received.await.unwrap();
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::join!(client, server);
        })
        .await
        .expect("priority requests must complete");
    }
}
