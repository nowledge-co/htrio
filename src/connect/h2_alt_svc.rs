//! Observe authenticated HTTP/2 ALTSVC frames while leaving Hyper's codec intact.

use std::io::{self, IoSlice};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use http::{HeaderMap, HeaderValue, StatusCode, Uri};
use hyper::rt::{Read, ReadBuf, ReadBufCursor, Write};
use hyper_util::client::legacy::connect::{Connected, Connection};

use super::{BoxConn, Conn};
use crate::alt_svc::AltSvcCache;

const MAX_PAYLOAD: usize = 16 * 1024;

pub(super) fn observe(mut connection: Conn, origin: Uri, cache: Arc<AltSvcCache>) -> Conn {
    if origin.scheme_str() == Some("https") && connection.inner.connected().is_negotiated_h2() {
        connection.inner = Box::new(Observed {
            inner: connection.inner,
            observer: Observer::new(origin, cache),
        });
    }
    connection
}

struct Observed {
    inner: BoxConn,
    observer: Observer,
}
impl Connection for Observed {
    fn connected(&self) -> Connected {
        self.inner.connected()
    }
}
#[cfg(feature = "__tls")]
impl super::TlsInfoFactory for Observed {
    fn tls_info(&self) -> Option<crate::tls::TlsInfo> {
        self.inner.tls_info()
    }
}
impl Read for Observed {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut destination: ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        // The nested ReadBuf reports exactly which caller-owned bytes became
        // initialized. Observe those bytes without copying the TCP data path.
        let mut received = ReadBuf::uninit(unsafe { destination.as_mut() });
        match Pin::new(&mut this.inner).poll_read(cx, received.unfilled()) {
            Poll::Ready(Ok(())) => {
                let count = received.filled().len();
                this.observer.received(received.filled());
                // The inner reader initialized and filled this exact prefix.
                unsafe {
                    destination.advance(count);
                }
                Poll::Ready(Ok(()))
            }
            result => result,
        }
    }
}
impl Write for Observed {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_write(cx, data) {
            Poll::Ready(Ok(count)) => {
                this.observer.sent(&data[..count]);
                Poll::Ready(Ok(count))
            }
            result => result,
        }
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_write_vectored(cx, data) {
            Poll::Ready(Ok(count)) => {
                let mut left = count;
                for part in data {
                    let used = left.min(part.len());
                    this.observer.sent(&part[..used]);
                    left -= used;
                    if left == 0 {
                        break;
                    }
                }
                Poll::Ready(Ok(count))
            }
            result => result,
        }
    }
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

struct Observer {
    origin: Uri,
    cache: Arc<AltSvcCache>,
    receive: Frames,
    send: Frames,
    preface_left: usize,
    highest_request: u32,
    settings_received: Option<bool>,
}
impl Observer {
    fn new(origin: Uri, cache: Arc<AltSvcCache>) -> Self {
        Self {
            origin,
            cache,
            receive: Frames::new(true),
            send: Frames::new(false),
            preface_left: 24,
            highest_request: 0,
            settings_received: None,
        }
    }
    fn sent(&mut self, mut bytes: &[u8]) {
        let preface = self.preface_left.min(bytes.len());
        self.preface_left -= preface;
        bytes = &bytes[preface..];
        let highest = &mut self.highest_request;
        self.send.feed(bytes, |kind, _, stream, _| {
            if kind == 1 && stream & 1 == 1 {
                *highest = (*highest).max(stream);
            }
        });
    }
    fn received(&mut self, bytes: &[u8]) {
        let ready = &mut self.settings_received;
        let highest = self.highest_request;
        let origin = &self.origin;
        let cache = &self.cache;
        self.receive.feed(bytes, |kind, flags, stream, payload| {
            if ready.is_none() {
                *ready = Some(kind == 4 && stream == 0 && flags & 1 == 0);
            }
            if *ready != Some(true) || kind != 0x0a {
                return;
            }
            let Some(payload) = payload else {
                return;
            };
            if payload.len() < 2 {
                return;
            }
            let length = usize::from(u16::from_be_bytes([payload[0], payload[1]]));
            if length > payload.len() - 2 {
                return;
            }
            if stream == 0 {
                if length == 0 {
                    return;
                }
                let Ok(name) = std::str::from_utf8(&payload[2..2 + length]) else {
                    return;
                };
                let Ok(named) = name.parse::<Uri>() else {
                    return;
                };
                if named.scheme_str() != Some("https")
                    || named.host().map(str::to_ascii_lowercase)
                        != origin.host().map(str::to_ascii_lowercase)
                    || named.port_u16().unwrap_or(443) != origin.port_u16().unwrap_or(443)
                {
                    return;
                }
            } else if length != 0 || stream & 1 == 0 || stream > highest {
                return;
            }
            let Ok(value) = HeaderValue::from_bytes(&payload[2 + length..]) else {
                return;
            };
            let mut headers = HeaderMap::new();
            headers.insert("alt-svc", value);
            cache.observe(origin, StatusCode::OK, &headers, Duration::ZERO);
        });
    }
}

struct Frames {
    header: [u8; 9],
    used: usize,
    remaining: usize,
    kind: u8,
    flags: u8,
    stream: u32,
    payload: Vec<u8>,
    capture: bool,
    keep: bool,
}
impl Frames {
    fn new(capture: bool) -> Self {
        Self {
            header: [0; 9],
            used: 0,
            remaining: 0,
            kind: 0,
            flags: 0,
            stream: 0,
            payload: Vec::new(),
            capture,
            keep: false,
        }
    }
    fn feed(&mut self, mut bytes: &[u8], mut frame: impl FnMut(u8, u8, u32, Option<&[u8]>)) {
        while !bytes.is_empty() {
            if self.used < 9 {
                let count = (9 - self.used).min(bytes.len());
                self.header[self.used..self.used + count].copy_from_slice(&bytes[..count]);
                self.used += count;
                bytes = &bytes[count..];
                if self.used < 9 {
                    return;
                }
                self.remaining = (usize::from(self.header[0]) << 16)
                    | (usize::from(self.header[1]) << 8)
                    | usize::from(self.header[2]);
                self.kind = self.header[3];
                self.flags = self.header[4];
                self.stream =
                    u32::from_be_bytes(self.header[5..9].try_into().unwrap()) & 0x7fff_ffff;
                self.keep = self.capture && self.kind == 0x0a && self.remaining <= MAX_PAYLOAD;
                self.payload.clear();
            }
            let count = self.remaining.min(bytes.len());
            if self.keep {
                self.payload.extend_from_slice(&bytes[..count]);
            }
            self.remaining -= count;
            bytes = &bytes[count..];
            if self.remaining == 0 {
                frame(
                    self.kind,
                    self.flags,
                    self.stream,
                    self.keep.then_some(self.payload.as_slice()),
                );
                self.used = 0;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn frame(kind: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
        let mut wire = vec![
            ((payload.len() >> 16) & 255) as u8,
            ((payload.len() >> 8) & 255) as u8,
            (payload.len() & 255) as u8,
            kind,
            0,
        ];
        wire.extend_from_slice(&stream.to_be_bytes());
        wire.extend_from_slice(payload);
        wire
    }
    fn alternative(origin: &str) -> Vec<u8> {
        let mut payload = (origin.len() as u16).to_be_bytes().to_vec();
        payload.extend_from_slice(origin.as_bytes());
        payload.extend_from_slice(b"h3=\":443\";ma=60");
        payload
    }
    #[test]
    fn fragmented_frames_bind_advertisements_to_authenticated_origins_and_requests() {
        let cache = Arc::new(AltSvcCache::new());
        let mut observer = Observer::new("https://origin.test/".parse().unwrap(), cache.clone());
        observer.received(&frame(4, 0, &[]));
        observer.received(&frame(0x0a, 0, &alternative("https://other.test")));
        observer.received(&frame(0x0a, 1, &alternative("")));
        assert!(cache.is_empty());
        observer.sent(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
        for bytes in frame(1, 1, b"headers").chunks(2) {
            observer.sent(bytes);
        }
        for bytes in frame(0x0a, 1, &alternative("")).chunks(1) {
            observer.received(bytes);
        }
        assert_eq!(cache.len(), 1);
        cache.clear();
        observer.received(&frame(0x0a, 0, &alternative("https://origin.test")));
        assert_eq!(cache.len(), 1);
    }
    #[test]
    fn oversized_frames_and_missing_initial_settings_do_not_change_cache_state() {
        let cache = Arc::new(AltSvcCache::new());
        let mut observer = Observer::new("https://origin.test/".parse().unwrap(), cache.clone());
        observer.received(&frame(0x0a, 0, &alternative("https://origin.test")));
        observer.received(&frame(4, 0, &[]));
        observer.received(&frame(0x0a, 0, &alternative("https://origin.test")));
        assert!(cache.is_empty());
        let mut observer = Observer::new("https://origin.test/".parse().unwrap(), cache.clone());
        observer.received(&frame(4, 0, &[]));
        for bytes in frame(0x0a, 0, &vec![0; MAX_PAYLOAD + 1]).chunks(100) {
            observer.received(bytes);
        }
        assert!(observer.receive.payload.is_empty());
        assert!(cache.is_empty());
    }
}
