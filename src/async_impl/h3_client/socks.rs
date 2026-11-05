//! SOCKS5 UDP ASSOCIATE adapter for Quinn. Each association has one QUIC peer.
//!
//! A connected UDP socket accepts packets only from the selected relay. The TCP
//! control connection stays open and its closure terminates QUIC. SOCKS5h leaves
//! target DNS to the proxy; local DNS and relay DNS use the client's resolver.
//! Fragmented UDP frames are discarded rather than buffered for reassembly.

use std::future::Future;
use std::io::{self, IoSliceMut};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{ready, Context, Poll};

use quinn::udp::{RecvMeta, Transmit};
use quinn::{AsyncUdpSocket, UdpPoller};
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpSocket, TcpStream, UdpSocket};

use super::dns::resolve_host;
use crate::dns::DynResolver;
use crate::error::BoxError;
use crate::proxy::Intercepted;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Target {
    Ip(SocketAddr),
    Domain(String, u16),
}

impl Target {
    fn port(&self) -> u16 {
        match self {
            Self::Ip(address) => address.port(),
            Self::Domain(_, port) => *port,
        }
    }

    fn encode(&self, bytes: &mut Vec<u8>) -> io::Result<()> {
        match self {
            Self::Ip(SocketAddr::V4(address)) => {
                bytes.push(1);
                bytes.extend_from_slice(&address.ip().octets());
            }
            Self::Ip(SocketAddr::V6(address)) => {
                bytes.push(4);
                bytes.extend_from_slice(&address.ip().octets());
            }
            Self::Domain(host, _) => {
                let length =
                    u8::try_from(host.len()).map_err(|_| invalid("SOCKS5 hostname is too long"))?;
                if length == 0 {
                    return Err(invalid("empty SOCKS5 hostname"));
                }
                bytes.extend_from_slice(&[3, length]);
                bytes.extend_from_slice(host.as_bytes());
            }
        }
        bytes.extend_from_slice(&self.port().to_be_bytes());
        Ok(())
    }

    fn matches_source(&self, source: &Self) -> bool {
        if self.port() != source.port() {
            return false;
        }
        match (self, source) {
            (Self::Ip(expected), Self::Ip(source)) => {
                expected.ip().to_canonical() == source.ip().to_canonical()
            }
            (Self::Domain(expected, _), Self::Domain(source, _)) => {
                expected.eq_ignore_ascii_case(source)
            }
            // With remote DNS, only the proxy knows the resolved target address.
            (Self::Domain(..), Self::Ip(..)) => true,
            _ => false,
        }
    }
}

#[derive(Debug)]
pub(super) struct SocksSocket {
    io: UdpSocket,
    control: TcpStream,
    target: Target,
    peer: SocketAddr,
    receive_buffer: Mutex<Vec<u8>>,
}

impl SocksSocket {
    pub(super) async fn closed(&self) {
        loop {
            if self.control.readable().await.is_err() {
                return;
            }
            match self.control.try_read(&mut [0]) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                _ => return,
            }
        }
    }

    pub(super) async fn connect(
        proxy: &Intercepted,
        target: Target,
        resolver: &mut DynResolver,
        local_address: Option<IpAddr>,
    ) -> Result<(Arc<Self>, SocketAddr), BoxError> {
        let uri = proxy.uri();
        let proxy_addresses = resolve_host(
            resolver,
            uri.host().ok_or("SOCKS5 proxy has no host")?,
            uri.port_u16(),
            1080,
        )
        .await?;
        let mut control = connect_tcp(proxy_addresses, local_address).await?;
        let credentials = proxy.raw_auth();
        let method = if credentials.is_some() { 2 } else { 0 };
        control.write_all(&[5, 1, method]).await?;
        let mut selection = [0; 2];
        control.read_exact(&mut selection).await?;
        if selection != [5, method] {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "SOCKS5 authentication negotiation rejected",
            )
            .into());
        }
        if let Some((username, password)) = credentials {
            let username_len =
                u8::try_from(username.len()).map_err(|_| invalid("SOCKS5 username is too long"))?;
            let password_len =
                u8::try_from(password.len()).map_err(|_| invalid("SOCKS5 password is too long"))?;
            if username_len == 0 || password_len == 0 {
                return Err(invalid("SOCKS5 credentials must not be empty").into());
            }
            let mut authentication = vec![1, username_len];
            authentication.extend_from_slice(username.as_bytes());
            authentication.push(password_len);
            authentication.extend_from_slice(password.as_bytes());
            control.write_all(&authentication).await?;
            control.read_exact(&mut selection).await?;
            if selection != [1, 0] {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "SOCKS5 authentication rejected",
                )
                .into());
            }
        }

        let io = UdpSocket::bind(SocketAddr::new(control.local_addr()?.ip(), 0)).await?;
        let mut associate = vec![5, 3, 0];
        Target::Ip(io.local_addr()?).encode(&mut associate)?;
        control.write_all(&associate).await?;
        let mut reply = [0; 4];
        control.read_exact(&mut reply).await?;
        if reply[0] != 5 || reply[2] != 0 {
            return Err(invalid("invalid SOCKS5 UDP ASSOCIATE response").into());
        }
        if reply[1] != 0 {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                "SOCKS5 UDP ASSOCIATE rejected",
            )
            .into());
        }
        let relay = read_target(&mut control, reply[3]).await?;
        if relay.port() == 0 {
            return Err(invalid("SOCKS5 relay returned port zero").into());
        }
        let relay_addresses = match relay {
            Target::Ip(mut address) => {
                if address.ip().is_unspecified() {
                    address.set_ip(control.peer_addr()?.ip());
                }
                vec![address]
            }
            Target::Domain(host, port) => resolve_host(resolver, &host, Some(port), port).await?,
        };
        let relay = relay_addresses
            .into_iter()
            .find(|address| address.is_ipv4() == io.local_addr().unwrap().is_ipv4())
            .ok_or("SOCKS5 relay has no compatible address")?;
        // A connected UDP socket discards packets from any source but this relay.
        io.connect(relay).await?;
        // Quinn's logical peer must match the relay socket's address family,
        // even when an IPv4 proxy forwards to an IPv6 target or vice versa.
        let virtual_ip: IpAddr = if relay.is_ipv4() {
            Ipv4Addr::new(192, 0, 2, 1).into()
        } else {
            Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1).into()
        };
        let peer = SocketAddr::new(virtual_ip, target.port());
        Ok((
            Arc::new(Self {
                io,
                control,
                target,
                peer,
                receive_buffer: Mutex::new(vec![0; 65_535]),
            }),
            peer,
        ))
    }

    fn poll_control(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut byte = [0];
        let mut buffer = ReadBuf::new(&mut byte);
        match self.control.poll_peek(cx, &mut buffer) {
            Poll::Pending => Poll::Ready(Ok(())),
            Poll::Ready(Ok(0)) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "SOCKS5 UDP association closed",
            ))),
            Poll::Ready(Ok(_)) => Poll::Ready(Err(invalid("unexpected SOCKS5 control data"))),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
        }
    }
}

async fn connect_tcp(
    addresses: Vec<SocketAddr>,
    local_address: Option<IpAddr>,
) -> Result<TcpStream, BoxError> {
    let mut last_error = io::Error::new(
        io::ErrorKind::NotFound,
        "SOCKS5 proxy resolved no compatible addresses",
    );
    for address in addresses {
        if local_address.is_some_and(|local| local.is_ipv4() != address.is_ipv4()) {
            continue;
        }
        let socket = if address.is_ipv4() {
            TcpSocket::new_v4()?
        } else {
            TcpSocket::new_v6()?
        };
        if let Some(local) = local_address {
            socket.bind(SocketAddr::new(local, 0))?;
        }
        match socket.connect(address).await {
            Ok(stream) => return Ok(stream),
            Err(error) => last_error = error,
        }
    }
    Err(last_error.into())
}

async fn read_target(stream: &mut TcpStream, kind: u8) -> io::Result<Target> {
    let length = match kind {
        1 => 4,
        4 => 16,
        3 => stream.read_u8().await? as usize,
        _ => return Err(invalid("invalid SOCKS5 address type")),
    };
    let mut address = vec![0; length];
    stream.read_exact(&mut address).await?;
    let port = stream.read_u16().await?;
    match kind {
        1 => Ok(Target::Ip(SocketAddr::new(
            Ipv4Addr::from(<[u8; 4]>::try_from(address).unwrap()).into(),
            port,
        ))),
        4 => Ok(Target::Ip(SocketAddr::new(
            Ipv6Addr::from(<[u8; 16]>::try_from(address).unwrap()).into(),
            port,
        ))),
        3 if length != 0 => Ok(Target::Domain(
            String::from_utf8(address).map_err(|_| invalid("invalid SOCKS5 hostname"))?,
            port,
        )),
        _ => Err(invalid("empty SOCKS5 address")),
    }
}

fn decode_packet(bytes: &[u8]) -> Option<(Target, &[u8])> {
    // Fragmentation is optional in RFC 1928. Unsupported fragments are dropped.
    if bytes.get(..3)? != [0, 0, 0] {
        return None;
    }
    let (target, end) = match *bytes.get(3)? {
        1 => (
            Target::Ip(SocketAddr::new(
                Ipv4Addr::from(<[u8; 4]>::try_from(bytes.get(4..8)?).ok()?).into(),
                u16::from_be_bytes(bytes.get(8..10)?.try_into().ok()?),
            )),
            10,
        ),
        4 => (
            Target::Ip(SocketAddr::new(
                Ipv6Addr::from(<[u8; 16]>::try_from(bytes.get(4..20)?).ok()?).into(),
                u16::from_be_bytes(bytes.get(20..22)?.try_into().ok()?),
            )),
            22,
        ),
        3 => {
            let length = *bytes.get(4)? as usize;
            if length == 0 {
                return None;
            }
            let host = std::str::from_utf8(bytes.get(5..5 + length)?)
                .ok()?
                .to_owned();
            let port = u16::from_be_bytes(bytes.get(5 + length..7 + length)?.try_into().ok()?);
            (Target::Domain(host, port), 7 + length)
        }
        _ => return None,
    };
    Some((target, &bytes[end..]))
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

impl AsyncUdpSocket for SocksSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(SocksPoller {
            socket: self,
            writable: None,
        })
    }

    fn try_send(&self, transmit: &Transmit<'_>) -> io::Result<()> {
        if transmit.destination != self.peer
            || transmit
                .segment_size
                .is_some_and(|size| size < transmit.contents.len())
        {
            return Err(invalid(
                "invalid SOCKS5 QUIC datagram destination or segmentation",
            ));
        }
        let mut packet = Vec::with_capacity(transmit.contents.len() + 32);
        packet.extend_from_slice(&[0, 0, 0]);
        self.target.encode(&mut packet)?;
        packet.extend_from_slice(transmit.contents);
        let sent = self.io.try_send(&packet)?;
        if sent != packet.len() {
            return Err(io::ErrorKind::WriteZero.into());
        }
        Ok(())
    }

    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        buffers: &mut [IoSliceMut<'_>],
        metadata: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        ready!(self.poll_control(cx))?;
        if buffers.is_empty() || metadata.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let mut bytes = self.receive_buffer.lock().unwrap();
        for _ in 0..32 {
            let mut buffer = ReadBuf::new(&mut bytes);
            ready!(self.io.poll_recv(cx, &mut buffer))?;
            let Some((source, payload)) = decode_packet(buffer.filled()) else {
                continue;
            };
            if !self.target.matches_source(&source)
                || payload.is_empty()
                || payload.len() > buffers[0].len()
            {
                continue;
            }
            buffers[0][..payload.len()].copy_from_slice(payload);
            metadata[0] = RecvMeta {
                addr: self.peer,
                len: payload.len(),
                stride: payload.len(),
                ecn: None,
                dst_ip: None,
            };
            return Poll::Ready(Ok(1));
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.io.local_addr()
    }
}

struct SocksPoller {
    socket: Arc<SocksSocket>,
    writable: Option<Pin<Box<dyn Future<Output = io::Result<()>> + Send + Sync>>>,
}

impl std::fmt::Debug for SocksPoller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SocksPoller").finish_non_exhaustive()
    }
}

impl UdpPoller for SocksPoller {
    fn poll_writable(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        ready!(self.socket.poll_control(cx))?;
        if self.writable.is_none() {
            let socket = self.socket.clone();
            self.writable = Some(Box::pin(async move { socket.io.writable().await }));
        }
        let result = self.writable.as_mut().unwrap().as_mut().poll(cx);
        if result.is_ready() {
            self.writable = None;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packet_codecs_preserve_addresses_and_reject_fragments() {
        for target in [
            Target::Ip("127.0.0.1:443".parse().unwrap()),
            Target::Ip("[::1]:8443".parse().unwrap()),
            Target::Domain("origin.test".into(), 443),
        ] {
            let mut bytes = vec![0, 0, 0];
            target.encode(&mut bytes).unwrap();
            bytes.extend_from_slice(b"quic");
            let (decoded, payload) = decode_packet(&bytes).unwrap();
            assert_eq!(decoded, target);
            assert_eq!(payload, b"quic");
            for length in 0..bytes.len() - 4 {
                assert!(decode_packet(&bytes[..length]).is_none());
            }
            bytes[2] = 1;
            assert!(decode_packet(&bytes).is_none());
        }
    }
}
