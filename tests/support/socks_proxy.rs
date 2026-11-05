use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};

#[derive(Clone, Debug)]
pub struct Destination {
    pub host: String,
    pub port: u16,
    pub domain: bool,
}

impl Destination {
    fn address(&self) -> SocketAddr {
        SocketAddr::new(
            self.host.parse().unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            self.port,
        )
    }
}

pub struct SocksProxy {
    pub addr: SocketAddr,
    pub destinations: Arc<Mutex<Vec<Destination>>>,
    pub associations: Arc<AtomicUsize>,
    pub active: Arc<AtomicUsize>,
    shutdown: Arc<Mutex<Vec<oneshot::Sender<()>>>>,
    task: JoinHandle<()>,
}

impl SocksProxy {
    pub async fn new(credentials: Option<(&str, &str)>, reject_udp: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let credentials = credentials.map(|(user, pass)| (user.to_owned(), pass.to_owned()));
        let destinations = Arc::new(Mutex::new(Vec::new()));
        let associations = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let shutdown = Arc::new(Mutex::new(Vec::new()));
        let task = {
            let destinations = destinations.clone();
            let associations = associations.clone();
            let active = active.clone();
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                let mut tasks = JoinSet::new();
                loop {
                    tokio::select! {
                        accepted = listener.accept() => {
                            let (mut control, _) = accepted.unwrap();
                            let credentials = credentials.clone();
                            let destinations = destinations.clone();
                            let associations = associations.clone();
                            let active = active.clone();
                            let shutdown = shutdown.clone();
                            tasks.spawn(async move {
                                let mut greeting = [0; 2];
                                if control.read_exact(&mut greeting).await.is_err() { return; }
                                assert_eq!(greeting[0], 5);
                                let mut methods = vec![0; greeting[1] as usize];
                                control.read_exact(&mut methods).await.unwrap();
                                let method = if credentials.is_some() { 2 } else { 0 };
                                if !methods.contains(&method) {
                                    let _ = control.write_all(&[5, 255]).await;
                                    return;
                                }
                                control.write_all(&[5, method]).await.unwrap();
                                if let Some((user, pass)) = credentials {
                                    assert_eq!(control.read_u8().await.unwrap(), 1);
                                    let n = control.read_u8().await.unwrap() as usize;
                                    let mut supplied_user = vec![0; n];
                                    control.read_exact(&mut supplied_user).await.unwrap();
                                    let n = control.read_u8().await.unwrap() as usize;
                                    let mut supplied_pass = vec![0; n];
                                    control.read_exact(&mut supplied_pass).await.unwrap();
                                    let valid = supplied_user == user.as_bytes() && supplied_pass == pass.as_bytes();
                                    control.write_all(&[1, if valid { 0 } else { 1 }]).await.unwrap();
                                    if !valid { return; }
                                }
                                let mut header = [0; 4];
                                if control.read_exact(&mut header).await.is_err() { return; }
                                assert_eq!(header[0], 5);
                                let destination = read_destination(&mut control, header[3]).await;
                                if header[1] == 1 {
                                    destinations.lock().unwrap().push(destination.clone());
                                    let Ok(mut target) = TcpStream::connect(destination.address()).await else { return; };
                                    let mut reply = vec![5, 0, 0];
                                    encode_address(target.local_addr().unwrap(), &mut reply);
                                    control.write_all(&reply).await.unwrap();
                                    let _ = tokio::io::copy_bidirectional(&mut control, &mut target).await;
                                    return;
                                }
                                assert_eq!(header[1], 3);
                                if reject_udp {
                                    control.write_all(&[5, 7, 0, 1, 0, 0, 0, 0, 0, 0]).await.unwrap();
                                    return;
                                }
                                let relay = UdpSocket::bind("127.0.0.1:0").await.unwrap();
                                let mut reply = vec![5, 0, 0];
                                encode_address(relay.local_addr().unwrap(), &mut reply);
                                control.write_all(&reply).await.unwrap();
                                associations.fetch_add(1, Ordering::SeqCst);
                                active.fetch_add(1, Ordering::SeqCst);
                                let (stop, mut stopped) = oneshot::channel();
                                shutdown.lock().unwrap().push(stop);
                                let mut client = None;
                                let mut target = None;
                                let mut bytes = vec![0; 65_535];
                                let mut probe = [0];
                                let forward_v6 = UdpSocket::bind("[::1]:0").await.unwrap();
                                let mut ipv6_bytes = vec![0; 65_535];
                                loop {
                                    tokio::select! {
                                        _ = &mut stopped => break,
                                        _ = control.read(&mut probe) => break,
                                        packet = forward_v6.recv_from(&mut ipv6_bytes) => {
                                            let (length, source) = packet.unwrap();
                                            if Some(source) == target {
                                                let mut response = vec![0, 0, 0];
                                                encode_address(source, &mut response);
                                                response.extend_from_slice(&ipv6_bytes[..length]);
                                                let _ = relay.send_to(&response, client.unwrap()).await;
                                            }
                                        }
                                        packet = relay.recv_from(&mut bytes) => {
                                            let (length, source) = packet.unwrap();
                                            let source = SocketAddr::new(source.ip().to_canonical(), source.port());
                                            if Some(source) == target {
                                                let mut response = vec![0, 0, 0];
                                                encode_address(source, &mut response);
                                                response.extend_from_slice(&bytes[..length]);
                                                let _ = relay.send_to(&response, client.unwrap()).await;
                                            } else if client.is_none() || client == Some(source) {
                                                let Some((destination, offset)) = decode(&bytes[..length]) else { continue; };
                                                destinations.lock().unwrap().push(destination.clone());
                                                target = Some(destination.address());
                                                client = Some(source);
                                                if target.unwrap().is_ipv6() {
                                                    let _ = forward_v6.send_to(&bytes[offset..length], target.unwrap()).await;
                                                } else {
                                                    let _ = relay.send_to(&bytes[offset..length], target.unwrap()).await;
                                                }
                                            }
                                        }
                                    }
                                }
                                active.fetch_sub(1, Ordering::SeqCst);
                            });
                        }
                        Some(result) = tasks.join_next() => { result.unwrap(); }
                    }
                }
            })
        };
        Self {
            addr,
            destinations,
            associations,
            active,
            shutdown,
            task,
        }
    }

    pub fn close_associations(&self) {
        for stop in self.shutdown.lock().unwrap().drain(..) {
            let _ = stop.send(());
        }
    }
}

impl Drop for SocksProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn read_destination(stream: &mut TcpStream, kind: u8) -> Destination {
    let host = match kind {
        1 => {
            let mut address = [0; 4];
            stream.read_exact(&mut address).await.unwrap();
            Ipv4Addr::from(address).to_string()
        }
        4 => {
            let mut address = [0; 16];
            stream.read_exact(&mut address).await.unwrap();
            Ipv6Addr::from(address).to_string()
        }
        3 => {
            let n = stream.read_u8().await.unwrap();
            let mut address = vec![0; n as usize];
            stream.read_exact(&mut address).await.unwrap();
            String::from_utf8(address).unwrap()
        }
        _ => panic!("invalid SOCKS5 address type"),
    };
    Destination {
        host,
        port: stream.read_u16().await.unwrap(),
        domain: kind == 3,
    }
}

fn encode_address(address: SocketAddr, bytes: &mut Vec<u8>) {
    match address.ip() {
        IpAddr::V4(ip) => {
            bytes.push(1);
            bytes.extend_from_slice(&ip.octets());
        }
        IpAddr::V6(ip) => {
            bytes.push(4);
            bytes.extend_from_slice(&ip.octets());
        }
    }
    bytes.extend_from_slice(&address.port().to_be_bytes());
}

fn decode(bytes: &[u8]) -> Option<(Destination, usize)> {
    if bytes.get(..3)? != [0, 0, 0] {
        return None;
    }
    let kind = *bytes.get(3)?;
    let (host, end) = match kind {
        1 => (
            Ipv4Addr::from(<[u8; 4]>::try_from(bytes.get(4..8)?).ok()?).to_string(),
            8,
        ),
        4 => (
            Ipv6Addr::from(<[u8; 16]>::try_from(bytes.get(4..20)?).ok()?).to_string(),
            20,
        ),
        3 => {
            let n = *bytes.get(4)? as usize;
            (
                std::str::from_utf8(bytes.get(5..5 + n)?).ok()?.to_owned(),
                5 + n,
            )
        }
        _ => return None,
    };
    let port = u16::from_be_bytes(bytes.get(end..end + 2)?.try_into().ok()?);
    Some((
        Destination {
            host,
            port,
            domain: kind == 3,
        },
        end + 2,
    ))
}
