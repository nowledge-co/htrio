#![allow(dead_code)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::task::{JoinHandle, JoinSet};

/// Hold each new path's server flight so the client can dispatch real 0-RTT.
pub struct Relay {
    pub address: SocketAddr,
    task: JoinHandle<()>,
}
impl Relay {
    pub async fn new(target: SocketAddr, delay: Duration) -> Self {
        let downstream = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let address = downstream.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let mut paths = HashMap::<SocketAddr, Arc<UdpSocket>>::new();
            let mut readers = JoinSet::new();
            let mut packet = vec![0; 65536];
            loop {
                let (length, client) = downstream.recv_from(&mut packet).await.unwrap();
                let upstream = match paths.get(&client) {
                    Some(socket) => socket.clone(),
                    None => {
                        assert!(paths.len() < 16, "test path bound exceeded");
                        let upstream = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
                        upstream.connect(target).await.unwrap();
                        paths.insert(client, upstream.clone());
                        let source = upstream.clone();
                        let downstream = downstream.clone();
                        let ready = tokio::time::Instant::now() + delay;
                        readers.spawn(async move {
                            let mut response = vec![0; 65536];
                            loop {
                                let length = source.recv(&mut response).await.unwrap();
                                tokio::time::sleep_until(ready).await;
                                downstream
                                    .send_to(&response[..length], client)
                                    .await
                                    .unwrap();
                            }
                        });
                        upstream
                    }
                };
                upstream.send(&packet[..length]).await.unwrap();
            }
        });
        Self { address, task }
    }
}
impl Drop for Relay {
    fn drop(&mut self) {
        self.task.abort();
    }
}
