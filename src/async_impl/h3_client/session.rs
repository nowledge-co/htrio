//! Route-scoped TLS resumption and an explicit early-data acceptance barrier.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rustls::client::{
    ClientSessionMemoryCache, ClientSessionStore, Tls12ClientSessionValue, Tls13ClientSessionValue,
};
use rustls::pki_types::ServerName;
use tokio::sync::watch;

use crate::error::BoxError;

pub(super) struct RouteState {
    pub(super) tls: Arc<rustls::ClientConfig>,
    pub(super) settings: Mutex<Option<h3::PeerSettings>>,
    pub(super) quic: Mutex<Option<(Option<Vec<u8>>, quinn::ClientConfig)>>,
    pub(super) last_peer: Mutex<Option<std::net::SocketAddr>>,
    tickets: Arc<Mutex<usize>>,
    attempts: AtomicUsize,
    rejections: AtomicUsize,
}

impl RouteState {
    pub(super) fn new(base: &rustls::ClientConfig, partition: bool) -> Self {
        let mut tls = base.clone();
        let tickets = Arc::new(Mutex::new(0));
        if partition {
            tls.resumption = rustls::client::Resumption::store(Arc::new(TicketStore {
                inner: ClientSessionMemoryCache::new(4),
                tickets: tickets.clone(),
            }));
        }
        Self {
            tls: Arc::new(tls),
            settings: Mutex::new(None),
            quic: Mutex::new(None),
            last_peer: Mutex::new(None),
            tickets,
            attempts: AtomicUsize::new(0),
            rejections: AtomicUsize::new(0),
        }
    }

    pub(super) fn stats(&self) -> crate::http3::SessionStats {
        crate::http3::SessionStats {
            cached_tickets: *self.tickets.lock().unwrap(),
            settings_cached: self.settings.lock().unwrap().is_some(),
            early_attempts: self.attempts.load(Ordering::Relaxed),
            early_rejections: self.rejections.load(Ordering::Relaxed),
        }
    }
}

struct TicketStore {
    inner: ClientSessionMemoryCache,
    tickets: Arc<Mutex<usize>>,
}
impl std::fmt::Debug for TicketStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TicketStore").finish_non_exhaustive()
    }
}
impl ClientSessionStore for TicketStore {
    fn set_kx_hint(&self, name: ServerName<'static>, group: rustls::NamedGroup) {
        self.inner.set_kx_hint(name, group);
    }
    fn kx_hint(&self, name: &ServerName<'_>) -> Option<rustls::NamedGroup> {
        self.inner.kx_hint(name)
    }
    fn set_tls12_session(&self, name: ServerName<'static>, value: Tls12ClientSessionValue) {
        self.inner.set_tls12_session(name, value);
    }
    fn tls12_session(&self, name: &ServerName<'_>) -> Option<Tls12ClientSessionValue> {
        self.inner.tls12_session(name)
    }
    fn remove_tls12_session(&self, name: &ServerName<'static>) {
        self.inner.remove_tls12_session(name);
    }
    fn insert_tls13_ticket(&self, name: ServerName<'static>, value: Tls13ClientSessionValue) {
        self.inner.insert_tls13_ticket(name, value);
        let mut count = self.tickets.lock().unwrap();
        *count = count.saturating_add(1).min(8);
    }
    fn take_tls13_ticket(&self, name: &ServerName<'static>) -> Option<Tls13ClientSessionValue> {
        let ticket = self.inner.take_tls13_ticket(name);
        if ticket.is_some() {
            let mut count = self.tickets.lock().unwrap();
            *count = count.saturating_sub(1);
        }
        ticket
    }
}

#[derive(Clone)]
pub(crate) struct EarlyData {
    pub(super) route: Arc<RouteState>,
    pub(super) attempted: bool,
    status: watch::Receiver<Option<bool>>,
}

impl EarlyData {
    pub(super) async fn connect(
        connecting: quinn::Connecting,
        route: Arc<RouteState>,
        allow: bool,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<(quinn::Connection, Self), BoxError> {
        let remembered = *route.settings.lock().unwrap();
        let connecting = if allow
            && route.tls.enable_early_data
            && !route.tls.client_auth_cert_resolver.has_certs()
            && remembered.is_some()
            && *route.last_peer.lock().unwrap() == Some(connecting.remote_address())
        {
            match connecting.into_0rtt() {
                Ok((connection, accepted)) => {
                    route.attempts.fetch_add(1, Ordering::Relaxed);
                    let (tx, status) = watch::channel(None);
                    let transport = connection.clone();
                    let tracked = route.clone();
                    tokio::spawn(async move {
                        tokio::select! {
                            accepted = async { match deadline { Some(deadline) => tokio::time::timeout_at(deadline, accepted).await.unwrap_or(false), None => accepted.await } } => {
                                if !accepted { tracked.rejections.fetch_add(1, Ordering::Relaxed); transport.close(0_u32.into(), b"early data rejected"); }
                                let _ = tx.send(Some(accepted));
                            }
                            _ = tx.closed() => transport.close(0_u32.into(), b"early connection cancelled"),
                        }
                    });
                    return Ok((
                        connection,
                        Self {
                            route,
                            attempted: true,
                            status,
                        },
                    ));
                }
                Err(connecting) => connecting,
            }
        } else {
            connecting
        };
        let connection = connecting.await?;
        let (_, status) = watch::channel(Some(true));
        Ok((
            connection,
            Self {
                route,
                attempted: false,
                status,
            },
        ))
    }

    pub(super) async fn accepted(&mut self) -> bool {
        if let Some(accepted) = *self.status.borrow() {
            return accepted;
        }
        match self.status.wait_for(Option::is_some).await {
            Ok(accepted) => accepted.unwrap_or(false),
            Err(_) => false,
        }
    }
    pub(crate) fn stats(&self) -> crate::http3::SessionStats {
        self.route.stats()
    }
    pub(super) fn pending(&self) -> bool {
        self.attempted && self.status.borrow().is_none()
    }
    pub(super) fn rejected(&self) -> bool {
        self.attempted && *self.status.borrow() == Some(false)
    }
    pub(crate) fn status(&self) -> crate::http3::EarlyDataStatus {
        use crate::http3::EarlyDataStatus::*;
        if !self.attempted {
            NotAttempted
        } else {
            match *self.status.borrow() {
                None => Pending,
                Some(true) => Accepted,
                Some(false) => Rejected,
            }
        }
    }
}

#[derive(Debug)]
pub(super) struct EarlyDataRejected;
impl std::fmt::Display for EarlyDataRejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HTTP/3 early data requires a confirmed retry")
    }
}
impl std::error::Error for EarlyDataRejected {}
