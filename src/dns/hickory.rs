//! DNS resolution via the [hickory-resolver](https://github.com/hickory-dns/hickory-dns) crate

use hickory_resolver::{
    config::{LookupIpStrategy, ResolverConfig, GOOGLE},
    net::{runtime::TokioRuntimeProvider, NetError},
    TokioResolver,
};
use once_cell::sync::OnceCell;

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use super::{Addrs, Name, Resolve, Resolving};

/// Wrapper around an `AsyncResolver`, which implements the `Resolve` trait.
#[derive(Debug, Default, Clone)]
pub(crate) struct HickoryDnsResolver {
    /// Since we might not have been called in the context of a
    /// Tokio Runtime in initialization, so we must delay the actual
    /// construction of the resolver.
    state: Arc<OnceCell<TokioResolver>>,
}

struct SocketAddrs {
    iter: std::vec::IntoIter<IpAddr>,
}

impl Resolve for HickoryDnsResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let resolver = self.clone();
        Box::pin(async move {
            let resolver = resolver.state.get_or_try_init(new_resolver)?;

            let lookup = resolver.lookup_ip(name.as_str()).await?;
            let addrs: Addrs = Box::new(SocketAddrs {
                iter: lookup.iter().collect::<Vec<_>>().into_iter(),
            });
            Ok(addrs)
        })
    }
    fn resolve_service(&self, name: Name, port: u16) -> super::ServiceResolving {
        use hickory_resolver::proto::rr::rdata::svcb::SvcParamValue;
        use hickory_resolver::proto::rr::{RData, RecordType};
        let resolver = self.clone();
        let host = name.as_str().to_owned();
        Box::pin(async move {
            let resolver = resolver.state.get_or_try_init(new_resolver)?;
            let mut owner = if port == 443 {
                host
            } else {
                format!("_{port}._https.{host}")
            };
            let mut visited = std::collections::HashSet::new();
            let mut chain_ttl = std::time::Duration::from_secs(86_400);
            for _ in 0..8 {
                if !visited.insert(owner.clone()) {
                    return Err("HTTPS alias loop".into());
                }
                let lookup = match resolver.lookup(owner.clone(), RecordType::HTTPS).await {
                    Ok(lookup) => lookup,
                    Err(_) => return Ok(Vec::new()),
                };
                chain_ttl = chain_ttl.min(
                    lookup
                        .valid_until()
                        .saturating_duration_since(std::time::Instant::now()),
                );
                let records = lookup
                    .answers()
                    .iter()
                    .filter_map(|record| match &record.data {
                        RData::HTTPS(data) => Some((&record.name, &data.0)),
                        _ => None,
                    })
                    .take(16)
                    .collect::<Vec<_>>();
                if let Some((_, alias)) =
                    records.iter().find(|(_, record)| record.svc_priority == 0)
                {
                    if alias.target_name.is_root() {
                        return Ok(Vec::new());
                    }
                    owner = alias.target_name.to_utf8();
                    continue;
                }
                let mut endpoints = Vec::new();
                for (record_owner, record) in records {
                    let target = if record.target_name.is_root() {
                        let owner = record_owner.to_utf8();
                        let owner = owner.trim_end_matches('.');
                        owner
                            .strip_prefix(&format!("_{port}._https."))
                            .unwrap_or(owner)
                            .to_owned()
                    } else {
                        record
                            .target_name
                            .to_utf8()
                            .trim_end_matches('.')
                            .to_owned()
                    };
                    let mut endpoint = super::ServiceEndpoint::http3(target, port);
                    endpoint.alpn.clear();
                    endpoint.priority = record.svc_priority;
                    endpoint.ttl = chain_ttl;
                    let mut usable = true;
                    for (_, value) in &record.svc_params {
                        match value {
                            SvcParamValue::Mandatory(required) => {
                                usable &= required.0.iter().all(|key| {
                                    record.svc_params.iter().any(|(found, value)| {
                                        found == key && !matches!(value, SvcParamValue::Unknown(_))
                                    })
                                });
                            }
                            SvcParamValue::Alpn(alpn) => {
                                endpoint.alpn = alpn
                                    .0
                                    .iter()
                                    .map(|value| value.as_bytes().to_vec())
                                    .collect()
                            }
                            SvcParamValue::Port(port) => endpoint.port = *port,
                            SvcParamValue::Ipv4Hint(hints) => endpoint.addresses.extend(
                                hints
                                    .0
                                    .iter()
                                    .map(|ip| SocketAddr::new(IpAddr::V4(ip.0), endpoint.port)),
                            ),
                            SvcParamValue::Ipv6Hint(hints) => endpoint.addresses.extend(
                                hints
                                    .0
                                    .iter()
                                    .map(|ip| SocketAddr::new(IpAddr::V6(ip.0), endpoint.port)),
                            ),
                            SvcParamValue::EchConfigList(ech) => {
                                endpoint.ech_config_list = Some(ech.0.clone())
                            }
                            _ => {}
                        }
                    }
                    if usable
                        && endpoint.port != 0
                        && endpoint.alpn.iter().any(|alpn| alpn == b"h3")
                    {
                        for address in &mut endpoint.addresses {
                            address.set_port(endpoint.port);
                        }
                        endpoints.push(endpoint);
                    }
                }
                endpoints.sort_by_key(|endpoint| endpoint.priority);
                return Ok(endpoints);
            }
            Err("HTTPS alias chain exceeds limit".into())
        })
    }
}

impl Iterator for SocketAddrs {
    type Item = SocketAddr;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|ip_addr| SocketAddr::new(ip_addr, 0))
    }
}

/// Create a new resolver with the default configuration,
/// which reads from `/etc/resolve.conf`. If reading `/etc/resolv.conf` fails,
/// it fallbacks to hickory_resolver's default config.
/// The options are overridden to look up for both IPv4 and IPv6 addresses
/// to work with "happy eyeballs" algorithm.
fn new_resolver() -> Result<TokioResolver, NetError> {
    let mut builder = TokioResolver::builder_tokio().unwrap_or_else(|err| {
        log::debug!(
            "hickory-dns: failed to load system DNS configuration; falling back to Google DNS: {:?}",
            err
        );
        TokioResolver::builder_with_config(
            ResolverConfig::udp_and_tcp(&GOOGLE),
            TokioRuntimeProvider::default(),
        )
    });
    builder.options_mut().ip_strategy = LookupIpStrategy::Ipv4AndIpv6;
    builder.build()
}
