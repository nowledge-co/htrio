//! Bounded, origin-scoped HTTP/3 alternative service cache (RFC 7838).
//!
//! RFC 7838 discovery requires authenticated HTTPS responses. A preconfigured
//! TLS verifier must enforce origin authentication. Share a cache with
//! [`crate::ClientBuilder::alt_svc_cache`]. Call
//! [`AltSvcCache::network_changed`] when the application detects a network change.
//! Cache contents are process-local and are never written to disk.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use http::uri::Authority;
use http::{HeaderMap, StatusCode, Uri};

const DEFAULT_MAX_AGE: u64 = 86_400;
const MAX_HEADER_BYTES: usize = 16 * 1024;
const MAX_ALTERNATIVES: usize = 8;
const BROKEN_BACKOFF: Duration = Duration::from_secs(300);

/// A bounded HTTP/3 alternative service cache shared by cloned clients.
///
/// Entries are scoped to the HTTPS origin's host and effective port. Cache
/// controls affect future route selection; they do not close active connections.
/// HTTP/2 ALTSVC frames and disk persistence are not implemented.
#[derive(Clone, Debug)]
pub struct AltSvcCache {
    inner: Arc<Mutex<State>>,
    capacity: usize,
}

#[derive(Debug, Default)]
struct State {
    origins: HashMap<String, Entry>,
    sequence: u64,
}

#[derive(Debug)]
struct Entry {
    alternatives: Vec<Alternative>,
    sequence: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct Alternative {
    pub(crate) authority: Authority,
    expires: Instant,
    persist: bool,
    broken_until: Option<Instant>,
}

impl Default for AltSvcCache {
    fn default() -> Self {
        Self::with_capacity(256)
    }
}

impl AltSvcCache {
    /// Create a cache holding at most 256 origins and eight alternatives per origin.
    pub fn new() -> Self {
        Self::default()
    }

    /// Limit the number of cached origins. A zero capacity disables storage.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(State::default())),
            capacity,
        }
    }

    /// Remove every cached alternative, including persistent alternatives.
    pub fn clear(&self) {
        self.inner.lock().unwrap().origins.clear();
    }

    /// Discard expired alternatives and those without `persist=1`.
    ///
    /// Network change detection is left to the embedding application.
    /// Persistent alternatives keep their existing expiry and failure backoff.
    pub fn network_changed(&self) {
        let now = Instant::now();
        self.inner.lock().unwrap().origins.retain(|_, entry| {
            entry
                .alternatives
                .retain(|alt| alt.persist && alt.expires > now);
            !entry.alternatives.is_empty()
        });
    }

    /// Return the number of origins with unexpired alternatives.
    pub fn len(&self) -> usize {
        let mut state = self.inner.lock().unwrap();
        prune(&mut state, Instant::now());
        state.origins.len()
    }

    /// Return whether the cache contains no unexpired alternatives.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn get(&self, origin: &Uri) -> Option<Alternative> {
        let key = origin_key(origin)?;
        let now = Instant::now();
        let mut state = self.inner.lock().unwrap();
        prune(&mut state, now);
        state
            .origins
            .get(&key)?
            .alternatives
            .iter()
            .find_map(|alt| {
                if alt.broken_until.is_some_and(|until| until > now) {
                    None
                } else {
                    Some(alt.clone())
                }
            })
    }

    pub(crate) fn mark_broken(&self, origin: &Uri, alternative: &Alternative) {
        let Some(key) = origin_key(origin) else {
            return;
        };
        if let Some(entry) = self.inner.lock().unwrap().origins.get_mut(&key) {
            for alt in &mut entry.alternatives {
                if alt.authority == alternative.authority {
                    alt.broken_until = Some(Instant::now() + BROKEN_BACKOFF);
                }
            }
        }
    }

    pub(crate) fn remove(&self, origin: &Uri, alternative: &Alternative) {
        let Some(key) = origin_key(origin) else {
            return;
        };
        let mut state = self.inner.lock().unwrap();
        if let Some(entry) = state.origins.get_mut(&key) {
            entry
                .alternatives
                .retain(|alt| alt.authority != alternative.authority);
            if entry.alternatives.is_empty() {
                state.origins.remove(&key);
            }
        }
    }

    pub(crate) fn observe(
        &self,
        origin: &Uri,
        status: StatusCode,
        headers: &HeaderMap,
        response_delay: Duration,
    ) {
        if self.capacity == 0 || status == StatusCode::MISDIRECTED_REQUEST {
            return;
        }
        let Some(key) = origin_key(origin) else {
            return;
        };
        if !headers.contains_key("alt-svc") {
            return;
        }

        let now = Instant::now();
        let age = response_age(headers, response_delay);
        let mut alternatives = Vec::new();
        let mut bytes = 0_usize;
        let values = headers.get_all("alt-svc");
        let mut clear = false;
        for value in &values {
            bytes = bytes.saturating_add(value.as_bytes().len());
            if bytes > MAX_HEADER_BYTES {
                return;
            }
            let Ok(value) = value.to_str() else {
                return;
            };
            if value.trim() == "clear" {
                clear = true;
                continue;
            }
            let Some(items) = split_quoted(value, b',') else {
                return;
            };
            for item in items {
                if item.trim().is_empty() {
                    continue;
                }
                let Some(alt) = parse_alternative(origin, item, age, now) else {
                    continue;
                };
                if alternatives.len() < MAX_ALTERNATIVES
                    && !alternatives
                        .iter()
                        .any(|old: &Alternative| old.authority == alt.authority)
                {
                    alternatives.push(alt);
                }
            }
        }

        let mut state = self.inner.lock().unwrap();
        prune(&mut state, now);
        // A new field replaces the complete set, even if none of its protocols
        // are supported. Re-advertising a broken endpoint must not erase backoff.
        if let Some(old) = state.origins.remove(&key) {
            for alt in &mut alternatives {
                if let Some(previous) = old
                    .alternatives
                    .iter()
                    .find(|previous| previous.authority == alt.authority)
                {
                    alt.broken_until = previous.broken_until;
                }
            }
        }
        if clear || alternatives.is_empty() {
            return;
        }
        if state.origins.len() >= self.capacity {
            if let Some(oldest) = state
                .origins
                .iter()
                .min_by_key(|(_, entry)| entry.sequence)
                .map(|(key, _)| key.clone())
            {
                state.origins.remove(&oldest);
            }
        }
        state.sequence = state.sequence.wrapping_add(1);
        let sequence = state.sequence;
        state.origins.insert(
            key,
            Entry {
                alternatives,
                sequence,
            },
        );
    }
}

fn origin_key(uri: &Uri) -> Option<String> {
    if uri.scheme_str()? != "https" {
        return None;
    }
    Some(format!(
        "https://{}:{}",
        uri.host()?.to_ascii_lowercase(),
        uri.port_u16().unwrap_or(443)
    ))
}

fn prune(state: &mut State, now: Instant) {
    state.origins.retain(|_, entry| {
        entry.alternatives.retain(|alt| alt.expires > now);
        !entry.alternatives.is_empty()
    });
}

fn response_age(headers: &HeaderMap, response_delay: Duration) -> Duration {
    let age = headers
        .get(http::header::AGE)
        .and_then(|age| age.to_str().ok())
        .and_then(|age| age.parse::<u64>().ok())
        .unwrap_or(0);
    let apparent_age = headers
        .get(http::header::DATE)
        .and_then(|date| date.to_str().ok())
        .and_then(|date| httpdate::parse_http_date(date).ok())
        .and_then(|date| SystemTime::now().duration_since(date).ok())
        .unwrap_or_default();
    apparent_age.max(Duration::from_secs(age).saturating_add(response_delay))
}

fn parse_alternative(origin: &Uri, item: &str, age: Duration, now: Instant) -> Option<Alternative> {
    let fields = split_quoted(item, b';')?;
    let (protocol, authority) = fields.first()?.trim().split_once('=')?;
    let protocol = protocol.trim();
    if !protocol.bytes().all(is_token) {
        return None;
    }
    if percent_encoding::percent_decode_str(protocol).collect::<Vec<_>>() != b"h3" {
        return None;
    }
    let authority = quoted(authority.trim())?;
    let authority = if authority.starts_with(':') {
        format!("{}{}", origin.host()?, authority)
    } else {
        authority
    };
    if authority.contains('@') {
        return None;
    }
    let authority: Authority = authority.parse().ok()?;
    if authority.host().is_empty() || authority.port_u16()? == 0 {
        return None;
    }

    let mut max_age = None;
    let mut persist = None;
    for field in fields.iter().skip(1) {
        let (name, value) = field.trim().split_once('=')?;
        let name = name.trim();
        if name.is_empty() || !name.bytes().all(is_token) {
            return None;
        }
        let value = value.trim();
        let value = if value.starts_with('"') {
            quoted(value)?
        } else if !value.is_empty() && value.bytes().all(is_token) {
            value.to_owned()
        } else {
            return None;
        };
        if name.eq_ignore_ascii_case("ma") {
            if max_age.is_some() || !value.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            max_age = Some(value.parse::<u64>().ok()?);
        } else if name.eq_ignore_ascii_case("persist") {
            if persist.is_some() {
                return None;
            }
            persist = Some(value == "1");
        }
    }
    // Clients may discard alternatives early. Bound very large server lifetimes
    // to one year so hostile values cannot overflow a platform's monotonic clock.
    let lifetime = Duration::from_secs(max_age.unwrap_or(DEFAULT_MAX_AGE).min(31_536_000));
    let remaining = lifetime.checked_sub(age)?;
    if remaining.is_zero() {
        return None;
    }
    Some(Alternative {
        authority,
        expires: now.checked_add(remaining)?,
        persist: persist.unwrap_or(false),
        broken_until: None,
    })
}

fn split_quoted(value: &str, delimiter: u8) -> Option<Vec<&str>> {
    let mut fields = Vec::new();
    let mut start = 0;
    let mut in_quotes = false;
    let mut escaped = false;
    for (index, byte) in value.bytes().enumerate() {
        if escaped {
            escaped = false;
        } else if in_quotes && byte == b'\\' {
            escaped = true;
        } else if byte == b'"' {
            in_quotes = !in_quotes;
        } else if !in_quotes && byte == delimiter {
            fields.push(&value[start..index]);
            start = index + 1;
        }
    }
    if in_quotes || escaped {
        return None;
    }
    fields.push(&value[start..]);
    Some(fields)
}

fn quoted(value: &str) -> Option<String> {
    let inner = value.strip_prefix('"')?.strip_suffix('"')?;
    let mut result = String::new();
    let mut escaped = false;
    for byte in inner.bytes() {
        if !(b' '..=b'~').contains(&byte) {
            return None;
        }
        if escaped {
            result.push(byte as char);
            escaped = false;
        } else if byte == b'\\' {
            escaped = true;
        } else if byte == b'"' {
            return None;
        } else {
            result.push(byte as char);
        }
    }
    (!escaped).then_some(result)
}

fn is_token(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn learn(cache: &AltSvcCache, origin: &str, value: &str) {
        let mut headers = HeaderMap::new();
        headers.insert("alt-svc", value.parse().unwrap());
        cache.observe(
            &origin.parse().unwrap(),
            StatusCode::OK,
            &headers,
            Duration::ZERO,
        );
    }

    #[test]
    fn origin_scope_and_default_port_normalization() {
        let cache = AltSvcCache::new();
        learn(&cache, "https://example.com/", "h3=\":8443\"");
        assert_eq!(
            cache
                .get(&"https://EXAMPLE.com:443/path".parse().unwrap())
                .unwrap()
                .authority,
            "example.com:8443"
        );
        for uri in [
            "http://example.com/",
            "https://other.com/",
            "https://example.com:444/",
        ] {
            assert!(cache.get(&uri.parse().unwrap()).is_none());
        }
    }

    #[test]
    fn parses_lists_escapes_parameters_and_ipv6() {
        let cache = AltSvcCache::new();
        learn(
            &cache,
            "https://[::1]/",
            "h2=\":443\", h%33=\":8443\"; ma=\"60\"; persist=1; ext=\"a,b;\\\"c\"",
        );
        let alt = cache.get(&"https://[::1]/".parse().unwrap()).unwrap();
        assert_eq!(alt.authority, "[::1]:8443");
        assert!(alt.persist);
    }

    #[test]
    fn expiration_age_replacement_and_clear() {
        let cache = AltSvcCache::new();
        let origin: Uri = "https://example.com/".parse().unwrap();
        learn(&cache, &origin.to_string(), "h3=\":443\"");
        let mut headers = HeaderMap::new();
        headers.insert("alt-svc", "h3=\":8443\"; ma=30".parse().unwrap());
        headers.insert("age", "30".parse().unwrap());
        cache.observe(&origin, StatusCode::OK, &headers, Duration::ZERO);
        assert!(cache.is_empty());
        learn(&cache, &origin.to_string(), "h3=\":443\"");
        learn(&cache, &origin.to_string(), "h2=\":443\"");
        assert!(cache.is_empty());
        learn(&cache, &origin.to_string(), "h3=\":443\"");
        learn(&cache, &origin.to_string(), "clear");
        assert!(cache.is_empty());
    }

    #[test]
    fn date_and_response_delay_reduce_freshness() {
        let cache = AltSvcCache::new();
        let origin = "https://example.com/".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("alt-svc", "h3=\":443\"; ma=1".parse().unwrap());
        headers.insert("date", "Sun, 06 Nov 1994 08:49:37 GMT".parse().unwrap());
        cache.observe(&origin, StatusCode::OK, &headers, Duration::ZERO);
        assert!(cache.is_empty());
        headers.remove("date");
        cache.observe(&origin, StatusCode::OK, &headers, Duration::from_secs(2));
        assert!(cache.is_empty());
    }

    #[test]
    fn rejects_invalid_authorities_and_lifetimes() {
        let cache = AltSvcCache::new();
        for value in [
            "h3=\"evil.example\"",
            "h3=\"user@evil.example:443\"",
            "h3=\":0\"",
            "h3=\":65536\"",
            "h3=\":443\"; ma=-1",
            "h3=\":443\"; ma=0",
            "h3=\":443\"; ma=1; ma=2",
            "h3=\":443\"; ma=999999999999999999999999",
            "h3=\"evil.example:443/path\"",
            "h3=\":443",
            "h3=:443",
        ] {
            learn(&cache, "https://example.com/", value);
            assert!(cache.is_empty(), "accepted {value}");
        }
    }

    #[test]
    fn bounded_storage_network_changes_and_backoff() {
        let cache = AltSvcCache::with_capacity(2);
        learn(&cache, "https://one.test/", "h3=\":443\"");
        learn(&cache, "https://two.test/", "h3=\":443\"; persist=1");
        learn(&cache, "https://three.test/", "h3=\":443\"");
        assert_eq!(cache.len(), 2);
        assert!(cache.get(&"https://one.test/".parse().unwrap()).is_none());
        cache.network_changed();
        assert_eq!(cache.len(), 1);
        let origin: Uri = "https://two.test/".parse().unwrap();
        let alternative = cache.get(&origin).unwrap();
        cache.mark_broken(&origin, &alternative);
        learn(&cache, "https://two.test/", "h3=\":443\"");
        assert!(cache.get(&origin).is_none());
        cache.clear();
        assert!(cache.is_empty());
        let disabled = AltSvcCache::with_capacity(0);
        learn(&disabled, "https://example.com/", "h3=\":443\"");
        assert!(disabled.is_empty());
    }

    #[test]
    fn misdirected_responses_cannot_repopulate_cache() {
        let cache = AltSvcCache::new();
        let mut headers = HeaderMap::new();
        headers.insert("alt-svc", "h3=\":443\"".parse().unwrap());
        cache.observe(
            &"https://example.com/".parse().unwrap(),
            StatusCode::MISDIRECTED_REQUEST,
            &headers,
            Duration::ZERO,
        );
        assert!(cache.is_empty());
    }
}
