//! Opt-in HTTP/3 upload measurements; see docs/BBR_LAB.md.
use bytes::{Buf, Bytes};
use htrio::http3::{CongestionControl, ConnectionInfo};
use serde_json::{json, Value};
use std::{
    env,
    error::Error,
    fs, io,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

type LabResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
const MAX_BYTES: usize = 64 * 1024 * 1024;
const TIMEOUT: Duration = Duration::from_secs(120);

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[tokio::main]
async fn main() -> LabResult<()> {
    let args: Vec<_> = env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("serve") if args.len() == 5 => serve(args[2].parse()?, &args[3], &args[4]).await,
        Some("run") if (5..=7).contains(&args.len()) => {
            let bytes = args.get(5).map(|v| v.parse()).transpose()?.unwrap_or(8 * 1024 * 1024);
            let runs = args.get(6).map(|v| v.parse()).transpose()?.unwrap_or(5);
            run(&args[2], &args[3], &args[4], bytes, runs).await
        }
        _ => Err(invalid("usage: bbr_lab serve BIND CERTIFICATE_NAME CA_FILE | run URL CA_FILE NETWORK [BYTES] [RUNS]").into()),
    }
}

fn token() -> LabResult<String> {
    let token = env::var("HTRIO_LAB_TOKEN")?;
    if token.len() < 16 || token.len() > 256 || !token.bytes().all(|b| b.is_ascii_alphanumeric()) {
        return Err(invalid("HTRIO_LAB_TOKEN must contain 16..256 ASCII letters or digits").into());
    }
    Ok(token)
}

fn segmentation_offload_enabled() -> LabResult<bool> {
    match env::var("HTRIO_LAB_DISABLE_GSO") {
        Err(env::VarError::NotPresent) => Ok(true),
        Ok(value) if value == "0" => Ok(true),
        Ok(value) if value == "1" => Ok(false),
        _ => Err(invalid("HTRIO_LAB_DISABLE_GSO must be 0 or 1").into()),
    }
}

async fn serve(bind: SocketAddr, name: &str, ca_file: &str) -> LabResult<()> {
    let token = Arc::new(token()?);
    let identity = rcgen::generate_simple_self_signed(vec![name.to_owned()])?;
    let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])?
    .with_no_client_auth()
    .with_single_cert(
        vec![identity.cert.der().clone()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(identity.signing_key.serialize_der()).into(),
    )?;
    tls.alpn_protocols = vec![b"h3".to_vec()];
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(tls)?,
    ));
    let mut transport = quinn::TransportConfig::default();
    transport.enable_segmentation_offload(segmentation_offload_enabled()?);
    transport.max_concurrent_bidi_streams(4_u32.into());
    transport.max_idle_timeout(Some(TIMEOUT.try_into()?));
    config.transport_config(Arc::new(transport));
    let endpoint = quinn::Endpoint::server(config, bind)?;
    // A new server identity is created on every invocation. Never overwrite a trust file.
    use io::Write;
    let mut output = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(ca_file)?;
    output.write_all(identity.cert.pem().as_bytes())?;
    eprintln!(
        "HTTP/3 upload lab listening on {}; certificate: {ca_file}",
        endpoint.local_addr()?
    );
    let slots = Arc::new(tokio::sync::Semaphore::new(8));
    while let Some(incoming) = endpoint.accept().await {
        let Ok(permit) = slots.clone().try_acquire_owned() else {
            incoming.refuse();
            continue;
        };
        let token = token.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(error) = serve_connection(incoming, token).await {
                eprintln!("lab connection: {error}");
            }
        });
    }
    Ok(())
}

async fn serve_connection(incoming: quinn::Incoming, token: Arc<String>) -> LabResult<()> {
    let connection = tokio::time::timeout(TIMEOUT, incoming).await??;
    let mut server =
        h3::server::Connection::new(h3_quinn::Connection::new(connection.clone())).await?;
    let mut requests = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            accepted = server.accept() => {
                let Some(resolver) = accepted? else { break; };
                let token = token.clone();
                requests.spawn(async move {
                    let request = async move {
                        let (request, mut stream) = resolver.resolve_request().await?;
                        if request.headers().get("x-htrio-lab-token").and_then(|v| v.to_str().ok()) != Some(token.as_str()) {
                            stream.send_response(http::Response::builder().status(401).body(())?).await?;
                            stream.finish().await?;
                            return Ok::<(), Box<dyn Error + Send + Sync>>(());
                        }
                        let mut received = 0_usize;
                        let mut valid = true;
                        let upload = request.method() == http::Method::POST && request.uri().path() == "/upload";
                        if upload {
                            while let Some(mut data) = stream.recv_data().await? {
                                received = received.checked_add(data.remaining()).ok_or_else(|| invalid("upload length overflow"))?;
                                if received > MAX_BYTES { return Err(invalid("upload exceeds 64 MiB").into()); }
                                while data.has_remaining() {
                                    let chunk = data.chunk();
                                    valid &= chunk.iter().all(|b| *b == 0x5a);
                                    let len = chunk.len();
                                    data.advance(len);
                                }
                            }
                        }
                        let ready = request.method() == http::Method::GET && request.uri().path() == "/ready";
                        let status = if ready || (upload && valid) { 200 } else { 400 };
                        stream.send_response(http::Response::builder().status(status).body(())?).await?;
                        stream.send_data(Bytes::from(received.to_string())).await?;
                        stream.finish().await?;
                        Ok(())
                    };
                    tokio::time::timeout(TIMEOUT, request).await?
                });
            }
            Some(result) = requests.join_next() => {
                if let Err(error) = result? { connection.close(1_u32.into(), b"lab request failed"); return Err(error); }
            }
        }
    }
    Ok(())
}

async fn run(url: &str, ca_file: &str, network: &str, bytes: usize, runs: usize) -> LabResult<()> {
    if !(1..=MAX_BYTES).contains(&bytes)
        || !(1..=100).contains(&runs)
        || network.is_empty()
        || network.len() > 128
    {
        return Err(invalid(
            "expected 1..64 MiB, 1..100 runs, and a nonempty network label up to 128 bytes",
        )
        .into());
    }
    let origin = htrio::Url::parse(url)?;
    if origin.scheme() != "https"
        || origin.path() != "/"
        || origin.query().is_some()
        || origin.fragment().is_some()
        || !origin.username().is_empty()
        || origin.password().is_some()
    {
        return Err(invalid(
            "URL must be an HTTPS origin with path / and no credentials, query or fragment",
        )
        .into());
    }
    let token = token()?;
    let gso_enabled = segmentation_offload_enabled()?;
    let certificate = htrio::Certificate::from_pem(&fs::read(ca_file)?)?;
    let payload = Bytes::from(vec![0x5a; bytes]);
    let algorithms = [
        ("cubic", CongestionControl::Cubic),
        ("bbr1", CongestionControl::Bbr1),
        ("bbr3", CongestionControl::Bbr3),
    ];
    let mut failures = 0;
    for trial in 0..runs {
        // Rotate order to reduce the correlation between algorithm and changing network load.
        for position in 0..algorithms.len() {
            let (name, algorithm) = algorithms[(trial + position) % algorithms.len()];
            let mut record = json!({
                "schema": 1, "kind": "upload", "algorithm": name, "network": network,
                "trial": trial + 1, "position": position + 1, "bytes": bytes,
                "revision": option_env!("HTRIO_LAB_REVISION").unwrap_or("unrecorded"),
                "package_version": env!("CARGO_PKG_VERSION"),
                "os": env::consts::OS, "arch": env::consts::ARCH,
                "gso_enabled": gso_enabled,
                "unix_ms": SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
            });
            match measure(
                &origin,
                certificate.clone(),
                &token,
                algorithm,
                payload.clone(),
                gso_enabled,
            )
            .await
            {
                Ok(measurement) => {
                    record
                        .as_object_mut()
                        .unwrap()
                        .extend(measurement.as_object().unwrap().clone());
                }
                Err(error) => {
                    failures += 1;
                    record["status"] = json!("error");
                    record["error"] = json!(error.to_string());
                }
            }
            println!("{record}");
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    if failures > 0 {
        return Err(invalid(&format!(
            "{failures} measurements failed; keep the error records"
        ))
        .into());
    }
    Ok(())
}

async fn measure(
    origin: &htrio::Url,
    certificate: htrio::Certificate,
    token: &str,
    algorithm: CongestionControl,
    payload: Bytes,
    gso_enabled: bool,
) -> LabResult<Value> {
    let mut transport = htrio::http3::TransportConfig::default();
    transport.enable_segmentation_offload(gso_enabled);
    let client = htrio::Client::builder()
        .no_proxy()
        .tls_certs_only([certificate])
        .http3_prior_knowledge()
        .http3_transport_config(transport)
        .http3_congestion_control(algorithm)
        .redirect(htrio::redirect::Policy::none())
        .retry(htrio::retry::never())
        .timeout(TIMEOUT)
        .http3_max_idle_timeout(TIMEOUT)
        .build()?;
    let ready = client
        .get(origin.join("ready")?)
        .header("x-htrio-lab-token", token)
        .send()
        .await?
        .error_for_status()?;
    if ready.version() != http::Version::HTTP_3 {
        return Err(invalid("warmup did not negotiate HTTP/3").into());
    }
    let connection = ready
        .extensions()
        .get::<ConnectionInfo>()
        .cloned()
        .ok_or_else(|| invalid("missing connection metadata"))?;
    ready.bytes().await?;
    let before = connection.stats();
    let baseline_rtt_ms = connection.rtt().as_secs_f64() * 1000.0;
    let bytes = payload.len();
    let start = Instant::now();
    let request = client
        .post(origin.join("upload")?)
        .header("x-htrio-lab-token", token)
        .header("accept-encoding", "identity")
        .body(payload)
        .send();
    tokio::pin!(request);
    let mut interval = tokio::time::interval(Duration::from_millis(25));
    let mut rtts = Vec::new();
    let response = loop {
        tokio::select! {
            response = &mut request => { break response?.error_for_status()?; }
            _ = interval.tick() => { rtts.push(connection.rtt().as_secs_f64() * 1000.0); }
        }
    };
    if response.version() != http::Version::HTTP_3
        || response
            .extensions()
            .get::<ConnectionInfo>()
            .is_none_or(|info| info.id() != connection.id())
    {
        return Err(invalid("upload changed protocol or connection after warmup").into());
    }
    let received: usize = response.text().await?.parse()?;
    if received != bytes {
        return Err(invalid("server upload receipt did not match payload length").into());
    }
    let elapsed = start.elapsed().as_secs_f64();
    rtts.push(connection.rtt().as_secs_f64() * 1000.0);
    rtts.sort_by(f64::total_cmp);
    let after = connection.stats();
    let udp_tx_datagrams = after
        .udp_tx
        .datagrams
        .saturating_sub(before.udp_tx.datagrams);
    let udp_tx_ios = after.udp_tx.ios.saturating_sub(before.udp_tx.ios);
    if !gso_enabled && udp_tx_datagrams != udp_tx_ios {
        return Err(invalid("GSO-disabled upload still batched UDP datagrams").into());
    }
    let sent = after
        .path
        .sent_packets
        .saturating_sub(before.path.sent_packets);
    let lost = after
        .path
        .lost_packets
        .saturating_sub(before.path.lost_packets);
    Ok(json!({
        "status": "ok", "elapsed_s": elapsed, "upload_mbps": bytes as f64 * 8.0 / elapsed / 1_000_000.0,
        "baseline_rtt_ms": baseline_rtt_ms, "p95_smoothed_rtt_ms": rtts[(95 * rtts.len()).div_ceil(100) - 1],
        "rtt_samples": rtts.len(), "sent_packets": sent, "lost_packets": lost,
        "udp_tx_datagrams": udp_tx_datagrams, "udp_tx_ios": udp_tx_ios,
        "lost_bytes": after.path.lost_bytes.saturating_sub(before.path.lost_bytes),
        "congestion_events": after.path.congestion_events.saturating_sub(before.path.congestion_events),
        "final_cwnd": after.path.cwnd, "mtu": after.path.current_mtu,
        "connect_ms": connection.connect_time().as_secs_f64() * 1000.0,
        "peer": connection.remote_addr().map(|address| address.to_string()),
    }))
}
