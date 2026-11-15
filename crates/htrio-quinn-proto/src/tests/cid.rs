use super::*;

struct ClientCidLimit {
    inner: Arc<dyn crypto::ClientConfig>,
    limit: VarInt,
}

impl crypto::ClientConfig for ClientCidLimit {
    fn start_session(
        self: Arc<Self>,
        version: u32,
        server_name: &str,
        params: &TransportParameters,
    ) -> Result<Box<dyn crypto::Session>, ConnectError> {
        // Exercise peer advertisements independently of Quinn's default CID policy.
        let params = TransportParameters {
            active_connection_id_limit: self.limit,
            ..*params
        };
        self.inner
            .clone()
            .start_session(version, server_name, &params)
    }
}

fn client_config_with_cid_limit(limit: u64) -> ClientConfig {
    let mut config = client_config();
    config.crypto = Arc::new(ClientCidLimit {
        inner: config.crypto.clone(),
        limit: VarInt(limit),
    });
    config
}

fn pair_with_cid_lengths(client_len: usize, server_len: usize) -> Pair {
    let mut client_config = EndpointConfig::default();
    client_config.cid_generator(move || Box::new(RandomConnectionIdGenerator::new(client_len)));
    let mut server_config = EndpointConfig::default();
    server_config.cid_generator(move || Box::new(RandomConnectionIdGenerator::new(server_len)));
    Pair::new_from_endpoint(
        Endpoint::new(Arc::new(client_config), None, true, None),
        Endpoint::new(
            Arc::new(server_config),
            Some(Arc::new(super::server_config())),
            true,
            None,
        ),
    )
}

#[test]
fn cid_issuance_respects_peer_limits_with_asymmetric_cids() {
    let _guard = subscribe();
    for (client_len, server_len) in [(0, 8), (8, 0), (0, 0), (8, 8)] {
        for limit in [2, 3, 5] {
            let mut pair = pair_with_cid_lengths(client_len, server_len);
            let (client, server) = pair.connect_with(client_config_with_cid_limit(limit));
            let server_cids = if server_len == 0 { 0 } else { limit - 1 };
            let client_cids = if client_len == 0 {
                0
            } else if server_len == 0 {
                2
            } else {
                5
            };
            let client_stats = pair.client_conn_mut(client).stats();
            let server_stats = pair.server_conn_mut(server).stats();
            assert_eq!(
                server_stats.frame_tx.new_connection_id, server_cids,
                "server issuance: client_len={client_len}, server_len={server_len}, limit={limit}"
            );
            assert_eq!(client_stats.frame_rx.new_connection_id, server_cids);
            assert_eq!(client_stats.frame_tx.new_connection_id, client_cids);
            assert_eq!(server_stats.frame_rx.new_connection_id, client_cids);
            if server_len != 0 {
                assert_eq!(
                    pair.server_conn_mut(server).active_local_cid_seq(),
                    (0, limit - 1)
                );
            }
            if client_len != 0 {
                // The server retires the initial client CID to enable stateless resets.
                // Its replacement increases cumulative issuance without exceeding the active limit.
                assert_eq!(
                    pair.client_conn_mut(client).active_local_cid_seq(),
                    (1, client_cids)
                );
            }
            assert!(!pair.client_conn_mut(client).is_closed());
            assert!(!pair.server_conn_mut(server).is_closed());
        }
    }
}

#[test]
fn cid_issuance_rejects_limits_below_protocol_minimum() {
    let _guard = subscribe();
    for client_len in [0, 8] {
        for limit in [0, 1] {
            let mut pair = pair_with_cid_lengths(client_len, 8);
            pair.begin_connect(client_config_with_cid_limit(limit));
            pair.drive();
            assert_matches!(
                pair.server.assert_accept_error(),
                ConnectionError::TransportError(error)
                    if error.code == TransportErrorCode::TRANSPORT_PARAMETER_ERROR
            );
        }
    }
}

#[test]
fn cid_issuance_caps_large_peer_advertisements() {
    let _guard = subscribe();
    for client_len in [0, 8] {
        for limit in [8, 9, VarInt::MAX.into_inner()] {
            let mut pair = pair_with_cid_lengths(client_len, 8);
            pair.begin_connect(client_config_with_cid_limit(limit));
            pair.drive_client();
            // Inspect allocation before delivering more CIDs than Quinn's receive queue can hold.
            pair.server.drive_incoming(pair.time, pair.client.addr);
            let server = pair.server.assert_accept();
            let connection = pair.server_conn_mut(server);
            let requested: Vec<_> = std::iter::from_fn(|| connection.poll_endpoint_events())
                .filter_map(|event| match event.0 {
                    shared::EndpointEventInner::NeedIdentifiers(_, count) => Some(count),
                    _ => None,
                })
                .collect();
            assert_eq!(requested, [7]);
        }
    }
}

#[test]
fn consecutive_migrations_use_spare_cids_without_replenishment() {
    let _guard = subscribe();
    for client_len in [0, 8] {
        for change_ip in [false, true] {
            let mut pair = pair_with_cid_lengths(client_len, 8);
            let (client, server) = pair.connect_with(client_config_with_cid_limit(3));
            let issued = pair
                .server_conn_mut(server)
                .stats()
                .frame_tx
                .new_connection_id;
            assert_eq!(issued, 2);

            for sequence in 1..=2 {
                pair.client.addr = SocketAddr::new(
                    if change_ip {
                        Ipv4Addr::new(127, 0, 0, sequence as u8).into()
                    } else {
                        Ipv6Addr::LOCALHOST.into()
                    },
                    CLIENT_PORTS.lock().unwrap().next().unwrap(),
                );
                pair.client_conn_mut(client).local_address_changed();
                assert_eq!(pair.client_conn_mut(client).active_rem_cid_seq(), sequence);
                assert_eq!(
                    pair.client_conn_mut(client)
                        .stats()
                        .frame_rx
                        .new_connection_id,
                    issued
                );
                pair.drive_client();
                pair.drive_server();
                if sequence == 1 {
                    // Lose replies so the second migration must use the original spare CID.
                    pair.client.inbound.clear();
                }
                assert_eq!(
                    pair.server_conn_mut(server).remote_address(),
                    pair.client.addr
                );
            }
            assert!(
                !pair.drive_bounded(),
                "migration must recover after lost replies"
            );
            assert_eq!(pair.client_conn_mut(client).active_rem_cid_seq(), 2);
            assert!(pair.server_conn_mut(server).stats().frame_rx.path_response > 0);
            pair.client_datagrams(client)
                .send(Bytes::from_static(b"after migration"), true)
                .unwrap();
            pair.drive();
            assert_eq!(
                pair.server_datagrams(server).recv().unwrap(),
                b"after migration"[..]
            );
            assert!(!pair.client_conn_mut(client).is_closed());
            assert!(!pair.server_conn_mut(server).is_closed());
        }
    }
}
