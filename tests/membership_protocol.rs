//! E2E tests for the fan-out membership protocol (Issue #168).
//!
//! Validates:
//! 1. Fan-out join: a new node announces itself to all peers after seed join.
//! 2. Fan-out leave: a departing node notifies all peers.
//! 3. Peer list exchange via ping reconciles peer lists across nodes.

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use asteroidb_poc::api::certified::CertifiedApi;
use asteroidb_poc::api::eventual::EventualApi;
use asteroidb_poc::control_plane::consensus::ControlPlaneConsensus;
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::http::handlers::AppState;
use asteroidb_poc::http::routes::router;
use asteroidb_poc::http::types::{
    AnnounceRequest, AnnounceResponse, PeerInfo, PingRequest, PingResponse,
};
use asteroidb_poc::network::membership::MembershipClient;
use asteroidb_poc::network::{PeerConfig, PeerRegistry};
use asteroidb_poc::ops::metrics::RuntimeMetrics;
use asteroidb_poc::types::{KeyRange, NodeId};

use tokio::sync::Mutex;
use tokio::task::JoinHandle;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn node_id(s: &str) -> NodeId {
    NodeId(s.into())
}

fn default_namespace() -> SystemNamespace {
    let mut ns = SystemNamespace::new();
    ns.set_authority_definition(AuthorityDefinition {
        key_range: KeyRange {
            prefix: String::new(),
        },
        authority_nodes: vec![node_id("auth-1"), node_id("auth-2"), node_id("auth-3")],
        auto_generated: false,
    });
    ns
}

/// Spawn an HTTP server for a node on an ephemeral port with a PeerRegistry.
/// Returns its state, address, and server task handle.
async fn spawn_node(
    name: &str,
    initial_peers: Vec<PeerConfig>,
) -> (Arc<AppState>, SocketAddr, JoinHandle<()>) {
    let nid = node_id(name);

    let namespace = Arc::new(RwLock::new(default_namespace()));
    let peer_registry = Arc::new(Mutex::new(
        PeerRegistry::new(nid.clone(), initial_peers).expect("valid peer list"),
    ));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let state = Arc::new(AppState {
        eventual: Arc::new(Mutex::new(EventualApi::new(nid.clone()))),
        certified: Arc::new(Mutex::new(CertifiedApi::new(
            nid.clone(),
            Arc::clone(&namespace),
        ))),
        namespace,
        metrics: Arc::new(RuntimeMetrics::default()),
        peers: Some(peer_registry),
        peer_persist_path: None,
        namespace_persist_path: None,
        consensus: Arc::new(Mutex::new(ControlPlaneConsensus::new(vec![]))),
        internal_token: None,
        self_node_id: Some(nid),
        self_addr: Some(addr.to_string()),
        latency_model: None,
        cluster_nodes: None,
        slo_tracker: Arc::new(asteroidb_poc::ops::slo::SloTracker::new()),
        keyset_registry: None,
        epoch_config: asteroidb_poc::authority::certificate::EpochConfig::default(),
        current_epoch: Arc::new(std::sync::atomic::AtomicU64::new(0)),
    });

    let app = router(state.clone());

    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (state, addr, handle)
}

/// Get the peer count from a node's AppState.
async fn peer_count(state: &AppState) -> usize {
    state.peers.as_ref().unwrap().lock().await.peer_count()
}

/// Get sorted peer node IDs from a node's AppState.
async fn peer_ids(state: &AppState) -> Vec<String> {
    let registry = state.peers.as_ref().unwrap().lock().await;
    let mut ids: Vec<String> = registry
        .all_peers_owned()
        .into_iter()
        .map(|p| p.node_id.0)
        .collect();
    ids.sort();
    ids
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Test fan-out join: 3 nodes form a cluster, a 4th joins via seed and
/// announces to all peers. All nodes should know about node-4.
#[tokio::test]
async fn fan_out_join_propagates_to_all_peers() {
    // Start 3 nodes: node-1, node-2, node-3.
    let (state1, addr1, _h1) = spawn_node("node-1", vec![]).await;
    let (state2, addr2, _h2) = spawn_node("node-2", vec![]).await;
    let (state3, addr3, _h3) = spawn_node("node-3", vec![]).await;

    // Register each node with the others manually (simulating a pre-existing cluster).
    {
        let mut r1 = state1.peers.as_ref().unwrap().lock().await;
        r1.add_peer(PeerConfig {
            node_id: node_id("node-2"),
            addr: addr2.to_string(),
        })
        .unwrap();
        r1.add_peer(PeerConfig {
            node_id: node_id("node-3"),
            addr: addr3.to_string(),
        })
        .unwrap();
    }
    {
        let mut r2 = state2.peers.as_ref().unwrap().lock().await;
        r2.add_peer(PeerConfig {
            node_id: node_id("node-1"),
            addr: addr1.to_string(),
        })
        .unwrap();
        r2.add_peer(PeerConfig {
            node_id: node_id("node-3"),
            addr: addr3.to_string(),
        })
        .unwrap();
    }
    {
        let mut r3 = state3.peers.as_ref().unwrap().lock().await;
        r3.add_peer(PeerConfig {
            node_id: node_id("node-1"),
            addr: addr1.to_string(),
        })
        .unwrap();
        r3.add_peer(PeerConfig {
            node_id: node_id("node-2"),
            addr: addr2.to_string(),
        })
        .unwrap();
    }

    // Start a 4th node.
    let (_state4, addr4, _h4) = spawn_node("node-4", vec![]).await;

    // Simulate seed join: node-4 joins via node-1.
    let client = reqwest::Client::new();
    let join_resp = client
        .post(format!("http://{addr1}/api/internal/join"))
        .json(&serde_json::json!({
            "node_id": "node-4",
            "address": addr4.to_string(),
            "tags": []
        }))
        .send()
        .await
        .unwrap();
    assert!(join_resp.status().is_success());

    // Now node-4 has the peer list from the seed. Build a membership client
    // with all known peers and fan out.
    let peer_registry = Arc::new(Mutex::new(
        PeerRegistry::new(
            node_id("node-4"),
            vec![
                PeerConfig {
                    node_id: node_id("node-1"),
                    addr: addr1.to_string(),
                },
                PeerConfig {
                    node_id: node_id("node-2"),
                    addr: addr2.to_string(),
                },
                PeerConfig {
                    node_id: node_id("node-3"),
                    addr: addr3.to_string(),
                },
            ],
        )
        .unwrap(),
    ));

    let membership = MembershipClient::new(node_id("node-4"), addr4.to_string(), peer_registry);

    let accepted = membership.fan_out_join().await;
    assert_eq!(accepted, 3, "all 3 peers should accept the announce");

    // Verify all nodes know about node-4.
    // node-1 already knows (from the join), node-2 and node-3 via announce.
    assert!(
        peer_ids(&state1).await.contains(&"node-4".to_string()),
        "node-1 should know about node-4"
    );
    assert!(
        peer_ids(&state2).await.contains(&"node-4".to_string()),
        "node-2 should know about node-4"
    );
    assert!(
        peer_ids(&state3).await.contains(&"node-4".to_string()),
        "node-3 should know about node-4"
    );
}

/// Test fan-out leave: a node announces its departure and all peers remove it.
#[tokio::test]
async fn fan_out_leave_propagates_to_all_peers() {
    let (state1, addr1, _h1) = spawn_node("node-1", vec![]).await;
    let (state2, addr2, _h2) = spawn_node("node-2", vec![]).await;
    let (state3, addr3, _h3) = spawn_node("node-3", vec![]).await;

    // Build full-mesh cluster.
    {
        let mut r1 = state1.peers.as_ref().unwrap().lock().await;
        r1.add_peer(PeerConfig {
            node_id: node_id("node-2"),
            addr: addr2.to_string(),
        })
        .unwrap();
        r1.add_peer(PeerConfig {
            node_id: node_id("node-3"),
            addr: addr3.to_string(),
        })
        .unwrap();
    }
    {
        let mut r2 = state2.peers.as_ref().unwrap().lock().await;
        r2.add_peer(PeerConfig {
            node_id: node_id("node-1"),
            addr: addr1.to_string(),
        })
        .unwrap();
        r2.add_peer(PeerConfig {
            node_id: node_id("node-3"),
            addr: addr3.to_string(),
        })
        .unwrap();
    }
    {
        let mut r3 = state3.peers.as_ref().unwrap().lock().await;
        r3.add_peer(PeerConfig {
            node_id: node_id("node-1"),
            addr: addr1.to_string(),
        })
        .unwrap();
        r3.add_peer(PeerConfig {
            node_id: node_id("node-2"),
            addr: addr2.to_string(),
        })
        .unwrap();
    }

    // node-3 announces departure.
    let peer_registry = Arc::new(Mutex::new(
        PeerRegistry::new(
            node_id("node-3"),
            vec![
                PeerConfig {
                    node_id: node_id("node-1"),
                    addr: addr1.to_string(),
                },
                PeerConfig {
                    node_id: node_id("node-2"),
                    addr: addr2.to_string(),
                },
            ],
        )
        .unwrap(),
    ));

    let membership = MembershipClient::new(node_id("node-3"), addr3.to_string(), peer_registry);

    let accepted = membership.fan_out_leave().await;
    assert_eq!(accepted, 2, "both peers should accept the leave announce");

    // Verify node-3 was removed from node-1 and node-2.
    assert!(
        !peer_ids(&state1).await.contains(&"node-3".to_string()),
        "node-1 should no longer know about node-3"
    );
    assert!(
        !peer_ids(&state2).await.contains(&"node-3".to_string()),
        "node-2 should no longer know about node-3"
    );
}

/// Test peer list exchange: 3 nodes where node-1 knows node-2 and node-3,
/// but node-2 only knows node-1. After a ping exchange, node-2 should
/// learn about node-3.
#[tokio::test]
async fn ping_exchange_reconciles_peer_lists() {
    let (state1, addr1, _h1) = spawn_node("node-1", vec![]).await;
    let (state2, addr2, _h2) = spawn_node("node-2", vec![]).await;
    let (_state3, addr3, _h3) = spawn_node("node-3", vec![]).await;

    // node-1 knows both node-2 and node-3.
    {
        let mut r1 = state1.peers.as_ref().unwrap().lock().await;
        r1.add_peer(PeerConfig {
            node_id: node_id("node-2"),
            addr: addr2.to_string(),
        })
        .unwrap();
        r1.add_peer(PeerConfig {
            node_id: node_id("node-3"),
            addr: addr3.to_string(),
        })
        .unwrap();
    }

    // node-2 only knows node-1.
    {
        let mut r2 = state2.peers.as_ref().unwrap().lock().await;
        r2.add_peer(PeerConfig {
            node_id: node_id("node-1"),
            addr: addr1.to_string(),
        })
        .unwrap();
    }

    assert_eq!(peer_count(&state2).await, 1, "node-2 starts with 1 peer");

    // node-2 sends a ping to node-1.
    let peer_registry = Arc::new(Mutex::new(
        PeerRegistry::new(
            node_id("node-2"),
            vec![PeerConfig {
                node_id: node_id("node-1"),
                addr: addr1.to_string(),
            }],
        )
        .unwrap(),
    ));

    let mut membership = MembershipClient::new(
        node_id("node-2"),
        addr2.to_string(),
        Arc::clone(&peer_registry),
    );

    let ping_result = membership.ping_all().await;
    assert!(
        ping_result.discovered >= 1,
        "node-2 should discover at least node-3 via ping"
    );

    // Verify node-2 now knows about node-3.
    let registry = peer_registry.lock().await;
    assert!(
        registry.get_peer(&node_id("node-3")).is_some(),
        "node-2 should have learned about node-3"
    );
}

/// Test announce is idempotent: sending join twice does not fail.
#[tokio::test]
async fn announce_join_is_idempotent() {
    let (_state1, addr1, _h1) = spawn_node("node-1", vec![]).await;

    let client = reqwest::Client::new();

    // First announce.
    let resp = client
        .post(format!("http://{addr1}/api/internal/announce"))
        .json(&AnnounceRequest {
            node_id: "node-2".into(),
            address: "127.0.0.1:9999".into(),
            joining: true,
        })
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: AnnounceResponse = resp.json().await.unwrap();
    assert!(body.accepted);

    // Second announce (same node).
    let resp = client
        .post(format!("http://{addr1}/api/internal/announce"))
        .json(&AnnounceRequest {
            node_id: "node-2".into(),
            address: "127.0.0.1:9999".into(),
            joining: true,
        })
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: AnnounceResponse = resp.json().await.unwrap();
    assert!(body.accepted);
}

/// Test ping endpoint directly: sender provides peers, receiver returns its list.
#[tokio::test]
async fn ping_endpoint_exchanges_peer_lists() {
    let (state1, addr1, _h1) = spawn_node("node-1", vec![]).await;

    // node-1 has no peers initially.
    assert_eq!(peer_count(&state1).await, 0);

    let client = reqwest::Client::new();

    // Send a ping with node-2 and node-3 in the known_peers list.
    let resp = client
        .post(format!("http://{addr1}/api/internal/ping"))
        .json(&PingRequest {
            sender_id: "node-2".into(),
            sender_addr: "127.0.0.1:4001".into(),
            known_peers: vec![
                PeerInfo {
                    node_id: "node-2".into(),
                    address: "127.0.0.1:4001".into(),
                },
                PeerInfo {
                    node_id: "node-3".into(),
                    address: "127.0.0.1:4002".into(),
                },
            ],
        })
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let ping_resp: PingResponse = resp.json().await.unwrap();
    // node-1's response should include the sender and any newly learned peers.
    assert!(!ping_resp.known_peers.is_empty());

    // Verify node-1 learned about node-2 and node-3.
    let ids = peer_ids(&state1).await;
    assert!(ids.contains(&"node-2".to_string()), "should know node-2");
    assert!(ids.contains(&"node-3".to_string()), "should know node-3");
}
