//! E2E tests for node join/leave bootstrap (Issue #97).
//!
//! Validates:
//! 1. A new (4th) node joins an existing 3-node cluster via `POST /api/internal/join`.
//! 2. The joining node receives the peer list and namespace snapshot.
//! 3. Data convergence via anti-entropy sync after join.
//! 4. A node leaves via `POST /api/internal/leave` and is removed from the registry.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use asteroidb_poc::api::certified::CertifiedApi;
use asteroidb_poc::api::eventual::EventualApi;
use asteroidb_poc::control_plane::consensus::ControlPlaneConsensus;
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::http::handlers::AppState;
use asteroidb_poc::http::routes::router;
use asteroidb_poc::http::types::{JoinRequest, JoinResponse, LeaveRequest, LeaveResponse};
use asteroidb_poc::network::PeerRegistry;
use asteroidb_poc::ops::metrics::RuntimeMetrics;
use asteroidb_poc::store::kv::CrdtValue;
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
    });
    ns
}

/// Spawn an HTTP server for a node on an ephemeral port with a PeerRegistry.
/// Returns its state, address, and server task handle.
async fn spawn_node_with_peers(
    name: &str,
    initial_peers: Vec<asteroidb_poc::network::PeerConfig>,
) -> (Arc<AppState>, SocketAddr, JoinHandle<()>) {
    let nid = node_id(name);

    let namespace = Arc::new(RwLock::new(default_namespace()));
    let peer_registry = Arc::new(Mutex::new(
        PeerRegistry::new(nid.clone(), initial_peers).expect("valid peer list"),
    ));

    let state = Arc::new(AppState {
        eventual: Arc::new(Mutex::new(EventualApi::new(nid.clone()))),
        certified: Arc::new(Mutex::new(CertifiedApi::new(nid, Arc::clone(&namespace)))),
        namespace,
        metrics: Arc::new(RuntimeMetrics::default()),
        peers: Some(peer_registry),
        peer_persist_path: None,
        consensus: Arc::new(Mutex::new(ControlPlaneConsensus::new(vec![]))),
        internal_token: None,
    });

    let app = router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (state, addr, handle)
}

/// Push all key-value pairs from `source` node to `target` node via HTTP sync.
async fn sync_via_http(
    source: &Arc<AppState>,
    source_name: &str,
    target_addr: SocketAddr,
    client: &reqwest::Client,
) {
    let entries: HashMap<String, CrdtValue> = {
        let api = source.eventual.lock().await;
        api.store()
            .all_entries()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    };

    if entries.is_empty() {
        return;
    }

    let sync_req = asteroidb_poc::network::sync::SyncRequest {
        sender: source_name.to_string(),
        entries,
    };

    let url = format!("http://{}/api/internal/sync", target_addr);
    let resp = client.post(&url).json(&sync_req).send().await.unwrap();
    assert!(
        resp.status().is_success(),
        "sync from {} to {} failed: {}",
        source_name,
        target_addr,
        resp.status()
    );
}

/// Write a counter_inc via HTTP POST.
async fn write_counter_inc_via_http(addr: SocketAddr, key: &str, client: &reqwest::Client) {
    let url = format!("http://{}/api/eventual/write", addr);
    let body = serde_json::json!({"type": "counter_inc", "key": key});
    let resp = client
        .post(&url)
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "counter_inc failed: {}",
        resp.status()
    );
}

/// Read the eventual counter value for a key via HTTP GET.
async fn read_counter_via_http(addr: SocketAddr, key: &str, client: &reqwest::Client) -> i64 {
    let url = format!("http://{}/api/eventual/{}", addr, key);
    let resp = client.get(&url).send().await.unwrap();
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await.unwrap();
    body["value"]["value"]
        .as_i64()
        .expect("expected counter value")
}

// ===========================================================================
// Test 1: 4th node joins a 3-node cluster and converges
// ===========================================================================

/// E2E test for node join bootstrap.
///
/// Scenario:
///   1. Start 3 nodes, each with an empty peer registry.
///   2. Write some data (counter increments) to node-1 and node-2.
///   3. Sync data across the 3 existing nodes.
///   4. Start a 4th node and have it join via POST /api/internal/join to node-1.
///   5. Verify the 4th node receives the peer list.
///   6. Sync data to the 4th node and verify convergence.
#[tokio::test]
async fn node_join_receives_peers_and_converges() {
    let (state1, addr1, server1) = spawn_node_with_peers("node-1", vec![]).await;
    let (state2, addr2, server2) = spawn_node_with_peers("node-2", vec![]).await;
    let (_state3, addr3, server3) = spawn_node_with_peers("node-3", vec![]).await;

    // Give servers time to start.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::new();

    // --- Phase 1: Write data on existing nodes ---
    // node-1: 5 counter_inc
    for _ in 0..5 {
        write_counter_inc_via_http(addr1, "hits", &client).await;
    }
    // node-2: 3 counter_inc
    for _ in 0..3 {
        write_counter_inc_via_http(addr2, "hits", &client).await;
    }

    // --- Phase 2: Sync across existing nodes ---
    sync_via_http(&state1, "node-1", addr2, &client).await;
    sync_via_http(&state2, "node-2", addr1, &client).await;
    sync_via_http(&state1, "node-1", addr3, &client).await;

    // Verify all 3 nodes see counter=8.
    for (addr, name) in [(addr1, "node-1"), (addr2, "node-2"), (addr3, "node-3")] {
        assert_eq!(
            read_counter_via_http(addr, "hits", &client).await,
            8,
            "{name} should see counter=8 before join"
        );
    }

    // --- Phase 3: 4th node joins via seed node (node-1) ---
    let (_state4, addr4, server4) = spawn_node_with_peers("node-4", vec![]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let join_req = JoinRequest {
        node_id: "node-4".to_string(),
        address: addr4.to_string(),
        tags: vec!["dc:tokyo".to_string()],
    };

    let resp = client
        .post(format!("http://{}/api/internal/join", addr1))
        .header("content-type", "application/json")
        .json(&join_req)
        .send()
        .await
        .unwrap();

    assert!(
        resp.status().is_success(),
        "join request failed: {}",
        resp.status()
    );

    let join_resp: JoinResponse = resp.json().await.unwrap();

    // Verify the join response contains the peer list.
    // The seed node (node-1) added node-4, so node-4 should be in the list.
    assert!(
        !join_resp.peers.is_empty(),
        "join response should contain peers"
    );
    let peer_ids: Vec<&str> = join_resp.peers.iter().map(|p| p.node_id.as_str()).collect();
    assert!(
        peer_ids.contains(&"node-4"),
        "peer list should contain the joining node: {:?}",
        peer_ids
    );

    // Verify the namespace snapshot is present and not null.
    assert!(
        !join_resp.namespace.is_null(),
        "namespace snapshot should not be null"
    );

    // --- Phase 4: Sync data to the 4th node ---
    // Use anti-entropy: push all data from node-1 to node-4.
    sync_via_http(&state1, "node-1", addr4, &client).await;

    // Verify node-4 has converged.
    let val = read_counter_via_http(addr4, "hits", &client).await;
    assert_eq!(
        val, 8,
        "node-4 should see counter=8 after sync, got {}",
        val
    );

    // --- Phase 5: Verify node-4 can also write and sync back ---
    write_counter_inc_via_http(addr4, "hits", &client).await;
    write_counter_inc_via_http(addr4, "hits", &client).await;

    // node-4 should now have counter=10.
    assert_eq!(
        read_counter_via_http(addr4, "hits", &client).await,
        10,
        "node-4 should see counter=10 after its own writes"
    );

    // Clean up.
    server1.abort();
    server2.abort();
    server3.abort();
    server4.abort();
}

// ===========================================================================
// Test 2: Node leave removes from peer registry
// ===========================================================================

/// E2E test for node leave.
///
/// Scenario:
///   1. Start 2 nodes with empty peer registries.
///   2. Node-2 joins node-1 via POST /api/internal/join.
///   3. Verify node-1's peer count increased.
///   4. Node-2 leaves via POST /api/internal/leave to node-1.
///   5. Verify node-1's peer count decreased.
#[tokio::test]
async fn node_leave_removes_from_registry() {
    let (_state1, addr1, server1) = spawn_node_with_peers("leave-node-1", vec![]).await;
    let (_state2, addr2, server2) = spawn_node_with_peers("leave-node-2", vec![]).await;

    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::new();

    // --- Node-2 joins node-1 ---
    let join_req = JoinRequest {
        node_id: "leave-node-2".to_string(),
        address: addr2.to_string(),
        tags: vec![],
    };

    let resp = client
        .post(format!("http://{}/api/internal/join", addr1))
        .header("content-type", "application/json")
        .json(&join_req)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "join failed");

    let join_resp: JoinResponse = resp.json().await.unwrap();
    let has_node2 = join_resp.peers.iter().any(|p| p.node_id == "leave-node-2");
    assert!(
        has_node2,
        "peer list should contain leave-node-2 after join"
    );

    // --- Node-2 leaves node-1 ---
    let leave_req = LeaveRequest {
        node_id: "leave-node-2".to_string(),
    };

    let resp = client
        .post(format!("http://{}/api/internal/leave", addr1))
        .header("content-type", "application/json")
        .json(&leave_req)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "leave failed");

    let leave_resp: LeaveResponse = resp.json().await.unwrap();
    assert!(leave_resp.success, "leave should succeed");

    // --- Verify: joining again should succeed (node was removed) ---
    let join_req2 = JoinRequest {
        node_id: "leave-node-2".to_string(),
        address: addr2.to_string(),
        tags: vec![],
    };

    let resp = client
        .post(format!("http://{}/api/internal/join", addr1))
        .header("content-type", "application/json")
        .json(&join_req2)
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "re-join after leave should succeed"
    );

    // Clean up.
    server1.abort();
    server2.abort();
}

// ===========================================================================
// Test 3: Duplicate join is rejected
// ===========================================================================

/// Attempting to join with the same node_id twice should fail.
#[tokio::test]
async fn duplicate_join_is_rejected() {
    let (_state1, addr1, server1) = spawn_node_with_peers("dup-node-1", vec![]).await;
    let (_state2, addr2, server2) = spawn_node_with_peers("dup-node-2", vec![]).await;

    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::new();

    // First join: should succeed.
    let join_req = JoinRequest {
        node_id: "dup-node-2".to_string(),
        address: addr2.to_string(),
        tags: vec![],
    };

    let resp = client
        .post(format!("http://{}/api/internal/join", addr1))
        .header("content-type", "application/json")
        .json(&join_req)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "first join should succeed");

    // Second join with same node_id: should fail.
    let resp = client
        .post(format!("http://{}/api/internal/join", addr1))
        .header("content-type", "application/json")
        .json(&join_req)
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_client_error(),
        "duplicate join should fail: {}",
        resp.status()
    );

    // Clean up.
    server1.abort();
    server2.abort();
}

// ===========================================================================
// Test 4: Leave for non-existent node returns success=false
// ===========================================================================

/// Leaving a node that was never registered should return success=false.
#[tokio::test]
async fn leave_nonexistent_returns_false() {
    let (_state1, addr1, server1) = spawn_node_with_peers("noexist-node-1", vec![]).await;

    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::new();

    let leave_req = LeaveRequest {
        node_id: "ghost-node".to_string(),
    };

    let resp = client
        .post(format!("http://{}/api/internal/leave", addr1))
        .header("content-type", "application/json")
        .json(&leave_req)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let leave_resp: LeaveResponse = resp.json().await.unwrap();
    assert!(
        !leave_resp.success,
        "leave of non-existent node should return success=false"
    );

    // Clean up.
    server1.abort();
}
