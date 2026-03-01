//! 3-node partition recovery E2E tests (Issue #77).
//!
//! Validates partition -> concurrent writes -> recovery -> convergence
//! for both eventual and certified consistency modes.
//!
//! Runs 3 HTTP servers in the same test process on ephemeral ports,
//! simulating a 3-node cluster. "Partition" is modelled by selectively
//! omitting sync operations between groups of nodes. "Recovery" is
//! modelled by resuming sync.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use asteroidb_poc::api::certified::CertifiedApi;
use asteroidb_poc::api::eventual::EventualApi;
use asteroidb_poc::authority::ack_frontier::AckFrontier;
use asteroidb_poc::control_plane::consensus::ControlPlaneConsensus;
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::http::handlers::AppState;
use asteroidb_poc::http::routes::router;
use asteroidb_poc::ops::metrics::RuntimeMetrics;
use asteroidb_poc::store::kv::CrdtValue;
use asteroidb_poc::types::{KeyRange, NodeId, PolicyVersion};

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

/// Spawn an HTTP server for a node on an ephemeral port and return
/// its state, address, and server task handle.
async fn spawn_node(name: &str) -> (Arc<AppState>, SocketAddr, JoinHandle<()>) {
    let nid = node_id(name);

    let namespace = Arc::new(RwLock::new(default_namespace()));
    let state = Arc::new(AppState {
        eventual: Arc::new(Mutex::new(EventualApi::new(nid.clone()))),
        certified: Arc::new(Mutex::new(CertifiedApi::new(nid, Arc::clone(&namespace)))),
        namespace,
        metrics: Arc::new(RuntimeMetrics::default()),
        peers: None,
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

/// Push all key-value pairs from `source` node to `target` node via
/// the HTTP internal sync endpoint.
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

/// Write a counter_inc via HTTP POST to the eventual write endpoint.
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

fn make_frontier(authority: &str, physical: u64, prefix: &str) -> AckFrontier {
    AckFrontier {
        authority_id: NodeId(authority.into()),
        frontier_hlc: HlcTimestamp {
            physical,
            logical: 0,
            node_id: authority.into(),
        },
        key_range: KeyRange {
            prefix: prefix.into(),
        },
        policy_version: PolicyVersion(1),
        digest_hash: format!("{authority}-{physical}"),
    }
}

// ===========================================================================
// Test 1: Eventual partition recovery
// ===========================================================================

/// 3-node eventual partition recovery E2E test.
///
/// Scenario:
///   1. Spin up 3 HTTP nodes on ephemeral ports.
///   2. Write counter_inc to node-1 (5 times) and node-2 (3 times).
///   3. Sync node-1 -> node-2, node-2 -> node-3 (full mesh partial).
///   4. All nodes should see counter = 8 (CRDT convergence).
///   5. "Partition": stop syncing to node-3.
///   6. Write 2 more counter_inc on node-1.
///   7. Sync node-1 -> node-2 only.
///   8. node-2 sees counter = 10, node-3 still sees 8.
///   9. "Recovery": sync node-2 -> node-3.
///  10. All nodes see counter = 10.
#[tokio::test]
async fn eventual_partition_recovery_three_nodes() {
    let (state1, addr1, server1) = spawn_node("node-1").await;
    let (state2, addr2, server2) = spawn_node("node-2").await;
    let (_state3, addr3, server3) = spawn_node("node-3").await;

    // Give servers time to start.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::new();

    // --- Phase 1: Initial writes ---
    // node-1: counter_inc x5
    for _ in 0..5 {
        write_counter_inc_via_http(addr1, "hits", &client).await;
    }
    // node-2: counter_inc x3
    for _ in 0..3 {
        write_counter_inc_via_http(addr2, "hits", &client).await;
    }

    // --- Phase 2: Sync all nodes ---
    // node-1 -> node-2
    sync_via_http(&state1, "node-1", addr2, &client).await;
    // node-2 -> node-1  (bidirectional so node-1 gets node-2's data)
    sync_via_http(&state2, "node-2", addr1, &client).await;
    // After bidirectional sync between node-1 and node-2, we need to
    // re-sync so node-2 has the merged state to push to node-3.
    sync_via_http(&state1, "node-1", addr2, &client).await;
    // node-2 -> node-3  (node-2 now has merged state from both)
    sync_via_http(&state2, "node-2", addr3, &client).await;

    // --- Phase 3: Verify all nodes converge to 8 ---
    assert_eq!(
        read_counter_via_http(addr1, "hits", &client).await,
        8,
        "node-1 should see counter=8 after initial sync"
    );
    assert_eq!(
        read_counter_via_http(addr2, "hits", &client).await,
        8,
        "node-2 should see counter=8 after initial sync"
    );
    assert_eq!(
        read_counter_via_http(addr3, "hits", &client).await,
        8,
        "node-3 should see counter=8 after initial sync"
    );

    // --- Phase 4: Partition (node-3 isolated) ---
    // Write 2 more counter_inc on node-1.
    for _ in 0..2 {
        write_counter_inc_via_http(addr1, "hits", &client).await;
    }

    // Sync node-1 -> node-2 only (node-3 is "partitioned").
    sync_via_http(&state1, "node-1", addr2, &client).await;

    // node-2 should see counter=10, node-3 should still see 8.
    assert_eq!(
        read_counter_via_http(addr2, "hits", &client).await,
        10,
        "node-2 should see counter=10 after partition writes"
    );
    assert_eq!(
        read_counter_via_http(addr3, "hits", &client).await,
        8,
        "node-3 should still see counter=8 during partition"
    );

    // --- Phase 5: Recovery ---
    // Sync node-2 -> node-3 (heal the partition).
    sync_via_http(&state2, "node-2", addr3, &client).await;

    // All nodes should converge to counter=10.
    assert_eq!(
        read_counter_via_http(addr1, "hits", &client).await,
        10,
        "node-1 should see counter=10 after recovery"
    );
    assert_eq!(
        read_counter_via_http(addr2, "hits", &client).await,
        10,
        "node-2 should see counter=10 after recovery"
    );
    assert_eq!(
        read_counter_via_http(addr3, "hits", &client).await,
        10,
        "node-3 should see counter=10 after recovery"
    );

    // Clean up.
    server1.abort();
    server2.abort();
    server3.abort();
}

// ===========================================================================
// Test 2: Certified partition recovery
// ===========================================================================

/// 3-node certified write E2E test.
///
/// Scenario:
///   1. Spin up 3 HTTP nodes.
///   2. Submit a certified_write with on_timeout=pending.
///   3. Check status: should be Pending.
///   4. POST frontiers for 2 of 3 authorities (majority) to the node.
///   5. The node processes certifications and the status should become Certified.
#[tokio::test]
async fn certified_partition_recovery_three_nodes() {
    let (state1, addr1, server1) = spawn_node("cert-node-1").await;
    let (_state2, _addr2, server2) = spawn_node("cert-node-2").await;
    let (_state3, _addr3, server3) = spawn_node("cert-node-3").await;

    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::new();

    // --- Phase 1: Submit certified_write ---
    let write_body =
        r#"{"key":"sensor","value":{"type":"counter","value":5},"on_timeout":"pending"}"#;
    let resp = client
        .post(format!("http://{}/api/certified/write", addr1))
        .header("content-type", "application/json")
        .body(write_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "Pending");

    // --- Phase 2: Verify status is Pending ---
    let resp = client
        .get(format!("http://{}/api/status/sensor", addr1))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["key"], "sensor");
    assert_eq!(body["status"], "Pending");

    // --- Phase 3: Get the write timestamp to construct frontiers ---
    // We need the timestamp of the pending write to construct frontiers
    // that are past it. We read it from the in-memory state.
    let write_ts = {
        let api = state1.certified.lock().await;
        let pw = api.pending_writes();
        assert!(!pw.is_empty(), "should have at least one pending write");
        pw[0].timestamp.physical
    };

    // --- Phase 4: POST frontiers for 2 of 3 authorities (majority) ---
    // Simulate authority-1 frontier.
    let frontier_body = serde_json::json!({
        "frontiers": [
            {
                "authority_id": "auth-1",
                "frontier_hlc": {
                    "physical": write_ts + 1000,
                    "logical": 0,
                    "node_id": "auth-1"
                },
                "key_range": {"prefix": ""},
                "policy_version": 1,
                "digest_hash": format!("auth-1-{}", write_ts + 1000)
            }
        ]
    });
    let resp = client
        .post(format!("http://{}/api/internal/frontiers", addr1))
        .header("content-type", "application/json")
        .json(&frontier_body)
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "frontier push for auth-1 failed"
    );

    // Status should still be Pending (only 1 of 3 authorities).
    let resp = client
        .get(format!("http://{}/api/status/sensor", addr1))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "Pending", "1/3 authority: still Pending");

    // Simulate authority-2 frontier (now majority = 2/3).
    let frontier_body = serde_json::json!({
        "frontiers": [
            {
                "authority_id": "auth-2",
                "frontier_hlc": {
                    "physical": write_ts + 2000,
                    "logical": 0,
                    "node_id": "auth-2"
                },
                "key_range": {"prefix": ""},
                "policy_version": 1,
                "digest_hash": format!("auth-2-{}", write_ts + 2000)
            }
        ]
    });
    let resp = client
        .post(format!("http://{}/api/internal/frontiers", addr1))
        .header("content-type", "application/json")
        .json(&frontier_body)
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "frontier push for auth-2 failed"
    );

    // --- Phase 5: Trigger certification processing ---
    // The frontiers have been applied, but the CertifiedApi needs to
    // re-evaluate pending writes. We do this by calling process_certifications
    // directly on the state (the HTTP API does not expose a "tick" endpoint).
    {
        let mut api = state1.certified.lock().await;
        api.process_certifications();
    }

    // --- Phase 6: Status should now be Certified ---
    let resp = client
        .get(format!("http://{}/api/status/sensor", addr1))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["status"], "Certified",
        "2/3 authorities: should be Certified"
    );

    // Verify the certified read also reflects Certified status.
    let resp = client
        .get(format!("http://{}/api/certified/sensor", addr1))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["key"], "sensor");
    assert!(body["value"].is_object(), "should have a value");
    assert_eq!(body["status"], "Certified");
    assert!(body["frontier"].is_object(), "should have a frontier");

    // Clean up.
    server1.abort();
    server2.abort();
    server3.abort();
}

// ===========================================================================
// Test 3: Eventual partition with concurrent writes on both sides
// ===========================================================================

/// Tests that concurrent writes on both sides of a partition converge
/// correctly after recovery via CRDT merge semantics.
///
/// Scenario:
///   1. 3 nodes, pre-partition state synced.
///   2. Partition: {node-1, node-2} vs {node-3}.
///   3. Both partitions write to the same counter key.
///   4. Recovery: sync between partitions.
///   5. All nodes converge to the sum of all writes.
#[tokio::test]
async fn eventual_concurrent_writes_both_partitions() {
    let (state1, addr1, server1) = spawn_node("cw-node-1").await;
    let (state2, addr2, server2) = spawn_node("cw-node-2").await;
    let (state3, addr3, server3) = spawn_node("cw-node-3").await;

    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::new();

    // --- Pre-partition: establish shared state ---
    // node-1 writes 2 increments.
    for _ in 0..2 {
        write_counter_inc_via_http(addr1, "score", &client).await;
    }
    // Sync to all nodes: node-1 -> node-2, node-1 -> node-3.
    sync_via_http(&state1, "cw-node-1", addr2, &client).await;
    sync_via_http(&state1, "cw-node-1", addr3, &client).await;

    // Verify: all see counter=2.
    for (addr, name) in [(addr1, "node-1"), (addr2, "node-2"), (addr3, "node-3")] {
        assert_eq!(
            read_counter_via_http(addr, "score", &client).await,
            2,
            "{name} should see score=2 before partition"
        );
    }

    // --- Partition: {node-1, node-2} vs {node-3} ---
    // Partition 1: node-1 and node-2 write.
    for _ in 0..3 {
        write_counter_inc_via_http(addr1, "score", &client).await;
    }
    for _ in 0..2 {
        write_counter_inc_via_http(addr2, "score", &client).await;
    }
    // Sync within partition 1: node-1 <-> node-2.
    sync_via_http(&state1, "cw-node-1", addr2, &client).await;
    sync_via_http(&state2, "cw-node-2", addr1, &client).await;

    // Partition 2: node-3 writes independently.
    for _ in 0..4 {
        write_counter_inc_via_http(addr3, "score", &client).await;
    }

    // Verify partition states diverge.
    // Partition 1: initial(2) + node-1(3) + node-2(2) = 7.
    assert_eq!(
        read_counter_via_http(addr1, "score", &client).await,
        7,
        "node-1 should see score=7 in partition 1"
    );
    assert_eq!(
        read_counter_via_http(addr2, "score", &client).await,
        7,
        "node-2 should see score=7 in partition 1"
    );
    // Partition 2: initial(2) + node-3(4) = 6.
    assert_eq!(
        read_counter_via_http(addr3, "score", &client).await,
        6,
        "node-3 should see score=6 in partition 2"
    );

    // --- Recovery: merge partitions ---
    // node-1 -> node-3  (partition 1 data flows to partition 2)
    sync_via_http(&state1, "cw-node-1", addr3, &client).await;
    // node-3 -> node-1  (partition 2 data flows to partition 1)
    sync_via_http(&state3, "cw-node-3", addr1, &client).await;
    // Propagate to node-2: node-1 -> node-2.
    sync_via_http(&state1, "cw-node-1", addr2, &client).await;
    // And ensure node-2 gets node-3's data too.
    sync_via_http(&state3, "cw-node-3", addr2, &client).await;

    // Expected: initial(2) + node-1(3) + node-2(2) + node-3(4) = 11.
    let expected = 11;
    for (addr, name) in [(addr1, "node-1"), (addr2, "node-2"), (addr3, "node-3")] {
        assert_eq!(
            read_counter_via_http(addr, "score", &client).await,
            expected,
            "{name} should see score={expected} after recovery"
        );
    }

    // Clean up.
    server1.abort();
    server2.abort();
    server3.abort();
}

// ===========================================================================
// Test 4: Certified write fails during partition, succeeds after recovery
// ===========================================================================

/// Tests that a certified write remains Pending when a node is partitioned
/// from the authorities, and transitions to Certified after partition
/// recovery delivers the missing frontier updates.
#[tokio::test]
async fn certified_pending_during_partition_certified_after_recovery() {
    let (state1, addr1, server1) = spawn_node("p-node-1").await;
    let (_state2, _addr2, server2) = spawn_node("p-node-2").await;
    let (_state3, _addr3, server3) = spawn_node("p-node-3").await;

    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::new();

    // --- Submit certified_write ---
    let write_body =
        r#"{"key":"isolated","value":{"type":"counter","value":10},"on_timeout":"pending"}"#;
    let resp = client
        .post(format!("http://{}/api/certified/write", addr1))
        .header("content-type", "application/json")
        .body(write_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // --- Partition: node-1 cannot reach any authorities ---
    // Status is Pending (no frontier updates).
    let resp = client
        .get(format!("http://{}/api/status/isolated", addr1))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "Pending");

    // Get write timestamp.
    let write_ts = {
        let api = state1.certified.lock().await;
        api.pending_writes()[0].timestamp.physical
    };

    // --- During partition: only auth-3 frontier arrives (1/3, no majority) ---
    let frontier_body = serde_json::json!({
        "frontiers": [make_frontier("auth-3", write_ts + 500, "")]
    });
    let resp = client
        .post(format!("http://{}/api/internal/frontiers", addr1))
        .header("content-type", "application/json")
        .json(&frontier_body)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    {
        let mut api = state1.certified.lock().await;
        api.process_certifications();
    }

    let resp = client
        .get(format!("http://{}/api/status/isolated", addr1))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["status"], "Pending",
        "1/3 authorities: should remain Pending"
    );

    // --- Recovery: auth-1 frontier arrives (now 2/3, majority!) ---
    let frontier_body = serde_json::json!({
        "frontiers": [make_frontier("auth-1", write_ts + 1000, "")]
    });
    let resp = client
        .post(format!("http://{}/api/internal/frontiers", addr1))
        .header("content-type", "application/json")
        .json(&frontier_body)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    {
        let mut api = state1.certified.lock().await;
        api.process_certifications();
    }

    let resp = client
        .get(format!("http://{}/api/status/isolated", addr1))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["status"], "Certified",
        "2/3 authorities: should be Certified after recovery"
    );

    // Clean up.
    server1.abort();
    server2.abort();
    server3.abort();
}

// ===========================================================================
// Test 5: Multiple CRDT types across partition
// ===========================================================================

/// Tests convergence of multiple CRDT types (counter, set, register)
/// across a 3-node partition boundary.
#[tokio::test]
async fn multiple_crdt_types_partition_recovery() {
    let (state1, addr1, server1) = spawn_node("mt-node-1").await;
    let (state2, addr2, server2) = spawn_node("mt-node-2").await;
    let (state3, addr3, server3) = spawn_node("mt-node-3").await;

    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::new();

    // --- Pre-partition writes ---
    // node-1: counter + set.
    write_counter_inc_via_http(addr1, "visits", &client).await;
    let resp = client
        .post(format!("http://{}/api/eventual/write", addr1))
        .header("content-type", "application/json")
        .json(&serde_json::json!({"type":"set_add","key":"users","element":"alice"}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Sync node-1 -> node-2, node-1 -> node-3.
    sync_via_http(&state1, "mt-node-1", addr2, &client).await;
    sync_via_http(&state1, "mt-node-1", addr3, &client).await;

    // --- Partition: {node-1, node-2} vs {node-3} ---
    // Partition 1: node-2 adds to set and increments counter.
    let resp = client
        .post(format!("http://{}/api/eventual/write", addr2))
        .header("content-type", "application/json")
        .json(&serde_json::json!({"type":"set_add","key":"users","element":"bob"}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    write_counter_inc_via_http(addr2, "visits", &client).await;

    // Partition 2: node-3 adds to set, sets a register, and increments counter.
    let resp = client
        .post(format!("http://{}/api/eventual/write", addr3))
        .header("content-type", "application/json")
        .json(&serde_json::json!({"type":"set_add","key":"users","element":"charlie"}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    write_counter_inc_via_http(addr3, "visits", &client).await;
    write_counter_inc_via_http(addr3, "visits", &client).await;
    let resp = client
        .post(format!("http://{}/api/eventual/write", addr3))
        .header("content-type", "application/json")
        .json(&serde_json::json!({"type":"register_set","key":"status","value":"degraded"}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // Sync within partition 1.
    sync_via_http(&state1, "mt-node-1", addr2, &client).await;
    sync_via_http(&state2, "mt-node-2", addr1, &client).await;

    // --- Recovery: merge all ---
    sync_via_http(&state1, "mt-node-1", addr3, &client).await;
    sync_via_http(&state3, "mt-node-3", addr1, &client).await;
    sync_via_http(&state3, "mt-node-3", addr2, &client).await;
    sync_via_http(&state1, "mt-node-1", addr2, &client).await;

    // Verify convergence: counter = 1 (node-1) + 1 (node-2) + 2 (node-3) = 4.
    for (addr, name) in [(addr1, "node-1"), (addr2, "node-2"), (addr3, "node-3")] {
        let val = read_counter_via_http(addr, "visits", &client).await;
        assert_eq!(val, 4, "{name}: visits counter should be 4, got {val}");
    }

    // Verify set convergence: should contain {alice, bob, charlie}.
    for (addr, name) in [(addr1, "node-1"), (addr2, "node-2"), (addr3, "node-3")] {
        let resp = client
            .get(format!("http://{}/api/eventual/users", addr))
            .send()
            .await
            .unwrap();
        let body: serde_json::Value = resp.json().await.unwrap();
        let elements = body["value"]["elements"]
            .as_array()
            .expect("expected array");
        let elems: Vec<String> = elements
            .iter()
            .map(|e| e.as_str().unwrap().to_string())
            .collect();
        assert!(
            elems.contains(&"alice".to_string()),
            "{name}: should contain alice"
        );
        assert!(
            elems.contains(&"bob".to_string()),
            "{name}: should contain bob"
        );
        assert!(
            elems.contains(&"charlie".to_string()),
            "{name}: should contain charlie"
        );
    }

    // Verify register propagation: all nodes should have the "status" register.
    for (addr, name) in [(addr1, "node-1"), (addr2, "node-2"), (addr3, "node-3")] {
        let resp = client
            .get(format!("http://{}/api/eventual/status", addr))
            .send()
            .await
            .unwrap();
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["key"], "status");
        assert!(
            body["value"].is_object(),
            "{name}: should have the register value"
        );
        assert_eq!(
            body["value"]["value"], "degraded",
            "{name}: register should be 'degraded'"
        );
    }

    // Clean up.
    server1.abort();
    server2.abort();
    server3.abort();
}
