//! Anti-entropy sync convergence integration tests (Issue #78).
//!
//! Validates that eventual data converges across nodes without manual merge,
//! using the anti-entropy sync loop (push-based replication via HTTP).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use asteroidb_poc::api::certified::CertifiedApi;
use asteroidb_poc::api::eventual::EventualApi;
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::crdt::pn_counter::PnCounter;
use asteroidb_poc::http::handlers::AppState;
use asteroidb_poc::http::routes::router;
use asteroidb_poc::network::sync::SyncClient;
use asteroidb_poc::network::{PeerConfig, PeerRegistry};
use asteroidb_poc::ops::metrics::RuntimeMetrics;
use asteroidb_poc::store::kv::CrdtValue;
use asteroidb_poc::types::{KeyRange, NodeId};

use tokio::sync::Mutex;

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

/// Spin up two HTTP servers with eventual stores, write data to each,
/// run anti-entropy sync, and verify convergence.
#[tokio::test]
async fn two_node_anti_entropy_convergence() {
    // Start two HTTP servers on ephemeral ports.
    let listener1 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr1 = listener1.local_addr().unwrap();

    let listener2 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = listener2.local_addr().unwrap();

    // Build state for node 1.
    let state1 = Arc::new(AppState {
        eventual: Mutex::new(EventualApi::new(node_id("node-1"))),
        certified: Mutex::new(CertifiedApi::new(node_id("node-1"), default_namespace())),
        metrics: Arc::new(RuntimeMetrics::default()),
    });

    // Build state for node 2.
    let state2 = Arc::new(AppState {
        eventual: Mutex::new(EventualApi::new(node_id("node-2"))),
        certified: Mutex::new(CertifiedApi::new(node_id("node-2"), default_namespace())),
        metrics: Arc::new(RuntimeMetrics::default()),
    });

    // Write some data to node 1.
    {
        let mut api = state1.eventual.lock().await;
        api.eventual_counter_inc("visits").unwrap();
        api.eventual_counter_inc("visits").unwrap();
        api.eventual_counter_inc("visits").unwrap();
        api.eventual_set_add("users", "alice".into()).unwrap();
        api.eventual_register_set("status", "online".into())
            .unwrap();
    }

    // Write different data to node 2.
    {
        let mut api = state2.eventual.lock().await;
        api.eventual_counter_inc("visits").unwrap();
        api.eventual_counter_inc("visits").unwrap();
        api.eventual_set_add("users", "bob".into()).unwrap();
        api.eventual_register_set("config", "production".into())
            .unwrap();
    }

    // Start HTTP servers.
    let app1 = router(state1.clone());
    let app2 = router(state2.clone());

    let server1 = tokio::spawn(async move {
        axum::serve(listener1, app1).await.unwrap();
    });
    let server2 = tokio::spawn(async move {
        axum::serve(listener2, app2).await.unwrap();
    });

    // Give servers a moment to start.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // --- Sync node 1 -> node 2 ---
    let registry1 = PeerRegistry::new(
        node_id("node-1"),
        vec![PeerConfig {
            node_id: node_id("node-2"),
            addr: addr2,
        }],
    )
    .unwrap();
    let sync_client1 = SyncClient::new(registry1);

    // Snapshot node 1's store and push to node 2.
    let entries1: HashMap<String, CrdtValue> = {
        let api = state1.eventual.lock().await;
        api.store()
            .all_entries()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    };
    let synced = sync_client1.push_all_keys(entries1, "node-1").await;
    assert_eq!(synced, 1, "should have synced to 1 peer");

    // --- Sync node 2 -> node 1 ---
    let registry2 = PeerRegistry::new(
        node_id("node-2"),
        vec![PeerConfig {
            node_id: node_id("node-1"),
            addr: addr1,
        }],
    )
    .unwrap();
    let sync_client2 = SyncClient::new(registry2);

    let entries2: HashMap<String, CrdtValue> = {
        let api = state2.eventual.lock().await;
        api.store()
            .all_entries()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    };
    let synced = sync_client2.push_all_keys(entries2, "node-2").await;
    assert_eq!(synced, 1, "should have synced to 1 peer");

    // --- Verify convergence ---
    // Both nodes should now have the same view of the data.

    // Check node 1's state (should have merged node 2's data).
    {
        let api = state1.eventual.lock().await;

        // visits: 3 (node-1) + 2 (node-2) = 5
        match api.get_eventual("visits") {
            Some(CrdtValue::Counter(c)) => {
                assert_eq!(c.value(), 5, "node-1 visits should be 5 after sync");
            }
            other => panic!("node-1: expected Counter for visits, got {:?}", other),
        }

        // users: {"alice", "bob"}
        match api.get_eventual("users") {
            Some(CrdtValue::Set(s)) => {
                assert!(s.contains(&"alice".to_string()), "node-1 should have alice");
                assert!(s.contains(&"bob".to_string()), "node-1 should have bob");
                assert_eq!(s.len(), 2);
            }
            other => panic!("node-1: expected Set for users, got {:?}", other),
        }

        // status: "online" (from node-1)
        assert!(
            api.get_eventual("status").is_some(),
            "node-1 should still have status"
        );

        // config: "production" (from node-2, merged in)
        match api.get_eventual("config") {
            Some(CrdtValue::Register(r)) => {
                assert_eq!(r.get(), Some(&"production".to_string()));
            }
            other => panic!("node-1: expected Register for config, got {:?}", other),
        }
    }

    // Check node 2's state (should have merged node 1's data).
    {
        let api = state2.eventual.lock().await;

        // visits: 3 (node-1) + 2 (node-2) = 5
        match api.get_eventual("visits") {
            Some(CrdtValue::Counter(c)) => {
                assert_eq!(c.value(), 5, "node-2 visits should be 5 after sync");
            }
            other => panic!("node-2: expected Counter for visits, got {:?}", other),
        }

        // users: {"alice", "bob"}
        match api.get_eventual("users") {
            Some(CrdtValue::Set(s)) => {
                assert!(s.contains(&"alice".to_string()), "node-2 should have alice");
                assert!(s.contains(&"bob".to_string()), "node-2 should have bob");
                assert_eq!(s.len(), 2);
            }
            other => panic!("node-2: expected Set for users, got {:?}", other),
        }

        // status: "online" (from node-1, merged in)
        match api.get_eventual("status") {
            Some(CrdtValue::Register(r)) => {
                assert_eq!(r.get(), Some(&"online".to_string()));
            }
            other => panic!("node-2: expected Register for status, got {:?}", other),
        }

        // config: "production" (from node-2)
        assert!(
            api.get_eventual("config").is_some(),
            "node-2 should still have config"
        );
    }

    // Clean up.
    server1.abort();
    server2.abort();
}

/// Test the pull-based sync: node pulls all keys from a peer.
#[tokio::test]
async fn pull_based_sync() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Build state with some data.
    let state = Arc::new(AppState {
        eventual: Mutex::new(EventualApi::new(node_id("source"))),
        certified: Mutex::new(CertifiedApi::new(node_id("source"), default_namespace())),
        metrics: Arc::new(RuntimeMetrics::default()),
    });

    {
        let mut api = state.eventual.lock().await;
        api.eventual_counter_inc("counter1").unwrap();
        api.eventual_counter_inc("counter1").unwrap();
        api.eventual_set_add("set1", "elem-a".into()).unwrap();
    }

    let app = router(state.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Pull from the source node.
    let registry = PeerRegistry::new(node_id("puller"), vec![]).unwrap();
    let sync_client = SyncClient::new(registry);

    let pulled = sync_client.pull_all_keys(&addr).await;
    assert!(pulled.is_some(), "pull should succeed");

    let entries = pulled.unwrap();
    assert_eq!(entries.len(), 2, "should have 2 keys");
    assert!(entries.contains_key("counter1"));
    assert!(entries.contains_key("set1"));

    // Verify the pulled counter value.
    match entries.get("counter1") {
        Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 2),
        other => panic!("expected Counter, got {:?}", other),
    }

    server.abort();
}

/// Test that the internal sync endpoint correctly handles type mismatches
/// (logs errors but merges the rest).
#[tokio::test]
async fn sync_endpoint_partial_failure() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let state = Arc::new(AppState {
        eventual: Mutex::new(EventualApi::new(node_id("target"))),
        certified: Mutex::new(CertifiedApi::new(node_id("target"), default_namespace())),
        metrics: Arc::new(RuntimeMetrics::default()),
    });

    // Pre-populate with a counter at "k".
    {
        let mut api = state.eventual.lock().await;
        api.eventual_counter_inc("k").unwrap();
    }

    let app = router(state.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send a sync request that has:
    // - "k" as a Set (type mismatch with existing Counter)
    // - "new_key" as a Counter (should succeed)
    let client = reqwest::Client::new();
    let mut entries = HashMap::new();

    use asteroidb_poc::crdt::or_set::OrSet;
    let mut set = OrSet::new();
    set.add("x".to_string(), &node_id("sender"));
    entries.insert("k".to_string(), CrdtValue::Set(set));

    let mut counter = PnCounter::new();
    counter.increment(&node_id("sender"));
    entries.insert("new_key".to_string(), CrdtValue::Counter(counter));

    let sync_req = asteroidb_poc::network::sync::SyncRequest {
        sender: "sender".to_string(),
        entries,
    };

    let resp = client
        .post(format!("http://{}/api/internal/sync", addr))
        .json(&sync_req)
        .send()
        .await
        .unwrap();

    assert!(resp.status().is_success());

    let sync_resp: asteroidb_poc::network::sync::SyncResponse = resp.json().await.unwrap();
    assert_eq!(sync_resp.merged, 1, "should merge 1 key successfully");
    assert_eq!(sync_resp.errors.len(), 1, "should have 1 error");
    assert_eq!(sync_resp.errors[0].key, "k");

    // Verify the successful merge.
    {
        let api = state.eventual.lock().await;
        assert!(api.get_eventual("new_key").is_some());
        match api.get_eventual("new_key") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 1),
            other => panic!("expected Counter, got {:?}", other),
        }

        // Original "k" should be unchanged (still a counter).
        match api.get_eventual("k") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 1),
            other => panic!("expected Counter for k, got {:?}", other),
        }
    }

    server.abort();
}

/// Test three-node convergence through sequential sync rounds.
#[tokio::test]
async fn three_node_convergence_via_sync() {
    // Start 3 HTTP servers.
    let mut listeners = Vec::new();
    let mut addrs = Vec::new();
    for _ in 0..3 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        addrs.push(addr);
        listeners.push(listener);
    }

    let mut states = Vec::new();
    for i in 0..3 {
        let nid = node_id(&format!("node-{}", i + 1));
        let state = Arc::new(AppState {
            eventual: Mutex::new(EventualApi::new(nid.clone())),
            certified: Mutex::new(CertifiedApi::new(nid, default_namespace())),
            metrics: Arc::new(RuntimeMetrics::default()),
        });
        states.push(state);
    }

    // Write distinct data to each node.
    {
        let mut api = states[0].eventual.lock().await;
        let mut c = PnCounter::new();
        c.increment(&node_id("node-1"));
        c.increment(&node_id("node-1"));
        api.eventual_write("score".into(), CrdtValue::Counter(c));
    }
    {
        let mut api = states[1].eventual.lock().await;
        let mut c = PnCounter::new();
        c.increment(&node_id("node-2"));
        c.increment(&node_id("node-2"));
        c.increment(&node_id("node-2"));
        api.eventual_write("score".into(), CrdtValue::Counter(c));
    }
    {
        let mut api = states[2].eventual.lock().await;
        let mut c = PnCounter::new();
        c.increment(&node_id("node-3"));
        api.eventual_write("score".into(), CrdtValue::Counter(c));
    }

    // Start servers.
    let mut servers = Vec::new();
    for _ in 0..3 {
        let state = states[servers.len()].clone();
        let listener = listeners.remove(0);
        let app = router(state);
        servers.push(tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        }));
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Run 2 rounds of full-mesh sync (push from each node to all others).
    for _round in 0..2 {
        for (i, state) in states.iter().enumerate() {
            let self_id = node_id(&format!("node-{}", i + 1));
            let peers: Vec<PeerConfig> = (0..3)
                .filter(|&j| j != i)
                .map(|j| PeerConfig {
                    node_id: node_id(&format!("node-{}", j + 1)),
                    addr: addrs[j],
                })
                .collect();

            let registry = PeerRegistry::new(self_id, peers).unwrap();
            let sync_client = SyncClient::new(registry);

            let entries: HashMap<String, CrdtValue> = {
                let api = state.eventual.lock().await;
                api.store()
                    .all_entries()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            };

            sync_client
                .push_all_keys(entries, &format!("node-{}", i + 1))
                .await;
        }
    }

    // Verify all nodes converge to score = 2 + 3 + 1 = 6.
    for (i, state) in states.iter().enumerate() {
        let api = state.eventual.lock().await;
        match api.get_eventual("score") {
            Some(CrdtValue::Counter(c)) => {
                assert_eq!(
                    c.value(),
                    6,
                    "node-{} should see score=6 after sync, got {}",
                    i + 1,
                    c.value()
                );
            }
            other => panic!("node-{}: expected Counter, got {:?}", i + 1, other),
        }
    }

    for s in servers {
        s.abort();
    }
}

/// Test that the internal /api/internal/keys endpoint returns all entries.
#[tokio::test]
async fn internal_keys_endpoint() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let state = Arc::new(AppState {
        eventual: Mutex::new(EventualApi::new(node_id("node-1"))),
        certified: Mutex::new(CertifiedApi::new(node_id("node-1"), default_namespace())),
        metrics: Arc::new(RuntimeMetrics::default()),
    });

    {
        let mut api = state.eventual.lock().await;
        api.eventual_counter_inc("a").unwrap();
        api.eventual_counter_inc("b").unwrap();
        api.eventual_counter_inc("c").unwrap();
    }

    let app = router(state);
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://{}/api/internal/keys", addr))
        .send()
        .await
        .unwrap();

    assert!(resp.status().is_success());

    let dump: asteroidb_poc::network::sync::KeyDumpResponse = resp.json().await.unwrap();
    assert_eq!(dump.entries.len(), 3);
    assert!(dump.entries.contains_key("a"));
    assert!(dump.entries.contains_key("b"));
    assert!(dump.entries.contains_key("c"));

    server.abort();
}
