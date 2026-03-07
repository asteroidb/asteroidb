//! Integration tests for latency measurement in the runtime sync/ping loop
//! (Issue #209).
//!
//! Validates:
//! 1. Two nodes exchange pings and the LatencyModel records entries for both
//!    directions.
//! 2. TopologyView is rebuilt after membership change.

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use asteroidb_poc::api::certified::CertifiedApi;
use asteroidb_poc::api::eventual::EventualApi;
use asteroidb_poc::control_plane::consensus::ControlPlaneConsensus;
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::http::handlers::AppState;
use asteroidb_poc::http::routes::router;
use asteroidb_poc::network::membership::MembershipClient;
use asteroidb_poc::network::{PeerConfig, PeerRegistry};
use asteroidb_poc::node::Node;
use asteroidb_poc::ops::metrics::RuntimeMetrics;
use asteroidb_poc::placement::latency::LatencyModel;
use asteroidb_poc::placement::topology::TopologyView;
use asteroidb_poc::types::{KeyRange, NodeId, NodeMode, Tag};

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

/// Spawn an HTTP server for a node on an ephemeral port.
/// Returns the shared latency model, cluster_nodes, address, and server handle.
async fn spawn_node_with_latency(
    name: &str,
    initial_peers: Vec<PeerConfig>,
    tags: &[&str],
) -> (
    Arc<RwLock<LatencyModel>>,
    Arc<RwLock<Vec<Node>>>,
    Arc<AppState>,
    SocketAddr,
    JoinHandle<()>,
) {
    let nid = node_id(name);

    let namespace = Arc::new(RwLock::new(default_namespace()));
    let peer_registry = Arc::new(Mutex::new(
        PeerRegistry::new(nid.clone(), initial_peers).expect("valid peer list"),
    ));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let latency_model = Arc::new(RwLock::new(LatencyModel::new()));

    // Create a Node with tags for topology testing.
    let mut node = Node::new(nid.clone(), NodeMode::Both);
    for t in tags {
        node.add_tag(Tag(t.to_string()));
    }
    let cluster_nodes = Arc::new(RwLock::new(vec![node]));

    let state = Arc::new(AppState {
        eventual: Arc::new(Mutex::new(EventualApi::new(nid.clone()))),
        certified: Arc::new(Mutex::new(CertifiedApi::new(nid, Arc::clone(&namespace)))),
        namespace,
        metrics: Arc::new(RuntimeMetrics::default()),
        peers: Some(peer_registry),
        peer_persist_path: None,
        consensus: Arc::new(Mutex::new(ControlPlaneConsensus::new(vec![]))),
        internal_token: None,
        self_node_id: Some(node_id(name)),
        self_addr: Some(addr.to_string()),
        latency_model: Some(Arc::clone(&latency_model)),
        cluster_nodes: Some(Arc::clone(&cluster_nodes)),
        slo_tracker: Arc::new(asteroidb_poc::ops::slo::SloTracker::new()),
    });

    let app = router(state.clone());

    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (latency_model, cluster_nodes, state, addr, handle)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Test: Two nodes exchange pings and the LatencyModel has entries for both
/// directions.
#[tokio::test]
async fn ping_records_bidirectional_latency() {
    // Spawn two nodes.
    let (model1, _cn1, state1, addr1, _h1) =
        spawn_node_with_latency("node-1", vec![], &["region:us-east"]).await;
    let (model2, _cn2, state2, addr2, _h2) =
        spawn_node_with_latency("node-2", vec![], &["region:eu-west"]).await;

    // Register each node with the other.
    {
        let mut r1 = state1.peers.as_ref().unwrap().lock().await;
        r1.add_peer(PeerConfig {
            node_id: node_id("node-2"),
            addr: addr2.to_string(),
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
    }

    // Create MembershipClients and ping. We run ping_all which returns
    // PeerRtt entries, and then manually record them into the latency model
    // (simulating what NodeRunner does).
    let registry1 = Arc::clone(state1.peers.as_ref().unwrap());
    let mut mc1 =
        MembershipClient::new(node_id("node-1"), addr1.to_string(), Arc::clone(&registry1));

    let registry2 = Arc::clone(state2.peers.as_ref().unwrap());
    let mut mc2 =
        MembershipClient::new(node_id("node-2"), addr2.to_string(), Arc::clone(&registry2));

    // Node-1 pings all its peers (node-2).
    let result1 = mc1.ping_all().await;
    assert!(
        !result1.peer_rtts.is_empty(),
        "node-1 should have RTT entries after pinging node-2"
    );

    // Record the RTTs into model1 (simulating NodeRunner::record_peer_rtt).
    for rtt_entry in &result1.peer_rtts {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let rtt_ms = rtt_entry.rtt.as_secs_f64() * 1000.0;
        model1.write().unwrap().update_latency(
            &node_id("node-1"),
            &rtt_entry.node_id,
            rtt_ms,
            now_ms,
        );
    }

    // Node-2 pings all its peers (node-1).
    let result2 = mc2.ping_all().await;
    assert!(
        !result2.peer_rtts.is_empty(),
        "node-2 should have RTT entries after pinging node-1"
    );

    // Record the RTTs into model2.
    for rtt_entry in &result2.peer_rtts {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let rtt_ms = rtt_entry.rtt.as_secs_f64() * 1000.0;
        model2.write().unwrap().update_latency(
            &node_id("node-2"),
            &rtt_entry.node_id,
            rtt_ms,
            now_ms,
        );
    }

    // Verify model1 has node-1 -> node-2 entry.
    {
        let m = model1.read().unwrap();
        let stats = m.get_latency(&node_id("node-1"), &node_id("node-2"));
        assert!(
            stats.is_some(),
            "model1 should have latency from node-1 to node-2"
        );
        let s = stats.unwrap();
        assert!(s.avg_ms > 0.0, "RTT should be positive");
        assert_eq!(s.samples, 1, "should have exactly 1 sample");
    }

    // Verify model2 has node-2 -> node-1 entry.
    {
        let m = model2.read().unwrap();
        let stats = m.get_latency(&node_id("node-2"), &node_id("node-1"));
        assert!(
            stats.is_some(),
            "model2 should have latency from node-2 to node-1"
        );
        let s = stats.unwrap();
        assert!(s.avg_ms > 0.0, "RTT should be positive");
        assert_eq!(s.samples, 1, "should have exactly 1 sample");
    }
}

/// Test: TopologyView is rebuilt after membership change.
///
/// We start with 2 nodes in different regions, add latency data, then
/// verify that the TopologyView reflects the updated topology.
#[tokio::test]
async fn topology_rebuilt_after_membership_change() {
    // Spawn two nodes in different regions.
    let (model1, cluster_nodes1, _state1, _addr1, _h1) =
        spawn_node_with_latency("node-1", vec![], &["region:us-east"]).await;
    let (_model2, _cn2, _state2, _addr2, _h2) =
        spawn_node_with_latency("node-2", vec![], &["region:eu-west"]).await;

    // Create a shared topology view.
    let topo_view = Arc::new(RwLock::new(TopologyView::build(&[], &LatencyModel::new())));

    // Initially the topology is empty.
    {
        let tv = topo_view.read().unwrap();
        assert_eq!(tv.total_nodes, 0);
    }

    // Simulate a membership change: add node-2 to node-1's cluster.
    {
        let mut nodes = cluster_nodes1.write().unwrap();
        let mut n2 = Node::new(node_id("node-2"), NodeMode::Both);
        n2.add_tag(Tag("region:eu-west".to_string()));
        nodes.push(n2);
    }

    // Add some latency data.
    {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let mut m = model1.write().unwrap();
        m.update_latency(&node_id("node-1"), &node_id("node-2"), 75.0, now_ms);
    }

    // Rebuild topology (simulating what NodeRunner::rebuild_topology does).
    {
        let nodes = cluster_nodes1.read().unwrap().clone();
        let model = model1.read().unwrap();
        let new_view = TopologyView::build(&nodes, &model);
        *topo_view.write().unwrap() = new_view;
    }

    // Verify topology now has 2 nodes in 2 regions.
    {
        let tv = topo_view.read().unwrap();
        assert_eq!(tv.total_nodes, 2, "topology should have 2 nodes");
        assert_eq!(tv.regions.len(), 2, "should have 2 regions");

        let us = tv.regions.iter().find(|r| r.name == "us-east").unwrap();
        assert_eq!(us.node_count, 1);
        assert!(
            us.inter_region_latency_ms.contains_key("eu-west"),
            "us-east should have latency to eu-west"
        );
        let latency = us.inter_region_latency_ms["eu-west"];
        assert!(
            (latency - 75.0).abs() < 0.01,
            "latency should be ~75ms, got {latency}"
        );
    }

    // Simulate another membership change: add node-3.
    {
        let mut nodes = cluster_nodes1.write().unwrap();
        let mut n3 = Node::new(node_id("node-3"), NodeMode::Both);
        n3.add_tag(Tag("region:us-east".to_string()));
        nodes.push(n3);
    }

    // Rebuild topology.
    {
        let nodes = cluster_nodes1.read().unwrap().clone();
        let model = model1.read().unwrap();
        let new_view = TopologyView::build(&nodes, &model);
        *topo_view.write().unwrap() = new_view;
    }

    // Verify topology now has 3 nodes.
    {
        let tv = topo_view.read().unwrap();
        assert_eq!(
            tv.total_nodes, 3,
            "topology should have 3 nodes after adding node-3"
        );
        let us = tv.regions.iter().find(|r| r.name == "us-east").unwrap();
        assert_eq!(us.node_count, 2, "us-east should have 2 nodes");
    }
}

/// Test: After multiple ping rounds, the LatencyModel accumulates samples.
#[tokio::test]
async fn multiple_pings_accumulate_latency_samples() {
    let (model1, _cn1, state1, addr1, _h1) = spawn_node_with_latency("node-1", vec![], &[]).await;
    let (_model2, _cn2, _state2, addr2, _h2) = spawn_node_with_latency("node-2", vec![], &[]).await;

    // Register peers.
    {
        let mut r1 = state1.peers.as_ref().unwrap().lock().await;
        r1.add_peer(PeerConfig {
            node_id: node_id("node-2"),
            addr: addr2.to_string(),
        })
        .unwrap();
    }

    let registry1 = Arc::clone(state1.peers.as_ref().unwrap());
    let mut mc1 =
        MembershipClient::new(node_id("node-1"), addr1.to_string(), Arc::clone(&registry1));

    // Perform 3 ping rounds.
    for _ in 0..3 {
        let result = mc1.ping_all().await;
        for rtt_entry in &result.peer_rtts {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let rtt_ms = rtt_entry.rtt.as_secs_f64() * 1000.0;
            model1.write().unwrap().update_latency(
                &node_id("node-1"),
                &rtt_entry.node_id,
                rtt_ms,
                now_ms,
            );
        }
        // Small delay between rounds.
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Verify 3 samples accumulated.
    {
        let m = model1.read().unwrap();
        let stats = m
            .get_latency(&node_id("node-1"), &node_id("node-2"))
            .expect("should have latency data");
        assert_eq!(
            stats.samples, 3,
            "should have 3 samples after 3 ping rounds"
        );
        assert!(stats.avg_ms > 0.0, "average RTT should be positive");
        assert!(stats.p99_ms > 0.0, "p99 RTT should be positive");
    }
}

/// Test: The /api/topology endpoint returns current latency data after it
/// is written to the shared latency model.
#[tokio::test]
async fn topology_endpoint_reflects_latency_model() {
    let (model1, cluster_nodes1, _state1, addr1, _h1) =
        spawn_node_with_latency("node-1", vec![], &["region:us-east"]).await;
    let (_model2, _cn2, _state2, _addr2, _h2) =
        spawn_node_with_latency("node-2", vec![], &["region:eu-west"]).await;

    // Add node-2 to cluster_nodes1 so topology can see it.
    {
        let mut nodes = cluster_nodes1.write().unwrap();
        let mut n2 = Node::new(node_id("node-2"), NodeMode::Both);
        n2.add_tag(Tag("region:eu-west".to_string()));
        nodes.push(n2);
    }

    // Add latency data to the shared model.
    {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let mut m = model1.write().unwrap();
        m.update_latency(&node_id("node-1"), &node_id("node-2"), 42.5, now_ms);
    }

    // Query the topology endpoint.
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://{addr1}/api/topology"))
        .send()
        .await
        .expect("topology request should succeed");
    assert!(resp.status().is_success());

    let topo: TopologyView = resp.json().await.expect("should parse TopologyView");
    assert_eq!(topo.total_nodes, 2);
    assert_eq!(topo.regions.len(), 2);

    let us = topo.regions.iter().find(|r| r.name == "us-east").unwrap();
    assert!(
        us.inter_region_latency_ms.contains_key("eu-west"),
        "topology should include inter-region latency"
    );
    let latency = us.inter_region_latency_ms["eu-west"];
    assert!(
        (latency - 42.5).abs() < 0.01,
        "latency should match the recorded value, got {latency}"
    );
}
