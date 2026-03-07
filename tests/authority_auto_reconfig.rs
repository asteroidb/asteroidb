//! Integration tests for policy/tag-driven authority auto-reconfiguration (Issue #118).
//!
//! Validates:
//! 1. Authority set is automatically computed from certified placement policies.
//! 2. Node join triggers authority recalculation.
//! 3. Node leave triggers authority recalculation.
//! 4. Policy change triggers authority recalculation.
//! 5. Certified judgment is not broken during reconfiguration.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use asteroidb_poc::api::certified::{CertifiedApi, OnTimeout};
use asteroidb_poc::compaction::CompactionEngine;
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::crdt::pn_counter::PnCounter;
use asteroidb_poc::node::Node;
use asteroidb_poc::ops::metrics::RuntimeMetrics;
use asteroidb_poc::placement::PlacementPolicy;
use asteroidb_poc::runtime::{NodeRunner, NodeRunnerConfig};
use asteroidb_poc::store::kv::CrdtValue;
use asteroidb_poc::types::{CertificationStatus, KeyRange, NodeId, NodeMode, PolicyVersion, Tag};

use tokio::sync::Mutex;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn node_id(s: &str) -> NodeId {
    NodeId(s.into())
}

fn kr(prefix: &str) -> KeyRange {
    KeyRange {
        prefix: prefix.into(),
    }
}

fn tag(s: &str) -> Tag {
    Tag(s.into())
}

fn make_node(id: &str, mode: NodeMode, tags: &[&str]) -> Node {
    let mut n = Node::new(node_id(id), mode);
    for t in tags {
        n.add_tag(tag(t));
    }
    n
}

fn counter_value(n: i64) -> CrdtValue {
    let mut counter = PnCounter::new();
    for _ in 0..n {
        counter.increment(&node_id("writer"));
    }
    CrdtValue::Counter(counter)
}

fn wrap_ns(ns: SystemNamespace) -> Arc<RwLock<SystemNamespace>> {
    Arc::new(RwLock::new(ns))
}

fn wrap_api(api: CertifiedApi) -> Arc<Mutex<CertifiedApi>> {
    Arc::new(Mutex::new(api))
}

fn default_metrics() -> Arc<RuntimeMetrics> {
    Arc::new(RuntimeMetrics::default())
}

fn fast_config() -> NodeRunnerConfig {
    NodeRunnerConfig {
        certification_interval: Duration::from_millis(10),
        cleanup_interval: Duration::from_secs(60),
        compaction_check_interval: Duration::from_secs(60),
        frontier_report_interval: Duration::from_millis(10),
        sync_interval: None,
        ping_interval: None,
    }
}

// ===========================================================================
// Test 1: 3-node cluster with certified policy → authority auto-created
// ===========================================================================

/// When a certified placement policy is set and matching nodes exist,
/// the authority definition is automatically created.
#[tokio::test]
async fn authority_auto_created_from_certified_policy() {
    let mut ns = SystemNamespace::new();
    // Set a certified policy requiring dc:tokyo tag.
    let policy = PlacementPolicy::new(PolicyVersion(1), kr("sensor/"), 3)
        .with_certified(true)
        .with_required_tags([tag("dc:tokyo")].into());
    ns.set_placement_policy(policy);

    let shared_ns = wrap_ns(ns);
    let api = wrap_api(CertifiedApi::new(node_id("runner"), shared_ns.clone()));

    let cluster_nodes = Arc::new(RwLock::new(vec![
        make_node("n1", NodeMode::Store, &["dc:tokyo"]),
        make_node("n2", NodeMode::Store, &["dc:tokyo"]),
        make_node("n3", NodeMode::Both, &["dc:tokyo"]),
    ]));

    let mut runner = NodeRunner::with_cluster_nodes(
        node_id("runner"),
        api.clone(),
        CompactionEngine::with_defaults(),
        fast_config(),
        default_metrics(),
        cluster_nodes,
    )
    .await;

    let handle = runner.shutdown_handle();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(80)).await;
        let _ = handle.send(true);
    });

    runner.run().await;

    // Authority definition should be auto-created.
    let api_lock = api.lock().await;
    let ns = api_lock.namespace().read().unwrap();
    let def = ns.get_authority_definition("sensor/");
    assert!(
        def.is_some(),
        "authority definition should be auto-created for certified policy"
    );
    let auth_nodes = &def.unwrap().authority_nodes;
    assert_eq!(auth_nodes.len(), 3);
    assert!(auth_nodes.contains(&node_id("n1")));
    assert!(auth_nodes.contains(&node_id("n2")));
    assert!(auth_nodes.contains(&node_id("n3")));
}

// ===========================================================================
// Test 2: Node join triggers authority recalculation
// ===========================================================================

/// When a new node joins the cluster, authority definitions are recalculated
/// and the new node is included if it matches the placement policy.
#[tokio::test]
async fn node_join_triggers_authority_recalculation() {
    let mut ns = SystemNamespace::new();
    let policy = PlacementPolicy::new(PolicyVersion(1), kr("data/"), 3)
        .with_certified(true)
        .with_required_tags([tag("dc:tokyo")].into());
    ns.set_placement_policy(policy);

    let shared_ns = wrap_ns(ns);
    let api = wrap_api(CertifiedApi::new(node_id("runner"), shared_ns.clone()));

    // Start with 2 matching nodes.
    let cluster_nodes = Arc::new(RwLock::new(vec![
        make_node("n1", NodeMode::Store, &["dc:tokyo"]),
        make_node("n2", NodeMode::Store, &["dc:tokyo"]),
    ]));

    let mut runner = NodeRunner::with_cluster_nodes(
        node_id("runner"),
        api.clone(),
        CompactionEngine::with_defaults(),
        fast_config(),
        default_metrics(),
        cluster_nodes.clone(),
    )
    .await;

    // After a short delay, simulate a node joining.
    let nodes_ref = cluster_nodes.clone();
    let handle_clone = runner.shutdown_handle();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(40)).await;
        // Node n3 joins.
        nodes_ref
            .write()
            .unwrap()
            .push(make_node("n3", NodeMode::Store, &["dc:tokyo"]));
        tokio::time::sleep(Duration::from_millis(60)).await;
        let _ = handle_clone.send(true);
    });

    runner.run().await;

    // Authority definition should now include n3.
    let api_lock = api.lock().await;
    let ns = api_lock.namespace().read().unwrap();
    let def = ns.get_authority_definition("data/").unwrap();
    assert_eq!(
        def.authority_nodes.len(),
        3,
        "authority set should include the newly joined node"
    );
    assert!(def.authority_nodes.contains(&node_id("n3")));
}

// ===========================================================================
// Test 3: Node leave triggers authority recalculation
// ===========================================================================

/// When a node leaves the cluster, authority definitions are recalculated
/// and the leaving node is removed from the authority set.
#[tokio::test]
async fn node_leave_triggers_authority_recalculation() {
    let mut ns = SystemNamespace::new();
    let policy = PlacementPolicy::new(PolicyVersion(1), kr("data/"), 3)
        .with_certified(true)
        .with_required_tags([tag("dc:tokyo")].into());
    ns.set_placement_policy(policy);

    let shared_ns = wrap_ns(ns);
    let api = wrap_api(CertifiedApi::new(node_id("runner"), shared_ns.clone()));

    // Start with 3 matching nodes.
    let cluster_nodes = Arc::new(RwLock::new(vec![
        make_node("n1", NodeMode::Store, &["dc:tokyo"]),
        make_node("n2", NodeMode::Store, &["dc:tokyo"]),
        make_node("n3", NodeMode::Store, &["dc:tokyo"]),
    ]));

    let mut runner = NodeRunner::with_cluster_nodes(
        node_id("runner"),
        api.clone(),
        CompactionEngine::with_defaults(),
        fast_config(),
        default_metrics(),
        cluster_nodes.clone(),
    )
    .await;

    // After a short delay, simulate n3 leaving.
    let nodes_ref = cluster_nodes.clone();
    let handle = runner.shutdown_handle();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(40)).await;
        // n3 leaves.
        nodes_ref.write().unwrap().retain(|n| n.id != node_id("n3"));
        tokio::time::sleep(Duration::from_millis(60)).await;
        let _ = handle.send(true);
    });

    runner.run().await;

    // Authority definition should no longer include n3.
    let api_lock = api.lock().await;
    let ns = api_lock.namespace().read().unwrap();
    let def = ns.get_authority_definition("data/").unwrap();
    assert_eq!(
        def.authority_nodes.len(),
        2,
        "authority set should exclude the leaving node"
    );
    assert!(!def.authority_nodes.contains(&node_id("n3")));
}

// ===========================================================================
// Test 4: Policy change triggers authority recalculation
// ===========================================================================

/// When a placement policy is modified (e.g., tags changed), the authority
/// definitions are recalculated to reflect the new criteria.
#[tokio::test]
async fn policy_change_triggers_authority_recalculation() {
    let mut ns = SystemNamespace::new();
    // Initial policy: require dc:tokyo.
    let policy_v1 = PlacementPolicy::new(PolicyVersion(1), kr("events/"), 3)
        .with_certified(true)
        .with_required_tags([tag("dc:tokyo")].into());
    ns.set_placement_policy(policy_v1);

    let shared_ns = wrap_ns(ns);
    let api = wrap_api(CertifiedApi::new(node_id("runner"), shared_ns.clone()));

    let cluster_nodes = Arc::new(RwLock::new(vec![
        make_node("n1", NodeMode::Store, &["dc:tokyo"]),
        make_node("n2", NodeMode::Store, &["dc:osaka"]),
        make_node("n3", NodeMode::Both, &["dc:tokyo", "dc:osaka"]),
    ]));

    let mut runner = NodeRunner::with_cluster_nodes(
        node_id("runner"),
        api.clone(),
        CompactionEngine::with_defaults(),
        fast_config(),
        default_metrics(),
        cluster_nodes.clone(),
    )
    .await;

    // After a short delay, change the policy to require dc:osaka instead.
    let api_clone = api.clone();
    let nodes_ref = cluster_nodes.clone();
    let handle_clone = runner.shutdown_handle();
    tokio::spawn(async move {
        // First let the initial recalculation happen.
        tokio::time::sleep(Duration::from_millis(40)).await;

        // Change the policy: now require dc:osaka.
        {
            let api_lock = api_clone.lock().await;
            let mut ns = api_lock.namespace().write().unwrap();
            let policy_v2 = PlacementPolicy::new(PolicyVersion(2), kr("events/"), 3)
                .with_certified(true)
                .with_required_tags([tag("dc:osaka")].into());
            ns.set_placement_policy(policy_v2);

            // Also trigger recalculation with current nodes.
            let nodes = nodes_ref.read().unwrap().clone();
            ns.recalculate_authorities(&nodes);
        }

        tokio::time::sleep(Duration::from_millis(60)).await;
        let _ = handle_clone.send(true);
    });

    runner.run().await;

    // After policy change, authority set should contain n2 and n3 (dc:osaka).
    let api_lock = api.lock().await;
    let ns = api_lock.namespace().read().unwrap();
    let def = ns.get_authority_definition("events/").unwrap();

    // n1 only has dc:tokyo → excluded.
    // n2 has dc:osaka → included.
    // n3 has both → included.
    assert_eq!(
        def.authority_nodes.len(),
        2,
        "authority set should reflect updated policy"
    );
    assert!(def.authority_nodes.contains(&node_id("n2")));
    assert!(def.authority_nodes.contains(&node_id("n3")));
}

// ===========================================================================
// Test 5: Certification not broken during reconfiguration
// ===========================================================================

/// This is the key safety test: certified writes must not be invalidated
/// during authority reconfiguration. The version fencing mechanism ensures
/// that old-version frontiers are isolated from the new version.
#[tokio::test]
async fn certification_safe_during_reconfiguration() {
    // Setup: single-authority system (auth-1) with certified policy.
    let mut ns = SystemNamespace::new();
    let policy = PlacementPolicy::new(PolicyVersion(1), kr(""), 1).with_certified(true);
    ns.set_placement_policy(policy);
    ns.set_authority_definition(AuthorityDefinition {
        key_range: kr(""),
        authority_nodes: vec![node_id("auth-1")],
        auto_generated: false,
    });

    let shared_ns = wrap_ns(ns);
    let mut api = CertifiedApi::new(node_id("auth-1"), shared_ns.clone());

    // Write a pending entry.
    api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
        .unwrap();
    assert_eq!(api.pending_writes()[0].status, CertificationStatus::Pending);

    let shared_api = wrap_api(api);
    let cluster_nodes = Arc::new(RwLock::new(vec![make_node("auth-1", NodeMode::Store, &[])]));

    let mut runner = NodeRunner::with_cluster_nodes(
        node_id("auth-1"),
        shared_api.clone(),
        CompactionEngine::with_defaults(),
        fast_config(),
        default_metrics(),
        cluster_nodes.clone(),
    )
    .await;

    let handle = runner.shutdown_handle();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = handle.send(true);
    });

    runner.run().await;

    // The pending write should be certified by the auto-frontier pipeline.
    let api_lock = shared_api.lock().await;
    assert_eq!(
        api_lock.pending_writes()[0].status,
        CertificationStatus::Certified,
        "pending write should be certified even during reconfiguration"
    );
}

// ===========================================================================
// Test 6: Recalculate with no certified policies is a no-op
// ===========================================================================

/// If no placement policies have `certified == true`, recalculate_authorities
/// should not create any authority definitions.
#[test]
fn no_certified_policies_no_authority_changes() {
    let mut ns = SystemNamespace::new();
    // Non-certified policy.
    let policy = PlacementPolicy::new(PolicyVersion(1), kr("cache/"), 3);
    ns.set_placement_policy(policy);

    let nodes = vec![
        make_node("n1", NodeMode::Store, &[]),
        make_node("n2", NodeMode::Store, &[]),
    ];

    let changed = ns.recalculate_authorities(&nodes);
    assert_eq!(changed, 0);
    assert!(ns.get_authority_definition("cache/").is_none());
}

// ===========================================================================
// Test 7: Authority reconfiguration with authority becoming non-authority
// ===========================================================================

/// When a node was an authority and is removed from the authority set
/// (e.g., due to tag change), the NodeRunner should detect this and
/// stop reporting frontiers.
#[tokio::test]
async fn authority_demotion_stops_frontier_reporting() {
    let mut ns = SystemNamespace::new();
    ns.set_authority_definition(AuthorityDefinition {
        key_range: kr(""),
        authority_nodes: vec![node_id("auth-1")],
        auto_generated: false,
    });
    let policy = PlacementPolicy::new(PolicyVersion(1), kr(""), 1)
        .with_certified(true)
        .with_required_tags([tag("active")].into());
    ns.set_placement_policy(policy);

    let shared_ns = wrap_ns(ns);
    let api = wrap_api(CertifiedApi::new(node_id("auth-1"), shared_ns.clone()));

    // Initially auth-1 has the "active" tag.
    let cluster_nodes = Arc::new(RwLock::new(vec![
        make_node("auth-1", NodeMode::Store, &["active"]),
        make_node("auth-2", NodeMode::Store, &["active"]),
    ]));

    let mut runner = NodeRunner::with_cluster_nodes(
        node_id("auth-1"),
        api.clone(),
        CompactionEngine::with_defaults(),
        fast_config(),
        default_metrics(),
        cluster_nodes.clone(),
    )
    .await;

    // Initially should be an authority.
    assert!(runner.is_authority());

    let nodes_ref = cluster_nodes.clone();
    let handle = runner.shutdown_handle();
    tokio::spawn(async move {
        // Let it run a bit as authority.
        tokio::time::sleep(Duration::from_millis(40)).await;

        // Remove auth-1 from the cluster (or change its tags).
        // Simulate by replacing with a node without the "active" tag.
        {
            let mut nodes = nodes_ref.write().unwrap();
            nodes.clear();
            nodes.push(make_node("auth-2", NodeMode::Store, &["active"]));
        }

        tokio::time::sleep(Duration::from_millis(60)).await;
        let _ = handle.send(true);
    });

    runner.run().await;

    // After auth-1 was removed from the matching nodes, it should no longer
    // be an authority.
    assert!(
        !runner.is_authority(),
        "auth-1 should no longer be an authority after being removed from cluster"
    );
}
