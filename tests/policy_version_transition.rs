//! Integration tests for policy version safe transition (#98).
//!
//! Verifies that frontier judgment is isolated at version boundaries,
//! that certified judgment rules are correct during migration, and
//! that the NodeRunner auto-detects version changes.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use asteroidb_poc::api::certified::{CertifiedApi, OnTimeout};
use asteroidb_poc::authority::ack_frontier::{AckFrontier, AckFrontierSet};
use asteroidb_poc::compaction::CompactionEngine;
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::crdt::pn_counter::PnCounter;
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::ops::metrics::RuntimeMetrics;
use asteroidb_poc::placement::PlacementPolicy;
use asteroidb_poc::runtime::{NodeRunner, NodeRunnerConfig};
use asteroidb_poc::store::kv::CrdtValue;
use asteroidb_poc::types::{CertificationStatus, KeyRange, NodeId, PolicyVersion};

fn node(name: &str) -> NodeId {
    NodeId(name.into())
}

fn kr(prefix: &str) -> KeyRange {
    KeyRange {
        prefix: prefix.into(),
    }
}

fn wrap_ns(ns: SystemNamespace) -> Arc<RwLock<SystemNamespace>> {
    Arc::new(RwLock::new(ns))
}

fn counter_value(n: i64) -> CrdtValue {
    let mut counter = PnCounter::new();
    for _ in 0..n {
        counter.increment(&node("writer"));
    }
    CrdtValue::Counter(counter)
}

fn make_frontier_v(
    authority: &str,
    physical: u64,
    logical: u32,
    prefix: &str,
    version: u64,
) -> AckFrontier {
    AckFrontier {
        authority_id: NodeId(authority.into()),
        frontier_hlc: HlcTimestamp {
            physical,
            logical,
            node_id: authority.into(),
        },
        key_range: KeyRange {
            prefix: prefix.into(),
        },
        policy_version: PolicyVersion(version),
        digest_hash: format!("{authority}-{physical}-{logical}"),
    }
}

// ---------------------------------------------------------------
// Test 1: Version isolation in AckFrontierSet
// ---------------------------------------------------------------

#[test]
fn version_isolation_in_frontier_set() {
    let mut set = AckFrontierSet::new();

    // Set up v1 frontiers.
    set.update(make_frontier_v("auth-1", 100, 0, "data/", 1));
    set.update(make_frontier_v("auth-2", 200, 0, "data/", 1));
    set.update(make_frontier_v("auth-3", 150, 0, "data/", 1));

    // Fence v1.
    set.fence_version(&kr("data/"), PolicyVersion(1));

    // Set up v2 frontiers (fresh start).
    set.update(make_frontier_v("auth-1", 10, 0, "data/", 2));
    set.update(make_frontier_v("auth-2", 20, 0, "data/", 2));
    set.update(make_frontier_v("auth-3", 15, 0, "data/", 2));

    // v1 frontiers should be frozen (existing entries readable but no new updates).
    let blocked = set.update(make_frontier_v("auth-1", 999, 0, "data/", 1));
    assert!(!blocked, "fenced v1 should reject new updates");

    // v1 majority frontier: sorted [100, 150, 200], majority=2 -> 150.
    let mf_v1 = set
        .majority_frontier_for_scope(&kr("data/"), &PolicyVersion(1), 3)
        .unwrap();
    assert_eq!(mf_v1.physical, 150);

    // v2 majority frontier: sorted [10, 15, 20], majority=2 -> 15.
    let mf_v2 = set
        .majority_frontier_for_scope(&kr("data/"), &PolicyVersion(2), 3)
        .unwrap();
    assert_eq!(mf_v2.physical, 15);

    // Certification isolation: ts=100 certified at v1 but not at v2.
    let ts_100 = HlcTimestamp {
        physical: 100,
        logical: 0,
        node_id: "client".into(),
    };
    assert!(set.is_certified_at_for_scope(&ts_100, &kr("data/"), &PolicyVersion(1), 3));
    assert!(!set.is_certified_at_for_scope(&ts_100, &kr("data/"), &PolicyVersion(2), 3));
}

// ---------------------------------------------------------------
// Test 2: Full version switch with CertifiedApi
// ---------------------------------------------------------------

#[test]
fn full_version_switch_with_certified_api() {
    let mut ns = SystemNamespace::new();
    ns.set_authority_definition(AuthorityDefinition {
        key_range: kr("data/"),
        authority_nodes: vec![node("auth-1"), node("auth-2"), node("auth-3")],
        auto_generated: false,
    });
    // Start at policy version 1.
    ns.set_placement_policy(
        PlacementPolicy::new(PolicyVersion(1), kr("data/"), 3).with_certified(true),
    );

    let shared_ns = wrap_ns(ns);
    let mut api = CertifiedApi::new(node("node-1"), shared_ns.clone());

    // Write under v1.
    api.certified_write("data/key1".into(), counter_value(1), OnTimeout::Pending)
        .unwrap();
    let ts1 = api.pending_writes()[0].timestamp.physical;
    assert_eq!(api.pending_writes()[0].policy_version, PolicyVersion(1));

    // Advance v1 frontiers past the write.
    api.update_frontier(make_frontier_v("auth-1", ts1 + 100, 0, "data/", 1));
    api.update_frontier(make_frontier_v("auth-2", ts1 + 200, 0, "data/", 1));
    api.process_certifications();

    assert_eq!(
        api.get_certification_status("data/key1"),
        CertificationStatus::Certified
    );

    // Transition to v2.
    {
        let mut ns = shared_ns.write().unwrap();
        ns.set_placement_policy(
            PlacementPolicy::new(PolicyVersion(2), kr("data/"), 3).with_certified(true),
        );
    }

    // Fence v1.
    api.fence_version(&kr("data/"), PolicyVersion(1));

    // Write under v2.
    api.certified_write("data/key2".into(), counter_value(2), OnTimeout::Pending)
        .unwrap();
    let pw_idx = api.pending_writes().len() - 1;
    let ts2 = api.pending_writes()[pw_idx].timestamp.physical;
    assert_eq!(
        api.pending_writes()[pw_idx].policy_version,
        PolicyVersion(2)
    );

    // v1 frontiers should be blocked.
    let blocked = api.update_frontier(make_frontier_v("auth-1", ts2 + 500, 0, "data/", 1));
    assert!(!blocked, "fenced v1 should reject frontier updates");

    // v2 frontiers should work.
    api.update_frontier(make_frontier_v("auth-1", ts2 + 100, 0, "data/", 2));
    api.update_frontier(make_frontier_v("auth-2", ts2 + 200, 0, "data/", 2));
    api.process_certifications();

    assert_eq!(
        api.get_certification_status("data/key2"),
        CertificationStatus::Certified
    );
}

// ---------------------------------------------------------------
// Test 3: Fencing preserves existing entries
// ---------------------------------------------------------------

#[test]
fn fencing_preserves_existing_entries() {
    let mut set = AckFrontierSet::new();

    // Build up v1 state.
    set.update(make_frontier_v("auth-1", 100, 0, "user/", 1));
    set.update(make_frontier_v("auth-2", 200, 0, "user/", 1));
    set.update(make_frontier_v("auth-3", 150, 0, "user/", 1));

    // Record pre-fence state.
    let pre_mf = set
        .majority_frontier_for_scope(&kr("user/"), &PolicyVersion(1), 3)
        .unwrap();
    assert_eq!(pre_mf.physical, 150);

    // Fence v1.
    set.fence_version(&kr("user/"), PolicyVersion(1));

    // Post-fence: existing entries are still readable.
    let post_mf = set
        .majority_frontier_for_scope(&kr("user/"), &PolicyVersion(1), 3)
        .unwrap();
    assert_eq!(
        post_mf.physical, 150,
        "fencing must preserve existing entries"
    );

    // Certification still works against existing entries.
    let ts = HlcTimestamp {
        physical: 100,
        logical: 0,
        node_id: "client".into(),
    };
    assert!(set.is_certified_at_for_scope(&ts, &kr("user/"), &PolicyVersion(1), 3));
}

// ---------------------------------------------------------------
// Test 4: Cross-version frontier pollution prevented
// ---------------------------------------------------------------

#[test]
fn cross_version_frontier_pollution_prevented() {
    let mut ns = SystemNamespace::new();
    ns.set_authority_definition(AuthorityDefinition {
        key_range: kr("data/"),
        authority_nodes: vec![node("auth-1"), node("auth-2"), node("auth-3")],
        auto_generated: false,
    });
    ns.set_placement_policy(
        PlacementPolicy::new(PolicyVersion(1), kr("data/"), 3).with_certified(true),
    );

    let shared_ns = wrap_ns(ns);
    let mut api = CertifiedApi::new(node("node-1"), shared_ns.clone());

    // Transition to v2 and fence v1.
    {
        let mut ns = shared_ns.write().unwrap();
        ns.set_placement_policy(
            PlacementPolicy::new(PolicyVersion(2), kr("data/"), 3).with_certified(true),
        );
    }
    api.fence_version(&kr("data/"), PolicyVersion(1));

    // Write under v2.
    api.certified_write("data/sensor".into(), counter_value(42), OnTimeout::Pending)
        .unwrap();
    let ts = api.pending_writes()[0].timestamp.physical;
    assert_eq!(api.pending_writes()[0].policy_version, PolicyVersion(2));

    // Attempt to advance using v1 frontiers (should be blocked).
    let blocked1 = api.update_frontier(make_frontier_v("auth-1", ts + 100, 0, "data/", 1));
    let blocked2 = api.update_frontier(make_frontier_v("auth-2", ts + 200, 0, "data/", 1));
    assert!(!blocked1);
    assert!(!blocked2);

    api.process_certifications();

    // Write should still be pending (no v2 frontiers).
    assert_eq!(
        api.get_certification_status("data/sensor"),
        CertificationStatus::Pending,
        "v1 frontiers must not certify a v2 write"
    );

    // Now advance v2 frontiers.
    api.update_frontier(make_frontier_v("auth-1", ts + 100, 0, "data/", 2));
    api.update_frontier(make_frontier_v("auth-2", ts + 200, 0, "data/", 2));
    api.process_certifications();

    assert_eq!(
        api.get_certification_status("data/sensor"),
        CertificationStatus::Certified,
        "v2 frontiers should certify the v2 write"
    );
}

// ---------------------------------------------------------------
// Test 5: NodeRunner auto-detects version changes
// ---------------------------------------------------------------

#[tokio::test]
async fn node_runner_auto_detects_version_changes() {
    let mut ns = SystemNamespace::new();
    ns.set_authority_definition(AuthorityDefinition {
        key_range: kr("data/"),
        authority_nodes: vec![node("auth-1")],
        auto_generated: false,
    });
    ns.set_placement_policy(
        PlacementPolicy::new(PolicyVersion(1), kr("data/"), 1).with_certified(true),
    );

    let shared_ns = wrap_ns(ns);
    let api = CertifiedApi::new(node("auth-1"), shared_ns.clone());
    let shared_api = Arc::new(tokio::sync::Mutex::new(api));
    let engine = CompactionEngine::with_defaults();
    let metrics = Arc::new(RuntimeMetrics::default());

    let config = NodeRunnerConfig {
        certification_interval: Duration::from_millis(10),
        cleanup_interval: Duration::from_secs(60),
        compaction_check_interval: Duration::from_secs(60),
        frontier_report_interval: Duration::from_millis(10),
        sync_interval: None,
        ping_interval: None,
        ..NodeRunnerConfig::default()
    };

    let mut runner =
        NodeRunner::new(node("auth-1"), shared_api.clone(), engine, config, metrics).await;
    let handle = runner.shutdown_handle();

    // Change policy version while runner is running.
    let ns_clone = shared_ns.clone();
    let api_clone = shared_api.clone();
    tokio::spawn(async move {
        // Wait for a few ticks to establish v1 frontiers.
        tokio::time::sleep(Duration::from_millis(40)).await;

        // Bump to v2.
        {
            let mut ns = ns_clone.write().unwrap();
            ns.set_placement_policy(
                PlacementPolicy::new(PolicyVersion(2), kr("data/"), 1).with_certified(true),
            );
        }

        // Wait for detection.
        tokio::time::sleep(Duration::from_millis(40)).await;

        // Verify that v1 was fenced.
        let api = api_clone.lock().await;
        assert!(
            api.is_version_fenced(&kr("data/"), &PolicyVersion(1)),
            "NodeRunner should auto-fence old version"
        );

        // Shutdown.
        let _ = handle.send(true);
    });

    runner.run().await;
}

// ---------------------------------------------------------------
// Test 6: Multiple version transitions
// ---------------------------------------------------------------

#[test]
fn multiple_version_transitions() {
    let mut ns = SystemNamespace::new();
    ns.set_authority_definition(AuthorityDefinition {
        key_range: kr("data/"),
        authority_nodes: vec![node("auth-1"), node("auth-2"), node("auth-3")],
        auto_generated: false,
    });
    ns.set_placement_policy(
        PlacementPolicy::new(PolicyVersion(1), kr("data/"), 3).with_certified(true),
    );

    let shared_ns = wrap_ns(ns);
    let mut api = CertifiedApi::new(node("node-1"), shared_ns.clone());

    // ---- v1 write and certify ----
    api.certified_write("data/v1key".into(), counter_value(1), OnTimeout::Pending)
        .unwrap();
    let ts1 = api.pending_writes()[0].timestamp.physical;

    api.update_frontier(make_frontier_v("auth-1", ts1 + 100, 0, "data/", 1));
    api.update_frontier(make_frontier_v("auth-2", ts1 + 200, 0, "data/", 1));
    api.process_certifications();
    assert_eq!(
        api.get_certification_status("data/v1key"),
        CertificationStatus::Certified
    );

    // ---- Transition v1 -> v2 ----
    {
        let mut ns = shared_ns.write().unwrap();
        ns.set_placement_policy(
            PlacementPolicy::new(PolicyVersion(2), kr("data/"), 3).with_certified(true),
        );
    }
    api.fence_version(&kr("data/"), PolicyVersion(1));

    // Write under v2.
    api.certified_write("data/v2key".into(), counter_value(2), OnTimeout::Pending)
        .unwrap();
    let pw_v2_idx = api.pending_writes().len() - 1;
    let ts2 = api.pending_writes()[pw_v2_idx].timestamp.physical;
    assert_eq!(
        api.pending_writes()[pw_v2_idx].policy_version,
        PolicyVersion(2)
    );

    api.update_frontier(make_frontier_v("auth-1", ts2 + 100, 0, "data/", 2));
    api.update_frontier(make_frontier_v("auth-2", ts2 + 200, 0, "data/", 2));
    api.process_certifications();
    assert_eq!(
        api.get_certification_status("data/v2key"),
        CertificationStatus::Certified
    );

    // ---- Transition v2 -> v3 ----
    {
        let mut ns = shared_ns.write().unwrap();
        ns.set_placement_policy(
            PlacementPolicy::new(PolicyVersion(3), kr("data/"), 3).with_certified(true),
        );
    }
    api.fence_version(&kr("data/"), PolicyVersion(2));

    // Write under v3.
    api.certified_write("data/v3key".into(), counter_value(3), OnTimeout::Pending)
        .unwrap();
    let pw_v3_idx = api.pending_writes().len() - 1;
    let ts3 = api.pending_writes()[pw_v3_idx].timestamp.physical;
    assert_eq!(
        api.pending_writes()[pw_v3_idx].policy_version,
        PolicyVersion(3)
    );

    // v1 and v2 frontiers should both be blocked.
    assert!(!api.update_frontier(make_frontier_v("auth-1", ts3 + 500, 0, "data/", 1)));
    assert!(!api.update_frontier(make_frontier_v("auth-1", ts3 + 500, 0, "data/", 2)));

    // v3 frontiers should work.
    api.update_frontier(make_frontier_v("auth-1", ts3 + 100, 0, "data/", 3));
    api.update_frontier(make_frontier_v("auth-2", ts3 + 200, 0, "data/", 3));
    api.process_certifications();
    assert_eq!(
        api.get_certification_status("data/v3key"),
        CertificationStatus::Certified
    );
}

// ---------------------------------------------------------------
// Test 7: Concurrent key ranges have independent fencing
// ---------------------------------------------------------------

#[test]
fn concurrent_key_ranges_independent_fencing() {
    let mut set = AckFrontierSet::new();

    // Set up v1 for both user/ and order/.
    set.update(make_frontier_v("auth-1", 100, 0, "user/", 1));
    set.update(make_frontier_v("auth-2", 200, 0, "user/", 1));

    set.update(make_frontier_v("auth-1", 300, 0, "order/", 1));
    set.update(make_frontier_v("auth-2", 400, 0, "order/", 1));

    // Fence only user/ v1.
    set.fence_version(&kr("user/"), PolicyVersion(1));

    // user/ v1 should be blocked.
    assert!(!set.update(make_frontier_v("auth-1", 999, 0, "user/", 1)));

    // order/ v1 should still accept updates.
    assert!(set.update(make_frontier_v("auth-1", 500, 0, "order/", 1)));

    // Verify order/ frontier advanced.
    let scope_order = asteroidb_poc::authority::ack_frontier::FrontierScope::new(
        kr("order/"),
        PolicyVersion(1),
        node("auth-1"),
    );
    assert_eq!(
        set.get_scoped(&scope_order).unwrap().frontier_hlc.physical,
        500
    );
}
