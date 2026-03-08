//! Integration tests for partition tolerance (Issue #32).
//!
//! Network partitions are simulated by controlling which nodes exchange
//! CRDT state via `merge_remote`.  A "partition" simply means that
//! `merge_remote` is not called between the two groups.  "Healing" means
//! resuming merges.
//!
//! Node topology: 3 nodes {A, B, C} with 3 Authority nodes {auth-1, auth-2, auth-3}.
//! Partition: {A, B} vs {C}.

use std::sync::{Arc, RwLock};

use asteroidb_poc::api::certified::{CertifiedApi, OnTimeout};
use asteroidb_poc::api::eventual::EventualApi;
use asteroidb_poc::authority::ack_frontier::{AckFrontier, AckFrontierSet};
use asteroidb_poc::compaction::{CompactionConfig, CompactionEngine};
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::crdt::pn_counter::PnCounter;
use asteroidb_poc::error::CrdtError;
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::placement::PlacementPolicy;
use asteroidb_poc::store::kv::CrdtValue;
use asteroidb_poc::types::{CertificationStatus, KeyRange, NodeId, PolicyVersion};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn node(name: &str) -> NodeId {
    NodeId(name.into())
}

fn make_ts(physical: u64, logical: u32, node_id: &str) -> HlcTimestamp {
    HlcTimestamp {
        physical,
        logical,
        node_id: node_id.into(),
    }
}

fn make_frontier(authority: &str, physical: u64, logical: u32, prefix: &str) -> AckFrontier {
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
        policy_version: PolicyVersion(1),
        digest_hash: format!("{authority}-{physical}-{logical}"),
    }
}

fn wrap_ns(ns: SystemNamespace) -> Arc<RwLock<SystemNamespace>> {
    Arc::new(RwLock::new(ns))
}

/// Create a namespace with a catch-all authority definition (prefix "").
fn default_namespace() -> Arc<RwLock<SystemNamespace>> {
    let mut ns = SystemNamespace::new();
    ns.set_authority_definition(AuthorityDefinition {
        key_range: KeyRange { prefix: "".into() },
        authority_nodes: vec![node("auth-1"), node("auth-2"), node("auth-3")],
        auto_generated: false,
    });
    ns.set_placement_policy(PlacementPolicy::new(
        PolicyVersion(1),
        KeyRange { prefix: "".into() },
        3,
    ));
    wrap_ns(ns)
}

/// Bidirectional merge between two EventualApi nodes for a single key.
fn sync_key(a: &mut EventualApi, b: &mut EventualApi, key: &str) {
    if let Some(val) = a.get_eventual(key) {
        let _ = b.merge_remote(key.to_string(), val);
    }
    if let Some(val) = b.get_eventual(key) {
        let _ = a.merge_remote(key.to_string(), val);
    }
}

/// Bidirectional merge between two EventualApi nodes for ALL shared keys.
fn sync_all(a: &mut EventualApi, b: &mut EventualApi) {
    let all_keys: Vec<String> = a
        .keys()
        .into_iter()
        .chain(b.keys())
        .cloned()
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    for key in &all_keys {
        if let Some(val) = a.get_eventual(key) {
            let cloned = val.clone();
            let _ = b.merge_remote(key.clone(), &cloned);
        }
        if let Some(val) = b.get_eventual(key) {
            let cloned = val.clone();
            let _ = a.merge_remote(key.clone(), &cloned);
        }
    }
}

/// Get counter value from an EventualApi.
fn counter_val(api: &EventualApi, key: &str) -> i64 {
    match api.get_eventual(key) {
        Some(CrdtValue::Counter(c)) => c.value(),
        _ => panic!("expected Counter at key '{key}'"),
    }
}

/// Check if an OR-Set contains a given element.
fn set_contains(api: &EventualApi, key: &str, elem: &str) -> bool {
    match api.get_eventual(key) {
        Some(CrdtValue::Set(s)) => s.contains(&elem.to_string()),
        None => false,
        _ => panic!("expected Set at key '{key}'"),
    }
}

/// Get register value from an EventualApi.
fn register_val(api: &EventualApi, key: &str) -> Option<String> {
    match api.get_eventual(key) {
        Some(CrdtValue::Register(r)) => r.get().cloned(),
        None => None,
        _ => panic!("expected Register at key '{key}'"),
    }
}

// ===========================================================================
// 1. Partition behavior: writes during partition
// ===========================================================================

#[test]
fn partition_eventual_write_succeeds_in_both_partitions() {
    // {A, B} partition and {C} partition — eventual writes should succeed
    // locally in both partitions.
    let mut node_a = EventualApi::new(node("node-a"));
    let mut node_b = EventualApi::new(node("node-b"));
    let mut node_c = EventualApi::new(node("node-c"));

    // --- Before partition: share initial state ---
    node_a.eventual_counter_inc("hits").unwrap();
    sync_key(&mut node_a, &mut node_b, "hits");
    sync_key(&mut node_a, &mut node_c, "hits");

    assert_eq!(counter_val(&node_a, "hits"), 1);
    assert_eq!(counter_val(&node_b, "hits"), 1);
    assert_eq!(counter_val(&node_c, "hits"), 1);

    // --- Partition: {A, B} vs {C} ---
    // Partition 1: A and B continue to write and sync with each other.
    node_a.eventual_counter_inc("hits").unwrap();
    node_b.eventual_counter_inc("hits").unwrap();
    sync_key(&mut node_a, &mut node_b, "hits");

    // Partition 2: C writes independently.
    node_c.eventual_counter_inc("hits").unwrap();
    node_c.eventual_counter_inc("hits").unwrap();

    // Verify partition 1 sees its own writes.
    assert_eq!(counter_val(&node_a, "hits"), 3); // initial(1) + A(1) + B(1)
    assert_eq!(counter_val(&node_b, "hits"), 3);

    // Verify partition 2 sees its own writes.
    assert_eq!(counter_val(&node_c, "hits"), 3); // initial(1) + C(2)
}

#[test]
fn partition_certified_write_fails_without_majority() {
    // During partition, {C} alone cannot reach majority (needs 2 of 3 authorities).
    // Auth-1 and Auth-2 are in partition {A,B}; Auth-3 is with {C}.
    let mut cert_c = CertifiedApi::new(node("node-c"), default_namespace());

    // Only auth-3 reports a frontier (1 of 3 — no majority).
    cert_c.update_frontier(make_frontier("auth-3", 1000, 0, ""));

    // certified_write with OnTimeout::Error should fail.
    let counter = CrdtValue::Counter(PnCounter::new());
    let result = cert_c.certified_write("key1".into(), counter, OnTimeout::Error);
    assert!(matches!(result, Err(CrdtError::Timeout)));
}

#[test]
fn partition_certified_write_pending_without_majority() {
    // Same scenario with OnTimeout::Pending — should return Pending.
    let mut cert_c = CertifiedApi::new(node("node-c"), default_namespace());
    cert_c.update_frontier(make_frontier("auth-3", 1000, 0, ""));

    let counter = CrdtValue::Counter(PnCounter::new());
    let result = cert_c.certified_write("key1".into(), counter, OnTimeout::Pending);
    assert_eq!(result.unwrap(), CertificationStatus::Pending);
    assert_eq!(
        cert_c.get_certification_status("key1"),
        CertificationStatus::Pending
    );
}

#[test]
fn partition_concurrent_update_same_key_counter() {
    // Both partitions increment the same counter key concurrently.
    let mut node_a = EventualApi::new(node("node-a"));
    let mut node_c = EventualApi::new(node("node-c"));

    // Partition 1: A increments 5 times.
    for _ in 0..5 {
        node_a.eventual_counter_inc("shared").unwrap();
    }

    // Partition 2: C increments 3 times.
    for _ in 0..3 {
        node_c.eventual_counter_inc("shared").unwrap();
    }

    // Before merge: each sees only their own writes.
    assert_eq!(counter_val(&node_a, "shared"), 5);
    assert_eq!(counter_val(&node_c, "shared"), 3);

    // After merge: converge to 5 + 3 = 8.
    sync_key(&mut node_a, &mut node_c, "shared");
    assert_eq!(counter_val(&node_a, "shared"), 8);
    assert_eq!(counter_val(&node_c, "shared"), 8);
}

#[test]
fn partition_concurrent_update_same_key_register() {
    // Both partitions set the same LWW-Register concurrently.
    let mut node_a = EventualApi::new(node("node-a"));
    let mut node_c = EventualApi::new(node("node-c"));

    // A sets with an earlier HLC; C sets with a later HLC.
    // (In practice HLC uses wall clock, but since the writes happen
    // almost simultaneously we just verify LWW semantics post-merge.)
    node_a
        .eventual_register_set("config", "value-a".into())
        .unwrap();
    node_c
        .eventual_register_set("config", "value-c".into())
        .unwrap();

    // Merge and verify LWW: the value with the higher timestamp wins.
    sync_key(&mut node_a, &mut node_c, "config");

    let a_val = register_val(&node_a, "config").unwrap();
    let c_val = register_val(&node_c, "config").unwrap();
    assert_eq!(a_val, c_val, "LWW-Register must converge after merge");
}

// ===========================================================================
// 2. Post-partition convergence
// ===========================================================================

#[test]
fn convergence_after_partition_heal_all_nodes_same_state() {
    // Full scenario: 3 nodes, partition, independent writes, heal, converge.
    let mut node_a = EventualApi::new(node("node-a"));
    let mut node_b = EventualApi::new(node("node-b"));
    let mut node_c = EventualApi::new(node("node-c"));

    // --- Pre-partition: all nodes agree ---
    node_a.eventual_counter_inc("count").unwrap();
    sync_key(&mut node_a, &mut node_b, "count");
    sync_key(&mut node_a, &mut node_c, "count");

    // --- Partition {A,B} vs {C} ---
    // Partition 1 writes:
    node_a.eventual_counter_inc("count").unwrap();
    node_b.eventual_counter_inc("count").unwrap();
    sync_key(&mut node_a, &mut node_b, "count");

    // Partition 2 writes:
    node_c.eventual_counter_inc("count").unwrap();

    // --- Heal: merge all pairs ---
    sync_key(&mut node_a, &mut node_c, "count");
    sync_key(&mut node_b, &mut node_c, "count");
    // A second round to ensure full propagation
    sync_key(&mut node_a, &mut node_b, "count");

    // All nodes should converge: 1 (pre) + 1 (A) + 1 (B) + 1 (C) = 4
    assert_eq!(counter_val(&node_a, "count"), 4);
    assert_eq!(counter_val(&node_b, "count"), 4);
    assert_eq!(counter_val(&node_c, "count"), 4);
}

#[test]
fn convergence_concurrent_delete_vs_add_or_set_add_wins() {
    // OR-Set with add-wins semantics: during partition, one side adds
    // and the other removes.  After merge, the add should win.
    let mut node_a = EventualApi::new(node("node-a"));
    let mut node_c = EventualApi::new(node("node-c"));

    // Common state: both have "x" in the set.
    node_a.eventual_set_add("items", "x".into()).unwrap();
    sync_key(&mut node_a, &mut node_c, "items");

    assert!(set_contains(&node_a, "items", "x"));
    assert!(set_contains(&node_c, "items", "x"));

    // --- Partition ---
    // Node A re-adds "x" (new dot) during partition.
    node_a.eventual_set_add("items", "x".into()).unwrap();

    // Node C removes "x" (only removes dots it has observed).
    node_c.eventual_set_remove("items", "x").unwrap();
    assert!(!set_contains(&node_c, "items", "x"));

    // --- Heal ---
    sync_key(&mut node_a, &mut node_c, "items");

    // Add-wins: "x" should be present on both sides.
    assert!(
        set_contains(&node_a, "items", "x"),
        "add-wins: element should survive concurrent add + remove"
    );
    assert!(
        set_contains(&node_c, "items", "x"),
        "add-wins: element should survive concurrent add + remove"
    );
}

#[test]
fn convergence_after_compaction_in_one_partition() {
    // One partition runs compaction during the split; after heal the
    // merge should still produce correct results.
    let mut node_a = EventualApi::new(node("node-a"));
    let mut node_c = EventualApi::new(node("node-c"));
    let mut engine = CompactionEngine::new(CompactionConfig {
        time_threshold_ms: 1_000,
        ops_threshold: 3,
    });
    let kr = KeyRange {
        prefix: "data/".into(),
    };

    // Pre-partition: shared state.
    node_a.eventual_counter_inc("data/hits").unwrap();
    sync_key(&mut node_a, &mut node_c, "data/hits");

    // --- Partition ---
    // Partition 1 (A): writes and triggers compaction.
    node_a.eventual_counter_inc("data/hits").unwrap();
    node_a.eventual_counter_inc("data/hits").unwrap();
    node_a.eventual_counter_inc("data/hits").unwrap();

    for _ in 0..3 {
        engine.record_op(&kr);
    }

    let now = make_ts(5000, 0, "node-a");
    assert!(engine.should_checkpoint(&kr, &now));
    let cp = engine.create_checkpoint(kr.clone(), now.clone(), "digest-a".into(), PolicyVersion(1));
    assert_eq!(cp.ops_since_last, 3);

    // Partition 2 (C): writes independently.
    node_c.eventual_counter_inc("data/hits").unwrap();
    node_c.eventual_counter_inc("data/hits").unwrap();

    // --- Heal ---
    sync_key(&mut node_a, &mut node_c, "data/hits");

    // A: initial(1) + 3 + C's 2 = 6
    // C: initial(1) + 2 + A's 3 = 6
    assert_eq!(counter_val(&node_a, "data/hits"), 6);
    assert_eq!(counter_val(&node_c, "data/hits"), 6);
}

#[test]
fn convergence_long_partition_hlc_drift() {
    // Simulate a long partition where physical times diverge significantly.
    let mut node_a = EventualApi::new(node("node-a"));
    let mut node_c = EventualApi::new(node("node-c"));

    // Both start by writing a register.
    node_a
        .eventual_register_set("drift-key", "initial".into())
        .unwrap();
    sync_key(&mut node_a, &mut node_c, "drift-key");

    // --- Partition ---
    // A writes (near present time).
    node_a
        .eventual_register_set("drift-key", "from-a".into())
        .unwrap();

    // C writes (physical time is much later — simulated by writing later).
    // Since the HLC uses real wall-clock, C's timestamp will be >= A's.
    // We just ensure convergence regardless.
    std::thread::sleep(std::time::Duration::from_millis(5));
    node_c
        .eventual_register_set("drift-key", "from-c".into())
        .unwrap();

    // --- Heal ---
    sync_key(&mut node_a, &mut node_c, "drift-key");

    let a_val = register_val(&node_a, "drift-key").unwrap();
    let c_val = register_val(&node_c, "drift-key").unwrap();
    assert_eq!(
        a_val, c_val,
        "LWW-Register must converge even after long partition"
    );
    // The later write (C, with higher HLC timestamp) should win.
    assert_eq!(a_val, "from-c");
}

#[test]
fn convergence_multiple_keys_across_partitions() {
    // Multiple keys written independently across partitions.
    let mut node_a = EventualApi::new(node("node-a"));
    let mut node_b = EventualApi::new(node("node-b"));
    let mut node_c = EventualApi::new(node("node-c"));

    // --- Partition {A,B} vs {C} ---
    // Partition 1 writes.
    node_a.eventual_counter_inc("key1").unwrap();
    node_b.eventual_set_add("key2", "alpha".into()).unwrap();
    sync_key(&mut node_a, &mut node_b, "key1");
    sync_key(&mut node_a, &mut node_b, "key2");

    // Partition 2 writes.
    node_c.eventual_counter_inc("key1").unwrap();
    node_c.eventual_set_add("key2", "beta".into()).unwrap();
    node_c
        .eventual_register_set("key3", "only-c".into())
        .unwrap();

    // --- Heal ---
    sync_all(&mut node_a, &mut node_c);
    sync_all(&mut node_b, &mut node_c);
    sync_all(&mut node_a, &mut node_b);

    // key1: Counter A(1) + C(1) = 2
    assert_eq!(counter_val(&node_a, "key1"), 2);
    assert_eq!(counter_val(&node_b, "key1"), 2);
    assert_eq!(counter_val(&node_c, "key1"), 2);

    // key2: Set should have both "alpha" and "beta"
    assert!(set_contains(&node_a, "key2", "alpha"));
    assert!(set_contains(&node_a, "key2", "beta"));
    assert!(set_contains(&node_c, "key2", "alpha"));
    assert!(set_contains(&node_c, "key2", "beta"));

    // key3: Register created only on C, should propagate to all
    assert_eq!(register_val(&node_a, "key3"), Some("only-c".into()));
    assert_eq!(register_val(&node_b, "key3"), Some("only-c".into()));
    assert_eq!(register_val(&node_c, "key3"), Some("only-c".into()));
}

// ===========================================================================
// 3. Authority failure scenarios
// ===========================================================================

#[test]
fn authority_majority_loss_and_recovery() {
    // With 3 authorities, losing 2 means no majority.
    // When they recover (report updated frontiers), certification resumes.
    let mut cert = CertifiedApi::new(node("node-a"), default_namespace());

    // Write while no authorities have reported.
    let counter = CrdtValue::Counter(PnCounter::new());
    let result = cert.certified_write("key1".into(), counter, OnTimeout::Pending);
    assert_eq!(result.unwrap(), CertificationStatus::Pending);

    let write_ts = cert.pending_writes()[0].timestamp.physical;

    // Only 1 authority reports (no majority).
    cert.update_frontier(make_frontier("auth-3", write_ts + 100, 0, ""));
    cert.process_certifications();
    assert_eq!(
        cert.get_certification_status("key1"),
        CertificationStatus::Pending
    );

    // Authority recovery: 2nd authority reports (now have majority 2/3).
    cert.update_frontier(make_frontier("auth-1", write_ts + 200, 0, ""));
    cert.process_certifications();
    assert_eq!(
        cert.get_certification_status("key1"),
        CertificationStatus::Certified
    );
}

#[test]
fn authority_ack_frontier_handoff_after_replacement() {
    // Simulate an authority node replacement: old authority stops updating,
    // new authority starts from where the old one left off.
    let mut frontiers = AckFrontierSet::new();

    // Old auth-1 was at frontier 500.
    frontiers.update(make_frontier("auth-1", 500, 0, "data/"));
    frontiers.update(make_frontier("auth-2", 600, 0, "data/"));
    frontiers.update(make_frontier("auth-3", 550, 0, "data/"));

    // Majority frontier should be 550 (sorted: [500, 550, 600], idx=1).
    assert_eq!(frontiers.majority_frontier(3).unwrap().physical, 550);

    // auth-1 is replaced by auth-1-new, starting at frontier 500 (handoff).
    // Old auth-1 stops updating.
    // In practice, the new authority gets its own NodeId.
    frontiers.update(make_frontier("auth-1-new", 500, 0, "data/"));

    // Now we have 4 frontier entries, but total authorities is still 3.
    // The system must track that the authority set changed.
    // With total_authorities=3, we need majority=2.
    // If the new set is {auth-1-new, auth-2, auth-3}:
    // To represent this correctly, we'd need to remove auth-1.
    // Since AckFrontierSet doesn't have remove, we check that with 4 entries
    // and total=4 (during transition): majority=3.
    // Sorted: [500, 500, 550, 600], idx=4-3=1 → 500
    let mf = frontiers.majority_frontier(4);
    assert!(mf.is_some());
    assert_eq!(mf.unwrap().physical, 500);

    // New authority catches up.
    frontiers.update(make_frontier("auth-1-new", 700, 0, "data/"));
    // Sorted: [500, 550, 600, 700], idx=4-3=1 → 550
    assert_eq!(frontiers.majority_frontier(4).unwrap().physical, 550);
}

#[test]
fn authority_partition_different_certified_values() {
    // During partition, authorities in different partitions may advance their
    // frontiers at different rates, producing "split" certification states.
    let mut cert_ab = CertifiedApi::new(node("node-a"), default_namespace());
    let mut cert_c = CertifiedApi::new(node("node-c"), default_namespace());

    // Partition {A,B} has auth-1 and auth-2.
    // Partition {C} has auth-3.

    // Node A writes (partition 1).
    let counter_a = CrdtValue::Counter(PnCounter::new());
    cert_ab
        .certified_write("shared".into(), counter_a, OnTimeout::Pending)
        .unwrap();
    let write_ts_ab = cert_ab.pending_writes()[0].timestamp.physical;

    // Node C writes (partition 2).
    let counter_c = CrdtValue::Counter(PnCounter::new());
    cert_c
        .certified_write("shared".into(), counter_c, OnTimeout::Pending)
        .unwrap();
    let write_ts_c = cert_c.pending_writes()[0].timestamp.physical;

    // Auth-1 and Auth-2 advance past A's write (majority in partition 1).
    cert_ab.update_frontier(make_frontier("auth-1", write_ts_ab + 100, 0, ""));
    cert_ab.update_frontier(make_frontier("auth-2", write_ts_ab + 100, 0, ""));
    cert_ab.process_certifications();

    // A's write is certified in partition 1.
    assert_eq!(
        cert_ab.get_certification_status("shared"),
        CertificationStatus::Certified
    );

    // Auth-3 alone is not enough for C's write (1 of 3, no majority).
    cert_c.update_frontier(make_frontier("auth-3", write_ts_c + 100, 0, ""));
    cert_c.process_certifications();

    // C's write remains Pending.
    assert_eq!(
        cert_c.get_certification_status("shared"),
        CertificationStatus::Pending
    );

    // --- Partition heals: C receives auth-1 and auth-2 frontiers ---
    cert_c.update_frontier(make_frontier("auth-1", write_ts_c + 200, 0, ""));
    cert_c.update_frontier(make_frontier("auth-2", write_ts_c + 200, 0, ""));
    cert_c.process_certifications();

    // Now C's write should be certified too.
    assert_eq!(
        cert_c.get_certification_status("shared"),
        CertificationStatus::Certified
    );
}

// ===========================================================================
// 4. Eventual consistency guarantees
// ===========================================================================

#[test]
fn eventual_read_returns_correct_value_after_full_flow() {
    // Full flow: partition → writes → heal → eventual read returns converged value.
    let mut node_a = EventualApi::new(node("node-a"));
    let mut node_b = EventualApi::new(node("node-b"));
    let mut node_c = EventualApi::new(node("node-c"));

    // Pre-partition: all nodes start with counter = 1.
    node_a.eventual_counter_inc("score").unwrap();
    sync_key(&mut node_a, &mut node_b, "score");
    sync_key(&mut node_a, &mut node_c, "score");

    // --- Partition {A,B} vs {C} ---
    node_a.eventual_counter_inc("score").unwrap(); // A += 1
    node_a.eventual_counter_dec("score").unwrap(); // A -= 1
    node_b.eventual_counter_inc("score").unwrap(); // B += 1
    sync_key(&mut node_a, &mut node_b, "score");

    node_c.eventual_counter_inc("score").unwrap(); // C += 1
    node_c.eventual_counter_inc("score").unwrap(); // C += 1

    // --- Heal ---
    sync_key(&mut node_a, &mut node_c, "score");
    sync_key(&mut node_b, &mut node_c, "score");
    sync_key(&mut node_a, &mut node_b, "score");

    // Expected: initial(1) + A(+1,-1) + B(+1) + C(+2) = 4
    let expected = 4;
    assert_eq!(counter_val(&node_a, "score"), expected);
    assert_eq!(counter_val(&node_b, "score"), expected);
    assert_eq!(counter_val(&node_c, "score"), expected);
}

#[test]
fn availability_during_partition_eventual_ops_succeed() {
    // During a partition, eventual write and read operations must
    // continue to succeed on all nodes (AP guarantee).
    let mut node_a = EventualApi::new(node("node-a"));
    let mut node_c = EventualApi::new(node("node-c"));

    // Node A: all CRDT operations succeed during partition.
    node_a.eventual_counter_inc("c").unwrap();
    node_a.eventual_set_add("s", "elem".into()).unwrap();
    node_a.eventual_register_set("r", "value".into()).unwrap();
    node_a
        .eventual_map_set("m", "k1".into(), "v1".into())
        .unwrap();

    assert_eq!(counter_val(&node_a, "c"), 1);
    assert!(set_contains(&node_a, "s", "elem"));
    assert_eq!(register_val(&node_a, "r"), Some("value".into()));
    assert!(node_a.get_eventual("m").is_some());

    // Node C: independent writes also succeed.
    node_c.eventual_counter_inc("c").unwrap();
    node_c.eventual_set_add("s", "other".into()).unwrap();
    node_c.eventual_register_set("r", "c-value".into()).unwrap();

    assert_eq!(counter_val(&node_c, "c"), 1);
    assert!(set_contains(&node_c, "s", "other"));
    assert_eq!(register_val(&node_c, "r"), Some("c-value".into()));
}

// ===========================================================================
// 5. Additional edge cases
// ===========================================================================

#[test]
fn three_way_partition_independent_writes_converge() {
    // Extreme case: all three nodes are isolated (3 partitions).
    let mut node_a = EventualApi::new(node("node-a"));
    let mut node_b = EventualApi::new(node("node-b"));
    let mut node_c = EventualApi::new(node("node-c"));

    // Each node writes independently.
    node_a.eventual_counter_inc("x").unwrap();
    node_a.eventual_counter_inc("x").unwrap();

    node_b.eventual_counter_inc("x").unwrap();

    node_c.eventual_counter_inc("x").unwrap();
    node_c.eventual_counter_inc("x").unwrap();
    node_c.eventual_counter_inc("x").unwrap();

    // Heal: merge in arbitrary order.
    sync_key(&mut node_b, &mut node_c, "x");
    sync_key(&mut node_a, &mut node_c, "x");
    sync_key(&mut node_a, &mut node_b, "x");

    // Expected: 2 + 1 + 3 = 6
    assert_eq!(counter_val(&node_a, "x"), 6);
    assert_eq!(counter_val(&node_b, "x"), 6);
    assert_eq!(counter_val(&node_c, "x"), 6);
}

#[test]
fn partition_or_set_add_on_both_sides_union_after_merge() {
    // Both partitions add different elements to the same OR-Set.
    let mut node_a = EventualApi::new(node("node-a"));
    let mut node_c = EventualApi::new(node("node-c"));

    // Partition 1 adds.
    node_a.eventual_set_add("tags", "alpha".into()).unwrap();
    node_a.eventual_set_add("tags", "beta".into()).unwrap();

    // Partition 2 adds.
    node_c.eventual_set_add("tags", "gamma".into()).unwrap();
    node_c.eventual_set_add("tags", "delta".into()).unwrap();

    // Heal.
    sync_key(&mut node_a, &mut node_c, "tags");

    // Both should contain all four elements.
    for elem in &["alpha", "beta", "gamma", "delta"] {
        assert!(
            set_contains(&node_a, "tags", elem),
            "A should contain '{elem}'"
        );
        assert!(
            set_contains(&node_c, "tags", elem),
            "C should contain '{elem}'"
        );
    }
}

#[test]
fn partition_compaction_eligibility_requires_majority_post_heal() {
    // Compaction should only be possible once majority of authorities have
    // consumed past the checkpoint — which requires partition heal.
    let mut engine = CompactionEngine::with_defaults();
    let kr = KeyRange {
        prefix: "data/".into(),
    };

    // Create a checkpoint at t=1000.
    engine.create_checkpoint(
        kr.clone(),
        make_ts(1000, 0, "node-a"),
        "digest".into(),
        PolicyVersion(1),
    );

    // During partition: only auth-3 has consumed past the checkpoint.
    let mut frontiers = AckFrontierSet::new();
    frontiers.update(make_frontier("auth-3", 2000, 0, "data/"));

    // Not compactable (1/3 authorities).
    assert!(!engine.is_compactable("data/", &frontiers, 3));

    // Partition heals: auth-1 and auth-2 catch up.
    frontiers.update(make_frontier("auth-1", 1500, 0, "data/"));
    frontiers.update(make_frontier("auth-2", 1200, 0, "data/"));

    // Now majority (3/3) have consumed past t=1000.
    // Majority frontier: sorted [1200, 1500, 2000], idx=1 → 1500 >= 1000 ✓
    assert!(engine.is_compactable("data/", &frontiers, 3));
}

#[test]
fn idempotent_merge_during_heal_does_not_corrupt() {
    // Merging the same state multiple times should not change the result.
    let mut node_a = EventualApi::new(node("node-a"));
    let mut node_c = EventualApi::new(node("node-c"));

    node_a.eventual_counter_inc("idem").unwrap();
    node_a.eventual_counter_inc("idem").unwrap();
    node_c.eventual_counter_inc("idem").unwrap();

    // Merge multiple times.
    sync_key(&mut node_a, &mut node_c, "idem");
    sync_key(&mut node_a, &mut node_c, "idem");
    sync_key(&mut node_a, &mut node_c, "idem");

    assert_eq!(counter_val(&node_a, "idem"), 3);
    assert_eq!(counter_val(&node_c, "idem"), 3);
}

#[test]
fn partition_map_concurrent_set_and_delete_add_wins() {
    // OR-Map: one partition sets a key, the other deletes it.
    // Add-wins semantics should preserve the set if the add creates a new dot.
    let mut node_a = EventualApi::new(node("node-a"));
    let mut node_c = EventualApi::new(node("node-c"));

    // Common state.
    node_a
        .eventual_map_set("conf", "mode".into(), "fast".into())
        .unwrap();
    sync_key(&mut node_a, &mut node_c, "conf");

    // --- Partition ---
    // A re-sets the key (new dot + timestamp).
    node_a
        .eventual_map_set("conf", "mode".into(), "turbo".into())
        .unwrap();

    // C deletes the key.
    node_c.eventual_map_delete("conf", "mode").unwrap();

    // --- Heal ---
    sync_key(&mut node_a, &mut node_c, "conf");

    // A's set should survive (add-wins).
    match node_a.get_eventual("conf") {
        Some(CrdtValue::Map(m)) => {
            assert!(
                m.contains_key(&"mode".to_string()),
                "add-wins: map key should survive concurrent set+delete"
            );
        }
        other => panic!("expected Map, got {:?}", other),
    }
}

#[test]
fn certified_write_succeeds_with_full_authority_set() {
    // Baseline: when all 3 authorities report frontiers past the write,
    // writes are certified.
    let mut cert = CertifiedApi::new(node("node-a"), default_namespace());

    // First write (will be Pending since no authorities have reported).
    let counter = CrdtValue::Counter(PnCounter::new());
    let result = cert.certified_write("k".into(), counter, OnTimeout::Pending);
    assert_eq!(result.unwrap(), CertificationStatus::Pending);

    // Get the write timestamp so we can advance authorities past it.
    let write_ts = cert.pending_writes()[0].timestamp.physical;

    // Advance all 3 authorities past the write timestamp.
    cert.update_frontier(make_frontier("auth-1", write_ts + 1000, 0, ""));
    cert.update_frontier(make_frontier("auth-2", write_ts + 1000, 0, ""));
    cert.update_frontier(make_frontier("auth-3", write_ts + 1000, 0, ""));

    cert.process_certifications();

    assert_eq!(
        cert.get_certification_status("k"),
        CertificationStatus::Certified
    );
}

#[test]
fn partition_new_key_created_during_partition_propagates_on_heal() {
    // A key that only exists on one side of the partition should
    // propagate to the other side after heal.
    let mut node_a = EventualApi::new(node("node-a"));
    let mut node_c = EventualApi::new(node("node-c"));

    // During partition: A creates a new key.
    node_a.eventual_counter_inc("new-key").unwrap();

    // C doesn't know about it.
    assert!(node_c.get_eventual("new-key").is_none());

    // Heal.
    sync_key(&mut node_a, &mut node_c, "new-key");

    // C should now have the key.
    assert_eq!(counter_val(&node_c, "new-key"), 1);
}

#[test]
fn partition_asymmetric_load_convergence() {
    // One partition has much more load than the other.
    let mut node_a = EventualApi::new(node("node-a"));
    let mut node_c = EventualApi::new(node("node-c"));

    // Heavy writes on partition 1.
    for _ in 0..100 {
        node_a.eventual_counter_inc("load").unwrap();
    }

    // Light writes on partition 2.
    for _ in 0..5 {
        node_c.eventual_counter_inc("load").unwrap();
    }

    // Heal.
    sync_key(&mut node_a, &mut node_c, "load");

    assert_eq!(counter_val(&node_a, "load"), 105);
    assert_eq!(counter_val(&node_c, "load"), 105);
}

#[test]
fn partition_certified_api_multiple_pending_writes_resolve_after_heal() {
    // Multiple pending writes should all be certified once authorities
    // catch up after partition heal.
    let mut cert = CertifiedApi::new(node("node-a"), default_namespace());

    // Write 3 values while no authorities are available.
    for i in 0..3 {
        let counter = CrdtValue::Counter(PnCounter::new());
        cert.certified_write(format!("key-{i}"), counter, OnTimeout::Pending)
            .unwrap();
    }

    assert_eq!(cert.pending_writes().len(), 3);
    for pw in cert.pending_writes() {
        assert_eq!(pw.status, CertificationStatus::Pending);
    }

    // Find the highest timestamp among pending writes.
    let max_ts = cert
        .pending_writes()
        .iter()
        .map(|pw| pw.timestamp.physical)
        .max()
        .unwrap();

    // Authorities recover and report frontiers past all write timestamps.
    cert.update_frontier(make_frontier("auth-1", max_ts + 1000, 0, ""));
    cert.update_frontier(make_frontier("auth-2", max_ts + 1000, 0, ""));
    cert.process_certifications();

    // All writes should now be certified.
    for i in 0..3 {
        assert_eq!(
            cert.get_certification_status(&format!("key-{i}")),
            CertificationStatus::Certified,
            "key-{i} should be certified after authority recovery"
        );
    }
}

#[test]
fn partition_heal_order_independence() {
    // The order in which partitions are healed should not affect the
    // final converged state (merge commutativity + associativity).
    let mut a1 = EventualApi::new(node("node-a"));
    let mut b1 = EventualApi::new(node("node-b"));
    let mut c1 = EventualApi::new(node("node-c"));

    let mut a2 = EventualApi::new(node("node-a"));
    let mut b2 = EventualApi::new(node("node-b"));
    let mut c2 = EventualApi::new(node("node-c"));

    // Same writes on both sets.
    for api in [&mut a1, &mut a2] {
        api.eventual_counter_inc("k").unwrap();
        api.eventual_counter_inc("k").unwrap();
    }
    for api in [&mut b1, &mut b2] {
        api.eventual_counter_inc("k").unwrap();
    }
    for api in [&mut c1, &mut c2] {
        api.eventual_counter_inc("k").unwrap();
        api.eventual_counter_inc("k").unwrap();
        api.eventual_counter_inc("k").unwrap();
    }

    // Heal order 1: A↔B, then A↔C, then B↔C
    sync_key(&mut a1, &mut b1, "k");
    sync_key(&mut a1, &mut c1, "k");
    sync_key(&mut b1, &mut c1, "k");

    // Heal order 2: C↔B, then A↔C, then A↔B
    sync_key(&mut c2, &mut b2, "k");
    sync_key(&mut a2, &mut c2, "k");
    sync_key(&mut a2, &mut b2, "k");

    // All should converge to the same value regardless of merge order.
    let expected = 6; // 2 + 1 + 3
    assert_eq!(counter_val(&a1, "k"), expected);
    assert_eq!(counter_val(&b1, "k"), expected);
    assert_eq!(counter_val(&c1, "k"), expected);
    assert_eq!(counter_val(&a2, "k"), expected);
    assert_eq!(counter_val(&b2, "k"), expected);
    assert_eq!(counter_val(&c2, "k"), expected);
}
