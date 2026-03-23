//! Concurrent merge correctness and Authority consensus tests (#302).
//!
//! Verifies that:
//! - CRDT merge operations produce correct results regardless of merge order
//!   (commutativity, associativity, idempotency).
//! - Authority certification and frontier tracking maintain invariants under
//!   concurrent access with a multi-threaded runtime.
//!
//! The CRDT merge tests (Test A, H) verify merge-order independence: since
//! CRDTs are designed to be commutative, testing different merge orderings
//! (even serialised behind a lock) validates correctness. The Authority and
//! store tests (B-G) use `flavor = "multi_thread"` to exercise real task
//! interleaving.
//!
//! Evaluation of model-checking tools:
//! - **shuttle-rs** (awslabs/shuttle): Provides deterministic concurrency testing
//!   via controlled scheduling. Requires replacing all std::sync / tokio primitives
//!   with shuttle's drop-in wrappers throughout the code under test. Not feasible
//!   for incremental adoption in an existing codebase without significant refactoring.
//! - **loom**: Exhaustive exploration but intractable for non-trivial programs.
//! - **turmoil** (tokio-rs): Network simulation for distributed systems, complementary
//!   but targets a different layer (network faults rather than CPU-level races).
//!
//! Decision: Use multi-threaded tokio::test with barriers for practical concurrent
//! coverage. Consider shuttle for new, isolated modules where the wrapper cost is
//! acceptable.

use std::sync::{Arc, RwLock};

use asteroidb_poc::api::certified::{CertifiedApi, OnTimeout};
use asteroidb_poc::api::eventual::EventualApi;
use asteroidb_poc::authority::ack_frontier::{AckFrontier, AckFrontierSet};
use asteroidb_poc::authority::certificate::{
    AuthoritySignature, KeysetVersion, MajorityCertificate, create_certificate_message,
    sign_message,
};
use asteroidb_poc::compaction::{CompactionConfig, CompactionEngine};
use asteroidb_poc::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
use asteroidb_poc::crdt::lww_register::LwwRegister;
use asteroidb_poc::crdt::or_map::OrMap;
use asteroidb_poc::crdt::or_set::OrSet;
use asteroidb_poc::crdt::pn_counter::PnCounter;
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::placement::PlacementPolicy;
use asteroidb_poc::store::kv::{CrdtValue, Store};
use asteroidb_poc::types::{CertificationStatus, KeyRange, NodeId, PolicyVersion};

use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use tokio::sync::{Barrier, Mutex};

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

fn ts(physical: u64, logical: u32, node: &str) -> HlcTimestamp {
    HlcTimestamp {
        physical,
        logical,
        node_id: node.into(),
    }
}

fn make_namespace_with_authorities(
    prefix: &str,
    authority_ids: &[&str],
) -> Arc<RwLock<SystemNamespace>> {
    let mut ns = SystemNamespace::new();
    let auth_def = AuthorityDefinition {
        key_range: kr(prefix),
        authority_nodes: authority_ids.iter().map(|id| node_id(id)).collect(),
        auto_generated: false,
    };
    ns.set_authority_definition(auth_def);

    let policy = PlacementPolicy::new(PolicyVersion(1), kr(prefix), authority_ids.len());
    ns.set_placement_policy(policy);

    Arc::new(RwLock::new(ns))
}

// =========================================================================
// Test A: CRDT merge correctness — order-independent convergence
// =========================================================================

/// PnCounter merges from N peers must converge to the same final value
/// regardless of merge order. All replicas start with independent
/// increments and are merged in task-scheduling order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pn_counter_merge_order_converges() {
    const NUM_PEERS: usize = 10;
    const INCREMENTS_PER_PEER: usize = 100;

    // Each peer builds its own counter independently.
    let mut peer_counters: Vec<PnCounter> = Vec::with_capacity(NUM_PEERS);
    for i in 0..NUM_PEERS {
        let nid = node_id(&format!("peer-{i}"));
        let mut counter = PnCounter::new();
        for _ in 0..INCREMENTS_PER_PEER {
            counter.increment(&nid);
        }
        peer_counters.push(counter);
    }

    let expected_value = (NUM_PEERS * INCREMENTS_PER_PEER) as i64;

    // Merge all peer counters into a shared counter concurrently.
    let shared_counter = Arc::new(Mutex::new(PnCounter::new()));
    let barrier = Arc::new(Barrier::new(NUM_PEERS));

    let mut handles = Vec::new();
    for counter in peer_counters {
        let shared = shared_counter.clone();
        let bar = barrier.clone();
        handles.push(tokio::spawn(async move {
            bar.wait().await; // synchronize start
            let mut locked = shared.lock().await;
            locked.merge(&counter);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let final_counter = shared_counter.lock().await;
    assert_eq!(
        final_counter.value(),
        expected_value,
        "concurrent PnCounter merge must converge to sum of all peer increments"
    );
}

/// OrSet merges with add-wins semantics: one peer adds, another removes
/// concurrently. After all merges, the add-wins property must hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn or_set_merge_add_wins_correctness() {
    const NUM_ADDERS: usize = 8;
    const NUM_REMOVERS: usize = 4;
    let total_tasks = NUM_ADDERS + NUM_REMOVERS;

    // Common base state: "x" is in the set.
    let mut common = OrSet::new();
    common.add("x".to_string(), &node_id("origin"));

    // Adders: each adds "x" with a new dot (concurrent add).
    let mut adder_sets: Vec<OrSet<String>> = Vec::new();
    for i in 0..NUM_ADDERS {
        let mut replica = common.clone();
        replica.add("x".to_string(), &node_id(&format!("adder-{i}")));
        adder_sets.push(replica);
    }

    // Removers: each removes "x" (only observes the original dot).
    let mut remover_sets: Vec<OrSet<String>> = Vec::new();
    for _ in 0..NUM_REMOVERS {
        let mut replica = common.clone();
        replica.remove(&"x".to_string());
        remover_sets.push(replica);
    }

    // Merge all concurrently into a shared set.
    let shared_set = Arc::new(Mutex::new(common.clone()));
    let barrier = Arc::new(Barrier::new(total_tasks));

    let mut handles = Vec::new();
    for set in adder_sets.into_iter().chain(remover_sets.into_iter()) {
        let shared = shared_set.clone();
        let bar = barrier.clone();
        handles.push(tokio::spawn(async move {
            bar.wait().await;
            let mut locked = shared.lock().await;
            locked.merge(&set);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let final_set = shared_set.lock().await;
    assert!(
        final_set.contains(&"x".to_string()),
        "add-wins: concurrent adds must survive concurrent removes"
    );
}

/// LwwRegister merges: highest timestamp must win regardless of merge order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lww_register_merge_highest_ts_wins() {
    const NUM_PEERS: usize = 20;

    let mut registers: Vec<LwwRegister<String>> = Vec::with_capacity(NUM_PEERS);
    for i in 0..NUM_PEERS {
        let mut reg = LwwRegister::new();
        // Each peer writes with a different physical timestamp.
        reg.set(
            format!("value-{i}"),
            ts(100 + i as u64, 0, &format!("peer-{i}")),
        );
        registers.push(reg);
    }

    let shared_reg = Arc::new(Mutex::new(LwwRegister::<String>::new()));
    let barrier = Arc::new(Barrier::new(NUM_PEERS));

    let mut handles = Vec::new();
    for reg in registers {
        let shared = shared_reg.clone();
        let bar = barrier.clone();
        handles.push(tokio::spawn(async move {
            bar.wait().await;
            let mut locked = shared.lock().await;
            locked.merge(&reg);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let final_reg = shared_reg.lock().await;
    // The highest timestamp is (119, 0, "peer-19") -> value is "value-19".
    assert_eq!(
        final_reg.get(),
        Some(&format!("value-{}", NUM_PEERS - 1)),
        "LwwRegister must converge to the value with the highest timestamp"
    );
}

/// OrMap merges: concurrent set and delete on the same key must resolve
/// with add-wins semantics and LWW for values.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn or_map_merge_add_wins_correctness() {
    const NUM_SETTERS: usize = 6;
    const NUM_DELETERS: usize = 3;
    let total_tasks = NUM_SETTERS + NUM_DELETERS;

    // Common base: key "k" exists.
    let mut common: OrMap<String, String> = OrMap::new();
    common.set(
        "k".to_string(),
        "original".to_string(),
        ts(100, 0, "origin"),
        &node_id("origin"),
    );

    // Setters: each sets "k" with a higher timestamp (concurrent set).
    let mut setter_maps = Vec::new();
    for i in 0..NUM_SETTERS {
        let mut replica = common.clone();
        replica.set(
            "k".to_string(),
            format!("setter-{i}"),
            ts(200 + i as u64, 0, &format!("setter-{i}")),
            &node_id(&format!("setter-{i}")),
        );
        setter_maps.push(replica);
    }

    // Deleters: each deletes "k" (observes only the original dot).
    let mut deleter_maps = Vec::new();
    for _ in 0..NUM_DELETERS {
        let mut replica = common.clone();
        replica.delete(&"k".to_string());
        deleter_maps.push(replica);
    }

    let shared_map = Arc::new(Mutex::new(common.clone()));
    let barrier = Arc::new(Barrier::new(total_tasks));

    let mut handles = Vec::new();
    for map in setter_maps.into_iter().chain(deleter_maps.into_iter()) {
        let shared = shared_map.clone();
        let bar = barrier.clone();
        handles.push(tokio::spawn(async move {
            bar.wait().await;
            let mut locked = shared.lock().await;
            locked.merge(&map);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let final_map = shared_map.lock().await;
    assert!(
        final_map.contains_key(&"k".to_string()),
        "add-wins: concurrent sets must survive concurrent deletes"
    );
    // LWW for value: highest timestamp setter wins.
    let expected_value = format!("setter-{}", NUM_SETTERS - 1);
    assert_eq!(
        final_map.get(&"k".to_string()),
        Some(&expected_value),
        "LWW value must be from the setter with the highest timestamp"
    );
}

// =========================================================================
// Test B: Concurrent authority certification requests
// =========================================================================

/// Multiple tasks concurrently submit certified writes and advance frontiers.
/// No write should be lost or misclassified. Verifies both certification
/// status and that values are actually persisted in the store.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_certified_write_and_frontier_advance() {
    let authority_ids = ["auth-1", "auth-2", "auth-3"];
    let ns = make_namespace_with_authorities("data/", &authority_ids);

    let certified_api = Arc::new(Mutex::new(CertifiedApi::new(node_id("node-1"), ns)));

    const NUM_WRITERS: usize = 10;
    let barrier = Arc::new(Barrier::new(NUM_WRITERS));

    // Phase 1: Concurrent certified writes.
    let mut handles = Vec::new();
    for i in 0..NUM_WRITERS {
        let api = certified_api.clone();
        let bar = barrier.clone();
        handles.push(tokio::spawn(async move {
            bar.wait().await;
            let mut locked = api.lock().await;
            let key = format!("data/key-{i}");
            let value = CrdtValue::Counter(PnCounter::from_value(&node_id("writer"), i as i64));
            let _ = locked.certified_write(key, value, OnTimeout::Pending);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Phase 2: Advance frontiers concurrently from all authorities.
    let frontier_barrier = Arc::new(Barrier::new(authority_ids.len()));
    let mut frontier_handles = Vec::new();
    for (idx, auth_id) in authority_ids.iter().enumerate() {
        let api = certified_api.clone();
        let bar = frontier_barrier.clone();
        let auth = auth_id.to_string();
        frontier_handles.push(tokio::spawn(async move {
            bar.wait().await;
            let mut locked = api.lock().await;
            let frontier = AckFrontier {
                authority_id: node_id(&auth),
                frontier_hlc: ts(1000 + idx as u64, 0, &auth),
                key_range: kr("data/"),
                policy_version: PolicyVersion(1),
                digest_hash: String::new(),
            };
            locked.update_frontier(frontier);
        }));
    }

    for h in frontier_handles {
        h.await.unwrap();
    }

    // Phase 3: Process certifications and verify no corruption.
    {
        let mut locked = certified_api.lock().await;
        locked.process_certifications();
    }

    // Verify all writes are tracked and values exist in the store.
    let locked = certified_api.lock().await;
    for i in 0..NUM_WRITERS {
        let key = format!("data/key-{i}");

        // Check certification status.
        let status = locked.get_certification_status(&key);
        assert!(
            status == CertificationStatus::Pending || status == CertificationStatus::Certified,
            "write for {key} must be Pending or Certified, got {status:?}"
        );

        // Verify the value actually exists in the store (not just status).
        let read = locked.get_certified(&key);
        assert!(
            read.value.is_some(),
            "key '{key}' must exist in store after certified_write"
        );
        // Verify the stored value matches what was written.
        match read.value {
            Some(CrdtValue::Counter(c)) => {
                assert_eq!(
                    c.value(),
                    i as i64,
                    "stored counter for {key} must have value {i}"
                );
            }
            other => panic!("expected Counter for {key}, got {other:?}"),
        }
    }
}

/// Concurrent signature collection on a MajorityCertificate must not
/// produce duplicate signers or corrupt the certificate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_certificate_signature_collection() {
    const NUM_AUTHORITIES: usize = 7;

    let key_range = kr("test/");
    let frontier_hlc = ts(500, 0, "leader");
    let policy_version = PolicyVersion(1);
    let keyset_version = KeysetVersion(1);

    let cert = Arc::new(Mutex::new(MajorityCertificate::new(
        key_range.clone(),
        frontier_hlc.clone(),
        policy_version,
        keyset_version.clone(),
    )));

    let message = create_certificate_message(&key_range, &frontier_hlc, &policy_version);
    let barrier = Arc::new(Barrier::new(NUM_AUTHORITIES));

    let mut handles = Vec::new();
    for i in 0..NUM_AUTHORITIES {
        let cert_clone = cert.clone();
        let bar = barrier.clone();
        let msg = message.clone();
        let ks_ver = keyset_version.clone();

        handles.push(tokio::spawn(async move {
            let signing_key = SigningKey::generate(&mut OsRng);
            let verifying_key = signing_key.verifying_key();
            let signature = sign_message(&signing_key, &msg);

            bar.wait().await;
            let mut locked = cert_clone.lock().await;
            locked.add_signature(AuthoritySignature {
                authority_id: node_id(&format!("auth-{i}")),
                public_key: verifying_key,
                signature,
                keyset_version: ks_ver,
            });
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let final_cert = cert.lock().await;
    assert_eq!(
        final_cert.signature_count(),
        NUM_AUTHORITIES,
        "all {} authority signatures must be collected",
        NUM_AUTHORITIES
    );
    assert!(
        final_cert.has_majority(NUM_AUTHORITIES),
        "certificate must have majority with all signatures present"
    );

    // Verify all signatures are valid.
    let valid = final_cert.verify_signatures(&message).unwrap();
    assert_eq!(
        valid.len(),
        NUM_AUTHORITIES,
        "all signatures must verify correctly"
    );
}

// =========================================================================
// Test C: Concurrent membership changes during sync (frontier updates)
// =========================================================================

/// Concurrent frontier updates from multiple authorities must not lose
/// updates or regress frontiers. Checks both per-authority and majority
/// frontier invariants.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_frontier_updates_no_regression() {
    const NUM_AUTHORITIES: usize = 5;
    const UPDATES_PER_AUTHORITY: usize = 50;

    let frontier_set = Arc::new(Mutex::new(AckFrontierSet::new()));
    let total_tasks = NUM_AUTHORITIES * UPDATES_PER_AUTHORITY;
    let barrier = Arc::new(Barrier::new(total_tasks));

    let mut handles = Vec::new();
    for auth_idx in 0..NUM_AUTHORITIES {
        for update_idx in 0..UPDATES_PER_AUTHORITY {
            let fs = frontier_set.clone();
            let bar = barrier.clone();
            handles.push(tokio::spawn(async move {
                bar.wait().await;
                let mut locked = fs.lock().await;
                let frontier = AckFrontier {
                    authority_id: node_id(&format!("auth-{auth_idx}")),
                    frontier_hlc: ts(1000 + update_idx as u64, 0, &format!("auth-{auth_idx}")),
                    key_range: kr("data/"),
                    policy_version: PolicyVersion(1),
                    digest_hash: String::new(),
                };
                locked.update(frontier);
            }));
        }
    }

    for h in handles {
        h.await.unwrap();
    }

    // After all updates, each authority's frontier should be at the highest
    // timestamp it ever reported (monotonic advancement).
    let locked = frontier_set.lock().await;
    let max_physical = 1000 + (UPDATES_PER_AUTHORITY - 1) as u64;

    // Check per-authority frontiers: each authority must be at its max.
    for auth_idx in 0..NUM_AUTHORITIES {
        let auth_id = node_id(&format!("auth-{auth_idx}"));
        let frontier = locked.get_for_scope(&kr("data/"), &PolicyVersion(1), &auth_id);
        assert!(
            frontier.is_some(),
            "authority auth-{auth_idx} must have a frontier entry"
        );
        let f = frontier.unwrap();
        assert_eq!(
            f.frontier_hlc.physical, max_physical,
            "auth-{auth_idx} frontier must be at max timestamp {max_physical}, got {}",
            f.frontier_hlc.physical
        );
    }

    // Also check majority frontier: with all authorities at the same max,
    // majority_frontier should exist and be at or above max.
    let majority =
        locked.majority_frontier_for_scope(&kr("data/"), &PolicyVersion(1), NUM_AUTHORITIES);
    assert!(
        majority.is_some(),
        "majority frontier must exist after all authorities reported"
    );

    let mf = majority.unwrap();
    assert!(
        mf.physical >= max_physical,
        "majority frontier must be at or above the maximum reported timestamp"
    );
}

/// Concurrent frontier updates across different key ranges must be isolated.
/// Updates for "users/" must not affect "orders/" and vice versa.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_frontier_updates_across_key_ranges() {
    let frontier_set = Arc::new(Mutex::new(AckFrontierSet::new()));
    let prefixes = ["users/", "orders/", "events/"];
    let total_tasks = prefixes.len() * 3; // 3 authorities per prefix
    let barrier = Arc::new(Barrier::new(total_tasks));

    let mut handles = Vec::new();
    for prefix in &prefixes {
        for auth_idx in 0..3 {
            let fs = frontier_set.clone();
            let bar = barrier.clone();
            let p = prefix.to_string();
            handles.push(tokio::spawn(async move {
                bar.wait().await;
                let mut locked = fs.lock().await;
                let frontier = AckFrontier {
                    authority_id: node_id(&format!("{p}auth-{auth_idx}")),
                    frontier_hlc: ts(500, 0, &format!("{p}auth-{auth_idx}")),
                    key_range: kr(&p),
                    policy_version: PolicyVersion(1),
                    digest_hash: String::new(),
                };
                locked.update(frontier);
            }));
        }
    }

    for h in handles {
        h.await.unwrap();
    }

    // Verify each prefix has its own frontier entries.
    let locked = frontier_set.lock().await;
    for prefix in &prefixes {
        let majority = locked.majority_frontier_for_scope(&kr(prefix), &PolicyVersion(1), 3);
        assert!(
            majority.is_some(),
            "prefix '{prefix}' must have a majority frontier"
        );
    }
}

// =========================================================================
// Test D: Concurrent compaction during merge operations
// =========================================================================

/// Concurrent store merges and compaction eligibility checks must not corrupt
/// the store or produce inconsistent state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_store_merge_and_compaction_check() {
    const NUM_MERGE_TASKS: usize = 10;
    const NUM_COMPACTION_TASKS: usize = 5;
    let total_tasks = NUM_MERGE_TASKS + NUM_COMPACTION_TASKS;

    let store = Arc::new(Mutex::new(Store::new()));
    let compaction = Arc::new(Mutex::new(CompactionEngine::new(
        CompactionConfig::default(),
    )));
    let frontier_set = Arc::new(Mutex::new(AckFrontierSet::new()));

    // Pre-populate the store with a counter.
    {
        let mut s = store.lock().await;
        s.put(
            "data/counter".to_string(),
            CrdtValue::Counter(PnCounter::new()),
        );
    }

    let barrier = Arc::new(Barrier::new(total_tasks));

    let mut handles = Vec::new();

    // Merge tasks: each merges a counter increment.
    for i in 0..NUM_MERGE_TASKS {
        let s = store.clone();
        let ce = compaction.clone();
        let bar = barrier.clone();
        handles.push(tokio::spawn(async move {
            let nid = node_id(&format!("merger-{i}"));
            let mut counter = PnCounter::new();
            counter.increment(&nid);
            let merge_value = CrdtValue::Counter(counter);

            bar.wait().await;

            // Merge into store.
            {
                let mut locked_store = s.lock().await;
                let _ = locked_store.merge_value("data/counter".to_string(), &merge_value);
            }

            // Record operation for compaction tracking.
            {
                let mut locked_ce = ce.lock().await;
                locked_ce.record_op(&kr("data/"));
            }
        }));
    }

    // Compaction check tasks: concurrently check eligibility.
    for _ in 0..NUM_COMPACTION_TASKS {
        let s = store.clone();
        let ce = compaction.clone();
        let fs = frontier_set.clone();
        let bar = barrier.clone();
        handles.push(tokio::spawn(async move {
            bar.wait().await;

            let locked_fs = fs.lock().await;
            let locked_ce = ce.lock().await;
            // Check compactability (will be false since no checkpoint exists).
            let _compactable = locked_ce.is_compactable("data/", &locked_fs, 3);

            // Also check if we should create a checkpoint.
            let _should_cp = locked_ce.should_checkpoint(&kr("data/"), &ts(2000, 0, "check"));

            // Also verify store is readable during compaction check.
            let locked_store = s.lock().await;
            let value = locked_store.get("data/counter");
            assert!(
                value.is_some(),
                "store key must remain readable during concurrent compaction checks"
            );
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Verify final store state.
    let locked = store.lock().await;
    if let Some(CrdtValue::Counter(c)) = locked.get("data/counter") {
        assert_eq!(
            c.value(),
            NUM_MERGE_TASKS as i64,
            "counter must reflect all merged increments"
        );
    } else {
        panic!("data/counter must be a Counter");
    }
}

// =========================================================================
// Test E: Concurrent delta sync pull/push on same peer
// =========================================================================

/// Simulates concurrent delta sync operations: multiple tasks push deltas
/// to a shared store while others read the store state. Verifies that
/// all pushed deltas are eventually reflected and no data is lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_delta_sync_push_pull() {
    const NUM_PUSH_TASKS: usize = 8;
    const NUM_PULL_TASKS: usize = 4;
    let total_tasks = NUM_PUSH_TASKS + NUM_PULL_TASKS;

    let eventual_api = Arc::new(Mutex::new(EventualApi::new(node_id("local-node"))));
    let barrier = Arc::new(Barrier::new(total_tasks));

    let mut handles = Vec::new();

    // Push tasks: each pushes a unique counter value via merge_remote.
    for i in 0..NUM_PUSH_TASKS {
        let api = eventual_api.clone();
        let bar = barrier.clone();
        handles.push(tokio::spawn(async move {
            let nid = node_id(&format!("remote-{i}"));
            let mut counter = PnCounter::new();
            for _ in 0..10 {
                counter.increment(&nid);
            }
            let remote_value = CrdtValue::Counter(counter);
            let key = format!("sync/key-{i}");

            bar.wait().await;
            let mut locked = api.lock().await;
            let result = locked.merge_remote(key, &remote_value);
            assert!(result.is_ok(), "merge_remote must succeed");
        }));
    }

    // Pull tasks: concurrently read keys from the store.
    for _ in 0..NUM_PULL_TASKS {
        let api = eventual_api.clone();
        let bar = barrier.clone();
        handles.push(tokio::spawn(async move {
            bar.wait().await;
            let locked = api.lock().await;
            // Read should not panic, even if data isn't there yet.
            for i in 0..NUM_PUSH_TASKS {
                let key = format!("sync/key-{i}");
                let _ = locked.get_eventual(&key);
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Verify all pushed keys exist and have correct values.
    let locked = eventual_api.lock().await;
    for i in 0..NUM_PUSH_TASKS {
        let key = format!("sync/key-{i}");
        let value = locked.get_eventual(&key);
        assert!(
            value.is_some(),
            "key '{key}' must exist after concurrent push"
        );
        if let Some(CrdtValue::Counter(c)) = value {
            assert_eq!(
                c.value(),
                10,
                "counter at '{key}' must have value 10 from 10 increments"
            );
        } else {
            panic!("key '{key}' must be a Counter");
        }
    }
}

/// Concurrent merges of the same key from multiple peers: all increments
/// from all peers must be reflected in the final counter value.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_merge_same_key_multiple_peers() {
    const NUM_PEERS: usize = 20;
    const INCREMENTS_PER_PEER: usize = 50;

    let eventual_api = Arc::new(Mutex::new(EventualApi::new(node_id("local-node"))));
    let barrier = Arc::new(Barrier::new(NUM_PEERS));

    let mut handles = Vec::new();
    for i in 0..NUM_PEERS {
        let api = eventual_api.clone();
        let bar = barrier.clone();
        handles.push(tokio::spawn(async move {
            let nid = node_id(&format!("peer-{i}"));
            let mut counter = PnCounter::new();
            for _ in 0..INCREMENTS_PER_PEER {
                counter.increment(&nid);
            }
            let remote_value = CrdtValue::Counter(counter);

            bar.wait().await;
            let mut locked = api.lock().await;
            let _ = locked.merge_remote("shared/counter".to_string(), &remote_value);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let locked = eventual_api.lock().await;
    if let Some(CrdtValue::Counter(c)) = locked.get_eventual("shared/counter") {
        assert_eq!(
            c.value(),
            (NUM_PEERS * INCREMENTS_PER_PEER) as i64,
            "counter must reflect all peer increments after concurrent merges"
        );
    } else {
        panic!("shared/counter must be a Counter");
    }
}

/// Concurrent writes to different CRDT types must not interfere with each
/// other or produce type mismatches.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writes_different_crdt_types() {
    const NUM_TASKS: usize = 12;

    let eventual_api = Arc::new(Mutex::new(EventualApi::new(node_id("node-1"))));
    let barrier = Arc::new(Barrier::new(NUM_TASKS));

    // Pre-create keys of different types.
    {
        let mut locked = eventual_api.lock().await;
        locked.eventual_write(
            "counter/key".to_string(),
            CrdtValue::Counter(PnCounter::new()),
        );
        locked.eventual_write("set/key".to_string(), CrdtValue::Set(OrSet::new()));
        locked.eventual_write(
            "register/key".to_string(),
            CrdtValue::Register(LwwRegister::new()),
        );
    }

    let mut handles = Vec::new();

    // 4 tasks increment the counter.
    for _ in 0..4 {
        let api = eventual_api.clone();
        let bar = barrier.clone();
        handles.push(tokio::spawn(async move {
            bar.wait().await;
            let mut locked = api.lock().await;
            let result = locked.eventual_counter_inc("counter/key");
            assert!(result.is_ok(), "counter increment must succeed");
        }));
    }

    // 4 tasks add to the set.
    for i in 0..4 {
        let api = eventual_api.clone();
        let bar = barrier.clone();
        handles.push(tokio::spawn(async move {
            bar.wait().await;
            let mut locked = api.lock().await;
            let result = locked.eventual_set_add("set/key", format!("elem-{i}"));
            assert!(result.is_ok(), "set add must succeed");
        }));
    }

    // 4 tasks write to the register.
    for i in 0..4 {
        let api = eventual_api.clone();
        let bar = barrier.clone();
        handles.push(tokio::spawn(async move {
            bar.wait().await;
            let mut locked = api.lock().await;
            let result = locked.eventual_register_set("register/key", format!("value-{i}"));
            assert!(result.is_ok(), "register set must succeed");
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let locked = eventual_api.lock().await;

    // Counter should have value 4.
    if let Some(CrdtValue::Counter(c)) = locked.get_eventual("counter/key") {
        assert_eq!(c.value(), 4, "counter must reflect 4 increments");
    } else {
        panic!("counter/key must be a Counter");
    }

    // Set should have 4 elements.
    if let Some(CrdtValue::Set(s)) = locked.get_eventual("set/key") {
        assert_eq!(s.len(), 4, "set must have 4 elements");
    } else {
        panic!("set/key must be a Set");
    }

    // Register should have a non-None value.
    if let Some(CrdtValue::Register(r)) = locked.get_eventual("register/key") {
        assert!(
            r.get().is_some(),
            "register must have a value after concurrent writes"
        );
    } else {
        panic!("register/key must be a Register");
    }
}

// =========================================================================
// Test F: Concurrent AckFrontierSet with fencing
// =========================================================================

/// Concurrent frontier updates with fencing: once a version is fenced,
/// no further updates for that version should be accepted, even under
/// high concurrency.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_frontier_fencing() {
    let frontier_set = Arc::new(Mutex::new(AckFrontierSet::new()));
    const UPDATES_BEFORE_FENCE: usize = 10;
    const UPDATES_AFTER_FENCE: usize = 20;

    // Phase 1: Establish some frontiers.
    for i in 0..UPDATES_BEFORE_FENCE {
        let mut locked = frontier_set.lock().await;
        locked.update(AckFrontier {
            authority_id: node_id("auth-0"),
            frontier_hlc: ts(100 + i as u64, 0, "auth-0"),
            key_range: kr("data/"),
            policy_version: PolicyVersion(1),
            digest_hash: String::new(),
        });
    }

    // Phase 2: Fence version 1 and concurrently attempt updates.
    {
        let mut locked = frontier_set.lock().await;
        locked.fence_version(&kr("data/"), PolicyVersion(1));
    }

    let barrier = Arc::new(Barrier::new(UPDATES_AFTER_FENCE));
    let mut handles = Vec::new();
    for i in 0..UPDATES_AFTER_FENCE {
        let fs = frontier_set.clone();
        let bar = barrier.clone();
        handles.push(tokio::spawn(async move {
            bar.wait().await;
            let mut locked = fs.lock().await;
            let accepted = locked.update(AckFrontier {
                authority_id: node_id(&format!("auth-{}", i % 5)),
                frontier_hlc: ts(500 + i as u64, 0, &format!("auth-{}", i % 5)),
                key_range: kr("data/"),
                policy_version: PolicyVersion(1),
                digest_hash: String::new(),
            });
            // All updates to the fenced version must be rejected.
            assert!(
                !accepted,
                "update to fenced version must be rejected (attempt {i})"
            );
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Version 2 updates should still be accepted.
    {
        let mut locked = frontier_set.lock().await;
        let accepted = locked.update(AckFrontier {
            authority_id: node_id("auth-0"),
            frontier_hlc: ts(1000, 0, "auth-0"),
            key_range: kr("data/"),
            policy_version: PolicyVersion(2),
            digest_hash: String::new(),
        });
        assert!(accepted, "update to unfenced version 2 must be accepted");
    }
}

// =========================================================================
// Test G: Stress test - interleaved operations on shared EventualApi
// =========================================================================

/// High-concurrency interleaved reads, writes, and merges on the same
/// EventualApi instance. Verifies that no panics or corruptions occur
/// under heavy concurrent load.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stress_interleaved_operations() {
    const TOTAL_TASKS: usize = 50;

    let eventual_api = Arc::new(Mutex::new(EventualApi::new(node_id("stress-node"))));

    // Pre-populate.
    {
        let mut locked = eventual_api.lock().await;
        locked.eventual_write(
            "stress/counter".to_string(),
            CrdtValue::Counter(PnCounter::new()),
        );
        locked.eventual_write("stress/set".to_string(), CrdtValue::Set(OrSet::new()));
    }

    let barrier = Arc::new(Barrier::new(TOTAL_TASKS));
    let mut handles = Vec::new();

    for i in 0..TOTAL_TASKS {
        let api = eventual_api.clone();
        let bar = barrier.clone();
        handles.push(tokio::spawn(async move {
            bar.wait().await;

            let mut locked = api.lock().await;

            match i % 5 {
                0 => {
                    // Increment counter.
                    let _ = locked.eventual_counter_inc("stress/counter");
                }
                1 => {
                    // Add to set.
                    let _ = locked.eventual_set_add("stress/set", format!("item-{i}"));
                }
                2 => {
                    // Read counter.
                    let _ = locked.get_eventual("stress/counter");
                }
                3 => {
                    // Merge remote counter.
                    let mut c = PnCounter::new();
                    c.increment(&node_id(&format!("remote-{i}")));
                    let _ =
                        locked.merge_remote("stress/counter".to_string(), &CrdtValue::Counter(c));
                }
                4 => {
                    // Read set.
                    let _ = locked.get_eventual("stress/set");
                }
                _ => unreachable!(),
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Verify final state is coherent (no panics, no corruption).
    let locked = eventual_api.lock().await;
    assert!(
        locked.get_eventual("stress/counter").is_some(),
        "counter must survive interleaved operations"
    );
    assert!(
        locked.get_eventual("stress/set").is_some(),
        "set must survive interleaved operations"
    );

    // Counter value should be the sum of all increments and merges.
    if let Some(CrdtValue::Counter(c)) = locked.get_eventual("stress/counter") {
        // Each i % 5 == 0 does an increment, each i % 5 == 3 does a merge (+1).
        let inc_count = (0..TOTAL_TASKS).filter(|i| i % 5 == 0).count();
        let merge_count = (0..TOTAL_TASKS).filter(|i| i % 5 == 3).count();
        assert_eq!(
            c.value(),
            (inc_count + merge_count) as i64,
            "counter must reflect all increments and merges"
        );
    }
}

// =========================================================================
// Test H: Pure CRDT merge commutativity (different orderings, multi-thread)
// =========================================================================

/// Verify that merging N replicas in any order produces the same result.
/// Uses concurrent tasks on a multi-threaded runtime to exercise different
/// merge orderings in parallel.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_order_independence_via_concurrent_fold() {
    const NUM_REPLICAS: usize = 10;

    // Build replicas with distinct data.
    let replicas: Vec<PnCounter> = (0..NUM_REPLICAS)
        .map(|i| {
            let nid = node_id(&format!("node-{i}"));
            let mut c = PnCounter::new();
            for _ in 0..=i {
                c.increment(&nid);
            }
            c
        })
        .collect();

    let expected = (0..NUM_REPLICAS).map(|i| (i + 1) as i64).sum::<i64>();

    // Run multiple concurrent merges in different orders.
    let results = Arc::new(Mutex::new(Vec::new()));
    let barrier = Arc::new(Barrier::new(5));

    let mut handles = Vec::new();
    for trial in 0..5 {
        let replicas_clone = replicas.clone();
        let res = results.clone();
        let bar = barrier.clone();

        handles.push(tokio::spawn(async move {
            bar.wait().await;

            // Rotate the merge order by `trial` positions.
            let mut merged = PnCounter::new();
            for i in 0..replicas_clone.len() {
                let idx = (i + trial) % replicas_clone.len();
                merged.merge(&replicas_clone[idx]);
            }

            let mut locked = res.lock().await;
            locked.push(merged.value());
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let locked = results.lock().await;
    for (i, &value) in locked.iter().enumerate() {
        assert_eq!(
            value, expected,
            "trial {i}: merge order must not affect final value (expected {expected}, got {value})"
        );
    }
}
