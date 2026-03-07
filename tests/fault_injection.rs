//! Fault injection integration tests (Issue #179).
//!
//! Validates CRDT convergence under various fault conditions simulated
//! at the in-process level:
//!   - Crash recovery with divergent state
//!   - Asymmetric merge (one-way replication)
//!   - Node rejoin after missing writes
//!   - Rolling isolation with writes at each step
//!   - Jitter tolerance (merge order independence)

use asteroidb_poc::api::eventual::EventualApi;
use asteroidb_poc::store::kv::CrdtValue;
use asteroidb_poc::types::NodeId;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn node(name: &str) -> NodeId {
    NodeId(name.into())
}

/// One-way merge: replicate all keys from `src` to `dst`.
fn one_way_sync(src: &EventualApi, dst: &mut EventualApi) {
    for key in src.keys() {
        if let Some(val) = src.get_eventual(key) {
            let cloned = val.clone();
            let _ = dst.merge_remote(key.clone(), &cloned);
        }
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
        _ => 0,
    }
}

// ===========================================================================
// 1. Crash recovery with divergent state (simulated as drop + recreate)
// ===========================================================================

#[test]
fn fault_crash_recovery_divergent_state() {
    // Simulate: 3 nodes synced, node-C crashes (dropped), A and B write more,
    // C recovers as a fresh node, receives full state via merge, and converges.
    let mut node_a = EventualApi::new(node("node-a"));
    let mut node_b = EventualApi::new(node("node-b"));
    let mut node_c = EventualApi::new(node("node-c"));

    // All nodes start with counter = 3 (written on A, synced to all).
    for _ in 0..3 {
        node_a.eventual_counter_inc("data").unwrap();
    }
    sync_all(&mut node_a, &mut node_b);
    sync_all(&mut node_a, &mut node_c);

    assert_eq!(counter_val(&node_a, "data"), 3);
    assert_eq!(counter_val(&node_c, "data"), 3);

    // Snapshot C's state before crash by cloning it into a temporary node.
    let mut node_c_snapshot = EventualApi::new(node("node-c"));
    one_way_sync(&node_c, &mut node_c_snapshot);
    assert_eq!(counter_val(&node_c_snapshot, "data"), 3);

    // Node C "crashes" (dropped).
    drop(node_c);

    // A and B write more while C is down.
    for _ in 0..5 {
        node_a.eventual_counter_inc("data").unwrap();
    }
    for _ in 0..2 {
        node_b.eventual_counter_inc("data").unwrap();
    }
    sync_all(&mut node_a, &mut node_b);

    assert_eq!(counter_val(&node_a, "data"), 10); // 3 + 5 + 2

    // C "recovers" from its snapshot (re-created with stale state).
    let mut node_c_recovered = node_c_snapshot;
    assert_eq!(counter_val(&node_c_recovered, "data"), 3); // Still old state.

    // C syncs with A and converges.
    sync_all(&mut node_a, &mut node_c_recovered);

    assert_eq!(counter_val(&node_c_recovered, "data"), 10);
    assert_eq!(counter_val(&node_a, "data"), 10);
}

// ===========================================================================
// 2. Asymmetric merge (one-way replication)
// ===========================================================================

#[test]
fn fault_asymmetric_partition_one_way_sync() {
    // A can send to B, but B cannot send to A (one-way block).
    let mut node_a = EventualApi::new(node("node-a"));
    let mut node_b = EventualApi::new(node("node-b"));

    // Both start with counter = 1.
    node_a.eventual_counter_inc("asym").unwrap();
    sync_all(&mut node_a, &mut node_b);

    // During asymmetric partition: A writes, one-way sync A -> B.
    node_a.eventual_counter_inc("asym").unwrap();
    node_a.eventual_counter_inc("asym").unwrap();

    // B writes independently.
    node_b.eventual_counter_inc("asym").unwrap();

    // One-way: A -> B (B receives A's state).
    one_way_sync(&node_a, &mut node_b);

    // B should see A's writes + its own: 1 + 2 + 1 = 4.
    assert_eq!(counter_val(&node_b, "asym"), 4);

    // A does NOT see B's independent write yet.
    assert_eq!(counter_val(&node_a, "asym"), 3); // 1 + 2

    // After partition heals (full bidirectional sync):
    sync_all(&mut node_a, &mut node_b);
    assert_eq!(counter_val(&node_a, "asym"), 4);
    assert_eq!(counter_val(&node_b, "asym"), 4);
}

// ===========================================================================
// 3. Node rejoin after missing writes
// ===========================================================================

#[test]
fn fault_node_rejoin_catches_up() {
    // Node C is "down" (not participating in sync) while A and B write.
    // After C "rejoins", it should catch up.
    let mut node_a = EventualApi::new(node("node-a"));
    let mut node_b = EventualApi::new(node("node-b"));
    let mut node_c = EventualApi::new(node("node-c"));

    // Initial state.
    node_a.eventual_counter_inc("rejoin").unwrap();
    sync_all(&mut node_a, &mut node_b);
    sync_all(&mut node_a, &mut node_c);

    // Node C goes offline.

    // A and B write and sync with each other.
    for _ in 0..3 {
        node_a.eventual_counter_inc("rejoin").unwrap();
    }
    for _ in 0..2 {
        node_b.eventual_counter_inc("rejoin").unwrap();
    }
    sync_all(&mut node_a, &mut node_b);

    assert_eq!(counter_val(&node_a, "rejoin"), 6); // 1 + 3 + 2
    assert_eq!(counter_val(&node_b, "rejoin"), 6);
    assert_eq!(counter_val(&node_c, "rejoin"), 1); // Stale.

    // Node C rejoins and syncs.
    sync_all(&mut node_a, &mut node_c);
    sync_all(&mut node_b, &mut node_c);

    assert_eq!(counter_val(&node_c, "rejoin"), 6);
}

// ===========================================================================
// 4. Rolling isolation with writes
// ===========================================================================

#[test]
fn fault_rolling_partition_convergence() {
    // Sequentially isolate each node, write during isolation, heal, verify.
    let mut node_a = EventualApi::new(node("node-a"));
    let mut node_b = EventualApi::new(node("node-b"));
    let mut node_c = EventualApi::new(node("node-c"));

    // Initial state.
    node_a.eventual_counter_inc("rolling").unwrap();
    sync_all(&mut node_a, &mut node_b);
    sync_all(&mut node_a, &mut node_c);

    // Round 1: Isolate A, write on B.
    node_b.eventual_counter_inc("rolling").unwrap();
    sync_all(&mut node_b, &mut node_c); // Only B and C sync.

    // Heal A.
    sync_all(&mut node_a, &mut node_b);
    sync_all(&mut node_a, &mut node_c);

    // Round 2: Isolate B, write on C.
    node_c.eventual_counter_inc("rolling").unwrap();
    sync_all(&mut node_a, &mut node_c); // Only A and C sync.

    // Heal B.
    sync_all(&mut node_a, &mut node_b);
    sync_all(&mut node_b, &mut node_c);

    // Round 3: Isolate C, write on A.
    node_a.eventual_counter_inc("rolling").unwrap();
    sync_all(&mut node_a, &mut node_b); // Only A and B sync.

    // Heal C.
    sync_all(&mut node_a, &mut node_c);
    sync_all(&mut node_b, &mut node_c);

    // Expected: 1 (initial) + 1 (B) + 1 (C) + 1 (A) = 4
    assert_eq!(counter_val(&node_a, "rolling"), 4);
    assert_eq!(counter_val(&node_b, "rolling"), 4);
    assert_eq!(counter_val(&node_c, "rolling"), 4);
}

// ===========================================================================
// 5. Jitter tolerance: multiple writes converge even with reorder
// ===========================================================================

#[test]
fn fault_jitter_tolerance_multiple_writes_converge() {
    // Simulate jitter by merging in different orders.
    let mut node_a = EventualApi::new(node("node-a"));
    let mut node_b = EventualApi::new(node("node-b"));
    let mut node_c = EventualApi::new(node("node-c"));

    // Each node writes independently (simulating jittered delivery).
    for _ in 0..10 {
        node_a.eventual_counter_inc("jitter").unwrap();
    }
    for _ in 0..7 {
        node_b.eventual_counter_inc("jitter").unwrap();
    }
    for _ in 0..5 {
        node_c.eventual_counter_inc("jitter").unwrap();
    }

    // Merge in arbitrary "jittered" order.
    sync_all(&mut node_c, &mut node_a);
    sync_all(&mut node_b, &mut node_c);
    sync_all(&mut node_a, &mut node_b);
    // Extra round for full propagation.
    sync_all(&mut node_a, &mut node_c);

    let expected = 22; // 10 + 7 + 5
    assert_eq!(counter_val(&node_a, "jitter"), expected);
    assert_eq!(counter_val(&node_b, "jitter"), expected);
    assert_eq!(counter_val(&node_c, "jitter"), expected);
}

// ===========================================================================
// 6. Crash during partition: node crashes while partitioned, recovers
// ===========================================================================

#[test]
fn fault_crash_during_partition_then_recover() {
    // Node C is partitioned, writes happen on A+B, then C "crashes" (dropped).
    // C recovers fresh and syncs to catch up on everything.
    let mut node_a = EventualApi::new(node("node-a"));
    let mut node_b = EventualApi::new(node("node-b"));
    let mut node_c = EventualApi::new(node("node-c"));

    // Pre-partition: all share counter = 2.
    node_a.eventual_counter_inc("combo").unwrap();
    node_a.eventual_counter_inc("combo").unwrap();
    sync_all(&mut node_a, &mut node_b);
    sync_all(&mut node_a, &mut node_c);

    // Partition C: C writes independently.
    node_c.eventual_counter_inc("combo").unwrap();

    // A and B write.
    node_a.eventual_counter_inc("combo").unwrap();
    node_b.eventual_counter_inc("combo").unwrap();
    sync_all(&mut node_a, &mut node_b);

    // C crashes while partitioned (losing its independent write).
    drop(node_c);

    // C recovers as a completely fresh node.
    let mut node_c_new = EventualApi::new(node("node-c"));

    // Full sync after recovery.
    sync_all(&mut node_a, &mut node_c_new);
    sync_all(&mut node_b, &mut node_c_new);

    // C lost its own independent write (counter inc was only local, never synced).
    // So converged state = 2 (initial) + 1 (A) + 1 (B) = 4.
    assert_eq!(counter_val(&node_a, "combo"), 4);
    assert_eq!(counter_val(&node_b, "combo"), 4);
    assert_eq!(counter_val(&node_c_new, "combo"), 4);
}
