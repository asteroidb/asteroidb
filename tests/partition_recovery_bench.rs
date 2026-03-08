//! In-process partition recovery time measurement (Issue #226).
//!
//! Simulates a 2-node cluster where a network partition is modelled by
//! suspending bidirectional merge calls.  Both sides write independently
//! during the partition window.  After the partition heals (merges resume),
//! we measure the number of sync ticks required to reach full convergence
//! and assert zero data loss.

use std::collections::HashSet;

use asteroidb_poc::api::eventual::EventualApi;
use asteroidb_poc::store::kv::CrdtValue;
use asteroidb_poc::types::NodeId;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn node(name: &str) -> NodeId {
    NodeId(name.into())
}

/// Bidirectional merge between two EventualApi nodes for ALL shared keys.
/// Returns the number of keys synced.
fn sync_all(a: &mut EventualApi, b: &mut EventualApi) -> usize {
    let all_keys: Vec<String> = a
        .keys()
        .into_iter()
        .chain(b.keys())
        .cloned()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    let count = all_keys.len();

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

    count
}

/// Get counter value from an EventualApi (returns 0 if key absent).
fn counter_val(api: &EventualApi, key: &str) -> i64 {
    match api.get_eventual(key) {
        Some(CrdtValue::Counter(c)) => c.value(),
        _ => 0,
    }
}

/// Get register value from an EventualApi.
fn register_val(api: &EventualApi, key: &str) -> Option<String> {
    match api.get_eventual(key) {
        Some(CrdtValue::Register(r)) => r.get().cloned(),
        _ => None,
    }
}

/// Check if two nodes have identical values for all given keys.
fn nodes_converged(a: &EventualApi, b: &EventualApi, keys: &[String]) -> bool {
    for key in keys {
        let a_val = a.get_eventual(key);
        let b_val = b.get_eventual(key);
        match (a_val, b_val) {
            (None, None) => {}
            (Some(va), Some(vb)) => {
                // Compare using Debug representations since CrdtValue may not impl PartialEq.
                if format!("{va:?}") != format!("{vb:?}") {
                    return false;
                }
            }
            _ => return false,
        }
    }
    true
}

// ===========================================================================
// 1. Basic partition recovery: 2 nodes, counter writes on both sides
// ===========================================================================

#[test]
fn partition_recovery_counter_two_nodes() {
    let mut node_a = EventualApi::new(node("node-a"));
    let mut node_b = EventualApi::new(node("node-b"));

    let baseline_keys = 100;
    let partition_keys_per_side = 25;

    // --- Baseline: write keys on A, sync to B ---
    for i in 0..baseline_keys {
        node_a.eventual_counter_inc(&format!("key-{i}")).unwrap();
    }
    sync_all(&mut node_a, &mut node_b);

    // Verify baseline replication.
    for i in 0..baseline_keys {
        assert_eq!(
            counter_val(&node_a, &format!("key-{i}")),
            counter_val(&node_b, &format!("key-{i}")),
            "baseline key-{i} should match"
        );
    }

    // --- Partition: no sync calls between A and B ---

    // Node A writes during partition.
    for i in 0..partition_keys_per_side {
        node_a
            .eventual_counter_inc(&format!("partition-a-{i}"))
            .unwrap();
    }
    // Also increment some baseline keys on A.
    for i in 0..10 {
        node_a.eventual_counter_inc(&format!("key-{i}")).unwrap();
    }

    // Node B writes during partition.
    for i in 0..partition_keys_per_side {
        node_b
            .eventual_counter_inc(&format!("partition-b-{i}"))
            .unwrap();
    }
    // Also increment some baseline keys on B (different range).
    for i in 50..60 {
        node_b.eventual_counter_inc(&format!("key-{i}")).unwrap();
    }

    // Collect all keys that should exist after convergence.
    let mut all_keys: Vec<String> = (0..baseline_keys).map(|i| format!("key-{i}")).collect();
    for i in 0..partition_keys_per_side {
        all_keys.push(format!("partition-a-{i}"));
        all_keys.push(format!("partition-b-{i}"));
    }

    // --- Heal: measure ticks until convergence ---
    let max_ticks = 10;
    let mut converged_at_tick = None;

    for tick in 1..=max_ticks {
        sync_all(&mut node_a, &mut node_b);

        if nodes_converged(&node_a, &node_b, &all_keys) {
            converged_at_tick = Some(tick);
            break;
        }
    }

    let ticks = converged_at_tick.expect("nodes should converge within max_ticks");
    println!("[partition_recovery_counter_two_nodes] converged in {ticks} sync tick(s)");

    // --- Assert zero data loss ---
    // Baseline keys: all should be reachable and have correct merged value.
    for i in 0..baseline_keys {
        let key = format!("key-{i}");
        let a_val = counter_val(&node_a, &key);
        let b_val = counter_val(&node_b, &key);
        assert_eq!(a_val, b_val, "key-{i} should match after convergence");

        // Keys 0..10 were incremented on A, keys 50..60 on B.
        if i < 10 {
            assert_eq!(
                a_val, 2,
                "key-{i} should be 2 (baseline + A partition write)"
            );
        } else if (50..60).contains(&i) {
            assert_eq!(
                a_val, 2,
                "key-{i} should be 2 (baseline + B partition write)"
            );
        } else {
            assert_eq!(a_val, 1, "key-{i} should be 1 (baseline only)");
        }
    }

    // Partition-only keys: should exist on both sides.
    for i in 0..partition_keys_per_side {
        assert_eq!(
            counter_val(&node_a, &format!("partition-a-{i}")),
            1,
            "partition-a-{i} should be 1 on node_a"
        );
        assert_eq!(
            counter_val(&node_b, &format!("partition-a-{i}")),
            1,
            "partition-a-{i} should be 1 on node_b"
        );
        assert_eq!(
            counter_val(&node_a, &format!("partition-b-{i}")),
            1,
            "partition-b-{i} should be 1 on node_a"
        );
        assert_eq!(
            counter_val(&node_b, &format!("partition-b-{i}")),
            1,
            "partition-b-{i} should be 1 on node_b"
        );
    }

    // Final total key count check.
    let a_keys: HashSet<_> = node_a.keys().into_iter().cloned().collect();
    let b_keys: HashSet<_> = node_b.keys().into_iter().cloned().collect();
    assert_eq!(a_keys, b_keys, "both nodes should have identical key sets");
    assert_eq!(a_keys.len(), all_keys.len(), "total key count should match");
}

// ===========================================================================
// 2. Register recovery: LWW-Register writes on both sides
// ===========================================================================

#[test]
fn partition_recovery_register_two_nodes() {
    let mut node_a = EventualApi::new(node("node-a"));
    let mut node_b = EventualApi::new(node("node-b"));

    let num_keys = 50;

    // --- Baseline: write registers on A, sync to B ---
    for i in 0..num_keys {
        node_a
            .eventual_register_set(&format!("reg-{i}"), format!("init-{i}"))
            .unwrap();
    }
    sync_all(&mut node_a, &mut node_b);

    for i in 0..num_keys {
        assert_eq!(
            register_val(&node_a, &format!("reg-{i}")),
            register_val(&node_b, &format!("reg-{i}")),
        );
    }

    // --- Partition: each side updates different registers ---
    // A updates even-numbered keys.
    for i in (0..num_keys).step_by(2) {
        node_a
            .eventual_register_set(&format!("reg-{i}"), format!("from-a-{i}"))
            .unwrap();
    }
    // B updates odd-numbered keys (with a small delay to ensure distinct HLC).
    std::thread::sleep(std::time::Duration::from_millis(1));
    for i in (1..num_keys).step_by(2) {
        node_b
            .eventual_register_set(&format!("reg-{i}"), format!("from-b-{i}"))
            .unwrap();
    }

    // --- Heal ---
    let max_ticks = 5;
    let all_keys: Vec<String> = (0..num_keys).map(|i| format!("reg-{i}")).collect();
    let mut converged_at_tick = None;

    for tick in 1..=max_ticks {
        sync_all(&mut node_a, &mut node_b);
        if nodes_converged(&node_a, &node_b, &all_keys) {
            converged_at_tick = Some(tick);
            break;
        }
    }

    let ticks = converged_at_tick.expect("register nodes should converge");
    println!("[partition_recovery_register_two_nodes] converged in {ticks} sync tick(s)");

    // Zero data loss: all keys present on both sides with matching values.
    for i in 0..num_keys {
        let a_val = register_val(&node_a, &format!("reg-{i}"));
        let b_val = register_val(&node_b, &format!("reg-{i}"));
        assert_eq!(a_val, b_val, "reg-{i} should match after convergence");
        assert!(a_val.is_some(), "reg-{i} should not be None");
    }
}

// ===========================================================================
// 3. Three-node partition recovery with isolated node
// ===========================================================================

#[test]
fn partition_recovery_three_nodes_isolated() {
    let mut node_a = EventualApi::new(node("node-a"));
    let mut node_b = EventualApi::new(node("node-b"));
    let mut node_c = EventualApi::new(node("node-c"));

    let baseline_keys = 100;
    let partition_keys = 50;

    // --- Baseline: write on A, replicate to all ---
    for i in 0..baseline_keys {
        node_a
            .eventual_register_set(&format!("k-{i}"), format!("v-{i}"))
            .unwrap();
    }
    sync_all(&mut node_a, &mut node_b);
    sync_all(&mut node_a, &mut node_c);

    for i in 0..baseline_keys {
        assert_eq!(
            register_val(&node_c, &format!("k-{i}")),
            Some(format!("v-{i}")),
            "baseline k-{i} should be on node_c"
        );
    }

    // --- Partition: node_c is isolated; A and B continue ---
    for i in baseline_keys..(baseline_keys + partition_keys) {
        node_a
            .eventual_register_set(&format!("k-{i}"), format!("v-{i}"))
            .unwrap();
    }
    // A and B sync normally during partition.
    sync_all(&mut node_a, &mut node_b);

    // Verify C does not see partition-time writes.
    for i in baseline_keys..(baseline_keys + partition_keys) {
        assert!(
            register_val(&node_c, &format!("k-{i}")).is_none(),
            "k-{i} should NOT be on node_c during partition"
        );
    }

    // --- Heal: resume sync with C, measure ticks ---
    let total_keys = baseline_keys + partition_keys;
    let all_keys: Vec<String> = (0..total_keys).map(|i| format!("k-{i}")).collect();
    let max_ticks = 10;
    let mut converged_at_tick = None;

    for tick in 1..=max_ticks {
        // Simulate anti-entropy: sync C with A and B.
        sync_all(&mut node_a, &mut node_c);
        sync_all(&mut node_b, &mut node_c);

        // Check if C has all keys.
        let c_has_all = all_keys.iter().all(|k| register_val(&node_c, k).is_some());
        if c_has_all && nodes_converged(&node_a, &node_c, &all_keys) {
            converged_at_tick = Some(tick);
            break;
        }
    }

    let ticks = converged_at_tick.expect("3-node cluster should converge after partition heal");
    println!("[partition_recovery_three_nodes_isolated] converged in {ticks} sync tick(s)");

    // --- Assert zero data loss ---
    let keys_verified = all_keys
        .iter()
        .filter(|k| register_val(&node_c, k).is_some())
        .count();
    assert_eq!(
        keys_verified, total_keys,
        "all {total_keys} keys should be present on node_c"
    );

    for i in 0..total_keys {
        let key = format!("k-{i}");
        let expected = format!("v-{i}");
        assert_eq!(
            register_val(&node_a, &key),
            Some(expected.clone()),
            "node_a should have k-{i}"
        );
        assert_eq!(
            register_val(&node_b, &key),
            Some(expected.clone()),
            "node_b should have k-{i}"
        );
        assert_eq!(
            register_val(&node_c, &key),
            Some(expected),
            "node_c should have k-{i}"
        );
    }
}

// ===========================================================================
// 4. Convergence tick measurement with varying partition sizes
// ===========================================================================

#[test]
fn partition_recovery_measures_tick_scaling() {
    // Measure whether convergence ticks scale with partition size.
    // With CRDT merges, a single full sync should suffice regardless of
    // partition size (since merge is idempotent and total).
    let sizes = [10, 50, 100, 200];
    let mut results: Vec<(usize, usize)> = Vec::new();

    for &size in &sizes {
        let mut node_a = EventualApi::new(node("node-a"));
        let mut node_b = EventualApi::new(node("node-b"));

        // Write `size` keys on each side during "partition".
        for i in 0..size {
            node_a
                .eventual_counter_inc(&format!("scale-a-{i}"))
                .unwrap();
            node_b
                .eventual_counter_inc(&format!("scale-b-{i}"))
                .unwrap();
        }

        let all_keys: Vec<String> = (0..size)
            .flat_map(|i| vec![format!("scale-a-{i}"), format!("scale-b-{i}")])
            .collect();

        // Heal and measure ticks.
        let max_ticks = 10;
        let mut ticks = 0;
        for tick in 1..=max_ticks {
            sync_all(&mut node_a, &mut node_b);
            ticks = tick;
            if nodes_converged(&node_a, &node_b, &all_keys) {
                break;
            }
        }

        results.push((size, ticks));

        // Assert zero data loss for every size.
        let a_keys: HashSet<_> = node_a.keys().into_iter().cloned().collect();
        let b_keys: HashSet<_> = node_b.keys().into_iter().cloned().collect();
        assert_eq!(a_keys, b_keys, "key sets should match for size={size}");
        assert_eq!(
            a_keys.len(),
            size * 2,
            "total keys should be {0} for size={size}",
            size * 2
        );
    }

    println!("[partition_recovery_measures_tick_scaling] results:");
    for (size, ticks) in &results {
        println!("  partition_size={size:>4}, convergence_ticks={ticks}");
    }

    // CRDT full-state merge should converge in 1 tick regardless of size.
    for (size, ticks) in &results {
        assert!(
            *ticks <= 2,
            "convergence for size={size} took {ticks} ticks; expected <= 2"
        );
    }
}

// ===========================================================================
// 5. Partition with concurrent writes to the same keys (conflict resolution)
// ===========================================================================

#[test]
fn partition_recovery_concurrent_same_key_zero_loss() {
    let mut node_a = EventualApi::new(node("node-a"));
    let mut node_b = EventualApi::new(node("node-b"));

    let num_keys = 50;
    let writes_per_side = 10;

    // Both sides increment the same counters during partition.
    for i in 0..num_keys {
        for _ in 0..writes_per_side {
            node_a.eventual_counter_inc(&format!("shared-{i}")).unwrap();
            node_b.eventual_counter_inc(&format!("shared-{i}")).unwrap();
        }
    }

    // Before merge: each node sees only its own writes.
    for i in 0..num_keys {
        assert_eq!(
            counter_val(&node_a, &format!("shared-{i}")),
            writes_per_side as i64
        );
        assert_eq!(
            counter_val(&node_b, &format!("shared-{i}")),
            writes_per_side as i64
        );
    }

    // Heal.
    let mut ticks = 0;
    let all_keys: Vec<String> = (0..num_keys).map(|i| format!("shared-{i}")).collect();
    for tick in 1..=5 {
        sync_all(&mut node_a, &mut node_b);
        ticks = tick;
        if nodes_converged(&node_a, &node_b, &all_keys) {
            break;
        }
    }

    println!("[partition_recovery_concurrent_same_key_zero_loss] converged in {ticks} tick(s)");

    // Assert zero data loss: each key should equal writes_per_side * 2.
    let expected = (writes_per_side * 2) as i64;
    for i in 0..num_keys {
        let key = format!("shared-{i}");
        assert_eq!(
            counter_val(&node_a, &key),
            expected,
            "{key} should be {expected} on node_a"
        );
        assert_eq!(
            counter_val(&node_b, &key),
            expected,
            "{key} should be {expected} on node_b"
        );
    }
}
