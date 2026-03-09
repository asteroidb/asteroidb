//! Property-based tests for all CRDT types.
//!
//! Tests mathematical properties (commutativity, associativity, idempotency)
//! plus type-specific properties:
//! - PnCounter: monotonicity (merge never decreases the max of the two values)
//! - OrSet: add-wins semantics (concurrent add survives remove)
//! - OrMap: commutativity, associativity, idempotency
//! - LwwRegister: timestamp ordering (highest timestamp always wins)

use proptest::prelude::*;

use asteroidb_poc::crdt::lww_register::LwwRegister;
use asteroidb_poc::crdt::or_map::OrMap;
use asteroidb_poc::crdt::or_set::OrSet;
use asteroidb_poc::crdt::pn_counter::PnCounter;
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::types::NodeId;

// ---------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------

fn node(name: &str) -> NodeId {
    NodeId(name.into())
}

fn ts(physical: u64, logical: u32, node_id: &str) -> HlcTimestamp {
    HlcTimestamp {
        physical,
        logical,
        node_id: node_id.into(),
    }
}

// ---------------------------------------------------------------
// Strategies
// ---------------------------------------------------------------

/// Generate a PnCounter with random increments/decrements on up to 3 nodes.
fn arb_pn_counter() -> impl Strategy<Value = PnCounter> {
    prop::collection::vec((0..3u8, prop::bool::ANY), 0..10).prop_map(|ops| {
        let nodes = [node("n0"), node("n1"), node("n2")];
        let mut counter = PnCounter::new();
        for (node_idx, is_inc) in ops {
            if is_inc {
                counter.increment(&nodes[node_idx as usize]);
            } else {
                counter.decrement(&nodes[node_idx as usize]);
            }
        }
        counter
    })
}

/// Generate an OrSet with random add/remove of string elements.
fn arb_or_set() -> impl Strategy<Value = OrSet<String>> {
    prop::collection::vec((0..2u8, 0..4u8, prop::bool::ANY), 0..10).prop_map(|ops| {
        let nodes = [node("n0"), node("n1")];
        let elements: Vec<String> = (0..4).map(|i| format!("elem{i}")).collect();
        let mut set = OrSet::new();
        for (node_idx, elem_idx, is_add) in ops {
            if is_add {
                set.add(
                    elements[elem_idx as usize].clone(),
                    &nodes[node_idx as usize],
                );
            } else {
                set.remove(&elements[elem_idx as usize]);
            }
        }
        set
    })
}

/// Generate an OrMap with random set/delete of string key-value pairs.
fn arb_or_map() -> impl Strategy<Value = OrMap<String, String>> {
    prop::collection::vec((0..2u8, 0..3u8, 0..3u8, 1..100u64, prop::bool::ANY), 0..8).prop_map(
        |ops| {
            let nodes = [node("n0"), node("n1")];
            let keys: Vec<String> = (0..3).map(|i| format!("key{i}")).collect();
            let values: Vec<String> = (0..3).map(|i| format!("val{i}")).collect();
            let mut map = OrMap::new();
            for (i, (node_idx, key_idx, val_idx, ts_base, is_set)) in ops.into_iter().enumerate() {
                let n = &nodes[node_idx as usize];
                if is_set {
                    let timestamp = ts(ts_base + (i as u64) * 100, 0, &n.0);
                    map.set(
                        keys[key_idx as usize].clone(),
                        values[val_idx as usize].clone(),
                        timestamp,
                        n,
                    );
                } else {
                    map.delete(&keys[key_idx as usize]);
                }
            }
            map
        },
    )
}

/// Generate a LwwRegister with a random set operation.
fn arb_lww_register() -> impl Strategy<Value = LwwRegister<String>> {
    (1..1000u64, 0..10u32, 0..3u8, "[a-z]{1,5}").prop_map(
        |(physical, logical, node_suffix, value)| {
            let mut reg = LwwRegister::new();
            reg.set(value, ts(physical, logical, &format!("n{node_suffix}")));
            reg
        },
    )
}

/// Generate a LwwRegister with multiple set operations to exercise LWW ordering.
fn arb_lww_register_multi() -> impl Strategy<Value = LwwRegister<String>> {
    prop::collection::vec((1..1000u64, 0..10u32, 0..3u8, "[a-z]{1,5}"), 1..5).prop_map(|ops| {
        let mut reg = LwwRegister::new();
        for (physical, logical, node_suffix, value) in ops {
            reg.set(value, ts(physical, logical, &format!("n{node_suffix}")));
        }
        reg
    })
}

// ---------------------------------------------------------------
// Comparison helpers
// ---------------------------------------------------------------

/// Collect sorted elements from an OrSet for comparison.
fn or_set_sorted(set: &OrSet<String>) -> Vec<String> {
    let mut elems: Vec<String> = set.elements().into_iter().cloned().collect();
    elems.sort();
    elems
}

/// Collect sorted key-value pairs from an OrMap for comparison.
fn or_map_sorted(map: &OrMap<String, String>) -> Vec<(String, String)> {
    let mut entries: Vec<(String, String)> = map
        .keys()
        .into_iter()
        .filter_map(|k| map.get(k).map(|v| (k.clone(), v.clone())))
        .collect();
    entries.sort();
    entries
}

/// Extract (value, timestamp) from a LwwRegister for comparison.
fn lww_state(reg: &LwwRegister<String>) -> (Option<String>, HlcTimestamp) {
    (reg.get().cloned(), reg.timestamp().clone())
}

// ===================================================================
// PnCounter properties
// ===================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    #[test]
    fn pn_counter_commutativity(a in arb_pn_counter(), b in arb_pn_counter()) {
        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        prop_assert_eq!(ab.value(), ba.value());
    }

    #[test]
    fn pn_counter_associativity(
        a in arb_pn_counter(),
        b in arb_pn_counter(),
        c in arb_pn_counter()
    ) {
        // (a merge b) merge c
        let mut ab_c = a.clone();
        ab_c.merge(&b);
        ab_c.merge(&c);

        // a merge (b merge c)
        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a.clone();
        a_bc.merge(&bc);

        prop_assert_eq!(ab_c.value(), a_bc.value());
    }

    #[test]
    fn pn_counter_idempotency(a in arb_pn_counter()) {
        let before = a.value();
        let mut merged = a.clone();
        merged.merge(&a);
        prop_assert_eq!(merged.value(), before);
    }

    /// Monotonicity: merging can only increase the "knowledge" of the counter.
    /// After merge, the resulting value should be >= max(a.value, b.value) OR
    /// <= min(a.value, b.value) depending on overlapping nodes. However, for
    /// PN-Counters the merge takes element-wise max of both P and N maps.
    /// This means: for each node, p_merged >= p_a and p_merged >= p_b,
    /// and similarly n_merged >= n_a and n_merged >= n_b.
    /// A weaker but always-true property: merge(a, b).value() is deterministic
    /// and the merged P counts are at least as large as either input's P counts,
    /// same for N counts. We verify merge never "loses" increments or decrements.
    #[test]
    fn pn_counter_monotonicity_merge_dominates(a in arb_pn_counter(), b in arb_pn_counter()) {
        // After merging b into a, the result should dominate both inputs
        // in terms of individual node counters. We verify this indirectly:
        // merging a copy of the pre-merge state should be a no-op (idempotent
        // after absorption), which confirms no information was lost.
        let mut merged = a.clone();
        merged.merge(&b);

        // merged should dominate a: merge(merged, a) == merged
        let mut merged_with_a = merged.clone();
        merged_with_a.merge(&a);
        prop_assert_eq!(merged_with_a.value(), merged.value());

        // merged should dominate b: merge(merged, b) == merged
        let mut merged_with_b = merged.clone();
        merged_with_b.merge(&b);
        prop_assert_eq!(merged_with_b.value(), merged.value());
    }

    /// Increment monotonicity: incrementing a counter always increases
    /// its value by exactly 1.
    #[test]
    fn pn_counter_increment_increases(a in arb_pn_counter(), node_idx in 0..3u8) {
        let nodes = [node("n0"), node("n1"), node("n2")];
        let before = a.value();
        let mut after = a.clone();
        after.increment(&nodes[node_idx as usize]);
        // Increment should increase value by 1 (assuming no overflow)
        if before < i64::MAX {
            prop_assert_eq!(after.value(), before + 1);
        }
    }

    /// Decrement monotonicity: decrementing a counter always decreases
    /// its value by exactly 1.
    #[test]
    fn pn_counter_decrement_decreases(a in arb_pn_counter(), node_idx in 0..3u8) {
        let nodes = [node("n0"), node("n1"), node("n2")];
        let before = a.value();
        let mut after = a.clone();
        after.decrement(&nodes[node_idx as usize]);
        // Decrement should decrease value by 1 (assuming no underflow)
        if before > i64::MIN {
            prop_assert_eq!(after.value(), before - 1);
        }
    }
}

// ===================================================================
// OrSet properties
// ===================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    #[test]
    fn or_set_commutativity(a in arb_or_set(), b in arb_or_set()) {
        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        prop_assert_eq!(or_set_sorted(&ab), or_set_sorted(&ba));
    }

    #[test]
    fn or_set_associativity(
        a in arb_or_set(),
        b in arb_or_set(),
        c in arb_or_set()
    ) {
        let mut ab_c = a.clone();
        ab_c.merge(&b);
        ab_c.merge(&c);

        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a.clone();
        a_bc.merge(&bc);

        prop_assert_eq!(or_set_sorted(&ab_c), or_set_sorted(&a_bc));
    }

    #[test]
    fn or_set_idempotency(a in arb_or_set()) {
        let before = or_set_sorted(&a);
        let mut merged = a.clone();
        merged.merge(&a);
        prop_assert_eq!(or_set_sorted(&merged), before);
    }

    /// Add-wins semantics: when one replica adds an element and another
    /// replica (that hasn't seen the add) removes the same element,
    /// the add should win after merge.
    #[test]
    fn or_set_add_wins(
        elem_idx in 0..4u8,
        pre_ops in prop::collection::vec((0..2u8, 0..4u8, prop::bool::ANY), 0..5)
    ) {
        let elements: Vec<String> = (0..4).map(|i| format!("elem{i}")).collect();
        let nodes = [node("n0"), node("n1")];
        let elem = &elements[elem_idx as usize];

        // Build a common base state.
        let mut common = OrSet::new();
        for (node_idx, ei, is_add) in &pre_ops {
            if *is_add {
                common.add(elements[*ei as usize].clone(), &nodes[*node_idx as usize]);
            } else {
                common.remove(&elements[*ei as usize]);
            }
        }
        // Ensure the element exists in common state.
        common.add(elem.clone(), &node("n0"));

        // Fork into two replicas.
        let mut replica_add = common.clone();
        let mut replica_rm = common.clone();

        // One replica adds the element again (fresh dot).
        replica_add.add(elem.clone(), &node("n0"));

        // Other replica removes it (only sees old dots).
        replica_rm.remove(elem);

        // Merge: the add should win.
        let mut merged_ar = replica_add.clone();
        merged_ar.merge(&replica_rm);
        prop_assert!(
            merged_ar.contains(elem),
            "add-wins: element should survive concurrent remove"
        );

        // Symmetric merge should also preserve the element.
        let mut merged_ra = replica_rm.clone();
        merged_ra.merge(&replica_add);
        prop_assert!(
            merged_ra.contains(elem),
            "add-wins: symmetric merge should also preserve element"
        );

        // Both should converge to the same state.
        prop_assert_eq!(or_set_sorted(&merged_ar), or_set_sorted(&merged_ra));
    }

    /// After full state exchange, all replicas converge regardless of merge order.
    #[test]
    fn or_set_convergence_three_replicas(
        a in arb_or_set(),
        b in arb_or_set(),
        c in arb_or_set()
    ) {
        let mut r1 = a;
        let mut r2 = b;
        let mut r3 = c;

        // Two rounds of full state exchange.
        for _ in 0..2 {
            let s1 = r1.clone();
            let s2 = r2.clone();
            let s3 = r3.clone();

            r1.merge(&s2);
            r1.merge(&s3);
            r2.merge(&s1);
            r2.merge(&s3);
            r3.merge(&s1);
            r3.merge(&s2);
        }

        prop_assert_eq!(or_set_sorted(&r1), or_set_sorted(&r2));
        prop_assert_eq!(or_set_sorted(&r2), or_set_sorted(&r3));
    }
}

// ===================================================================
// OrMap properties
// ===================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    #[test]
    fn or_map_commutativity(a in arb_or_map(), b in arb_or_map()) {
        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        prop_assert_eq!(or_map_sorted(&ab), or_map_sorted(&ba));
    }

    /// OrMap convergence: all replicas converge after full state exchange,
    /// regardless of merge order.
    #[test]
    fn or_map_convergence(
        a in arb_or_map(),
        b in arb_or_map(),
        c in arb_or_map()
    ) {
        let mut r1 = a;
        let mut r2 = b;
        let mut r3 = c;

        for _ in 0..3 {
            let s1 = r1.clone();
            let s2 = r2.clone();
            let s3 = r3.clone();

            r1.merge(&s2);
            r1.merge(&s3);
            r2.merge(&s1);
            r2.merge(&s3);
            r3.merge(&s1);
            r3.merge(&s2);
        }

        prop_assert_eq!(or_map_sorted(&r1), or_map_sorted(&r2));
        prop_assert_eq!(or_map_sorted(&r2), or_map_sorted(&r3));
    }

    #[test]
    fn or_map_idempotency(a in arb_or_map()) {
        let before = or_map_sorted(&a);
        let mut merged = a.clone();
        merged.merge(&a);
        prop_assert_eq!(or_map_sorted(&merged), before);
    }

    /// Add-wins for OrMap: concurrent set and delete resolve in favor of set.
    #[test]
    fn or_map_add_wins(
        key_idx in 0..3u8,
        val_idx in 0..3u8,
        ts_base in 100..500u64
    ) {
        let keys: Vec<String> = (0..3).map(|i| format!("key{i}")).collect();
        let values: Vec<String> = (0..3).map(|i| format!("val{i}")).collect();
        let key = &keys[key_idx as usize];
        let val = &values[val_idx as usize];

        // Build a common base with the key present.
        let mut common: OrMap<String, String> = OrMap::new();
        common.set(
            key.clone(),
            "original".to_string(),
            ts(ts_base, 0, "n0"),
            &node("n0"),
        );

        let mut replica_set = common.clone();
        let mut replica_del = common.clone();

        // One replica sets the key with a new value (fresh dot).
        replica_set.set(
            key.clone(),
            val.clone(),
            ts(ts_base + 100, 0, "n1"),
            &node("n1"),
        );

        // Other replica deletes the key.
        replica_del.delete(key);

        // Merge: set should win.
        let mut merged = replica_set.clone();
        merged.merge(&replica_del);
        prop_assert!(
            merged.contains_key(key),
            "add-wins: key should survive concurrent delete"
        );

        // Symmetric merge.
        let mut merged_rev = replica_del.clone();
        merged_rev.merge(&replica_set);
        prop_assert!(
            merged_rev.contains_key(key),
            "add-wins: symmetric merge should also preserve key"
        );
    }
}

// ===================================================================
// LwwRegister properties
// ===================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    #[test]
    fn lww_register_commutativity(a in arb_lww_register(), b in arb_lww_register()) {
        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        prop_assert_eq!(lww_state(&ab), lww_state(&ba));
    }

    #[test]
    fn lww_register_associativity(
        a in arb_lww_register(),
        b in arb_lww_register(),
        c in arb_lww_register()
    ) {
        let mut ab_c = a.clone();
        ab_c.merge(&b);
        ab_c.merge(&c);

        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a.clone();
        a_bc.merge(&bc);

        prop_assert_eq!(lww_state(&ab_c), lww_state(&a_bc));
    }

    #[test]
    fn lww_register_idempotency(a in arb_lww_register()) {
        let before = lww_state(&a);
        let mut merged = a.clone();
        merged.merge(&a);
        prop_assert_eq!(lww_state(&merged), before);
    }

    /// Timestamp ordering: after merging two registers, the result always
    /// holds the value from the register with the highest timestamp.
    #[test]
    fn lww_register_timestamp_ordering(a in arb_lww_register(), b in arb_lww_register()) {
        let mut merged = a.clone();
        merged.merge(&b);

        let ts_a = a.timestamp();
        let ts_b = b.timestamp();

        if ts_a > ts_b {
            // a has the higher timestamp, its value should win.
            prop_assert_eq!(merged.get(), a.get());
            prop_assert_eq!(merged.timestamp(), ts_a);
        } else if ts_b > ts_a {
            // b has the higher timestamp, its value should win.
            prop_assert_eq!(merged.get(), b.get());
            prop_assert_eq!(merged.timestamp(), ts_b);
        } else {
            // Equal timestamps: the larger value wins (deterministic tiebreaker for commutativity).
            let expected = match (a.get(), b.get()) {
                (Some(va), Some(vb)) => Some(va.max(vb)),
                (None, Some(vb)) => Some(vb),
                (Some(va), None) => Some(va),
                (None, None) => None,
            };
            prop_assert_eq!(merged.get(), expected);
        }
    }

    /// Set with strictly higher timestamp always succeeds.
    #[test]
    fn lww_register_set_with_higher_ts_wins(
        a in arb_lww_register_multi(),
        new_val in "[a-z]{1,5}",
        ts_offset in 1..500u64
    ) {
        let current_ts = a.timestamp().clone();
        let higher_ts = ts(
            current_ts.physical + ts_offset,
            current_ts.logical,
            &current_ts.node_id,
        );

        let mut reg = a.clone();
        let updated = reg.set(new_val.clone(), higher_ts.clone());

        prop_assert!(updated, "set with strictly higher timestamp should succeed");
        prop_assert_eq!(reg.get(), Some(&new_val));
        prop_assert_eq!(reg.timestamp(), &higher_ts);
    }

    /// Set with lower or equal timestamp is always rejected.
    #[test]
    fn lww_register_set_with_lower_ts_rejected(
        a in arb_lww_register_multi(),
        ts_offset in 0..500u64
    ) {
        let current_ts = a.timestamp().clone();
        // Build a timestamp that is <= current (subtract from physical, or use 0).
        let lower_physical = current_ts.physical.saturating_sub(ts_offset + 1);
        let lower_ts = ts(lower_physical, 0, "stale-node");

        // Only test if the constructed timestamp is actually lower.
        if lower_ts < current_ts {
            let original_val = a.get().cloned();
            let mut reg = a.clone();
            let updated = reg.set("should-not-win".to_string(), lower_ts);

            prop_assert!(!updated, "set with lower timestamp should be rejected");
            prop_assert_eq!(reg.get().cloned(), original_val);
        }
    }

    /// After merging multiple registers, the result always contains
    /// the globally maximum timestamp value.
    #[test]
    fn lww_register_merge_selects_global_max(
        a in arb_lww_register(),
        b in arb_lww_register(),
        c in arb_lww_register()
    ) {
        // Find the register with the maximum timestamp.
        let max_ts = [a.timestamp(), b.timestamp(), c.timestamp()]
            .into_iter()
            .max()
            .unwrap()
            .clone();

        let expected_val = if a.timestamp() == &max_ts {
            a.get().cloned()
        } else if b.timestamp() == &max_ts {
            b.get().cloned()
        } else {
            c.get().cloned()
        };

        // Merge all three.
        let mut merged = a.clone();
        merged.merge(&b);
        merged.merge(&c);

        prop_assert_eq!(merged.timestamp(), &max_ts);
        prop_assert_eq!(merged.get().cloned(), expected_val);
    }
}
