//! Integration tests: Store + CRDT + HLC multi-node update merge verification.
//!
//! Validates that CRDT values written by multiple simulated nodes converge
//! correctly after merging through the Store layer (FR-001, FR-005).

use asteroidb_poc::crdt::lww_register::LwwRegister;
use asteroidb_poc::crdt::or_map::OrMap;
use asteroidb_poc::crdt::or_set::OrSet;
use asteroidb_poc::crdt::pn_counter::PnCounter;
use asteroidb_poc::error::CrdtError;
use asteroidb_poc::hlc::{Hlc, HlcTimestamp};
use asteroidb_poc::store::{CrdtValue, Store};
use asteroidb_poc::types::NodeId;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

/// Helper: extract counter value from a store entry.
fn counter_value(store: &Store, key: &str) -> i64 {
    match store.get(key) {
        Some(CrdtValue::Counter(c)) => c.value(),
        other => panic!("expected Counter at key '{key}', got {other:?}"),
    }
}

/// Helper: extract register value from a store entry.
fn register_value<'a>(store: &'a Store, key: &str) -> Option<&'a String> {
    match store.get(key) {
        Some(CrdtValue::Register(r)) => r.get(),
        other => panic!("expected Register at key '{key}', got {other:?}"),
    }
}

/// Helper: merge all entries from `src` into `dst`.
fn merge_stores(dst: &mut Store, src: &Store) {
    for key in src.keys() {
        let value = src.get(key).unwrap();
        dst.merge_value(key.clone(), value).unwrap();
    }
}

// ===========================================================================
// 1. Type-mismatch detection across nodes
// ===========================================================================

#[test]
fn two_nodes_different_crdt_types_same_key_returns_type_mismatch() {
    let mut store_a = Store::new();
    let mut store_b = Store::new();

    // Node A writes a Counter.
    let mut counter = PnCounter::new();
    counter.increment(&node("A"));
    store_a.put("shared".into(), CrdtValue::Counter(counter));

    // Node B writes a Set to the same key.
    let mut set = OrSet::new();
    set.add("item".to_string(), &node("B"));
    store_b.put("shared".into(), CrdtValue::Set(set));

    // Merging B's value into A should fail with TypeMismatch.
    let b_val = store_b.get("shared").unwrap();
    let err = store_a.merge_value("shared".into(), b_val).unwrap_err();

    assert_eq!(
        err,
        CrdtError::TypeMismatch {
            expected: "Counter".into(),
            actual: "Set".into(),
        }
    );
}

#[test]
fn type_mismatch_register_vs_map() {
    let mut store_a = Store::new();
    let mut store_b = Store::new();

    let mut reg = LwwRegister::new();
    reg.set("val".to_string(), ts(100, 0, "A"));
    store_a.put("key".into(), CrdtValue::Register(reg));

    let mut map = OrMap::new();
    map.set("k".into(), "v".into(), ts(100, 0, "B"), &node("B"));
    store_b.put("key".into(), CrdtValue::Map(map));

    let err = store_a
        .merge_value("key".into(), store_b.get("key").unwrap())
        .unwrap_err();

    assert_eq!(
        err,
        CrdtError::TypeMismatch {
            expected: "Register".into(),
            actual: "Map".into(),
        }
    );
}

// ===========================================================================
// 2. PN-Counter: multi-node increment/decrement merge
// ===========================================================================

#[test]
fn pn_counter_two_nodes_increment_decrement_merge() {
    let na = node("A");
    let nb = node("B");

    // Node A: +3
    let mut counter_a = PnCounter::new();
    counter_a.increment(&na);
    counter_a.increment(&na);
    counter_a.increment(&na);

    // Node B: +2, -1
    let mut counter_b = PnCounter::new();
    counter_b.increment(&nb);
    counter_b.increment(&nb);
    counter_b.decrement(&nb);

    let mut store_a = Store::new();
    store_a.put("hits".into(), CrdtValue::Counter(counter_a));

    let mut store_b = Store::new();
    store_b.put("hits".into(), CrdtValue::Counter(counter_b));

    // Merge B into A.
    store_a
        .merge_value("hits".into(), store_b.get("hits").unwrap())
        .unwrap();

    // Expected: P(A)=3 + P(B)=2 - N(B)=1 = 4
    assert_eq!(counter_value(&store_a, "hits"), 4);

    // Merge A into B (commutativity).
    store_b
        .merge_value("hits".into(), store_a.get("hits").unwrap())
        .unwrap();
    assert_eq!(counter_value(&store_b, "hits"), 4);
}

#[test]
fn pn_counter_three_nodes_convergence() {
    let na = node("A");
    let nb = node("B");
    let nc = node("C");

    let mut ca = PnCounter::new();
    ca.increment(&na); // +1

    let mut cb = PnCounter::new();
    cb.increment(&nb);
    cb.increment(&nb);
    cb.decrement(&nb); // +2 -1 = net +1

    let mut cc = PnCounter::new();
    cc.increment(&nc);
    cc.increment(&nc);
    cc.increment(&nc); // +3

    // Expected total: 1 + (2-1) + 3 = 5
    let mut store_a = Store::new();
    store_a.put("cnt".into(), CrdtValue::Counter(ca));

    let mut store_b = Store::new();
    store_b.put("cnt".into(), CrdtValue::Counter(cb));

    let mut store_c = Store::new();
    store_c.put("cnt".into(), CrdtValue::Counter(cc));

    // Merge path: A <- B, A <- C
    store_a
        .merge_value("cnt".into(), store_b.get("cnt").unwrap())
        .unwrap();
    store_a
        .merge_value("cnt".into(), store_c.get("cnt").unwrap())
        .unwrap();
    assert_eq!(counter_value(&store_a, "cnt"), 5);

    // Merge path: B <- C, B <- A
    store_b
        .merge_value("cnt".into(), store_c.get("cnt").unwrap())
        .unwrap();
    store_b
        .merge_value("cnt".into(), store_a.get("cnt").unwrap())
        .unwrap();
    assert_eq!(counter_value(&store_b, "cnt"), 5);

    // Merge path: C <- A (already merged), gives C everything
    store_c
        .merge_value("cnt".into(), store_a.get("cnt").unwrap())
        .unwrap();
    assert_eq!(counter_value(&store_c, "cnt"), 5);
}

#[test]
fn pn_counter_merge_is_idempotent_through_store() {
    let na = node("A");
    let nb = node("B");

    let mut ca = PnCounter::new();
    ca.increment(&na);
    ca.increment(&na);

    let mut cb = PnCounter::new();
    cb.increment(&nb);

    let mut store_a = Store::new();
    store_a.put("x".into(), CrdtValue::Counter(ca));

    let store_b = {
        let mut s = Store::new();
        s.put("x".into(), CrdtValue::Counter(cb));
        s
    };

    // Merge twice — value should not change.
    store_a
        .merge_value("x".into(), store_b.get("x").unwrap())
        .unwrap();
    let val_after_first = counter_value(&store_a, "x");

    store_a
        .merge_value("x".into(), store_b.get("x").unwrap())
        .unwrap();
    assert_eq!(counter_value(&store_a, "x"), val_after_first);
}

// ===========================================================================
// 3. OR-Map: concurrent set/delete with add-wins semantics
// ===========================================================================

#[test]
fn or_map_concurrent_set_delete_add_wins_through_store() {
    // Start with common state on both nodes.
    let mut common_map = OrMap::new();
    common_map.set(
        "config".to_string(),
        "initial".to_string(),
        ts(100, 0, "A"),
        &node("A"),
    );

    let mut store_a = Store::new();
    store_a.put("data".into(), CrdtValue::Map(common_map.clone()));

    let mut store_b = Store::new();
    store_b.put("data".into(), CrdtValue::Map(common_map));

    // Node A deletes the key.
    if let Some(CrdtValue::Map(m)) = store_a.get_mut("data") {
        m.delete(&"config".to_string());
    }

    // Node B concurrently sets a new value (new dot).
    if let Some(CrdtValue::Map(m)) = store_b.get_mut("data") {
        m.set(
            "config".to_string(),
            "updated".to_string(),
            ts(200, 0, "B"),
            &node("B"),
        );
    }

    // Merge B into A — add wins.
    store_a
        .merge_value("data".into(), store_b.get("data").unwrap())
        .unwrap();

    match store_a.get("data") {
        Some(CrdtValue::Map(m)) => {
            assert!(
                m.contains_key(&"config".to_string()),
                "add-wins: key should survive concurrent delete"
            );
            assert_eq!(m.get(&"config".to_string()), Some(&"updated".to_string()));
        }
        other => panic!("expected Map, got {other:?}"),
    }
}

#[test]
fn or_map_multi_node_disjoint_keys_merge() {
    let mut store_a = Store::new();
    let mut store_b = Store::new();

    let mut map_a = OrMap::new();
    map_a.set(
        "name".to_string(),
        "Alice".to_string(),
        ts(100, 0, "A"),
        &node("A"),
    );
    store_a.put("profile".into(), CrdtValue::Map(map_a));

    let mut map_b = OrMap::new();
    map_b.set(
        "email".to_string(),
        "bob@example.com".to_string(),
        ts(100, 0, "B"),
        &node("B"),
    );
    store_b.put("profile".into(), CrdtValue::Map(map_b));

    // Merge both directions.
    store_a
        .merge_value("profile".into(), store_b.get("profile").unwrap())
        .unwrap();
    store_b
        .merge_value("profile".into(), store_a.get("profile").unwrap())
        .unwrap();

    // Both stores should have both keys.
    for store in [&store_a, &store_b] {
        match store.get("profile") {
            Some(CrdtValue::Map(m)) => {
                assert_eq!(m.get(&"name".to_string()), Some(&"Alice".to_string()));
                assert_eq!(
                    m.get(&"email".to_string()),
                    Some(&"bob@example.com".to_string())
                );
                assert_eq!(m.len(), 2);
            }
            other => panic!("expected Map, got {other:?}"),
        }
    }
}

#[test]
fn or_map_same_key_lww_through_store() {
    let mut store_a = Store::new();
    let mut store_b = Store::new();

    let mut map_a = OrMap::new();
    map_a.set(
        "status".to_string(),
        "online".to_string(),
        ts(100, 0, "A"),
        &node("A"),
    );
    store_a.put("user".into(), CrdtValue::Map(map_a));

    let mut map_b = OrMap::new();
    map_b.set(
        "status".to_string(),
        "offline".to_string(),
        ts(200, 0, "B"),
        &node("B"),
    );
    store_b.put("user".into(), CrdtValue::Map(map_b));

    store_a
        .merge_value("user".into(), store_b.get("user").unwrap())
        .unwrap();

    match store_a.get("user") {
        Some(CrdtValue::Map(m)) => {
            // B's timestamp is higher → B's value wins.
            assert_eq!(m.get(&"status".to_string()), Some(&"offline".to_string()));
        }
        other => panic!("expected Map, got {other:?}"),
    }
}

// ===========================================================================
// 4. OR-Set: concurrent add/remove convergence
// ===========================================================================

#[test]
fn or_set_concurrent_add_remove_add_wins_through_store() {
    let na = node("A");

    // Common initial state.
    let mut common = OrSet::new();
    common.add("item".to_string(), &na);

    let mut store_a = Store::new();
    store_a.put("tags".into(), CrdtValue::Set(common.clone()));

    let mut store_b = Store::new();
    store_b.put("tags".into(), CrdtValue::Set(common));

    // Node A re-adds (new dot).
    if let Some(CrdtValue::Set(s)) = store_a.get_mut("tags") {
        s.add("item".to_string(), &na);
    }

    // Node B removes (only knows old dots).
    if let Some(CrdtValue::Set(s)) = store_b.get_mut("tags") {
        s.remove(&"item".to_string());
    }

    // Merge B into A — A's new dot survives.
    store_a
        .merge_value("tags".into(), store_b.get("tags").unwrap())
        .unwrap();

    match store_a.get("tags") {
        Some(CrdtValue::Set(s)) => {
            assert!(
                s.contains(&"item".to_string()),
                "add-wins: item should be present"
            );
        }
        other => panic!("expected Set, got {other:?}"),
    }
}

#[test]
fn or_set_multi_node_disjoint_elements_merge() {
    let na = node("A");
    let nb = node("B");

    let mut set_a = OrSet::new();
    set_a.add("alpha".to_string(), &na);
    set_a.add("beta".to_string(), &na);

    let mut set_b = OrSet::new();
    set_b.add("gamma".to_string(), &nb);
    set_b.add("delta".to_string(), &nb);

    let mut store_a = Store::new();
    store_a.put("items".into(), CrdtValue::Set(set_a));

    let mut store_b = Store::new();
    store_b.put("items".into(), CrdtValue::Set(set_b));

    // Cross-merge.
    store_a
        .merge_value("items".into(), store_b.get("items").unwrap())
        .unwrap();
    store_b
        .merge_value("items".into(), store_a.get("items").unwrap())
        .unwrap();

    for store in [&store_a, &store_b] {
        match store.get("items") {
            Some(CrdtValue::Set(s)) => {
                assert_eq!(s.len(), 4);
                assert!(s.contains(&"alpha".to_string()));
                assert!(s.contains(&"beta".to_string()));
                assert!(s.contains(&"gamma".to_string()));
                assert!(s.contains(&"delta".to_string()));
            }
            other => panic!("expected Set, got {other:?}"),
        }
    }
}

#[test]
fn or_set_three_nodes_convergence() {
    let na = node("A");
    let nb = node("B");
    let nc = node("C");

    let mut set_a = OrSet::new();
    set_a.add("x".to_string(), &na);

    let mut set_b = OrSet::new();
    set_b.add("y".to_string(), &nb);

    let mut set_c = OrSet::new();
    set_c.add("z".to_string(), &nc);

    let mut store_a = Store::new();
    store_a.put("s".into(), CrdtValue::Set(set_a));

    let mut store_b = Store::new();
    store_b.put("s".into(), CrdtValue::Set(set_b));

    let mut store_c = Store::new();
    store_c.put("s".into(), CrdtValue::Set(set_c));

    // Chain merge: A <- B, B <- C, C <- A
    store_a
        .merge_value("s".into(), store_b.get("s").unwrap())
        .unwrap();
    store_b
        .merge_value("s".into(), store_c.get("s").unwrap())
        .unwrap();
    store_c
        .merge_value("s".into(), store_a.get("s").unwrap())
        .unwrap();

    // After chain, A has {x,y}, B has {y,z}, C has {x,y,z}
    // Now propagate C back to A and B.
    store_a
        .merge_value("s".into(), store_c.get("s").unwrap())
        .unwrap();
    store_b
        .merge_value("s".into(), store_c.get("s").unwrap())
        .unwrap();

    // All should converge to {x, y, z}.
    for (name, store) in [("A", &store_a), ("B", &store_b), ("C", &store_c)] {
        match store.get("s") {
            Some(CrdtValue::Set(s)) => {
                assert_eq!(s.len(), 3, "node {name} should have 3 elements");
                assert!(s.contains(&"x".to_string()));
                assert!(s.contains(&"y".to_string()));
                assert!(s.contains(&"z".to_string()));
            }
            other => panic!("node {name}: expected Set, got {other:?}"),
        }
    }
}

// ===========================================================================
// 5. LWW-Register: HLC timestamp ordering preserved through Store
// ===========================================================================

#[test]
fn lww_register_hlc_ordering_through_store() {
    let mut store_a = Store::new();
    let mut store_b = Store::new();

    let mut reg_a = LwwRegister::new();
    reg_a.set("old_value".to_string(), ts(100, 0, "A"));
    store_a.put("config".into(), CrdtValue::Register(reg_a));

    let mut reg_b = LwwRegister::new();
    reg_b.set("new_value".to_string(), ts(200, 0, "B"));
    store_b.put("config".into(), CrdtValue::Register(reg_b));

    // Merge B into A — B has higher timestamp, so B's value wins.
    store_a
        .merge_value("config".into(), store_b.get("config").unwrap())
        .unwrap();

    assert_eq!(
        register_value(&store_a, "config"),
        Some(&"new_value".to_string())
    );
}

#[test]
fn lww_register_lower_timestamp_does_not_overwrite() {
    let mut store_a = Store::new();
    let mut store_b = Store::new();

    let mut reg_a = LwwRegister::new();
    reg_a.set("winner".to_string(), ts(500, 0, "A"));
    store_a.put("r".into(), CrdtValue::Register(reg_a));

    let mut reg_b = LwwRegister::new();
    reg_b.set("loser".to_string(), ts(100, 0, "B"));
    store_b.put("r".into(), CrdtValue::Register(reg_b));

    store_a
        .merge_value("r".into(), store_b.get("r").unwrap())
        .unwrap();

    assert_eq!(register_value(&store_a, "r"), Some(&"winner".to_string()));
}

#[test]
fn lww_register_logical_counter_tiebreak_through_store() {
    let mut store_a = Store::new();
    let mut store_b = Store::new();

    let mut reg_a = LwwRegister::new();
    reg_a.set("first".to_string(), ts(100, 0, "A"));
    store_a.put("r".into(), CrdtValue::Register(reg_a));

    let mut reg_b = LwwRegister::new();
    reg_b.set("second".to_string(), ts(100, 1, "B"));
    store_b.put("r".into(), CrdtValue::Register(reg_b));

    store_a
        .merge_value("r".into(), store_b.get("r").unwrap())
        .unwrap();

    // Logical 1 > 0, so B's value wins.
    assert_eq!(register_value(&store_a, "r"), Some(&"second".to_string()));
}

#[test]
fn lww_register_node_id_tiebreak_through_store() {
    let mut store_a = Store::new();
    let mut store_b = Store::new();

    let mut reg_a = LwwRegister::new();
    reg_a.set("alpha".to_string(), ts(100, 0, "A"));
    store_a.put("r".into(), CrdtValue::Register(reg_a));

    let mut reg_b = LwwRegister::new();
    reg_b.set("beta".to_string(), ts(100, 0, "B"));
    store_b.put("r".into(), CrdtValue::Register(reg_b));

    store_a
        .merge_value("r".into(), store_b.get("r").unwrap())
        .unwrap();

    // node_id "B" > "A" lexicographically, so B's value wins.
    assert_eq!(register_value(&store_a, "r"), Some(&"beta".to_string()));
}

#[test]
fn lww_register_with_live_hlc_clocks() {
    let mut clock_a = Hlc::new("node-A".into());
    let mut clock_b = Hlc::new("node-B".into());

    let mut store_a = Store::new();
    let mut store_b = Store::new();

    // Node A writes first.
    let mut reg_a = LwwRegister::new();
    let ts_a = clock_a.now();
    reg_a.set("from_A".to_string(), ts_a.clone());
    store_a.put("reg".into(), CrdtValue::Register(reg_a));

    // Node B sees A's timestamp and writes later.
    clock_b.update(&ts_a);
    let mut reg_b = LwwRegister::new();
    let ts_b = clock_b.now();
    reg_b.set("from_B".to_string(), ts_b);
    store_b.put("reg".into(), CrdtValue::Register(reg_b));

    // B's timestamp is strictly after A's → B wins.
    store_a
        .merge_value("reg".into(), store_b.get("reg").unwrap())
        .unwrap();

    assert_eq!(register_value(&store_a, "reg"), Some(&"from_B".to_string()));
}

// ===========================================================================
// 6. keys_with_prefix after merge
// ===========================================================================

#[test]
fn keys_with_prefix_works_after_multi_node_merge() {
    let na = node("A");
    let nb = node("B");

    let mut store_a = Store::new();
    let mut store_b = Store::new();

    // Node A adds user-prefixed keys.
    let mut ca = PnCounter::new();
    ca.increment(&na);
    store_a.put("user/alice".into(), CrdtValue::Counter(ca));

    let mut ca2 = PnCounter::new();
    ca2.increment(&na);
    store_a.put("user/bob".into(), CrdtValue::Counter(ca2));

    // Node B adds config-prefixed and user-prefixed keys.
    let mut cb = PnCounter::new();
    cb.increment(&nb);
    store_b.put("config/db".into(), CrdtValue::Counter(cb));

    let mut cb2 = PnCounter::new();
    cb2.increment(&nb);
    store_b.put("user/charlie".into(), CrdtValue::Counter(cb2));

    // Merge all of B into A.
    merge_stores(&mut store_a, &store_b);

    // Verify prefix queries.
    let mut user_keys: Vec<&String> = store_a.keys_with_prefix("user/");
    user_keys.sort();
    assert_eq!(user_keys, vec!["user/alice", "user/bob", "user/charlie"]);

    let config_keys = store_a.keys_with_prefix("config/");
    assert_eq!(config_keys.len(), 1);
    assert_eq!(config_keys[0], "config/db");

    assert_eq!(store_a.len(), 4);
}

#[test]
fn keys_with_prefix_empty_prefix_returns_all_after_merge() {
    let na = node("A");
    let nb = node("B");

    let mut store_a = Store::new();
    let mut store_b = Store::new();

    let mut ca = PnCounter::new();
    ca.increment(&na);
    store_a.put("a".into(), CrdtValue::Counter(ca));

    let mut cb = PnCounter::new();
    cb.increment(&nb);
    store_b.put("b".into(), CrdtValue::Counter(cb));

    merge_stores(&mut store_a, &store_b);

    let all_keys = store_a.keys_with_prefix("");
    assert_eq!(all_keys.len(), 2);
}

// ===========================================================================
// 7. Three-node ring merge: A→B, B→C, C→A convergence
// ===========================================================================

#[test]
fn three_node_ring_merge_pn_counter_convergence() {
    let na = node("A");
    let nb = node("B");
    let nc = node("C");

    let mut ca = PnCounter::new();
    ca.increment(&na);
    ca.increment(&na); // +2

    let mut cb = PnCounter::new();
    cb.increment(&nb);
    cb.decrement(&nb);
    cb.increment(&nb); // +2 -1 = +1

    let mut cc = PnCounter::new();
    cc.increment(&nc);
    cc.increment(&nc);
    cc.increment(&nc);
    cc.decrement(&nc); // +3 -1 = +2

    // Total expected: 2 + 1 + 2 = 5
    let mut store_a = Store::new();
    store_a.put("cnt".into(), CrdtValue::Counter(ca));

    let mut store_b = Store::new();
    store_b.put("cnt".into(), CrdtValue::Counter(cb));

    let mut store_c = Store::new();
    store_c.put("cnt".into(), CrdtValue::Counter(cc));

    // Ring merge: A→B
    store_b
        .merge_value("cnt".into(), store_a.get("cnt").unwrap())
        .unwrap();

    // B→C (B now has A+B)
    store_c
        .merge_value("cnt".into(), store_b.get("cnt").unwrap())
        .unwrap();

    // C→A (C now has A+B+C)
    store_a
        .merge_value("cnt".into(), store_c.get("cnt").unwrap())
        .unwrap();

    // After ring, A has everything. Propagate back.
    store_b
        .merge_value("cnt".into(), store_a.get("cnt").unwrap())
        .unwrap();
    store_c
        .merge_value("cnt".into(), store_a.get("cnt").unwrap())
        .unwrap();

    // All nodes converge.
    assert_eq!(counter_value(&store_a, "cnt"), 5);
    assert_eq!(counter_value(&store_b, "cnt"), 5);
    assert_eq!(counter_value(&store_c, "cnt"), 5);
}

#[test]
fn three_node_ring_merge_or_set_convergence() {
    let na = node("A");
    let nb = node("B");
    let nc = node("C");

    let mut sa = OrSet::new();
    sa.add("apple".to_string(), &na);

    let mut sb = OrSet::new();
    sb.add("banana".to_string(), &nb);

    let mut sc = OrSet::new();
    sc.add("cherry".to_string(), &nc);

    let mut store_a = Store::new();
    store_a.put("fruits".into(), CrdtValue::Set(sa));

    let mut store_b = Store::new();
    store_b.put("fruits".into(), CrdtValue::Set(sb));

    let mut store_c = Store::new();
    store_c.put("fruits".into(), CrdtValue::Set(sc));

    // Ring: A→B, B→C, C→A
    store_b
        .merge_value("fruits".into(), store_a.get("fruits").unwrap())
        .unwrap();
    store_c
        .merge_value("fruits".into(), store_b.get("fruits").unwrap())
        .unwrap();
    store_a
        .merge_value("fruits".into(), store_c.get("fruits").unwrap())
        .unwrap();

    // Propagate A's fully-merged state to B and C.
    store_b
        .merge_value("fruits".into(), store_a.get("fruits").unwrap())
        .unwrap();
    store_c
        .merge_value("fruits".into(), store_a.get("fruits").unwrap())
        .unwrap();

    for (name, store) in [("A", &store_a), ("B", &store_b), ("C", &store_c)] {
        match store.get("fruits") {
            Some(CrdtValue::Set(s)) => {
                assert_eq!(s.len(), 3, "node {name} should have 3 fruits");
                assert!(s.contains(&"apple".to_string()));
                assert!(s.contains(&"banana".to_string()));
                assert!(s.contains(&"cherry".to_string()));
            }
            other => panic!("node {name}: expected Set, got {other:?}"),
        }
    }
}

#[test]
fn three_node_ring_merge_or_map_convergence() {
    let mut ma = OrMap::new();
    ma.set(
        "name".to_string(),
        "Alice".to_string(),
        ts(100, 0, "A"),
        &node("A"),
    );

    let mut mb = OrMap::new();
    mb.set(
        "role".to_string(),
        "admin".to_string(),
        ts(100, 0, "B"),
        &node("B"),
    );

    let mut mc = OrMap::new();
    mc.set(
        "team".to_string(),
        "infra".to_string(),
        ts(100, 0, "C"),
        &node("C"),
    );

    let mut store_a = Store::new();
    store_a.put("profile".into(), CrdtValue::Map(ma));

    let mut store_b = Store::new();
    store_b.put("profile".into(), CrdtValue::Map(mb));

    let mut store_c = Store::new();
    store_c.put("profile".into(), CrdtValue::Map(mc));

    // Ring: A→B, B→C, C→A
    store_b
        .merge_value("profile".into(), store_a.get("profile").unwrap())
        .unwrap();
    store_c
        .merge_value("profile".into(), store_b.get("profile").unwrap())
        .unwrap();
    store_a
        .merge_value("profile".into(), store_c.get("profile").unwrap())
        .unwrap();

    // Propagate.
    store_b
        .merge_value("profile".into(), store_a.get("profile").unwrap())
        .unwrap();
    store_c
        .merge_value("profile".into(), store_a.get("profile").unwrap())
        .unwrap();

    for (name, store) in [("A", &store_a), ("B", &store_b), ("C", &store_c)] {
        match store.get("profile") {
            Some(CrdtValue::Map(m)) => {
                assert_eq!(m.len(), 3, "node {name} should have 3 keys");
                assert_eq!(m.get(&"name".to_string()), Some(&"Alice".to_string()));
                assert_eq!(m.get(&"role".to_string()), Some(&"admin".to_string()));
                assert_eq!(m.get(&"team".to_string()), Some(&"infra".to_string()));
            }
            other => panic!("node {name}: expected Map, got {other:?}"),
        }
    }
}

#[test]
fn three_node_ring_merge_lww_register_convergence() {
    let mut ra = LwwRegister::new();
    ra.set("val_A".to_string(), ts(100, 0, "A"));

    let mut rb = LwwRegister::new();
    rb.set("val_B".to_string(), ts(200, 0, "B"));

    let mut rc = LwwRegister::new();
    rc.set("val_C".to_string(), ts(150, 0, "C"));

    let mut store_a = Store::new();
    store_a.put("r".into(), CrdtValue::Register(ra));

    let mut store_b = Store::new();
    store_b.put("r".into(), CrdtValue::Register(rb));

    let mut store_c = Store::new();
    store_c.put("r".into(), CrdtValue::Register(rc));

    // Ring: A→B, B→C, C→A
    store_b
        .merge_value("r".into(), store_a.get("r").unwrap())
        .unwrap();
    store_c
        .merge_value("r".into(), store_b.get("r").unwrap())
        .unwrap();
    store_a
        .merge_value("r".into(), store_c.get("r").unwrap())
        .unwrap();

    // Propagate.
    store_b
        .merge_value("r".into(), store_a.get("r").unwrap())
        .unwrap();
    store_c
        .merge_value("r".into(), store_a.get("r").unwrap())
        .unwrap();

    // B has highest timestamp (200) → "val_B" wins everywhere.
    for (name, store) in [("A", &store_a), ("B", &store_b), ("C", &store_c)] {
        assert_eq!(
            register_value(store, "r"),
            Some(&"val_B".to_string()),
            "node {name} should have val_B"
        );
    }
}

// ===========================================================================
// 8. Mixed CRDT types across multiple keys
// ===========================================================================

#[test]
fn multi_key_mixed_crdt_types_merge() {
    let na = node("A");
    let nb = node("B");

    let mut store_a = Store::new();
    let mut store_b = Store::new();

    // Node A writes a counter and a set.
    let mut ca = PnCounter::new();
    ca.increment(&na);
    store_a.put("counter".into(), CrdtValue::Counter(ca));

    let mut sa = OrSet::new();
    sa.add("x".to_string(), &na);
    store_a.put("set".into(), CrdtValue::Set(sa));

    // Node B writes the same counter (different ops) and a register.
    let mut cb = PnCounter::new();
    cb.increment(&nb);
    cb.increment(&nb);
    store_b.put("counter".into(), CrdtValue::Counter(cb));

    let mut rb = LwwRegister::new();
    rb.set("hello".to_string(), ts(100, 0, "B"));
    store_b.put("register".into(), CrdtValue::Register(rb));

    // Merge all of B into A.
    merge_stores(&mut store_a, &store_b);

    // Counter: A(1) + B(2) = 3
    assert_eq!(counter_value(&store_a, "counter"), 3);

    // Set: still has "x" from A.
    match store_a.get("set") {
        Some(CrdtValue::Set(s)) => assert!(s.contains(&"x".to_string())),
        other => panic!("expected Set, got {other:?}"),
    }

    // Register: from B.
    assert_eq!(
        register_value(&store_a, "register"),
        Some(&"hello".to_string())
    );

    assert_eq!(store_a.len(), 3);
}

// ===========================================================================
// 9. Merge into empty store (key not present yet)
// ===========================================================================

#[test]
fn merge_into_empty_store_inserts_value() {
    let na = node("A");

    let mut store_a = Store::new();
    let mut counter = PnCounter::new();
    counter.increment(&na);
    counter.increment(&na);
    store_a.put("cnt".into(), CrdtValue::Counter(counter));

    // Empty target store.
    let mut store_b = Store::new();
    merge_stores(&mut store_b, &store_a);

    assert_eq!(store_b.len(), 1);
    assert_eq!(counter_value(&store_b, "cnt"), 2);
}

// ===========================================================================
// 10. Large-scale ring: 5-node convergence
// ===========================================================================

#[test]
fn five_node_ring_convergence() {
    let nodes: Vec<NodeId> = (0..5).map(|i| node(&format!("N{i}"))).collect();
    let mut stores: Vec<Store> = Vec::new();

    // Each node increments a shared counter by its index+1 times.
    for (i, nid) in nodes.iter().enumerate() {
        let mut counter = PnCounter::new();
        for _ in 0..=i {
            counter.increment(nid);
        }
        let mut store = Store::new();
        store.put("global".into(), CrdtValue::Counter(counter));
        stores.push(store);
    }

    // Expected total: 1 + 2 + 3 + 4 + 5 = 15

    // Ring merge: store[0] → store[1] → store[2] → store[3] → store[4] → store[0]
    for i in 0..5 {
        let next = (i + 1) % 5;
        let val = stores[i].get("global").unwrap().clone();
        stores[next].merge_value("global".into(), &val).unwrap();
    }

    // Second ring pass to fully propagate (needed because single ring
    // pass doesn't reach all nodes in a 5-node ring).
    for i in 0..5 {
        let next = (i + 1) % 5;
        let val = stores[i].get("global").unwrap().clone();
        stores[next].merge_value("global".into(), &val).unwrap();
    }

    // Third pass for full convergence in worst case.
    for i in 0..5 {
        let next = (i + 1) % 5;
        let val = stores[i].get("global").unwrap().clone();
        stores[next].merge_value("global".into(), &val).unwrap();
    }

    // Fourth pass.
    for i in 0..5 {
        let next = (i + 1) % 5;
        let val = stores[i].get("global").unwrap().clone();
        stores[next].merge_value("global".into(), &val).unwrap();
    }

    for (i, store) in stores.iter().enumerate() {
        assert_eq!(
            counter_value(store, "global"),
            15,
            "store[{i}] should converge to 15"
        );
    }
}

// ===========================================================================
// 11. HLC monotonicity preserved across merge with live clocks
// ===========================================================================

#[test]
fn hlc_timestamps_advance_monotonically_across_stores() {
    let mut clock_a = Hlc::new("A".into());
    let mut clock_b = Hlc::new("B".into());

    let mut store_a = Store::new();
    let mut store_b = Store::new();

    // A writes a register.
    let ts_a1 = clock_a.now();
    let mut reg_a = LwwRegister::new();
    reg_a.set("v1".to_string(), ts_a1.clone());
    store_a.put("r".into(), CrdtValue::Register(reg_a));

    // B sees A's clock, advances, and writes.
    clock_b.update(&ts_a1);
    let ts_b1 = clock_b.now();
    assert!(ts_b1 > ts_a1, "B's timestamp should be after A's");

    let mut reg_b = LwwRegister::new();
    reg_b.set("v2".to_string(), ts_b1.clone());
    store_b.put("r".into(), CrdtValue::Register(reg_b));

    // Merge: A sees B's value.
    store_a
        .merge_value("r".into(), store_b.get("r").unwrap())
        .unwrap();

    // B's later timestamp wins.
    assert_eq!(register_value(&store_a, "r"), Some(&"v2".to_string()));

    // A advances clock past B's timestamp.
    clock_a.update(&ts_b1);
    let ts_a2 = clock_a.now();
    assert!(ts_a2 > ts_b1, "A's new timestamp should be after B's");

    // A writes again with later timestamp.
    if let Some(CrdtValue::Register(r)) = store_a.get_mut("r") {
        r.set("v3".to_string(), ts_a2);
    }

    // After merge, A's latest write wins.
    store_b
        .merge_value("r".into(), store_a.get("r").unwrap())
        .unwrap();
    assert_eq!(register_value(&store_b, "r"), Some(&"v3".to_string()));
}

// ===========================================================================
// 12. Merge non-existent key into store with existing different keys
// ===========================================================================

#[test]
fn merge_new_key_does_not_affect_existing_keys() {
    let na = node("A");
    let nb = node("B");

    let mut store_a = Store::new();
    let mut ca = PnCounter::new();
    ca.increment(&na);
    store_a.put("existing".into(), CrdtValue::Counter(ca));

    let mut store_b = Store::new();
    let mut cb = PnCounter::new();
    cb.increment(&nb);
    store_b.put("new_key".into(), CrdtValue::Counter(cb));

    merge_stores(&mut store_a, &store_b);

    // Both keys should be present with correct values.
    assert_eq!(counter_value(&store_a, "existing"), 1);
    assert_eq!(counter_value(&store_a, "new_key"), 1);
    assert_eq!(store_a.len(), 2);
}
