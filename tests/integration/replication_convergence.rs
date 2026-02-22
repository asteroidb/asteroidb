//! Multi-node replication and CRDT convergence integration tests (Issue #31).
//!
//! Validates distributed system invariants:
//! - Pairwise merge in ring topology leads to convergence
//! - Concurrent writes converge to the same state across all replicas
//! - Node join with full state sync achieves convergence
//! - Mixed CRDT types merge correctly across stores
//! - CRDT mathematical properties: commutativity, associativity, idempotency

use std::collections::HashSet;

use asteroidb_poc::api::eventual::EventualApi;
use asteroidb_poc::crdt::lww_register::LwwRegister;
use asteroidb_poc::crdt::or_map::OrMap;
use asteroidb_poc::crdt::or_set::OrSet;
use asteroidb_poc::crdt::pn_counter::PnCounter;
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::store::{CrdtValue, Store};
use asteroidb_poc::types::NodeId;

// ---------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------

fn node(name: &str) -> NodeId {
    NodeId(name.into())
}

fn ts(physical: u64, logical: u32, node: &str) -> HlcTimestamp {
    HlcTimestamp {
        physical,
        logical,
        node_id: node.into(),
    }
}

/// Compare two Store instances for observational equality.
/// Two stores are "equal" when they have the same keys and each key's
/// observable value is identical.
fn stores_equal(a: &Store, b: &Store) -> bool {
    let mut keys_a: Vec<&String> = a.keys();
    let mut keys_b: Vec<&String> = b.keys();
    keys_a.sort();
    keys_b.sort();
    if keys_a != keys_b {
        return false;
    }
    for key in &keys_a {
        match (a.get(key), b.get(key)) {
            (Some(va), Some(vb)) => {
                if !crdt_values_equal(va, vb) {
                    return false;
                }
            }
            _ => return false,
        }
    }
    true
}

/// Compare two CrdtValue instances for observational equality.
fn crdt_values_equal(a: &CrdtValue, b: &CrdtValue) -> bool {
    match (a, b) {
        (CrdtValue::Counter(ca), CrdtValue::Counter(cb)) => ca.value() == cb.value(),
        (CrdtValue::Set(sa), CrdtValue::Set(sb)) => sa.elements() == sb.elements(),
        (CrdtValue::Map(ma), CrdtValue::Map(mb)) => {
            let mut ka: Vec<&String> = ma.keys();
            let mut kb: Vec<&String> = mb.keys();
            ka.sort();
            kb.sort();
            if ka != kb {
                return false;
            }
            ka.iter().all(|k| ma.get(k) == mb.get(k))
        }
        (CrdtValue::Register(ra), CrdtValue::Register(rb)) => ra.get() == rb.get(),
        _ => false,
    }
}

/// Perform a full pairwise merge between all stores (simulating gossip convergence).
/// Each store merges from every other store for all keys.
fn full_mesh_merge(stores: &mut [Store]) {
    let n = stores.len();
    // Multiple rounds to ensure full propagation through any topology.
    for _ in 0..n {
        for i in 0..n {
            for j in 0..n {
                if i == j {
                    continue;
                }
                let source = stores[j].clone();
                for key in source.keys() {
                    if let Some(val) = source.get(key) {
                        let _ = stores[i].merge_value(key.clone(), val);
                    }
                }
            }
        }
    }
}

/// Perform a ring merge: each store merges with its immediate neighbor,
/// repeated enough rounds for full convergence.
fn ring_merge(stores: &mut [Store]) {
    let n = stores.len();
    // n rounds of ring propagation guarantees full convergence.
    for _ in 0..n {
        for i in 0..n {
            let next = (i + 1) % n;
            // i merges from next
            let source = stores[next].clone();
            for key in source.keys() {
                if let Some(val) = source.get(key) {
                    let _ = stores[i].merge_value(key.clone(), val);
                }
            }
            // next merges from i
            let source = stores[i].clone();
            for key in source.keys() {
                if let Some(val) = source.get(key) {
                    let _ = stores[next].merge_value(key.clone(), val);
                }
            }
        }
    }
}

// ---------------------------------------------------------------
// Scenario 1: 3-node ring topology convergence
// ---------------------------------------------------------------

#[test]
fn three_node_ring_counter_convergence() {
    let na = node("node-a");
    let nb = node("node-b");
    let nc = node("node-c");

    let mut store_a = Store::new();
    let mut store_b = Store::new();
    let mut store_c = Store::new();

    // Each node writes to "hits" counter.
    let mut ca = PnCounter::new();
    ca.increment(&na);
    ca.increment(&na);
    store_a.put("hits".into(), CrdtValue::Counter(ca));

    let mut cb = PnCounter::new();
    cb.increment(&nb);
    cb.increment(&nb);
    cb.increment(&nb);
    store_b.put("hits".into(), CrdtValue::Counter(cb));

    let mut cc = PnCounter::new();
    cc.increment(&nc);
    store_c.put("hits".into(), CrdtValue::Counter(cc));

    // Ring merge: A↔B↔C↔A
    let mut stores = [store_a, store_b, store_c];
    ring_merge(&mut stores);

    // All nodes should see hits = 2 + 3 + 1 = 6.
    for (i, store) in stores.iter().enumerate() {
        match store.get("hits") {
            Some(CrdtValue::Counter(c)) => {
                assert_eq!(c.value(), 6, "node {} should see counter=6", i);
            }
            other => panic!("node {}: expected Counter, got {:?}", i, other),
        }
    }

    // All stores should be observationally equal.
    assert!(stores_equal(&stores[0], &stores[1]));
    assert!(stores_equal(&stores[1], &stores[2]));
}

#[test]
fn three_node_ring_mixed_keys() {
    let na = node("node-a");
    let nb = node("node-b");
    let mut store_a = Store::new();
    let mut store_b = Store::new();
    let mut store_c = Store::new();

    // Node A: writes a counter.
    let mut ca = PnCounter::new();
    ca.increment(&na);
    store_a.put("counter".into(), CrdtValue::Counter(ca));

    // Node B: writes a set.
    let mut sb = OrSet::new();
    sb.add("alice".into(), &nb);
    store_b.put("users".into(), CrdtValue::Set(sb));

    // Node C: writes a register.
    let mut rc = LwwRegister::new();
    rc.set("hello".into(), ts(100, 0, "node-c"));
    store_c.put("greeting".into(), CrdtValue::Register(rc));

    let mut stores = [store_a, store_b, store_c];
    ring_merge(&mut stores);

    // Every node should see all three keys.
    for (i, store) in stores.iter().enumerate() {
        assert_eq!(store.len(), 3, "node {} should have 3 keys", i);
        assert!(store.contains_key("counter"), "node {} missing counter", i);
        assert!(store.contains_key("users"), "node {} missing users", i);
        assert!(
            store.contains_key("greeting"),
            "node {} missing greeting",
            i
        );
    }

    assert!(stores_equal(&stores[0], &stores[1]));
    assert!(stores_equal(&stores[1], &stores[2]));
}

// ---------------------------------------------------------------
// Scenario 2: 5-node concurrent write on same key
// ---------------------------------------------------------------

#[test]
fn five_node_concurrent_counter_write() {
    let nodes: Vec<NodeId> = (0..5).map(|i| node(&format!("node-{}", i))).collect();
    let mut stores: Vec<Store> = (0..5).map(|_| Store::new()).collect();

    // Each node concurrently increments the same counter.
    for (i, store) in stores.iter_mut().enumerate() {
        let mut counter = PnCounter::new();
        for _ in 0..(i + 1) {
            counter.increment(&nodes[i]);
        }
        store.put("shared".into(), CrdtValue::Counter(counter));
    }

    // Full mesh merge.
    full_mesh_merge(&mut stores);

    // Expected: 1 + 2 + 3 + 4 + 5 = 15
    for (i, store) in stores.iter().enumerate() {
        match store.get("shared") {
            Some(CrdtValue::Counter(c)) => {
                assert_eq!(c.value(), 15, "node {} should see counter=15", i);
            }
            other => panic!("node {}: expected Counter, got {:?}", i, other),
        }
    }

    // All stores equal.
    for i in 1..5 {
        assert!(
            stores_equal(&stores[0], &stores[i]),
            "store 0 != store {}",
            i
        );
    }
}

#[test]
fn five_node_concurrent_set_write() {
    let nodes: Vec<NodeId> = (0..5).map(|i| node(&format!("node-{}", i))).collect();
    let mut stores: Vec<Store> = (0..5).map(|_| Store::new()).collect();

    // Each node adds its own element to the same set.
    for (i, store) in stores.iter_mut().enumerate() {
        let mut set = OrSet::new();
        set.add(format!("elem-{}", i), &nodes[i]);
        store.put("shared-set".into(), CrdtValue::Set(set));
    }

    full_mesh_merge(&mut stores);

    // All nodes should see all 5 elements.
    for (i, store) in stores.iter().enumerate() {
        match store.get("shared-set") {
            Some(CrdtValue::Set(s)) => {
                assert_eq!(s.len(), 5, "node {} should see 5 elements", i);
                for j in 0..5 {
                    assert!(
                        s.contains(&format!("elem-{}", j)),
                        "node {} missing elem-{}",
                        i,
                        j
                    );
                }
            }
            other => panic!("node {}: expected Set, got {:?}", i, other),
        }
    }
}

#[test]
fn five_node_concurrent_register_write_lww_wins() {
    let mut stores: Vec<Store> = (0..5).map(|_| Store::new()).collect();

    // Each node writes to the same register with increasing timestamps.
    // Node 4 has the highest timestamp, so its value should win.
    for i in 0..5 {
        let mut reg = LwwRegister::new();
        reg.set(
            format!("value-{}", i),
            ts((i as u64 + 1) * 100, 0, &format!("node-{}", i)),
        );
        stores[i].put("shared-reg".into(), CrdtValue::Register(reg));
    }

    full_mesh_merge(&mut stores);

    // Node 4's value wins (timestamp 500).
    for (i, store) in stores.iter().enumerate() {
        match store.get("shared-reg") {
            Some(CrdtValue::Register(r)) => {
                assert_eq!(
                    r.get(),
                    Some(&"value-4".to_string()),
                    "node {} should see value-4 (LWW)",
                    i
                );
            }
            other => panic!("node {}: expected Register, got {:?}", i, other),
        }
    }
}

// ---------------------------------------------------------------
// Scenario 3: Node join (late joiner full state sync)
// ---------------------------------------------------------------

#[test]
fn node_join_full_state_sync() {
    let na = node("node-a");
    let nb = node("node-b");
    let nc = node("node-c");

    // Two existing nodes have been operating.
    let mut store_a = Store::new();
    let mut store_b = Store::new();

    let mut ca = PnCounter::new();
    ca.increment(&na);
    ca.increment(&na);
    store_a.put("counter".into(), CrdtValue::Counter(ca));

    let mut sa = OrSet::new();
    sa.add("alice".into(), &na);
    store_a.put("users".into(), CrdtValue::Set(sa));

    let mut cb = PnCounter::new();
    cb.increment(&nb);
    store_b.put("counter".into(), CrdtValue::Counter(cb));

    let mut sb = OrSet::new();
    sb.add("bob".into(), &nb);
    store_b.put("users".into(), CrdtValue::Set(sb));

    // Cross-merge existing nodes.
    let clone_a = store_a.clone();
    let clone_b = store_b.clone();
    for key in clone_b.keys() {
        if let Some(val) = clone_b.get(key) {
            store_a.merge_value(key.clone(), val).unwrap();
        }
    }
    for key in clone_a.keys() {
        if let Some(val) = clone_a.get(key) {
            store_b.merge_value(key.clone(), val).unwrap();
        }
    }

    // New node C joins with its own data.
    let mut store_c = Store::new();
    let mut cc = PnCounter::new();
    cc.increment(&nc);
    cc.increment(&nc);
    cc.increment(&nc);
    store_c.put("counter".into(), CrdtValue::Counter(cc));

    let mut sc = OrSet::new();
    sc.add("charlie".into(), &nc);
    store_c.put("users".into(), CrdtValue::Set(sc));

    // Full state sync: C receives from A (which already has B's state).
    let clone_a = store_a.clone();
    for key in clone_a.keys() {
        if let Some(val) = clone_a.get(key) {
            store_c.merge_value(key.clone(), val).unwrap();
        }
    }

    // And A,B receive C's state.
    let clone_c = store_c.clone();
    for key in clone_c.keys() {
        if let Some(val) = clone_c.get(key) {
            store_a.merge_value(key.clone(), val).unwrap();
            store_b.merge_value(key.clone(), val).unwrap();
        }
    }

    // All three should converge.
    // counter = 2 + 1 + 3 = 6
    for store in [&store_a, &store_b, &store_c] {
        match store.get("counter") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 6),
            other => panic!("expected Counter, got {:?}", other),
        }
        match store.get("users") {
            Some(CrdtValue::Set(s)) => {
                assert_eq!(s.len(), 3);
                assert!(s.contains(&"alice".to_string()));
                assert!(s.contains(&"bob".to_string()));
                assert!(s.contains(&"charlie".to_string()));
            }
            other => panic!("expected Set, got {:?}", other),
        }
    }

    assert!(stores_equal(&store_a, &store_b));
    assert!(stores_equal(&store_b, &store_c));
}

// ---------------------------------------------------------------
// Scenario 4: Large-scale convergence (1000+ keys)
// ---------------------------------------------------------------

#[test]
#[ignore] // Long-running benchmark test.
fn large_scale_convergence_1000_keys() {
    let nodes: Vec<NodeId> = (0..3).map(|i| node(&format!("node-{}", i))).collect();
    let mut stores: Vec<Store> = (0..3).map(|_| Store::new()).collect();
    let keys_per_node = 400; // 400 * 3 = 1200 keys total

    // Each node writes a distinct set of counter keys.
    for (i, store) in stores.iter_mut().enumerate() {
        for k in 0..keys_per_node {
            let key = format!("key/{}/{}", i, k);
            let mut counter = PnCounter::new();
            counter.increment(&nodes[i]);
            store.put(key, CrdtValue::Counter(counter));
        }
    }

    full_mesh_merge(&mut stores);

    // All nodes should have 1200 keys with value 1 each.
    for (i, store) in stores.iter().enumerate() {
        assert_eq!(
            store.len(),
            keys_per_node * 3,
            "node {} should have {} keys",
            i,
            keys_per_node * 3
        );
    }
    assert!(stores_equal(&stores[0], &stores[1]));
    assert!(stores_equal(&stores[1], &stores[2]));
}

#[test]
#[ignore] // Long-running benchmark test.
fn large_scale_concurrent_writes_same_keys() {
    let nodes: Vec<NodeId> = (0..3).map(|i| node(&format!("node-{}", i))).collect();
    let mut stores: Vec<Store> = (0..3).map(|_| Store::new()).collect();
    let num_keys = 1000;

    // All nodes write to the same 1000 keys concurrently.
    for (i, store) in stores.iter_mut().enumerate() {
        for k in 0..num_keys {
            let key = format!("shared/{}", k);
            let mut counter = PnCounter::new();
            for _ in 0..(i + 1) {
                counter.increment(&nodes[i]);
            }
            store.put(key, CrdtValue::Counter(counter));
        }
    }

    full_mesh_merge(&mut stores);

    // Each key should have counter = 1 + 2 + 3 = 6.
    for (i, store) in stores.iter().enumerate() {
        assert_eq!(
            store.len(),
            num_keys,
            "node {} should have {} keys",
            i,
            num_keys
        );
        for k in 0..num_keys {
            let key = format!("shared/{}", k);
            match store.get(&key) {
                Some(CrdtValue::Counter(c)) => {
                    assert_eq!(c.value(), 6, "node {} key {} should be 6", i, k);
                }
                other => panic!("node {} key {}: expected Counter, got {:?}", i, k, other),
            }
        }
    }
    assert!(stores_equal(&stores[0], &stores[1]));
    assert!(stores_equal(&stores[1], &stores[2]));
}

// ---------------------------------------------------------------
// Scenario 5: Mixed CRDT types in a single Store
// ---------------------------------------------------------------

#[test]
fn mixed_crdt_types_convergence() {
    let na = node("node-a");
    let nb = node("node-b");
    let nc = node("node-c");

    let mut store_a = Store::new();
    let mut store_b = Store::new();
    let mut store_c = Store::new();

    // Node A: counter + set.
    let mut ca = PnCounter::new();
    ca.increment(&na);
    store_a.put("counter".into(), CrdtValue::Counter(ca));

    let mut sa = OrSet::new();
    sa.add("x".into(), &na);
    store_a.put("set".into(), CrdtValue::Set(sa));

    // Node B: counter + map.
    let mut cb = PnCounter::new();
    cb.increment(&nb);
    cb.increment(&nb);
    store_b.put("counter".into(), CrdtValue::Counter(cb));

    let mut mb = OrMap::new();
    mb.set("k1".into(), "v1".into(), ts(100, 0, "node-b"), &nb);
    store_b.put("map".into(), CrdtValue::Map(mb));

    // Node C: counter + register + set.
    let mut cc = PnCounter::new();
    cc.increment(&nc);
    cc.decrement(&nc);
    store_c.put("counter".into(), CrdtValue::Counter(cc));

    let mut rc = LwwRegister::new();
    rc.set("world".into(), ts(200, 0, "node-c"));
    store_c.put("register".into(), CrdtValue::Register(rc));

    let mut sc = OrSet::new();
    sc.add("y".into(), &nc);
    store_c.put("set".into(), CrdtValue::Set(sc));

    let mut stores = [store_a, store_b, store_c];
    full_mesh_merge(&mut stores);

    // All stores should have 4 keys: counter, set, map, register.
    for (i, store) in stores.iter().enumerate() {
        assert_eq!(store.len(), 4, "node {} should have 4 keys", i);
    }

    // counter: 1 + 2 + (1-1) = 3
    for store in &stores {
        match store.get("counter") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 3),
            other => panic!("expected Counter, got {:?}", other),
        }
    }

    // set: {"x", "y"}
    for store in &stores {
        match store.get("set") {
            Some(CrdtValue::Set(s)) => {
                assert_eq!(s.len(), 2);
                assert!(s.contains(&"x".to_string()));
                assert!(s.contains(&"y".to_string()));
            }
            other => panic!("expected Set, got {:?}", other),
        }
    }

    // map: {"k1": "v1"}
    for store in &stores {
        match store.get("map") {
            Some(CrdtValue::Map(m)) => {
                assert_eq!(m.get(&"k1".to_string()), Some(&"v1".to_string()));
            }
            other => panic!("expected Map, got {:?}", other),
        }
    }

    // register: "world"
    for store in &stores {
        match store.get("register") {
            Some(CrdtValue::Register(r)) => {
                assert_eq!(r.get(), Some(&"world".to_string()));
            }
            other => panic!("expected Register, got {:?}", other),
        }
    }

    assert!(stores_equal(&stores[0], &stores[1]));
    assert!(stores_equal(&stores[1], &stores[2]));
}

// ---------------------------------------------------------------
// Scenario 6: End-to-end EventualApi workflow
// ---------------------------------------------------------------

#[test]
fn eventual_api_write_merge_read_end_to_end() {
    let mut api_a = EventualApi::new(node("node-a"));
    let mut api_b = EventualApi::new(node("node-b"));
    let mut api_c = EventualApi::new(node("node-c"));

    // Node A: increment counter and add to set.
    api_a.eventual_counter_inc("visits").unwrap();
    api_a.eventual_counter_inc("visits").unwrap();
    api_a.eventual_set_add("tags", "rust".into()).unwrap();

    // Node B: increment counter and add to set.
    api_b.eventual_counter_inc("visits").unwrap();
    api_b
        .eventual_set_add("tags", "distributed".into())
        .unwrap();

    // Node C: increment counter, set register.
    api_c.eventual_counter_inc("visits").unwrap();
    api_c.eventual_counter_inc("visits").unwrap();
    api_c.eventual_counter_inc("visits").unwrap();
    api_c
        .eventual_register_set("status", "online".into())
        .unwrap();

    // Simulate replication via merge_remote.
    // Gather all states from each node.
    let keys_a: Vec<String> = api_a.keys().iter().map(|s| s.to_string()).collect();
    let keys_b: Vec<String> = api_b.keys().iter().map(|s| s.to_string()).collect();
    let keys_c: Vec<String> = api_c.keys().iter().map(|s| s.to_string()).collect();

    // Helper: get snapshot of values from an api.
    let snapshot_a: Vec<(String, CrdtValue)> = keys_a
        .iter()
        .filter_map(|k| api_a.get_eventual(k).map(|v| (k.clone(), v.clone())))
        .collect();
    let snapshot_b: Vec<(String, CrdtValue)> = keys_b
        .iter()
        .filter_map(|k| api_b.get_eventual(k).map(|v| (k.clone(), v.clone())))
        .collect();
    let snapshot_c: Vec<(String, CrdtValue)> = keys_c
        .iter()
        .filter_map(|k| api_c.get_eventual(k).map(|v| (k.clone(), v.clone())))
        .collect();

    // Cross-merge all pairs.
    for (key, val) in &snapshot_b {
        api_a.merge_remote(key.clone(), val).unwrap();
    }
    for (key, val) in &snapshot_c {
        api_a.merge_remote(key.clone(), val).unwrap();
    }
    for (key, val) in &snapshot_a {
        api_b.merge_remote(key.clone(), val).unwrap();
    }
    for (key, val) in &snapshot_c {
        api_b.merge_remote(key.clone(), val).unwrap();
    }
    for (key, val) in &snapshot_a {
        api_c.merge_remote(key.clone(), val).unwrap();
    }
    for (key, val) in &snapshot_b {
        api_c.merge_remote(key.clone(), val).unwrap();
    }

    // Verify convergence: visits = 2 + 1 + 3 = 6.
    for (name, api) in [("A", &api_a), ("B", &api_b), ("C", &api_c)] {
        match api.get_eventual("visits") {
            Some(CrdtValue::Counter(c)) => {
                assert_eq!(c.value(), 6, "node {} visits should be 6", name);
            }
            other => panic!(
                "node {}: expected Counter for visits, got {:?}",
                name, other
            ),
        }

        // tags: {"rust", "distributed"}
        match api.get_eventual("tags") {
            Some(CrdtValue::Set(s)) => {
                assert!(
                    s.contains(&"rust".to_string()),
                    "node {} missing tag 'rust'",
                    name
                );
                assert!(
                    s.contains(&"distributed".to_string()),
                    "node {} missing tag 'distributed'",
                    name
                );
            }
            other => panic!("node {}: expected Set for tags, got {:?}", name, other),
        }

        // status register: "online"
        match api.get_eventual("status") {
            Some(CrdtValue::Register(r)) => {
                assert_eq!(
                    r.get(),
                    Some(&"online".to_string()),
                    "node {} status should be 'online'",
                    name
                );
            }
            other => panic!(
                "node {}: expected Register for status, got {:?}",
                name, other
            ),
        }
    }
}

#[test]
fn eventual_api_map_operations_converge() {
    let mut api_a = EventualApi::new(node("node-a"));
    let mut api_b = EventualApi::new(node("node-b"));

    // Both nodes write to the same map key.
    api_a
        .eventual_map_set("config", "db_host".into(), "10.0.0.1".into())
        .unwrap();
    api_a
        .eventual_map_set("config", "db_port".into(), "5432".into())
        .unwrap();

    api_b
        .eventual_map_set("config", "cache_host".into(), "10.0.0.2".into())
        .unwrap();

    // Snapshot and merge.
    let val_a = api_a.get_eventual("config").unwrap().clone();
    let val_b = api_b.get_eventual("config").unwrap().clone();

    api_a.merge_remote("config".into(), &val_b).unwrap();
    api_b.merge_remote("config".into(), &val_a).unwrap();

    // Both should see all three keys.
    for (name, api) in [("A", &api_a), ("B", &api_b)] {
        match api.get_eventual("config") {
            Some(CrdtValue::Map(m)) => {
                assert_eq!(m.len(), 3, "node {} should see 3 map keys", name);
                assert_eq!(m.get(&"db_host".to_string()), Some(&"10.0.0.1".to_string()));
                assert_eq!(m.get(&"db_port".to_string()), Some(&"5432".to_string()));
                assert_eq!(
                    m.get(&"cache_host".to_string()),
                    Some(&"10.0.0.2".to_string())
                );
            }
            other => panic!("node {}: expected Map, got {:?}", name, other),
        }
    }
}

// ---------------------------------------------------------------
// Scenario 7: CRDT mathematical properties
// ---------------------------------------------------------------

// --- 7a. Commutativity: merge(A,B) == merge(B,A) ---

#[test]
fn commutativity_pn_counter() {
    let na = node("node-a");
    let nb = node("node-b");

    let mut a = PnCounter::new();
    a.increment(&na);
    a.increment(&na);
    a.decrement(&na);

    let mut b = PnCounter::new();
    b.increment(&nb);
    b.increment(&nb);
    b.increment(&nb);
    b.decrement(&nb);

    let mut ab = a.clone();
    ab.merge(&b);

    let mut ba = b.clone();
    ba.merge(&a);

    assert_eq!(
        ab.value(),
        ba.value(),
        "PnCounter merge must be commutative"
    );
}

#[test]
fn commutativity_or_set() {
    let na = node("node-a");
    let nb = node("node-b");

    let mut a: OrSet<String> = OrSet::new();
    a.add("x".into(), &na);
    a.add("y".into(), &na);

    let mut b: OrSet<String> = OrSet::new();
    b.add("y".into(), &nb);
    b.add("z".into(), &nb);

    let mut ab = a.clone();
    ab.merge(&b);

    let mut ba = b.clone();
    ba.merge(&a);

    assert_eq!(
        ab.elements(),
        ba.elements(),
        "OrSet merge must be commutative"
    );
}

#[test]
fn commutativity_or_map() {
    let na = node("node-a");
    let nb = node("node-b");

    let mut a: OrMap<String, String> = OrMap::new();
    a.set("k1".into(), "v1".into(), ts(100, 0, "node-a"), &na);

    let mut b: OrMap<String, String> = OrMap::new();
    b.set("k1".into(), "v2".into(), ts(200, 0, "node-b"), &nb);
    b.set("k2".into(), "v3".into(), ts(200, 1, "node-b"), &nb);

    let mut ab = a.clone();
    ab.merge(&b);

    let mut ba = b.clone();
    ba.merge(&a);

    // Same keys.
    let mut keys_ab: Vec<&String> = ab.keys();
    let mut keys_ba: Vec<&String> = ba.keys();
    keys_ab.sort();
    keys_ba.sort();
    assert_eq!(keys_ab, keys_ba, "OrMap merge keys must be commutative");

    // Same values.
    for k in &keys_ab {
        assert_eq!(
            ab.get(k),
            ba.get(k),
            "OrMap merge values must be commutative for key {:?}",
            k
        );
    }
}

#[test]
fn commutativity_lww_register() {
    let mut a: LwwRegister<String> = LwwRegister::new();
    a.set("old".into(), ts(100, 0, "node-a"));

    let mut b: LwwRegister<String> = LwwRegister::new();
    b.set("new".into(), ts(200, 0, "node-b"));

    let mut ab = a.clone();
    ab.merge(&b);

    let mut ba = b.clone();
    ba.merge(&a);

    assert_eq!(ab.get(), ba.get(), "LwwRegister merge must be commutative");
}

#[test]
fn commutativity_store_level() {
    let na = node("node-a");
    let nb = node("node-b");

    let mut store_a = Store::new();
    let mut ca = PnCounter::new();
    ca.increment(&na);
    store_a.put("c".into(), CrdtValue::Counter(ca));

    let mut sa = OrSet::new();
    sa.add("x".into(), &na);
    store_a.put("s".into(), CrdtValue::Set(sa));

    let mut store_b = Store::new();
    let mut cb = PnCounter::new();
    cb.increment(&nb);
    store_b.put("c".into(), CrdtValue::Counter(cb));

    let mut sb = OrSet::new();
    sb.add("y".into(), &nb);
    store_b.put("s".into(), CrdtValue::Set(sb));

    // merge(A, B)
    let mut ab = store_a.clone();
    for key in store_b.keys() {
        if let Some(val) = store_b.get(key) {
            ab.merge_value(key.clone(), val).unwrap();
        }
    }

    // merge(B, A)
    let mut ba = store_b.clone();
    for key in store_a.keys() {
        if let Some(val) = store_a.get(key) {
            ba.merge_value(key.clone(), val).unwrap();
        }
    }

    assert!(
        stores_equal(&ab, &ba),
        "Store-level merge must be commutative"
    );
}

// --- 7b. Associativity: merge(merge(A,B),C) == merge(A,merge(B,C)) ---

#[test]
fn associativity_pn_counter() {
    let na = node("node-a");
    let nb = node("node-b");
    let nc = node("node-c");

    let mut a = PnCounter::new();
    a.increment(&na);
    a.increment(&na);

    let mut b = PnCounter::new();
    b.increment(&nb);
    b.decrement(&nb);

    let mut c = PnCounter::new();
    c.increment(&nc);
    c.increment(&nc);
    c.increment(&nc);

    // (A merge B) merge C
    let mut ab_c = a.clone();
    ab_c.merge(&b);
    ab_c.merge(&c);

    // A merge (B merge C)
    let mut bc = b.clone();
    bc.merge(&c);
    let mut a_bc = a.clone();
    a_bc.merge(&bc);

    assert_eq!(
        ab_c.value(),
        a_bc.value(),
        "PnCounter merge must be associative"
    );
}

#[test]
fn associativity_or_set() {
    let na = node("node-a");
    let nb = node("node-b");
    let nc = node("node-c");

    let mut a: OrSet<String> = OrSet::new();
    a.add("1".into(), &na);

    let mut b: OrSet<String> = OrSet::new();
    b.add("2".into(), &nb);

    let mut c: OrSet<String> = OrSet::new();
    c.add("3".into(), &nc);

    let mut ab_c = a.clone();
    ab_c.merge(&b);
    ab_c.merge(&c);

    let mut bc = b.clone();
    bc.merge(&c);
    let mut a_bc = a.clone();
    a_bc.merge(&bc);

    assert_eq!(
        ab_c.elements(),
        a_bc.elements(),
        "OrSet merge must be associative"
    );
}

#[test]
fn associativity_or_map() {
    let na = node("node-a");
    let nb = node("node-b");
    let nc = node("node-c");

    let mut a: OrMap<String, String> = OrMap::new();
    a.set("k".into(), "va".into(), ts(100, 0, "node-a"), &na);

    let mut b: OrMap<String, String> = OrMap::new();
    b.set("k".into(), "vb".into(), ts(200, 0, "node-b"), &nb);

    let mut c: OrMap<String, String> = OrMap::new();
    c.set("k".into(), "vc".into(), ts(300, 0, "node-c"), &nc);
    c.set("k2".into(), "vc2".into(), ts(300, 1, "node-c"), &nc);

    // (A merge B) merge C
    let mut ab_c = a.clone();
    ab_c.merge(&b);
    ab_c.merge(&c);

    // A merge (B merge C)
    let mut bc = b.clone();
    bc.merge(&c);
    let mut a_bc = a.clone();
    a_bc.merge(&bc);

    let mut keys_ab_c: Vec<&String> = ab_c.keys();
    let mut keys_a_bc: Vec<&String> = a_bc.keys();
    keys_ab_c.sort();
    keys_a_bc.sort();
    assert_eq!(keys_ab_c, keys_a_bc, "OrMap merge keys must be associative");
    for k in &keys_ab_c {
        assert_eq!(
            ab_c.get(k),
            a_bc.get(k),
            "OrMap merge values must be associative for key {:?}",
            k
        );
    }
}

#[test]
fn associativity_lww_register() {
    let mut a: LwwRegister<String> = LwwRegister::new();
    a.set("va".into(), ts(100, 0, "node-a"));

    let mut b: LwwRegister<String> = LwwRegister::new();
    b.set("vb".into(), ts(200, 0, "node-b"));

    let mut c: LwwRegister<String> = LwwRegister::new();
    c.set("vc".into(), ts(300, 0, "node-c"));

    let mut ab_c = a.clone();
    ab_c.merge(&b);
    ab_c.merge(&c);

    let mut bc = b.clone();
    bc.merge(&c);
    let mut a_bc = a.clone();
    a_bc.merge(&bc);

    assert_eq!(
        ab_c.get(),
        a_bc.get(),
        "LwwRegister merge must be associative"
    );
}

#[test]
fn associativity_store_level() {
    let na = node("node-a");
    let nb = node("node-b");
    let nc = node("node-c");

    let mut sa = Store::new();
    let mut ca = PnCounter::new();
    ca.increment(&na);
    sa.put("c".into(), CrdtValue::Counter(ca));

    let mut sb = Store::new();
    let mut cb = PnCounter::new();
    cb.increment(&nb);
    sb.put("c".into(), CrdtValue::Counter(cb));

    let mut sc = Store::new();
    let mut cc = PnCounter::new();
    cc.increment(&nc);
    sc.put("c".into(), CrdtValue::Counter(cc));

    // (A merge B) merge C
    let mut ab_c = sa.clone();
    for key in sb.keys() {
        if let Some(val) = sb.get(key) {
            ab_c.merge_value(key.clone(), val).unwrap();
        }
    }
    for key in sc.keys() {
        if let Some(val) = sc.get(key) {
            ab_c.merge_value(key.clone(), val).unwrap();
        }
    }

    // A merge (B merge C)
    let mut bc = sb.clone();
    for key in sc.keys() {
        if let Some(val) = sc.get(key) {
            bc.merge_value(key.clone(), val).unwrap();
        }
    }
    let mut a_bc = sa.clone();
    for key in bc.keys() {
        if let Some(val) = bc.get(key) {
            a_bc.merge_value(key.clone(), val).unwrap();
        }
    }

    assert!(
        stores_equal(&ab_c, &a_bc),
        "Store-level merge must be associative"
    );
}

// --- 7c. Idempotency: merge(A,A) == A ---

#[test]
fn idempotency_pn_counter() {
    let na = node("node-a");

    let mut a = PnCounter::new();
    a.increment(&na);
    a.increment(&na);
    a.decrement(&na);

    let before = a.value();
    let snapshot = a.clone();
    a.merge(&snapshot);
    assert_eq!(a.value(), before, "PnCounter merge must be idempotent");
}

#[test]
fn idempotency_or_set() {
    let na = node("node-a");

    let mut a: OrSet<String> = OrSet::new();
    a.add("x".into(), &na);
    a.add("y".into(), &na);

    let before: HashSet<&String> = a.elements();
    let before_owned: HashSet<String> = before.into_iter().cloned().collect();
    let snapshot = a.clone();
    a.merge(&snapshot);

    let after: HashSet<String> = a.elements().into_iter().cloned().collect();
    assert_eq!(after, before_owned, "OrSet merge must be idempotent");
}

#[test]
fn idempotency_or_map() {
    let na = node("node-a");

    let mut a: OrMap<String, String> = OrMap::new();
    a.set("k1".into(), "v1".into(), ts(100, 0, "node-a"), &na);
    a.set("k2".into(), "v2".into(), ts(200, 0, "node-a"), &na);

    let before_keys: Vec<String> = a.keys().into_iter().cloned().collect();
    let before_vals: Vec<Option<String>> = before_keys.iter().map(|k| a.get(k).cloned()).collect();

    let snapshot = a.clone();
    a.merge(&snapshot);

    let after_keys: Vec<String> = a.keys().into_iter().cloned().collect();
    let after_vals: Vec<Option<String>> = after_keys.iter().map(|k| a.get(k).cloned()).collect();

    let mut bk = before_keys.clone();
    let mut ak = after_keys.clone();
    bk.sort();
    ak.sort();
    assert_eq!(bk, ak, "OrMap keys must be idempotent after self-merge");
    assert_eq!(
        before_vals, after_vals,
        "OrMap values must be idempotent after self-merge"
    );
}

#[test]
fn idempotency_lww_register() {
    let mut a: LwwRegister<String> = LwwRegister::new();
    a.set("hello".into(), ts(100, 0, "node-a"));

    let before = a.get().cloned();
    let snapshot = a.clone();
    a.merge(&snapshot);

    assert_eq!(
        a.get().cloned(),
        before,
        "LwwRegister merge must be idempotent"
    );
}

#[test]
fn idempotency_store_level() {
    let na = node("node-a");

    let mut store = Store::new();
    let mut ca = PnCounter::new();
    ca.increment(&na);
    store.put("c".into(), CrdtValue::Counter(ca));

    let mut sa = OrSet::new();
    sa.add("x".into(), &na);
    store.put("s".into(), CrdtValue::Set(sa));

    let snapshot = store.clone();

    // Self-merge: merge(A, A)
    for key in snapshot.keys() {
        if let Some(val) = snapshot.get(key) {
            store.merge_value(key.clone(), val).unwrap();
        }
    }

    assert!(
        stores_equal(&store, &snapshot),
        "Store-level merge must be idempotent"
    );
}

// ---------------------------------------------------------------
// Additional convergence scenarios
// ---------------------------------------------------------------

#[test]
fn concurrent_increment_and_decrement_convergence() {
    let na = node("node-a");
    let nb = node("node-b");

    let mut store_a = Store::new();
    let mut store_b = Store::new();

    // Node A only increments.
    let mut ca = PnCounter::new();
    for _ in 0..10 {
        ca.increment(&na);
    }
    store_a.put("score".into(), CrdtValue::Counter(ca));

    // Node B increments and decrements.
    let mut cb = PnCounter::new();
    for _ in 0..5 {
        cb.increment(&nb);
    }
    for _ in 0..3 {
        cb.decrement(&nb);
    }
    store_b.put("score".into(), CrdtValue::Counter(cb));

    let mut stores = [store_a, store_b];
    full_mesh_merge(&mut stores);

    // Expected: 10 + (5-3) = 12
    for store in &stores {
        match store.get("score") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 12),
            other => panic!("expected Counter, got {:?}", other),
        }
    }
}

#[test]
fn or_set_add_wins_across_nodes() {
    let na = node("node-a");

    // Common state: both nodes have {"x"}.
    let mut common = OrSet::new();
    common.add("x".into(), &na);

    let mut store_a = Store::new();
    store_a.put("items".into(), CrdtValue::Set(common.clone()));

    let mut store_b = Store::new();
    store_b.put("items".into(), CrdtValue::Set(common));

    // Node A concurrently re-adds "x" (new dot).
    if let Some(CrdtValue::Set(s)) = store_a.get_mut("items") {
        s.add("x".into(), &na);
    }

    // Node B concurrently removes "x".
    if let Some(CrdtValue::Set(s)) = store_b.get_mut("items") {
        s.remove(&"x".to_string());
    }

    let mut stores = [store_a, store_b];
    full_mesh_merge(&mut stores);

    // Add-wins: "x" should be present on both nodes.
    for (i, store) in stores.iter().enumerate() {
        match store.get("items") {
            Some(CrdtValue::Set(s)) => {
                assert!(
                    s.contains(&"x".to_string()),
                    "node {} should have 'x' (add-wins)",
                    i
                );
            }
            other => panic!("node {}: expected Set, got {:?}", i, other),
        }
    }
}

#[test]
fn or_map_concurrent_set_delete_add_wins() {
    let na = node("node-a");

    // Common state.
    let mut common: OrMap<String, String> = OrMap::new();
    common.set("k".into(), "v1".into(), ts(100, 0, "node-a"), &na);

    let mut store_a = Store::new();
    store_a.put("data".into(), CrdtValue::Map(common.clone()));

    let mut store_b = Store::new();
    store_b.put("data".into(), CrdtValue::Map(common));

    // Node A concurrently sets a new value for "k".
    if let Some(CrdtValue::Map(m)) = store_a.get_mut("data") {
        m.set("k".into(), "v2".into(), ts(200, 0, "node-a"), &na);
    }

    // Node B concurrently deletes "k".
    if let Some(CrdtValue::Map(m)) = store_b.get_mut("data") {
        m.delete(&"k".to_string());
    }

    let mut stores = [store_a, store_b];
    full_mesh_merge(&mut stores);

    // Add-wins: "k" should be present with "v2".
    for (i, store) in stores.iter().enumerate() {
        match store.get("data") {
            Some(CrdtValue::Map(m)) => {
                assert!(
                    m.contains_key(&"k".to_string()),
                    "node {} should have key 'k' (add-wins)",
                    i
                );
                assert_eq!(
                    m.get(&"k".to_string()),
                    Some(&"v2".to_string()),
                    "node {} should see v2",
                    i
                );
            }
            other => panic!("node {}: expected Map, got {:?}", i, other),
        }
    }
}

#[test]
fn merge_order_independence_three_nodes() {
    let na = node("node-a");
    let nb = node("node-b");
    let nc = node("node-c");

    let mut sa = Store::new();
    let mut ca = PnCounter::new();
    ca.increment(&na);
    sa.put("x".into(), CrdtValue::Counter(ca));

    let mut sb = Store::new();
    let mut cb = PnCounter::new();
    cb.increment(&nb);
    cb.increment(&nb);
    sb.put("x".into(), CrdtValue::Counter(cb));

    let mut sc = Store::new();
    let mut cc = PnCounter::new();
    cc.increment(&nc);
    cc.increment(&nc);
    cc.increment(&nc);
    sc.put("x".into(), CrdtValue::Counter(cc));

    // Merge in different orders: ABC, BCA, CAB, ACB, BAC, CBA.
    let merge_into = |base: &Store, others: &[&Store]| -> Store {
        let mut result = base.clone();
        for other in others {
            for key in other.keys() {
                if let Some(val) = other.get(key) {
                    let _ = result.merge_value(key.clone(), val);
                }
            }
        }
        result
    };

    let abc = merge_into(&sa, &[&sb, &sc]);
    let bca = merge_into(&sb, &[&sc, &sa]);
    let cab = merge_into(&sc, &[&sa, &sb]);
    let acb = merge_into(&sa, &[&sc, &sb]);
    let bac = merge_into(&sb, &[&sa, &sc]);
    let cba = merge_into(&sc, &[&sb, &sa]);

    // All orderings should produce the same result.
    assert!(stores_equal(&abc, &bca));
    assert!(stores_equal(&bca, &cab));
    assert!(stores_equal(&cab, &acb));
    assert!(stores_equal(&acb, &bac));
    assert!(stores_equal(&bac, &cba));

    // Value should be 1 + 2 + 3 = 6.
    match abc.get("x") {
        Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 6),
        other => panic!("expected Counter, got {:?}", other),
    }
}

#[test]
fn repeated_merge_convergence() {
    let na = node("node-a");
    let nb = node("node-b");

    let mut store_a = Store::new();
    let mut store_b = Store::new();

    let mut ca = PnCounter::new();
    ca.increment(&na);
    store_a.put("c".into(), CrdtValue::Counter(ca));

    let mut cb = PnCounter::new();
    cb.increment(&nb);
    store_b.put("c".into(), CrdtValue::Counter(cb));

    // Merge multiple times — should be stable after first merge.
    for _ in 0..5 {
        let clone_b = store_b.clone();
        for key in clone_b.keys() {
            if let Some(val) = clone_b.get(key) {
                store_a.merge_value(key.clone(), val).unwrap();
            }
        }
        let clone_a = store_a.clone();
        for key in clone_a.keys() {
            if let Some(val) = clone_a.get(key) {
                store_b.merge_value(key.clone(), val).unwrap();
            }
        }
    }

    assert!(stores_equal(&store_a, &store_b));
    match store_a.get("c") {
        Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 2),
        other => panic!("expected Counter, got {:?}", other),
    }
}

#[test]
fn empty_store_merge_with_populated() {
    let na = node("node-a");

    let mut populated = Store::new();
    let mut ca = PnCounter::new();
    ca.increment(&na);
    populated.put("c".into(), CrdtValue::Counter(ca));

    let mut sa = OrSet::new();
    sa.add("x".into(), &na);
    populated.put("s".into(), CrdtValue::Set(sa));

    let mut empty = Store::new();

    // Merge populated into empty.
    for key in populated.keys() {
        if let Some(val) = populated.get(key) {
            empty.merge_value(key.clone(), val).unwrap();
        }
    }

    assert!(stores_equal(&empty, &populated));
}

#[test]
fn disjoint_key_sets_merge() {
    let na = node("node-a");
    let nb = node("node-b");

    let mut store_a = Store::new();
    let mut ca = PnCounter::new();
    ca.increment(&na);
    store_a.put("key-a".into(), CrdtValue::Counter(ca));

    let mut store_b = Store::new();
    let mut cb = PnCounter::new();
    cb.increment(&nb);
    store_b.put("key-b".into(), CrdtValue::Counter(cb));

    let mut stores = [store_a, store_b];
    full_mesh_merge(&mut stores);

    for store in &stores {
        assert_eq!(store.len(), 2);
        assert!(store.contains_key("key-a"));
        assert!(store.contains_key("key-b"));
    }
    assert!(stores_equal(&stores[0], &stores[1]));
}
