//! Property-based tests for Store convergence.
//!
//! Generates random sequences of operations on two independent stores,
//! merges them bidirectionally, and asserts both stores end up identical.

use proptest::prelude::*;

use asteroidb_poc::crdt::lww_register::LwwRegister;
use asteroidb_poc::crdt::or_set::OrSet;
use asteroidb_poc::crdt::pn_counter::PnCounter;
use asteroidb_poc::hlc::HlcTimestamp;
use asteroidb_poc::store::kv::{CrdtValue, Store};
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

/// An operation that can be applied to a store.
#[derive(Debug, Clone)]
enum StoreOp {
    Counter {
        key: String,
        node_id: String,
        increments: u8,
        decrements: u8,
    },
    Set {
        key: String,
        node_id: String,
        elements: Vec<String>,
    },
    Register {
        key: String,
        value: String,
        physical: u64,
        logical: u32,
        node_id: String,
    },
}

fn apply_op(store: &mut Store, op: &StoreOp) {
    match op {
        StoreOp::Counter {
            key,
            node_id,
            increments,
            decrements,
        } => {
            let n = node(node_id);
            let mut counter = PnCounter::new();
            for _ in 0..*increments {
                counter.increment(&n);
            }
            for _ in 0..*decrements {
                counter.decrement(&n);
            }
            let _ = store.merge_value(key.clone(), &CrdtValue::Counter(counter));
        }
        StoreOp::Set {
            key,
            node_id,
            elements,
        } => {
            let n = node(node_id);
            let mut set = OrSet::new();
            for elem in elements {
                set.add(elem.clone(), &n);
            }
            let _ = store.merge_value(key.clone(), &CrdtValue::Set(set));
        }
        StoreOp::Register {
            key,
            value,
            physical,
            logical,
            node_id,
        } => {
            let mut reg = LwwRegister::new();
            reg.set(value.clone(), ts(*physical, *logical, node_id));
            let _ = store.merge_value(key.clone(), &CrdtValue::Register(reg));
        }
    }
}

// ---------------------------------------------------------------
// Strategies
// ---------------------------------------------------------------

/// Strategy for generating a StoreOp.
fn arb_store_op(node_id: &'static str) -> impl Strategy<Value = StoreOp> {
    // Use a small set of fixed keys to ensure overlap between stores
    let key_strategy = prop::sample::select(vec![
        "counter/hits".to_string(),
        "counter/views".to_string(),
        "set/users".to_string(),
        "set/tags".to_string(),
        "reg/config".to_string(),
        "reg/status".to_string(),
    ]);

    prop_oneof![
        // Counter ops
        (key_strategy.clone(), 0..5u8, 0..3u8).prop_map(move |(key, inc, dec)| {
            // Only use counter keys for counters
            let k = if key.starts_with("counter/") {
                key
            } else {
                "counter/hits".to_string()
            };
            StoreOp::Counter {
                key: k,
                node_id: node_id.to_string(),
                increments: inc,
                decrements: dec,
            }
        }),
        // Set ops
        (
            key_strategy.clone(),
            prop::collection::vec("[a-z]{1,4}", 0..4)
        )
            .prop_map(move |(key, elems)| {
                let k = if key.starts_with("set/") {
                    key
                } else {
                    "set/users".to_string()
                };
                StoreOp::Set {
                    key: k,
                    node_id: node_id.to_string(),
                    elements: elems,
                }
            }),
        // Register ops
        (key_strategy, 1..500u64, 0..5u32, "[a-z]{1,6}").prop_map(move |(key, phys, log, val)| {
            let k = if key.starts_with("reg/") {
                key
            } else {
                "reg/config".to_string()
            };
            StoreOp::Register {
                key: k,
                value: val,
                physical: phys,
                logical: log,
                node_id: node_id.to_string(),
            }
        }),
    ]
}

/// Extract a comparable snapshot of a store: sorted (key, type_name, observable_value).
fn store_snapshot(store: &Store) -> Vec<(String, String)> {
    let mut keys: Vec<String> = store.keys().into_iter().cloned().collect();
    keys.sort();

    keys.into_iter()
        .map(|k| {
            let val = store.get(&k).unwrap();
            let desc = match val {
                CrdtValue::Counter(c) => format!("Counter({})", c.value()),
                CrdtValue::Set(s) => {
                    let mut elems: Vec<String> = s.elements().into_iter().cloned().collect();
                    elems.sort();
                    format!("Set({elems:?})")
                }
                CrdtValue::Map(m) => {
                    let mut entries: Vec<(String, String)> = m
                        .keys()
                        .into_iter()
                        .filter_map(|mk| m.get(mk).map(|v| (mk.clone(), v.clone())))
                        .collect();
                    entries.sort();
                    format!("Map({entries:?})")
                }
                CrdtValue::Register(r) => {
                    format!("Register({:?})", r.get())
                }
            };
            (k, desc)
        })
        .collect()
}

// ---------------------------------------------------------------
// Property test
// ---------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// Two stores with independent operations, after bidirectional merge,
    /// must converge to identical state.
    #[test]
    fn store_bidirectional_merge_converges(
        ops_a in prop::collection::vec(arb_store_op("node-a"), 1..8),
        ops_b in prop::collection::vec(arb_store_op("node-b"), 1..8),
    ) {
        let mut store_a = Store::new();
        let mut store_b = Store::new();

        // Apply operations independently
        for op in &ops_a {
            apply_op(&mut store_a, op);
        }
        for op in &ops_b {
            apply_op(&mut store_b, op);
        }

        // Bidirectional merge: A <- B, then B <- A
        // We need to merge all entries from B into A and vice versa.
        // Collect B's entries first to avoid borrow conflicts.
        let b_entries: Vec<(String, CrdtValue)> = store_b
            .all_entries()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        for (key, value) in &b_entries {
            let _ = store_a.merge_value(key.clone(), value);
        }

        let a_entries: Vec<(String, CrdtValue)> = store_a
            .all_entries()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        for (key, value) in &a_entries {
            let _ = store_b.merge_value(key.clone(), value);
        }

        // Both stores must now be identical
        let snapshot_a = store_snapshot(&store_a);
        let snapshot_b = store_snapshot(&store_b);

        prop_assert_eq!(
            snapshot_a,
            snapshot_b,
            "Stores must converge after bidirectional merge"
        );
    }
}
