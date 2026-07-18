//! Property-based tests for CRDT convergence guarantees.
//!
//! Verifies commutativity, associativity, and idempotency of merge for
//! all CRDT types: PnCounter, OrSet, OrMap, LwwRegister.

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
    // (node_index, ops): up to 5 operations on up to 3 nodes
    prop::collection::vec((0..3u8, prop::bool::ANY), 0..8).prop_map(|ops| {
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
    // (node_index, element_index, is_add)
    prop::collection::vec((0..2u8, 0..4u8, prop::bool::ANY), 0..8).prop_map(|ops| {
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

/// Generate an OrMap with random put/delete of string key-value pairs.
fn arb_or_map() -> impl Strategy<Value = OrMap<String, String>> {
    // (node_index, key_index, value_index, ts_physical, is_set)
    prop::collection::vec((0..2u8, 0..3u8, 0..3u8, 1..100u64, prop::bool::ANY), 0..6).prop_map(
        |ops| {
            let nodes = [node("n0"), node("n1")];
            let keys: Vec<String> = (0..3).map(|i| format!("key{i}")).collect();
            let values: Vec<String> = (0..3).map(|i| format!("val{i}")).collect();
            let mut map = OrMap::new();
            // Use incrementing physical timestamps to ensure LWW progresses
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
    // (physical, logical, node_id_suffix, value)
    (1..1000u64, 0..10u32, 0..3u8, "[a-z]{1,5}").prop_map(
        |(physical, logical, node_suffix, value)| {
            let mut reg = LwwRegister::new();
            reg.set(value, ts(physical, logical, &format!("n{node_suffix}")));
            reg
        },
    )
}

// ---------------------------------------------------------------
// PnCounter properties
// ---------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

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
}

// ---------------------------------------------------------------
// OrSet properties
// ---------------------------------------------------------------

/// Collect sorted elements from an OrSet for comparison.
fn or_set_sorted(set: &OrSet<String>) -> Vec<String> {
    let mut elems: Vec<String> = set.elements().into_iter().cloned().collect();
    elems.sort();
    elems
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

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
}

// ---------------------------------------------------------------
// OrMap properties
// ---------------------------------------------------------------

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

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn or_map_commutativity(a in arb_or_map(), b in arb_or_map()) {
        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        prop_assert_eq!(or_map_sorted(&ab), or_map_sorted(&ba));
    }

    /// OrMap convergence: all replicas converge after sufficient
    /// cross-merge rounds, regardless of initial merge order.
    /// Pure associativity may not hold for OR-Map with deferred
    /// tombstones, but full state exchange convergence does.
    #[test]
    fn or_map_convergence(
        a in arb_or_map(),
        b in arb_or_map(),
        c in arb_or_map()
    ) {
        // Simulate full state exchange: each replica merges all others,
        // then repeat until stable (2 rounds suffices for 3 replicas).
        let mut r1 = a.clone();
        let mut r2 = b.clone();
        let mut r3 = c.clone();

        for _ in 0..3 {
            let snap1 = r1.clone();
            let snap2 = r2.clone();
            let snap3 = r3.clone();

            r1.merge(&snap2);
            r1.merge(&snap3);
            r2.merge(&snap1);
            r2.merge(&snap3);
            r3.merge(&snap1);
            r3.merge(&snap2);
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
}

// ---------------------------------------------------------------
// LwwRegister properties
// ---------------------------------------------------------------

/// Extract (value, timestamp) for comparison.
fn lww_state(reg: &LwwRegister<String>) -> (Option<String>, HlcTimestamp) {
    (reg.get().cloned(), reg.timestamp().clone())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

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
}

// ---------------------------------------------------------------
// Certified sweep / compaction floor properties (M-8)
// ---------------------------------------------------------------

use std::collections::BTreeMap;

use asteroidb_poc::store::digest::compute_store_digest;
use asteroidb_poc::store::kv::CrdtValue;

/// Canonical digest of a single OrSet (scheme v2: live + counters +
/// floor + uncovered deferred). Two sets with equal canonical digests
/// are semantically interchangeable.
fn canonical_digest(set: &OrSet<String>) -> [u8; 32] {
    let entries: BTreeMap<String, CrdtValue> =
        BTreeMap::from([("k".to_string(), CrdtValue::Set(set.clone()))]);
    compute_store_digest(&entries).root
}

fn observable(set: &OrSet<String>) -> Vec<String> {
    let mut v: Vec<String> = set.elements().into_iter().cloned().collect();
    v.sort();
    v
}

/// One step of a replicated execution over three OrSet replicas.
#[derive(Debug, Clone)]
enum ScriptOp {
    Add {
        replica: usize,
        elem: u8,
    },
    Remove {
        replica: usize,
        elem: u8,
    },
    /// Full-state merge src -> dst (what a bucket transfer / full sync does).
    Merge {
        dst: usize,
        src: usize,
    },
    /// UNGATED certified sweep (candidates = all current tombstones).
    /// The floor's standalone safety must not depend on sweep timing —
    /// the runtime gates exist for old (pre-floor) peers and hole-jump,
    /// not for v2 merge semantics.
    Sweep {
        replica: usize,
    },
}

fn arb_script() -> impl Strategy<Value = Vec<ScriptOp>> {
    prop::collection::vec(
        prop_oneof![
            (0..3usize, 0..5u8).prop_map(|(replica, elem)| ScriptOp::Add { replica, elem }),
            (0..3usize, 0..5u8).prop_map(|(replica, elem)| ScriptOp::Remove { replica, elem }),
            (0..3usize, 0..3usize).prop_map(|(dst, src)| ScriptOp::Merge { dst, src }),
            (0..3usize).prop_map(|replica| ScriptOp::Sweep { replica }),
        ],
        0..40,
    )
}

fn elem_name(e: u8) -> String {
    format!("elem{e}")
}

proptest! {
    /// Interleave random adds/removes/full-state merges with UNGATED
    /// certified sweeps on 3 replicas, against an oracle execution that
    /// never sweeps:
    /// (a) the observable state of every replica matches the oracle
    ///     after every step (a sweep is an information-equivalent
    ///     compression, never a semantic change), and
    /// (b) after a full pairwise merge closure, all replicas of the
    ///     swept world converge to one canonical state (equal canonical
    ///     digests) whose observable content equals the oracle's.
    /// This subsumes the M-8 livelock: a merge rolling a sweep back
    /// would diverge from the oracle or break the fixed point.
    #[test]
    fn certified_sweep_is_observably_transparent(script in arb_script()) {
        let nodes = [node("n0"), node("n1"), node("n2")];
        let mut swept: Vec<OrSet<String>> = (0..3).map(|_| OrSet::new()).collect();
        let mut oracle: Vec<OrSet<String>> = (0..3).map(|_| OrSet::new()).collect();

        for op in &script {
            match op {
                ScriptOp::Add { replica, elem } => {
                    swept[*replica].add(elem_name(*elem), &nodes[*replica]);
                    oracle[*replica].add(elem_name(*elem), &nodes[*replica]);
                }
                ScriptOp::Remove { replica, elem } => {
                    swept[*replica].remove(&elem_name(*elem));
                    oracle[*replica].remove(&elem_name(*elem));
                }
                ScriptOp::Merge { dst, src } if dst != src => {
                    let s = swept[*src].clone();
                    swept[*dst].merge(&s);
                    let o = oracle[*src].clone();
                    oracle[*dst].merge(&o);
                }
                ScriptOp::Merge { .. } => {}
                ScriptOp::Sweep { replica } => {
                    let candidates = swept[*replica].deferred_dots();
                    swept[*replica].compact_deferred_certified(&candidates, None);
                }
            }
            for i in 0..3 {
                prop_assert_eq!(
                    observable(&swept[i]),
                    observable(&oracle[i]),
                    "replica {} diverged from the no-GC oracle after {:?}",
                    i,
                    op
                );
            }
        }

        // Full pairwise closure (two rounds reach the global join).
        for _ in 0..2 {
            for dst in 0..3 {
                for src in 0..3 {
                    if dst != src {
                        let s = swept[src].clone();
                        swept[dst].merge(&s);
                        let o = oracle[src].clone();
                        oracle[dst].merge(&o);
                    }
                }
            }
        }
        for i in 0..3 {
            prop_assert_eq!(observable(&swept[i]), observable(&oracle[i]));
        }
        let d0 = canonical_digest(&swept[0]);
        for set in &swept[1..] {
            prop_assert_eq!(
                canonical_digest(set),
                d0,
                "swept replicas must reach one canonical fixed point"
            );
        }
    }

    /// Merge stays commutative / associative / idempotent at the
    /// CANONICAL level with floors in play (floor = pointwise-max join,
    /// covered deferred excluded from the canonical form).
    #[test]
    fn merge_laws_hold_with_floors(
        script_a in arb_script(),
        script_b in arb_script(),
        script_c in arb_script()
    ) {
        let build = |script: &[ScriptOp]| {
            let nodes = [node("n0"), node("n1"), node("n2")];
            let mut replicas: Vec<OrSet<String>> = (0..3).map(|_| OrSet::new()).collect();
            for op in script {
                match op {
                    ScriptOp::Add { replica, elem } => {
                        replicas[*replica].add(elem_name(*elem), &nodes[*replica]);
                    }
                    ScriptOp::Remove { replica, elem } => {
                        replicas[*replica].remove(&elem_name(*elem));
                    }
                    ScriptOp::Merge { dst, src } if dst != src => {
                        let s = replicas[*src].clone();
                        replicas[*dst].merge(&s);
                    }
                    ScriptOp::Merge { .. } => {}
                    ScriptOp::Sweep { replica } => {
                        let candidates = replicas[*replica].deferred_dots();
                        replicas[*replica].compact_deferred_certified(&candidates, None);
                    }
                }
            }
            replicas.swap_remove(0)
        };
        let a = build(&script_a);
        let b = build(&script_b);
        let c = build(&script_c);

        // Commutativity.
        let mut ab = a.clone();
        ab.merge(&b);
        let mut ba = b.clone();
        ba.merge(&a);
        prop_assert_eq!(canonical_digest(&ab), canonical_digest(&ba));

        // Associativity.
        let mut ab_c = ab.clone();
        ab_c.merge(&c);
        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a.clone();
        a_bc.merge(&bc);
        prop_assert_eq!(canonical_digest(&ab_c), canonical_digest(&a_bc));

        // Idempotency.
        let mut aa = a.clone();
        aa.merge(&a);
        prop_assert_eq!(canonical_digest(&aa), canonical_digest(&a));
    }
}

// ---------------------------------------------------------------
// Certified sweep / compaction floor properties — OrMap (M-8)
//
// OrMap::merge and OrMap::compact_deferred_certified are INDEPENDENT
// implementations, not delegations to OrSet (entries are
// (HashSet<Dot>, LwwRegister) pairs, floor kill interacts with LWW
// values, self-only entries have their own kill loop), so the OrSet
// properties above prove nothing about them. Same two properties:
// oracle transparency and canonical merge laws.
// ---------------------------------------------------------------

/// Canonical digest of a single OrMap (scheme v2), via the store digest.
fn canonical_digest_map(map: &OrMap<String, String>) -> [u8; 32] {
    let entries: BTreeMap<String, CrdtValue> =
        BTreeMap::from([("k".to_string(), CrdtValue::Map(map.clone()))]);
    compute_store_digest(&entries).root
}

/// Observable state of an OrMap: sorted (key, value) pairs.
fn observable_map(map: &OrMap<String, String>) -> Vec<(String, String)> {
    let mut v: Vec<(String, String)> = map
        .keys()
        .into_iter()
        .map(|k| {
            (
                k.clone(),
                map.get(k).expect("present key has value").clone(),
            )
        })
        .collect();
    v.sort();
    v
}

/// One step of a replicated execution over three OrMap replicas.
#[derive(Debug, Clone)]
enum MapScriptOp {
    Set {
        replica: usize,
        key: u8,
        val: u8,
    },
    Delete {
        replica: usize,
        key: u8,
    },
    /// Full-state merge src -> dst (bucket transfer / full sync).
    Merge {
        dst: usize,
        src: usize,
    },
    /// UNGATED certified sweep (candidates = all current tombstones);
    /// see the OrSet script for the rationale.
    Sweep {
        replica: usize,
    },
}

fn arb_map_script() -> impl Strategy<Value = Vec<MapScriptOp>> {
    prop::collection::vec(
        prop_oneof![
            (0..3usize, 0..5u8, 0..7u8).prop_map(|(replica, key, val)| MapScriptOp::Set {
                replica,
                key,
                val
            }),
            (0..3usize, 0..5u8).prop_map(|(replica, key)| MapScriptOp::Delete { replica, key }),
            (0..3usize, 0..3usize).prop_map(|(dst, src)| MapScriptOp::Merge { dst, src }),
            (0..3usize).prop_map(|replica| MapScriptOp::Sweep { replica }),
        ],
        0..40,
    )
}

fn key_name(k: u8) -> String {
    format!("key{k}")
}

/// Run a map script. Timestamps come from a strictly monotonic counter
/// (one tick per Set), so LWW resolution is deterministic and identical
/// between the swept execution and the no-GC oracle. `node_prefix`
/// namespaces the writer NodeIds so states built from DIFFERENT scripts
/// never collide on dots (dots are globally unique per (node, counter)
/// in a real system; cross-script reuse would model an unreachable
/// state).
fn run_map_script(
    script: &[MapScriptOp],
    node_prefix: &str,
    sweeps_enabled: bool,
) -> Vec<OrMap<String, String>> {
    let nodes: Vec<NodeId> = (0..3).map(|i| node(&format!("{node_prefix}{i}"))).collect();
    let mut replicas: Vec<OrMap<String, String>> = (0..3).map(|_| OrMap::new()).collect();
    let mut tick: u64 = 0;
    for op in script {
        match op {
            MapScriptOp::Set { replica, key, val } => {
                tick += 1;
                replicas[*replica].set(
                    key_name(*key),
                    format!("val{val}"),
                    ts(1_000 + tick, 0, &nodes[*replica].0),
                    &nodes[*replica],
                );
            }
            MapScriptOp::Delete { replica, key } => {
                replicas[*replica].delete(&key_name(*key));
            }
            MapScriptOp::Merge { dst, src } if dst != src => {
                let s = replicas[*src].clone();
                replicas[*dst].merge(&s);
            }
            MapScriptOp::Merge { .. } => {}
            MapScriptOp::Sweep { replica } => {
                if sweeps_enabled {
                    let candidates = replicas[*replica].deferred_dots();
                    replicas[*replica].compact_deferred_certified(&candidates, None);
                }
            }
        }
    }
    replicas
}

proptest! {
    /// OrMap analog of `certified_sweep_is_observably_transparent`:
    /// random set/delete/full-state merges interleaved with UNGATED
    /// certified sweeps on 3 replicas, against a no-GC oracle running
    /// the SAME script with the SAME timestamps:
    /// (a) every replica's observable (key, value) state matches the
    ///     oracle after every step, and
    /// (b) after a full pairwise merge closure the swept replicas reach
    ///     one canonical fixed point (equal scheme-v2 digests) whose
    ///     observable content equals the oracle's.
    #[test]
    fn ormap_certified_sweep_is_observably_transparent(script in arb_map_script()) {
        let nodes = [node("n0"), node("n1"), node("n2")];
        let mut swept: Vec<OrMap<String, String>> = (0..3).map(|_| OrMap::new()).collect();
        let mut oracle: Vec<OrMap<String, String>> = (0..3).map(|_| OrMap::new()).collect();
        let mut tick: u64 = 0;

        for op in &script {
            match op {
                MapScriptOp::Set { replica, key, val } => {
                    tick += 1;
                    let stamp = ts(1_000 + tick, 0, &nodes[*replica].0);
                    let value = format!("val{val}");
                    swept[*replica].set(key_name(*key), value.clone(), stamp.clone(), &nodes[*replica]);
                    oracle[*replica].set(key_name(*key), value, stamp, &nodes[*replica]);
                }
                MapScriptOp::Delete { replica, key } => {
                    swept[*replica].delete(&key_name(*key));
                    oracle[*replica].delete(&key_name(*key));
                }
                MapScriptOp::Merge { dst, src } if dst != src => {
                    let s = swept[*src].clone();
                    swept[*dst].merge(&s);
                    let o = oracle[*src].clone();
                    oracle[*dst].merge(&o);
                }
                MapScriptOp::Merge { .. } => {}
                MapScriptOp::Sweep { replica } => {
                    let candidates = swept[*replica].deferred_dots();
                    swept[*replica].compact_deferred_certified(&candidates, None);
                }
            }
            for i in 0..3 {
                prop_assert_eq!(
                    observable_map(&swept[i]),
                    observable_map(&oracle[i]),
                    "map replica {} diverged from the no-GC oracle after {:?}",
                    i,
                    op
                );
            }
        }

        // Full pairwise closure (two rounds reach the global join).
        for _ in 0..2 {
            for dst in 0..3 {
                for src in 0..3 {
                    if dst != src {
                        let s = swept[src].clone();
                        swept[dst].merge(&s);
                        let o = oracle[src].clone();
                        oracle[dst].merge(&o);
                    }
                }
            }
        }
        for i in 0..3 {
            prop_assert_eq!(observable_map(&swept[i]), observable_map(&oracle[i]));
        }
        let d0 = canonical_digest_map(&swept[0]);
        for map in &swept[1..] {
            prop_assert_eq!(
                canonical_digest_map(map),
                d0,
                "swept map replicas must reach one canonical fixed point"
            );
        }
    }

    /// OrMap analog of `merge_laws_hold_with_floors`: merge stays
    /// commutative / associative / idempotent at the CANONICAL level with
    /// floors and LWW registers in play.
    #[test]
    fn ormap_merge_laws_hold_with_floors(
        script_a in arb_map_script(),
        script_b in arb_map_script(),
        script_c in arb_map_script()
    ) {
        let a = run_map_script(&script_a, "na", true).swap_remove(0);
        let b = run_map_script(&script_b, "nb", true).swap_remove(0);
        let c = run_map_script(&script_c, "nc", true).swap_remove(0);

        // Commutativity.
        let mut ab = a.clone();
        ab.merge(&b);
        let mut ba = b.clone();
        ba.merge(&a);
        prop_assert_eq!(canonical_digest_map(&ab), canonical_digest_map(&ba));

        // Associativity.
        let mut ab_c = ab.clone();
        ab_c.merge(&c);
        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a.clone();
        a_bc.merge(&bc);
        prop_assert_eq!(canonical_digest_map(&ab_c), canonical_digest_map(&a_bc));

        // Idempotency.
        let mut aa = a.clone();
        aa.merge(&a);
        prop_assert_eq!(canonical_digest_map(&aa), canonical_digest_map(&a));
    }
}

// ---------------------------------------------------------------
// Merge `changed` flag properties (M-6, RR gate)
//
// Ground truth: `changed == (before != after)` over the PHYSICAL state
// (`PartialEq` covers all components), and merge idempotency implies the
// second application of the same input is always a reported no-op.
// ---------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn pn_counter_changed_matches_ground_truth(a in arb_pn_counter(), b in arb_pn_counter()) {
        let mut merged = a.clone();
        let changed = merged.merge(&b);
        prop_assert_eq!(changed, merged != a, "changed must equal pre/post difference");

        // Second merge of the same input must be a reported no-op.
        let snapshot = merged.clone();
        let changed_again = merged.merge(&b);
        prop_assert!(!changed_again, "idempotent re-merge must report no-op");
        prop_assert_eq!(merged, snapshot);
    }

    #[test]
    fn or_set_changed_matches_ground_truth(a in arb_or_set(), b in arb_or_set()) {
        let mut merged = a.clone();
        let fx = merged.merge(&b);
        prop_assert_eq!(fx.changed, merged != a, "changed must equal pre/post difference");

        let snapshot = merged.clone();
        let fx_again = merged.merge(&b);
        prop_assert!(!fx_again.changed, "idempotent re-merge must report no-op");
        prop_assert_eq!(merged, snapshot);
    }

    #[test]
    fn or_map_changed_matches_ground_truth(a in arb_or_map(), b in arb_or_map()) {
        let mut merged = a.clone();
        let fx = merged.merge(&b);
        prop_assert_eq!(fx.changed, merged != a, "changed must equal pre/post difference");

        let snapshot = merged.clone();
        let fx_again = merged.merge(&b);
        prop_assert!(!fx_again.changed, "idempotent re-merge must report no-op");
        prop_assert_eq!(merged, snapshot);
    }

    #[test]
    fn lww_register_changed_matches_ground_truth(
        a in arb_lww_register(),
        b in arb_lww_register()
    ) {
        let mut merged = a.clone();
        let changed = merged.merge(&b);
        prop_assert_eq!(changed, merged != a, "changed must equal pre/post difference");

        let snapshot = merged.clone();
        let changed_again = merged.merge(&b);
        prop_assert!(!changed_again, "idempotent re-merge must report no-op");
        prop_assert_eq!(merged, snapshot);
    }
}
