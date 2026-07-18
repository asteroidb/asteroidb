//! Tombstone garbage collection for OR-Set and OR-Map CRDTs.
//!
//! The `deferred` (tombstone) sets in [`crate::crdt::or_set::OrSet`] and
//! [`crate::crdt::or_map::OrMap`] grow unboundedly over time because every
//! remove operation appends dots. This module provides a `TombstoneGc`
//! that periodically reclaims tombstones that are provably safe to
//! discard.
//!
//! # Safety criterion
//!
//! A tombstone dot `(node_id, counter)` is safe to GC when:
//! 1. All known replicas have already incorporated it (they can never
//!    again offer the removed dot as live state in a merge), AND
//! 2. The dot is not referenced by any live element in the CRDT, AND
//! 3. The dot is locally dominated (`counter < max_counter` for its
//!    node), so no future dot can collide with it.
//!
//! Criterion (1) is what a purely local check can NEVER establish: a
//! replica partitioned away for longer than any wall-clock retention
//! window still holds the pre-remove state, and merging it after the
//! tombstone is gone permanently resurrects the removed element.
//!
//! # Gated mark-and-sweep
//!
//! [`TombstoneGc::mark_and_sweep`] therefore runs in two passes:
//!
//! - **Mark**: snapshot the current deferred dots (per store key) and
//!   record the mark's wall-clock time `mark_ms`.
//! - **Sweep** (a later pass, at least `retention_period` after the
//!   mark): the CALLER evaluates its replica-synchronisation gates
//!   against `mark_ms` (see `NodeRunner::run_gc`: every authority's ack
//!   frontier AND every registered peer's push evidence — the local
//!   wall-clock time of the last fully-successful push — must have
//!   passed `mark_ms`) and passes the verdict in. Only when the gates
//!   pass are the MARKED dots collected (still subject to criteria 2
//!   and 3); dots that appeared after the mark wait for the next cycle.
//!
//! "Collected dots existed at mark time, and every known replica has
//! provably synchronised past the mark" is what makes the sweep safe: a
//! replica that has consumed state as of `mark_ms` has consumed the
//! post-remove state, so it can never re-offer the removed dots. When
//! the gates fail (partition, lagging authority, dead peer still in the
//! registry) the mark is simply KEPT and nothing is collected —
//! tombstones accumulate until the cluster heals (fail-closed).
//!
//! The legacy per-node **version floor** machinery
//! (`compact_deferred_with_floor`) is retained for callers that obtain
//! genuine cross-replica dot-counter floors out of band; the runtime GC
//! does not use it (no protocol currently transports per-key dot
//! counters — see P1-10 for why HLC frontiers must never be used as
//! counter floors).

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use crate::store::kv::{CrdtValue, Store};
use crate::types::NodeId;

/// Configuration and state for tombstone garbage collection.
#[derive(Debug, Clone)]
pub struct TombstoneGc {
    /// Per-node minimum known version across all replicas.
    ///
    /// A dot `(node_id, counter)` with `counter < version_floor[node_id]`
    /// is safe to garbage-collect (assuming it is not in any live element).
    version_floor: HashMap<NodeId, u64>,
    /// Global version floor applied to ALL writer nodes, in **dot-counter units**.
    ///
    /// When set, a dot `(node_id, counter)` with `counter < global_floor` is
    /// considered safe to GC regardless of the per-node floor entries.
    ///
    /// **Units**: this must be a dot counter value (a small monotonic integer),
    /// NOT an HLC physical timestamp (Unix milliseconds, ~10^12). Passing an
    /// HLC-scale value causes every dot counter to appear below the floor and
    /// bulk-GCs all tombstones. See `set_global_floor` for a guard.
    global_floor: Option<u64>,
    /// Configurable interval between GC runs.
    pub gc_interval: Duration,
    /// Minimum time tombstones must be retained after creation.
    ///
    /// Even if a dot is below the version floor, it is kept until at least
    /// `retention_period` has elapsed since the last GC run. This gives
    /// slow replicas extra time to merge.
    pub retention_period: Duration,
    /// Wall-clock millisecond timestamp of the last GC run.
    last_gc_ms: u64,
    /// Cumulative count of tombstones removed across all GC runs.
    total_collected: u64,
    /// Mark-and-sweep state: deferred dots snapshotted per store key at
    /// [`marked_at_ms`](Self::pending_mark_ms). Only these candidates may
    /// be collected by the next sweep.
    marked: HashMap<String, HashSet<(NodeId, u64)>>,
    /// Wall-clock time of the pending mark; `None` when no mark is
    /// outstanding (the next pass will mark, not sweep).
    marked_at_ms: Option<u64>,
}

impl Default for TombstoneGc {
    fn default() -> Self {
        Self {
            version_floor: HashMap::new(),
            global_floor: None,
            gc_interval: Duration::from_secs(60),
            retention_period: Duration::from_secs(300),
            last_gc_ms: 0,
            total_collected: 0,
            marked: HashMap::new(),
            marked_at_ms: None,
        }
    }
}

impl TombstoneGc {
    /// Create a new `TombstoneGc` with the given interval and retention period.
    pub fn new(gc_interval: Duration, retention_period: Duration) -> Self {
        Self {
            version_floor: HashMap::new(),
            global_floor: None,
            gc_interval,
            retention_period,
            last_gc_ms: 0,
            total_collected: 0,
            marked: HashMap::new(),
            marked_at_ms: None,
        }
    }

    /// Update the version floor for a specific node.
    ///
    /// The floor is set to the minimum of the current floor and the provided
    /// version. Call this with each replica's known counter for the node to
    /// build the global minimum.
    pub fn update_floor(&mut self, node_id: &NodeId, version: u64) {
        let entry = self.version_floor.entry(node_id.clone()).or_insert(version);
        if version < *entry {
            *entry = version;
        }
    }

    /// Set the version floor for a node to an exact value (replacing any
    /// previous value). Useful for bulk-setting floors from authority data.
    pub fn set_floor(&mut self, node_id: &NodeId, version: u64) {
        self.version_floor.insert(node_id.clone(), version);
    }

    /// Return the current version floor for a node, if known.
    pub fn floor_for(&self, node_id: &NodeId) -> Option<u64> {
        self.version_floor.get(node_id).copied()
    }

    /// Set the global version floor (in dot-counter units) for ALL writer nodes.
    ///
    /// `floor` must be a **dot counter** value — a small monotonic integer
    /// incremented once per write operation per node. It must NOT be an HLC
    /// physical timestamp (Unix milliseconds, ~10^12): dot counters are always
    /// smaller than HLC timestamps, so an HLC-scale floor would mark every
    /// tombstone as below the floor and bulk-GC them all.
    pub fn set_global_floor(&mut self, floor: u64) {
        assert!(
            floor < 1_000_000_000_000,
            "global_floor must be in dot-counter units (small int), not HLC ms (~10^12); got {floor}"
        );
        self.global_floor = Some(floor);
    }

    /// Return the current global version floor, if set.
    pub fn global_floor(&self) -> Option<u64> {
        self.global_floor
    }

    /// Return the wall-clock timestamp (ms) of the last GC run.
    pub fn last_gc_ms(&self) -> u64 {
        self.last_gc_ms
    }

    /// Return the total number of tombstones collected so far.
    pub fn total_collected(&self) -> u64 {
        self.total_collected
    }

    /// Check whether enough time has elapsed since `last_gc_ms` was last set.
    ///
    /// `last_gc_ms` advances only when a sweep actually collects
    /// tombstones, so after a collection the next ATTEMPT waits a full
    /// `gc_interval`; while nothing is collected (no mark yet, gates
    /// blocked, nothing eligible) every call past the interval attempts
    /// again promptly.
    ///
    /// **`gc_interval = 0` note**: the comparison uses a minimum of 1 ms to prevent
    /// callers that loop on `should_run` from busy-polling at nanosecond cadence.
    /// In tests that want "run immediately", set the interval to `Duration::from_millis(1)`.
    ///
    /// The `gc_interval` field controls *how often to attempt* a GC pass.
    /// The `retention_period` field, checked inside
    /// [`mark_and_sweep`](Self::mark_and_sweep), is the *minimum age* a
    /// mark must reach before its sweep may collect.
    pub fn should_run(&self, now_ms: u64) -> bool {
        let interval_ms = self.gc_interval.as_millis() as u64;
        let interval_ms = interval_ms.max(1);
        now_ms.saturating_sub(self.last_gc_ms) >= interval_ms
    }

    /// Wall-clock time (ms) of the pending mark, if one is outstanding.
    ///
    /// The caller evaluates its replica-synchronisation gates against
    /// this value before the sweep pass: collection is safe only when
    /// every known replica has provably synchronised past the mark (see
    /// the module docs and `NodeRunner::run_gc`).
    pub fn pending_mark_ms(&self) -> Option<u64> {
        self.marked_at_ms
    }

    /// Gated mark-and-sweep over all CRDT values in the store.
    ///
    /// - With no outstanding mark, this pass MARKS: the current deferred
    ///   dots are snapshotted per key and `now_ms` is recorded; nothing
    ///   is collected.
    /// - With an outstanding mark that is at least `retention_period`
    ///   old AND `gates_passed == true` (the caller verified every known
    ///   replica synchronised past [`pending_mark_ms`](Self::pending_mark_ms)),
    ///   this pass SWEEPS: marked dots that are not live and are locally
    ///   dominated are removed, then a fresh mark is taken.
    /// - Otherwise (mark too young, or gates failed) nothing happens:
    ///   the mark is KEPT so the same `mark_ms` keeps being re-evaluated
    ///   — a partition or a lagging replica stalls collection entirely
    ///   (fail-closed) and it resumes automatically once the gates pass.
    ///
    /// The two-pass structure is what makes collection safe against
    /// resurrection: every collected dot existed at mark time, and the
    /// gates prove every known replica consumed post-remove state from
    /// AFTER the mark — so no known replica can re-offer the dot as live
    /// state. A purely wall-clock retention (the previous design) could
    /// not exclude a replica partitioned for longer than the retention
    /// window.
    ///
    /// Returns the number of tombstones removed in this pass.
    pub fn mark_and_sweep(&mut self, store: &mut Store, now_ms: u64, gates_passed: bool) -> u64 {
        let mut collected = 0u64;
        let retention_ms = self.retention_period.as_millis() as u64;
        let sweep_ready = self
            .marked_at_ms
            .is_some_and(|mark| now_ms.saturating_sub(mark) >= retention_ms);

        if sweep_ready && gates_passed {
            for key in store.keys().into_iter().cloned().collect::<Vec<_>>() {
                let Some(candidates) = self.marked.get(&key) else {
                    continue;
                };
                if let Some(value) = store.get_mut(&key) {
                    match value {
                        CrdtValue::Set(set) => {
                            let before = set.deferred_len();
                            set.compact_deferred_marked(candidates);
                            collected += before.saturating_sub(set.deferred_len()) as u64;
                        }
                        CrdtValue::Map(map) => {
                            let before = map.deferred_len();
                            map.compact_deferred_marked(candidates);
                            collected += before.saturating_sub(map.deferred_len()) as u64;
                        }
                        CrdtValue::Counter(_) | CrdtValue::Register(_) => {
                            // No tombstones for counters or registers.
                        }
                    }
                }
            }
            // The mark is consumed regardless of how much was collected;
            // a fresh mark is taken below for the next cycle.
            self.marked.clear();
            self.marked_at_ms = None;
            if collected > 0 {
                self.total_collected += collected;
                self.last_gc_ms = now_ms;
            }
        }

        // (Re-)mark when no mark is outstanding. A blocked sweep
        // (gates_passed == false) deliberately KEEPS its mark: re-marking
        // would slide mark_ms forward and a cluster that heals more
        // slowly than the attempt cadence could never collect.
        if self.marked_at_ms.is_none() {
            let mut marked: HashMap<String, HashSet<(NodeId, u64)>> = HashMap::new();
            for (key, value) in store.all_entries() {
                let dots = match value {
                    CrdtValue::Set(set) => set.deferred_dots(),
                    CrdtValue::Map(map) => map.deferred_dots(),
                    CrdtValue::Counter(_) | CrdtValue::Register(_) => continue,
                };
                if !dots.is_empty() {
                    marked.insert(key.clone(), dots);
                }
            }
            self.marked = marked;
            self.marked_at_ms = Some(now_ms);
        }

        collected
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::or_map::OrMap;
    use crate::crdt::or_set::OrSet;
    use crate::hlc::HlcTimestamp;
    use crate::store::kv::{CrdtValue, Store};
    use crate::types::NodeId;

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

    #[test]
    fn default_gc_has_sensible_defaults() {
        let gc = TombstoneGc::default();
        assert_eq!(gc.gc_interval, Duration::from_secs(60));
        assert_eq!(gc.retention_period, Duration::from_secs(300));
        assert_eq!(gc.last_gc_ms, 0);
        assert_eq!(gc.total_collected, 0);
    }

    #[test]
    fn should_run_respects_interval() {
        let gc = TombstoneGc::new(Duration::from_secs(60), Duration::from_secs(0));
        // At t=0, elapsed = 0 - 0 = 0, which equals the interval (60s) only if
        // we wait. With interval=60s, should_run(0) is false (0 < 60000).
        // should_run(60000) should be true.
        assert!(gc.should_run(60_000));
        assert!(gc.should_run(100_000));
    }

    #[test]
    fn should_run_zero_interval_not_busy_poll() {
        // gc_interval=0 must be clamped to 1ms; should_run(0) must return false
        // (elapsed=0 < 1) so callers that loop on should_run don't busy-poll.
        let gc = TombstoneGc::new(Duration::ZERO, Duration::ZERO);
        assert!(
            !gc.should_run(0),
            "should_run(0) must be false with gc_interval=0 (clamped to 1ms)"
        );
        assert!(
            gc.should_run(1),
            "should_run(1) must be true: elapsed 1 >= clamped interval 1"
        );
    }

    #[test]
    fn should_run_after_interval_elapsed() {
        let mut gc = TombstoneGc::new(Duration::from_secs(60), Duration::from_secs(0));
        gc.last_gc_ms = 1000;
        // 59 seconds later: not yet.
        assert!(!gc.should_run(60_000));
        // 60 seconds later: yes.
        assert!(gc.should_run(61_000));
    }

    #[test]
    fn update_floor_takes_minimum() {
        let mut gc = TombstoneGc::default();
        let n = node("A");
        gc.update_floor(&n, 10);
        assert_eq!(gc.floor_for(&n), Some(10));

        gc.update_floor(&n, 5);
        assert_eq!(gc.floor_for(&n), Some(5));

        // Higher value should not raise the floor.
        gc.update_floor(&n, 20);
        assert_eq!(gc.floor_for(&n), Some(5));
    }

    #[test]
    fn set_floor_replaces_value() {
        let mut gc = TombstoneGc::default();
        let n = node("A");
        gc.set_floor(&n, 5);
        assert_eq!(gc.floor_for(&n), Some(5));

        gc.set_floor(&n, 20);
        assert_eq!(gc.floor_for(&n), Some(20));
    }

    /// Build a store with one OrSet holding a locally-dominated tombstone
    /// dot (A,1): add x → remove x → add y (counter advances past 1).
    fn store_with_set_tombstone() -> Store {
        let n = node("A");
        let mut set = OrSet::new();
        set.add("x".to_string(), &n); // counter=1
        set.remove(&"x".to_string()); // dot (A,1) in deferred
        set.add("y".to_string(), &n); // counter=2, dominance
        assert_eq!(set.deferred_len(), 1);
        let mut store = Store::new();
        store.put("myset".into(), CrdtValue::Set(set));
        store
    }

    const RET: u64 = 300_000; // 300s retention in ms

    #[test]
    fn mark_and_sweep_collects_or_set_after_mark_retention_and_gates() {
        let mut gc = TombstoneGc::new(Duration::from_secs(0), Duration::from_secs(300));
        let mut store = store_with_set_tombstone();

        // Pass 1: marks only — nothing is collected even with gates open.
        let collected = gc.mark_and_sweep(&mut store, 1_000, true);
        assert_eq!(collected, 0, "first pass only marks");
        assert_eq!(gc.pending_mark_ms(), Some(1_000));

        // Pass 2 after retention with gates passed: sweep collects.
        let collected = gc.mark_and_sweep(&mut store, 1_000 + RET, true);
        assert_eq!(collected, 1);
        assert_eq!(gc.total_collected(), 1);
        if let Some(CrdtValue::Set(s)) = store.get("myset") {
            assert_eq!(s.deferred_len(), 0);
        } else {
            panic!("expected Set");
        }
        // A fresh mark was taken for the next cycle.
        assert_eq!(gc.pending_mark_ms(), Some(1_000 + RET));
    }

    #[test]
    fn mark_and_sweep_collects_or_map() {
        let mut gc = TombstoneGc::new(Duration::from_secs(0), Duration::from_secs(300));
        let mut store = Store::new();
        let n = node("A");

        let mut map: OrMap<String, String> = OrMap::new();
        map.set("k1".into(), "v1".into(), ts(100, 0, "A"), &n); // counter=1
        map.delete(&"k1".to_string()); // dot (A,1) in deferred
        map.set("k2".into(), "v2".into(), ts(200, 0, "A"), &n); // counter=2
        assert!(map.deferred_len() > 0);
        store.put("mymap".into(), CrdtValue::Map(map));

        assert_eq!(gc.mark_and_sweep(&mut store, 1_000, true), 0);
        let collected = gc.mark_and_sweep(&mut store, 1_000 + RET, true);
        assert!(collected > 0);
        if let Some(CrdtValue::Map(m)) = store.get("mymap") {
            assert_eq!(m.deferred_len(), 0);
        } else {
            panic!("expected Map");
        }
    }

    #[test]
    fn mark_and_sweep_skips_counters_and_registers() {
        let mut gc = TombstoneGc::new(Duration::from_secs(0), Duration::from_secs(300));
        let mut store = Store::new();

        let mut counter = crate::crdt::pn_counter::PnCounter::new();
        counter.increment(&node("A"));
        store.put("cnt".into(), CrdtValue::Counter(counter));

        let mut reg = crate::crdt::lww_register::LwwRegister::new();
        reg.set("hello".to_string(), ts(100, 0, "A"));
        store.put("reg".into(), CrdtValue::Register(reg));

        assert_eq!(gc.mark_and_sweep(&mut store, 1_000, true), 0);
        assert_eq!(gc.mark_and_sweep(&mut store, 1_000 + RET, true), 0);
    }

    /// The sweep may only run once the mark is at least `retention_period`
    /// old — a young mark is kept, not consumed.
    #[test]
    fn sweep_requires_mark_age_of_retention_period() {
        let mut gc = TombstoneGc::new(Duration::from_secs(0), Duration::from_secs(300));
        let mut store = store_with_set_tombstone();

        assert_eq!(gc.mark_and_sweep(&mut store, 1_000, true), 0); // mark
        // Too young: even with gates open nothing is collected and the
        // mark is retained.
        assert_eq!(gc.mark_and_sweep(&mut store, 2_000, true), 0);
        assert_eq!(gc.pending_mark_ms(), Some(1_000), "young mark is kept");
        // Old enough: collect.
        assert_eq!(gc.mark_and_sweep(&mut store, 1_000 + RET, true), 1);
    }

    /// C-2 regression (resurrection prevention): while any known replica
    /// has NOT synchronised past the mark (gates fail — e.g. a network
    /// partition longer than the retention window), the sweep must not
    /// collect. A lagging replica's stale state can then still be merged
    /// WITHOUT resurrecting the removed element; under the old
    /// wall-clock-only design the tombstone would already be gone and the
    /// remove would silently undo itself cluster-wide.
    #[test]
    fn blocked_gates_prevent_resurrection_after_long_partition() {
        let n = node("A");

        // Replica state before the partition: both sides hold {x}.
        let mut local = OrSet::new();
        local.add("x".to_string(), &n); // dot (A,1)
        let lagging_replica: OrSet<String> = local.clone();

        // Local removes x during the partition (tombstone A,1) and keeps
        // writing (dominance).
        local.remove(&"x".to_string());
        local.add("y".to_string(), &n); // dot (A,2)
        let mut store = Store::new();
        store.put("myset".into(), CrdtValue::Set(local));

        let mut gc = TombstoneGc::new(Duration::from_secs(0), Duration::from_secs(300));
        assert_eq!(gc.mark_and_sweep(&mut store, 1_000, false), 0); // mark

        // The partition outlives the retention window: gates still fail,
        // so NOTHING is collected — no matter how much wall-clock time
        // has passed.
        assert_eq!(
            gc.mark_and_sweep(&mut store, 1_000 + 10 * RET, false),
            0,
            "gates must stall collection through arbitrarily long partitions"
        );
        assert_eq!(
            gc.pending_mark_ms(),
            Some(1_000),
            "the blocked mark is kept, not re-taken"
        );

        // Partition heals: the lagging replica pushes its STALE state.
        // The retained tombstone absorbs the old dot — no resurrection.
        if let Some(CrdtValue::Set(s)) = store.get_mut("myset") {
            s.merge(&lagging_replica);
            assert!(
                !s.contains(&"x".to_string()),
                "remove must survive a stale merge while the tombstone is retained"
            );
        } else {
            panic!("expected Set");
        }

        // The replica has now provably synchronised past the mark: gates
        // pass and the ORIGINAL mark is finally swept.
        let collected = gc.mark_and_sweep(&mut store, 1_000 + 11 * RET, true);
        assert_eq!(collected, 1, "healing the cluster resumes collection");
        if let Some(CrdtValue::Set(s)) = store.get("myset") {
            assert_eq!(s.deferred_len(), 0);
            assert!(!s.contains(&"x".to_string()));
        }
    }

    /// The sweep only collects dots that existed at mark time: tombstones
    /// created after the mark survive and wait for the next cycle (whose
    /// gate will cover them).
    #[test]
    fn sweep_only_collects_marked_dots() {
        let n = node("A");
        let mut gc = TombstoneGc::new(Duration::from_secs(0), Duration::from_secs(300));
        let mut store = store_with_set_tombstone(); // tombstone (A,1)

        assert_eq!(gc.mark_and_sweep(&mut store, 1_000, true), 0); // marks (A,1)

        // A remove AFTER the mark: dot (A,2) enters deferred, dominated
        // by a later add (A,3).
        if let Some(CrdtValue::Set(s)) = store.get_mut("myset") {
            s.remove(&"y".to_string());
            s.add("z".to_string(), &n);
            assert_eq!(s.deferred_len(), 2);
        }

        // Sweep collects only the marked (A,1); the younger (A,2) stays.
        assert_eq!(gc.mark_and_sweep(&mut store, 1_000 + RET, true), 1);
        if let Some(CrdtValue::Set(s)) = store.get("myset") {
            assert_eq!(s.deferred_len(), 1, "post-mark tombstone must survive");
        }

        // The re-mark taken at sweep time covers it for the next cycle.
        assert_eq!(gc.mark_and_sweep(&mut store, 1_000 + 2 * RET, true), 1);
        if let Some(CrdtValue::Set(s)) = store.get("myset") {
            assert_eq!(s.deferred_len(), 0);
        }
    }

    #[test]
    fn mark_and_sweep_handles_multiple_values() {
        let mut gc = TombstoneGc::new(Duration::from_secs(0), Duration::from_secs(300));
        let mut store = Store::new();
        let n = node("A");

        let mut set1 = OrSet::new();
        set1.add("a".to_string(), &n);
        set1.remove(&"a".to_string());
        set1.add("a2".to_string(), &n);

        let nb = node("B");
        let mut set2 = OrSet::new();
        set2.add("b".to_string(), &nb);
        set2.remove(&"b".to_string());
        set2.add("b2".to_string(), &nb);

        store.put("s1".into(), CrdtValue::Set(set1));
        store.put("s2".into(), CrdtValue::Set(set2));

        assert_eq!(gc.mark_and_sweep(&mut store, 1_000, true), 0);
        assert_eq!(gc.mark_and_sweep(&mut store, 1_000 + RET, true), 2);
        assert_eq!(gc.total_collected(), 2);
    }

    /// `last_gc_ms` advances only when a sweep collects, so `should_run`
    /// keeps attempting while marks are pending or gates are blocked.
    #[test]
    fn last_gc_ms_advances_only_on_collection() {
        let mut gc = TombstoneGc::new(Duration::from_secs(0), Duration::from_secs(300));
        let mut store = store_with_set_tombstone();

        assert_eq!(gc.last_gc_ms(), 0);
        gc.mark_and_sweep(&mut store, 5_000, true); // mark
        assert_eq!(gc.last_gc_ms(), 0, "marking must not advance last_gc_ms");

        gc.mark_and_sweep(&mut store, 5_000 + RET, false); // blocked sweep
        assert_eq!(gc.last_gc_ms(), 0, "blocked sweep must not advance");

        let collected = gc.mark_and_sweep(&mut store, 5_000 + 2 * RET, true);
        assert_eq!(collected, 1);
        assert_eq!(gc.last_gc_ms(), 5_000 + 2 * RET);
    }

    #[test]
    fn set_global_floor_rejects_hlc_scale_values() {
        // set_global_floor must panic when given an HLC physical timestamp
        // (~10^12 ms) instead of a dot-counter value (small int). Without this
        // guard, an HLC-scale floor would mark every tombstone as below the
        // floor and bulk-GC them all.
        let result = std::panic::catch_unwind(|| {
            let mut gc = TombstoneGc::default();
            gc.set_global_floor(1_700_000_000_000u64); // ~2023 in ms (HLC scale)
        });
        assert!(
            result.is_err(),
            "set_global_floor should panic on HLC-scale values"
        );
    }

    #[test]
    fn set_global_floor_accepts_dot_counter_values() {
        let mut gc = TombstoneGc::default();
        gc.set_global_floor(42); // small monotonic integer, OK
        assert_eq!(gc.global_floor(), Some(42));
    }

    /// P1-10 regression: using HLC physical timestamps (huge ms values) as
    /// the global_floor for compact_deferred_with_floor causes all tombstones
    /// to be removed because dot counters (small integers) are always below
    /// the HLC-scale floor. This test demonstrates the bug: a removed element
    /// resurrects after GC if HLC timestamps are used as counter floors.
    #[test]
    fn hlc_as_floor_causes_premature_tombstone_gc() {
        let n = node("A");
        let mut set = OrSet::new();
        set.add("x".to_string(), &n); // counter=1
        set.remove(&"x".to_string()); // dot (A,1) in deferred
        set.add("y".to_string(), &n); // counter=2

        assert_eq!(set.deferred_len(), 1);

        // Simulate the old buggy code: use HLC physical timestamp (~10^12)
        // as global_floor. Since dot counter (1) < floor (1_700_000_000_000),
        // the tombstone would be removed even though a lagging replica might
        // not have seen it yet.
        let hlc_floor = 1_700_000_000_000u64; // ~2023 in ms
        let empty_floor = std::collections::HashMap::new();
        set.compact_deferred_with_floor(&empty_floor, Some(hlc_floor));

        // BUG: the tombstone was removed because counter < hlc_floor
        // This would allow "x" to resurrect on a lagging replica.
        assert_eq!(
            set.deferred_len(),
            0,
            "with HLC-scale floor, tombstone is incorrectly removed"
        );

        // Now demonstrate that without the HLC floor, compact_deferred
        // correctly uses counter-based dominance and removes the tombstone
        // only because counter < max_counter for the node (1 < 2).
        let mut set2 = OrSet::new();
        set2.add("x".to_string(), &n);
        set2.remove(&"x".to_string());
        set2.add("y".to_string(), &n);
        assert_eq!(set2.deferred_len(), 1);

        // compact_deferred() uses counter dominance only — safe.
        set2.compact_deferred();
        assert_eq!(
            set2.deferred_len(),
            0,
            "counter-based GC should remove tombstone when counter < max"
        );
    }

    /// Verify that compact_deferred_with_floor removes a tombstone via the
    /// floor-only path: locally_dominated=false (no newer add), but
    /// dot counter < global_floor (below_floor=true).
    /// This exercises the OR-semantics new criterion 2 added by this PR.
    #[test]
    fn compact_deferred_with_floor_removes_tombstone_via_floor_only_path() {
        let n = node("A");
        let mut set = OrSet::new();
        set.add("x".to_string(), &n); // counter=1 for A
        set.remove(&"x".to_string()); // dot (A,1) in deferred
        // No further adds — max counter for A is still 1, so locally_dominated=false.

        assert_eq!(set.deferred_len(), 1);

        // Set global_floor=2 > dot counter(1). floor-only path should remove it.
        let empty_floor = std::collections::HashMap::new();
        set.compact_deferred_with_floor(&empty_floor, Some(2));

        assert_eq!(
            set.deferred_len(),
            0,
            "floor-only path must remove tombstone when dot counter < global_floor"
        );
    }

    /// Verify that compact_deferred_with_floor uses global_floor as a fallback
    /// for nodes that are absent from version_floor.
    ///
    /// Semantic: global_floor represents the minimum version confirmed by ALL
    /// known replicas for ALL writer nodes. A dot (X, counter) from a node X
    /// that is absent from version_floor should still be GC'd if counter <
    /// global_floor, because global_floor already covers X.
    ///
    /// The tombstone must NOT be locally dominated (no subsequent add after remove),
    /// so removal is driven solely by global_floor — this distinguishes the new
    /// "either criterion" logic from the old "both required" logic.
    #[test]
    fn compact_deferred_with_floor_uses_global_floor_for_absent_node() {
        let n = node("A"); // node "A" will be absent from version_floor
        let mut set = OrSet::new();
        set.add("x".to_string(), &n); // counter=1 for A
        set.remove(&"x".to_string()); // dot (A,1) in deferred
        // No further adds — counters["A"]=1, so locally_dominated=(1<1)=false.
        // Removal must be driven by global_floor alone.

        assert_eq!(set.deferred_len(), 1);

        // version_floor has no entry for node "A" (absent); global_floor=2 should apply.
        let empty_version_floor = std::collections::HashMap::new();
        set.compact_deferred_with_floor(&empty_version_floor, Some(2));

        assert_eq!(
            set.deferred_len(),
            0,
            "global_floor must GC dots from nodes absent from version_floor even when not locally dominated"
        );
    }

    /// Verify that compact_deferred_with_floor retains tombstones from absent
    /// nodes when global_floor does not cover them.
    #[test]
    fn compact_deferred_with_floor_retains_dot_above_global_floor_for_absent_node() {
        let n = node("A");
        let mut set = OrSet::new();
        set.add("x".to_string(), &n); // counter=1 for A
        set.remove(&"x".to_string()); // dot (A,1) in deferred
        // No further adds — locally_dominated=false (max counter still 1)

        assert_eq!(set.deferred_len(), 1);

        // global_floor=1 does NOT cover dot counter=1 (strictly less-than check)
        let empty_version_floor = std::collections::HashMap::new();
        set.compact_deferred_with_floor(&empty_version_floor, Some(1));

        assert_eq!(
            set.deferred_len(),
            1,
            "tombstone must be retained when dot.counter >= global_floor"
        );
    }

    /// Verify that compact_deferred without floor doesn't remove tombstones
    /// when the counter hasn't been superseded (no newer dots from that node).
    #[test]
    fn compact_deferred_retains_unsuperseded_tombstone() {
        let n = node("A");
        let mut set = OrSet::new();
        set.add("x".to_string(), &n); // counter=1
        set.remove(&"x".to_string()); // dot (A,1) in deferred
        // No further adds — max counter for A is still 1.

        assert_eq!(set.deferred_len(), 1);
        set.compact_deferred();
        // Tombstone should be retained because counter (1) is NOT < max_counter (1).
        assert_eq!(
            set.deferred_len(),
            1,
            "tombstone must be retained when counter is not superseded"
        );
    }
}
