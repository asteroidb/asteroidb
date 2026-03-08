//! Tombstone garbage collection for OR-Set and OR-Map CRDTs.
//!
//! The `deferred` (tombstone) sets in [`OrSet`] and [`OrMap`] grow unboundedly
//! over time because every remove operation appends dots. This module provides
//! a `TombstoneGc` that periodically reclaims tombstones that are provably safe
//! to discard.
//!
//! # Safety criterion
//!
//! A tombstone dot `(node_id, counter)` is safe to GC when:
//! 1. All known replicas have already incorporated it (their version for the
//!    node is past this counter), AND
//! 2. The dot is not referenced by any live element in the CRDT.
//!
//! The GC tracks a **version floor** per node — the minimum known version
//! across all replicas. Dots strictly below this floor satisfy criterion (1).
//! Criterion (2) is checked by `compact_deferred()` on the CRDT itself.

use std::collections::HashMap;
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
    /// Global version floor applied to ALL writer nodes.
    ///
    /// Derived from the minimum acknowledged frontier HLC physical timestamp
    /// across all authorities. When set, a dot `(node_id, counter)` with
    /// `counter < global_floor` is considered safe to GC regardless of the
    /// per-node floor entries.
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

    /// Set the global version floor that applies to ALL writer nodes.
    ///
    /// Typically set to the minimum acknowledged frontier HLC physical
    /// timestamp across all authorities.
    pub fn set_global_floor(&mut self, floor: u64) {
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

    /// Check whether enough time has elapsed since the last GC run.
    pub fn should_run(&self, now_ms: u64) -> bool {
        let interval_ms = self.gc_interval.as_millis() as u64;
        now_ms.saturating_sub(self.last_gc_ms) >= interval_ms
    }

    /// Run garbage collection on all CRDT values in the store.
    ///
    /// Iterates over every `CrdtValue::Set` and `CrdtValue::Map` in the store
    /// and calls `compact_deferred()` on each, which removes tombstone dots
    /// that are below the node's known counter and not referenced by any live
    /// element.
    ///
    /// Returns the number of tombstones removed in this run.
    pub fn gc_tombstones(&mut self, store: &mut Store, now_ms: u64) -> u64 {
        // Check retention: if we haven't waited long enough since the last GC,
        // skip this run. The first run (last_gc_ms == 0) always proceeds.
        if self.last_gc_ms > 0 {
            let retention_ms = self.retention_period.as_millis() as u64;
            if now_ms.saturating_sub(self.last_gc_ms) < retention_ms {
                return 0;
            }
        }

        let mut collected = 0u64;
        let has_floor = !self.version_floor.is_empty() || self.global_floor.is_some();

        for key in store.keys().into_iter().cloned().collect::<Vec<_>>() {
            if let Some(value) = store.get_mut(&key) {
                match value {
                    CrdtValue::Set(set) => {
                        let before = set.deferred_len();
                        if has_floor {
                            set.compact_deferred_with_floor(&self.version_floor, self.global_floor);
                        } else {
                            set.compact_deferred();
                        }
                        let after = set.deferred_len();
                        collected += before.saturating_sub(after) as u64;
                    }
                    CrdtValue::Map(map) => {
                        let before = map.deferred_len();
                        if has_floor {
                            map.compact_deferred_with_floor(&self.version_floor, self.global_floor);
                        } else {
                            map.compact_deferred();
                        }
                        let after = map.deferred_len();
                        collected += before.saturating_sub(after) as u64;
                    }
                    CrdtValue::Counter(_) | CrdtValue::Register(_) => {
                        // No tombstones for counters or registers.
                    }
                }
            }
        }

        self.last_gc_ms = now_ms;
        self.total_collected += collected;
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

    #[test]
    fn gc_removes_tombstones_from_or_set() {
        let mut gc = TombstoneGc::new(Duration::from_secs(0), Duration::from_secs(0));
        let mut store = Store::new();
        let n = node("A");

        // Build an OrSet with tombstones.
        // compact_deferred() only removes dots where counter < max_counter
        // for the node, so we need at least one more add after the remove
        // to advance the counter past the tombstoned dot.
        let mut set = OrSet::new();
        set.add("x".to_string(), &n); // counter=1
        set.remove(&"x".to_string()); // dot (A,1) goes to deferred
        set.add("y".to_string(), &n); // counter=2, advancing past the tombstoned dot
        // deferred has dot (A,1); max counter for A is 2; 1 < 2 → safe to GC.
        assert_eq!(set.deferred_len(), 1);

        store.put("myset".into(), CrdtValue::Set(set));

        let collected = gc.gc_tombstones(&mut store, 1000);
        assert_eq!(collected, 1);
        assert_eq!(gc.total_collected(), 1);

        // Verify the deferred set is now empty.
        if let Some(CrdtValue::Set(s)) = store.get("myset") {
            assert_eq!(s.deferred_len(), 0);
        } else {
            panic!("expected Set");
        }
    }

    #[test]
    fn gc_removes_tombstones_from_or_map() {
        let mut gc = TombstoneGc::new(Duration::from_secs(0), Duration::from_secs(0));
        let mut store = Store::new();
        let n = node("A");

        let mut map: OrMap<String, String> = OrMap::new();
        map.set("k1".into(), "v1".into(), ts(100, 0, "A"), &n); // counter=1
        map.delete(&"k1".to_string()); // dot (A,1) goes to deferred
        // Add another key to advance the counter past the tombstoned dot.
        map.set("k2".into(), "v2".into(), ts(200, 0, "A"), &n); // counter=2
        assert!(map.deferred_len() > 0);

        store.put("mymap".into(), CrdtValue::Map(map));

        let collected = gc.gc_tombstones(&mut store, 1000);
        assert!(collected > 0);

        if let Some(CrdtValue::Map(m)) = store.get("mymap") {
            assert_eq!(m.deferred_len(), 0);
        } else {
            panic!("expected Map");
        }
    }

    #[test]
    fn gc_skips_counters_and_registers() {
        let mut gc = TombstoneGc::new(Duration::from_secs(0), Duration::from_secs(0));
        let mut store = Store::new();

        let mut counter = crate::crdt::pn_counter::PnCounter::new();
        counter.increment(&node("A"));
        store.put("cnt".into(), CrdtValue::Counter(counter));

        let mut reg = crate::crdt::lww_register::LwwRegister::new();
        reg.set("hello".to_string(), ts(100, 0, "A"));
        store.put("reg".into(), CrdtValue::Register(reg));

        let collected = gc.gc_tombstones(&mut store, 1000);
        assert_eq!(collected, 0);
    }

    #[test]
    fn gc_retention_period_prevents_early_collection() {
        let mut gc = TombstoneGc::new(Duration::from_secs(0), Duration::from_secs(300));
        let mut store = Store::new();
        let n = node("A");

        // compact_deferred only removes dots where counter < max_counter,
        // so we need add->remove->add to ensure the counter advances.
        let mut set = OrSet::new();
        set.add("x".to_string(), &n); // counter=1
        set.remove(&"x".to_string()); // dot (A,1) in deferred
        set.add("y".to_string(), &n); // counter=2, advances past tombstoned dot
        store.put("myset".into(), CrdtValue::Set(set));

        // First run always proceeds.
        let collected = gc.gc_tombstones(&mut store, 1000);
        assert_eq!(collected, 1);

        // Re-add and remove to create more tombstones, then add again to advance.
        if let Some(CrdtValue::Set(s)) = store.get_mut("myset") {
            s.add("z".to_string(), &n); // counter=3
            s.remove(&"z".to_string()); // dot (A,3) in deferred
            s.add("w".to_string(), &n); // counter=4, advances past
        }

        // Second run at 2000ms: retention period (300s) not elapsed.
        let collected = gc.gc_tombstones(&mut store, 2000);
        assert_eq!(collected, 0, "should skip due to retention period");

        // After retention period.
        let collected = gc.gc_tombstones(&mut store, 301_001);
        assert_eq!(collected, 1, "should collect after retention period");
    }

    #[test]
    fn gc_multiple_values_in_store() {
        let mut gc = TombstoneGc::new(Duration::from_secs(0), Duration::from_secs(0));
        let mut store = Store::new();
        let n = node("A");

        // Two OrSets with tombstones.
        // Need add->remove->add to advance counter past the tombstoned dot.
        let mut set1 = OrSet::new();
        set1.add("a".to_string(), &n); // counter=1
        set1.remove(&"a".to_string()); // dot (A,1) in deferred
        set1.add("a2".to_string(), &n); // counter=2

        let nb = node("B");
        let mut set2 = OrSet::new();
        set2.add("b".to_string(), &nb); // counter=1
        set2.remove(&"b".to_string()); // dot (B,1) in deferred
        set2.add("b2".to_string(), &nb); // counter=2

        store.put("s1".into(), CrdtValue::Set(set1));
        store.put("s2".into(), CrdtValue::Set(set2));

        let collected = gc.gc_tombstones(&mut store, 1000);
        assert_eq!(collected, 2);
        assert_eq!(gc.total_collected(), 2);
    }

    #[test]
    fn gc_updates_last_gc_ms() {
        let mut gc = TombstoneGc::new(Duration::from_secs(0), Duration::from_secs(0));
        let mut store = Store::new();

        assert_eq!(gc.last_gc_ms(), 0);
        gc.gc_tombstones(&mut store, 5000);
        assert_eq!(gc.last_gc_ms(), 5000);
    }
}
