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
    /// Whether `gc_tombstones` has been called at least once.
    ///
    /// Separate from `last_gc_ms` so that a first call with `now_ms = 0`
    /// (e.g. in unit tests) does not leave `last_gc_ms == 0` and cause every
    /// subsequent call to re-enter the first-run bypass indefinitely.
    has_run: bool,
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
            has_run: false,
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
            has_run: false,
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
    /// `last_gc_ms` is set on the very first call to
    /// [`gc_tombstones`](Self::gc_tombstones) (even with an empty store), and
    /// thereafter only when at least one tombstone is actually collected.  On
    /// no-op runs after the first, `last_gc_ms` is left unchanged so subsequent
    /// calls to `should_run` return `true` again once `gc_interval` has elapsed
    /// from the last successful collection.  This prevents the GC interval from
    /// resetting on empty-store no-op runs and ensures the scheduler re-checks
    /// promptly when the store is initially empty.
    ///
    /// **`gc_interval = 0` note**: the comparison uses a minimum of 1 ms to prevent
    /// callers that loop on `should_run` from busy-polling at nanosecond cadence.
    /// In tests that want "run immediately", set the interval to `Duration::from_millis(1)`.
    ///
    /// The `gc_interval` field controls *how often to attempt* a GC pass.
    /// The `retention_period` field, checked inside `gc_tombstones`, controls
    /// the *minimum quiet time* between successive actual collections.
    pub fn should_run(&self, now_ms: u64) -> bool {
        let interval_ms = self.gc_interval.as_millis() as u64;
        let interval_ms = interval_ms.max(1);
        now_ms.saturating_sub(self.last_gc_ms) >= interval_ms
    }

    /// Run garbage collection on all CRDT values in the store.
    ///
    /// Iterates over every `CrdtValue::Set` and `CrdtValue::Map` in the store
    /// and calls `compact_deferred()` on each, which removes tombstone dots
    /// that are below the node's known counter and not referenced by any live
    /// element.
    ///
    /// # Timing semantics
    ///
    /// Two independent timing fields govern when collection actually happens:
    ///
    /// - **`gc_interval`** (checked by [`should_run`](Self::should_run)): how
    ///   often the caller should *attempt* a GC pass. Callers are expected to
    ///   call `should_run` before calling this method.
    /// - **`retention_period`** (checked here): the minimum quiet time that must
    ///   elapse between successive *successful* collections. This prevents
    ///   premature GC of tombstones that slow replicas may not have merged yet.
    ///
    /// `last_gc_ms` is updated when at least one tombstone is collected, **or**
    /// on the very first invocation (even with an empty store) to start the
    /// retention clock. On subsequent no-op runs `last_gc_ms` is left unchanged
    /// so that `should_run` continues to return `true` and the caller re-checks
    /// promptly without waiting a full `gc_interval`.
    ///
    /// Returns the number of tombstones removed in this run.
    pub fn gc_tombstones(&mut self, store: &mut Store, now_ms: u64) -> u64 {
        // Determine whether this is the very first invocation.
        //
        // On the first call we always proceed (bypass retention) so a freshly-started
        // node can collect immediately. We also record now_ms as the retention baseline
        // regardless of whether any tombstones exist. Without this, a node that starts
        // with an empty store (last_gc_ms stays 0 across all no-op calls) would bypass
        // the retention check the first time tombstones appear, potentially GC-ing them
        // before slow replicas have had retention_period to merge them.
        //
        // `has_run` is used rather than `last_gc_ms == 0` to avoid an infinite
        // first-run loop when `now_ms = 0` (e.g. in unit tests): without this flag,
        // setting `last_gc_ms = 0` on the first call would leave the sentinel
        // unchanged and every subsequent call would re-enter the first-run bypass.
        let is_first_run = !self.has_run;

        if !is_first_run {
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

        // Advance the GC timestamp when tombstones were collected, OR on the very
        // first run (even with an empty store) to start the retention clock.
        // On subsequent no-op runs last_gc_ms is left unchanged so that should_run()
        // keeps returning true, ensuring the scheduler re-checks promptly without
        // waiting a full gc_interval. The two timing fields remain orthogonal:
        // gc_interval = attempt cadence, retention_period = minimum gap between
        // actual collections.
        if collected > 0 {
            self.total_collected += collected;
        }
        if collected > 0 || is_first_run {
            self.last_gc_ms = now_ms;
        }
        self.has_run = true;
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
    fn gc_updates_last_gc_ms_on_first_run_and_on_collection() {
        let mut gc = TombstoneGc::new(Duration::from_secs(0), Duration::from_secs(0));
        let mut store = Store::new();

        // First run on an empty store: last_gc_ms MUST advance to start the
        // retention clock, even though no tombstones are collected.  Without
        // this, tombstones created later would bypass the retention check
        // because last_gc_ms would still be 0 when they first appear.
        assert_eq!(gc.last_gc_ms(), 0);
        let collected = gc.gc_tombstones(&mut store, 5000);
        assert_eq!(collected, 0);
        assert_eq!(
            gc.last_gc_ms(),
            5000,
            "first run must advance last_gc_ms to start the retention clock"
        );

        // Add a tombstone-bearing OrSet; with retention=0 the second call collects.
        let n = node("A");
        let mut set = OrSet::new();
        set.add("x".to_string(), &n); // counter=1
        set.remove(&"x".to_string()); // dot (A,1) in deferred
        set.add("y".to_string(), &n); // counter=2, advances past tombstoned dot
        store.put("s".into(), CrdtValue::Set(set));

        let collected = gc.gc_tombstones(&mut store, 9000);
        assert_eq!(collected, 1);
        assert_eq!(
            gc.last_gc_ms(),
            9000,
            "last_gc_ms must advance when tombstones are collected"
        );

        // Third call with empty deferred: last_gc_ms must NOT advance so
        // should_run keeps returning true until the next collection.
        let collected = gc.gc_tombstones(&mut store, 12_000);
        assert_eq!(collected, 0);
        assert_eq!(
            gc.last_gc_ms(),
            9000,
            "last_gc_ms must not advance on subsequent no-op runs after first"
        );
    }

    #[test]
    fn gc_first_run_starts_retention_clock_for_late_tombstones() {
        // Regression test: a node that starts with an empty store should NOT
        // bypass the retention check when tombstones appear later.  Previously,
        // last_gc_ms stayed 0 across all empty-store calls, so the first call
        // with tombstones would see last_gc_ms==0 and skip the retention check.
        let retention = Duration::from_secs(300);
        let mut gc = TombstoneGc::new(Duration::from_secs(0), retention);
        let mut store = Store::new();
        let n = node("A");

        // First call at T=1000: empty store, starts the retention clock.
        let collected = gc.gc_tombstones(&mut store, 1_000);
        assert_eq!(collected, 0);
        assert_eq!(
            gc.last_gc_ms(),
            1_000,
            "first call must start retention clock"
        );

        // Tombstones appear shortly after at T=2000 (well within retention window).
        let mut set = OrSet::new();
        set.add("x".to_string(), &n); // counter=1
        set.remove(&"x".to_string()); // dot (A,1) in deferred
        set.add("y".to_string(), &n); // counter=2, advances past tombstoned dot
        store.put("s".into(), CrdtValue::Set(set));

        // T=2001: retention=300s, elapsed=1001ms < 300_000ms → skip.
        let collected = gc.gc_tombstones(&mut store, 2_001);
        assert_eq!(
            collected, 0,
            "tombstones within retention window must not be collected"
        );

        // T=302001: 301001ms has elapsed since first run (T=1000);
        // retention=300_000ms → collect the deferred tombstone.
        let collected = gc.gc_tombstones(&mut store, 302_001);
        assert_eq!(
            collected, 1,
            "tombstone must be collected after retention window expires"
        );
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
