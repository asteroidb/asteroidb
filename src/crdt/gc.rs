//! Tombstone garbage collection for OR-Set and OR-Map CRDTs.
//!
//! The `deferred` (tombstone) sets in [`crate::crdt::or_set::OrSet`] and
//! [`crate::crdt::or_map::OrMap`] grow unboundedly over time because every
//! remove operation appends dots. This module provides a `TombstoneGc`
//! that periodically compacts tombstones into the per-value, per-node
//! **compaction floor** (`compaction_floor` on each CRDT), the M-8 fix
//! for the tombstone-GC livelock.
//!
//! # Why a floor instead of plain deletion
//!
//! The pre-floor sweep physically deleted tombstone dots without leaving
//! a trace. Deletion is one-sided, but the anti-entropy digest compares
//! full CRDT states: after an asymmetric sweep the states genuinely
//! differ, the mismatched bucket is transferred, and the old `merge`
//! union-extended the peer's stale tombstones straight back — every
//! transfer ROLLED THE GC BACK, and under sustained digest fallback the
//! cluster never converged (livelock; "sweep destroys information, merge
//! undoes the destruction").
//!
//! The certified sweep instead performs an information-EQUIVALENT
//! compression: after the gates pass it folds the marked tombstones into
//! a contiguous per-node floor (`floor[n] = c` ⟹ the fate of every dot
//! `(n, c' <= c)` is decided — live if present in the live sets, removed
//! otherwise), then drops the covered tombstones. The floor joins by
//! pointwise max on merge (irrevocable), rejects stale tombstones and
//! stale live dots, and kills live dots a lagging replica should have
//! removed. The same bucket transfer that used to roll GC back now
//! CARRIES the floor — one round trip heals the asymmetry.
//!
//! # Safety criterion
//!
//! A tombstone dot `(node_id, counter)` may be folded into the floor when
//! all known replicas have already incorporated the remove (they can
//! never again offer the removed dot as live state that would survive a
//! merge). That is what a purely local check can NEVER establish: a
//! replica partitioned away for longer than any wall-clock retention
//! window still holds the pre-remove state. Hence the gated
//! mark-and-sweep below. (Post-floor, a late stale live dot is killed by
//! the floor anyway — the floor closes the previously documented
//! unknown-replica residual — but the gate is still what authorises the
//! IRREVOCABLE floor advance for the marked dots.)
//!
//! # Gated mark-and-sweep
//!
//! [`TombstoneGc::mark_and_sweep`] runs in two passes:
//!
//! - **Mark**: snapshot the current deferred dots (per store key) and
//!   record the mark's wall-clock time `mark_ms`.
//! - **Sweep** (a later pass, at least `retention_period` after the
//!   mark): the CALLER evaluates its replica-synchronisation gates
//!   against `mark_ms` (see `NodeRunner::run_gc`: every authority's ack
//!   frontier AND every registered peer's push evidence must have passed
//!   `mark_ms`) and passes the verdict in. Only when the gates pass are
//!   the MARKED dots folded into the floor and physically dropped; dots
//!   that appeared after the mark wait for the next cycle
//!   (origin-retention — see `OrSet::compact_deferred_certified`).
//!
//! When the gates fail (partition, lagging authority, dead peer still in
//! the registry) the mark is simply KEPT and nothing is collected —
//! tombstones accumulate until the cluster heals (fail-closed).
//!
//! # Legacy holes and Stage 2 hole-jump
//!
//! Dots deleted by the pre-floor sweep are *holes*: at or below
//! `counters[n]`, neither live nor deferred. The floor walk stops at a
//! hole (Stage 1, fail-closed; observable via
//! `gc_floor_stalled_hole_dots`).
//! If any replica still holds the tombstone, a bucket transfer re-offers
//! it (it is uncovered, so it IS adopted), the hole fills, and the next
//! cycle collects it — self-healing. If the whole cluster swept it, the
//! floor for that (key, node) stalls permanently under Stage 1; Stage 2
//! (`allow_hole_jump`, `ASTEROIDB_GC_HOLE_JUMP=1`) lets the walk cross
//! holes once the caller's additional INBOUND gate holds: this node has
//! merged every registry peer's complete state since the mark, so a dot
//! that is still a hole is live on no known replica — i.e. removed.
//!
//! That argument only covers dots that were ALREADY holes at mark time:
//! an inbound partial delta can mint a NEW hole after the mark (counters
//! ride every delta in full, but an entry whose origin timestamp sits at
//! or below the requested frontier is filtered out — the receiver gains
//! the writer counter without the live dot), and the "complete pull
//! since the mark" evidence says nothing about such a dot, which may be
//! live on the pushing peer. The mark therefore snapshots each value's
//! per-node counters alongside the candidates, and the Stage 2 walk only
//! jumps holes at or below that snapshot (a dot that was live or
//! deferred at mark time can never become an above-floor hole, so a hole
//! at or below the snapshot provably existed at mark time — see
//! `advance_compaction_floor`). Post-mark holes stall the walk even
//! under Stage 2 and wait for the next mark.
//!
//! # HLC floors are forbidden (P1-10)
//!
//! The floor lives in DOT-COUNTER space and advances only through the
//! certified contiguous walk and merge inheritance. Never derive a floor
//! from HLC timestamps or frontier physicals: dot counters are small
//! per-writer integers (~10^0..10^6) while HLC physicals are Unix
//! milliseconds (~10^12), so an HLC-scale "floor" covers every dot and
//! bulk-deletes all tombstones (the original P1-10 bug). The legacy
//! `version_floor` / `global_floor` APIs that made this mistake
//! expressible were removed together with the pre-floor sweep.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use crate::crdt::SweepOutcome;
use crate::store::kv::{CrdtValue, Store};
use crate::types::NodeId;

/// Aggregated result of one [`TombstoneGc::mark_and_sweep`] pass across
/// the whole store (sum of the per-value [`SweepOutcome`]s).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SweepStats {
    /// Whether a sweep actually executed this pass (mark old enough AND
    /// gates passed). Mark-only and blocked passes return `false` with
    /// zeroed tallies; callers publishing "at the latest sweep" gauges
    /// (`gc_floor_stalled_*`) must skip those passes or a persistent
    /// stall would read as zero on every non-sweep tick.
    pub swept: bool,
    /// Tombstone dots physically removed (now represented by the floor).
    pub collected: u64,
    /// Floor walks stalled on a legacy hole (Stage 2 hole-jump resolves).
    pub stalled_holes: u64,
    /// Floor walks stalled on a post-mark tombstone (next cycle covers).
    pub stalled_uncandidated: u64,
}

/// Per-key mark snapshot: the tombstone candidates the next sweep may
/// collect, plus the per-node counters at mark time — the Stage 2
/// hole-jump ceilings (only holes that already existed at mark time may
/// be crossed; see the module docs).
#[derive(Debug, Clone)]
struct MarkSnapshot {
    candidates: HashSet<(NodeId, u64)>,
    counters_at_mark: HashMap<NodeId, u64>,
}

/// Configuration and state for tombstone garbage collection.
#[derive(Debug, Clone)]
pub struct TombstoneGc {
    /// Configurable interval between GC runs.
    pub gc_interval: Duration,
    /// Minimum time tombstones must be retained after creation.
    ///
    /// Even when the gates pass, a mark must be at least this old before
    /// its sweep may collect. This gives slow replicas extra time to merge.
    pub retention_period: Duration,
    /// Wall-clock millisecond timestamp of the last GC run.
    last_gc_ms: u64,
    /// Cumulative count of tombstones removed across all GC runs.
    total_collected: u64,
    /// Mark-and-sweep state: deferred dots and per-node counters
    /// snapshotted per store key at
    /// [`marked_at_ms`](Self::pending_mark_ms). Only the candidate dots
    /// may be collected by the next sweep, and Stage 2 may only jump
    /// holes at or below the counter snapshot.
    marked: HashMap<String, MarkSnapshot>,
    /// Wall-clock time of the pending mark; `None` when no mark is
    /// outstanding (the next pass will mark, not sweep).
    marked_at_ms: Option<u64>,
}

impl Default for TombstoneGc {
    fn default() -> Self {
        Self {
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
            gc_interval,
            retention_period,
            last_gc_ms: 0,
            total_collected: 0,
            marked: HashMap::new(),
            marked_at_ms: None,
        }
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
    ///   this pass SWEEPS: each value's certified sweep folds the marked
    ///   dots into its compaction floor and drops the covered tombstones
    ///   (see `OrSet::compact_deferred_certified`), then a fresh mark is
    ///   taken.
    /// - Otherwise (mark too young, or gates failed) nothing happens:
    ///   the mark is KEPT so the same `mark_ms` keeps being re-evaluated
    ///   — a partition or a lagging replica stalls collection entirely
    ///   (fail-closed) and it resumes automatically once the gates pass.
    ///
    /// `allow_hole_jump` (Stage 2) lets the floor walk cross legacy holes
    /// — restricted to holes at or below the counters snapshotted at
    /// mark time, so holes minted by inbound merges AFTER the mark stall
    /// regardless (see the module docs) — and is only sound under the
    /// caller's ADDITIONAL inbound gate (all registry peers' complete
    /// states merged since the mark); pass `false` otherwise
    /// (fail-closed).
    ///
    /// The two-pass structure is what makes the irrevocable floor advance
    /// safe against resurrection: every folded dot existed at mark time,
    /// and the gates prove every known replica consumed post-remove state
    /// from AFTER the mark — so no known replica can re-offer the dot as
    /// live state that a merge would accept. A purely wall-clock
    /// retention (the original design) could not exclude a replica
    /// partitioned for longer than the retention window.
    ///
    /// Returns the aggregated [`SweepStats`] for this pass
    /// (`swept == false` with zeroed tallies for a mark-only or blocked
    /// pass — callers must not publish those zeros as "latest sweep"
    /// observations).
    pub fn mark_and_sweep(
        &mut self,
        store: &mut Store,
        now_ms: u64,
        gates_passed: bool,
        allow_hole_jump: bool,
    ) -> SweepStats {
        let mut stats = SweepStats::default();
        let retention_ms = self.retention_period.as_millis() as u64;
        let sweep_ready = self
            .marked_at_ms
            .is_some_and(|mark| now_ms.saturating_sub(mark) >= retention_ms);

        if sweep_ready && gates_passed {
            stats.swept = true;
            for key in store.keys().into_iter().cloned().collect::<Vec<_>>() {
                let Some(snapshot) = self.marked.get(&key) else {
                    continue;
                };
                // Stage 2 may only jump holes that existed at mark time:
                // the mark-time counters bound the jumpable range.
                let ceilings = allow_hole_jump.then_some(&snapshot.counters_at_mark);
                if let Some(value) = store.get_mut(&key) {
                    let outcome = match value {
                        CrdtValue::Set(set) => {
                            set.compact_deferred_certified(&snapshot.candidates, ceilings)
                        }
                        CrdtValue::Map(map) => {
                            map.compact_deferred_certified(&snapshot.candidates, ceilings)
                        }
                        CrdtValue::Counter(_) | CrdtValue::Register(_) => {
                            // No tombstones for counters or registers.
                            SweepOutcome::default()
                        }
                    };
                    stats.collected += outcome.collected;
                    stats.stalled_holes += outcome.stalled_holes;
                    stats.stalled_uncandidated += outcome.stalled_uncandidated;
                }
            }
            // The mark is consumed regardless of how much was collected;
            // a fresh mark is taken below for the next cycle.
            self.marked.clear();
            self.marked_at_ms = None;
            if stats.collected > 0 {
                self.total_collected += stats.collected;
                self.last_gc_ms = now_ms;
            }
        }

        // (Re-)mark when no mark is outstanding. A blocked sweep
        // (gates_passed == false) deliberately KEEPS its mark: re-marking
        // would slide mark_ms forward and a cluster that heals more
        // slowly than the attempt cadence could never collect.
        if self.marked_at_ms.is_none() {
            let mut marked: HashMap<String, MarkSnapshot> = HashMap::new();
            for (key, value) in store.all_entries() {
                let (dots, counters) = match value {
                    CrdtValue::Set(set) => (set.deferred_dots(), set.counters().clone()),
                    CrdtValue::Map(map) => (map.deferred_dots(), map.counters().clone()),
                    CrdtValue::Counter(_) | CrdtValue::Register(_) => continue,
                };
                if !dots.is_empty() {
                    marked.insert(
                        key.clone(),
                        MarkSnapshot {
                            candidates: dots,
                            counters_at_mark: counters,
                        },
                    );
                }
            }
            self.marked = marked;
            self.marked_at_ms = Some(now_ms);
        }

        stats
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

    /// Build a store with one OrSet holding a tombstone dot (A,1):
    /// add x → remove x → add y (counter advances past 1).
    fn store_with_set_tombstone() -> Store {
        let n = node("A");
        let mut set = OrSet::new();
        set.add("x".to_string(), &n); // counter=1
        set.remove(&"x".to_string()); // dot (A,1) in deferred
        set.add("y".to_string(), &n); // counter=2
        assert_eq!(set.deferred_len(), 1);
        let mut store = Store::new();
        store.put("myset".into(), CrdtValue::Set(set));
        store
    }

    fn set_floor_for<'a>(store: &'a Store, key: &str, n: &NodeId) -> Option<&'a u64> {
        match store.get(key) {
            Some(CrdtValue::Set(s)) => s.compaction_floor().get(n),
            other => panic!("expected Set, got {other:?}"),
        }
    }

    const RET: u64 = 300_000; // 300s retention in ms

    #[test]
    fn mark_and_sweep_collects_or_set_after_mark_retention_and_gates() {
        let mut gc = TombstoneGc::new(Duration::from_secs(0), Duration::from_secs(300));
        let mut store = store_with_set_tombstone();

        // Pass 1: marks only — nothing is collected even with gates open.
        let stats = gc.mark_and_sweep(&mut store, 1_000, true, false);
        assert_eq!(stats.collected, 0, "first pass only marks");
        assert_eq!(gc.pending_mark_ms(), Some(1_000));

        // Pass 2 after retention with gates passed: sweep collects and
        // advances the floor over the tombstone and the live dot.
        let stats = gc.mark_and_sweep(&mut store, 1_000 + RET, true, false);
        assert_eq!(stats.collected, 1);
        assert_eq!(gc.total_collected(), 1);
        if let Some(CrdtValue::Set(s)) = store.get("myset") {
            assert_eq!(s.deferred_len(), 0);
        } else {
            panic!("expected Set");
        }
        assert_eq!(set_floor_for(&store, "myset", &node("A")), Some(&2));
        // A fresh mark was taken for the next cycle.
        assert_eq!(gc.pending_mark_ms(), Some(1_000 + RET));
    }

    #[test]
    fn mark_and_sweep_collects_or_map() {
        let mut gc = TombstoneGc::new(Duration::from_secs(0), Duration::from_secs(300));
        let mut store = Store::new();
        let n = node("A");

        let mut map: OrMap<String, String> = OrMap::new();
        let _ = map.set("k1".into(), "v1".into(), ts(100, 0, "A"), &n); // counter=1
        map.delete(&"k1".to_string()); // dot (A,1) in deferred
        let _ = map.set("k2".into(), "v2".into(), ts(200, 0, "A"), &n); // counter=2
        assert!(map.deferred_len() > 0);
        store.put("mymap".into(), CrdtValue::Map(map));

        assert_eq!(
            gc.mark_and_sweep(&mut store, 1_000, true, false).collected,
            0
        );
        let stats = gc.mark_and_sweep(&mut store, 1_000 + RET, true, false);
        assert!(stats.collected > 0);
        if let Some(CrdtValue::Map(m)) = store.get("mymap") {
            assert_eq!(m.deferred_len(), 0);
            assert_eq!(m.compaction_floor().get(&n), Some(&2));
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
        let _ = reg.set("hello".to_string(), ts(100, 0, "A"));
        store.put("reg".into(), CrdtValue::Register(reg));

        assert_eq!(
            gc.mark_and_sweep(&mut store, 1_000, true, false).collected,
            0
        );
        assert_eq!(
            gc.mark_and_sweep(&mut store, 1_000 + RET, true, false),
            SweepStats {
                swept: true,
                ..SweepStats::default()
            }
        );
    }

    /// The sweep may only run once the mark is at least `retention_period`
    /// old — a young mark is kept, not consumed.
    #[test]
    fn sweep_requires_mark_age_of_retention_period() {
        let mut gc = TombstoneGc::new(Duration::from_secs(0), Duration::from_secs(300));
        let mut store = store_with_set_tombstone();

        assert_eq!(
            gc.mark_and_sweep(&mut store, 1_000, true, false).collected,
            0
        ); // mark
        // Too young: even with gates open nothing is collected and the
        // mark is retained.
        assert_eq!(
            gc.mark_and_sweep(&mut store, 2_000, true, false).collected,
            0
        );
        assert_eq!(gc.pending_mark_ms(), Some(1_000), "young mark is kept");
        // Old enough: collect.
        assert_eq!(
            gc.mark_and_sweep(&mut store, 1_000 + RET, true, false)
                .collected,
            1
        );
    }

    /// C-2 regression (resurrection prevention): while any known replica
    /// has NOT synchronised past the mark (gates fail — e.g. a network
    /// partition longer than the retention window), the sweep must not
    /// collect and the floor must not advance. A lagging replica's stale
    /// state can then still be merged WITHOUT resurrecting the removed
    /// element; under the old wall-clock-only design the tombstone would
    /// already be gone and the remove would silently undo itself
    /// cluster-wide.
    #[test]
    fn blocked_gates_prevent_resurrection_after_long_partition() {
        let n = node("A");

        // Replica state before the partition: both sides hold {x}.
        let mut local = OrSet::new();
        local.add("x".to_string(), &n); // dot (A,1)
        let lagging_replica: OrSet<String> = local.clone();

        // Local removes x during the partition (tombstone A,1) and keeps
        // writing.
        local.remove(&"x".to_string());
        local.add("y".to_string(), &n); // dot (A,2)
        let mut store = Store::new();
        store.put("myset".into(), CrdtValue::Set(local));

        let mut gc = TombstoneGc::new(Duration::from_secs(0), Duration::from_secs(300));
        assert_eq!(
            gc.mark_and_sweep(&mut store, 1_000, false, false).collected,
            0
        ); // mark

        // The partition outlives the retention window: gates still fail,
        // so NOTHING is collected — no matter how much wall-clock time
        // has passed — and the floor does not advance (the advance is
        // irrevocable, so it must be gate-authorised).
        assert_eq!(
            gc.mark_and_sweep(&mut store, 1_000 + 10 * RET, false, false)
                .collected,
            0,
            "gates must stall collection through arbitrarily long partitions"
        );
        assert_eq!(
            gc.pending_mark_ms(),
            Some(1_000),
            "the blocked mark is kept, not re-taken"
        );
        assert_eq!(
            set_floor_for(&store, "myset", &n),
            None,
            "a blocked sweep must not advance the floor"
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
        let stats = gc.mark_and_sweep(&mut store, 1_000 + 11 * RET, true, false);
        assert_eq!(stats.collected, 1, "healing the cluster resumes collection");
        if let Some(CrdtValue::Set(s)) = store.get("myset") {
            assert_eq!(s.deferred_len(), 0);
            assert!(!s.contains(&"x".to_string()));
            assert_eq!(s.compaction_floor().get(&n), Some(&2));
        }
    }

    /// The sweep only collects dots that existed at mark time: tombstones
    /// created after the mark survive (the floor walk stalls on them) and
    /// wait for the next cycle, whose gate will cover them.
    #[test]
    fn sweep_only_collects_marked_dots() {
        let n = node("A");
        let mut gc = TombstoneGc::new(Duration::from_secs(0), Duration::from_secs(300));
        let mut store = store_with_set_tombstone(); // tombstone (A,1)

        assert_eq!(
            gc.mark_and_sweep(&mut store, 1_000, true, false).collected,
            0
        ); // marks (A,1)

        // A remove AFTER the mark: dot (A,2) enters deferred, followed by
        // a later add (A,3).
        if let Some(CrdtValue::Set(s)) = store.get_mut("myset") {
            s.remove(&"y".to_string());
            s.add("z".to_string(), &n);
            assert_eq!(s.deferred_len(), 2);
        }

        // Sweep collects only the marked (A,1); the younger (A,2) stalls
        // the floor walk and stays.
        let stats = gc.mark_and_sweep(&mut store, 1_000 + RET, true, false);
        assert_eq!(stats.collected, 1);
        assert_eq!(stats.stalled_uncandidated, 1);
        if let Some(CrdtValue::Set(s)) = store.get("myset") {
            assert_eq!(s.deferred_len(), 1, "post-mark tombstone must survive");
            assert_eq!(s.compaction_floor().get(&n), Some(&1));
        }

        // The re-mark taken at sweep time covers it for the next cycle.
        let stats = gc.mark_and_sweep(&mut store, 1_000 + 2 * RET, true, false);
        assert_eq!(stats.collected, 1);
        if let Some(CrdtValue::Set(s)) = store.get("myset") {
            assert_eq!(s.deferred_len(), 0);
            assert_eq!(s.compaction_floor().get(&n), Some(&3));
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

        assert_eq!(
            gc.mark_and_sweep(&mut store, 1_000, true, false).collected,
            0
        );
        assert_eq!(
            gc.mark_and_sweep(&mut store, 1_000 + RET, true, false)
                .collected,
            2
        );
        assert_eq!(gc.total_collected(), 2);
    }

    /// `last_gc_ms` advances only when a sweep collects, so `should_run`
    /// keeps attempting while marks are pending or gates are blocked.
    #[test]
    fn last_gc_ms_advances_only_on_collection() {
        let mut gc = TombstoneGc::new(Duration::from_secs(0), Duration::from_secs(300));
        let mut store = store_with_set_tombstone();

        assert_eq!(gc.last_gc_ms(), 0);
        gc.mark_and_sweep(&mut store, 5_000, true, false); // mark
        assert_eq!(gc.last_gc_ms(), 0, "marking must not advance last_gc_ms");

        gc.mark_and_sweep(&mut store, 5_000 + RET, false, false); // blocked sweep
        assert_eq!(gc.last_gc_ms(), 0, "blocked sweep must not advance");

        let stats = gc.mark_and_sweep(&mut store, 5_000 + 2 * RET, true, false);
        assert_eq!(stats.collected, 1);
        assert_eq!(gc.last_gc_ms(), 5_000 + 2 * RET);
    }

    /// Stage 2 hole-jump: a legacy hole (dot deleted by the pre-floor
    /// sweep) stalls the floor under Stage 1 and is crossed under Stage 2,
    /// unblocking collection of later tombstones for the same writer.
    #[test]
    fn hole_jump_unblocks_legacy_holes_only_when_allowed() {
        let n = node("A");
        // Legacy state: counters A=2, live (A,2); (A,1) is a hole.
        let json = r#"{"elements":{"y":[{"node_id":"A","counter":2}]},"counters":{"A":2}}"#;
        let mut set: OrSet<String> = serde_json::from_str(json).unwrap();
        // A new remove creates a tombstone above the hole.
        set.remove(&"y".to_string()); // tombstone (A,2)
        let mut store = Store::new();
        store.put("myset".into(), CrdtValue::Set(set));

        let mut gc = TombstoneGc::new(Duration::from_secs(0), Duration::from_secs(300));
        gc.mark_and_sweep(&mut store, 1_000, true, false); // mark (A,2)

        // Stage 1: the hole stalls the walk; nothing is collected
        // (fail-closed) and the stall is observable.
        let stats = gc.mark_and_sweep(&mut store, 1_000 + RET, true, false);
        assert_eq!(stats.collected, 0);
        assert_eq!(stats.stalled_holes, 1);
        assert_eq!(set_floor_for(&store, "myset", &n), None);

        // Stage 2 (inbound gate passed): the hole is jumped, the floor
        // reaches the tombstone and collection proceeds — but never past
        // counters[n].
        let stats = gc.mark_and_sweep(&mut store, 1_000 + 2 * RET, true, true);
        assert_eq!(stats.collected, 1);
        assert_eq!(stats.stalled_holes, 0);
        assert_eq!(set_floor_for(&store, "myset", &n), Some(&2));
        if let Some(CrdtValue::Set(s)) = store.get("myset") {
            assert_eq!(s.deferred_len(), 0);
        }
    }

    /// `swept` is set ONLY on a pass that actually executed a sweep:
    /// mark-only and gate-blocked passes report `swept == false` (with
    /// zeroed tallies), so callers publishing "latest sweep" stall gauges
    /// can skip them — otherwise a persistent hole stall would read as 0
    /// on every non-sweep tick (ops-guide 3.7's Stage 2 signal).
    #[test]
    fn swept_flag_marks_executed_sweeps_only() {
        let n = node("A");
        let mut gc = TombstoneGc::new(Duration::from_secs(0), Duration::from_secs(300));
        // Legacy hole state (A,1) + tombstone (A,2): the sweep stalls on
        // the hole under Stage 1, so stall tallies stay non-trivial.
        let json = r#"{"elements":{"y":[{"node_id":"A","counter":2}]},"counters":{"A":2}}"#;
        let mut set: OrSet<String> = serde_json::from_str(json).unwrap();
        set.remove(&"y".to_string());
        let mut store = Store::new();
        store.put("myset".into(), CrdtValue::Set(set));

        // Mark-only pass: no sweep executed.
        let stats = gc.mark_and_sweep(&mut store, 1_000, true, false);
        assert!(!stats.swept, "mark-only pass must not report a sweep");

        // Retention not reached: no sweep executed.
        let stats = gc.mark_and_sweep(&mut store, 2_000, true, false);
        assert!(!stats.swept, "pre-retention pass must not report a sweep");

        // Gates blocked: no sweep executed.
        let stats = gc.mark_and_sweep(&mut store, 1_000 + RET, false, false);
        assert!(!stats.swept, "blocked pass must not report a sweep");

        // Gates pass after retention: the sweep runs (and stalls on the
        // legacy hole — the stall is part of an EXECUTED sweep's stats).
        let stats = gc.mark_and_sweep(&mut store, 1_000 + 2 * RET, true, false);
        assert!(stats.swept);
        assert_eq!(stats.stalled_holes, 1);
        assert_eq!(set_floor_for(&store, "myset", &n), None);
    }

    /// Stage 2 must not jump holes minted AFTER the mark: the mark
    /// snapshots per-node counters and the sweep only jumps holes at or
    /// below that snapshot, even when the caller's inbound gate allowed
    /// hole-jumping (the gate's pull evidence predates the new hole).
    #[test]
    fn hole_jump_is_bounded_by_mark_time_counters() {
        let n = node("A");
        let mut store = store_with_set_tombstone(); // tombstone (A,1), live (A,2)
        let mut gc = TombstoneGc::new(Duration::from_secs(0), Duration::from_secs(300));
        gc.mark_and_sweep(&mut store, 1_000, true, true); // mark: counters {A:2}

        // AFTER the mark, an inbound partial delta mints holes: counters
        // jump to A=5 with no live/deferred dots for (A,3)..(A,5) — any
        // of them may be live on the pushing peer.
        if let Some(CrdtValue::Set(s)) = store.get_mut("myset") {
            let counters_only: OrSet<String> = serde_json::from_str(
                r#"{"elements":{},"counters":{"A":5},"deferred":[],"compaction_floor":{}}"#,
            )
            .unwrap();
            s.merge(&counters_only);
        }

        // Stage 2 sweep: collects the marked tombstone, walks over the
        // mark-time range, and stalls at the first post-mark hole (A,3)
        // instead of jumping to A=5.
        let stats = gc.mark_and_sweep(&mut store, 1_000 + RET, true, true);
        assert!(stats.swept);
        assert_eq!(stats.collected, 1);
        assert_eq!(stats.stalled_holes, 1, "post-mark hole must stall");
        assert_eq!(
            set_floor_for(&store, "myset", &n),
            Some(&2),
            "the floor must stop at the mark-time counter snapshot"
        );
    }

    /// Crash-shaped floor regression: a snapshot taken before a sweep
    /// "loses" the floor advance. Recovery is conservative (tombstones
    /// re-appear, floor is lower), a peer merge restores the floor via
    /// pointwise max, and re-sweeping is harmless.
    #[test]
    fn floor_regression_recovers_via_peer_merge_and_resweep() {
        let n = node("A");
        let mut set = OrSet::new();
        set.add("x".to_string(), &n);
        set.remove(&"x".to_string());
        set.add("y".to_string(), &n);

        let pre_sweep = set.clone(); // "snapshot" before the sweep
        set.compact_deferred_certified(&set.deferred_dots(), None);
        let post_sweep = set.clone(); // peer state after the sweep

        // Crash: revert to the pre-sweep snapshot (floor lost — the
        // fail-safe direction: tombstones are back, nothing was over-GCed).
        let mut recovered = pre_sweep;
        assert!(recovered.compaction_floor().is_empty());
        assert_eq!(recovered.deferred_len(), 1);

        // Peer merge restores the floor (max) and absorbs the tombstone.
        recovered.merge(&post_sweep);
        assert_eq!(recovered.compaction_floor().get(&n), Some(&2));
        assert_eq!(recovered.deferred_len(), 0);

        // A second sweep over the recovered state is a no-op.
        let outcome = recovered.compact_deferred_certified(&recovered.deferred_dots(), None);
        assert_eq!(outcome, crate::crdt::SweepOutcome::default());
        assert!(!recovered.contains(&"x".to_string()));
        assert!(recovered.contains(&"y".to_string()));
    }
}
