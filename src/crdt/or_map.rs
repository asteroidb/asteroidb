use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::crdt::lww_register::LwwRegister;
use crate::crdt::{MergeEffects, SweepOutcome, covered};
use crate::hlc::HlcTimestamp;
use crate::types::NodeId;

/// A unique event identifier (node, counter) for OR-Set semantics.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct Dot {
    node_id: NodeId,
    counter: u64,
}

/// Observed-Remove Map (FR-005).
///
/// Combines OR-Set semantics for key presence (add-wins on concurrent
/// add/remove) with LWW-Register for values. Each key tracks its causal
/// dots so that concurrent `set` and `delete` operations resolve correctly:
/// a `set` that is concurrent with a `delete` will re-add the key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone + Ord,
{
    /// Active entries: key -> (dots that justify presence, LWW value).
    entries: HashMap<K, (HashSet<Dot>, LwwRegister<V>)>,
    /// Per-node monotonic counters for generating unique dots.
    counters: HashMap<NodeId, u64>,
    /// Tombstone: all dots that have ever been removed.
    /// Needed so merge can tell "this dot was deleted" vs "never seen".
    deferred: HashSet<Dot>,
    /// Per-node GC floor — same semantics and invariants (INV-FLOOR,
    /// INV-W, INV-CTR, origin-retention) as
    /// [`OrSet::compaction_floor`](crate::crdt::or_set::OrSet), which
    /// carries the full documentation. NOTE the OrMap-specific INV-W
    /// consequence: [`delta_since`](Self::delta_since) is a PARTIAL
    /// payload here (entries newer than the frontier only), so it must
    /// ship an EMPTY floor — unlike `OrSet::delta_since`, which is a full
    /// clone and keeps it.
    #[serde(default)]
    compaction_floor: HashMap<NodeId, u64>,
}

/// Frozen structural layout of [`OrMap`] as persisted before the
/// `compaction_floor` field existed (snapshot formats v1–v4, WAL format
/// v1). bincode is positional, so old payloads must be decoded with
/// exactly this layout and converted (`From`) with an empty floor.
#[derive(Debug, Deserialize)]
pub(crate) struct OrMapV4Layout<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone + Ord,
{
    entries: HashMap<K, (HashSet<Dot>, LwwRegister<V>)>,
    counters: HashMap<NodeId, u64>,
    deferred: HashSet<Dot>,
}

impl<K, V> From<OrMapV4Layout<K, V>> for OrMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone + Ord,
{
    fn from(old: OrMapV4Layout<K, V>) -> Self {
        OrMap {
            entries: old.entries,
            counters: old.counters,
            deferred: old.deferred,
            compaction_floor: HashMap::new(),
        }
    }
}

impl<K, V> OrMap<K, V>
where
    K: Eq + Hash + Clone + Serialize + DeserializeOwned,
    V: Clone + Ord + Serialize + DeserializeOwned,
{
    /// Create an empty OR-Map.
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            counters: HashMap::new(),
            deferred: HashSet::new(),
            compaction_floor: HashMap::new(),
        }
    }

    /// Generate a fresh dot for the given node.
    fn next_dot(&mut self, node_id: &NodeId) -> Dot {
        let counter = self.counters.entry(node_id.clone()).or_insert(0);
        *counter += 1;
        Dot {
            node_id: node_id.clone(),
            counter: *counter,
        }
    }

    /// Set a key to a value with add-wins semantics.
    ///
    /// This removes existing dots for the key (superseding prior presence)
    /// and adds a fresh dot. The value is updated via LWW-Register.
    ///
    /// Returns `true` if the value was updated, `false` if the timestamp
    /// was stale compared to the current LWW-Register value. When `false`
    /// is returned, no dots are modified to prevent inconsistency between
    /// key presence and the register value.
    pub fn set(&mut self, key: K, value: V, timestamp: HlcTimestamp, node_id: &NodeId) -> bool {
        // Pre-check: if the key already has a higher or equal timestamp,
        // skip the entire operation to avoid adding a dot without updating
        // the register value.
        if let Some(entry) = self.entries.get(&key)
            && timestamp <= *entry.1.timestamp()
        {
            return false;
        }

        let dot = self.next_dot(node_id);

        let entry = self.entries.entry(key).or_insert_with(|| {
            let reg = LwwRegister::new();
            (HashSet::new(), reg)
        });

        // Remove old dots for this key (current set supersedes them).
        let old_dots: Vec<Dot> = entry.0.drain().collect();
        for d in old_dots {
            self.deferred.insert(d);
        }

        // Add the new dot and update the register value.
        entry.0.insert(dot);
        entry.1.set(value, timestamp);
        true
    }

    /// Delete a key using OR-Set remove semantics.
    ///
    /// All currently observed dots for the key are moved to the deferred
    /// (tombstone) set. A concurrent `set` on another node that introduces
    /// a dot not in the deferred set will cause the key to reappear after
    /// merge (add-wins).
    pub fn delete(&mut self, key: &K) {
        if let Some((dots, _)) = self.entries.remove(key) {
            for d in dots {
                self.deferred.insert(d);
            }
        }
    }

    /// Get a reference to the value associated with a key.
    pub fn get(&self, key: &K) -> Option<&V> {
        self.entries
            .get(key)
            .and_then(|(dots, reg)| if dots.is_empty() { None } else { reg.get() })
    }

    /// Check whether a key is present.
    pub fn contains_key(&self, key: &K) -> bool {
        self.entries
            .get(key)
            .is_some_and(|(dots, _)| !dots.is_empty())
    }

    /// Return all currently present keys.
    pub fn keys(&self) -> Vec<&K> {
        self.entries
            .iter()
            .filter(|(_, (dots, _))| !dots.is_empty())
            .map(|(k, _)| k)
            .collect()
    }

    /// Merge another OR-Map into this one, returning floor diagnostics.
    ///
    /// For each key:
    /// - Dots present in the other but not in our deferred set are added,
    ///   unless they are stale (covered by our pre-merge compaction floor
    ///   and not already held live — certified-removed dots re-offered).
    /// - Dots present in ours but in the other's deferred set are
    ///   removed, as are dots covered by the other's floor while absent
    ///   from the other's live set for the key (floor kill; sound only
    ///   under INV-W — a non-empty floor rides complete live sets only).
    /// - LWW-Register values are merged by timestamp.
    ///
    /// This ensures add-wins semantics: if node A deletes a key while node B
    /// concurrently sets it, the set wins because B's dot is not in A's
    /// deferred set (and is above every floor).
    ///
    /// Counters and floors merge by pointwise max; the deferred set
    /// merges as a union minus floor-covered dots, with the same
    /// origin-retention exception as
    /// [`OrSet::merge`](crate::crdt::or_set::OrSet::merge) (which carries
    /// the full rationale).
    pub fn merge(&mut self, other: &OrMap<K, V>) -> MergeEffects {
        let mut fx = MergeEffects::default();
        let floor_pre = self.compaction_floor.clone();

        // `fx.changed` is set at EVERY site that mutates physical state
        // and nowhere else (see `OrSet::merge` for the full contract):
        // `changed == false` guarantees `pre == post` — the RR gate (M-6).

        for (key, (other_dots, other_reg)) in &other.entries {
            if let Some(entry) = self.entries.get_mut(key) {
                // Key exists in both — merge dots and register in place.

                // Add dots from other that we haven't tombstoned and that
                // are not stale under our pre-merge floor.
                for dot in other_dots {
                    if self.deferred.contains(dot) || entry.0.contains(dot) {
                        continue;
                    }
                    if covered(&floor_pre, &dot.node_id, dot.counter) {
                        fx.rejected_stale_live += 1;
                        continue;
                    }
                    entry.0.insert(dot.clone());
                    fx.changed = true;
                }

                // Remove our dots that the other has tombstoned or
                // floor-compacted away.
                entry.0.retain(|dot| {
                    if other.deferred.contains(dot) {
                        fx.changed = true;
                        return false;
                    }
                    if covered(&other.compaction_floor, &dot.node_id, dot.counter)
                        && !other_dots.contains(dot)
                    {
                        fx.killed_by_floor += 1;
                        fx.changed = true;
                        return false;
                    }
                    true
                });

                // Merge LWW value.
                if entry.1.merge(other_reg) {
                    fx.changed = true;
                }
            } else {
                // Key only in other — filter the dots FIRST and adopt
                // nothing (not even the register) when every dot is
                // deferred/stale. The previous in-place formulation
                // created an empty entry, merged the register into it and
                // relied on the retain below to delete the entry again —
                // a net no-op that a naive `changed` instrumentation
                // would misreport as a change on every round.
                let mut filtered: HashSet<Dot> = HashSet::new();
                for dot in other_dots {
                    if self.deferred.contains(dot) {
                        continue;
                    }
                    if covered(&floor_pre, &dot.node_id, dot.counter) {
                        fx.rejected_stale_live += 1;
                        continue;
                    }
                    filtered.insert(dot.clone());
                }
                if !filtered.is_empty() {
                    let mut reg = LwwRegister::new();
                    reg.merge(other_reg);
                    self.entries.insert(key.clone(), (filtered, reg));
                    fx.changed = true;
                }
            }
        }

        // Apply other's tombstones and floor to self-only entries (keys
        // not in other.entries — trivially absent from its live set).
        for (key, (dots, _)) in &mut self.entries {
            if !other.entries.contains_key(key) {
                dots.retain(|dot| {
                    if other.deferred.contains(dot) {
                        fx.changed = true;
                        return false;
                    }
                    if covered(&other.compaction_floor, &dot.node_id, dot.counter) {
                        fx.killed_by_floor += 1;
                        fx.changed = true;
                        return false;
                    }
                    true
                });
            }
        }

        // Remove entries with no remaining dots. (Not a `changed` site:
        // an entry can only become empty through a dot-dropping retain
        // above, which already flagged.)
        self.entries.retain(|_, (dots, _)| !dots.is_empty());

        // Merge counters (take max). Only an actual raise mutates state;
        // no ghost zero entries (see OrSet::merge).
        for (node_id, &counter) in &other.counters {
            match self.counters.get_mut(node_id) {
                Some(our_counter) => {
                    if counter > *our_counter {
                        *our_counter = counter;
                        fx.changed = true;
                    }
                }
                None => {
                    if counter > 0 {
                        self.counters.insert(node_id.clone(), counter);
                        fx.changed = true;
                    }
                }
            }
        }

        // Merge the floor (pointwise max, irrevocable), then clamp
        // counters to the floor (INV-CTR).
        for (node_id, &f) in &other.compaction_floor {
            match self.compaction_floor.get_mut(node_id) {
                Some(entry) => {
                    if f > *entry {
                        *entry = f;
                        fx.changed = true;
                    }
                }
                None => {
                    if f > 0 {
                        self.compaction_floor.insert(node_id.clone(), f);
                        fx.changed = true;
                    }
                }
            }
        }
        for (node_id, &f) in &self.compaction_floor {
            if f == 0 {
                continue;
            }
            match self.counters.get_mut(node_id) {
                Some(counter) => {
                    if *counter < f {
                        *counter = f;
                        fx.changed = true;
                    }
                }
                None => {
                    self.counters.insert(node_id.clone(), f);
                    fx.changed = true;
                }
            }
        }

        // Merge deferred sets under the merged floor (origin-retention:
        // keep own fresh tombstones already covered pre-merge; see
        // OrSet::merge).
        let floor_merged = &self.compaction_floor;
        self.deferred.retain(|d| {
            let keep = !covered(floor_merged, &d.node_id, d.counter)
                || covered(&floor_pre, &d.node_id, d.counter);
            if !keep {
                fx.changed = true;
            }
            keep
        });
        for d in &other.deferred {
            if covered(&self.compaction_floor, &d.node_id, d.counter) {
                if !self.deferred.contains(d) {
                    fx.rejected_covered_deferred += 1;
                }
            } else if self.deferred.insert(d.clone()) {
                fx.changed = true;
            }
        }

        debug_assert!(
            fx.killed_by_floor == 0 || fx.changed,
            "a floor kill always removes a dot, so it must imply changed"
        );
        fx
    }

    /// Merge a delta into this map.
    ///
    /// For OrMap, `merge_delta` is identical to `merge` because the delta
    /// is the same type (a subset of entries and deferred dots).
    pub fn merge_delta(&mut self, delta: &OrMap<K, V>) -> MergeEffects {
        self.merge(delta)
    }

    /// Extract changes since the given frontier timestamp.
    ///
    /// OrMap entries carry LWW-Register timestamps, so this method returns
    /// only entries whose register timestamp is strictly greater than
    /// `frontier`, along with any tombstones. Returns `None` when there
    /// are no entries or tombstones newer than the frontier.
    ///
    /// Unlike `OrSet::delta_since` (a full clone), this is a PARTIAL
    /// payload: entries at or below the frontier are omitted. The
    /// `compaction_floor` is therefore deliberately EMPTY (INV-W) — a
    /// receiver applying the floor kill rule against this incomplete live
    /// set would destroy its own live entries that simply were not part
    /// of the delta.
    pub fn delta_since(&self, frontier: &HlcTimestamp) -> Option<Self> {
        let mut delta = OrMap {
            entries: HashMap::new(),
            counters: self.counters.clone(),
            deferred: self.deferred.clone(),
            // INV-W: partial payload — never ship the floor.
            compaction_floor: HashMap::new(),
        };
        let mut has_entries = false;

        for (key, (dots, reg)) in &self.entries {
            if !dots.is_empty() && *reg.timestamp() > *frontier {
                delta
                    .entries
                    .insert(key.clone(), (dots.clone(), reg.clone()));
                has_entries = true;
            }
        }

        if !has_entries && delta.deferred.is_empty() {
            return None;
        }
        Some(delta)
    }

    /// Compute a true incremental delta against a known old state.
    ///
    /// Returns an OrMap containing only:
    /// - Entries whose dots are NOT present in `old`
    /// - Entries whose LWW-Register has a newer timestamp than in `old`
    /// - Deferred (tombstone) dots NOT present in `old`
    /// - Updated counters
    ///
    /// The `compaction_floor` is deliberately EMPTY (INV-W): partial
    /// payloads never carry the floor (see [`delta_since`](Self::delta_since)).
    ///
    /// Returns `None` if there are no changes.
    pub fn delta_from(&self, old: &OrMap<K, V>) -> Option<Self>
    where
        V: PartialEq,
    {
        let mut delta = OrMap {
            entries: HashMap::new(),
            counters: HashMap::new(),
            deferred: HashSet::new(),
            // INV-W: partial payload — never ship the floor.
            compaction_floor: HashMap::new(),
        };
        let mut has_changes = false;

        // Collect all dots in old state for comparison.
        let old_all_dots: HashSet<&Dot> = old
            .entries
            .values()
            .flat_map(|(dots, _)| dots.iter())
            .collect();

        for (key, (dots, reg)) in &self.entries {
            // Check if this entry has new dots or a newer register value.
            let new_dots: HashSet<Dot> = dots
                .iter()
                .filter(|d| !old_all_dots.contains(d))
                .cloned()
                .collect();

            let reg_changed = match old.entries.get(key) {
                Some((_, old_reg)) => *reg.timestamp() > *old_reg.timestamp(),
                None => true,
            };

            if !new_dots.is_empty() || reg_changed {
                delta
                    .entries
                    .insert(key.clone(), (dots.clone(), reg.clone()));
                has_changes = true;
            }
        }

        // Find new tombstones.
        for d in &self.deferred {
            if !old.deferred.contains(d) {
                delta.deferred.insert(d.clone());
                has_changes = true;
            }
        }

        // Include updated counters.
        for (node_id, &counter) in &self.counters {
            let old_counter = old.counters.get(node_id).copied().unwrap_or(0);
            if counter > old_counter {
                delta.counters.insert(node_id.clone(), counter);
                has_changes = true;
            }
        }

        if has_changes { Some(delta) } else { None }
    }

    /// Return the number of dots currently in the tombstone (deferred) set.
    ///
    /// Useful for monitoring GC effectiveness.
    pub fn deferred_len(&self) -> usize {
        self.deferred.len()
    }

    /// Return the number of deferred dots NOT covered by the compaction
    /// floor — the canonical tombstone count (what the digest sees).
    pub fn uncovered_deferred_len(&self) -> usize {
        self.deferred
            .iter()
            .filter(|d| !covered(&self.compaction_floor, &d.node_id, d.counter))
            .count()
    }

    /// The per-node compaction floor (see the field docs for semantics).
    pub fn compaction_floor(&self) -> &HashMap<NodeId, u64> {
        &self.compaction_floor
    }

    /// The per-node add counters (highest dot counter ever allocated per
    /// writer). The tombstone-GC mark phase snapshots these as the Stage 2
    /// hole-jump ceilings (see [`Self::compact_deferred_certified`]).
    pub fn counters(&self) -> &HashMap<NodeId, u64> {
        &self.counters
    }

    /// Snapshot the deferred (tombstone) dots as `(node_id, counter)`
    /// pairs — the MARK phase of the gated mark-and-sweep tombstone GC.
    ///
    /// See [`OrSet::deferred_dots`] for semantics.
    ///
    /// [`OrSet::deferred_dots`]: crate::crdt::or_set::OrSet::deferred_dots
    pub fn deferred_dots(&self) -> HashSet<(NodeId, u64)> {
        self.deferred
            .iter()
            .map(|d| (d.node_id.clone(), d.counter))
            .collect()
    }

    /// Certified sweep: fold the MARKED tombstones into a contiguous
    /// advance of the per-node compaction floor, then physically drop the
    /// tombstones the floor now covers (candidates only).
    ///
    /// See [`OrSet::compact_deferred_certified`] for the full walk
    /// semantics, gating requirements, hole-jump precondition and
    /// origin-retention argument.
    ///
    /// [`OrSet::compact_deferred_certified`]: crate::crdt::or_set::OrSet::compact_deferred_certified
    pub fn compact_deferred_certified(
        &mut self,
        candidates: &HashSet<(NodeId, u64)>,
        hole_jump_ceilings: Option<&HashMap<NodeId, u64>>,
    ) -> SweepOutcome {
        let live_dots: HashSet<(&NodeId, u64)> = self
            .entries
            .values()
            .flat_map(|(dots, _)| dots.iter())
            .map(|d| (&d.node_id, d.counter))
            .collect();
        let deferred_dots: HashSet<(&NodeId, u64)> = self
            .deferred
            .iter()
            .map(|d| (&d.node_id, d.counter))
            .collect();

        let mut outcome = crate::crdt::advance_compaction_floor(
            &live_dots,
            &deferred_dots,
            &self.counters,
            &mut self.compaction_floor,
            candidates,
            hole_jump_ceilings,
        );

        // Physical deletion is candidate-gated (origin-retention).
        let before = self.deferred.len();
        let floor = &self.compaction_floor;
        self.deferred.retain(|d| {
            !(covered(floor, &d.node_id, d.counter)
                && candidates.contains(&(d.node_id.clone(), d.counter)))
        });
        outcome.collected = (before - self.deferred.len()) as u64;
        outcome
    }

    /// Return the number of present keys.
    pub fn len(&self) -> usize {
        self.entries
            .iter()
            .filter(|(_, (dots, _))| !dots.is_empty())
            .count()
    }

    /// Check whether the map has no present keys.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl OrMap<String, String> {
    /// Feed this map's canonical byte representation into `hasher`
    /// (digest-based anti-entropy, scheme v2).
    ///
    /// Stream: `0x04` ‖ live entries (key byte order), each as
    /// `str(key)` ‖ dots (dot order) ‖ LWW-register canonical stream ‖
    /// counters (node-id order) ‖ compaction floor (node-id order) ‖
    /// UNCOVERED deferred dots (dot order). Entries whose dot set is
    /// empty are skipped (normalisation: semantically equal to absent
    /// entries). Deferred dots covered by the floor are excluded — see
    /// [`OrSet::digest_into`](crate::crdt::or_set::OrSet::digest_into)
    /// for the canonical-form argument.
    ///
    /// # MAINTAINER CONTRACT
    /// Adding a field to `OrMap`/`Dot` REQUIRES updating this method and
    /// bumping `crate::store::digest::DIGEST_SCHEME_VERSION` — otherwise
    /// replicas that differ only in the new field report "digest matched"
    /// and session-guarantee claims become unsound ("digest matched" is
    /// defined as CANONICAL state equality). Instantiating `OrMap`
    /// for new key/value types in `CrdtValue` requires defining their
    /// canonical byte encoding here, plus a scheme version bump.
    pub(crate) fn digest_into(&self, hasher: &mut sha2::Sha256) {
        use crate::crdt::digest::{write_counters, write_dots, write_str, write_u32};
        use sha2::Digest as _;

        hasher.update([0x04]);
        type MapEntryRef<'a> = (&'a String, &'a (HashSet<Dot>, LwwRegister<String>));
        let mut items: Vec<MapEntryRef<'_>> = self
            .entries
            .iter()
            .filter(|(_, (dots, _))| !dots.is_empty())
            .collect();
        items.sort_unstable_by(|a, b| a.0.cmp(b.0));
        write_u32(hasher, items.len() as u32);
        for (key, (dots, reg)) in items {
            write_str(hasher, key);
            write_dots(
                hasher,
                dots.iter().map(|d| (d.node_id.0.as_str(), d.counter)),
            );
            reg.digest_into(hasher);
        }
        write_counters(hasher, &self.counters);
        write_counters(hasher, &self.compaction_floor);
        write_dots(
            hasher,
            self.deferred
                .iter()
                .filter(|d| !covered(&self.compaction_floor, &d.node_id, d.counter))
                .map(|d| (d.node_id.0.as_str(), d.counter)),
        );
    }
}

impl<K, V> Default for OrMap<K, V>
where
    K: Eq + Hash + Clone + Serialize + DeserializeOwned,
    V: Clone + Ord + Serialize + DeserializeOwned,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(physical: u64, logical: u32, node: &str) -> HlcTimestamp {
        HlcTimestamp {
            physical,
            logical,
            node_id: node.into(),
        }
    }

    fn node(id: &str) -> NodeId {
        NodeId(id.into())
    }

    #[test]
    fn new_map_is_empty() {
        let map: OrMap<String, i32> = OrMap::new();
        assert!(map.is_empty());
        assert_eq!(map.len(), 0);
    }

    #[test]
    fn set_and_get() {
        let mut map = OrMap::new();
        map.set(
            "key1".to_string(),
            42,
            ts(100, 0, "node-a"),
            &node("node-a"),
        );
        assert_eq!(map.get(&"key1".to_string()), Some(&42));
        assert!(map.contains_key(&"key1".to_string()));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn set_overwrites_value() {
        let mut map = OrMap::new();
        map.set("k".to_string(), 1, ts(100, 0, "node-a"), &node("node-a"));
        map.set("k".to_string(), 2, ts(200, 0, "node-a"), &node("node-a"));
        assert_eq!(map.get(&"k".to_string()), Some(&2));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn delete_removes_key() {
        let mut map = OrMap::new();
        map.set("k".to_string(), 1, ts(100, 0, "node-a"), &node("node-a"));
        assert!(map.contains_key(&"k".to_string()));

        map.delete(&"k".to_string());
        assert!(!map.contains_key(&"k".to_string()));
        assert!(map.get(&"k".to_string()).is_none());
        assert!(map.is_empty());
    }

    #[test]
    fn delete_and_re_add() {
        let mut map = OrMap::new();
        map.set("k".to_string(), 1, ts(100, 0, "node-a"), &node("node-a"));
        map.delete(&"k".to_string());
        assert!(map.is_empty());

        map.set("k".to_string(), 2, ts(200, 0, "node-a"), &node("node-a"));
        assert_eq!(map.get(&"k".to_string()), Some(&2));
    }

    #[test]
    fn multiple_keys() {
        let mut map = OrMap::new();
        map.set("a".to_string(), 1, ts(100, 0, "node-a"), &node("node-a"));
        map.set("b".to_string(), 2, ts(101, 0, "node-a"), &node("node-a"));
        map.set("c".to_string(), 3, ts(102, 0, "node-a"), &node("node-a"));

        assert_eq!(map.len(), 3);
        assert_eq!(map.get(&"a".to_string()), Some(&1));
        assert_eq!(map.get(&"b".to_string()), Some(&2));
        assert_eq!(map.get(&"c".to_string()), Some(&3));

        let mut keys: Vec<&String> = map.keys();
        keys.sort();
        assert_eq!(keys, vec!["a", "b", "c"]);
    }

    #[test]
    fn get_nonexistent_key() {
        let map: OrMap<String, i32> = OrMap::new();
        assert!(map.get(&"nope".to_string()).is_none());
        assert!(!map.contains_key(&"nope".to_string()));
    }

    #[test]
    fn merge_disjoint_keys() {
        let mut map_a = OrMap::new();
        map_a.set("x".to_string(), 10, ts(100, 0, "node-a"), &node("node-a"));

        let mut map_b = OrMap::new();
        map_b.set("y".to_string(), 20, ts(100, 0, "node-b"), &node("node-b"));

        map_a.merge(&map_b);

        assert_eq!(map_a.get(&"x".to_string()), Some(&10));
        assert_eq!(map_a.get(&"y".to_string()), Some(&20));
        assert_eq!(map_a.len(), 2);
    }

    #[test]
    fn merge_same_key_lww() {
        let mut map_a = OrMap::new();
        map_a.set("k".to_string(), 1, ts(100, 0, "node-a"), &node("node-a"));

        let mut map_b = OrMap::new();
        map_b.set("k".to_string(), 2, ts(200, 0, "node-b"), &node("node-b"));

        map_a.merge(&map_b);

        // LWW: node-b's value wins because higher timestamp.
        assert_eq!(map_a.get(&"k".to_string()), Some(&2));
    }

    #[test]
    fn merge_convergence() {
        // Both directions of merge should produce the same result.
        let mut map_a = OrMap::new();
        map_a.set("k".to_string(), 1, ts(100, 0, "node-a"), &node("node-a"));
        map_a.set("x".to_string(), 10, ts(100, 0, "node-a"), &node("node-a"));

        let mut map_b = OrMap::new();
        map_b.set("k".to_string(), 2, ts(200, 0, "node-b"), &node("node-b"));
        map_b.set("y".to_string(), 20, ts(100, 0, "node-b"), &node("node-b"));

        let mut merged_ab = map_a.clone();
        merged_ab.merge(&map_b);

        let mut merged_ba = map_b.clone();
        merged_ba.merge(&map_a);

        // Both should have the same keys.
        assert_eq!(merged_ab.len(), merged_ba.len());
        assert_eq!(
            merged_ab.get(&"k".to_string()),
            merged_ba.get(&"k".to_string())
        );
        assert_eq!(
            merged_ab.get(&"x".to_string()),
            merged_ba.get(&"x".to_string())
        );
        assert_eq!(
            merged_ab.get(&"y".to_string()),
            merged_ba.get(&"y".to_string())
        );

        // LWW for "k": node-b's value wins.
        assert_eq!(merged_ab.get(&"k".to_string()), Some(&2));
    }

    #[test]
    fn concurrent_delete_and_set_add_wins() {
        // Node A has key "k" and deletes it.
        // Node B concurrently sets key "k".
        // After merge, key "k" should be present (add-wins).

        let mut map_a = OrMap::new();
        map_a.set("k".to_string(), 1, ts(100, 0, "node-a"), &node("node-a"));

        // Clone to node B before the delete.
        let mut map_b = map_a.clone();

        // Node A deletes.
        map_a.delete(&"k".to_string());
        assert!(!map_a.contains_key(&"k".to_string()));

        // Node B concurrently sets (new dot).
        map_b.set("k".to_string(), 2, ts(200, 0, "node-b"), &node("node-b"));

        // Merge: B's new dot is not in A's deferred -> key survives.
        map_a.merge(&map_b);
        assert!(map_a.contains_key(&"k".to_string()));
        assert_eq!(map_a.get(&"k".to_string()), Some(&2));
    }

    #[test]
    fn concurrent_delete_and_set_add_wins_reverse() {
        // Same as above but merge in the other direction.
        let mut map_a = OrMap::new();
        map_a.set("k".to_string(), 1, ts(100, 0, "node-a"), &node("node-a"));

        let mut map_b = map_a.clone();

        map_a.delete(&"k".to_string());
        map_b.set("k".to_string(), 2, ts(200, 0, "node-b"), &node("node-b"));

        // Merge B <- A.
        map_b.merge(&map_a);
        assert!(map_b.contains_key(&"k".to_string()));
        assert_eq!(map_b.get(&"k".to_string()), Some(&2));
    }

    #[test]
    fn both_delete_then_merge() {
        let mut map_a = OrMap::new();
        map_a.set("k".to_string(), 1, ts(100, 0, "node-a"), &node("node-a"));

        let mut map_b = map_a.clone();

        map_a.delete(&"k".to_string());
        map_b.delete(&"k".to_string());

        map_a.merge(&map_b);
        assert!(!map_a.contains_key(&"k".to_string()));
    }

    #[test]
    fn delete_propagates_to_self_only_entry_via_merge() {
        // Regression test for #124:
        // Both replicas have key "k". Node B deletes "k", so "k" is NOT in
        // B's entries but IS in B's deferred. When A merges B, A's self-only
        // entry for "k" must have its dots checked against B's deferred set.
        let mut map_a = OrMap::new();
        map_a.set("k".to_string(), 1, ts(100, 0, "node-a"), &node("node-a"));

        // Clone to B so both replicas share the same dot for "k".
        let mut map_b = map_a.clone();

        // B deletes "k" — dot moves to B's deferred, entry removed.
        map_b.delete(&"k".to_string());
        assert!(!map_b.contains_key(&"k".to_string()));

        // A still has "k". Merge B into A.
        // Before fix: "k" survived because the merge loop only iterated
        // over other.entries (which doesn't contain "k").
        map_a.merge(&map_b);
        assert!(
            !map_a.contains_key(&"k".to_string()),
            "delete on B should propagate to A via merge"
        );
        assert!(map_a.is_empty());
    }

    #[test]
    fn delete_propagates_to_self_only_entry_with_other_keys_surviving() {
        // Ensure the fix only removes the correct key and not unrelated ones.
        let mut map_a = OrMap::new();
        map_a.set("k".to_string(), 1, ts(100, 0, "node-a"), &node("node-a"));
        map_a.set(
            "other".to_string(),
            99,
            ts(101, 0, "node-a"),
            &node("node-a"),
        );

        let mut map_b = map_a.clone();

        // B deletes only "k".
        map_b.delete(&"k".to_string());

        map_a.merge(&map_b);
        assert!(
            !map_a.contains_key(&"k".to_string()),
            "deleted key should be gone"
        );
        assert_eq!(
            map_a.get(&"other".to_string()),
            Some(&99),
            "unrelated key should survive"
        );
        assert_eq!(map_a.len(), 1);
    }

    #[test]
    fn merge_is_idempotent() {
        let mut map_a = OrMap::new();
        map_a.set("x".to_string(), 1, ts(100, 0, "node-a"), &node("node-a"));

        let mut map_b = OrMap::new();
        map_b.set("y".to_string(), 2, ts(200, 0, "node-b"), &node("node-b"));

        map_a.merge(&map_b);
        let len_after_first = map_a.len();
        let val_x = map_a.get(&"x".to_string()).cloned();
        let val_y = map_a.get(&"y".to_string()).cloned();

        map_a.merge(&map_b);
        assert_eq!(map_a.len(), len_after_first);
        assert_eq!(map_a.get(&"x".to_string()).cloned(), val_x);
        assert_eq!(map_a.get(&"y".to_string()).cloned(), val_y);
    }

    #[test]
    fn default_is_empty() {
        let map: OrMap<String, i32> = OrMap::default();
        assert!(map.is_empty());
    }

    #[test]
    fn delete_nonexistent_is_noop() {
        let mut map: OrMap<String, i32> = OrMap::new();
        map.delete(&"nope".to_string());
        assert!(map.is_empty());
    }

    #[test]
    fn set_after_merge_with_higher_timestamp_is_noop() {
        // Regression test for #126: after merging a higher-timestamp value,
        // a local set with a lower timestamp should be a no-op.
        let mut map_a = OrMap::new();
        map_a.set(
            "k".to_string(),
            "value_a".to_string(),
            ts(100, 0, "node-a"),
            &node("node-a"),
        );

        let mut map_b = OrMap::new();
        map_b.set(
            "k".to_string(),
            "value_b".to_string(),
            ts(200, 0, "node-b"),
            &node("node-b"),
        );

        // A merges B: register now holds value_b (ts=200).
        map_a.merge(&map_b);
        assert_eq!(map_a.get(&"k".to_string()), Some(&"value_b".to_string()));

        // A tries to set with ts=150 (stale). Should be rejected.
        let updated = map_a.set(
            "k".to_string(),
            "value_c".to_string(),
            ts(150, 0, "node-a"),
            &node("node-a"),
        );
        assert!(!updated, "set with stale timestamp should return false");
        assert_eq!(map_a.get(&"k".to_string()), Some(&"value_b".to_string()));
    }

    #[test]
    fn set_returns_true_on_success() {
        let mut map = OrMap::new();
        let result = map.set("k".to_string(), 42, ts(100, 0, "node-a"), &node("node-a"));
        assert!(result);
        assert_eq!(map.get(&"k".to_string()), Some(&42));
    }

    #[test]
    fn set_with_equal_timestamp_is_noop() {
        let mut map = OrMap::new();
        map.set("k".to_string(), 1, ts(100, 0, "node-a"), &node("node-a"));

        let updated = map.set("k".to_string(), 2, ts(100, 0, "node-a"), &node("node-a"));
        assert!(!updated);
        assert_eq!(map.get(&"k".to_string()), Some(&1));
    }

    #[test]
    fn concurrent_set_different_keys() {
        let mut map_a = OrMap::new();
        map_a.set("a".to_string(), 1, ts(100, 0, "node-a"), &node("node-a"));

        let mut map_b = OrMap::new();
        map_b.set("b".to_string(), 2, ts(100, 0, "node-b"), &node("node-b"));

        map_a.merge(&map_b);
        assert_eq!(map_a.len(), 2);
        assert_eq!(map_a.get(&"a".to_string()), Some(&1));
        assert_eq!(map_a.get(&"b".to_string()), Some(&2));
    }

    // ---------------------------------------------------------------
    // Delta tests
    // ---------------------------------------------------------------

    fn frontier(physical: u64) -> HlcTimestamp {
        HlcTimestamp {
            physical,
            logical: 0,
            node_id: String::new(),
        }
    }

    #[test]
    fn delta_since_empty_returns_none() {
        let map: OrMap<String, i32> = OrMap::new();
        assert!(map.delta_since(&frontier(0)).is_none());
    }

    #[test]
    fn delta_since_returns_entries_after_frontier() {
        let mut map = OrMap::new();
        map.set("a".to_string(), 1, ts(100, 0, "A"), &node("A"));
        map.set("b".to_string(), 2, ts(200, 0, "A"), &node("A"));

        // Frontier at 150 should only include "b".
        let delta = map.delta_since(&ts(150, 0, "")).unwrap();
        assert!(!delta.contains_key(&"a".to_string()));
        assert!(delta.contains_key(&"b".to_string()));
    }

    #[test]
    fn delta_since_returns_none_when_all_older() {
        let mut map = OrMap::new();
        map.set("a".to_string(), 1, ts(100, 0, "A"), &node("A"));

        let delta = map.delta_since(&ts(200, 0, ""));
        assert!(delta.is_none());
    }

    #[test]
    fn delta_from_no_changes_returns_none() {
        let mut map = OrMap::new();
        map.set("a".to_string(), 1, ts(100, 0, "A"), &node("A"));
        let old = map.clone();

        assert!(map.delta_from(&old).is_none());
    }

    #[test]
    fn delta_from_detects_new_entry() {
        let mut map = OrMap::new();
        map.set("a".to_string(), 1, ts(100, 0, "A"), &node("A"));
        let old = map.clone();

        map.set("b".to_string(), 2, ts(200, 0, "A"), &node("A"));

        let delta = map.delta_from(&old).unwrap();
        assert!(delta.contains_key(&"b".to_string()));
        assert_eq!(delta.get(&"b".to_string()), Some(&2));
    }

    #[test]
    fn delta_from_detects_updated_value() {
        let mut map = OrMap::new();
        map.set("a".to_string(), 1, ts(100, 0, "A"), &node("A"));
        let old = map.clone();

        map.set("a".to_string(), 2, ts(200, 0, "A"), &node("A"));

        let delta = map.delta_from(&old).unwrap();
        assert!(delta.contains_key(&"a".to_string()));
        assert_eq!(delta.get(&"a".to_string()), Some(&2));
    }

    #[test]
    fn delta_from_detects_delete() {
        let mut map = OrMap::new();
        map.set("a".to_string(), 1, ts(100, 0, "A"), &node("A"));
        let old = map.clone();

        map.delete(&"a".to_string());

        let delta = map.delta_from(&old).unwrap();
        // Should have new tombstone dots.
        assert!(!delta.deferred.is_empty());
    }

    #[test]
    fn delta_round_trip_add_produces_same_result() {
        let mut map = OrMap::new();
        map.set("a".to_string(), 1, ts(100, 0, "A"), &node("A"));
        let old = map.clone();

        map.set("b".to_string(), 2, ts(200, 0, "B"), &node("B"));

        // Full merge path.
        let mut via_full = old.clone();
        via_full.merge(&map);

        // Delta merge path.
        let delta = map.delta_from(&old).unwrap();
        let mut via_delta = old.clone();
        via_delta.merge_delta(&delta);

        assert_eq!(
            via_full.get(&"a".to_string()),
            via_delta.get(&"a".to_string())
        );
        assert_eq!(
            via_full.get(&"b".to_string()),
            via_delta.get(&"b".to_string())
        );
        assert_eq!(via_full.len(), via_delta.len());
    }

    #[test]
    fn delta_round_trip_delete_produces_same_result() {
        let mut map = OrMap::new();
        map.set("a".to_string(), 1, ts(100, 0, "A"), &node("A"));
        map.set("b".to_string(), 2, ts(101, 0, "A"), &node("A"));
        let old = map.clone();

        map.delete(&"a".to_string());

        // Full merge path.
        let mut via_full = old.clone();
        via_full.merge(&map);

        // Delta merge path.
        let delta = map.delta_from(&old).unwrap();
        let mut via_delta = old.clone();
        via_delta.merge_delta(&delta);

        assert!(!via_full.contains_key(&"a".to_string()));
        assert!(!via_delta.contains_key(&"a".to_string()));
        assert_eq!(
            via_full.get(&"b".to_string()),
            via_delta.get(&"b".to_string())
        );
    }

    #[test]
    fn merge_delta_is_equivalent_to_merge() {
        let mut map_a = OrMap::new();
        map_a.set("x".to_string(), 10, ts(100, 0, "A"), &node("A"));

        let mut map_b = OrMap::new();
        map_b.set("y".to_string(), 20, ts(200, 0, "B"), &node("B"));

        let mut via_merge = map_a.clone();
        via_merge.merge(&map_b);

        let mut via_delta = map_a.clone();
        via_delta.merge_delta(&map_b);

        assert_eq!(via_merge.len(), via_delta.len());
        assert_eq!(
            via_merge.get(&"x".to_string()),
            via_delta.get(&"x".to_string())
        );
        assert_eq!(
            via_merge.get(&"y".to_string()),
            via_delta.get(&"y".to_string())
        );
    }

    // ---------------------------------------------------------------
    // Certified sweep + compaction floor (M-8)
    //
    // (The P1-10 HLC-floor lesson lives in the gc.rs module docs: the
    // legacy version-floor APIs that made that bug expressible were
    // deleted along with them — the floor now advances exclusively in
    // dot space via the certified walk and merge inheritance.)
    // ---------------------------------------------------------------

    /// M-8 livelock reproduction, map edition: with the old union-merge
    /// deferred handling, a peer's stale tombstone re-entered after every
    /// sweep. The floor now rejects it.
    #[test]
    fn swept_tombstone_is_not_reinjected_by_merge() {
        let n = node("A");
        let mut map_a = OrMap::new();
        map_a.set("k".to_string(), 1, ts(100, 0, "A"), &n); // dot (A,1)
        map_a.delete(&"k".to_string()); // tombstone (A,1)
        let map_b = map_a.clone();

        let swept = map_a.compact_deferred_certified(&map_a.deferred_dots(), None);
        assert_eq!(swept.collected, 1);
        assert_eq!(map_a.deferred_len(), 0);
        assert_eq!(map_a.compaction_floor().get(&n), Some(&1));

        let fx = map_a.merge(&map_b);
        assert_eq!(
            map_a.deferred_len(),
            0,
            "merge must not roll the sweep back (M-8 livelock)"
        );
        assert_eq!(fx.rejected_covered_deferred, 1);
        assert!(!map_a.contains_key(&"k".to_string()));
    }

    /// Floor kill: a replica that never saw the delete learns it from the
    /// floor riding a complete state (INV-W).
    #[test]
    fn floor_kills_stale_live_entry_on_merge() {
        let n = node("A");
        let mut map_a = OrMap::new();
        map_a.set("k".to_string(), 1, ts(100, 0, "A"), &n);
        let mut map_b = map_a.clone(); // B holds (A,1) live

        map_a.delete(&"k".to_string());
        map_a.compact_deferred_certified(&map_a.deferred_dots(), None);
        assert_eq!(map_a.deferred_len(), 0, "tombstone gone — only the floor");

        // B <- A (complete state with floor): B's live dot is killed.
        let fx = map_b.merge(&map_a);
        assert!(!map_b.contains_key(&"k".to_string()), "floor must kill");
        assert_eq!(fx.killed_by_floor, 1);

        // A <- B (stale live dot re-offered): rejected, no resurrection.
        let fx = map_a.merge(&map_b);
        assert!(!map_a.contains_key(&"k".to_string()));
        assert_eq!(fx.killed_by_floor, 0);
    }

    /// A fresh set() after the floor advanced survives (new dots are above
    /// every floor — INV-CTR).
    #[test]
    fn set_after_floor_survives() {
        let n = node("A");
        let mut map = OrMap::new();
        map.set("k".to_string(), 1, ts(100, 0, "A"), &n);
        map.delete(&"k".to_string());
        map.compact_deferred_certified(&map.deferred_dots(), None);
        assert_eq!(map.compaction_floor().get(&n), Some(&1));

        assert!(map.set("k".to_string(), 2, ts(200, 0, "A"), &n));
        assert_eq!(map.get(&"k".to_string()), Some(&2));

        // And it survives a self-merge / peer round trip.
        let clone = map.clone();
        map.merge(&clone);
        assert_eq!(map.get(&"k".to_string()), Some(&2));
    }

    /// §0-B(1) regression: `OrMap::delta_since` is a PARTIAL payload and
    /// must ship an empty floor — otherwise the receiver's live entries
    /// that are simply older than the frontier would be floor-killed.
    #[test]
    fn delta_since_ships_empty_floor_and_does_not_kill_receiver_entries() {
        let n = node("A");
        let mut sender = OrMap::new();
        sender.set("old".to_string(), 1, ts(100, 0, "A"), &n); // dot (A,1)
        let mut receiver = sender.clone(); // receiver holds "old" live

        // Sender advances its floor past (A,1) — "old" stays live.
        sender.set("gone".to_string(), 2, ts(150, 0, "A"), &n); // dot (A,2)
        sender.delete(&"gone".to_string());
        sender.compact_deferred_certified(&sender.deferred_dots(), None);
        assert_eq!(sender.compaction_floor().get(&n), Some(&2));

        // New entry only — the delta excludes "old" (ts 100 <= frontier 200).
        sender.set("new".to_string(), 3, ts(300, 0, "A"), &n);
        let delta = sender.delta_since(&ts(200, 0, "")).unwrap();
        assert!(!delta.contains_key(&"old".to_string()));
        assert!(
            delta.compaction_floor.is_empty(),
            "INV-W: partial payloads must never carry the floor"
        );

        let fx = receiver.merge(&delta);
        assert!(
            receiver.contains_key(&"old".to_string()),
            "receiver's live entry outside the delta must survive"
        );
        assert!(receiver.contains_key(&"new".to_string()));
        assert_eq!(fx.killed_by_floor, 0);
    }

    /// `delta_from` (partial payload) also ships an empty floor.
    #[test]
    fn delta_from_ships_empty_floor() {
        let n = node("A");
        let mut map = OrMap::new();
        map.set("a".to_string(), 1, ts(100, 0, "A"), &n);
        map.delete(&"a".to_string());
        map.compact_deferred_certified(&map.deferred_dots(), None);
        let old = map.clone();

        map.set("b".to_string(), 2, ts(200, 0, "A"), &n);
        let delta = map.delta_from(&old).unwrap();
        assert!(delta.compaction_floor.is_empty(), "INV-W");
    }

    /// Old serialized format without `compaction_floor` still loads
    /// (mixed-version JSON compatibility).
    #[test]
    fn serde_backward_compat_missing_floor() {
        let json = r#"{"entries":{"k":[[{"node_id":"A","counter":1}],{"value":"v","timestamp":{"physical":100,"logical":0,"node_id":"A"}}]},"counters":{"A":1},"deferred":[]}"#;
        let map: OrMap<String, String> = serde_json::from_str(json).unwrap();
        assert!(map.contains_key(&"k".to_string()));
        assert!(map.compaction_floor().is_empty());
    }

    /// The sweep stalls on tombstones outside the candidate set and on
    /// unknown-writer dots (fail-closed conservatism).
    #[test]
    fn certified_sweep_respects_candidates_and_unknown_nodes() {
        let n = node("A");
        let mut map = OrMap::new();
        map.set("k1".to_string(), 1, ts(100, 0, "A"), &n); // dot (A,1)
        map.delete(&"k1".to_string());
        let marked = map.deferred_dots();
        map.set("k2".to_string(), 2, ts(200, 0, "A"), &n); // dot (A,2)
        map.delete(&"k2".to_string()); // post-mark tombstone

        let outcome = map.compact_deferred_certified(&marked, None);
        assert_eq!(outcome.collected, 1, "only the marked dot is collected");
        assert_eq!(outcome.stalled_uncandidated, 1);
        assert_eq!(map.deferred_len(), 1, "post-mark tombstone survives");
        assert_eq!(map.compaction_floor().get(&n), Some(&1));
    }

    // ---------------------------------------------------------------
    // MergeEffects::changed ground truth (M-6, RR gate)
    // ---------------------------------------------------------------

    /// Merge `b` into `a`, asserting the changed flag equals the physical
    /// state difference, and return the effects.
    fn merge_ground_truth(
        a: &mut OrMap<String, String>,
        b: &OrMap<String, String>,
    ) -> MergeEffects {
        let before = a.clone();
        let fx = a.merge(b);
        assert_eq!(
            fx.changed,
            *a != before,
            "changed flag must equal physical pre/post difference"
        );
        fx
    }

    #[test]
    fn merge_changed_identical_and_subset_are_noops() {
        let n = node("A");
        let mut a = OrMap::new();
        a.set("k".to_string(), "v".to_string(), ts(100, 0, "A"), &n);
        a.set("gone".to_string(), "g".to_string(), ts(110, 0, "A"), &n);
        a.delete(&"gone".to_string());
        let b = a.clone();

        let fx = merge_ground_truth(&mut a, &b);
        assert!(!fx.changed, "identical states must merge as a no-op");

        // Dominated subset: b lacks a's later entry.
        let mut sup = b.clone();
        sup.set("extra".to_string(), "e".to_string(), ts(200, 0, "A"), &n);
        let mut target = sup.clone();
        let fx = merge_ground_truth(&mut target, &b);
        assert!(!fx.changed, "merging a dominated subset must be a no-op");
    }

    #[test]
    fn merge_changed_register_timestamp_directions() {
        let n = node("A");
        let mut a = OrMap::new();
        a.set("k".to_string(), "old".to_string(), ts(100, 0, "A"), &n);
        let mut newer = OrMap::new();
        newer.set(
            "k".to_string(),
            "new".to_string(),
            ts(200, 0, "B"),
            &node("B"),
        );

        // Newer register on b: changed (dot + register both advance).
        let fx = merge_ground_truth(&mut a, &newer);
        assert!(fx.changed);
        assert_eq!(a.get(&"k".to_string()), Some(&"new".to_string()));

        // Older register re-offered: the stale dot is still adopted (it
        // is a distinct live dot) — but repeating the SAME merge again
        // must be a no-op.
        let mut older = OrMap::new();
        older.set("k".to_string(), "old".to_string(), ts(100, 0, "A"), &n);
        merge_ground_truth(&mut a, &older);
        let fx = merge_ground_truth(&mut a, &older);
        assert!(!fx.changed, "repeated merge of the same state is a no-op");
    }

    #[test]
    fn merge_changed_tombstone_counter_floor_paths() {
        let n = node("A");
        let mut a = OrMap::new();
        a.set("k".to_string(), "v".to_string(), ts(100, 0, "A"), &n);
        let mut b = a.clone();

        // Tombstone-only difference.
        b.delete(&"k".to_string());
        let fx = merge_ground_truth(&mut a, &b);
        assert!(fx.changed, "a delete must report changed");

        // Counter-only advance.
        let counters_only: OrMap<String, String> = serde_json::from_str(
            r#"{"entries":{},"counters":{"A":5},"deferred":[],"compaction_floor":{}}"#,
        )
        .unwrap();
        let fx = merge_ground_truth(&mut a, &counters_only);
        assert!(fx.changed, "a counter-only advance must report changed");

        // Floor advance + floor kill on a lagging replica.
        let mut swept = OrMap::new();
        swept.set("k".to_string(), "v".to_string(), ts(100, 0, "A"), &n);
        let mut lagging = swept.clone();
        swept.delete(&"k".to_string());
        swept.compact_deferred_certified(&swept.deferred_dots(), None);
        let fx = merge_ground_truth(&mut lagging, &swept);
        assert!(fx.changed, "a floor kill must report changed");
        assert_eq!(fx.killed_by_floor, 1);
    }

    #[test]
    fn merge_changed_stale_reoffers_are_noops() {
        let n = node("A");
        let mut a = OrMap::new();
        a.set("k".to_string(), "v".to_string(), ts(100, 0, "A"), &n);
        a.delete(&"k".to_string());
        let pre_sweep = a.clone(); // holds tombstone (A,1)
        a.compact_deferred_certified(&a.deferred_dots(), None);

        // Covered deferred re-offer: rejected, no state change.
        let fx = merge_ground_truth(&mut a, &pre_sweep);
        assert_eq!(fx.rejected_covered_deferred, 1);
        assert!(
            !fx.changed,
            "a rejected covered tombstone must NOT report changed"
        );
    }

    /// Regression pin for the other-only-key refactor: a key that exists
    /// only on the other side but whose dots are ALL stale (deferred or
    /// floor-covered) must be a full no-op — no entry created, no
    /// register adopted, `changed == false`. The previous formulation
    /// created an empty entry (or_insert_with), merged the register into
    /// it and deleted the entry again in the retain — a net no-op that a
    /// naive instrumentation would misreport as a change on EVERY round
    /// (permanent ping-pong for lagging-peer re-offers).
    #[test]
    fn or_map_other_only_key_all_dots_stale_is_noop() {
        let n = node("A");

        // Case 1: all dots deferred (we deleted the key).
        let mut sender = OrMap::new();
        sender.set("k".to_string(), "v".to_string(), ts(100, 0, "A"), &n);
        let mut a = sender.clone();
        a.delete(&"k".to_string());
        let fx = merge_ground_truth(&mut a, &sender);
        assert!(!fx.changed, "all-deferred other-only key must be a no-op");
        assert!(
            !a.entries.contains_key("k"),
            "no entry may be created for an all-stale other-only key"
        );

        // Case 2: all dots floor-covered (certified-removed re-offer).
        let mut b = sender.clone();
        b.delete(&"k".to_string());
        b.compact_deferred_certified(&b.deferred_dots(), None);
        assert_eq!(b.deferred_len(), 0, "tombstone folded into the floor");
        let fx = merge_ground_truth(&mut b, &sender);
        assert_eq!(fx.rejected_stale_live, 1);
        assert!(!fx.changed, "floor-stale other-only key must be a no-op");
        assert!(!b.entries.contains_key("k"));
    }

    #[test]
    fn merge_changed_zero_counter_creates_no_ghost_entry() {
        let n = node("A");
        let mut a = OrMap::new();
        a.set("k".to_string(), "v".to_string(), ts(100, 0, "A"), &n);
        let b: OrMap<String, String> = serde_json::from_str(
            r#"{"entries":{},"counters":{"B":0},"deferred":[],"compaction_floor":{}}"#,
        )
        .unwrap();

        let fx = merge_ground_truth(&mut a, &b);
        assert!(!fx.changed, "a zero counter carries no information");
        assert!(!a.counters.contains_key(&node("B")));
    }
}
