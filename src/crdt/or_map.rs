use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::ops::{Deref, DerefMut};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::crdt::{DotCounterExhausted, MergeEffects, SweepOutcome, covered};
use crate::hlc::HlcTimestamp;
use crate::types::NodeId;

/// A live event dot must keep one key and one immutable value/HLC across
/// replicas. A merge of individually valid snapshots can violate this rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum OrMapMergeConflict {
    #[error("OR-Map dot is live under different keys across states")]
    DifferentKey,
    #[error("OR-Map dot has different values or timestamps across states")]
    DifferentValue,
}

/// A unique event identifier (node, counter) for OR-Set semantics.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct Dot {
    node_id: NodeId,
    counter: u64,
}

/// The immutable payload assigned to one event dot. Only live dots are kept
/// in `entries`, so a removed write can never influence the visible value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DotValue<V> {
    value: V,
    timestamp: HlcTimestamp,
}

/// Hash lookup internally, a dot/value sequence on disk and JSON wire.
/// Struct-valued map keys cannot be encoded by `serde_json`.
#[derive(Debug, Clone, PartialEq)]
struct DotValues<V>(HashMap<Dot, DotValue<V>>);

const DOT_VALUES_SCHEMA: u8 = 250;

impl<V> Default for DotValues<V> {
    fn default() -> Self {
        Self(HashMap::new())
    }
}

impl<V> Deref for DotValues<V> {
    type Target = HashMap<Dot, DotValue<V>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<V> DerefMut for DotValues<V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<V: Serialize> Serialize for DotValues<V> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut entries: Vec<_> = self.0.iter().collect();
        entries.sort_unstable_by(|a, b| {
            a.0.node_id
                .0
                .cmp(&b.0.node_id.0)
                .then_with(|| a.0.counter.cmp(&b.0.counter))
        });
        (DOT_VALUES_SCHEMA, entries).serialize(serializer)
    }
}

impl<'de, V: Deserialize<'de>> Deserialize<'de> for DotValues<V> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let (schema, entries) = <(u8, Vec<(Dot, DotValue<V>)>)>::deserialize(deserializer)?;
        if schema != DOT_VALUES_SCHEMA {
            return Err(serde::de::Error::custom("invalid OR-Map dot-value schema"));
        }
        let mut map = HashMap::with_capacity(entries.len());
        for (dot, value) in entries {
            if map.insert(dot, value).is_some() {
                return Err(serde::de::Error::custom("duplicate OR-Map dot"));
            }
        }
        Ok(Self(map))
    }
}

/// Observed-Remove Map (FR-005).
///
/// Combines OR-Set semantics for key presence (add-wins on concurrent
/// add/remove) with immutable per-dot values. Each key tracks its causal
/// dots so that concurrent `set` and `delete` operations resolve correctly:
/// a `set` that is concurrent with a `delete` will re-add the key.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OrMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone + Ord,
{
    /// Active entries: key -> dot -> immutable value and write timestamp.
    entries: HashMap<K, DotValues<V>>,
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

#[derive(Deserialize)]
#[serde(bound(deserialize = "K: Eq + Hash + Deserialize<'de>, V: Deserialize<'de>"))]
struct OrMapWire<K, V> {
    entries: HashMap<K, DotValues<V>>,
    counters: HashMap<NodeId, u64>,
    deferred: HashSet<Dot>,
    #[serde(default)]
    compaction_floor: HashMap<NodeId, u64>,
}

impl<'de, K, V> Deserialize<'de> for OrMap<K, V>
where
    K: Eq + Hash + Clone + Deserialize<'de>,
    V: Clone + Ord + Deserialize<'de>,
{
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = OrMapWire::<K, V>::deserialize(deserializer)?;
        let mut live = HashSet::new();
        for dots in wire.entries.values() {
            for dot in dots.keys() {
                if dot.counter == 0 {
                    return Err(serde::de::Error::custom(
                        "OR-Map dot counter must be positive",
                    ));
                }
                if !live.insert(dot) {
                    return Err(serde::de::Error::custom(
                        "OR-Map dot is live under multiple keys",
                    ));
                }
                if wire.deferred.contains(dot) {
                    return Err(serde::de::Error::custom(
                        "OR-Map dot is both live and removed",
                    ));
                }
                if wire.counters.get(&dot.node_id).copied().unwrap_or(0) < dot.counter {
                    return Err(serde::de::Error::custom(
                        "OR-Map counter is below a live dot",
                    ));
                }
            }
        }
        for dot in &wire.deferred {
            if dot.counter == 0 {
                return Err(serde::de::Error::custom(
                    "OR-Map dot counter must be positive",
                ));
            }
            if wire.counters.get(&dot.node_id).copied().unwrap_or(0) < dot.counter {
                return Err(serde::de::Error::custom(
                    "OR-Map counter is below a removed dot",
                ));
            }
        }
        for (node, floor) in &wire.compaction_floor {
            if wire.counters.get(node).copied().unwrap_or(0) < *floor {
                return Err(serde::de::Error::custom(
                    "OR-Map counter is below the compaction floor",
                ));
            }
        }
        Ok(Self {
            entries: wire.entries,
            counters: wire.counters,
            deferred: wire.deferred,
            compaction_floor: wire.compaction_floor,
        })
    }
}

impl<K, V> OrMap<K, V>
where
    K: Eq + Hash + Clone + Serialize + DeserializeOwned,
    V: Clone + Ord + Serialize + DeserializeOwned,
{
    /// Validate immutable live-dot assignments before merging an external
    /// state. Deserialization checks each state in isolation; this catches
    /// conflicts that exist only between the two states.
    pub fn validate_merge_compatible(&self, other: &Self) -> Result<(), OrMapMergeConflict> {
        let mut assignments = HashMap::new();
        for (key, dots) in &self.entries {
            for (dot, value) in dots.iter() {
                assignments.insert(dot, (key, value));
            }
        }
        for (key, dots) in &other.entries {
            for (dot, value) in dots.iter() {
                if let Some((held_key, held_value)) = assignments.get(dot) {
                    if *held_key != key {
                        return Err(OrMapMergeConflict::DifferentKey);
                    }
                    if *held_value != value {
                        return Err(OrMapMergeConflict::DifferentValue);
                    }
                }
            }
        }
        Ok(())
    }

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
    fn next_dot(&mut self, node_id: &NodeId) -> Result<Dot, DotCounterExhausted> {
        let counter = self.counters.entry(node_id.clone()).or_insert(0);
        *counter = counter.checked_add(1).ok_or(DotCounterExhausted)?;
        Ok(Dot {
            node_id: node_id.clone(),
            counter: *counter,
        })
    }

    /// Set a key to a value with add-wins semantics.
    ///
    /// This adds a fresh dot with its own immutable value and timestamp.
    /// Previously observed dots remain live until an explicit delete.
    pub fn set(&mut self, key: K, value: V, timestamp: HlcTimestamp, node_id: &NodeId) -> bool {
        self.try_set(key, value, timestamp, node_id)
            .expect("OR-Map dot counter exhausted")
    }

    /// Distinguish dot exhaustion before changing the map.
    pub fn try_set(
        &mut self,
        key: K,
        value: V,
        timestamp: HlcTimestamp,
        node_id: &NodeId,
    ) -> Result<bool, DotCounterExhausted> {
        let dot = self.next_dot(node_id)?;
        let entry = self.entries.entry(key).or_default();

        entry.insert(dot, DotValue { value, timestamp });
        Ok(true)
    }

    /// Delete a key using OR-Set remove semantics.
    ///
    /// All currently observed dots for the key are moved to the deferred
    /// (tombstone) set. A concurrent `set` on another node that introduces
    /// a dot not in the deferred set will cause the key to reappear after
    /// merge (add-wins).
    pub fn delete(&mut self, key: &K) {
        if let Some(dots) = self.entries.remove(key) {
            for (d, _) in dots.0 {
                self.deferred.insert(d);
            }
        }
    }

    /// Get a reference to the value associated with a key.
    pub fn get(&self, key: &K) -> Option<&V> {
        self.entries.get(key).and_then(|dots| {
            dots.values()
                .max_by(|a, b| (&a.timestamp, &a.value).cmp(&(&b.timestamp, &b.value)))
                .map(|winner| &winner.value)
        })
    }

    /// Check whether a key is present.
    pub fn contains_key(&self, key: &K) -> bool {
        self.entries.get(key).is_some_and(|dots| !dots.is_empty())
    }

    /// Return all currently present keys.
    pub fn keys(&self) -> Vec<&K> {
        self.entries
            .iter()
            .filter(|(_, dots)| !dots.is_empty())
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
    /// - Each surviving dot keeps its own immutable value. Call
    ///   [`Self::validate_merge_compatible`] before merging externally
    ///   supplied state: this infallible method assumes one key and one
    ///   value/HLC assignment per live dot across both states. The
    ///   `(timestamp, value)` tie break is only a deterministic fallback
    ///   for direct callers that bypass the checked ingress.
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

        for (key, other_dots) in &other.entries {
            if let Some(entry) = self.entries.get_mut(key) {
                // Key exists in both — merge dot/value assignments.

                // Add dots from other that we haven't tombstoned and that
                // are not stale under our pre-merge floor.
                for (dot, other_value) in other_dots.iter() {
                    if self.deferred.contains(dot) {
                        continue;
                    }
                    if let Some(value) = entry.get_mut(dot) {
                        if (&other_value.timestamp, &other_value.value)
                            > (&value.timestamp, &value.value)
                        {
                            *value = other_value.clone();
                            fx.changed = true;
                        }
                        continue;
                    }
                    if covered(&floor_pre, &dot.node_id, dot.counter) {
                        fx.rejected_stale_live += 1;
                        continue;
                    }
                    entry.insert(dot.clone(), other_value.clone());
                    fx.changed = true;
                }

                // Remove our dots that the other has tombstoned or
                // floor-compacted away.
                entry.retain(|dot, _| {
                    if other.deferred.contains(dot) {
                        fx.changed = true;
                        return false;
                    }
                    if covered(&other.compaction_floor, &dot.node_id, dot.counter)
                        && !other_dots.contains_key(dot)
                    {
                        fx.killed_by_floor += 1;
                        fx.changed = true;
                        return false;
                    }
                    true
                });
            } else {
                // Key only in other — adopt only dots that are not removed.
                let mut filtered = DotValues::default();
                for (dot, value) in other_dots.iter() {
                    if self.deferred.contains(dot) {
                        continue;
                    }
                    if covered(&floor_pre, &dot.node_id, dot.counter) {
                        fx.rejected_stale_live += 1;
                        continue;
                    }
                    filtered.insert(dot.clone(), value.clone());
                }
                if !filtered.is_empty() {
                    self.entries.insert(key.clone(), filtered);
                    fx.changed = true;
                }
            }
        }

        // Apply other's tombstones and floor to self-only entries (keys
        // not in other.entries — trivially absent from its live set).
        for (key, dots) in &mut self.entries {
            if !other.entries.contains_key(key) {
                dots.retain(|dot, _| {
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
        self.entries.retain(|_, dots| !dots.is_empty());

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
    /// OrMap entries carry per-dot timestamps, so this method returns
    /// only keys with a live dot timestamp strictly greater than
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

        for (key, dots) in &self.entries {
            if dots.values().any(|value| value.timestamp > *frontier) {
                delta.entries.insert(key.clone(), dots.clone());
                has_entries = true;
            }
        }

        if !has_entries && delta.deferred.is_empty() {
            return None;
        }
        Some(delta)
    }

    /// Compute a delta against a known old state.
    ///
    /// Ordinarily returns an OrMap containing only:
    /// - Entries with a new dot or a changed same-dot assignment
    /// - Deferred (tombstone) dots NOT present in `old`
    /// - Updated counters
    ///
    /// The `compaction_floor` is deliberately EMPTY (INV-W): partial
    /// payloads never carry the floor (see [`delta_since`](Self::delta_since)).
    /// If the floor changed, or a formerly live dot disappeared without a
    /// tombstone, return a full state instead: a receiver needs the complete
    /// live set to distinguish removed dots from live dots below the floor.
    ///
    /// Returns `None` if there are no changes.
    pub fn delta_from(&self, old: &OrMap<K, V>) -> Option<Self>
    where
        V: PartialEq,
    {
        let unrepresented_removal = old.entries.iter().any(|(key, dots)| {
            dots.keys().any(|dot| {
                !self
                    .entries
                    .get(key)
                    .is_some_and(|live| live.contains_key(dot))
                    && !self.deferred.contains(dot)
            })
        });
        if self.compaction_floor != old.compaction_floor || unrepresented_removal {
            return Some(self.clone());
        }
        let mut delta = OrMap {
            entries: HashMap::new(),
            counters: HashMap::new(),
            deferred: HashSet::new(),
            // INV-W: partial payload — never ship the floor.
            compaction_floor: HashMap::new(),
        };
        let mut has_changes = false;

        // Collect all dots in old state for comparison.
        let old_all_dots: HashSet<&Dot> =
            old.entries.values().flat_map(|dots| dots.keys()).collect();

        for (key, dots) in &self.entries {
            let changed = dots.iter().any(|(dot, value)| {
                !old_all_dots.contains(dot)
                    || old.entries.get(key).and_then(|entries| entries.get(dot)) != Some(value)
            });

            if changed {
                delta.entries.insert(key.clone(), dots.clone());
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

        // Changed keys carry their complete live dot set. The delta is
        // decoded on its own, so include counters for older dots in those
        // keys and for newly transmitted tombstones as well.
        for dot in delta
            .entries
            .values()
            .flat_map(|dots| dots.keys())
            .chain(delta.deferred.iter())
        {
            let counter = self
                .counters
                .get(&dot.node_id)
                .copied()
                .expect("valid OR-Map has a counter for every dot");
            delta.counters.entry(dot.node_id.clone()).or_insert(counter);
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
    /// writer). The tombstone-GC mark phase snapshots these for the
    /// internal Stage 2 boundary tests.
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
    /// semantics, gating requirements, and origin-retention argument.
    ///
    /// [`OrSet::compact_deferred_certified`]: crate::crdt::or_set::OrSet::compact_deferred_certified
    pub fn compact_deferred_certified(
        &mut self,
        candidates: &HashSet<(NodeId, u64)>,
    ) -> SweepOutcome {
        self.compact_deferred_with_bounds(candidates, None, None)
    }

    /// Internal walk used by saved-mark GC and Stage 2 boundary tests.
    /// A mark cap never enables hole jumping on its own.
    pub(in crate::crdt) fn compact_deferred_with_bounds(
        &mut self,
        candidates: &HashSet<(NodeId, u64)>,
        mark_counter_caps: Option<&HashMap<NodeId, u64>>,
        hole_jump_ceilings: Option<&HashMap<NodeId, u64>>,
    ) -> SweepOutcome {
        let live_dots: HashSet<(&NodeId, u64)> = self
            .entries
            .values()
            .flat_map(|dots| dots.keys())
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
            mark_counter_caps,
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
            .filter(|(_, dots)| !dots.is_empty())
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
    /// `str(key)` ‖ live `(dot, value, HLC)` records in dot order ‖
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
        use crate::crdt::digest::{
            write_counters, write_dots, write_hlc, write_str, write_u32, write_u64,
        };
        use sha2::Digest as _;

        hasher.update([0x04]);
        type MapEntryRef<'a> = (&'a String, &'a DotValues<String>);
        let mut items: Vec<MapEntryRef<'_>> = self
            .entries
            .iter()
            .filter(|(_, dots)| !dots.is_empty())
            .collect();
        items.sort_unstable_by(|a, b| a.0.cmp(b.0));
        write_u32(hasher, items.len() as u32);
        for (key, dots) in items {
            write_str(hasher, key);
            let mut ordered: Vec<_> = dots.iter().collect();
            ordered.sort_unstable_by(|a, b| {
                a.0.node_id
                    .0
                    .cmp(&b.0.node_id.0)
                    .then_with(|| a.0.counter.cmp(&b.0.counter))
            });
            write_u32(hasher, ordered.len() as u32);
            for (dot, assignment) in ordered {
                write_str(hasher, &dot.node_id.0);
                write_u64(hasher, dot.counter);
                write_str(hasher, &assignment.value);
                write_hlc(hasher, &assignment.timestamp);
            }
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
    fn merge_preflight_rejects_cross_state_dot_assignment_conflicts() {
        let mut original = OrMap::new();
        original.set(
            "first".to_string(),
            "one".to_string(),
            ts(10, 0, "A"),
            &node("A"),
        );

        let mut different_key = OrMap::new();
        different_key.set(
            "second".to_string(),
            "one".to_string(),
            ts(10, 0, "A"),
            &node("A"),
        );
        assert_eq!(
            original.validate_merge_compatible(&different_key),
            Err(OrMapMergeConflict::DifferentKey)
        );

        let mut different_value = OrMap::new();
        different_value.set(
            "first".to_string(),
            "two".to_string(),
            ts(10, 0, "A"),
            &node("A"),
        );
        assert_eq!(
            original.validate_merge_compatible(&different_value),
            Err(OrMapMergeConflict::DifferentValue)
        );

        let mut different_timestamp = OrMap::new();
        different_timestamp.set(
            "first".to_string(),
            "one".to_string(),
            ts(11, 0, "A"),
            &node("A"),
        );
        assert_eq!(
            original.validate_merge_compatible(&different_timestamp),
            Err(OrMapMergeConflict::DifferentValue)
        );
        assert_eq!(original.validate_merge_compatible(&original), Ok(()));
    }

    fn assert_decode_rejected(map: &OrMap<String, i32>) {
        let json = serde_json::to_vec(map).unwrap();
        assert!(serde_json::from_slice::<OrMap<String, i32>>(&json).is_err());

        let binary = bincode::serde::encode_to_vec(map, bincode::config::standard()).unwrap();
        assert!(
            bincode::serde::decode_from_slice::<OrMap<String, i32>, _>(
                &binary,
                bincode::config::standard(),
            )
            .is_err()
        );
    }

    #[test]
    fn deserialize_rejects_dot_live_and_removed() {
        let mut map = OrMap::new();
        map.set("a".to_string(), 1, ts(1, 0, "A"), &node("A"));
        let dot = map.entries["a"].keys().next().unwrap().clone();
        map.deferred.insert(dot);
        assert_decode_rejected(&map);
    }

    #[test]
    fn deserialize_rejects_dot_live_under_multiple_keys() {
        let mut map = OrMap::new();
        map.set("a".to_string(), 1, ts(1, 0, "A"), &node("A"));
        map.entries
            .insert("b".to_string(), map.entries["a"].clone());
        assert_decode_rejected(&map);
    }

    #[test]
    fn deserialize_rejects_counters_below_retained_dots_or_floor() {
        let mut live = OrMap::new();
        live.set("a".to_string(), 1, ts(1, 0, "A"), &node("A"));
        live.counters.insert(node("A"), 0);
        assert_decode_rejected(&live);

        let mut removed = OrMap::new();
        removed.set("a".to_string(), 1, ts(1, 0, "A"), &node("A"));
        removed.delete(&"a".to_string());
        removed.counters.remove(&node("A"));
        assert_decode_rejected(&removed);

        let mut floored: OrMap<String, i32> = OrMap::new();
        floored.compaction_floor.insert(node("A"), 1);
        assert_decode_rejected(&floored);
    }

    #[test]
    fn deserialize_rejects_zero_counter_dots() {
        let dot = Dot {
            node_id: node("A"),
            counter: 0,
        };
        let mut live: OrMap<String, i32> = OrMap::new();
        live.entries.insert(
            "a".into(),
            DotValues(HashMap::from([(
                dot.clone(),
                DotValue {
                    value: 1,
                    timestamp: ts(1, 0, "A"),
                },
            )])),
        );
        live.counters.insert(node("A"), 0);
        assert_decode_rejected(&live);

        let mut removed: OrMap<String, i32> = OrMap::new();
        removed.deferred.insert(dot);
        removed.counters.insert(node("A"), 0);
        assert_decode_rejected(&removed);
    }

    #[test]
    fn deserialize_accepts_live_dot_below_floor() {
        let mut map = OrMap::new();
        map.set("live".to_string(), 1, ts(1, 0, "A"), &node("A"));
        map.compaction_floor.insert(node("A"), 1);

        let json = serde_json::to_vec(&map).unwrap();
        let restored: OrMap<String, i32> = serde_json::from_slice(&json).unwrap();
        assert_eq!(restored, map);

        let binary = bincode::serde::encode_to_vec(&map, bincode::config::standard()).unwrap();
        let (restored, _): (OrMap<String, i32>, usize) =
            bincode::serde::decode_from_slice(&binary, bincode::config::standard()).unwrap();
        assert_eq!(restored, map);
    }

    #[test]
    fn exhausted_counter_cannot_reuse_a_dot_on_another_key() {
        let writer = node("writer");
        let mut map = OrMap::new();
        assert!(map.set("old".to_string(), 1, ts(1, 0, "writer"), &writer));
        map.counters.insert(writer.clone(), u64::MAX);
        let before = map.clone();
        assert_eq!(
            map.try_set("new".to_string(), 2, ts(2, 0, "writer"), &writer),
            Err(DotCounterExhausted)
        );
        assert_eq!(map, before);
        assert_eq!(map.get(&"old".to_string()), Some(&1));
        assert_eq!(map.get(&"new".to_string()), None);
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
    fn set_after_merge_with_higher_timestamp_records_new_dot() {
        // A local set creates a new dot even if its HLC is lower; the
        // already live higher-HLC dot remains the visible winner.
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

        // A merges B: the higher-HLC live dot wins.
        map_a.merge(&map_b);
        assert_eq!(map_a.get(&"k".to_string()), Some(&"value_b".to_string()));

        // A writes at ts=150, retaining the earlier live dots.
        let updated = map_a.set(
            "k".to_string(),
            "value_c".to_string(),
            ts(150, 0, "node-a"),
            &node("node-a"),
        );
        assert!(updated);
        assert_eq!(map_a.get(&"k".to_string()), Some(&"value_b".to_string()));
        assert_eq!(map_a.entries["k"].len(), 3);
    }

    #[test]
    fn set_returns_true_on_success() {
        let mut map = OrMap::new();
        let result = map.set("k".to_string(), 42, ts(100, 0, "node-a"), &node("node-a"));
        assert!(result);
        assert_eq!(map.get(&"k".to_string()), Some(&42));
    }

    #[test]
    fn set_with_equal_timestamp_uses_value_tiebreak() {
        let mut map = OrMap::new();
        map.set("k".to_string(), 1, ts(100, 0, "node-a"), &node("node-a"));

        let updated = map.set("k".to_string(), 2, ts(100, 0, "node-a"), &node("node-a"));
        assert!(updated);
        assert_eq!(map.get(&"k".to_string()), Some(&2));
        assert_eq!(map.entries["k"].len(), 2);
    }

    #[test]
    fn removed_higher_value_never_wins_over_concurrent_live_dot() {
        let key = "k".to_string();
        let mut first = OrMap::new();
        first.set(key.clone(), 100, ts(100, 0, "A"), &node("A"));
        let mut concurrent = OrMap::new();
        concurrent.set(key.clone(), 50, ts(50, 0, "B"), &node("B"));

        // B learned the higher value before it learned A's deletion.
        concurrent.merge(&first);
        assert_eq!(concurrent.get(&key), Some(&100));
        let mut deleted = first;
        deleted.delete(&key);
        concurrent.merge(&deleted);
        assert_eq!(concurrent.get(&key), Some(&50));

        let mut opposite = deleted;
        opposite.merge(&concurrent);
        assert_eq!(opposite.get(&key), Some(&50));
    }

    #[test]
    fn delete_removes_every_observed_write_dot() {
        let key = "k".to_string();
        let mut map = OrMap::new();
        map.set(key.clone(), 1, ts(100, 0, "A"), &node("A"));
        map.set(key.clone(), 2, ts(200, 0, "A"), &node("A"));
        let before_delete = map.clone();
        assert_eq!(map.entries["k"].len(), 2);

        map.delete(&key);
        assert_eq!(map.deferred_len(), 2);
        map.merge(&before_delete);
        assert_eq!(map.get(&key), None);
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
    fn delta_from_keeps_wire_counter_context_for_live_and_removed_dots() {
        let mut current = OrMap::new();
        current.set("a".to_string(), 1, ts(100, 0, "A"), &node("A"));
        let old = current.clone();

        current.set("a".to_string(), 2, ts(200, 0, "B"), &node("B"));
        let delta = current.delta_from(&old).unwrap();
        let bytes = serde_json::to_vec(&delta).unwrap();
        let received: OrMap<String, i32> = serde_json::from_slice(&bytes).unwrap();
        let mut merged = old.clone();
        merged.merge(&received);
        assert_eq!(merged.get(&"a".to_string()), current.get(&"a".to_string()));

        current.delete(&"a".to_string());
        let delta = current.delta_from(&old).unwrap();
        let bytes = serde_json::to_vec(&delta).unwrap();
        let received: OrMap<String, i32> = serde_json::from_slice(&bytes).unwrap();
        let mut merged = old;
        merged.merge(&received);
        assert!(!merged.contains_key(&"a".to_string()));
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

        let swept = map_a.compact_deferred_certified(&map_a.deferred_dots());
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
        map_a.compact_deferred_certified(&map_a.deferred_dots());
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
        map.compact_deferred_certified(&map.deferred_dots());
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
        sender.compact_deferred_certified(&sender.deferred_dots());
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
        map.compact_deferred_certified(&map.deferred_dots());
        let old = map.clone();

        map.set("b".to_string(), 2, ts(200, 0, "A"), &n);
        let delta = map.delta_from(&old).unwrap();
        assert!(delta.compaction_floor.is_empty(), "INV-W");
    }

    #[test]
    fn delta_from_floor_advance_sends_complete_live_set() {
        let n = node("A");
        let mut sender = OrMap::new();
        sender.set("kept".to_string(), 1, ts(100, 0, "A"), &n);
        sender.set("removed".to_string(), 2, ts(101, 0, "A"), &n);
        let old = sender.clone();

        sender.delete(&"removed".to_string());
        sender.compact_deferred_certified(&sender.deferred_dots());
        assert_eq!(sender.compaction_floor().get(&n), Some(&2));
        assert_eq!(sender.deferred_len(), 0);

        let delta = sender.delta_from(&old).expect("floor advance must be sent");
        assert_eq!(delta.compaction_floor(), sender.compaction_floor());
        let mut receiver = old;
        receiver.merge_delta(&delta);
        assert_eq!(receiver.get(&"kept".to_string()), Some(&1));
        assert!(!receiver.contains_key(&"removed".to_string()));
    }

    #[test]
    fn delta_from_detects_removal_of_live_dot_under_unchanged_floor() {
        let n = node("A");
        let mut sender = OrMap::new();
        sender.set("kept".to_string(), 1, ts(100, 0, "A"), &n);
        sender.set("already_removed".to_string(), 2, ts(101, 0, "A"), &n);
        sender.delete(&"already_removed".to_string());
        sender.compact_deferred_certified(&sender.deferred_dots());
        assert_eq!(sender.compaction_floor().get(&n), Some(&2));
        let old = sender.clone();

        sender.delete(&"kept".to_string());
        sender.compact_deferred_certified(&sender.deferred_dots());
        assert_eq!(sender.compaction_floor(), old.compaction_floor());
        assert_eq!(sender.deferred_len(), 0);

        let delta = sender
            .delta_from(&old)
            .expect("covered live removal must be sent");
        assert_eq!(delta.compaction_floor(), sender.compaction_floor());
        let mut receiver = old;
        receiver.merge_delta(&delta);
        assert!(!receiver.contains_key(&"kept".to_string()));
    }

    /// The optional floor defaults to empty when omitted from JSON.
    #[test]
    fn serde_missing_floor_defaults_empty() {
        let mut map = OrMap::new();
        map.set(
            "k".to_string(),
            "v".to_string(),
            ts(100, 0, "A"),
            &node("A"),
        );
        let mut json = serde_json::to_value(&map).unwrap();
        json.as_object_mut().unwrap().remove("compaction_floor");
        let restored: OrMap<String, String> = serde_json::from_value(json).unwrap();
        assert_eq!(restored.get(&"k".to_string()), Some(&"v".to_string()));
        assert!(restored.compaction_floor().is_empty());
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

        let outcome = map.compact_deferred_certified(&marked);
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
    fn merge_changed_dot_value_timestamp_directions() {
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

        // B's newer live dot wins after merge.
        let fx = merge_ground_truth(&mut a, &newer);
        assert!(fx.changed);
        assert_eq!(a.get(&"k".to_string()), Some(&"new".to_string()));

        // Older value re-offered: the dot is still adopted (it
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
        swept.compact_deferred_certified(&swept.deferred_dots());
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
        a.compact_deferred_certified(&a.deferred_dots());

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
    /// value adopted, `changed == false`. A rejected re-offer must not
    /// create a transient empty entry, since that would make the RR gate
    /// report a change on every round.
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
        b.compact_deferred_certified(&b.deferred_dots());
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
