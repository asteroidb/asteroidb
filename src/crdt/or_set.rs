//! Observed-Remove Set (OR-Set) CRDT with add-wins semantics.
//!
//! Each add operation is tagged with a unique dot (node_id, counter) pair.
//! Remove only deletes the dots currently observed, so a concurrent add
//! on another node will survive after merge — giving "add-wins" behaviour.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::crdt::{MergeEffects, SweepOutcome, covered};
use crate::hlc::HlcTimestamp;
use crate::types::NodeId;

/// A unique identifier for each add operation (a "dot" in the dot-store model).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Dot {
    pub node_id: NodeId,
    pub counter: u64,
}

/// Observed-Remove Set with add-wins semantics.
///
/// Elements are associated with the set of dots that added them.
/// Removal only tombstones the currently observed dots, so a concurrent
/// add (with a new dot) always wins.
///
/// A causal context (`deferred` set) tracks all dots that have been removed.
/// During merge, dots present in the remote's deferred set are discarded,
/// ensuring that a remove on one replica propagates correctly to others.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrSet<T: Eq + Hash> {
    /// Maps each element to the set of dots that justify its presence.
    elements: HashMap<T, HashSet<Dot>>,
    /// Per-node monotonic counters used to generate fresh dots.
    counters: HashMap<NodeId, u64>,
    /// Causal context / tombstone set: all dots that have ever been removed.
    /// Needed so merge can distinguish "this dot was removed" from "never seen".
    #[serde(default)]
    deferred: HashSet<Dot>,
    /// Per-node GC floor: the fate of every dot `(n, c)` with
    /// `c <= compaction_floor[n]` is DECIDED — live if it is in the live
    /// dot sets, removed otherwise. Tombstones at or below the floor are
    /// therefore redundant (a compressed tombstone) and can be physically
    /// dropped, while stale live dots at or below the floor can be killed
    /// on merge exactly like a tombstone would.
    ///
    /// INV-FLOOR: the floor advances ONLY through (i) the gated certified
    /// sweep's contiguous walk ([`compact_deferred_certified`]) and
    /// (ii) pointwise-max inheritance on [`merge`]. It is NEVER derived
    /// from counters or HLC timestamps (the P1-10 unit-mismatch bug is
    /// structurally excluded).
    ///
    /// INV-W (wire invariant): a payload carrying a non-empty floor must
    /// contain the value's COMPLETE live dot set. Full states and
    /// [`delta_since`] (a full clone) keep the floor; the partial
    /// [`delta_from`] payload ships an empty floor.
    ///
    /// [`compact_deferred_certified`]: Self::compact_deferred_certified
    /// [`merge`]: Self::merge
    /// [`delta_since`]: Self::delta_since
    /// [`delta_from`]: Self::delta_from
    #[serde(default)]
    compaction_floor: HashMap<NodeId, u64>,
}

/// Frozen structural layout of [`OrSet`] as persisted before the
/// `compaction_floor` field existed (snapshot formats v1–v4, WAL format
/// v1). bincode is positional and non-self-describing, so old payloads
/// must be decoded with exactly this layout and converted (`From`) with
/// an empty floor.
#[derive(Debug, Deserialize)]
pub(crate) struct OrSetV4Layout<T: Eq + Hash> {
    elements: HashMap<T, HashSet<Dot>>,
    counters: HashMap<NodeId, u64>,
    #[serde(default)]
    deferred: HashSet<Dot>,
}

impl<T: Eq + Hash> From<OrSetV4Layout<T>> for OrSet<T> {
    fn from(old: OrSetV4Layout<T>) -> Self {
        OrSet {
            elements: old.elements,
            counters: old.counters,
            deferred: old.deferred,
            compaction_floor: HashMap::new(),
        }
    }
}

impl<T> OrSet<T>
where
    T: Eq + Hash + Clone + Serialize + DeserializeOwned,
{
    /// Creates an empty OR-Set.
    pub fn new() -> Self {
        Self {
            elements: HashMap::new(),
            counters: HashMap::new(),
            deferred: HashSet::new(),
            compaction_floor: HashMap::new(),
        }
    }

    /// Adds an element with a fresh unique dot generated from `node_id`.
    pub fn add(&mut self, element: T, node_id: &NodeId) {
        let counter = self.counters.entry(node_id.clone()).or_insert(0);
        *counter += 1;
        let dot = Dot {
            node_id: node_id.clone(),
            counter: *counter,
        };
        self.elements.entry(element).or_default().insert(dot);
    }

    /// Removes an element by moving all of its currently observed dots
    /// into the causal context (deferred / tombstone set).
    ///
    /// If the element is not present this is a no-op. After removal,
    /// merging with a replica that still has those dots will NOT resurrect
    /// the element, because the dots are in the deferred set.
    pub fn remove(&mut self, element: &T) {
        if let Some(dots) = self.elements.remove(element) {
            for d in dots {
                self.deferred.insert(d);
            }
        }
    }

    /// Returns `true` if the set currently contains the element.
    pub fn contains(&self, element: &T) -> bool {
        self.elements
            .get(element)
            .is_some_and(|dots| !dots.is_empty())
    }

    /// Returns an iterator-collected `HashSet` of references to all
    /// elements currently in the set.
    pub fn elements(&self) -> HashSet<&T> {
        self.elements
            .iter()
            .filter(|(_, dots)| !dots.is_empty())
            .map(|(elem, _)| elem)
            .collect()
    }

    /// Returns the number of distinct elements in the set.
    pub fn len(&self) -> usize {
        self.elements
            .iter()
            .filter(|(_, dots)| !dots.is_empty())
            .count()
    }

    /// Returns `true` if the set contains no elements.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Merges another OR-Set into this one, returning floor diagnostics.
    ///
    /// For each element:
    /// - Dots from the other side are added only if NOT in our deferred
    ///   set AND not stale — a dot covered by our pre-merge floor that we
    ///   do not already hold live is a certified-removed dot and is
    ///   rejected (the floor acts as a compressed tombstone).
    /// - Dots on our side are removed if they ARE in the other's deferred
    ///   set, OR are covered by the other's floor while absent from the
    ///   other's live set for the element (floor kill; sound only because
    ///   of INV-W — a non-empty floor rides complete live sets only).
    ///
    /// This gives correct observed-remove semantics: a remove on one
    /// replica propagates via its deferred set (or, once compacted, via
    /// its floor), while a concurrent add (with a fresh dot above every
    /// floor and in no deferred set) survives — add-wins behaviour.
    ///
    /// Node counters and compaction floors are merged by pointwise max
    /// (the floor is a join-semilattice: inheritance is irrevocable).
    /// Deferred (tombstone) sets are merged as a union MINUS the dots
    /// covered by the merged floor, with one origin-retention exception:
    /// our own tombstones that were ALREADY covered by our pre-merge
    /// floor (fresh removes below the floor) are kept until our own gated
    /// sweep discards them — old (pre-floor) peers learn removes from
    /// tombstones only, and the C-2 gate discipline requires the physical
    /// discard to happen strictly after every registry peer acked the
    /// post-remove state.
    pub fn merge(&mut self, other: &OrSet<T>) -> MergeEffects {
        let mut fx = MergeEffects::default();
        let floor_pre = self.compaction_floor.clone();
        let other_has_tombstones = !other.deferred.is_empty();
        let other_has_floor = !other.compaction_floor.is_empty();

        // `fx.changed` is set at EVERY site that mutates physical state
        // (dot adopted/dropped, counter/floor raised, tombstone
        // adopted/dropped) and nowhere else, so `changed == false`
        // guarantees `pre == post` — the RR-gate contract (M-6). The
        // rejection sites (`rejected_stale_live`,
        // `rejected_covered_deferred`) adopt nothing and must not flag.

        // Process elements present in the other replica.
        for (elem, other_dots) in &other.elements {
            if let Some(dots) = self.elements.get_mut(elem) {
                // Element exists in both — merge dots in place without cloning the key.
                for dot in other_dots {
                    if self.deferred.contains(dot) || dots.contains(dot) {
                        continue;
                    }
                    if covered(&floor_pre, &dot.node_id, dot.counter) {
                        // Fate decided by our floor and we do not hold it
                        // live: a stale re-offer of a removed dot.
                        fx.rejected_stale_live += 1;
                        continue;
                    }
                    dots.insert(dot.clone());
                    fx.changed = true;
                }
                // Remove our dots that the other has tombstoned or
                // floor-compacted away.
                if other_has_tombstones || other_has_floor {
                    dots.retain(|dot| {
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
                }
            } else {
                // Element only in other — filter and insert directly.
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
                    self.elements.insert(elem.clone(), filtered);
                    fx.changed = true;
                }
            }
        }

        // Apply other's tombstones and floor to self-only elements (not in
        // other.elements — their dots are trivially absent from the
        // other's live set, so the floor clause applies unconditionally).
        if other_has_tombstones || other_has_floor {
            for (elem, dots) in &mut self.elements {
                if !other.elements.contains_key(elem) {
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
        }

        // Remove entries with no remaining dots. (Not a `changed` site:
        // an entry can only become empty through a dot-dropping retain
        // above, which already flagged.)
        self.elements.retain(|_, dots| !dots.is_empty());

        // Merge counters so future dots stay globally unique. Only an
        // actual raise mutates state; in particular an absent entry is
        // NOT materialised for `other_counter == 0` (a ghost zero entry
        // would be a physical change invisible to `changed`, breaking
        // the `changed == false ⇒ pre == post` contract).
        for (node_id, &other_counter) in &other.counters {
            match self.counters.get_mut(node_id) {
                Some(counter) => {
                    if other_counter > *counter {
                        *counter = other_counter;
                        fx.changed = true;
                    }
                }
                None => {
                    if other_counter > 0 {
                        self.counters.insert(node_id.clone(), other_counter);
                        fx.changed = true;
                    }
                }
            }
        }

        // Merge the compaction floor by pointwise max (irrevocable), then
        // defensively clamp counters to the floor (INV-CTR: a corrupted
        // payload with floor > counters must not let a future add reuse a
        // fate-decided counter value). Same no-ghost-entry discipline as
        // the counters above.
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

        // Merge deferred (tombstone) sets under the merged floor.
        // Origin-retention: keep our own tombstones that were already
        // covered by our PRE-merge floor (fresh removes below the floor —
        // they wait for our own gated sweep); drop the ones newly covered
        // by the inherited floor (their remove is certified cluster-wide).
        let floor_merged = &self.compaction_floor;
        self.deferred.retain(|d| {
            let keep = !covered(floor_merged, &d.node_id, d.counter)
                || covered(&floor_pre, &d.node_id, d.counter);
            if !keep {
                fx.changed = true;
            }
            keep
        });
        // Incoming tombstones covered by the merged floor are redundant
        // ("floor + absence" already encodes the remove) — never adopted.
        // This is the closure of the M-8 reinjection path: a lagging
        // peer's stale tombstone can no longer roll our GC back.
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

    /// Merge a delta into this set.
    ///
    /// For OrSet, `merge_delta` is identical to `merge` because the delta
    /// is the same type (a subset of elements and deferred entries).
    pub fn merge_delta(&mut self, delta: &OrSet<T>) -> MergeEffects {
        self.merge(delta)
    }

    /// Extract changes since the given frontier timestamp.
    ///
    /// OrSet dots do not carry HLC timestamps, so this method returns the
    /// full set state when the set is non-empty (the caller is responsible
    /// for checking the key-level HLC before invoking this). Returns `None`
    /// when the set is empty and has no tombstones.
    pub fn delta_since(&self, _frontier: &HlcTimestamp) -> Option<Self> {
        if self.elements.is_empty() && self.deferred.is_empty() && self.counters.is_empty() {
            return None;
        }
        Some(self.clone())
    }

    /// Compute a true incremental delta against a known old state.
    ///
    /// Returns an OrSet containing only:
    /// - Elements whose dots are NOT present in `old`
    /// - Deferred (tombstone) dots NOT present in `old`
    /// - Updated counters
    ///
    /// The `compaction_floor` is deliberately EMPTY (INV-W): this payload
    /// carries an incomplete live set, and a receiver applying the floor
    /// kill rule against it would destroy live dots that simply were not
    /// part of the delta. The floor travels on complete states only.
    ///
    /// Returns `None` if there are no changes.
    pub fn delta_from(&self, old: &OrSet<T>) -> Option<Self> {
        let mut delta = OrSet {
            elements: HashMap::new(),
            counters: HashMap::new(),
            deferred: HashSet::new(),
            // INV-W: partial payload — never ship the floor.
            compaction_floor: HashMap::new(),
        };
        let mut has_changes = false;

        // Find new/changed elements (dots not in old).
        let old_all_dots: HashSet<&Dot> =
            old.elements.values().flat_map(|dots| dots.iter()).collect();

        for (elem, dots) in &self.elements {
            let new_dots: HashSet<Dot> = dots
                .iter()
                .filter(|d| !old_all_dots.contains(d))
                .cloned()
                .collect();
            if !new_dots.is_empty() {
                delta.elements.insert(elem.clone(), new_dots);
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

        // Include updated counters so the receiver can generate fresh dots.
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
    /// pairs — the MARK phase of the gated mark-and-sweep tombstone GC
    /// (see [`crate::crdt::gc::TombstoneGc`]).
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
    /// MUST only be called after the caller's replica-synchronisation
    /// gates passed against the mark that produced `candidates` (see
    /// [`crate::crdt::gc::TombstoneGc::mark_and_sweep`]): every dot folded
    /// into the floor becomes irrevocably "removed unless live".
    ///
    /// The walk for node `n` starts at `floor[n] + 1` and advances while
    /// the dot is live (fate: live), or a MARKED tombstone (fate: removed
    /// — this is the compression), stopping at:
    /// - an UNMARKED tombstone (created after the mark; the next cycle's
    ///   gate covers it) → `stalled_uncandidated`;
    /// - a hole (neither live nor deferred: a dot discarded by the legacy
    ///   pre-floor sweep) → `stalled_holes`, unless the hole is covered
    ///   by `hole_jump_ceilings` (Stage 2): the node's counters AS
    ///   SNAPSHOTTED AT MARK TIME. Hole-jump additionally requires the
    ///   caller's INBOUND gate: having merged every registry peer's
    ///   complete state since the mark proves a MARK-TIME hole dot is
    ///   live nowhere, i.e. removed. Holes above the mark-time counters
    ///   were minted after the mark (e.g. by an inbound partial delta
    ///   whose counters arrived without their entry) and stall the walk
    ///   even under Stage 2 — the inbound evidence says nothing about
    ///   them. Pass `None` for Stage 1 (fail-closed on every hole).
    ///
    /// Hole spans are crossed as intervals between consecutive known
    /// dots (`O(state·log state)`, independent of the node's lifetime
    /// add count — see [`crate::crdt::advance_compaction_floor`]).
    ///
    /// Dots of nodes absent from `counters` keep a floor of 0 and their
    /// tombstones are retained (unknown writer — keep, same conservatism
    /// as the legacy sweep). Physical deletion is candidate-gated: a
    /// covered but UNMARKED tombstone (a fresh remove below the floor) is
    /// retained until a later gated sweep — origin-retention, the C-2
    /// discipline that old peers learn removes from tombstones only.
    pub fn compact_deferred_certified(
        &mut self,
        candidates: &HashSet<(NodeId, u64)>,
        hole_jump_ceilings: Option<&HashMap<NodeId, u64>>,
    ) -> SweepOutcome {
        let live_dots: HashSet<(&NodeId, u64)> = self
            .elements
            .values()
            .flat_map(|dots| dots.iter())
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
}

impl OrSet<String> {
    /// Feed this set's canonical byte representation into `hasher`
    /// (digest-based anti-entropy, scheme v2).
    ///
    /// Stream: `0x03` ‖ live elements (byte order) with their dots (dot
    /// order) ‖ counters (node-id order) ‖ compaction floor (node-id
    /// order) ‖ UNCOVERED deferred dots (dot order). Elements whose dot
    /// set is empty are skipped: they are semantically equivalent to
    /// absent entries (`merge` normally retains them away, but the
    /// normalisation is specified defensively so representation
    /// differences never cause false digest mismatches).
    ///
    /// Deferred dots covered by the floor are EXCLUDED (canonical form):
    /// an origin-retained fresh tombstone below the floor is
    /// information-equivalent to "floor + absence", and including it
    /// would produce a false mismatch on every remove until the origin's
    /// gated sweep. With the exclusion, "digest matched" ⟺ canonical
    /// state equality ⟺ merging either way is a no-op on observable
    /// state — which is what keeps session-claim adoption on a match
    /// sound. The floor itself and the uncovered deferred dots ARE
    /// digested: replicas differing there are semantically different and
    /// must exchange state (the bucket transfer then propagates the
    /// floor — the self-healing round trip).
    ///
    /// # MAINTAINER CONTRACT
    /// Adding a field to `OrSet`/`Dot` REQUIRES updating this method and
    /// bumping `crate::store::digest::DIGEST_SCHEME_VERSION` — otherwise
    /// replicas that differ only in the new field report "digest matched"
    /// and session-guarantee claims become unsound. "Digest matched" is
    /// defined as CANONICAL state equality: any new field must either be
    /// digested or be provably redundant with the digested fields (as the
    /// covered deferred dots are). Instantiating `OrSet` for a new
    /// element type in `CrdtValue` requires defining that type's
    /// canonical byte encoding here, plus a scheme version bump.
    pub(crate) fn digest_into(&self, hasher: &mut sha2::Sha256) {
        use crate::crdt::digest::{write_counters, write_dots, write_str, write_u32};
        use sha2::Digest as _;

        hasher.update([0x03]);
        let mut elems: Vec<(&String, &HashSet<Dot>)> = self
            .elements
            .iter()
            .filter(|(_, dots)| !dots.is_empty())
            .collect();
        elems.sort_unstable_by(|a, b| a.0.cmp(b.0));
        write_u32(hasher, elems.len() as u32);
        for (elem, dots) in elems {
            write_str(hasher, elem);
            write_dots(
                hasher,
                dots.iter().map(|d| (d.node_id.0.as_str(), d.counter)),
            );
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

impl<T: Eq + Hash + Clone + Serialize + DeserializeOwned> Default for OrSet<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(name: &str) -> NodeId {
        NodeId(name.into())
    }

    // ---------------------------------------------------------------
    // Basic operations
    // ---------------------------------------------------------------

    #[test]
    fn new_set_is_empty() {
        let set: OrSet<String> = OrSet::new();
        assert!(set.is_empty());
        assert_eq!(set.len(), 0);
    }

    #[test]
    fn add_and_contains() {
        let mut set = OrSet::new();
        let n = node("A");
        set.add("x".to_string(), &n);
        assert!(set.contains(&"x".to_string()));
        assert!(!set.contains(&"y".to_string()));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn add_and_remove() {
        let mut set = OrSet::new();
        let n = node("A");
        set.add("x".to_string(), &n);
        assert!(set.contains(&"x".to_string()));

        set.remove(&"x".to_string());
        assert!(!set.contains(&"x".to_string()));
        assert!(set.is_empty());
    }

    #[test]
    fn remove_nonexistent_is_noop() {
        let mut set: OrSet<String> = OrSet::new();
        set.remove(&"ghost".to_string()); // should not panic
        assert!(set.is_empty());
    }

    #[test]
    fn add_duplicate_element() {
        let mut set = OrSet::new();
        let n = node("A");
        set.add("x".to_string(), &n);
        set.add("x".to_string(), &n);
        // Still one logical element, but two dots.
        assert_eq!(set.len(), 1);
        assert!(set.contains(&"x".to_string()));
    }

    #[test]
    fn multiple_elements() {
        let mut set = OrSet::new();
        let n = node("A");
        set.add("a".to_string(), &n);
        set.add("b".to_string(), &n);
        set.add("c".to_string(), &n);
        assert_eq!(set.len(), 3);

        let elems = set.elements();
        assert!(elems.contains(&"a".to_string()));
        assert!(elems.contains(&"b".to_string()));
        assert!(elems.contains(&"c".to_string()));
    }

    #[test]
    fn re_add_after_remove() {
        let mut set = OrSet::new();
        let n = node("A");
        set.add("x".to_string(), &n);
        set.remove(&"x".to_string());
        assert!(!set.contains(&"x".to_string()));

        set.add("x".to_string(), &n);
        assert!(set.contains(&"x".to_string()));
    }

    // ---------------------------------------------------------------
    // Merge & convergence
    // ---------------------------------------------------------------

    #[test]
    fn merge_disjoint_elements() {
        let na = node("A");
        let nb = node("B");

        let mut set_a = OrSet::new();
        set_a.add("x".to_string(), &na);

        let mut set_b = OrSet::new();
        set_b.add("y".to_string(), &nb);

        set_a.merge(&set_b);
        assert!(set_a.contains(&"x".to_string()));
        assert!(set_a.contains(&"y".to_string()));
        assert_eq!(set_a.len(), 2);
    }

    #[test]
    fn add_wins_concurrent_add_remove() {
        // Node A adds "x", node B independently removes "x".
        // After merge the add should win because B's remove only
        // tombstones the dots B has observed — not A's fresh dot.
        let na = node("A");

        // Start from a common state where both know "x".
        let mut common = OrSet::new();
        common.add("x".to_string(), &na);

        // Fork into two replicas.
        let mut replica_a = common.clone();
        let mut replica_b = common.clone();

        // A adds "x" again (new dot) concurrently.
        replica_a.add("x".to_string(), &na);

        // B removes "x" (only sees the original dot).
        replica_b.remove(&"x".to_string());

        // Merge B into A — A's new dot survives.
        replica_a.merge(&replica_b);
        assert!(
            replica_a.contains(&"x".to_string()),
            "add-wins: element should be present after merge"
        );

        // Merge A into B — symmetric result.
        replica_b.merge(&replica_a);
        assert!(
            replica_b.contains(&"x".to_string()),
            "add-wins: element should be present after symmetric merge"
        );
    }

    #[test]
    fn two_node_convergence() {
        let na = node("A");
        let nb = node("B");

        let mut set_a = OrSet::new();
        set_a.add("apple".to_string(), &na);
        set_a.add("banana".to_string(), &na);

        let mut set_b = OrSet::new();
        set_b.add("cherry".to_string(), &nb);
        set_b.add("date".to_string(), &nb);

        // Cross-merge.
        set_a.merge(&set_b);
        set_b.merge(&set_a);

        // Both replicas should see the same four elements.
        assert_eq!(set_a.len(), 4);
        assert_eq!(set_b.len(), 4);
        assert_eq!(set_a.elements(), set_b.elements());
    }

    #[test]
    fn idempotent_merge() {
        let na = node("A");

        let mut set_a = OrSet::new();
        set_a.add("x".to_string(), &na);

        let nb = node("B");
        let mut set_b = OrSet::new();
        set_b.add("y".to_string(), &nb);

        set_a.merge(&set_b);
        let snapshot = set_a.clone();

        // Merging again should not change anything.
        set_a.merge(&set_b);
        assert_eq!(set_a.len(), snapshot.len());
        assert_eq!(set_a.elements(), snapshot.elements());
    }

    #[test]
    fn merge_updates_counters() {
        let na = node("A");

        let mut set_a = OrSet::new();
        set_a.add("x".to_string(), &na);
        set_a.add("y".to_string(), &na); // counter for A is now 2

        let mut set_b: OrSet<String> = OrSet::new();
        set_b.merge(&set_a);

        // After merge, B's counter for node A should be at least 2
        // so that a subsequent add on B (as A) generates counter 3.
        set_b.add("z".to_string(), &na);
        assert_eq!(*set_b.counters.get(&na).unwrap(), 3);
    }

    // ---------------------------------------------------------------
    // Serde round-trip
    // ---------------------------------------------------------------

    #[test]
    fn serde_round_trip() {
        let na = node("A");
        let mut set = OrSet::new();
        set.add("hello".to_string(), &na);
        set.add("world".to_string(), &na);

        let json = serde_json::to_string(&set).unwrap();
        let restored: OrSet<String> = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.len(), 2);
        assert!(restored.contains(&"hello".to_string()));
        assert!(restored.contains(&"world".to_string()));
    }

    // ---------------------------------------------------------------
    // Integer element type
    // ---------------------------------------------------------------

    #[test]
    fn works_with_integer_elements() {
        let na = node("A");
        let mut set = OrSet::new();
        set.add(42_i64, &na);
        set.add(99_i64, &na);
        assert_eq!(set.len(), 2);
        assert!(set.contains(&42));
        assert!(set.contains(&99));

        set.remove(&42);
        assert_eq!(set.len(), 1);
        assert!(!set.contains(&42));
    }

    // ---------------------------------------------------------------
    // Remove propagation via causal context (#200)
    // ---------------------------------------------------------------

    #[test]
    fn remove_propagates_across_replicas() {
        // Both replicas share "x". B removes "x". After merging B into A,
        // "x" should be gone — the bug before #200 was that A's dot for "x"
        // survived because merge was a pure union.
        let na = node("A");
        let mut common = OrSet::new();
        common.add("x".to_string(), &na);

        let mut replica_a = common.clone();
        let mut replica_b = common.clone();

        // B removes "x".
        replica_b.remove(&"x".to_string());
        assert!(!replica_b.contains(&"x".to_string()));

        // Merge B into A — A should now also be missing "x".
        replica_a.merge(&replica_b);
        assert!(
            !replica_a.contains(&"x".to_string()),
            "remove on B should propagate to A via merge"
        );
        assert!(replica_a.is_empty());
    }

    #[test]
    fn remove_propagates_symmetrically() {
        // Same test but merging in the other direction.
        let na = node("A");
        let mut common = OrSet::new();
        common.add("x".to_string(), &na);

        let mut replica_a = common.clone();
        let mut replica_b = common.clone();

        // A removes "x".
        replica_a.remove(&"x".to_string());

        // Merge A into B.
        replica_b.merge(&replica_a);
        assert!(
            !replica_b.contains(&"x".to_string()),
            "remove on A should propagate to B via merge"
        );
    }

    #[test]
    fn concurrent_add_and_remove_add_wins() {
        // A adds "x" again (new dot) while B removes "x" (old dot).
        // After merge, the new dot from A should survive — add-wins.
        let na = node("A");

        let mut common = OrSet::new();
        common.add("x".to_string(), &na);

        let mut replica_a = common.clone();
        let mut replica_b = common.clone();

        // A adds "x" again (fresh dot, counter=2).
        replica_a.add("x".to_string(), &na);

        // B removes "x" (only has the original dot with counter=1).
        replica_b.remove(&"x".to_string());

        // Merge B into A — A's new dot (counter=2) is NOT in B's deferred.
        replica_a.merge(&replica_b);
        assert!(
            replica_a.contains(&"x".to_string()),
            "add-wins: concurrent add should survive remove"
        );

        // Merge A into B — symmetric result.
        replica_b.merge(&replica_a);
        assert!(
            replica_b.contains(&"x".to_string()),
            "add-wins: symmetric merge should also preserve the element"
        );
    }

    #[test]
    fn both_replicas_remove_then_merge() {
        // Both replicas remove the same element. After merge, the element
        // should still be gone.
        let na = node("A");
        let mut common = OrSet::new();
        common.add("x".to_string(), &na);

        let mut replica_a = common.clone();
        let mut replica_b = common.clone();

        replica_a.remove(&"x".to_string());
        replica_b.remove(&"x".to_string());

        replica_a.merge(&replica_b);
        assert!(!replica_a.contains(&"x".to_string()));

        replica_b.merge(&replica_a);
        assert!(!replica_b.contains(&"x".to_string()));
    }

    #[test]
    fn remove_propagates_only_for_correct_element() {
        // Ensure removing "x" on B does not affect unrelated "y" on A.
        let na = node("A");
        let nb = node("B");

        let mut replica_a = OrSet::new();
        replica_a.add("x".to_string(), &na);
        replica_a.add("y".to_string(), &na);

        let mut replica_b = replica_a.clone();

        // B removes only "x".
        replica_b.remove(&"x".to_string());

        replica_a.merge(&replica_b);
        assert!(
            !replica_a.contains(&"x".to_string()),
            "removed element should be gone"
        );
        assert!(
            replica_a.contains(&"y".to_string()),
            "unrelated element should survive"
        );
        assert_eq!(replica_a.len(), 1);

        // Also test: B adds something new that should survive.
        replica_b.add("z".to_string(), &nb);
        replica_a.merge(&replica_b);
        assert!(replica_a.contains(&"z".to_string()));
        assert_eq!(replica_a.len(), 2); // "y" and "z"
    }

    #[test]
    fn multiple_add_remove_cycles_converge() {
        // Simulate several add/remove cycles across two replicas.
        let na = node("A");

        let mut replica_a = OrSet::new();
        let mut replica_b = OrSet::new();

        // A adds "x".
        replica_a.add("x".to_string(), &na);
        // Sync.
        replica_b.merge(&replica_a);
        assert!(replica_b.contains(&"x".to_string()));

        // B removes "x".
        replica_b.remove(&"x".to_string());
        // Sync.
        replica_a.merge(&replica_b);
        assert!(!replica_a.contains(&"x".to_string()));

        // A adds "x" again (fresh dot).
        replica_a.add("x".to_string(), &na);
        // Sync.
        replica_b.merge(&replica_a);
        assert!(replica_b.contains(&"x".to_string()));

        // B removes "x" again.
        replica_b.remove(&"x".to_string());
        // A concurrently adds "x" yet again.
        replica_a.add("x".to_string(), &na);

        // Cross-merge — A's newest add should win.
        replica_a.merge(&replica_b);
        replica_b.merge(&replica_a);

        assert!(replica_a.contains(&"x".to_string()));
        assert!(replica_b.contains(&"x".to_string()));
        assert_eq!(replica_a.elements(), replica_b.elements());
    }

    #[test]
    fn three_replica_convergence() {
        let na = node("A");
        let nb = node("B");
        let nc = node("C");

        let mut r1 = OrSet::new();
        let mut r2 = OrSet::new();
        let mut r3 = OrSet::new();

        // Everyone adds something.
        r1.add("x".to_string(), &na);
        r2.add("y".to_string(), &nb);
        r3.add("z".to_string(), &nc);

        // Full exchange round.
        let snap1 = r1.clone();
        let snap2 = r2.clone();
        let snap3 = r3.clone();
        r1.merge(&snap2);
        r1.merge(&snap3);
        r2.merge(&snap1);
        r2.merge(&snap3);
        r3.merge(&snap1);
        r3.merge(&snap2);

        assert_eq!(r1.len(), 3);
        assert_eq!(r1.elements(), r2.elements());
        assert_eq!(r2.elements(), r3.elements());

        // R2 removes "x".
        r2.remove(&"x".to_string());

        // Full exchange again.
        let snap1 = r1.clone();
        let snap2 = r2.clone();
        let snap3 = r3.clone();
        r1.merge(&snap2);
        r1.merge(&snap3);
        r2.merge(&snap1);
        r2.merge(&snap3);
        r3.merge(&snap1);
        r3.merge(&snap2);

        // All replicas should agree: "x" is gone, "y" and "z" remain.
        for r in [&r1, &r2, &r3] {
            assert!(!r.contains(&"x".to_string()), "x should be removed");
            assert!(r.contains(&"y".to_string()), "y should survive");
            assert!(r.contains(&"z".to_string()), "z should survive");
            assert_eq!(r.len(), 2);
        }
    }

    // ---------------------------------------------------------------
    // Serde round-trip with deferred (#200)
    // ---------------------------------------------------------------

    #[test]
    fn serde_round_trip_with_deferred() {
        let na = node("A");
        let mut set = OrSet::new();
        set.add("hello".to_string(), &na);
        set.add("world".to_string(), &na);
        set.remove(&"hello".to_string());

        let json = serde_json::to_string(&set).unwrap();
        let restored: OrSet<String> = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.len(), 1);
        assert!(!restored.contains(&"hello".to_string()));
        assert!(restored.contains(&"world".to_string()));

        // The deferred set should have been preserved.
        assert!(!restored.deferred.is_empty());
    }

    #[test]
    fn serde_backward_compat_missing_deferred() {
        // Old serialized format without "deferred" field should still
        // deserialize thanks to #[serde(default)].
        let json = r#"{"elements":{"a":[{"node_id":"A","counter":1}]},"counters":{"A":1}}"#;
        let set: OrSet<String> = serde_json::from_str(json).unwrap();
        assert!(set.contains(&"a".to_string()));
        assert!(set.deferred.is_empty());
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
        let set: OrSet<String> = OrSet::new();
        assert!(set.delta_since(&frontier(0)).is_none());
    }

    #[test]
    fn delta_since_non_empty_returns_full_state() {
        let mut set = OrSet::new();
        set.add("x".to_string(), &node("A"));

        let delta = set.delta_since(&frontier(0));
        assert!(delta.is_some());
        assert!(delta.unwrap().contains(&"x".to_string()));
    }

    #[test]
    fn delta_from_no_changes_returns_none() {
        let mut set = OrSet::new();
        set.add("x".to_string(), &node("A"));
        let old = set.clone();

        assert!(set.delta_from(&old).is_none());
    }

    #[test]
    fn delta_from_detects_new_element() {
        let mut set = OrSet::new();
        set.add("x".to_string(), &node("A"));
        let old = set.clone();

        set.add("y".to_string(), &node("A"));

        let delta = set.delta_from(&old).unwrap();
        assert!(delta.contains(&"y".to_string()));
        // "x" should NOT be in the delta (its dot was already in old).
        assert!(!delta.contains(&"x".to_string()));
    }

    #[test]
    fn delta_from_detects_new_tombstone() {
        let mut set = OrSet::new();
        set.add("x".to_string(), &node("A"));
        let old = set.clone();

        set.remove(&"x".to_string());

        let delta = set.delta_from(&old).unwrap();
        assert!(!delta.deferred.is_empty());
    }

    #[test]
    fn delta_from_detects_counter_advance() {
        let mut set = OrSet::new();
        let old = set.clone();

        set.add("x".to_string(), &node("A"));

        let delta = set.delta_from(&old).unwrap();
        // Counter for node A should be included.
        assert!(delta.counters.contains_key(&node("A")));
    }

    #[test]
    fn delta_round_trip_add_produces_same_result() {
        let na = node("A");
        let nb = node("B");

        let mut set = OrSet::new();
        set.add("x".to_string(), &na);
        set.add("y".to_string(), &na);
        let old = set.clone();

        set.add("z".to_string(), &nb);

        // Full merge path.
        let mut via_full = old.clone();
        via_full.merge(&set);

        // Delta merge path.
        let delta = set.delta_from(&old).unwrap();
        let mut via_delta = old.clone();
        via_delta.merge_delta(&delta);

        assert_eq!(via_full.elements(), via_delta.elements());
    }

    #[test]
    fn delta_round_trip_remove_produces_same_result() {
        let na = node("A");

        let mut set = OrSet::new();
        set.add("x".to_string(), &na);
        set.add("y".to_string(), &na);
        let old = set.clone();

        set.remove(&"x".to_string());

        // Full merge path.
        let mut via_full = old.clone();
        via_full.merge(&set);

        // Delta merge path.
        let delta = set.delta_from(&old).unwrap();
        let mut via_delta = old.clone();
        via_delta.merge_delta(&delta);

        assert_eq!(via_full.elements(), via_delta.elements());
    }

    #[test]
    fn merge_delta_is_equivalent_to_merge() {
        let na = node("A");
        let nb = node("B");

        let mut set_a = OrSet::new();
        set_a.add("x".to_string(), &na);

        let mut set_b = OrSet::new();
        set_b.add("y".to_string(), &nb);

        let mut via_merge = set_a.clone();
        via_merge.merge(&set_b);

        let mut via_delta = set_a.clone();
        via_delta.merge_delta(&set_b);

        assert_eq!(via_merge.elements(), via_delta.elements());
    }

    // ---------------------------------------------------------------
    // Certified sweep + compaction floor (M-8)
    // ---------------------------------------------------------------

    fn digest_of(set: &OrSet<String>) -> [u8; 32] {
        use sha2::Digest as _;
        let mut hasher = sha2::Sha256::new();
        set.digest_into(&mut hasher);
        hasher.finalize().into()
    }

    /// M-8 livelock reproduction (red-first against the old code): A and
    /// B share a removed element's tombstone; A sweeps it. Under the old
    /// implementation, `merge` union-extended `other.deferred` back into
    /// A — every bucket transfer rolled the GC back and the cluster never
    /// converged under sustained digest fallback. With the floor, the
    /// stale tombstone is rejected, B self-heals in one round trip, and
    /// repeated sweep/merge cycles reach a fixed point.
    #[test]
    fn livelock_swept_tombstone_is_not_reinjected_and_cluster_converges() {
        let n = node("A");
        let mut set_a = OrSet::new();
        set_a.add("x".to_string(), &n); // dot (A,1)
        set_a.remove(&"x".to_string()); // tombstone (A,1)
        let mut set_b = set_a.clone(); // identical replicas

        // A marks (candidates = current tombstones) and certified-sweeps.
        let candidates = set_a.deferred_dots();
        let outcome = set_a.compact_deferred_certified(&candidates, None);
        assert_eq!(outcome.collected, 1);
        assert_eq!(set_a.deferred_len(), 0);
        assert_eq!(set_a.compaction_floor().get(&n), Some(&1));

        // Bucket transfer B -> A: the stale tombstone must NOT re-enter
        // (this assert fails on the pre-floor implementation).
        let fx = set_a.merge(&set_b);
        assert_eq!(
            set_a.deferred_len(),
            0,
            "merge must not reinject a swept tombstone (M-8 livelock)"
        );
        assert_eq!(fx.rejected_covered_deferred, 1);

        // Bucket transfer A -> B: B inherits the floor and drops its
        // now-absorbed tombstone (one-round-trip self-heal).
        set_b.merge(&set_a);
        assert_eq!(set_b.deferred_len(), 0);
        assert_eq!(set_b.compaction_floor().get(&n), Some(&1));
        assert_eq!(digest_of(&set_a), digest_of(&set_b), "digests must match");

        // Fixed point: further sweep/merge rounds change nothing.
        for _ in 0..3 {
            set_a.compact_deferred_certified(&set_a.deferred_dots().clone(), None);
            set_b.compact_deferred_certified(&set_b.deferred_dots().clone(), None);
            let snap_a = set_a.clone();
            set_a.merge(&set_b);
            set_b.merge(&snap_a);
            assert_eq!(set_a.deferred_len(), 0);
            assert_eq!(set_b.deferred_len(), 0);
            assert_eq!(digest_of(&set_a), digest_of(&set_b));
        }
    }

    /// Floor inheritance is pointwise max and never regresses.
    #[test]
    fn floor_merge_is_monotone_pointwise_max() {
        let na = node("A");
        let nb = node("B");
        let mut set_a = OrSet::new();
        set_a.add("x".to_string(), &na);
        set_a.remove(&"x".to_string());
        set_a.compact_deferred_certified(&set_a.deferred_dots().clone(), None);
        assert_eq!(set_a.compaction_floor().get(&na), Some(&1));

        let mut set_b = OrSet::new();
        set_b.add("y".to_string(), &nb);
        set_b.add("z".to_string(), &nb);
        set_b.remove(&"y".to_string());
        set_b.remove(&"z".to_string());
        set_b.compact_deferred_certified(&set_b.deferred_dots().clone(), None);
        assert_eq!(set_b.compaction_floor().get(&nb), Some(&2));

        set_a.merge(&set_b);
        assert_eq!(set_a.compaction_floor().get(&na), Some(&1));
        assert_eq!(set_a.compaction_floor().get(&nb), Some(&2));

        // Merging an empty-floor peer must not lower anything.
        let empty: OrSet<String> = OrSet::new();
        set_a.merge(&empty);
        assert_eq!(set_a.compaction_floor().get(&na), Some(&1));
        assert_eq!(set_a.compaction_floor().get(&nb), Some(&2));
    }

    /// Floor kill: a replica partitioned across the remove+sweep learns
    /// the remove from the floor alone (scalar-floor counterexample
    /// fixed: the in-flight stale live dot cannot resurrect either).
    #[test]
    fn floor_kills_live_dot_of_replica_that_missed_the_remove() {
        let n = node("A");
        let mut set_a = OrSet::new();
        set_a.add("x".to_string(), &n); // dot (A,1)
        let mut lagging = set_a.clone(); // holds (A,1) live

        set_a.remove(&"x".to_string());
        set_a.compact_deferred_certified(&set_a.deferred_dots().clone(), None);
        assert_eq!(set_a.deferred_len(), 0, "tombstone gone — floor only");

        // Lagging <- A: floor kills the stale live dot.
        let fx = lagging.merge(&set_a);
        assert!(!lagging.contains(&"x".to_string()), "floor must kill");
        assert_eq!(fx.killed_by_floor, 1);

        // A <- lagging (stale live dot in flight AFTER the tombstone left
        // the cluster): rejected as stale — no permanent resurrection.
        let mut stale_sender = OrSet::new();
        stale_sender.add("x".to_string(), &n); // reconstruct (A,1) live
        let fx = set_a.merge(&stale_sender);
        assert!(
            !set_a.contains(&"x".to_string()),
            "floor must reject the in-flight stale dot (C-2 residual hole)"
        );
        assert_eq!(fx.rejected_stale_live, 1);
    }

    /// A re-add after the floor advanced generates a fresh dot above the
    /// floor and survives merges in both directions.
    #[test]
    fn re_add_after_floor_survives() {
        let n = node("A");
        let mut set = OrSet::new();
        set.add("x".to_string(), &n);
        set.remove(&"x".to_string());
        set.compact_deferred_certified(&set.deferred_dots().clone(), None);

        set.add("x".to_string(), &n); // dot (A,2) > floor 1
        assert!(set.contains(&"x".to_string()));
        let clone = set.clone();
        set.merge(&clone);
        assert!(set.contains(&"x".to_string()));
    }

    /// The walk stops at a hole unless hole-jump is allowed, and stops at
    /// an unmarked tombstone; unknown-writer dots are kept.
    #[test]
    fn certified_sweep_stall_and_hole_jump_behaviour() {
        let n = node("A");
        // Legacy-hole state: counters A=2, live (A,2), neither live nor
        // deferred holds (A,1) — the shape a pre-floor sweep left behind.
        let json = r#"{"elements":{"y":[{"node_id":"A","counter":2}]},"counters":{"A":2}}"#;
        let mut set: OrSet<String> = serde_json::from_str(json).unwrap();

        let outcome = set.compact_deferred_certified(&HashSet::new(), None);
        assert_eq!(outcome.stalled_holes, 1, "hole must stall Stage 1");
        assert!(set.compaction_floor().is_empty(), "floor must not advance");

        // Stage 2: the hole is jumped (it is at or below the mark-time
        // counter ceiling), and the walk never exceeds the node's counter.
        let ceilings: HashMap<NodeId, u64> = set.counters().clone();
        let outcome = set.compact_deferred_certified(&HashSet::new(), Some(&ceilings));
        assert_eq!(outcome.stalled_holes, 0);
        assert_eq!(set.compaction_floor().get(&n), Some(&2));

        // Unmarked tombstone stalls; unknown-writer dot is kept.
        let mut set2 = OrSet::new();
        set2.add("x".to_string(), &n);
        set2.remove(&"x".to_string()); // unmarked tombstone (A,1)
        let unknown = Dot {
            node_id: node("GHOST"),
            counter: 7,
        };
        set2.deferred.insert(unknown.clone());
        let outcome = set2.compact_deferred_certified(&HashSet::new(), None);
        assert_eq!(outcome.collected, 0);
        assert_eq!(outcome.stalled_uncandidated, 1);
        assert!(set2.deferred.contains(&unknown), "unknown node dot kept");
        assert_eq!(set2.deferred_len(), 2);
    }

    /// Stage 2 must not jump a hole minted AFTER the mark: the inbound
    /// "complete pull since the mark" evidence proves nothing about a dot
    /// whose counter arrived later (e.g. counters riding a partial delta
    /// whose old-origin-timestamp entry was filtered out — the dot may be
    /// LIVE on the pushing peer). The mark-time counter ceilings bound
    /// the jump; the post-mark hole stalls the walk (fail-closed) and the
    /// floor never crosses it.
    #[test]
    fn hole_jump_never_crosses_post_mark_holes() {
        let n = node("A");
        let mut set = OrSet::new();
        set.add("x".to_string(), &n); // dot (A,1)
        set.remove(&"x".to_string()); // tombstone (A,1)

        // Mark time: candidates {(A,1)}, counters snapshot {A:1}.
        let candidates = set.deferred_dots();
        let ceilings = set.counters().clone();
        assert_eq!(ceilings.get(&n), Some(&1));

        // AFTER the mark an inbound partial delta mints holes: counters
        // jump to A=3 while neither (A,2) nor (A,3) arrives as a live or
        // deferred dot ((A,2) may be live on the pushing peer).
        let counters_only: OrSet<String> = serde_json::from_str(
            r#"{"elements":{},"counters":{"A":3},"deferred":[],"compaction_floor":{}}"#,
        )
        .unwrap();
        set.merge(&counters_only);
        assert_eq!(set.counters().get(&n), Some(&3));

        // Stage 2 sweep with the MARK-TIME ceilings: the marked tombstone
        // is collected, but the walk stalls at the post-mark hole (A,2)
        // instead of jumping to A=3.
        let outcome = set.compact_deferred_certified(&candidates, Some(&ceilings));
        assert_eq!(outcome.collected, 1);
        assert_eq!(outcome.stalled_holes, 1, "post-mark hole must stall");
        assert_eq!(
            set.compaction_floor().get(&n),
            Some(&1),
            "the floor must never cross a hole the mark did not witness"
        );
    }

    /// The dominance rule is gone: the TOP tombstone (counter == max) is
    /// collected by the certified sweep. New adds cannot collide because
    /// counters never fall below the floor (INV-CTR).
    #[test]
    fn top_tombstone_is_collected() {
        let n = node("A");
        let mut set = OrSet::new();
        set.add("x".to_string(), &n); // dot (A,1) — the highest dot
        set.remove(&"x".to_string());

        let outcome = set.compact_deferred_certified(&set.deferred_dots().clone(), None);
        assert_eq!(
            outcome.collected, 1,
            "the non-dominated top tombstone must be collected (behaviour change vs the legacy sweep)"
        );
        assert_eq!(set.compaction_floor().get(&n), Some(&1));

        // The next add is (A,2) — above the floor.
        set.add("x".to_string(), &n);
        assert!(set.contains(&"x".to_string()));
    }

    /// INV-CTR: merging a payload whose floor exceeds its counters clamps
    /// the counters up so a future local add cannot reuse a fate-decided
    /// counter value.
    #[test]
    fn counters_are_clamped_to_the_floor() {
        let n = node("A");
        let mut broken: OrSet<String> = OrSet::new();
        broken.compaction_floor.insert(n.clone(), 5);
        // counters deliberately left empty (corrupted / adversarial payload)

        let mut set: OrSet<String> = OrSet::new();
        set.merge(&broken);
        assert_eq!(set.counters.get(&n), Some(&5), "INV-CTR clamp");
        set.add("x".to_string(), &n);
        // Fresh dot must be (A,6) — above the inherited floor.
        assert!(!set.elements.values().flatten().any(|d| d.counter <= 5));
    }

    /// Fresh-remove-below-floor suite (origin-retention): after the first
    /// GC nearly all live dots sit below the floor. A later remove
    /// produces a tombstone that is covered at birth; it must be kept
    /// across merges (old peers learn the remove from it), shipped in
    /// deltas, invisible to the canonical digest, and reclaimed only by
    /// the origin's own gated sweep.
    #[test]
    fn fresh_remove_below_floor_is_origin_retained() {
        let n = node("A");
        let mut set_a = OrSet::new();
        set_a.add("keep".to_string(), &n); // (A,1)
        set_a.add("x".to_string(), &n); // (A,2)
        set_a.add("pad".to_string(), &n); // (A,3)
        set_a.remove(&"pad".to_string());
        // First GC: floor walks over live (A,1), (A,2) and tombstone (A,3).
        set_a.compact_deferred_certified(&set_a.deferred_dots().clone(), None);
        assert_eq!(set_a.compaction_floor().get(&n), Some(&3));
        let mut set_b = set_a.clone();

        // Fresh remove of a live dot BELOW the floor.
        let old = set_a.clone();
        set_a.remove(&"x".to_string()); // tombstone (A,2), covered at birth
        assert_eq!(set_a.deferred_len(), 1);
        assert_eq!(
            set_a.uncovered_deferred_len(),
            0,
            "fresh tombstone below the floor is canonical-invisible"
        );

        // (1) Inbound merges must NOT strip it before the gate.
        set_a.merge(&set_b);
        assert_eq!(
            set_a.deferred_len(),
            1,
            "origin-retention: a peer merge must not strip the pre-gate tombstone"
        );
        assert!(!set_a.contains(&"x".to_string()));

        // (2) It ships in the incremental delta and kills the dot at B.
        let delta = set_a.delta_from(&old).expect("remove must produce a delta");
        assert!(delta.compaction_floor.is_empty(), "INV-W");
        assert_eq!(delta.deferred.len(), 1);
        set_b.merge_delta(&delta);
        assert!(!set_b.contains(&"x".to_string()), "remove propagates");

        // (3) Canonical digest: A (covered own tombstone) and B (killed,
        // tombstone rejected as covered) must already match.
        assert_eq!(digest_of(&set_a), digest_of(&set_b));

        // (4) The origin's next gated sweep reclaims it.
        let outcome = set_a.compact_deferred_certified(&set_a.deferred_dots().clone(), None);
        assert_eq!(outcome.collected, 1);
        assert_eq!(set_a.deferred_len(), 0);
        assert_eq!(digest_of(&set_a), digest_of(&set_b));
    }

    /// `OrSet::delta_since` is a full clone and keeps the floor;
    /// `OrSet::delta_from` is partial and ships an empty floor; replaying
    /// a tombstone-bearing delta into its producer is a no-op.
    #[test]
    fn delta_floor_wire_invariants() {
        let n = node("A");
        let mut set = OrSet::new();
        set.add("x".to_string(), &n);
        set.remove(&"x".to_string());
        set.compact_deferred_certified(&set.deferred_dots().clone(), None);
        set.add("y".to_string(), &n);

        let since = set.delta_since(&frontier(0)).unwrap();
        assert_eq!(
            since.compaction_floor, set.compaction_floor,
            "delta_since is a full clone — floor preserved (INV-W satisfied)"
        );

        let old = set.clone();
        set.remove(&"y".to_string());
        let from = set.delta_from(&old).unwrap();
        assert!(from.compaction_floor.is_empty(), "INV-W: partial payload");

        // Delta reinjection is a no-op on the producer.
        let snapshot = set.clone();
        set.merge_delta(&from);
        assert_eq!(set.elements(), snapshot.elements());
        assert_eq!(set.deferred_len(), snapshot.deferred_len());
        assert_eq!(set.compaction_floor, snapshot.compaction_floor);
    }

    /// Old serialized format without `compaction_floor` still loads
    /// (mixed-version JSON compatibility).
    #[test]
    fn serde_backward_compat_missing_floor() {
        let json =
            r#"{"elements":{"a":[{"node_id":"A","counter":1}]},"counters":{"A":1},"deferred":[]}"#;
        let set: OrSet<String> = serde_json::from_str(json).unwrap();
        assert!(set.contains(&"a".to_string()));
        assert!(set.compaction_floor().is_empty());
    }

    /// Mixed-version simulation: a v1 node (JSON round trip with the
    /// floor stripped) merging with a floor-bearing v2 node must neither
    /// resurrect removed elements nor roll the v2 node's GC back; the v1
    /// node does not gain a floor from the stripped payload.
    #[test]
    fn mixed_version_merge_is_safe() {
        let n = node("A");
        let mut v2 = OrSet::new();
        v2.add("x".to_string(), &n);
        v2.add("keep".to_string(), &n);
        v2.remove(&"x".to_string());
        let pre_sweep = v2.clone(); // what the v1 node holds
        v2.compact_deferred_certified(&v2.deferred_dots().clone(), None);

        // Strip the floor via JSON surgery (a v1 node's view of v2 state).
        let mut json = serde_json::to_value(&v2).unwrap();
        json.as_object_mut().unwrap().remove("compaction_floor");
        let stripped: OrSet<String> = serde_json::from_value(json).unwrap();
        assert!(stripped.compaction_floor().is_empty());

        // v1 node = pre-sweep state merged with the stripped v2 state.
        let mut v1 = pre_sweep;
        v1.merge(&stripped);
        assert!(!v1.contains(&"x".to_string()), "no resurrection on v1");
        assert!(v1.compaction_floor().is_empty(), "floor not fabricated");

        // v2 <- v1 (stale tombstone): GC must not roll back.
        v2.merge(&v1);
        assert_eq!(v2.deferred_len(), 0, "v1 tombstone must not reinject");
        assert!(!v2.contains(&"x".to_string()));
        assert!(v2.contains(&"keep".to_string()));
    }

    /// Canonical digest rules: covered own tombstones are invisible;
    /// uncovered tombstone differences and floor differences are visible.
    #[test]
    fn digest_canonical_form_rules() {
        let n = node("A");
        let mut base = OrSet::new();
        base.add("keep".to_string(), &n); // (A,1)
        base.add("x".to_string(), &n); // (A,2)
        base.remove(&"x".to_string());
        base.compact_deferred_certified(&base.deferred_dots().clone(), None);

        // Covered own tombstone only difference -> digests match.
        let mut with_fresh = base.clone();
        with_fresh.remove(&"keep".to_string()); // covered tombstone (A,1)
        let mut without = base.clone();
        without.merge(&with_fresh); // learns the kill, rejects the tombstone
        assert_eq!(with_fresh.deferred_len(), 1);
        assert_eq!(without.deferred_len(), 0);
        assert_eq!(
            digest_of(&with_fresh),
            digest_of(&without),
            "covered own tombstone must be canonical-invisible"
        );

        // Uncovered tombstone difference -> digests differ.
        let mut uncovered = base.clone();
        uncovered.add("z".to_string(), &n);
        uncovered.remove(&"z".to_string()); // (A,3) above floor
        assert_ne!(digest_of(&base), digest_of(&uncovered));

        // Floor-only difference -> digests differ (v2 includes the floor).
        // `floored` is a swept remove (floor {A:1}, no tombstone);
        // `holed` is the same observable state WITHOUT the floor (the
        // legacy-hole shape) — they differ only in the floor map and the
        // digest must distinguish them so the transfer propagates it.
        let mut floored = OrSet::new();
        floored.add("x".to_string(), &n);
        floored.remove(&"x".to_string());
        floored.compact_deferred_certified(&floored.deferred_dots().clone(), None);
        let holed: OrSet<String> =
            serde_json::from_str(r#"{"elements":{},"counters":{"A":1},"deferred":[]}"#).unwrap();
        assert_eq!(floored.elements(), holed.elements());
        assert_eq!(floored.deferred_len(), holed.deferred_len());
        assert_ne!(
            digest_of(&floored),
            digest_of(&holed),
            "a floor advance is a semantic difference and must mismatch"
        );
    }

    // ---------------------------------------------------------------
    // MergeEffects::changed ground truth (M-6, RR gate)
    //
    // Contract: `changed == (a != a_before)` over the PHYSICAL state.
    // The bidirectional form holds for canonically-constructed states
    // (the production debug oracle only checks the `changed == false ⇒
    // pre == post` direction, but tests pin both).
    // ---------------------------------------------------------------

    /// Merge `b` into `a`, asserting the changed flag equals the physical
    /// state difference, and return the effects.
    fn merge_ground_truth(a: &mut OrSet<String>, b: &OrSet<String>) -> MergeEffects {
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
    fn merge_changed_identical_state_is_noop() {
        let n = node("A");
        let mut a = OrSet::new();
        a.add("x".to_string(), &n);
        a.add("y".to_string(), &n);
        a.remove(&"y".to_string());
        let b = a.clone();

        let fx = merge_ground_truth(&mut a, &b);
        assert!(!fx.changed, "identical states must merge as a no-op");
    }

    #[test]
    fn merge_changed_subset_is_noop() {
        let n = node("A");
        let mut b = OrSet::new();
        b.add("x".to_string(), &n);
        let mut a = b.clone();
        a.add("y".to_string(), &n); // a ⊋ b

        let fx = merge_ground_truth(&mut a, &b);
        assert!(!fx.changed, "merging a dominated subset must be a no-op");
    }

    #[test]
    fn merge_changed_new_dot_and_tombstone() {
        let n = node("A");
        let mut a = OrSet::new();
        a.add("x".to_string(), &n);
        let mut b = a.clone();

        // New dot on b.
        b.add("x".to_string(), &n);
        let fx = merge_ground_truth(&mut a, &b);
        assert!(fx.changed, "a new dot must report changed");

        // Tombstone-only difference.
        let mut b2 = a.clone();
        b2.remove(&"x".to_string());
        let fx = merge_ground_truth(&mut a, &b2);
        assert!(fx.changed, "a new tombstone must report changed");

        // And the echo back is a no-op again.
        let snapshot = a.clone();
        let fx = merge_ground_truth(&mut a, &snapshot);
        assert!(!fx.changed, "self-echo must be a no-op");
    }

    #[test]
    fn merge_changed_counter_only_advance() {
        let n = node("A");
        let mut a = OrSet::new();
        a.add("x".to_string(), &n); // counters {A:1}
        let b: OrSet<String> = serde_json::from_str(
            r#"{"elements":{},"counters":{"A":3},"deferred":[],"compaction_floor":{}}"#,
        )
        .unwrap();

        let fx = merge_ground_truth(&mut a, &b);
        assert!(fx.changed, "a counter-only advance must report changed");
        // The advance must keep propagating: a second merge is a no-op.
        let fx = merge_ground_truth(&mut a, &b);
        assert!(!fx.changed);
    }

    #[test]
    fn merge_changed_floor_only_advance_and_floor_kill() {
        let n = node("A");
        let mut swept = OrSet::new();
        swept.add("x".to_string(), &n);
        swept.remove(&"x".to_string());
        swept.compact_deferred_certified(&swept.deferred_dots().clone(), None);
        assert_eq!(swept.compaction_floor().get(&n), Some(&1));

        // Floor kill: the lagging replica still holds (A,1) live.
        let mut lagging = OrSet::new();
        lagging.add("x".to_string(), &n);
        let fx = merge_ground_truth(&mut lagging, &swept);
        assert!(fx.changed, "a floor kill must report changed");
        assert_eq!(fx.killed_by_floor, 1);

        // Floor-only advance: receiver has neither the element nor the
        // floor (counters get clamped up too — both are real changes).
        let mut empty: OrSet<String> = OrSet::new();
        let fx = merge_ground_truth(&mut empty, &swept);
        assert!(fx.changed, "a floor-only advance must report changed");
    }

    #[test]
    fn merge_changed_stale_reoffers_are_noops() {
        let n = node("A");
        let mut a = OrSet::new();
        a.add("x".to_string(), &n);
        a.remove(&"x".to_string());
        let pre_sweep = a.clone(); // holds tombstone (A,1)
        a.compact_deferred_certified(&a.deferred_dots().clone(), None);

        // (viii) stale live re-offer: rejected, no state change.
        let mut stale_live = OrSet::new();
        stale_live.add("x".to_string(), &n); // reconstructs (A,1) live
        let fx = merge_ground_truth(&mut a, &stale_live);
        assert_eq!(fx.rejected_stale_live, 1);
        assert!(
            !fx.changed,
            "a rejected stale live dot must NOT report changed (would re-dirty forever)"
        );

        // (ix) covered deferred re-offer: rejected, no state change.
        let fx = merge_ground_truth(&mut a, &pre_sweep);
        assert_eq!(fx.rejected_covered_deferred, 1);
        assert!(
            !fx.changed,
            "a rejected covered tombstone must NOT report changed"
        );
    }

    #[test]
    fn merge_changed_zero_counter_creates_no_ghost_entry() {
        let na = node("A");
        let nb = node("B");
        let mut a = OrSet::new();
        a.add("x".to_string(), &na);
        // Adversarial/legacy payload: an explicit zero counter for a node
        // we have never seen.
        let b: OrSet<String> = serde_json::from_str(
            r#"{"elements":{},"counters":{"B":0},"deferred":[],"compaction_floor":{}}"#,
        )
        .unwrap();

        let fx = merge_ground_truth(&mut a, &b);
        assert!(!fx.changed, "a zero counter carries no information");
        assert!(
            !a.counters.contains_key(&nb),
            "no ghost zero entry may be materialised (it would break the no-op oracle)"
        );
    }
}
