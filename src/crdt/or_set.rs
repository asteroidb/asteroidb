//! Observed-Remove Set (OR-Set) CRDT with add-wins semantics.
//!
//! Each add operation is tagged with a unique dot (node_id, counter) pair.
//! Remove only deletes the dots currently observed, so a concurrent add
//! on another node will survive after merge — giving "add-wins" behaviour.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrSet<T: Eq + Hash> {
    /// Maps each element to the set of dots that justify its presence.
    elements: HashMap<T, HashSet<Dot>>,
    /// Per-node monotonic counters used to generate fresh dots.
    counters: HashMap<NodeId, u64>,
    /// Causal context / tombstone set: all dots that have ever been removed.
    /// Needed so merge can distinguish "this dot was removed" from "never seen".
    #[serde(default)]
    deferred: HashSet<Dot>,
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

    /// Merges another OR-Set into this one.
    ///
    /// For each element:
    /// - Dots from the other side are added only if NOT in our deferred set.
    /// - Dots on our side are removed if they ARE in the other's deferred set.
    ///
    /// This gives correct observed-remove semantics: a remove on one
    /// replica propagates via its deferred set, while a concurrent add
    /// (with a fresh dot not in anyone's deferred set) survives — giving
    /// add-wins behaviour.
    ///
    /// Node counters are merged by taking the maximum.
    /// Deferred (tombstone) sets are merged as a union.
    pub fn merge(&mut self, other: &OrSet<T>) {
        // Process elements present in the other replica.
        for (elem, other_dots) in &other.elements {
            let dots = self.elements.entry(elem.clone()).or_default();

            // Add dots from other that we haven't tombstoned.
            dots.extend(
                other_dots
                    .iter()
                    .filter(|dot| !self.deferred.contains(dot))
                    .cloned(),
            );

            // Remove our dots that the other has tombstoned.
            dots.retain(|dot| !other.deferred.contains(dot));
        }

        // Apply other's tombstones to self-only elements (not in other.elements).
        for (elem, dots) in &mut self.elements {
            if !other.elements.contains_key(elem) {
                dots.retain(|dot| !other.deferred.contains(dot));
            }
        }

        // Remove entries with no remaining dots.
        self.elements.retain(|_, dots| !dots.is_empty());

        // Merge counters so future dots stay globally unique.
        for (node_id, &other_counter) in &other.counters {
            let counter = self.counters.entry(node_id.clone()).or_insert(0);
            *counter = (*counter).max(other_counter);
        }

        // Merge deferred (tombstone) sets.
        self.deferred.extend(other.deferred.iter().cloned());
    }

    /// Return the number of dots currently in the tombstone (deferred) set.
    ///
    /// Useful for monitoring GC effectiveness.
    pub fn deferred_len(&self) -> usize {
        self.deferred.len()
    }

    /// Remove tombstone dots from `deferred` that are already absent from
    /// all element dot sets AND whose counter is dominated by the known
    /// counter for that node.
    ///
    /// Call this periodically (e.g., after a full sync round completes) to
    /// bound the growth of the deferred set. A dot `(node_id, counter)` is
    /// safe to remove when no element references it AND `counter` is below
    /// the maximum counter we track for that node — meaning any future dot
    /// for that node will have a strictly higher counter and cannot collide.
    ///
    /// **Do not** call this in the middle of a partial sync round; wait
    /// until all replicas have exchanged state to avoid prematurely
    /// discarding tombstones that a not-yet-merged replica still needs.
    pub fn compact_deferred(&mut self) {
        let live_dots: HashSet<&Dot> = self
            .elements
            .values()
            .flat_map(|dots| dots.iter())
            .collect();
        self.deferred.retain(|d| {
            if live_dots.contains(d) {
                return true;
            }
            // Only remove if counter is dominated by the known max for this node.
            match self.counters.get(&d.node_id) {
                Some(&max_counter) => d.counter >= max_counter,
                None => true, // unknown node — keep to be safe
            }
        });
    }

    /// Remove tombstone dots from `deferred` that satisfy **both** the local
    /// counter check and a cross-replica version floor check.
    ///
    /// A dot `(node_id, counter)` is removed when:
    /// 1. It is not referenced by any live element, AND
    /// 2. `counter < max_counter` for that node (local dominance), AND
    /// 3. `counter < floor` where `floor` is the per-node floor from
    ///    `version_floor`, falling back to `global_floor` if no per-node
    ///    entry exists.
    pub fn compact_deferred_with_floor(
        &mut self,
        version_floor: &std::collections::HashMap<NodeId, u64>,
        global_floor: Option<u64>,
    ) {
        let live_dots: HashSet<&Dot> = self
            .elements
            .values()
            .flat_map(|dots| dots.iter())
            .collect();
        self.deferred.retain(|d| {
            if live_dots.contains(d) {
                return true;
            }
            let locally_dominated = match self.counters.get(&d.node_id) {
                Some(&max_counter) => d.counter < max_counter,
                None => false,
            };
            if !locally_dominated {
                return true;
            }
            let effective_floor = version_floor.get(&d.node_id).copied().or(global_floor);
            match effective_floor {
                Some(floor) => d.counter >= floor,
                None => true,
            }
        });
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
}
