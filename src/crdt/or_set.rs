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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrSet<T: Eq + Hash> {
    /// Maps each element to the set of dots that justify its presence.
    elements: HashMap<T, HashSet<Dot>>,
    /// Per-node monotonic counters used to generate fresh dots.
    counters: HashMap<NodeId, u64>,
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

    /// Removes an element by discarding all of its currently observed dots.
    ///
    /// If the element is not present this is a no-op.
    pub fn remove(&mut self, element: &T) {
        self.elements.remove(element);
    }

    /// Returns `true` if the set currently contains the element.
    pub fn contains(&self, element: &T) -> bool {
        self.elements
            .get(element)
            .is_some_and(|dots| !dots.is_empty())
    }

    /// Returns a `HashSet` of references to all elements currently in the set.
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
    /// For each element the resulting dot-set is the union of both sides.
    /// This gives add-wins semantics: if node A adds an element while node B
    /// concurrently removes it, the add's fresh dot survives the merge.
    ///
    /// Node counters are also merged by taking the maximum.
    pub fn merge(&mut self, other: &OrSet<T>) {
        for (elem, other_dots) in &other.elements {
            let dots = self.elements.entry(elem.clone()).or_default();
            for dot in other_dots {
                dots.insert(dot.clone());
            }
        }

        for (node_id, &other_counter) in &other.counters {
            let counter = self.counters.entry(node_id.clone()).or_insert(0);
            if other_counter > *counter {
                *counter = other_counter;
            }
        }
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
        set.remove(&"ghost".to_string());
        assert!(set.is_empty());
    }

    #[test]
    fn add_duplicate_element() {
        let mut set = OrSet::new();
        let n = node("A");
        set.add("x".to_string(), &n);
        set.add("x".to_string(), &n);
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
}
