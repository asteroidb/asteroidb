use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use serde::{Deserialize, Serialize};

use crate::crdt::lww_register::LwwRegister;
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
#[derive(Debug, Clone)]
pub struct OrMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    /// Active entries: key -> (dots that justify presence, LWW value).
    entries: HashMap<K, (HashSet<Dot>, LwwRegister<V>)>,
    /// Per-node monotonic counters for generating unique dots.
    counters: HashMap<NodeId, u64>,
    /// Tombstone: all dots that have ever been removed.
    /// Needed so merge can tell "this dot was deleted" vs "never seen".
    deferred: HashSet<Dot>,
}

impl<K, V> OrMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    /// Create an empty OR-Map.
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            counters: HashMap::new(),
            deferred: HashSet::new(),
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
    pub fn set(&mut self, key: K, value: V, timestamp: HlcTimestamp, node_id: &NodeId) {
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

        // Add the new dot.
        entry.0.insert(dot);
        entry.1.set(value, timestamp);
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

    /// Merge another OR-Map into this one.
    ///
    /// For each key:
    /// - Dots present in the other but not in our deferred set are added.
    /// - Dots present in ours but in the other's deferred set are removed.
    /// - LWW-Register values are merged by timestamp.
    ///
    /// This ensures add-wins semantics: if node A deletes a key while node B
    /// concurrently sets it, the set wins because B's dot is not in A's
    /// deferred set.
    pub fn merge(&mut self, other: &OrMap<K, V>) {
        for (key, (other_dots, other_reg)) in &other.entries {
            let entry = self.entries.entry(key.clone()).or_insert_with(|| {
                let reg = LwwRegister::new();
                (HashSet::new(), reg)
            });

            // Add dots from other that we haven't tombstoned.
            for dot in other_dots {
                if !self.deferred.contains(dot) {
                    entry.0.insert(dot.clone());
                }
            }

            // Remove our dots that the other has tombstoned.
            entry.0.retain(|dot| !other.deferred.contains(dot));

            // Merge LWW value.
            entry.1.merge(other_reg);
        }

        // Remove entries with no remaining dots.
        self.entries.retain(|_, (dots, _)| !dots.is_empty());

        // Merge counters (take max).
        for (node_id, &counter) in &other.counters {
            let our_counter = self.counters.entry(node_id.clone()).or_insert(0);
            *our_counter = (*our_counter).max(counter);
        }

        // Merge deferred sets.
        for dot in &other.deferred {
            self.deferred.insert(dot.clone());
        }
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

impl<K, V> Default for OrMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
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
}
