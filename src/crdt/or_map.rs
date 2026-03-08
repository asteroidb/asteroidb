use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use serde::de::DeserializeOwned;
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    K: Eq + Hash + Clone + Serialize + DeserializeOwned,
    V: Clone + Serialize + DeserializeOwned,
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
            entry.0.extend(
                other_dots
                    .iter()
                    .filter(|dot| !self.deferred.contains(dot))
                    .cloned(),
            );

            // Remove our dots that the other has tombstoned.
            entry.0.retain(|dot| !other.deferred.contains(dot));

            // Merge LWW value.
            entry.1.merge(other_reg);
        }

        // Apply other's tombstones to self-only entries (keys not in other.entries).
        for (key, (dots, _)) in &mut self.entries {
            if !other.entries.contains_key(key) {
                dots.retain(|dot| !other.deferred.contains(dot));
            }
        }

        // Remove entries with no remaining dots.
        self.entries.retain(|_, (dots, _)| !dots.is_empty());

        // Merge counters (take max).
        for (node_id, &counter) in &other.counters {
            let our_counter = self.counters.entry(node_id.clone()).or_insert(0);
            *our_counter = (*our_counter).max(counter);
        }

        // Merge deferred sets.
        self.deferred.extend(other.deferred.iter().cloned());
    }

    /// Merge a delta into this map.
    ///
    /// For OrMap, `merge_delta` is identical to `merge` because the delta
    /// is the same type (a subset of entries and deferred dots).
    pub fn merge_delta(&mut self, delta: &OrMap<K, V>) {
        self.merge(delta);
    }

    /// Extract changes since the given frontier timestamp.
    ///
    /// OrMap entries carry LWW-Register timestamps, so this method returns
    /// only entries whose register timestamp is strictly greater than
    /// `frontier`, along with any tombstones. Returns `None` when there
    /// are no entries or tombstones newer than the frontier.
    pub fn delta_since(&self, frontier: &HlcTimestamp) -> Option<Self> {
        let mut delta = OrMap {
            entries: HashMap::new(),
            counters: self.counters.clone(),
            deferred: self.deferred.clone(),
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
    /// Returns `None` if there are no changes.
    pub fn delta_from(&self, old: &OrMap<K, V>) -> Option<Self>
    where
        V: PartialEq,
    {
        let mut delta = OrMap {
            entries: HashMap::new(),
            counters: HashMap::new(),
            deferred: HashSet::new(),
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

    /// Remove tombstone dots from `deferred` that are already absent from
    /// all entry dot sets AND whose counter is dominated by the known
    /// counter for that node.
    ///
    /// Call this periodically (e.g., after a full sync round completes) to
    /// bound the growth of the deferred set. A dot `(node_id, counter)` is
    /// safe to remove when no entry references it AND `counter` is below
    /// the maximum counter we track for that node — meaning any future dot
    /// for that node will have a strictly higher counter and cannot collide.
    ///
    /// **Do not** call this in the middle of a partial sync round; wait
    /// until all replicas have exchanged state to avoid prematurely
    /// discarding tombstones that a not-yet-merged replica still needs.
    pub fn compact_deferred(&mut self) {
        let live_dots: HashSet<&Dot> = self
            .entries
            .values()
            .flat_map(|(dots, _)| dots.iter())
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
    /// See [`OrSet::compact_deferred_with_floor`] for detailed semantics.
    pub fn compact_deferred_with_floor(
        &mut self,
        version_floor: &std::collections::HashMap<crate::types::NodeId, u64>,
        global_floor: Option<u64>,
    ) {
        let live_dots: HashSet<&Dot> = self
            .entries
            .values()
            .flat_map(|(dots, _)| dots.iter())
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
    K: Eq + Hash + Clone + Serialize + DeserializeOwned,
    V: Clone + Serialize + DeserializeOwned,
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
}
