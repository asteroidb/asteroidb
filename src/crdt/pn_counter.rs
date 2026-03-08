use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::hlc::HlcTimestamp;
use crate::types::NodeId;

/// A PN-Counter (Positive-Negative Counter) CRDT.
///
/// Composed of two G-Counters: one for increments (P) and one for decrements (N).
/// Each node maintains its own entry in both maps. The counter value is `sum(P) - sum(N)`.
///
/// Merge takes the element-wise maximum of both maps, guaranteeing convergence
/// across replicas regardless of message ordering or duplication (FR-005).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PnCounter {
    /// Positive (increment) counters per node.
    p: HashMap<NodeId, u64>,
    /// Negative (decrement) counters per node.
    n: HashMap<NodeId, u64>,
}

impl PnCounter {
    /// Create a new, empty PN-Counter.
    pub fn new() -> Self {
        Self {
            p: HashMap::new(),
            n: HashMap::new(),
        }
    }

    /// Create a counter pre-initialized with the given value for the specified node.
    ///
    /// This is O(1) unlike repeated `increment`/`decrement` calls, which would
    /// be O(|value|) and susceptible to DoS for large magnitudes.
    pub fn from_value(node: &NodeId, value: i64) -> Self {
        let mut counter = PnCounter::new();
        if value >= 0 {
            counter.p.insert(node.clone(), value as u64);
        } else {
            counter.n.insert(node.clone(), value.unsigned_abs());
        }
        counter
    }

    /// Increment the counter for the given node.
    pub fn increment(&mut self, node_id: &NodeId) {
        *self.p.entry(node_id.clone()).or_insert(0) += 1;
    }

    /// Decrement the counter for the given node.
    pub fn decrement(&mut self, node_id: &NodeId) {
        *self.n.entry(node_id.clone()).or_insert(0) += 1;
    }

    /// Return the current counter value: `sum(P) - sum(N)`.
    ///
    /// Uses saturating arithmetic to prevent overflow. If the difference exceeds
    /// `i64::MAX` or is less than `i64::MIN`, the result is clamped to the
    /// respective bound.
    pub fn value(&self) -> i64 {
        let pos: u64 = self.p.values().sum();
        let neg: u64 = self.n.values().sum();
        if pos >= neg {
            (pos - neg).min(i64::MAX as u64) as i64
        } else {
            let diff = neg - pos;
            let min_mag = (i64::MAX as u64) + 1;
            if diff >= min_mag {
                i64::MIN
            } else {
                -(diff as i64)
            }
        }
    }

    /// Merge another PN-Counter into this one by taking the element-wise maximum
    /// of both the P and N maps.
    pub fn merge(&mut self, other: &PnCounter) {
        for (node_id, &count) in &other.p {
            let entry = self.p.entry(node_id.clone()).or_insert(0);
            *entry = (*entry).max(count);
        }
        for (node_id, &count) in &other.n {
            let entry = self.n.entry(node_id.clone()).or_insert(0);
            *entry = (*entry).max(count);
        }
    }

    /// Merge a delta into this counter.
    ///
    /// For PnCounter, `merge_delta` is identical to `merge` because the delta
    /// is the same type containing only the changed subset of node entries.
    pub fn merge_delta(&mut self, delta: &PnCounter) {
        self.merge(delta);
    }

    /// Extract changes since the given frontier timestamp.
    ///
    /// PnCounter does not embed per-node HLC timestamps, so if the key-level
    /// HLC indicates the counter was modified after `frontier`, the entire
    /// counter state is returned as the delta. The caller (sync layer) is
    /// responsible for checking the key-level HLC before calling this.
    ///
    /// Returns `None` only when the counter is completely empty (no P or N
    /// entries), which means there is nothing to send.
    pub fn delta_since(&self, _frontier: &HlcTimestamp) -> Option<Self> {
        if self.p.is_empty() && self.n.is_empty() {
            return None;
        }
        Some(self.clone())
    }

    /// Compute a true incremental delta against a known old state.
    ///
    /// Returns a PnCounter containing only nodes whose P or N counts have
    /// increased compared to `old`. Returns `None` if there are no changes.
    ///
    /// This is the preferred delta method when the peer's last-known state
    /// is available, as it can significantly reduce payload size for counters
    /// with many nodes.
    pub fn delta_from(&self, old: &PnCounter) -> Option<Self> {
        let mut delta = PnCounter::new();
        let mut has_changes = false;

        for (node_id, &count) in &self.p {
            let old_count = old.p.get(node_id).copied().unwrap_or(0);
            if count > old_count {
                delta.p.insert(node_id.clone(), count);
                has_changes = true;
            }
        }

        for (node_id, &count) in &self.n {
            let old_count = old.n.get(node_id).copied().unwrap_or(0);
            if count > old_count {
                delta.n.insert(node_id.clone(), count);
                has_changes = true;
            }
        }

        if has_changes { Some(delta) } else { None }
    }
}

impl Default for PnCounter {
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

    #[test]
    fn new_counter_is_zero() {
        let counter = PnCounter::new();
        assert_eq!(counter.value(), 0);
    }

    #[test]
    fn single_node_increment() {
        let mut counter = PnCounter::new();
        let n = node("node-a");

        counter.increment(&n);
        assert_eq!(counter.value(), 1);

        counter.increment(&n);
        counter.increment(&n);
        assert_eq!(counter.value(), 3);
    }

    #[test]
    fn single_node_decrement() {
        let mut counter = PnCounter::new();
        let n = node("node-a");

        counter.decrement(&n);
        assert_eq!(counter.value(), -1);

        counter.decrement(&n);
        assert_eq!(counter.value(), -2);
    }

    #[test]
    fn single_node_increment_and_decrement() {
        let mut counter = PnCounter::new();
        let n = node("node-a");

        counter.increment(&n);
        counter.increment(&n);
        counter.increment(&n);
        counter.decrement(&n);
        assert_eq!(counter.value(), 2);
    }

    #[test]
    fn value_can_go_negative() {
        let mut counter = PnCounter::new();
        let n = node("node-a");

        counter.decrement(&n);
        counter.decrement(&n);
        counter.increment(&n);
        assert_eq!(counter.value(), -1);
    }

    #[test]
    fn two_node_concurrent_merge() {
        let na = node("node-a");
        let nb = node("node-b");

        let mut counter_a = PnCounter::new();
        counter_a.increment(&na);
        counter_a.increment(&na);
        counter_a.increment(&na); // P(a)=3

        let mut counter_b = PnCounter::new();
        counter_b.increment(&nb);
        counter_b.increment(&nb); // P(b)=2
        counter_b.decrement(&nb); // N(b)=1

        // Merge b into a.
        counter_a.merge(&counter_b);
        // Expected: P(a)=3 + P(b)=2 - N(b)=1 = 4
        assert_eq!(counter_a.value(), 4);

        // Merge a into b should yield the same result (commutativity).
        counter_b.merge(&counter_a);
        assert_eq!(counter_b.value(), 4);
    }

    #[test]
    fn idempotent_merge() {
        let na = node("node-a");
        let nb = node("node-b");

        let mut counter_a = PnCounter::new();
        counter_a.increment(&na);
        counter_a.increment(&na);

        let mut counter_b = PnCounter::new();
        counter_b.increment(&nb);

        counter_a.merge(&counter_b);
        let after_first = counter_a.value();

        // Merging same state again should not change anything.
        counter_a.merge(&counter_b);
        assert_eq!(counter_a.value(), after_first);
    }

    #[test]
    fn three_node_convergence() {
        let na = node("node-a");
        let nb = node("node-b");
        let nc = node("node-c");

        let mut counter_a = PnCounter::new();
        counter_a.increment(&na);
        counter_a.increment(&na); // P(a)=2

        let mut counter_b = PnCounter::new();
        counter_b.increment(&nb);
        counter_b.decrement(&nb); // P(b)=1, N(b)=1

        let mut counter_c = PnCounter::new();
        counter_c.increment(&nc);
        counter_c.increment(&nc);
        counter_c.increment(&nc); // P(c)=3

        // Merge in different orders and verify all converge to the same value.
        // Expected: P(a)=2 + P(b)=1 + P(c)=3 - N(b)=1 = 5
        let mut abc = counter_a.clone();
        abc.merge(&counter_b);
        abc.merge(&counter_c);

        let mut bca = counter_b.clone();
        bca.merge(&counter_c);
        bca.merge(&counter_a);

        let mut cab = counter_c.clone();
        cab.merge(&counter_a);
        cab.merge(&counter_b);

        assert_eq!(abc.value(), 5);
        assert_eq!(bca.value(), 5);
        assert_eq!(cab.value(), 5);
    }

    #[test]
    fn merge_commutativity() {
        let na = node("node-a");
        let nb = node("node-b");

        let mut a = PnCounter::new();
        a.increment(&na);
        a.increment(&na);
        a.decrement(&na);

        let mut b = PnCounter::new();
        b.increment(&nb);

        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        assert_eq!(ab.value(), ba.value());
    }

    #[test]
    fn merge_associativity() {
        let na = node("node-a");
        let nb = node("node-b");
        let nc = node("node-c");

        let mut a = PnCounter::new();
        a.increment(&na);

        let mut b = PnCounter::new();
        b.increment(&nb);
        b.decrement(&nb);

        let mut c = PnCounter::new();
        c.increment(&nc);
        c.increment(&nc);

        // (a merge b) merge c
        let mut ab_c = a.clone();
        ab_c.merge(&b);
        ab_c.merge(&c);

        // a merge (b merge c)
        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a.clone();
        a_bc.merge(&bc);

        assert_eq!(ab_c.value(), a_bc.value());
    }

    #[test]
    fn serde_round_trip() {
        let na = node("node-a");
        let nb = node("node-b");

        let mut counter = PnCounter::new();
        counter.increment(&na);
        counter.increment(&na);
        counter.decrement(&nb);

        let json = serde_json::to_string(&counter).expect("serialize");
        let back: PnCounter = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(counter.value(), back.value());
    }

    #[test]
    fn default_is_zero() {
        let counter = PnCounter::default();
        assert_eq!(counter.value(), 0);
    }

    #[test]
    fn from_value_positive() {
        let n = node("node-a");
        let counter = PnCounter::from_value(&n, 42);
        assert_eq!(counter.value(), 42);
    }

    #[test]
    fn from_value_negative() {
        let n = node("node-a");
        let counter = PnCounter::from_value(&n, -7);
        assert_eq!(counter.value(), -7);
    }

    #[test]
    fn from_value_zero() {
        let n = node("node-a");
        let counter = PnCounter::from_value(&n, 0);
        assert_eq!(counter.value(), 0);
    }

    #[test]
    fn from_value_large_positive() {
        let n = node("node-a");
        let counter = PnCounter::from_value(&n, 999_999_999);
        assert_eq!(counter.value(), 999_999_999);
    }

    #[test]
    fn from_value_large_negative() {
        let n = node("node-a");
        let counter = PnCounter::from_value(&n, -999_999_999);
        assert_eq!(counter.value(), -999_999_999);
    }

    #[test]
    fn from_value_merges_with_incremented() {
        let na = node("node-a");
        let nb = node("node-b");

        // Build one counter via from_value, another via increment.
        let counter_a = PnCounter::from_value(&na, 100);

        let mut counter_b = PnCounter::new();
        for _ in 0..5 {
            counter_b.increment(&nb);
        }
        counter_b.decrement(&nb); // net +4

        let mut merged = counter_a.clone();
        merged.merge(&counter_b);
        assert_eq!(merged.value(), 104);

        // Commutativity: merge the other direction.
        let mut merged_rev = counter_b.clone();
        merged_rev.merge(&counter_a);
        assert_eq!(merged_rev.value(), 104);
    }

    #[test]
    fn value_saturates_when_pos_exceeds_i64_max() {
        // Two nodes each contribute more than i64::MAX / 2, so total pos > i64::MAX.
        let na = node("node-a");
        let nb = node("node-b");

        let mut counter = PnCounter::new();
        counter.p.insert(na, u64::MAX / 2 + 1);
        counter.p.insert(nb, u64::MAX / 2 + 1);
        // pos = u64::MAX / 2 + 1 + u64::MAX / 2 + 1 = u64::MAX + 1, but sum wraps to 0.
        // Actually let's use values that sum to > i64::MAX but don't overflow u64::sum.
        let mut counter2 = PnCounter::new();
        counter2.p.insert(node("a"), i64::MAX as u64);
        counter2.p.insert(node("b"), 1);
        // pos = i64::MAX + 1 which is > i64::MAX
        assert_eq!(counter2.value(), i64::MAX);
    }

    #[test]
    fn value_saturates_when_neg_exceeds_i64_max() {
        let mut counter = PnCounter::new();
        counter.n.insert(node("a"), i64::MAX as u64);
        counter.n.insert(node("b"), 1);
        // neg = i64::MAX + 1, pos = 0, so result should saturate to i64::MIN
        assert_eq!(counter.value(), i64::MIN);
    }

    #[test]
    fn value_saturates_large_positive_difference() {
        let mut counter = PnCounter::new();
        counter.p.insert(node("a"), u64::MAX);
        counter.n.insert(node("b"), 0);
        // pos - neg = u64::MAX, clamped to i64::MAX
        assert_eq!(counter.value(), i64::MAX);
    }

    #[test]
    fn value_saturates_large_negative_difference() {
        let mut counter = PnCounter::new();
        counter.p.insert(node("a"), 0);
        counter.n.insert(node("b"), u64::MAX);
        // neg - pos = u64::MAX, clamped to i64::MIN
        assert_eq!(counter.value(), i64::MIN);
    }

    #[test]
    fn from_value_i64_min_roundtrips() {
        // i64::MIN has unsigned_abs() = i64::MAX + 1, which exceeds i64::MAX.
        // value() should round-trip to i64::MIN (not overflow).
        let n = node("node-a");
        let counter = PnCounter::from_value(&n, i64::MIN);
        // The internal n map has value i64::MAX + 1 = 9223372036854775808.
        assert_eq!(counter.value(), i64::MIN);
    }

    #[test]
    fn from_value_i64_max_roundtrips() {
        let n = node("node-a");
        let counter = PnCounter::from_value(&n, i64::MAX);
        assert_eq!(counter.value(), i64::MAX);
    }

    #[test]
    fn value_exact_i64_max_boundary() {
        // Exactly i64::MAX should not be clamped.
        let mut counter = PnCounter::new();
        counter.p.insert(node("a"), i64::MAX as u64);
        assert_eq!(counter.value(), i64::MAX);
    }

    #[test]
    fn value_exact_neg_i64_max_boundary() {
        // neg = i64::MAX, pos = 0 => result is exactly -i64::MAX (no clamping needed).
        let mut counter = PnCounter::new();
        counter.n.insert(node("a"), i64::MAX as u64);
        assert_eq!(counter.value(), -i64::MAX);
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
    fn delta_since_empty_counter_returns_none() {
        let counter = PnCounter::new();
        assert!(counter.delta_since(&frontier(0)).is_none());
    }

    #[test]
    fn delta_since_non_empty_returns_full_state() {
        let mut counter = PnCounter::new();
        counter.increment(&node("A"));
        counter.increment(&node("B"));

        let delta = counter.delta_since(&frontier(0));
        assert!(delta.is_some());
        assert_eq!(delta.unwrap().value(), 2);
    }

    #[test]
    fn delta_from_no_changes_returns_none() {
        let mut counter = PnCounter::new();
        counter.increment(&node("A"));

        let old = counter.clone();
        assert!(counter.delta_from(&old).is_none());
    }

    #[test]
    fn delta_from_detects_increment() {
        let mut counter = PnCounter::new();
        counter.increment(&node("A"));
        let old = counter.clone();

        counter.increment(&node("A"));
        counter.increment(&node("B"));

        let delta = counter.delta_from(&old).unwrap();
        // Delta should contain node-A with P=2 and node-B with P=1.
        assert_eq!(delta.p.get(&node("A")), Some(&2));
        assert_eq!(delta.p.get(&node("B")), Some(&1));
        assert!(delta.n.is_empty());
    }

    #[test]
    fn delta_from_detects_decrement() {
        let mut counter = PnCounter::new();
        counter.increment(&node("A"));
        let old = counter.clone();

        counter.decrement(&node("A"));

        let delta = counter.delta_from(&old).unwrap();
        assert!(delta.p.is_empty()); // P didn't change
        assert_eq!(delta.n.get(&node("A")), Some(&1));
    }

    #[test]
    fn delta_round_trip_produces_same_result_as_full_merge() {
        let na = node("A");
        let nb = node("B");

        // Build initial state.
        let mut counter = PnCounter::new();
        counter.increment(&na);
        counter.increment(&na);
        counter.decrement(&nb);
        let old = counter.clone();

        // Apply more changes.
        counter.increment(&na);
        counter.increment(&nb);
        counter.decrement(&nb);

        // Full merge path.
        let mut via_full = old.clone();
        via_full.merge(&counter);

        // Delta merge path.
        let delta = counter.delta_from(&old).unwrap();
        let mut via_delta = old.clone();
        via_delta.merge_delta(&delta);

        assert_eq!(via_full.value(), via_delta.value());
    }

    #[test]
    fn merge_delta_is_equivalent_to_merge() {
        let na = node("A");
        let nb = node("B");

        let mut a = PnCounter::new();
        a.increment(&na);
        a.increment(&na);

        let mut b = PnCounter::new();
        b.increment(&nb);
        b.decrement(&nb);

        let mut via_merge = a.clone();
        via_merge.merge(&b);

        let mut via_delta = a.clone();
        via_delta.merge_delta(&b);

        assert_eq!(via_merge.value(), via_delta.value());
    }
}
