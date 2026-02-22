use std::collections::HashMap;

use serde::{Deserialize, Serialize};

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

    /// Increment the counter for the given node.
    pub fn increment(&mut self, node_id: &NodeId) {
        *self.p.entry(node_id.clone()).or_insert(0) += 1;
    }

    /// Decrement the counter for the given node.
    pub fn decrement(&mut self, node_id: &NodeId) {
        *self.n.entry(node_id.clone()).or_insert(0) += 1;
    }

    /// Return the current counter value: `sum(P) - sum(N)`.
    pub fn value(&self) -> i64 {
        let pos: u64 = self.p.values().sum();
        let neg: u64 = self.n.values().sum();
        pos as i64 - neg as i64
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
}
