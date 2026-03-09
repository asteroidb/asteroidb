use serde::{Deserialize, Serialize};

use crate::hlc::HlcTimestamp;

/// Last-Writer-Wins Register (FR-005).
///
/// Concurrent writes are resolved by timestamp ordering.
/// The write with the highest `HlcTimestamp` wins.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LwwRegister<T> {
    value: Option<T>,
    timestamp: HlcTimestamp,
}

impl<T: Clone> LwwRegister<T> {
    /// Create an empty register with a zero timestamp.
    pub fn new() -> Self {
        Self {
            value: None,
            timestamp: HlcTimestamp {
                physical: 0,
                logical: 0,
                node_id: String::new(),
            },
        }
    }

    /// Set the value if the given timestamp is strictly greater than the current one.
    ///
    /// Returns `true` if the value was updated, `false` if the timestamp was stale.
    pub fn set(&mut self, value: T, timestamp: HlcTimestamp) -> bool {
        if timestamp > self.timestamp {
            self.value = Some(value);
            self.timestamp = timestamp;
            true
        } else {
            false
        }
    }

    /// Get a reference to the current value, if any.
    pub fn get(&self) -> Option<&T> {
        self.value.as_ref()
    }

    /// Get a reference to the current timestamp.
    pub fn timestamp(&self) -> &HlcTimestamp {
        &self.timestamp
    }

    /// Extract changes since the given frontier timestamp.
    ///
    /// If the register's timestamp is strictly greater than `frontier`, the
    /// whole register is the delta (it was modified after the frontier).
    /// Otherwise returns `None` — the peer already has the current value.
    pub fn delta_since(&self, frontier: &HlcTimestamp) -> Option<Self> {
        if self.timestamp > *frontier {
            Some(self.clone())
        } else {
            None
        }
    }
}

impl<T: Clone + Ord> LwwRegister<T> {
    /// Merge another register into this one, keeping the value with the higher timestamp.
    ///
    /// When timestamps are equal, the larger value (by `Ord`) wins to
    /// guarantee commutativity: `merge(a, b)` always produces the same
    /// result as `merge(b, a)`.
    pub fn merge(&mut self, other: &LwwRegister<T>) {
        match other.timestamp.cmp(&self.timestamp) {
            std::cmp::Ordering::Greater => {
                self.value = other.value.clone();
                self.timestamp = other.timestamp.clone();
            }
            std::cmp::Ordering::Equal => {
                // Deterministic tiebreaker: keep the larger value so that
                // merge(a, b) == merge(b, a) even when timestamps collide.
                match (&self.value, &other.value) {
                    (Some(s), Some(o)) if o > s => {
                        self.value = other.value.clone();
                    }
                    (None, Some(_)) => {
                        self.value = other.value.clone();
                    }
                    _ => {}
                }
            }
            std::cmp::Ordering::Less => {}
        }
    }

    /// Merge a delta into this register.
    ///
    /// For LwwRegister, `merge_delta` is identical to `merge` because the
    /// delta is a complete register snapshot (value + timestamp).
    pub fn merge_delta(&mut self, delta: &LwwRegister<T>) {
        self.merge(delta);
    }
}

impl<T: Clone> Default for LwwRegister<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: PartialEq> PartialEq for LwwRegister<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value && self.timestamp == other.timestamp
    }
}

impl<T: Eq> Eq for LwwRegister<T> {}

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

    #[test]
    fn new_register_is_empty() {
        let reg: LwwRegister<String> = LwwRegister::new();
        assert!(reg.get().is_none());
    }

    #[test]
    fn set_and_get() {
        let mut reg = LwwRegister::new();
        let updated = reg.set("hello".to_string(), ts(100, 0, "node-a"));
        assert!(updated);
        assert_eq!(reg.get(), Some(&"hello".to_string()));
    }

    #[test]
    fn later_timestamp_wins() {
        let mut reg = LwwRegister::new();
        reg.set(1, ts(100, 0, "node-a"));
        let updated = reg.set(2, ts(200, 0, "node-a"));
        assert!(updated);
        assert_eq!(reg.get(), Some(&2));
    }

    #[test]
    fn earlier_timestamp_ignored() {
        let mut reg = LwwRegister::new();
        reg.set(1, ts(200, 0, "node-a"));
        let updated = reg.set(2, ts(100, 0, "node-b"));
        assert!(!updated);
        assert_eq!(reg.get(), Some(&1));
    }

    #[test]
    fn equal_timestamp_ignored() {
        let mut reg = LwwRegister::new();
        reg.set(1, ts(100, 0, "node-a"));
        let updated = reg.set(2, ts(100, 0, "node-a"));
        assert!(!updated);
        assert_eq!(reg.get(), Some(&1));
    }

    #[test]
    fn logical_counter_breaks_tie() {
        let mut reg = LwwRegister::new();
        reg.set("first", ts(100, 0, "node-a"));
        let updated = reg.set("second", ts(100, 1, "node-a"));
        assert!(updated);
        assert_eq!(reg.get(), Some(&"second"));
    }

    #[test]
    fn node_id_breaks_tie() {
        let mut reg = LwwRegister::new();
        reg.set("alpha", ts(100, 0, "node-a"));
        let updated = reg.set("beta", ts(100, 0, "node-b"));
        assert!(updated);
        assert_eq!(reg.get(), Some(&"beta"));
    }

    #[test]
    fn merge_higher_timestamp_wins() {
        let mut reg_a = LwwRegister::new();
        reg_a.set(10, ts(100, 0, "node-a"));

        let mut reg_b = LwwRegister::new();
        reg_b.set(20, ts(200, 0, "node-b"));

        reg_a.merge(&reg_b);
        assert_eq!(reg_a.get(), Some(&20));
    }

    #[test]
    fn merge_lower_timestamp_no_change() {
        let mut reg_a = LwwRegister::new();
        reg_a.set(10, ts(200, 0, "node-a"));

        let mut reg_b = LwwRegister::new();
        reg_b.set(20, ts(100, 0, "node-b"));

        reg_a.merge(&reg_b);
        assert_eq!(reg_a.get(), Some(&10));
    }

    #[test]
    fn merge_is_commutative() {
        let mut reg_a = LwwRegister::new();
        reg_a.set(1, ts(100, 0, "node-a"));

        let mut reg_b = LwwRegister::new();
        reg_b.set(2, ts(200, 0, "node-b"));

        let mut merged_ab = reg_a.clone();
        merged_ab.merge(&reg_b);

        let mut merged_ba = reg_b.clone();
        merged_ba.merge(&reg_a);

        assert_eq!(merged_ab, merged_ba);
    }

    #[test]
    fn merge_is_idempotent() {
        let mut reg_a = LwwRegister::new();
        reg_a.set(42, ts(100, 0, "node-a"));

        let mut reg_b = LwwRegister::new();
        reg_b.set(99, ts(200, 0, "node-b"));

        let mut merged = reg_a.clone();
        merged.merge(&reg_b);
        let after_first = merged.clone();
        merged.merge(&reg_b);
        assert_eq!(merged, after_first);
    }

    #[test]
    fn timestamp_accessor() {
        let mut reg = LwwRegister::new();
        let t = ts(500, 3, "node-x");
        reg.set("val", t.clone());
        assert_eq!(reg.timestamp(), &t);
    }

    #[test]
    fn default_is_empty() {
        let reg: LwwRegister<i32> = LwwRegister::default();
        assert!(reg.get().is_none());
    }

    #[test]
    fn serde_roundtrip() {
        let mut reg = LwwRegister::new();
        reg.set("hello".to_string(), ts(100, 1, "node-a"));

        let json = serde_json::to_string(&reg).unwrap();
        let back: LwwRegister<String> = serde_json::from_str(&json).unwrap();
        assert_eq!(back.get(), Some(&"hello".to_string()));
    }

    // ---------------------------------------------------------------
    // Delta tests
    // ---------------------------------------------------------------

    #[test]
    fn delta_since_returns_some_when_newer() {
        let mut reg = LwwRegister::new();
        reg.set("hello", ts(200, 0, "node-a"));

        let delta = reg.delta_since(&ts(100, 0, ""));
        assert!(delta.is_some());
        assert_eq!(delta.unwrap().get(), Some(&"hello"));
    }

    #[test]
    fn delta_since_returns_none_when_older() {
        let mut reg = LwwRegister::new();
        reg.set("hello", ts(100, 0, "node-a"));

        let delta = reg.delta_since(&ts(200, 0, ""));
        assert!(delta.is_none());
    }

    #[test]
    fn delta_since_returns_none_when_equal() {
        let mut reg = LwwRegister::new();
        reg.set("hello", ts(100, 0, "node-a"));

        let delta = reg.delta_since(&ts(100, 0, "node-a"));
        assert!(delta.is_none());
    }

    #[test]
    fn delta_since_empty_register() {
        let reg: LwwRegister<String> = LwwRegister::new();
        // Empty register has timestamp (0, 0, ""), frontier at (0, 0, "") => not >
        let delta = reg.delta_since(&ts(0, 0, ""));
        assert!(delta.is_none());
    }

    #[test]
    fn delta_round_trip_produces_same_result() {
        let mut reg_a = LwwRegister::new();
        reg_a.set(10, ts(100, 0, "node-a"));

        let mut reg_b = LwwRegister::new();
        reg_b.set(20, ts(200, 0, "node-b"));

        // Full merge path.
        let mut via_full = reg_a.clone();
        via_full.merge(&reg_b);

        // Delta path: extract delta from reg_b since reg_a's frontier.
        let delta = reg_b.delta_since(&ts(100, 0, "node-a")).unwrap();
        let mut via_delta = reg_a.clone();
        via_delta.merge_delta(&delta);

        assert_eq!(via_full, via_delta);
    }

    #[test]
    fn merge_delta_is_equivalent_to_merge() {
        let mut reg_a = LwwRegister::new();
        reg_a.set(1, ts(100, 0, "node-a"));

        let mut reg_b = LwwRegister::new();
        reg_b.set(2, ts(200, 0, "node-b"));

        let mut via_merge = reg_a.clone();
        via_merge.merge(&reg_b);

        let mut via_delta = reg_a.clone();
        via_delta.merge_delta(&reg_b);

        assert_eq!(via_merge, via_delta);
    }
}
