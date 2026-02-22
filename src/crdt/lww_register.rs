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

    /// Merge another register into this one, keeping the value with the higher timestamp.
    pub fn merge(&mut self, other: &LwwRegister<T>) {
        if other.timestamp > self.timestamp {
            self.value = other.value.clone();
            self.timestamp = other.timestamp.clone();
        }
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
}
