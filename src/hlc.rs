use std::cmp::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// A snapshot of the Hybrid Logical Clock at a point in time.
///
/// Ordering: physical time first, then logical counter, then node_id for total ordering.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct HlcTimestamp {
    /// Physical timestamp in milliseconds since UNIX epoch.
    pub physical: u64,
    /// Logical counter for ordering events at the same physical time.
    pub logical: u32,
    /// Node that generated this timestamp.
    pub node_id: String,
}

impl Ord for HlcTimestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        self.physical
            .cmp(&other.physical)
            .then_with(|| self.logical.cmp(&other.logical))
            .then_with(|| self.node_id.cmp(&other.node_id))
    }
}

impl PartialOrd for HlcTimestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Hybrid Logical Clock combining physical time and logical counter.
///
/// Used as the basis for ack_frontier (FR-008).
pub struct Hlc {
    /// Latest known physical timestamp in milliseconds.
    physical: u64,
    /// Logical counter for ordering events at the same physical time.
    logical: u32,
    /// Node that owns this clock.
    node_id: String,
}

impl Hlc {
    /// Create a new HLC for the given node.
    pub fn new(node_id: String) -> Self {
        Self {
            physical: 0,
            logical: 0,
            node_id,
        }
    }

    /// Generate a new timestamp, ensuring monotonicity.
    pub fn now(&mut self) -> HlcTimestamp {
        let wall = physical_ms();

        if wall > self.physical {
            self.physical = wall;
            self.logical = 0;
        } else {
            self.logical += 1;
        }

        HlcTimestamp {
            physical: self.physical,
            logical: self.logical,
            node_id: self.node_id.clone(),
        }
    }

    /// Update the local clock based on a received timestamp.
    ///
    /// Takes the maximum of the local physical time, the received physical time,
    /// and the current wall clock, then adjusts the logical counter accordingly.
    pub fn update(&mut self, received: &HlcTimestamp) {
        let wall = physical_ms();
        let max_physical = wall.max(self.physical).max(received.physical);

        if max_physical == self.physical && max_physical == received.physical {
            // All three equal (or wall <= both): advance logical beyond both.
            self.logical = self.logical.max(received.logical) + 1;
        } else if max_physical == self.physical {
            // Local physical is ahead: just bump logical.
            self.logical += 1;
        } else if max_physical == received.physical {
            // Received physical is ahead: adopt its logical + 1.
            self.logical = received.logical + 1;
        } else {
            // Wall clock is ahead of both: reset logical.
            self.logical = 0;
        }

        self.physical = max_physical;
    }
}

/// Returns current wall-clock time in milliseconds since UNIX epoch.
fn physical_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monotonicity() {
        let mut clock = Hlc::new("node-a".into());
        let t1 = clock.now();
        let t2 = clock.now();
        let t3 = clock.now();

        assert!(t1 < t2, "successive now() must increase");
        assert!(t2 < t3, "successive now() must increase");
    }

    #[test]
    fn update_advances_clock() {
        let mut clock = Hlc::new("node-a".into());
        let local = clock.now();

        // Simulate receiving a timestamp far in the future.
        let future = HlcTimestamp {
            physical: local.physical + 100_000,
            logical: 5,
            node_id: "node-b".into(),
        };
        clock.update(&future);

        let after = clock.now();
        assert!(after > future, "clock must advance past received timestamp");
    }

    #[test]
    fn update_with_past_timestamp() {
        let mut clock = Hlc::new("node-a".into());
        let local = clock.now();

        // A timestamp in the past should not regress the clock.
        let past = HlcTimestamp {
            physical: 1,
            logical: 0,
            node_id: "node-b".into(),
        };
        clock.update(&past);

        let after = clock.now();
        assert!(after > local, "clock must never regress");
    }

    #[test]
    fn ordering_physical_first() {
        let a = HlcTimestamp {
            physical: 100,
            logical: 99,
            node_id: "z".into(),
        };
        let b = HlcTimestamp {
            physical: 200,
            logical: 0,
            node_id: "a".into(),
        };
        assert!(a < b);
    }

    #[test]
    fn ordering_logical_second() {
        let a = HlcTimestamp {
            physical: 100,
            logical: 1,
            node_id: "z".into(),
        };
        let b = HlcTimestamp {
            physical: 100,
            logical: 2,
            node_id: "a".into(),
        };
        assert!(a < b);
    }

    #[test]
    fn ordering_node_id_tiebreak() {
        let a = HlcTimestamp {
            physical: 100,
            logical: 1,
            node_id: "alpha".into(),
        };
        let b = HlcTimestamp {
            physical: 100,
            logical: 1,
            node_id: "beta".into(),
        };
        assert!(a < b);
        assert_ne!(a, b);
    }

    #[test]
    fn concurrent_events_two_nodes() {
        let mut clock_a = Hlc::new("node-a".into());
        let mut clock_b = Hlc::new("node-b".into());

        let ta = clock_a.now();
        let tb = clock_b.now();

        // Even if physical times happen to match, timestamps are still totally ordered.
        assert_ne!(ta, tb, "different nodes produce different timestamps");
        // One must be less than the other (total order).
        assert!(ta < tb || tb < ta);
    }

    #[test]
    fn mutual_update() {
        let mut clock_a = Hlc::new("node-a".into());
        let mut clock_b = Hlc::new("node-b".into());

        let ta1 = clock_a.now();
        clock_b.update(&ta1);
        let tb1 = clock_b.now();

        // b's timestamp must be after a's.
        assert!(tb1 > ta1);

        clock_a.update(&tb1);
        let ta2 = clock_a.now();

        // a's new timestamp must be after b's.
        assert!(ta2 > tb1);
    }

    #[test]
    fn serialization_roundtrip() {
        let ts = HlcTimestamp {
            physical: 1_700_000_000_000,
            logical: 42,
            node_id: "node-x".into(),
        };
        let json = serde_json::to_string(&ts).expect("serialize");
        let back: HlcTimestamp = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(ts, back);
    }
}
