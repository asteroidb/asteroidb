use std::cmp::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::error::HlcError;

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
    ///
    /// Returns `Err(HlcError::Overflow)` if the logical counter would overflow
    /// `u32::MAX` without the physical clock advancing. This prevents silent
    /// clamping (the former `saturating_add` behaviour) that would produce
    /// duplicate timestamps and violate strict monotonicity.
    pub fn now(&mut self) -> Result<HlcTimestamp, HlcError> {
        let wall = physical_ms();

        if wall > self.physical {
            self.physical = wall;
            self.logical = 0;
        } else {
            self.logical = self.logical.checked_add(1).ok_or(HlcError::Overflow)?;
        }

        Ok(HlcTimestamp {
            physical: self.physical,
            logical: self.logical,
            node_id: self.node_id.clone(),
        })
    }

    /// Update the local clock based on a received timestamp.
    ///
    /// Takes the maximum of the local physical time, the received physical time,
    /// and the current wall clock, then adjusts the logical counter accordingly.
    ///
    /// Returns `Err(HlcError::Overflow)` if the logical counter would overflow
    /// `u32::MAX`. This surfaces clearly instead of silently clamping (the former
    /// `saturating_add` behaviour), which could produce duplicate timestamps.
    pub fn update(&mut self, received: &HlcTimestamp) -> Result<(), HlcError> {
        let wall = physical_ms();
        let max_physical = wall.max(self.physical).max(received.physical);

        let logical_result = if max_physical == self.physical && max_physical == received.physical {
            // All three equal (or wall <= both): advance logical beyond both.
            self.logical
                .max(received.logical)
                .checked_add(1)
                .ok_or(HlcError::Overflow)
        } else if max_physical == self.physical {
            // Local physical is ahead: just bump logical.
            self.logical.checked_add(1).ok_or(HlcError::Overflow)
        } else if max_physical == received.physical {
            // Received physical is ahead: adopt its logical + 1.
            received.logical.checked_add(1).ok_or(HlcError::Overflow)
        } else {
            // Wall clock is ahead of both: reset logical.
            Ok(0u32)
        };

        // Always advance the physical clock before returning, even on overflow.
        // Without this, a cascade of overflow errors in the same millisecond
        // would prevent self.physical from advancing to the next millisecond,
        // causing every subsequent update() call to also fail with Overflow.
        self.physical = max_physical;

        self.logical = logical_result?;
        Ok(())
    }
}

/// Returns current wall-clock time in milliseconds since UNIX epoch.
fn physical_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monotonicity() {
        let mut clock = Hlc::new("node-a".into());
        let t1 = clock.now().expect("HLC overflow");
        let t2 = clock.now().expect("HLC overflow");
        let t3 = clock.now().expect("HLC overflow");

        assert!(t1 < t2, "successive now() must increase");
        assert!(t2 < t3, "successive now() must increase");
    }

    #[test]
    fn update_advances_clock() {
        let mut clock = Hlc::new("node-a".into());
        let local = clock.now().expect("HLC overflow");

        // Simulate receiving a timestamp far in the future.
        let future = HlcTimestamp {
            physical: local.physical + 100_000,
            logical: 5,
            node_id: "node-b".into(),
        };
        clock.update(&future).expect("HLC overflow");

        let after = clock.now().expect("HLC overflow");
        assert!(after > future, "clock must advance past received timestamp");
    }

    #[test]
    fn update_with_past_timestamp() {
        let mut clock = Hlc::new("node-a".into());
        let local = clock.now().expect("HLC overflow");

        // A timestamp in the past should not regress the clock.
        let past = HlcTimestamp {
            physical: 1,
            logical: 0,
            node_id: "node-b".into(),
        };
        clock.update(&past).expect("HLC overflow");

        let after = clock.now().expect("HLC overflow");
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

        let ta = clock_a.now().expect("HLC overflow");
        let tb = clock_b.now().expect("HLC overflow");

        // Even if physical times happen to match, timestamps are still totally ordered.
        assert_ne!(ta, tb, "different nodes produce different timestamps");
        // One must be less than the other (total order).
        assert!(ta < tb || tb < ta);
    }

    #[test]
    fn mutual_update() {
        let mut clock_a = Hlc::new("node-a".into());
        let mut clock_b = Hlc::new("node-b".into());

        let ta1 = clock_a.now().expect("HLC overflow");
        clock_b.update(&ta1).expect("HLC overflow");
        let tb1 = clock_b.now().expect("HLC overflow");

        // b's timestamp must be after a's.
        assert!(tb1 > ta1);

        clock_a.update(&tb1).expect("HLC overflow");
        let ta2 = clock_a.now().expect("HLC overflow");

        // a's new timestamp must be after b's.
        assert!(ta2 > tb1);
    }

    #[test]
    fn now_returns_overflow_error_when_logical_is_at_max() {
        // Drive the clock to physical=u64::MAX, logical=u32::MAX by first
        // doing an update() with a far-future peer timestamp (logical=u32::MAX-1),
        // which sets local logical to u32::MAX without itself overflowing.
        // Then calling now() increments logical past u32::MAX → Overflow.
        let mut clock = Hlc::new("node-a".into());
        let near_max = HlcTimestamp {
            physical: u64::MAX,
            logical: u32::MAX - 1,
            node_id: "node-b".into(),
        };
        clock.update(&near_max).expect("should not overflow yet");
        // Now logical == u32::MAX, physical == u64::MAX.
        // next now() must return Overflow.
        let result = clock.now();
        assert_eq!(result, Err(HlcError::Overflow));
    }

    #[test]
    fn update_returns_overflow_error_when_logical_would_overflow() {
        // A peer sends logical=u32::MAX with a physical timestamp far in the
        // future.  The "received physical is ahead" branch attempts
        // received.logical.checked_add(1) which overflows → Err.
        let mut clock = Hlc::new("node-a".into());
        let overflow_ts = HlcTimestamp {
            physical: u64::MAX,
            logical: u32::MAX,
            node_id: "node-b".into(),
        };
        let result = clock.update(&overflow_ts);
        assert_eq!(result, Err(HlcError::Overflow));
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
