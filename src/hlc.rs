use std::cmp::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::error::HlcError;

/// Maximum acceptable clock skew from a remote peer, in milliseconds (60 seconds).
///
/// `update()` rejects received timestamps whose `physical` field exceeds
/// `wall_clock + MAX_CLOCK_SKEW_MS`. A far-future physical timestamp would
/// advance `self.physical` beyond the real wall clock, causing `now()` to
/// stop advancing and eventually fail with `Overflow` — a DoS vector.
const MAX_CLOCK_SKEW_MS: u64 = 60_000;

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
    ///
    /// On `Overflow`, the physical clock is advanced by 1 ms and the logical
    /// counter is reset to 0, allowing the next `now()` call to succeed.
    /// This mirrors the recovery guarantee in `update()`.
    pub fn now(&mut self) -> Result<HlcTimestamp, HlcError> {
        let wall = physical_ms();

        if wall > self.physical {
            self.physical = wall;
            self.logical = 0;
        } else {
            match self.logical.checked_add(1) {
                Some(l) => self.logical = l,
                None => {
                    // Logical saturated: advance physical by 1 ms and reset
                    // logical so subsequent calls can succeed. The caller
                    // receives Overflow to signal the exceptional tick.
                    self.physical = self.physical.saturating_add(1);
                    self.logical = 0;
                    return Err(HlcError::Overflow);
                }
            }
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
    ///
    /// Returns `Err(HlcError::ClockSkew)` if `received.physical` is more than
    /// [`MAX_CLOCK_SKEW_MS`] ahead of the local wall clock. Accepting a far-future
    /// physical timestamp would set `self.physical` to that value, causing `now()`
    /// to stop advancing and eventually return `Overflow` indefinitely (DoS vector).
    pub fn update(&mut self, received: &HlcTimestamp) -> Result<(), HlcError> {
        let wall = physical_ms();

        // Reject timestamps from peers that are too far in the future.
        if received.physical > wall.saturating_add(MAX_CLOCK_SKEW_MS) {
            return Err(HlcError::ClockSkew {
                received_ms: received.physical,
                wall_ms: wall,
                max_skew_ms: MAX_CLOCK_SKEW_MS,
            });
        }

        let max_physical = wall.max(self.physical).max(received.physical);

        let logical_result = if max_physical == self.physical && max_physical == received.physical {
            // Local and received physical are equal and dominate wall; advance logical beyond both.
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

        // Advance the physical clock to max_physical, then handle the logical result.
        self.physical = max_physical;

        match logical_result {
            Ok(l) => {
                self.logical = l;
                Ok(())
            }
            Err(e) => {
                // On overflow the logical counter cannot be expressed at the current
                // physical time.  Advance physical by 1 ms and reset logical to 0 so
                // that the next now() or update() call produces a timestamp strictly
                // greater than any peer timestamp at max_physical (including one with
                // logical == u32::MAX).  Without this reset, self.logical would retain
                // its pre-call value and a subsequent now() could return a timestamp
                // less than the overflowed peer timestamp, violating HLC causality.
                self.physical = self.physical.saturating_add(1);
                self.logical = 0;
                Err(e)
            }
        }
    }
}

/// Returns current wall-clock time in milliseconds since UNIX epoch.
///
/// Public so that other subsystems (e.g. the attestation pool's future-skew
/// guard) share the same wall-clock source as the HLC.
pub fn wall_clock_ms() -> u64 {
    physical_ms()
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

        // Simulate receiving a timestamp within the allowed clock skew (10s ahead).
        let future = HlcTimestamp {
            physical: local.physical + 10_000,
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
        // Drive logical to u32::MAX using a peer timestamp within the allowed
        // clock skew range. Use +30s (half of MAX_CLOCK_SKEW_MS) so the skew
        // guard does not reject it; the 30s gap also ensures the real wall clock
        // cannot advance past self.physical during the test, which would reset
        // logical to 0 and cause now() to return Ok instead of Overflow.
        // received.logical = u32::MAX - 1 → local logical = u32::MAX after update.
        // Then now() tries to increment past u32::MAX → Overflow.
        let mut clock = Hlc::new("node-a".into());
        let wall = physical_ms();
        let near_max = HlcTimestamp {
            physical: wall + 30_000, // 30s ahead, within MAX_CLOCK_SKEW_MS (60s)
            logical: u32::MAX - 1,
            node_id: "node-b".into(),
        };
        clock.update(&near_max).expect("should not overflow yet");
        // logical == u32::MAX; now() increments → Overflow.
        let result = clock.now();
        assert_eq!(result, Err(HlcError::Overflow));
    }

    #[test]
    fn now_recovers_after_overflow() {
        // After now() returns Overflow, the physical clock is advanced by 1 ms
        // and logical is reset to 0. The very next call to now() must succeed.
        let mut clock = Hlc::new("node-a".into());
        let wall = physical_ms();
        // Force physical 30s into the future (within MAX_CLOCK_SKEW_MS) and
        // logical to u32::MAX - 1 so the first now() call overflows. The 30s
        // margin prevents the wall clock from advancing past self.physical
        // during the test, which would otherwise reset logical and avoid Overflow.
        let near_max = HlcTimestamp {
            physical: wall + 30_000,
            logical: u32::MAX - 1,
            node_id: "node-b".into(),
        };
        clock.update(&near_max).expect("update should succeed");
        // First now(): Overflow.
        assert_eq!(clock.now(), Err(HlcError::Overflow));
        // Second now(): must succeed — physical was advanced, logical reset.
        assert!(
            clock.now().is_ok(),
            "now() must recover on the call after Overflow"
        );
    }

    #[test]
    fn update_physical_advanced_before_overflow_return() {
        // update() must advance self.physical and reset self.logical=0 when it
        // returns Err(Overflow), so that subsequent now() calls produce timestamps
        // strictly greater than the overflowed peer timestamp (HLC causality invariant).
        // Use +30s (within MAX_CLOCK_SKEW_MS) to prevent the real wall clock from
        // catching up to self.physical during the test.
        let mut clock = Hlc::new("node-a".into());
        let wall = physical_ms();
        let overflow_ts = HlcTimestamp {
            physical: wall + 30_000,
            logical: u32::MAX,
            node_id: "node-b".into(),
        };
        assert_eq!(clock.update(&overflow_ts), Err(HlcError::Overflow));

        // After overflow: self.physical = wall+30001 (advanced 1ms), self.logical = 0.
        // now() succeeds and produces a timestamp strictly > overflow_ts.
        let after = clock
            .now()
            .expect("now() must succeed after update() Overflow");
        assert!(
            after > overflow_ts,
            "now() after overflow must produce timestamp > the overflowed peer ts; \
             got after={after:?}, overflow_ts={overflow_ts:?}"
        );
    }

    #[test]
    fn update_returns_overflow_error_when_logical_would_overflow() {
        // A peer sends logical=u32::MAX with a physical timestamp 1s in the
        // future (within skew limit). The "received physical is ahead" branch
        // attempts received.logical.checked_add(1) which overflows → Err.
        let mut clock = Hlc::new("node-a".into());
        let wall = physical_ms();
        let overflow_ts = HlcTimestamp {
            physical: wall + 1_000,
            logical: u32::MAX,
            node_id: "node-b".into(),
        };
        let result = clock.update(&overflow_ts);
        assert_eq!(result, Err(HlcError::Overflow));
    }

    #[test]
    fn update_rejects_far_future_timestamp() {
        // A peer sends physical = wall + MAX_CLOCK_SKEW_MS + 1_000 (1s margin
        // avoids flakiness from clock drift between the wall sample here and the
        // internal re-sample inside update()). update() must return ClockSkew
        // without touching self.physical, so that now() continues to work
        // normally after the bad update.
        let mut clock = Hlc::new("node-a".into());
        let wall = physical_ms();
        let far_future = HlcTimestamp {
            physical: wall + MAX_CLOCK_SKEW_MS + 1_000,
            logical: 0,
            node_id: "malicious".into(),
        };
        let result = clock.update(&far_future);
        assert!(
            matches!(result, Err(HlcError::ClockSkew { .. })),
            "expected ClockSkew, got {:?}",
            result
        );
        // The local clock must still be usable after rejecting the bad update.
        assert!(clock.now().is_ok(), "now() must work after rejected update");
    }

    #[test]
    fn update_accepts_timestamp_at_skew_boundary() {
        // A timestamp exactly at MAX_CLOCK_SKEW_MS ahead should be accepted.
        let mut clock = Hlc::new("node-a".into());
        let wall = physical_ms();
        let at_boundary = HlcTimestamp {
            physical: wall + MAX_CLOCK_SKEW_MS,
            logical: 0,
            node_id: "peer".into(),
        };
        assert!(clock.update(&at_boundary).is_ok());
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
