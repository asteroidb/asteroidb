//! SLO (Service Level Objective) framework for operational monitoring.
//!
//! Tracks error budgets for pre-defined SLOs and exposes a snapshot API
//! for dashboards and alerting integration.

use serde::Serialize;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Definition of a single SLO target.
#[derive(Debug, Clone, Serialize)]
pub struct SloTarget {
    /// Human-readable name of the SLO.
    pub name: String,
    /// Target value (e.g., 100.0 for 100ms P99, 99.0 for 99% availability).
    pub target_value: f64,
    /// Evaluation window in seconds (e.g., 3600 for 1 hour).
    pub window_secs: u64,
}

/// Budget tracking for a single SLO within its evaluation window.
#[derive(Debug, Clone, Serialize)]
pub struct SloBudget {
    /// SLO target definition.
    pub target: SloTarget,
    /// Total number of observations in the current window.
    pub total_requests: u64,
    /// Number of observations that violated the SLO.
    pub violations: u64,
}

impl SloBudget {
    /// Create a new empty budget for the given target.
    pub fn new(target: SloTarget) -> Self {
        Self {
            target,
            total_requests: 0,
            violations: 0,
        }
    }

    /// Remaining error budget as a percentage (0.0 to 100.0).
    ///
    /// Returns 100.0 when no requests have been observed (no budget consumed).
    pub fn budget_remaining(&self) -> f64 {
        if self.total_requests == 0 {
            return 100.0;
        }
        let violation_rate = self.violations as f64 / self.total_requests as f64;
        let remaining = (1.0 - violation_rate) * 100.0;
        remaining.max(0.0)
    }

    /// Whether the SLO is in warning state (>50% of budget consumed).
    pub fn is_warning(&self) -> bool {
        self.budget_remaining() < 50.0
    }

    /// Whether the SLO is in critical state (>80% of budget consumed).
    pub fn is_critical(&self) -> bool {
        self.budget_remaining() < 20.0
    }
}

/// A single observation with its timestamp.
#[derive(Debug, Clone)]
struct Observation {
    timestamp: Instant,
    value: f64,
}

/// Internal state for a single SLO being tracked.
#[derive(Debug)]
struct SloState {
    target: SloTarget,
    window: Duration,
    observations: Vec<Observation>,
}

impl SloState {
    fn new(target: SloTarget) -> Self {
        let window = Duration::from_secs(target.window_secs);
        Self {
            target,
            window,
            observations: Vec::new(),
        }
    }

    /// Record an observation, evicting expired entries.
    fn record(&mut self, value: f64, now: Instant) {
        self.evict_expired(now);
        self.observations.push(Observation {
            timestamp: now,
            value,
        });
    }

    /// Remove observations outside the evaluation window.
    fn evict_expired(&mut self, now: Instant) {
        let cutoff = now.checked_sub(self.window).unwrap_or(now);
        self.observations.retain(|o| o.timestamp >= cutoff);
    }

    /// Compute the budget snapshot.
    fn budget(&self, now: Instant) -> SloBudget {
        let cutoff = now.checked_sub(self.window).unwrap_or(now);
        let active: Vec<&Observation> = self
            .observations
            .iter()
            .filter(|o| o.timestamp >= cutoff)
            .collect();

        let total = active.len() as u64;

        // For "less than" SLOs (latency), a violation is value >= target.
        // For "greater than" SLOs (availability), a violation is value < target.
        let violations = if self.target.name.contains("availability") {
            // Availability: each observation is a success (1.0) or failure (0.0).
            // Violation means the observation was a failure.
            active
                .iter()
                .filter(|o| o.value < self.target.target_value)
                .count() as u64
        } else {
            // Latency / convergence: violation means value exceeds the target.
            active
                .iter()
                .filter(|o| o.value >= self.target.target_value)
                .count() as u64
        };

        SloBudget {
            target: self.target.clone(),
            total_requests: total,
            violations,
        }
    }
}

/// Serializable snapshot of all SLO budgets.
#[derive(Debug, Clone, Serialize)]
pub struct SloSnapshot {
    /// Per-SLO budget status.
    pub budgets: HashMap<String, SloBudget>,
}

/// Well-known SLO names.
pub const SLO_EVENTUAL_READ_P99: &str = "eventual_read_p99";
pub const SLO_CERTIFIED_READ_P99: &str = "certified_read_p99";
pub const SLO_REPLICATION_CONVERGENCE: &str = "replication_convergence";
pub const SLO_AUTHORITY_AVAILABILITY: &str = "authority_availability";

/// Create the pre-defined SLO targets for AsteroidDB.
fn default_slo_targets() -> Vec<SloTarget> {
    vec![
        SloTarget {
            name: SLO_EVENTUAL_READ_P99.to_string(),
            target_value: 50.0, // < 50ms
            window_secs: 3600,  // 1 hour
        },
        SloTarget {
            name: SLO_CERTIFIED_READ_P99.to_string(),
            target_value: 500.0, // < 500ms
            window_secs: 3600,
        },
        SloTarget {
            name: SLO_REPLICATION_CONVERGENCE.to_string(),
            target_value: 5000.0, // < 5s (5000ms)
            window_secs: 3600,
        },
        SloTarget {
            name: SLO_AUTHORITY_AVAILABILITY.to_string(),
            target_value: 1.0, // each observation is 1.0 (up) or 0.0 (down)
            window_secs: 3600,
        },
    ]
}

/// Tracks SLO observations and computes budget snapshots.
///
/// Thread-safe via interior `Mutex`. Shared via `Arc<SloTracker>`.
#[derive(Debug)]
pub struct SloTracker {
    states: Mutex<HashMap<String, SloState>>,
}

impl Default for SloTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl SloTracker {
    /// Create a tracker with default SLO targets.
    pub fn new() -> Self {
        let mut states = HashMap::new();
        for target in default_slo_targets() {
            let name = target.name.clone();
            states.insert(name, SloState::new(target));
        }
        Self {
            states: Mutex::new(states),
        }
    }

    /// Create a tracker with custom SLO targets (for testing).
    pub fn with_targets(targets: Vec<SloTarget>) -> Self {
        let mut states = HashMap::new();
        for target in targets {
            let name = target.name.clone();
            states.insert(name, SloState::new(target));
        }
        Self {
            states: Mutex::new(states),
        }
    }

    /// Record an observation for the given SLO.
    ///
    /// Ignores unknown SLO names silently.
    pub fn record_observation(&self, slo_name: &str, value: f64) {
        self.record_observation_at(slo_name, value, Instant::now());
    }

    /// Record an observation at a specific instant (for testing).
    pub fn record_observation_at(&self, slo_name: &str, value: f64, now: Instant) {
        let mut states = self.states.lock().unwrap();
        if let Some(state) = states.get_mut(slo_name) {
            state.record(value, now);
        }
    }

    /// Produce a snapshot of all SLO budgets.
    pub fn snapshot(&self) -> SloSnapshot {
        self.snapshot_at(Instant::now())
    }

    /// Produce a snapshot at a specific instant (for testing).
    pub fn snapshot_at(&self, now: Instant) -> SloSnapshot {
        let states = self.states.lock().unwrap();
        let budgets = states
            .iter()
            .map(|(name, state)| (name.clone(), state.budget(now)))
            .collect();
        SloSnapshot { budgets }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budget_remaining_with_no_observations() {
        let budget = SloBudget::new(SloTarget {
            name: "test".into(),
            target_value: 100.0,
            window_secs: 3600,
        });
        assert_eq!(budget.budget_remaining(), 100.0);
        assert!(!budget.is_warning());
        assert!(!budget.is_critical());
    }

    #[test]
    fn budget_remaining_no_violations() {
        let budget = SloBudget {
            target: SloTarget {
                name: "test".into(),
                target_value: 100.0,
                window_secs: 3600,
            },
            total_requests: 100,
            violations: 0,
        };
        assert_eq!(budget.budget_remaining(), 100.0);
        assert!(!budget.is_warning());
        assert!(!budget.is_critical());
    }

    #[test]
    fn budget_remaining_half_violated() {
        let budget = SloBudget {
            target: SloTarget {
                name: "test".into(),
                target_value: 100.0,
                window_secs: 3600,
            },
            total_requests: 100,
            violations: 50,
        };
        assert!((budget.budget_remaining() - 50.0).abs() < f64::EPSILON);
        assert!(!budget.is_warning()); // exactly 50% remaining is not warning
        assert!(!budget.is_critical());
    }

    #[test]
    fn budget_warning_threshold() {
        let budget = SloBudget {
            target: SloTarget {
                name: "test".into(),
                target_value: 100.0,
                window_secs: 3600,
            },
            total_requests: 100,
            violations: 51,
        };
        assert!(budget.is_warning());
        assert!(!budget.is_critical());
    }

    #[test]
    fn budget_critical_threshold() {
        let budget = SloBudget {
            target: SloTarget {
                name: "test".into(),
                target_value: 100.0,
                window_secs: 3600,
            },
            total_requests: 100,
            violations: 81,
        };
        assert!(budget.is_warning());
        assert!(budget.is_critical());
    }

    #[test]
    fn budget_all_violated() {
        let budget = SloBudget {
            target: SloTarget {
                name: "test".into(),
                target_value: 100.0,
                window_secs: 3600,
            },
            total_requests: 100,
            violations: 100,
        };
        assert_eq!(budget.budget_remaining(), 0.0);
        assert!(budget.is_warning());
        assert!(budget.is_critical());
    }

    #[test]
    fn tracker_default_has_four_slos() {
        let tracker = SloTracker::new();
        let snap = tracker.snapshot();
        assert_eq!(snap.budgets.len(), 4);
        assert!(snap.budgets.contains_key(SLO_EVENTUAL_READ_P99));
        assert!(snap.budgets.contains_key(SLO_CERTIFIED_READ_P99));
        assert!(snap.budgets.contains_key(SLO_REPLICATION_CONVERGENCE));
        assert!(snap.budgets.contains_key(SLO_AUTHORITY_AVAILABILITY));
    }

    #[test]
    fn tracker_record_and_snapshot_latency() {
        let tracker = SloTracker::with_targets(vec![SloTarget {
            name: "test_latency".into(),
            target_value: 100.0,
            window_secs: 3600,
        }]);
        let now = Instant::now();

        // 8 requests under target, 2 over target
        for _ in 0..8 {
            tracker.record_observation_at("test_latency", 50.0, now);
        }
        for _ in 0..2 {
            tracker.record_observation_at("test_latency", 150.0, now);
        }

        let snap = tracker.snapshot_at(now);
        let budget = &snap.budgets["test_latency"];
        assert_eq!(budget.total_requests, 10);
        assert_eq!(budget.violations, 2);
        assert!((budget.budget_remaining() - 80.0).abs() < f64::EPSILON);
    }

    #[test]
    fn tracker_record_availability() {
        let tracker = SloTracker::with_targets(vec![SloTarget {
            name: "test_availability".into(),
            target_value: 1.0,
            window_secs: 3600,
        }]);
        let now = Instant::now();

        // 9 up, 1 down
        for _ in 0..9 {
            tracker.record_observation_at("test_availability", 1.0, now);
        }
        tracker.record_observation_at("test_availability", 0.0, now);

        let snap = tracker.snapshot_at(now);
        let budget = &snap.budgets["test_availability"];
        assert_eq!(budget.total_requests, 10);
        assert_eq!(budget.violations, 1);
        assert!((budget.budget_remaining() - 90.0).abs() < f64::EPSILON);
    }

    #[test]
    fn tracker_window_expiry() {
        let tracker = SloTracker::with_targets(vec![SloTarget {
            name: "test_latency".into(),
            target_value: 100.0,
            window_secs: 2,
        }]);
        let base = Instant::now();

        // Record a violation at base time.
        tracker.record_observation_at("test_latency", 200.0, base);

        // 3 seconds later, the old observation should be expired.
        let later = base + Duration::from_secs(3);
        tracker.record_observation_at("test_latency", 50.0, later);

        let snap = tracker.snapshot_at(later);
        let budget = &snap.budgets["test_latency"];
        assert_eq!(budget.total_requests, 1);
        assert_eq!(budget.violations, 0);
        assert_eq!(budget.budget_remaining(), 100.0);
    }

    #[test]
    fn tracker_ignores_unknown_slo() {
        let tracker = SloTracker::new();
        // Should not panic.
        tracker.record_observation("nonexistent_slo", 42.0);
        let snap = tracker.snapshot();
        assert!(!snap.budgets.contains_key("nonexistent_slo"));
    }

    #[test]
    fn snapshot_serialization() {
        let tracker = SloTracker::new();
        let now = Instant::now();
        tracker.record_observation_at(SLO_EVENTUAL_READ_P99, 10.0, now);

        let snap = tracker.snapshot_at(now);
        let json = serde_json::to_string(&snap).unwrap();
        assert!(json.contains("eventual_read_p99"));
        assert!(json.contains("budget"));
    }

    #[test]
    fn slo_budget_serialization() {
        let budget = SloBudget {
            target: SloTarget {
                name: "test".into(),
                target_value: 100.0,
                window_secs: 3600,
            },
            total_requests: 10,
            violations: 2,
        };
        let json = serde_json::to_string(&budget).unwrap();
        assert!(json.contains("\"total_requests\":10"));
        assert!(json.contains("\"violations\":2"));
        assert!(json.contains("\"target_value\":100.0"));
    }
}
