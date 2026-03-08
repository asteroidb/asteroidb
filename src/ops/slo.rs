//! SLO (Service Level Objective) framework for operational monitoring.
//!
//! Tracks error budgets for pre-defined SLOs and exposes a snapshot API
//! for dashboards and alerting integration.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Whether the SLO target value is an upper or lower bound.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SloKind {
    /// Value should be below target (e.g., latency < 50ms).
    LessThan,
    /// Value should be above target (e.g., availability > 99%).
    GreaterThan,
}

/// Definition of a single SLO target.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SloTarget {
    /// Human-readable name of the SLO.
    pub name: String,
    /// Comparison direction for violation detection.
    pub kind: SloKind,
    /// Target value (e.g., 50.0 for 50ms P99, 99.0 for 99% availability).
    pub target_value: f64,
    /// Target percentage that must be met (e.g., 99.9 means 99.9% of observations must pass).
    pub target_percentage: f64,
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
    /// Remaining error budget as a percentage (0.0 to 100.0).
    pub budget_remaining: f64,
    /// Whether the SLO is in warning state (>50% of budget consumed).
    pub is_warning: bool,
    /// Whether the SLO is in critical state (>80% of budget consumed).
    pub is_critical: bool,
}

impl SloBudget {
    /// Create a new empty budget for the given target.
    pub fn new(target: SloTarget) -> Self {
        Self {
            target,
            total_requests: 0,
            violations: 0,
            budget_remaining: 100.0,
            is_warning: false,
            is_critical: false,
        }
    }

    /// Build a budget from raw counts; computes derived fields automatically.
    fn from_counts(target: SloTarget, total_requests: u64, violations: u64) -> Self {
        let mut budget = Self {
            target,
            total_requests,
            violations,
            budget_remaining: 0.0,
            is_warning: false,
            is_critical: false,
        };
        budget.recompute();
        budget
    }

    /// Recompute derived fields from current counts.
    fn recompute(&mut self) {
        self.budget_remaining = self.compute_budget_remaining();
        self.is_warning = self.budget_remaining < 50.0;
        self.is_critical = self.budget_remaining < 20.0;
    }

    /// Remaining error budget as a percentage (0.0 to 100.0).
    ///
    /// The error budget is the fraction of allowed errors that has not yet
    /// been consumed.  For a 99.9% SLO the allowed error rate is 0.1%;
    /// if the actual error rate is 0.05% then 50% of the budget remains.
    fn compute_budget_remaining(&self) -> f64 {
        if self.total_requests == 0 {
            return 100.0;
        }
        let allowed_error_rate = 1.0 - (self.target.target_percentage / 100.0);
        if allowed_error_rate <= 0.0 {
            return 0.0;
        }
        let actual_error_rate = self.violations as f64 / self.total_requests as f64;
        let budget_consumed = actual_error_rate / allowed_error_rate;
        (1.0 - budget_consumed).max(0.0) * 100.0
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
    observations: VecDeque<Observation>,
}

impl SloState {
    fn new(target: SloTarget) -> Self {
        let window = Duration::from_secs(target.window_secs);
        Self {
            target,
            window,
            observations: VecDeque::new(),
        }
    }

    /// Record an observation, evicting expired entries.
    fn record(&mut self, value: f64, now: Instant) {
        self.evict_expired(now);
        self.observations.push_back(Observation {
            timestamp: now,
            value,
        });
    }

    /// Remove observations outside the evaluation window.
    fn evict_expired(&mut self, now: Instant) {
        let cutoff = now.checked_sub(self.window).unwrap_or(now);
        while let Some(front) = self.observations.front() {
            if front.timestamp < cutoff {
                self.observations.pop_front();
            } else {
                break;
            }
        }
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

        let violations = active
            .iter()
            .filter(|o| match self.target.kind {
                SloKind::LessThan => o.value > self.target.target_value,
                SloKind::GreaterThan => o.value < self.target.target_value,
            })
            .count() as u64;

        SloBudget::from_counts(self.target.clone(), total, violations)
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
            kind: SloKind::LessThan,
            target_value: 50.0, // < 50ms
            target_percentage: 99.0,
            window_secs: 3600, // 1 hour
        },
        SloTarget {
            name: SLO_CERTIFIED_READ_P99.to_string(),
            kind: SloKind::LessThan,
            target_value: 500.0, // < 500ms
            target_percentage: 99.0,
            window_secs: 3600,
        },
        SloTarget {
            name: SLO_REPLICATION_CONVERGENCE.to_string(),
            kind: SloKind::LessThan,
            target_value: 5000.0, // < 5s (5000ms)
            target_percentage: 95.0,
            window_secs: 3600,
        },
        SloTarget {
            name: SLO_AUTHORITY_AVAILABILITY.to_string(),
            kind: SloKind::GreaterThan,
            target_value: 99.0, // > 99%
            target_percentage: 99.9,
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

    /// Helper to create a LessThan SloTarget with the given target_percentage.
    fn lt_target(name: &str, target_value: f64, target_percentage: f64) -> SloTarget {
        SloTarget {
            name: name.into(),
            kind: SloKind::LessThan,
            target_value,
            target_percentage,
            window_secs: 3600,
        }
    }

    /// Helper to create a GreaterThan SloTarget.
    fn gt_target(name: &str, target_value: f64, target_percentage: f64) -> SloTarget {
        SloTarget {
            name: name.into(),
            kind: SloKind::GreaterThan,
            target_value,
            target_percentage,
            window_secs: 3600,
        }
    }

    #[test]
    fn budget_remaining_with_no_observations() {
        let budget = SloBudget::new(lt_target("test", 100.0, 99.0));
        assert_eq!(budget.budget_remaining, 100.0);
        assert!(!budget.is_warning);
        assert!(!budget.is_critical);
    }

    #[test]
    fn budget_remaining_no_violations() {
        let budget = SloBudget::from_counts(lt_target("test", 100.0, 99.0), 100, 0);
        assert_eq!(budget.budget_remaining, 100.0);
        assert!(!budget.is_warning);
        assert!(!budget.is_critical);
    }

    #[test]
    fn budget_remaining_half_budget_consumed() {
        // 99% SLO: allowed error rate = 1%.
        // 0.5% actual error rate => 50% budget consumed => 50% remaining.
        let budget = SloBudget::from_counts(lt_target("test", 100.0, 99.0), 1000, 5);
        assert!((budget.budget_remaining - 50.0).abs() < 0.01);
        assert!(!budget.is_warning);
        assert!(!budget.is_critical);
    }

    #[test]
    fn budget_warning_threshold() {
        // 99% SLO: allowed error rate = 1%.
        // 0.6% actual error rate => 60% consumed => 40% remaining (warning).
        let budget = SloBudget::from_counts(lt_target("test", 100.0, 99.0), 1000, 6);
        assert!(budget.is_warning);
        assert!(!budget.is_critical);
    }

    #[test]
    fn budget_critical_threshold() {
        // 99% SLO: allowed error rate = 1%.
        // 0.9% actual error rate => 90% consumed => 10% remaining (critical).
        let budget = SloBudget::from_counts(lt_target("test", 100.0, 99.0), 1000, 9);
        assert!(budget.is_warning);
        assert!(budget.is_critical);
    }

    #[test]
    fn budget_all_violated() {
        let budget = SloBudget::from_counts(lt_target("test", 100.0, 99.0), 100, 100);
        assert_eq!(budget.budget_remaining, 0.0);
        assert!(budget.is_warning);
        assert!(budget.is_critical);
    }

    #[test]
    fn budget_one_pct_error_against_999_slo() {
        // 99.9% SLO: allowed error rate = 0.1%.
        // 1% actual error rate => 10x over budget => budget exhausted.
        let budget = SloBudget::from_counts(lt_target("test", 100.0, 99.9), 1000, 10);
        assert_eq!(budget.budget_remaining, 0.0);
        assert!(budget.is_critical);
    }

    #[test]
    fn violation_detection_less_than() {
        let tracker = SloTracker::with_targets(vec![lt_target("latency", 100.0, 99.0)]);
        let now = Instant::now();

        // Exactly at target should NOT be a violation for LessThan.
        tracker.record_observation_at("latency", 100.0, now);
        // Below target: not a violation.
        tracker.record_observation_at("latency", 50.0, now);
        // Above target: violation.
        tracker.record_observation_at("latency", 150.0, now);

        let snap = tracker.snapshot_at(now);
        let budget = &snap.budgets["latency"];
        assert_eq!(budget.total_requests, 3);
        assert_eq!(budget.violations, 1);
    }

    #[test]
    fn violation_detection_greater_than() {
        let tracker = SloTracker::with_targets(vec![gt_target("avail", 99.0, 99.9)]);
        let now = Instant::now();

        // Above target: not a violation.
        tracker.record_observation_at("avail", 100.0, now);
        // Exactly at target: not a violation for GreaterThan.
        tracker.record_observation_at("avail", 99.0, now);
        // Below target: violation.
        tracker.record_observation_at("avail", 98.0, now);

        let snap = tracker.snapshot_at(now);
        let budget = &snap.budgets["avail"];
        assert_eq!(budget.total_requests, 3);
        assert_eq!(budget.violations, 1);
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
        // 99% SLO with target_value 100.0
        let tracker = SloTracker::with_targets(vec![lt_target("test_latency", 100.0, 99.0)]);
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
        // 20% error rate against 1% allowed => budget exhausted
        assert_eq!(budget.budget_remaining, 0.0);
    }

    #[test]
    fn tracker_record_availability() {
        // 99.9% SLO, target_value 99.0 (GreaterThan)
        let tracker = SloTracker::with_targets(vec![gt_target("test_availability", 99.0, 99.9)]);
        let now = Instant::now();

        // 9 above threshold, 1 below
        for _ in 0..9 {
            tracker.record_observation_at("test_availability", 100.0, now);
        }
        tracker.record_observation_at("test_availability", 50.0, now);

        let snap = tracker.snapshot_at(now);
        let budget = &snap.budgets["test_availability"];
        assert_eq!(budget.total_requests, 10);
        assert_eq!(budget.violations, 1);
    }

    #[test]
    fn tracker_window_expiry() {
        let mut target = lt_target("test_latency", 100.0, 99.0);
        target.window_secs = 2;
        let tracker = SloTracker::with_targets(vec![target]);
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
        assert_eq!(budget.budget_remaining, 100.0);
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
        assert!(json.contains("budget_remaining"));
    }

    #[test]
    fn slo_budget_serialization_includes_derived_fields() {
        let budget = SloBudget::from_counts(lt_target("test", 100.0, 99.0), 10, 2);
        let json = serde_json::to_string(&budget).unwrap();
        assert!(json.contains("\"total_requests\":10"));
        assert!(json.contains("\"violations\":2"));
        assert!(json.contains("\"target_value\":100.0"));
        assert!(json.contains("\"budget_remaining\":"));
        assert!(json.contains("\"is_warning\":"));
        assert!(json.contains("\"is_critical\":"));
        assert!(json.contains("\"kind\":"));
        assert!(json.contains("\"target_percentage\":"));
    }
}
