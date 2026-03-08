use std::collections::{HashMap, HashSet, VecDeque};

use serde::Serialize;

use super::CompactionConfig;

/// Default sliding window duration for write rate tracking (60 seconds).
const DEFAULT_WINDOW_SECS: u64 = 60;

/// Default tuning interval in milliseconds (30 seconds).
const DEFAULT_TUNING_INTERVAL_MS: u64 = 30_000;

/// Default maximum checkpoint history per key range.
const DEFAULT_MAX_CHECKPOINT_HISTORY: usize = 10;

/// Minimum span (in milliseconds) required for a meaningful rate calculation.
///
/// If the actual time span between the oldest in-window entry and `now` is
/// less than this value, `ops_per_sec` returns 0.0 to avoid wildly inflated
/// rates from sub-second bursts.
const MIN_RATE_SPAN_MS: u64 = 1_000;

/// Tracks write operations in a sliding window for a single key range prefix.
#[derive(Debug, Clone)]
struct WriteRateBucket {
    /// (timestamp_ms, ops_count) entries within the window.
    entries: VecDeque<(u64, u64)>,
}

impl WriteRateBucket {
    fn new() -> Self {
        Self {
            entries: VecDeque::new(),
        }
    }

    /// Record `count` operations at the given timestamp (milliseconds).
    ///
    /// If the last entry has the same timestamp, the count is merged to avoid
    /// unbounded growth when called per-operation (e.g. via `record_op_at`).
    fn record(&mut self, timestamp_ms: u64, count: u64, window_ms: u64) {
        self.evict_expired(timestamp_ms, window_ms);
        if let Some((ts, existing)) = self
            .entries
            .back_mut()
            .filter(|(ts, _)| *ts == timestamp_ms)
        {
            *existing += count;
            let _ = ts;
            return;
        }
        self.entries.push_back((timestamp_ms, count));
    }

    /// Remove entries older than the window.
    fn evict_expired(&mut self, now_ms: u64, window_ms: u64) {
        let cutoff = now_ms.saturating_sub(window_ms);
        while let Some((ts, _)) = self.entries.front() {
            if *ts < cutoff {
                self.entries.pop_front();
            } else {
                break;
            }
        }
    }

    /// Compute the current write rate in ops/sec within the window.
    fn ops_per_sec(&self, now_ms: u64, window_ms: u64) -> f64 {
        let cutoff = now_ms.saturating_sub(window_ms);
        let total_ops: u64 = self
            .entries
            .iter()
            .filter(|(ts, _)| *ts >= cutoff)
            .map(|(_, count)| count)
            .sum();

        // Use the actual time span covered, capped by window duration.
        let oldest = self
            .entries
            .iter()
            .find(|(ts, _)| *ts >= cutoff)
            .map(|(ts, _)| *ts);

        match oldest {
            Some(oldest_ts) => {
                let span_ms = now_ms.saturating_sub(oldest_ts);
                // Avoid inflated rates from very short spans (e.g. single
                // burst within 1 ms).  Return 0.0 until enough time has
                // elapsed for a meaningful measurement.
                if span_ms < MIN_RATE_SPAN_MS {
                    return 0.0;
                }
                let span_secs = span_ms as f64 / 1000.0;
                total_ops as f64 / span_secs
            }
            None => 0.0,
        }
    }
}

/// Tracks write rates across all key range prefixes using a sliding window.
#[derive(Debug, Clone)]
pub struct WriteRateTracker {
    buckets: HashMap<String, WriteRateBucket>,
    window_ms: u64,
}

impl WriteRateTracker {
    /// Create a new tracker with the default 60-second window.
    pub fn new() -> Self {
        Self {
            buckets: HashMap::new(),
            window_ms: DEFAULT_WINDOW_SECS * 1000,
        }
    }

    /// Create a tracker with a custom window duration in milliseconds.
    pub fn with_window_ms(window_ms: u64) -> Self {
        Self {
            buckets: HashMap::new(),
            window_ms,
        }
    }

    /// Record `count` operations for a key range prefix at the given timestamp (ms).
    pub fn record_ops(&mut self, prefix: &str, timestamp_ms: u64, count: u64) {
        let bucket = self
            .buckets
            .entry(prefix.to_string())
            .or_insert_with(WriteRateBucket::new);
        bucket.record(timestamp_ms, count, self.window_ms);
    }

    /// Query the current write rate (ops/sec) for a key range prefix.
    pub fn write_rate(&self, prefix: &str, now_ms: u64) -> f64 {
        match self.buckets.get(prefix) {
            Some(bucket) => bucket.ops_per_sec(now_ms, self.window_ms),
            None => 0.0,
        }
    }
}

impl Default for WriteRateTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Point-in-time snapshot of the adaptive tuning state for diagnostics.
#[derive(Debug, Clone, Serialize)]
pub struct TuningSnapshot {
    /// Current effective time threshold in milliseconds.
    pub effective_time_threshold_ms: u64,
    /// Current effective operations threshold.
    pub effective_ops_threshold: u64,
    /// Maximum checkpoint history per key range.
    pub max_checkpoint_history: usize,
    /// Per-prefix write rates (ops/sec).
    pub write_rates: HashMap<String, f64>,
    /// Pinned prefixes exempt from auto-tuning.
    pub pinned_prefixes: Vec<String>,
    /// Timestamp of last tuning evaluation (ms since epoch).
    pub last_tuning_ms: u64,
}

/// Adaptive compaction configuration that wraps a base `CompactionConfig`
/// and adjusts thresholds based on runtime workload signals.
#[derive(Debug, Clone)]
pub struct AdaptiveCompactionConfig {
    /// The base (initial) configuration.
    base: CompactionConfig,
    /// Current effective configuration after tuning adjustments.
    effective: CompactionConfig,
    /// Write rate tracker for all key range prefixes.
    write_rate_tracker: WriteRateTracker,
    /// Key range prefixes exempt from auto-tuning.
    pinned: HashSet<String>,
    /// Tuning re-evaluation interval in milliseconds.
    tuning_interval_ms: u64,
    /// Timestamp (ms) of the last tuning evaluation.
    last_tuning_ms: u64,
    /// Maximum number of checkpoints to retain per key range.
    pub max_checkpoint_history: usize,
}

impl AdaptiveCompactionConfig {
    /// Create a new adaptive config wrapping the given base config.
    pub fn new(base: CompactionConfig) -> Self {
        let effective = base.clone();
        Self {
            base,
            effective,
            write_rate_tracker: WriteRateTracker::new(),
            pinned: HashSet::new(),
            tuning_interval_ms: DEFAULT_TUNING_INTERVAL_MS,
            last_tuning_ms: 0,
            max_checkpoint_history: DEFAULT_MAX_CHECKPOINT_HISTORY,
        }
    }

    /// Create with a custom write rate window (in milliseconds).
    pub fn with_write_rate_window(base: CompactionConfig, window_ms: u64) -> Self {
        let effective = base.clone();
        Self {
            base,
            effective,
            write_rate_tracker: WriteRateTracker::with_window_ms(window_ms),
            pinned: HashSet::new(),
            tuning_interval_ms: DEFAULT_TUNING_INTERVAL_MS,
            last_tuning_ms: 0,
            max_checkpoint_history: DEFAULT_MAX_CHECKPOINT_HISTORY,
        }
    }

    /// Return the current effective compaction configuration.
    pub fn effective(&self) -> &CompactionConfig {
        &self.effective
    }

    /// Return the base (initial) compaction configuration.
    pub fn base(&self) -> &CompactionConfig {
        &self.base
    }

    /// Add a key range prefix to the pinned set (exempt from auto-tuning).
    pub fn pin_prefix(&mut self, prefix: &str) {
        self.pinned.insert(prefix.to_string());
    }

    /// Remove a key range prefix from the pinned set.
    pub fn unpin_prefix(&mut self, prefix: &str) {
        self.pinned.remove(prefix);
    }

    /// Check if a prefix is pinned.
    pub fn is_pinned(&self, prefix: &str) -> bool {
        self.pinned.contains(prefix)
    }

    /// Record write operations for a key range prefix.
    pub fn record_ops(&mut self, prefix: &str, timestamp_ms: u64, count: u64) {
        self.write_rate_tracker
            .record_ops(prefix, timestamp_ms, count);
    }

    /// Query write rate for a specific prefix.
    pub fn write_rate(&self, prefix: &str, now_ms: u64) -> f64 {
        self.write_rate_tracker.write_rate(prefix, now_ms)
    }

    /// Return a reference to the write rate tracker.
    pub fn write_rate_tracker(&self) -> &WriteRateTracker {
        &self.write_rate_tracker
    }

    /// Set a custom tuning interval in milliseconds.
    pub fn set_tuning_interval_ms(&mut self, interval_ms: u64) {
        self.tuning_interval_ms = interval_ms;
    }

    /// Evaluate and adjust thresholds based on current workload metrics.
    ///
    /// Parameters:
    /// - `now_ms`: current timestamp in milliseconds
    /// - `avg_frontier_lag_ms`: average Authority frontier lag in milliseconds
    ///   (pass `None` if lag data is unavailable)
    ///
    /// Returns `true` if thresholds were adjusted.
    pub fn tune(&mut self, now_ms: u64, avg_frontier_lag_ms: Option<u64>) -> bool {
        // Only re-evaluate at the tuning interval.
        if now_ms.saturating_sub(self.last_tuning_ms) < self.tuning_interval_ms {
            return false;
        }
        self.last_tuning_ms = now_ms;

        // If all prefixes are pinned, skip tuning entirely.
        let all_pinned = !self.write_rate_tracker.buckets.is_empty()
            && self
                .write_rate_tracker
                .buckets
                .keys()
                .all(|k| self.pinned.contains(k));
        if all_pinned {
            return false;
        }

        let mut changed = false;

        // Compute aggregate write rate across all non-pinned prefixes.
        let aggregate_rate: f64 = self
            .write_rate_tracker
            .buckets
            .keys()
            .filter(|k| !self.pinned.contains(k.as_str()))
            .map(|k| self.write_rate_tracker.write_rate(k, now_ms))
            .sum();

        // Adjust ops_threshold based on write rate.
        // Dead zone: only adjust when rate is well outside the band (>750 or <30)
        // to prevent oscillation at boundaries.
        let new_ops = if aggregate_rate > 750.0 {
            // High write rate: halve ops threshold (min 1,000).
            let halved = self.effective.ops_threshold / 2;
            halved.max(1_000)
        } else if aggregate_rate < 30.0 {
            // Low write rate: double ops threshold (max 50,000).
            let doubled = self.effective.ops_threshold.saturating_mul(2);
            doubled.min(50_000)
        } else {
            self.effective.ops_threshold
        };

        if new_ops != self.effective.ops_threshold {
            self.effective.ops_threshold = new_ops;
            changed = true;
        }

        // Adjust time_threshold based on frontier lag.
        // Dead zone: only adjust when lag is well outside the band (>15s or <1s)
        // to prevent oscillation at boundaries.
        if let Some(lag_ms) = avg_frontier_lag_ms {
            let new_time = if lag_ms > 15_000 {
                // High lag: increase time threshold by 50% (max 120s).
                let increased =
                    self.effective.time_threshold_ms + self.effective.time_threshold_ms / 2;
                increased.min(120_000)
            } else if lag_ms < 1_000 {
                // Low lag: decrease time threshold by 25% (min 10s).
                let decreased =
                    self.effective.time_threshold_ms - self.effective.time_threshold_ms / 4;
                decreased.max(10_000)
            } else {
                self.effective.time_threshold_ms
            };

            if new_time != self.effective.time_threshold_ms {
                self.effective.time_threshold_ms = new_time;
                changed = true;
            }
        }

        changed
    }

    /// Produce a diagnostic snapshot of the current tuning state.
    pub fn tuning_snapshot(&self, now_ms: u64) -> TuningSnapshot {
        let write_rates: HashMap<String, f64> = self
            .write_rate_tracker
            .buckets
            .keys()
            .map(|k| (k.clone(), self.write_rate_tracker.write_rate(k, now_ms)))
            .collect();

        TuningSnapshot {
            effective_time_threshold_ms: self.effective.time_threshold_ms,
            effective_ops_threshold: self.effective.ops_threshold,
            max_checkpoint_history: self.max_checkpoint_history,
            write_rates,
            pinned_prefixes: self.pinned.iter().cloned().collect(),
            last_tuning_ms: self.last_tuning_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------------------------------------------------------------
    // WriteRateTracker tests
    // ---------------------------------------------------------------

    #[test]
    fn write_rate_empty_tracker() {
        let tracker = WriteRateTracker::new();
        assert_eq!(tracker.write_rate("user/", 10_000), 0.0);
    }

    #[test]
    fn write_rate_single_entry() {
        let mut tracker = WriteRateTracker::with_window_ms(10_000);
        // Record 100 ops at t=5000
        tracker.record_ops("user/", 5_000, 100);

        // At t=6000 (1 second after the entry), rate = 100 / 1.0 = 100 ops/sec
        let rate = tracker.write_rate("user/", 6_000);
        assert!((rate - 100.0).abs() < 1.0, "rate was {rate}");
    }

    #[test]
    fn write_rate_multiple_entries() {
        let mut tracker = WriteRateTracker::with_window_ms(10_000);
        // Record 100 ops at t=1000, 200 ops at t=3000
        tracker.record_ops("user/", 1_000, 100);
        tracker.record_ops("user/", 3_000, 200);

        // At t=5000: total=300, span=5000-1000=4000ms=4s, rate=300/4=75
        let rate = tracker.write_rate("user/", 5_000);
        assert!((rate - 75.0).abs() < 1.0, "rate was {rate}");
    }

    #[test]
    fn write_rate_window_expiry() {
        let mut tracker = WriteRateTracker::with_window_ms(5_000);
        tracker.record_ops("user/", 1_000, 100);
        tracker.record_ops("user/", 3_000, 200);

        // At t=8000: window is [3000, 8000], only 200 ops remain
        // span = 8000-3000=5000ms=5s, rate=200/5=40
        let rate = tracker.write_rate("user/", 8_000);
        assert!((rate - 40.0).abs() < 1.0, "rate was {rate}");
    }

    #[test]
    fn write_rate_independent_prefixes() {
        let mut tracker = WriteRateTracker::with_window_ms(10_000);
        tracker.record_ops("user/", 1_000, 100);
        tracker.record_ops("order/", 1_000, 500);

        let user_rate = tracker.write_rate("user/", 2_000);
        let order_rate = tracker.write_rate("order/", 2_000);

        assert!((user_rate - 100.0).abs() < 1.0);
        assert!((order_rate - 500.0).abs() < 1.0);
    }

    // ---------------------------------------------------------------
    // AdaptiveCompactionConfig threshold adaptation tests
    // ---------------------------------------------------------------

    #[test]
    fn tune_high_write_rate_halves_ops_threshold() {
        let base = CompactionConfig {
            time_threshold_ms: 30_000,
            ops_threshold: 10_000,
        };
        let mut adaptive = AdaptiveCompactionConfig::with_write_rate_window(base, 10_000);
        adaptive.set_tuning_interval_ms(0); // Tune on every call.

        // Simulate high write rate: 800 ops in 1 second > 750 ops/sec (dead zone boundary)
        adaptive.record_ops("user/", 1_000, 800);

        let changed = adaptive.tune(2_000, None);
        assert!(changed);
        assert_eq!(adaptive.effective().ops_threshold, 5_000);
    }

    #[test]
    fn tune_high_write_rate_respects_minimum() {
        let base = CompactionConfig {
            time_threshold_ms: 30_000,
            ops_threshold: 1_500,
        };
        let mut adaptive = AdaptiveCompactionConfig::with_write_rate_window(base, 10_000);
        adaptive.set_tuning_interval_ms(0);

        adaptive.record_ops("user/", 1_000, 800);

        // First tune: 1500 / 2 = 750, but min is 1000
        let changed = adaptive.tune(2_000, None);
        assert!(changed);
        assert_eq!(adaptive.effective().ops_threshold, 1_000);

        // Second tune: already at 1000, halving would give 500, clamped to 1000
        adaptive.record_ops("user/", 3_000, 800);
        let changed = adaptive.tune(4_000, None);
        assert!(!changed); // No change since already at minimum
    }

    #[test]
    fn tune_low_write_rate_doubles_ops_threshold() {
        let base = CompactionConfig {
            time_threshold_ms: 30_000,
            ops_threshold: 10_000,
        };
        let mut adaptive = AdaptiveCompactionConfig::with_write_rate_window(base, 60_000);
        adaptive.set_tuning_interval_ms(0);

        // Simulate low write rate: 10 ops in 1 second = 10 ops/sec < 50
        adaptive.record_ops("user/", 1_000, 10);

        let changed = adaptive.tune(2_000, None);
        assert!(changed);
        assert_eq!(adaptive.effective().ops_threshold, 20_000);
    }

    #[test]
    fn tune_low_write_rate_respects_maximum() {
        let base = CompactionConfig {
            time_threshold_ms: 30_000,
            ops_threshold: 30_000,
        };
        let mut adaptive = AdaptiveCompactionConfig::with_write_rate_window(base, 60_000);
        adaptive.set_tuning_interval_ms(0);

        adaptive.record_ops("user/", 1_000, 10);

        // 30000 * 2 = 60000, clamped to 50000
        let changed = adaptive.tune(2_000, None);
        assert!(changed);
        assert_eq!(adaptive.effective().ops_threshold, 50_000);
    }

    #[test]
    fn tune_high_frontier_lag_increases_time_threshold() {
        let base = CompactionConfig {
            time_threshold_ms: 30_000,
            ops_threshold: 10_000,
        };
        let mut adaptive = AdaptiveCompactionConfig::new(base);
        adaptive.set_tuning_interval_ms(0);

        // avg lag > 15s (dead zone boundary)
        let changed = adaptive.tune(1_000, Some(16_000));
        assert!(changed);
        // 30000 + 15000 = 45000
        assert_eq!(adaptive.effective().time_threshold_ms, 45_000);
    }

    #[test]
    fn tune_high_frontier_lag_respects_maximum() {
        let base = CompactionConfig {
            time_threshold_ms: 100_000,
            ops_threshold: 10_000,
        };
        let mut adaptive = AdaptiveCompactionConfig::new(base);
        adaptive.set_tuning_interval_ms(0);

        // 100000 + 50000 = 150000, clamped to 120000
        let changed = adaptive.tune(1_000, Some(16_000));
        assert!(changed);
        assert_eq!(adaptive.effective().time_threshold_ms, 120_000);
    }

    #[test]
    fn tune_low_frontier_lag_decreases_time_threshold() {
        let base = CompactionConfig {
            time_threshold_ms: 30_000,
            ops_threshold: 10_000,
        };
        let mut adaptive = AdaptiveCompactionConfig::new(base);
        adaptive.set_tuning_interval_ms(0);

        // avg lag < 1s (dead zone boundary)
        let changed = adaptive.tune(1_000, Some(900));
        assert!(changed);
        // 30000 - 7500 = 22500
        assert_eq!(adaptive.effective().time_threshold_ms, 22_500);
    }

    #[test]
    fn tune_low_frontier_lag_respects_minimum() {
        let base = CompactionConfig {
            time_threshold_ms: 12_000,
            ops_threshold: 10_000,
        };
        let mut adaptive = AdaptiveCompactionConfig::new(base);
        adaptive.set_tuning_interval_ms(0);

        // 12000 - 3000 = 9000, clamped to 10000
        let changed = adaptive.tune(1_000, Some(500));
        assert!(changed);
        assert_eq!(adaptive.effective().time_threshold_ms, 10_000);
    }

    #[test]
    fn tune_respects_tuning_interval() {
        let base = CompactionConfig {
            time_threshold_ms: 30_000,
            ops_threshold: 10_000,
        };
        let mut adaptive = AdaptiveCompactionConfig::new(base);
        // Default 30s interval

        adaptive.record_ops("user/", 1_000, 800);

        // First tune at t=0 should work (last_tuning_ms starts at 0).
        let changed = adaptive.tune(31_000, None);
        assert!(changed);

        // Second tune at t=31001 should be skipped (< 30s since last).
        adaptive.record_ops("user/", 31_001, 800);
        let changed = adaptive.tune(31_001, None);
        assert!(!changed);
    }

    #[test]
    fn tune_pinned_prefix_excluded() {
        let base = CompactionConfig {
            time_threshold_ms: 30_000,
            ops_threshold: 10_000,
        };
        let mut adaptive = AdaptiveCompactionConfig::with_write_rate_window(base, 10_000);
        adaptive.set_tuning_interval_ms(0);

        // Pin the only prefix — should skip tuning.
        adaptive.pin_prefix("user/");
        adaptive.record_ops("user/", 1_000, 800);

        let changed = adaptive.tune(2_000, None);
        assert!(!changed);
        // Threshold unchanged.
        assert_eq!(adaptive.effective().ops_threshold, 10_000);
    }

    #[test]
    fn tune_pinned_does_not_affect_unpinned() {
        let base = CompactionConfig {
            time_threshold_ms: 30_000,
            ops_threshold: 10_000,
        };
        let mut adaptive = AdaptiveCompactionConfig::with_write_rate_window(base, 10_000);
        adaptive.set_tuning_interval_ms(0);

        adaptive.pin_prefix("system/");
        // system/ has high rate but is pinned; user/ has high rate and is not pinned
        adaptive.record_ops("system/", 1_000, 1_000);
        adaptive.record_ops("user/", 1_000, 800);

        let changed = adaptive.tune(2_000, None);
        assert!(changed);
        // Only user/ rate (800 ops/sec > 750) drives halving.
        assert_eq!(adaptive.effective().ops_threshold, 5_000);
    }

    #[test]
    fn tuning_snapshot_includes_current_state() {
        let base = CompactionConfig {
            time_threshold_ms: 30_000,
            ops_threshold: 10_000,
        };
        let mut adaptive = AdaptiveCompactionConfig::with_write_rate_window(base, 10_000);
        adaptive.pin_prefix("system/");
        adaptive.record_ops("user/", 1_000, 100);

        let snap = adaptive.tuning_snapshot(2_000);
        assert_eq!(snap.effective_time_threshold_ms, 30_000);
        assert_eq!(snap.effective_ops_threshold, 10_000);
        assert_eq!(snap.max_checkpoint_history, 10);
        assert!(snap.write_rates.contains_key("user/"));
        assert!(snap.pinned_prefixes.contains(&"system/".to_string()));
    }

    #[test]
    fn unpin_prefix_re_enables_tuning() {
        let base = CompactionConfig {
            time_threshold_ms: 30_000,
            ops_threshold: 10_000,
        };
        let mut adaptive = AdaptiveCompactionConfig::with_write_rate_window(base, 10_000);
        adaptive.set_tuning_interval_ms(0);

        adaptive.pin_prefix("user/");
        adaptive.record_ops("user/", 1_000, 800);

        // Pinned — no tuning.
        assert!(!adaptive.tune(2_000, None));
        assert_eq!(adaptive.effective().ops_threshold, 10_000);

        // Unpin and record fresh high-rate ops, then tune.
        adaptive.unpin_prefix("user/");
        adaptive.record_ops("user/", 3_000, 800);
        // Rate at t=3100: entries at 1000 (800) and 3000 (800) => 1600 ops over 2.1s = 762 > 750
        assert!(adaptive.tune(3_100, None));
        assert_eq!(adaptive.effective().ops_threshold, 5_000);
    }

    // ---------------------------------------------------------------
    // Dead zone hysteresis tests
    // ---------------------------------------------------------------

    #[test]
    fn dead_zone_ops_rate_in_band_no_adjustment() {
        // Rates between 30 and 750 ops/sec should NOT trigger any adjustment.
        let base = CompactionConfig {
            time_threshold_ms: 30_000,
            ops_threshold: 10_000,
        };
        let mut adaptive = AdaptiveCompactionConfig::with_write_rate_window(base, 10_000);
        adaptive.set_tuning_interval_ms(0);

        // 500 ops in 1 second = 500 ops/sec — inside the dead zone [30, 750]
        adaptive.record_ops("user/", 1_000, 500);
        let changed = adaptive.tune(2_000, None);
        assert!(!changed);
        assert_eq!(adaptive.effective().ops_threshold, 10_000);
    }

    #[test]
    fn dead_zone_prevents_oscillation_at_old_boundary() {
        // At 500 ops/sec (the old threshold), the system should NOT oscillate.
        let base = CompactionConfig {
            time_threshold_ms: 30_000,
            ops_threshold: 10_000,
        };
        let mut adaptive = AdaptiveCompactionConfig::with_write_rate_window(base, 10_000);
        adaptive.set_tuning_interval_ms(0);

        // Repeatedly tune at exactly 500 ops/sec — no adjustment should happen.
        for i in 0..5 {
            let t_base = 1_000 + i * 2_000;
            adaptive.record_ops("user/", t_base, 500);
            let changed = adaptive.tune(t_base + 1_000, None);
            assert!(!changed, "iteration {i} should not change threshold");
        }
        assert_eq!(adaptive.effective().ops_threshold, 10_000);
    }

    #[test]
    fn dead_zone_lag_in_band_no_adjustment() {
        // Lag between 1s and 15s should NOT trigger any time threshold adjustment.
        let base = CompactionConfig {
            time_threshold_ms: 30_000,
            ops_threshold: 10_000,
        };
        let mut adaptive = AdaptiveCompactionConfig::with_write_rate_window(base, 60_000);
        adaptive.set_tuning_interval_ms(0);

        // Record moderate write rate (inside ops dead zone) with >= 1s span
        // so the rate is not suppressed by MIN_RATE_SPAN_MS.
        adaptive.record_ops("user/", 1_000, 100);

        // Lag at 5s — inside the dead zone [1000, 15000]
        let changed = adaptive.tune(2_000, Some(5_000));
        assert!(!changed);
        assert_eq!(adaptive.effective().time_threshold_ms, 30_000);
    }

    #[test]
    fn dead_zone_lag_at_old_boundary_no_adjustment() {
        // At lag=2000ms (old boundary), no adjustment should occur.
        let base = CompactionConfig {
            time_threshold_ms: 30_000,
            ops_threshold: 10_000,
        };
        let mut adaptive = AdaptiveCompactionConfig::with_write_rate_window(base, 60_000);
        adaptive.set_tuning_interval_ms(0);

        // Record moderate write rate (inside ops dead zone) with >= 1s span
        // so the rate is not suppressed by MIN_RATE_SPAN_MS.
        adaptive.record_ops("user/", 1_000, 100);

        let changed = adaptive.tune(2_000, Some(2_000));
        assert!(!changed);
        assert_eq!(adaptive.effective().time_threshold_ms, 30_000);

        // Also at lag=10000 (old high boundary)
        // Record enough ops to keep rate in the dead zone (between 30 and 750).
        // Window is 60s, entries at (1000,100) and (31500,2000).
        // At t=32000: total=2100, span=31.0s, rate=67.7 ops/sec — in dead zone.
        adaptive.record_ops("user/", 31_500, 2_000);
        let changed = adaptive.tune(32_000, Some(10_000));
        assert!(!changed);
        assert_eq!(adaptive.effective().time_threshold_ms, 30_000);
    }
}
