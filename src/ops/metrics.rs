use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Aggregated benchmark result for a single measurement.
///
/// Captures latency statistics (mean, percentiles, min, max) for a named
/// benchmark across a given number of iterations. All latency values are
/// reported in microseconds.
#[derive(Debug, Clone, Serialize)]
pub struct BenchmarkResult {
    /// Human-readable name of the benchmark.
    pub name: String,
    /// Number of iterations measured.
    pub iterations: usize,
    /// Mean latency in microseconds.
    pub mean_us: f64,
    /// Median (50th percentile) latency in microseconds.
    pub p50_us: f64,
    /// 95th percentile latency in microseconds.
    pub p95_us: f64,
    /// 99th percentile latency in microseconds.
    pub p99_us: f64,
    /// Minimum observed latency in microseconds.
    pub min_us: f64,
    /// Maximum observed latency in microseconds.
    pub max_us: f64,
}

/// Compute latency statistics from a slice of [`Duration`] measurements.
///
/// Returns a [`BenchmarkResult`] with the given name, populated with
/// mean, p50, p95, p99, min, and max latencies in microseconds.
///
/// # Panics
///
/// Panics if `durations` is empty.
pub fn collect_latencies(name: &str, durations: &[Duration]) -> BenchmarkResult {
    assert!(!durations.is_empty(), "durations must not be empty");

    let mut us_values: Vec<f64> = durations
        .iter()
        .map(|d| d.as_secs_f64() * 1_000_000.0)
        .collect();
    us_values.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let n = us_values.len();
    let sum: f64 = us_values.iter().sum();
    let mean = sum / n as f64;

    BenchmarkResult {
        name: name.to_string(),
        iterations: n,
        mean_us: mean,
        p50_us: percentile(&us_values, 50.0),
        p95_us: percentile(&us_values, 95.0),
        p99_us: percentile(&us_values, 99.0),
        min_us: us_values[0],
        max_us: us_values[n - 1],
    }
}

/// Compute the value at a given percentile from a **sorted** slice.
///
/// Uses nearest-rank interpolation.
fn percentile(sorted: &[f64], pct: f64) -> f64 {
    let idx = ((pct / 100.0) * sorted.len() as f64).ceil() as usize;
    let idx = idx.min(sorted.len()).saturating_sub(1);
    sorted[idx]
}

/// Format a [`BenchmarkResult`] as a single-line CSV record.
///
/// Header: `name,iterations,mean_us,p50_us,p95_us,p99_us,min_us,max_us`
pub fn to_csv_row(result: &BenchmarkResult) -> String {
    format!(
        "{},{},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2}",
        result.name,
        result.iterations,
        result.mean_us,
        result.p50_us,
        result.p95_us,
        result.p99_us,
        result.min_us,
        result.max_us,
    )
}

/// Return the CSV header line matching [`to_csv_row`] output.
pub fn csv_header() -> &'static str {
    "name,iterations,mean_us,p50_us,p95_us,p99_us,min_us,max_us"
}

/// Runtime metrics for operational monitoring.
///
/// All counters use [`AtomicU64`] for lock-free concurrent access.
/// Shared via `Arc<RuntimeMetrics>` between [`NodeRunner`](crate::runtime::NodeRunner)
/// and HTTP handlers.
#[derive(Debug, Default)]
pub struct RuntimeMetrics {
    /// Current number of pending certification writes.
    pub pending_count: AtomicU64,

    /// Cumulative certified write count.
    pub certified_total: AtomicU64,

    /// Sum of certification latencies in microseconds.
    pub certification_latency_sum_us: AtomicU64,

    /// Number of certification latency samples.
    pub certification_latency_count: AtomicU64,

    /// Maximum frontier skew in milliseconds across authority scopes.
    pub frontier_skew_ms: AtomicU64,

    /// Cumulative sync failure count.
    pub sync_failure_total: AtomicU64,

    /// Cumulative sync attempt count.
    pub sync_attempt_total: AtomicU64,

    /// Cumulative count of delta-fail -> full-sync fallback events.
    pub sync_fallback_total: AtomicU64,
}

impl RuntimeMetrics {
    /// Get the mean certification latency in microseconds.
    pub fn mean_certification_latency_us(&self) -> f64 {
        let count = self.certification_latency_count.load(Ordering::Relaxed);
        if count == 0 {
            return 0.0;
        }
        let sum = self.certification_latency_sum_us.load(Ordering::Relaxed);
        sum as f64 / count as f64
    }

    /// Get the sync failure rate (0.0 to 1.0).
    pub fn sync_failure_rate(&self) -> f64 {
        let attempts = self.sync_attempt_total.load(Ordering::Relaxed);
        if attempts == 0 {
            return 0.0;
        }
        let failures = self.sync_failure_total.load(Ordering::Relaxed);
        failures as f64 / attempts as f64
    }

    /// Create a snapshot for JSON serialization.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            pending_count: self.pending_count.load(Ordering::Relaxed),
            certified_total: self.certified_total.load(Ordering::Relaxed),
            certification_latency_mean_us: self.mean_certification_latency_us(),
            frontier_skew_ms: self.frontier_skew_ms.load(Ordering::Relaxed),
            sync_failure_rate: self.sync_failure_rate(),
            sync_attempt_total: self.sync_attempt_total.load(Ordering::Relaxed),
            sync_failure_total: self.sync_failure_total.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time snapshot of runtime metrics for JSON serialization.
#[derive(Debug, Clone, Serialize)]
pub struct MetricsSnapshot {
    /// Current number of pending certification writes.
    pub pending_count: u64,
    /// Cumulative certified write count.
    pub certified_total: u64,
    /// Mean certification latency in microseconds.
    pub certification_latency_mean_us: f64,
    /// Maximum frontier skew in milliseconds across authority scopes.
    pub frontier_skew_ms: u64,
    /// Sync failure rate (0.0 to 1.0).
    pub sync_failure_rate: f64,
    /// Cumulative sync attempt count.
    pub sync_attempt_total: u64,
    /// Cumulative sync failure count.
    pub sync_failure_total: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collect_single_duration() {
        let durations = vec![Duration::from_micros(100)];
        let result = collect_latencies("single", &durations);
        assert_eq!(result.iterations, 1);
        assert!((result.mean_us - 100.0).abs() < 1.0);
        assert!((result.p50_us - 100.0).abs() < 1.0);
        assert!((result.min_us - 100.0).abs() < 1.0);
        assert!((result.max_us - 100.0).abs() < 1.0);
    }

    #[test]
    fn collect_multiple_durations() {
        let durations: Vec<Duration> = (1..=100).map(Duration::from_micros).collect();
        let result = collect_latencies("range", &durations);
        assert_eq!(result.iterations, 100);
        assert!((result.mean_us - 50.5).abs() < 1.0);
        assert!((result.min_us - 1.0).abs() < 1.0);
        assert!((result.max_us - 100.0).abs() < 1.0);
        // p50 should be around 50
        assert!(result.p50_us >= 49.0 && result.p50_us <= 51.0);
        // p95 should be around 95
        assert!(result.p95_us >= 94.0 && result.p95_us <= 96.0);
        // p99 should be around 99
        assert!(result.p99_us >= 98.0 && result.p99_us <= 100.0);
    }

    #[test]
    fn csv_output_format() {
        let result = BenchmarkResult {
            name: "test".to_string(),
            iterations: 10,
            mean_us: 100.0,
            p50_us: 95.0,
            p95_us: 150.0,
            p99_us: 200.0,
            min_us: 50.0,
            max_us: 250.0,
        };
        let csv = to_csv_row(&result);
        assert_eq!(csv, "test,10,100.00,95.00,150.00,200.00,50.00,250.00");
    }

    #[test]
    fn csv_header_matches_row_fields() {
        let header = csv_header();
        let fields: Vec<&str> = header.split(',').collect();
        assert_eq!(fields.len(), 8);
        assert_eq!(fields[0], "name");
        assert_eq!(fields[7], "max_us");
    }

    #[test]
    fn json_serialization() {
        let result = BenchmarkResult {
            name: "bench".to_string(),
            iterations: 5,
            mean_us: 42.0,
            p50_us: 40.0,
            p95_us: 50.0,
            p99_us: 55.0,
            min_us: 30.0,
            max_us: 60.0,
        };
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("\"name\":\"bench\""));
        assert!(json.contains("\"iterations\":5"));
    }

    #[test]
    #[should_panic(expected = "durations must not be empty")]
    fn collect_empty_panics() {
        collect_latencies("empty", &[]);
    }

    // ---------------------------------------------------------------
    // RuntimeMetrics unit tests
    // ---------------------------------------------------------------

    #[test]
    fn runtime_metrics_default_all_zeros() {
        let m = RuntimeMetrics::default();
        assert_eq!(m.pending_count.load(Ordering::Relaxed), 0);
        assert_eq!(m.certified_total.load(Ordering::Relaxed), 0);
        assert_eq!(m.certification_latency_sum_us.load(Ordering::Relaxed), 0);
        assert_eq!(m.certification_latency_count.load(Ordering::Relaxed), 0);
        assert_eq!(m.frontier_skew_ms.load(Ordering::Relaxed), 0);
        assert_eq!(m.sync_failure_total.load(Ordering::Relaxed), 0);
        assert_eq!(m.sync_attempt_total.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn mean_certification_latency_zero_when_no_samples() {
        let m = RuntimeMetrics::default();
        assert_eq!(m.mean_certification_latency_us(), 0.0);
    }

    #[test]
    fn mean_certification_latency_computed_correctly() {
        let m = RuntimeMetrics::default();
        m.certification_latency_sum_us
            .store(3000, Ordering::Relaxed);
        m.certification_latency_count.store(3, Ordering::Relaxed);
        assert!((m.mean_certification_latency_us() - 1000.0).abs() < f64::EPSILON);
    }

    #[test]
    fn sync_failure_rate_zero_when_no_attempts() {
        let m = RuntimeMetrics::default();
        assert_eq!(m.sync_failure_rate(), 0.0);
    }

    #[test]
    fn sync_failure_rate_computed_correctly() {
        let m = RuntimeMetrics::default();
        m.sync_attempt_total.store(10, Ordering::Relaxed);
        m.sync_failure_total.store(3, Ordering::Relaxed);
        assert!((m.sync_failure_rate() - 0.3).abs() < f64::EPSILON);
    }

    #[test]
    fn snapshot_returns_consistent_values() {
        let m = RuntimeMetrics::default();
        m.pending_count.store(5, Ordering::Relaxed);
        m.certified_total.store(10, Ordering::Relaxed);
        m.certification_latency_sum_us
            .store(2000, Ordering::Relaxed);
        m.certification_latency_count.store(4, Ordering::Relaxed);
        m.frontier_skew_ms.store(42, Ordering::Relaxed);
        m.sync_attempt_total.store(20, Ordering::Relaxed);
        m.sync_failure_total.store(2, Ordering::Relaxed);

        let snap = m.snapshot();
        assert_eq!(snap.pending_count, 5);
        assert_eq!(snap.certified_total, 10);
        assert!((snap.certification_latency_mean_us - 500.0).abs() < f64::EPSILON);
        assert_eq!(snap.frontier_skew_ms, 42);
        assert!((snap.sync_failure_rate - 0.1).abs() < f64::EPSILON);
        assert_eq!(snap.sync_attempt_total, 20);
        assert_eq!(snap.sync_failure_total, 2);
    }

    #[test]
    fn atomic_updates_across_threads() {
        use std::sync::Arc;

        let m = Arc::new(RuntimeMetrics::default());
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let m = Arc::clone(&m);
                std::thread::spawn(move || {
                    for _ in 0..100 {
                        m.pending_count.fetch_add(1, Ordering::Relaxed);
                        m.certified_total.fetch_add(1, Ordering::Relaxed);
                        m.sync_attempt_total.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(m.pending_count.load(Ordering::Relaxed), 400);
        assert_eq!(m.certified_total.load(Ordering::Relaxed), 400);
        assert_eq!(m.sync_attempt_total.load(Ordering::Relaxed), 400);
    }

    #[test]
    fn snapshot_json_serialization() {
        let m = RuntimeMetrics::default();
        m.pending_count.store(3, Ordering::Relaxed);
        m.certified_total.store(7, Ordering::Relaxed);

        let snap = m.snapshot();
        let json = serde_json::to_string(&snap).unwrap();
        assert!(json.contains("\"pending_count\":3"));
        assert!(json.contains("\"certified_total\":7"));
        assert!(json.contains("\"certification_latency_mean_us\":"));
        assert!(json.contains("\"frontier_skew_ms\":"));
        assert!(json.contains("\"sync_failure_rate\":"));
    }
}
