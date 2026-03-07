use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Default window duration for time-series metrics (60 seconds).
const WINDOW_SECS: u64 = 60;

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

/// Per-peer sync statistics tracked in a sliding window.
#[derive(Debug)]
struct PeerSyncStats {
    /// Sliding window of (timestamp, latency) for sync operations.
    latencies: VecDeque<(Instant, Duration)>,
    /// Cumulative success count.
    success_count: u64,
    /// Cumulative failure count.
    failure_count: u64,
}

impl PeerSyncStats {
    fn new() -> Self {
        Self {
            latencies: VecDeque::new(),
            success_count: 0,
            failure_count: 0,
        }
    }

    /// Remove entries older than the window duration.
    fn evict_expired(&mut self, now: Instant, window: Duration) {
        let cutoff = now - window;
        while let Some((ts, _)) = self.latencies.front() {
            if *ts < cutoff {
                self.latencies.pop_front();
            } else {
                break;
            }
        }
    }

    /// Record a successful sync with its latency.
    fn record_success(&mut self, now: Instant, latency: Duration, window: Duration) {
        self.evict_expired(now, window);
        self.latencies.push_back((now, latency));
        self.success_count += 1;
    }

    /// Record a failed sync attempt.
    fn record_failure(&mut self, now: Instant, window: Duration) {
        self.evict_expired(now, window);
        self.failure_count += 1;
    }

    /// Compute a snapshot of this peer's sync stats within the window.
    fn snapshot(&self, now: Instant, window: Duration) -> PeerSyncSnapshot {
        let cutoff = now - window;
        let active: Vec<f64> = self
            .latencies
            .iter()
            .filter(|(ts, _)| *ts >= cutoff)
            .map(|(_, d)| d.as_secs_f64() * 1_000_000.0)
            .collect();

        let (mean_us, p99_us) = if active.is_empty() {
            (0.0, 0.0)
        } else {
            let sum: f64 = active.iter().sum();
            let mean = sum / active.len() as f64;
            let mut sorted = active;
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let p99 = percentile(&sorted, 99.0);
            (mean, p99)
        };

        PeerSyncSnapshot {
            mean_latency_us: mean_us,
            p99_latency_us: p99_us,
            success_count: self.success_count,
            failure_count: self.failure_count,
        }
    }
}

/// Sliding window of certification latency samples for time-series tracking.
#[derive(Debug, Default)]
struct CertificationLatencyWindow {
    /// (timestamp, latency) pairs within the window.
    samples: VecDeque<(Instant, Duration)>,
}

impl CertificationLatencyWindow {
    /// Remove entries older than the window duration.
    fn evict_expired(&mut self, now: Instant, window: Duration) {
        let cutoff = now - window;
        while let Some((ts, _)) = self.samples.front() {
            if *ts < cutoff {
                self.samples.pop_front();
            } else {
                break;
            }
        }
    }

    /// Record a certification latency sample.
    fn record(&mut self, now: Instant, latency: Duration, window: Duration) {
        self.evict_expired(now, window);
        self.samples.push_back((now, latency));
    }

    /// Compute windowed statistics.
    fn snapshot(&self, now: Instant, window: Duration) -> CertificationLatencySnapshot {
        let cutoff = now - window;
        let active: Vec<f64> = self
            .samples
            .iter()
            .filter(|(ts, _)| *ts >= cutoff)
            .map(|(_, d)| d.as_secs_f64() * 1_000_000.0)
            .collect();

        if active.is_empty() {
            return CertificationLatencySnapshot {
                sample_count: 0,
                mean_us: 0.0,
                p99_us: 0.0,
            };
        }

        let sum: f64 = active.iter().sum();
        let mean = sum / active.len() as f64;
        let mut sorted = active.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p99 = percentile(&sorted, 99.0);

        CertificationLatencySnapshot {
            sample_count: active.len() as u64,
            mean_us: mean,
            p99_us: p99,
        }
    }
}

/// Runtime metrics for operational monitoring.
///
/// All counters use [`AtomicU64`] for lock-free concurrent access.
/// Per-peer and windowed metrics use [`Mutex`] for interior mutability.
/// Shared via `Arc<RuntimeMetrics>` between [`NodeRunner`](crate::runtime::NodeRunner)
/// and HTTP handlers.
#[derive(Debug)]
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

    /// Per-peer sync statistics (peer_id -> stats).
    peer_sync_stats: Mutex<HashMap<String, PeerSyncStats>>,

    /// Sliding window of certification latency samples.
    certification_latency_window: Mutex<CertificationLatencyWindow>,

    /// Window duration for time-series metrics.
    window_duration: Duration,
}

impl Default for RuntimeMetrics {
    fn default() -> Self {
        Self {
            pending_count: AtomicU64::default(),
            certified_total: AtomicU64::default(),
            certification_latency_sum_us: AtomicU64::default(),
            certification_latency_count: AtomicU64::default(),
            frontier_skew_ms: AtomicU64::default(),
            sync_failure_total: AtomicU64::default(),
            sync_attempt_total: AtomicU64::default(),
            sync_fallback_total: AtomicU64::default(),
            peer_sync_stats: Mutex::new(HashMap::new()),
            certification_latency_window: Mutex::new(CertificationLatencyWindow::default()),
            window_duration: Duration::from_secs(WINDOW_SECS),
        }
    }
}

impl RuntimeMetrics {
    /// Create a `RuntimeMetrics` with a custom window duration (for testing).
    pub fn with_window(window: Duration) -> Self {
        Self {
            window_duration: window,
            ..Default::default()
        }
    }

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

    /// Record a successful sync operation for a specific peer.
    pub fn record_peer_sync_success(&self, peer_id: &str, latency: Duration) {
        self.record_peer_sync_success_at(peer_id, latency, Instant::now());
    }

    /// Record a successful sync for a peer at a specific instant (for testing).
    pub fn record_peer_sync_success_at(&self, peer_id: &str, latency: Duration, now: Instant) {
        let mut stats = self.peer_sync_stats.lock().unwrap();
        let entry = stats
            .entry(peer_id.to_string())
            .or_insert_with(PeerSyncStats::new);
        entry.record_success(now, latency, self.window_duration);
    }

    /// Record a failed sync operation for a specific peer.
    pub fn record_peer_sync_failure(&self, peer_id: &str) {
        self.record_peer_sync_failure_at(peer_id, Instant::now());
    }

    /// Record a failed sync for a peer at a specific instant (for testing).
    pub fn record_peer_sync_failure_at(&self, peer_id: &str, now: Instant) {
        let mut stats = self.peer_sync_stats.lock().unwrap();
        let entry = stats
            .entry(peer_id.to_string())
            .or_insert_with(PeerSyncStats::new);
        entry.record_failure(now, self.window_duration);
    }

    /// Record a certification latency sample in the sliding window.
    pub fn record_certification_latency(&self, latency: Duration) {
        self.record_certification_latency_at(latency, Instant::now());
    }

    /// Record a certification latency at a specific instant (for testing).
    pub fn record_certification_latency_at(&self, latency: Duration, now: Instant) {
        let mut window = self.certification_latency_window.lock().unwrap();
        window.record(now, latency, self.window_duration);
    }

    /// Create a snapshot for JSON serialization.
    pub fn snapshot(&self) -> MetricsSnapshot {
        self.snapshot_at(Instant::now())
    }

    /// Create a snapshot at a specific instant (for testing).
    pub fn snapshot_at(&self, now: Instant) -> MetricsSnapshot {
        let peer_snapshots = {
            let stats = self.peer_sync_stats.lock().unwrap();
            stats
                .iter()
                .map(|(peer_id, s)| (peer_id.clone(), s.snapshot(now, self.window_duration)))
                .collect()
        };

        let cert_latency_window = {
            let window = self.certification_latency_window.lock().unwrap();
            window.snapshot(now, self.window_duration)
        };

        MetricsSnapshot {
            pending_count: self.pending_count.load(Ordering::Relaxed),
            certified_total: self.certified_total.load(Ordering::Relaxed),
            certification_latency_mean_us: self.mean_certification_latency_us(),
            frontier_skew_ms: self.frontier_skew_ms.load(Ordering::Relaxed),
            sync_failure_rate: self.sync_failure_rate(),
            sync_attempt_total: self.sync_attempt_total.load(Ordering::Relaxed),
            sync_failure_total: self.sync_failure_total.load(Ordering::Relaxed),
            peer_sync: peer_snapshots,
            certification_latency_window: cert_latency_window,
        }
    }
}

/// Point-in-time snapshot of per-peer sync statistics.
#[derive(Debug, Clone, Serialize)]
pub struct PeerSyncSnapshot {
    /// Mean sync latency in microseconds (within window).
    pub mean_latency_us: f64,
    /// 99th percentile sync latency in microseconds (within window).
    pub p99_latency_us: f64,
    /// Cumulative successful sync count.
    pub success_count: u64,
    /// Cumulative failed sync count.
    pub failure_count: u64,
}

/// Point-in-time snapshot of certification latency window statistics.
#[derive(Debug, Clone, Serialize)]
pub struct CertificationLatencySnapshot {
    /// Number of samples in the current window.
    pub sample_count: u64,
    /// Mean certification latency in microseconds (within window).
    pub mean_us: f64,
    /// 99th percentile certification latency in microseconds (within window).
    pub p99_us: f64,
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
    /// Per-peer sync statistics.
    pub peer_sync: HashMap<String, PeerSyncSnapshot>,
    /// Windowed certification latency statistics.
    pub certification_latency_window: CertificationLatencySnapshot,
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
        assert!(json.contains("\"peer_sync\":"));
        assert!(json.contains("\"certification_latency_window\":"));
    }

    // ---------------------------------------------------------------
    // Per-peer sync metrics tests
    // ---------------------------------------------------------------

    #[test]
    fn peer_sync_success_recorded() {
        let m = RuntimeMetrics::default();
        let now = Instant::now();

        m.record_peer_sync_success_at("peer-a", Duration::from_millis(10), now);
        m.record_peer_sync_success_at("peer-a", Duration::from_millis(20), now);
        m.record_peer_sync_success_at("peer-b", Duration::from_millis(5), now);

        let snap = m.snapshot_at(now);
        assert_eq!(snap.peer_sync.len(), 2);

        let a = &snap.peer_sync["peer-a"];
        assert_eq!(a.success_count, 2);
        assert_eq!(a.failure_count, 0);
        // Mean of 10ms and 20ms = 15ms = 15000us
        assert!((a.mean_latency_us - 15000.0).abs() < 1.0);

        let b = &snap.peer_sync["peer-b"];
        assert_eq!(b.success_count, 1);
        assert!((b.mean_latency_us - 5000.0).abs() < 1.0);
    }

    #[test]
    fn peer_sync_failure_recorded() {
        let m = RuntimeMetrics::default();
        let now = Instant::now();

        m.record_peer_sync_failure_at("peer-a", now);
        m.record_peer_sync_failure_at("peer-a", now);
        m.record_peer_sync_success_at("peer-a", Duration::from_millis(10), now);

        let snap = m.snapshot_at(now);
        let a = &snap.peer_sync["peer-a"];
        assert_eq!(a.success_count, 1);
        assert_eq!(a.failure_count, 2);
    }

    #[test]
    fn peer_sync_window_expiry() {
        // Use a 2-second window for quick testing.
        let m = RuntimeMetrics::with_window(Duration::from_secs(2));
        let base = Instant::now();

        // Record at base time.
        m.record_peer_sync_success_at("peer-a", Duration::from_millis(100), base);

        // Record 3 seconds later (first entry should be expired).
        let later = base + Duration::from_secs(3);
        m.record_peer_sync_success_at("peer-a", Duration::from_millis(50), later);

        let snap = m.snapshot_at(later);
        let a = &snap.peer_sync["peer-a"];
        // Success count is cumulative (2), but windowed mean should only reflect the 50ms sample.
        assert_eq!(a.success_count, 2);
        assert!((a.mean_latency_us - 50000.0).abs() < 1.0);
    }

    #[test]
    fn peer_sync_p99_calculation() {
        let m = RuntimeMetrics::default();
        let now = Instant::now();

        // Record 100 samples: 1ms, 2ms, ..., 100ms.
        for i in 1..=100 {
            m.record_peer_sync_success_at("peer-a", Duration::from_millis(i), now);
        }

        let snap = m.snapshot_at(now);
        let a = &snap.peer_sync["peer-a"];
        // P99 of 1..=100 ms should be around 99ms-100ms.
        assert!(a.p99_latency_us >= 99000.0 && a.p99_latency_us <= 100000.0);
    }

    // ---------------------------------------------------------------
    // Certification latency window tests
    // ---------------------------------------------------------------

    #[test]
    fn certification_latency_window_recorded() {
        let m = RuntimeMetrics::default();
        let now = Instant::now();

        m.record_certification_latency_at(Duration::from_millis(10), now);
        m.record_certification_latency_at(Duration::from_millis(20), now);

        let snap = m.snapshot_at(now);
        assert_eq!(snap.certification_latency_window.sample_count, 2);
        // Mean of 10ms and 20ms = 15ms = 15000us
        assert!((snap.certification_latency_window.mean_us - 15000.0).abs() < 1.0);
    }

    #[test]
    fn certification_latency_window_expiry() {
        let m = RuntimeMetrics::with_window(Duration::from_secs(2));
        let base = Instant::now();

        m.record_certification_latency_at(Duration::from_millis(100), base);

        let later = base + Duration::from_secs(3);
        m.record_certification_latency_at(Duration::from_millis(50), later);

        let snap = m.snapshot_at(later);
        assert_eq!(snap.certification_latency_window.sample_count, 1);
        assert!((snap.certification_latency_window.mean_us - 50000.0).abs() < 1.0);
    }

    #[test]
    fn certification_latency_window_p99() {
        let m = RuntimeMetrics::default();
        let now = Instant::now();

        for i in 1..=100 {
            m.record_certification_latency_at(Duration::from_millis(i), now);
        }

        let snap = m.snapshot_at(now);
        assert_eq!(snap.certification_latency_window.sample_count, 100);
        assert!(
            snap.certification_latency_window.p99_us >= 99000.0
                && snap.certification_latency_window.p99_us <= 100000.0
        );
    }

    #[test]
    fn certification_latency_window_empty_snapshot() {
        let m = RuntimeMetrics::default();
        let snap = m.snapshot();
        assert_eq!(snap.certification_latency_window.sample_count, 0);
        assert_eq!(snap.certification_latency_window.mean_us, 0.0);
        assert_eq!(snap.certification_latency_window.p99_us, 0.0);
    }

    #[test]
    fn snapshot_includes_peer_sync_and_cert_window() {
        let m = RuntimeMetrics::default();
        let now = Instant::now();

        m.record_peer_sync_success_at("node-1", Duration::from_millis(5), now);
        m.record_certification_latency_at(Duration::from_millis(15), now);

        let snap = m.snapshot_at(now);

        // Verify JSON serialization includes new fields.
        let json = serde_json::to_string(&snap).unwrap();
        assert!(json.contains("\"peer_sync\""));
        assert!(json.contains("\"node-1\""));
        assert!(json.contains("\"certification_latency_window\""));
        assert!(json.contains("\"sample_count\":1"));
    }
}
