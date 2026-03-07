use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::types::NodeId;

/// Sliding window size for latency samples.
const DEFAULT_MAX_SAMPLES: usize = 100;

/// Statistics for the measured RTT between a pair of nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyStats {
    /// Average RTT in milliseconds.
    pub avg_ms: f64,
    /// 99th percentile RTT in milliseconds.
    pub p99_ms: f64,
    /// Number of samples collected.
    pub samples: u64,
    /// Timestamp of the last update (unix epoch milliseconds).
    pub last_updated_ms: u64,
}

/// Internal sample buffer for computing rolling statistics.
#[derive(Debug, Clone)]
struct LatencySamples {
    /// Ring buffer of RTT samples in milliseconds.
    values: Vec<f64>,
    /// Total number of samples ever recorded (may exceed buffer capacity).
    total_samples: u64,
    /// Timestamp of last update (unix epoch milliseconds).
    last_updated_ms: u64,
    /// Maximum number of samples to retain.
    max_samples: usize,
}

impl LatencySamples {
    fn new(max_samples: usize) -> Self {
        Self {
            values: Vec::with_capacity(max_samples),
            total_samples: 0,
            last_updated_ms: 0,
            max_samples,
        }
    }

    fn add(&mut self, rtt_ms: f64, now_ms: u64) {
        if self.values.len() >= self.max_samples {
            // Remove oldest sample (FIFO).
            self.values.remove(0);
        }
        self.values.push(rtt_ms);
        self.total_samples += 1;
        self.last_updated_ms = now_ms;
    }

    fn stats(&self) -> LatencyStats {
        if self.values.is_empty() {
            return LatencyStats {
                avg_ms: 0.0,
                p99_ms: 0.0,
                samples: 0,
                last_updated_ms: self.last_updated_ms,
            };
        }

        let sum: f64 = self.values.iter().sum();
        let avg = sum / self.values.len() as f64;

        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let p99_idx = ((0.99 * sorted.len() as f64).ceil() as usize)
            .min(sorted.len())
            .saturating_sub(1);
        let p99 = sorted[p99_idx];

        LatencyStats {
            avg_ms: avg,
            p99_ms: p99,
            samples: self.total_samples,
            last_updated_ms: self.last_updated_ms,
        }
    }
}

/// Model that tracks measured RTT between node pairs.
///
/// Used by placement policies to enforce latency constraints when
/// selecting replica nodes.
#[derive(Debug, Clone)]
pub struct LatencyModel {
    /// Measured latency samples indexed by (from, to) node pair.
    samples: HashMap<(NodeId, NodeId), LatencySamples>,
    /// Maximum samples per pair.
    max_samples: usize,
}

impl LatencyModel {
    /// Create a new empty latency model with the default sample window.
    pub fn new() -> Self {
        Self {
            samples: HashMap::new(),
            max_samples: DEFAULT_MAX_SAMPLES,
        }
    }

    /// Create a latency model with a custom maximum sample count per pair.
    pub fn with_max_samples(max_samples: usize) -> Self {
        Self {
            samples: HashMap::new(),
            max_samples,
        }
    }

    /// Record a latency measurement between two nodes.
    pub fn update_latency(&mut self, from: &NodeId, to: &NodeId, rtt_ms: f64, now_ms: u64) {
        let key = (from.clone(), to.clone());
        let entry = self
            .samples
            .entry(key)
            .or_insert_with(|| LatencySamples::new(self.max_samples));
        entry.add(rtt_ms, now_ms);
    }

    /// Get the current latency statistics for a node pair.
    pub fn get_latency(&self, from: &NodeId, to: &NodeId) -> Option<LatencyStats> {
        let key = (from.clone(), to.clone());
        self.samples.get(&key).map(|s| s.stats())
    }

    /// Return all nodes reachable from `from` within the given latency bound.
    ///
    /// Filters by average RTT. Only nodes for which measurements exist and
    /// whose average RTT is at most `max_ms` are included.
    pub fn nodes_within_latency(&self, from: &NodeId, max_ms: f64) -> Vec<NodeId> {
        self.samples
            .iter()
            .filter_map(|((f, t), samples)| {
                if f == from {
                    let stats = samples.stats();
                    if stats.avg_ms <= max_ms && stats.samples > 0 {
                        Some(t.clone())
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect()
    }

    /// Return all tracked node pairs and their statistics.
    pub fn all_stats(&self) -> HashMap<(NodeId, NodeId), LatencyStats> {
        self.samples
            .iter()
            .map(|(k, v)| (k.clone(), v.stats()))
            .collect()
    }
}

impl Default for LatencyModel {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nid(s: &str) -> NodeId {
        NodeId(s.into())
    }

    // --- LatencyModel update and query ---

    #[test]
    fn update_and_get_latency() {
        let mut model = LatencyModel::new();
        model.update_latency(&nid("a"), &nid("b"), 10.0, 1000);
        model.update_latency(&nid("a"), &nid("b"), 20.0, 2000);

        let stats = model.get_latency(&nid("a"), &nid("b")).unwrap();
        assert!((stats.avg_ms - 15.0).abs() < 0.01);
        assert_eq!(stats.samples, 2);
        assert_eq!(stats.last_updated_ms, 2000);
    }

    #[test]
    fn get_latency_missing_pair_returns_none() {
        let model = LatencyModel::new();
        assert!(model.get_latency(&nid("a"), &nid("b")).is_none());
    }

    #[test]
    fn update_latency_direction_matters() {
        let mut model = LatencyModel::new();
        model.update_latency(&nid("a"), &nid("b"), 10.0, 1000);

        assert!(model.get_latency(&nid("a"), &nid("b")).is_some());
        assert!(model.get_latency(&nid("b"), &nid("a")).is_none());
    }

    // --- Sliding window expiry ---

    #[test]
    fn sliding_window_evicts_oldest() {
        let mut model = LatencyModel::with_max_samples(3);
        model.update_latency(&nid("a"), &nid("b"), 100.0, 1000);
        model.update_latency(&nid("a"), &nid("b"), 100.0, 2000);
        model.update_latency(&nid("a"), &nid("b"), 100.0, 3000);
        // This should evict the first sample (100.0) and add 10.0.
        model.update_latency(&nid("a"), &nid("b"), 10.0, 4000);

        let stats = model.get_latency(&nid("a"), &nid("b")).unwrap();
        // Buffer now: [100, 100, 10], avg = 70.0
        assert!((stats.avg_ms - 70.0).abs() < 0.01);
        assert_eq!(stats.samples, 4); // total samples ever
    }

    // --- nodes_within_latency ---

    #[test]
    fn nodes_within_latency_filters_correctly() {
        let mut model = LatencyModel::new();
        model.update_latency(&nid("a"), &nid("b"), 5.0, 1000);
        model.update_latency(&nid("a"), &nid("c"), 50.0, 1000);
        model.update_latency(&nid("a"), &nid("d"), 10.0, 1000);

        let within = model.nodes_within_latency(&nid("a"), 20.0);
        assert!(within.contains(&nid("b")));
        assert!(within.contains(&nid("d")));
        assert!(!within.contains(&nid("c")));
    }

    #[test]
    fn nodes_within_latency_empty_model() {
        let model = LatencyModel::new();
        let within = model.nodes_within_latency(&nid("a"), 100.0);
        assert!(within.is_empty());
    }

    #[test]
    fn nodes_within_latency_ignores_other_sources() {
        let mut model = LatencyModel::new();
        model.update_latency(&nid("a"), &nid("b"), 5.0, 1000);
        model.update_latency(&nid("c"), &nid("b"), 5.0, 1000);

        let within = model.nodes_within_latency(&nid("a"), 100.0);
        assert_eq!(within.len(), 1);
        assert!(within.contains(&nid("b")));
    }

    // --- p99 calculation ---

    #[test]
    fn p99_calculation() {
        let mut model = LatencyModel::new();
        // Insert 100 samples: 1, 2, 3, ..., 100
        for i in 1..=100 {
            model.update_latency(&nid("a"), &nid("b"), i as f64, i * 1000);
        }

        let stats = model.get_latency(&nid("a"), &nid("b")).unwrap();
        // p99 of 1..=100 should be 99 or 100
        assert!(stats.p99_ms >= 99.0 && stats.p99_ms <= 100.0);
    }

    // --- all_stats ---

    #[test]
    fn all_stats_returns_all_pairs() {
        let mut model = LatencyModel::new();
        model.update_latency(&nid("a"), &nid("b"), 10.0, 1000);
        model.update_latency(&nid("c"), &nid("d"), 20.0, 2000);

        let all = model.all_stats();
        assert_eq!(all.len(), 2);
        assert!(all.contains_key(&(nid("a"), nid("b"))));
        assert!(all.contains_key(&(nid("c"), nid("d"))));
    }

    // --- Default trait ---

    #[test]
    fn default_creates_empty_model() {
        let model = LatencyModel::default();
        assert!(model.all_stats().is_empty());
    }
}
