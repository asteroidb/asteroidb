use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::hlc::HlcTimestamp;
use crate::types::{KeyRange, NodeId, PolicyVersion};

/// Tracks how far an Authority node has consumed updates for a key range.
///
/// Each Authority maintains a frontier that represents the latest HLC timestamp
/// it has processed. This is used for compaction decisions and certified read
/// eligibility (FR-008).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AckFrontier {
    /// The Authority node that owns this frontier.
    pub authority_id: NodeId,
    /// The latest HLC timestamp this Authority has consumed.
    pub frontier_hlc: HlcTimestamp,
    /// The key range this frontier covers.
    pub key_range: KeyRange,
    /// The placement policy version in effect.
    pub policy_version: PolicyVersion,
    /// Hex-encoded digest hash of the checkpoint (for compaction verification).
    pub digest_hash: String,
}

/// Manages ack_frontiers for a set of Authority nodes within a key range.
///
/// Provides queries for compaction safety (`min_frontier`) and certified read
/// eligibility (`majority_frontier`, `is_certified_at`).
#[derive(Debug, Clone)]
pub struct AckFrontierSet {
    frontiers: HashMap<NodeId, AckFrontier>,
}

impl AckFrontierSet {
    /// Create an empty frontier set.
    pub fn new() -> Self {
        Self {
            frontiers: HashMap::new(),
        }
    }

    /// Update the frontier for an authority.
    ///
    /// Only advances the frontier forward; an older `frontier_hlc` is ignored
    /// to prevent regression.
    pub fn update(&mut self, frontier: AckFrontier) {
        let id = frontier.authority_id.clone();
        match self.frontiers.get(&id) {
            Some(existing) if existing.frontier_hlc >= frontier.frontier_hlc => {
                // Existing frontier is same or newer; ignore the update.
            }
            _ => {
                self.frontiers.insert(id, frontier);
            }
        }
    }

    /// Get the frontier for a specific authority.
    pub fn get(&self, authority_id: &NodeId) -> Option<&AckFrontier> {
        self.frontiers.get(authority_id)
    }

    /// Return all tracked frontiers.
    pub fn all(&self) -> Vec<&AckFrontier> {
        self.frontiers.values().collect()
    }

    /// The lowest frontier across all authorities.
    ///
    /// Updates at or below this timestamp have been consumed by every known
    /// authority, making them safe candidates for compaction.
    pub fn min_frontier(&self) -> Option<&HlcTimestamp> {
        self.frontiers.values().map(|f| &f.frontier_hlc).min()
    }

    /// The frontier that at least a majority of authorities have reached.
    ///
    /// Given `total_authorities` (the full authority set size, which may be
    /// larger than the number of frontiers tracked), this returns the highest
    /// HLC timestamp *t* such that `>= ceil((total_authorities + 1) / 2)`
    /// authorities have a frontier `>= t`.
    ///
    /// Returns `None` if fewer than a majority of authorities have reported.
    pub fn majority_frontier(&self, total_authorities: usize) -> Option<&HlcTimestamp> {
        let majority = total_authorities / 2 + 1;
        if self.frontiers.len() < majority {
            return None;
        }

        let mut timestamps: Vec<&HlcTimestamp> =
            self.frontiers.values().map(|f| &f.frontier_hlc).collect();
        timestamps.sort();

        // The (majority - 1)-th smallest timestamp is the highest value that
        // at least `majority` authorities have reached or exceeded.
        Some(timestamps[timestamps.len() - majority])
    }

    /// Check whether a given timestamp is certified (i.e., below the majority frontier).
    pub fn is_certified_at(&self, timestamp: &HlcTimestamp, total_authorities: usize) -> bool {
        match self.majority_frontier(total_authorities) {
            Some(mf) => timestamp <= mf,
            None => false,
        }
    }
}

impl Default for AckFrontierSet {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_ts(physical: u64, logical: u32, node: &str) -> HlcTimestamp {
        HlcTimestamp {
            physical,
            logical,
            node_id: node.into(),
        }
    }

    fn make_frontier(authority: &str, physical: u64, logical: u32, prefix: &str) -> AckFrontier {
        AckFrontier {
            authority_id: NodeId(authority.into()),
            frontier_hlc: make_ts(physical, logical, authority),
            key_range: KeyRange {
                prefix: prefix.into(),
            },
            policy_version: PolicyVersion(1),
            digest_hash: format!("{authority}-{physical}-{logical}"),
        }
    }

    #[test]
    fn single_authority_update() {
        let mut set = AckFrontierSet::new();
        let f = make_frontier("auth-1", 100, 0, "user/");
        set.update(f.clone());

        let got = set.get(&NodeId("auth-1".into())).unwrap();
        assert_eq!(*got, f);
    }

    #[test]
    fn multiple_authority_tracking() {
        let mut set = AckFrontierSet::new();
        set.update(make_frontier("auth-1", 100, 0, "user/"));
        set.update(make_frontier("auth-2", 200, 0, "user/"));
        set.update(make_frontier("auth-3", 150, 0, "user/"));

        assert_eq!(set.all().len(), 3);
        assert!(set.get(&NodeId("auth-1".into())).is_some());
        assert!(set.get(&NodeId("auth-2".into())).is_some());
        assert!(set.get(&NodeId("auth-3".into())).is_some());
    }

    #[test]
    fn min_frontier_calculation() {
        let mut set = AckFrontierSet::new();
        set.update(make_frontier("auth-1", 100, 0, "user/"));
        set.update(make_frontier("auth-2", 200, 0, "user/"));
        set.update(make_frontier("auth-3", 150, 0, "user/"));

        let min = set.min_frontier().unwrap();
        assert_eq!(min.physical, 100);
    }

    #[test]
    fn min_frontier_empty() {
        let set = AckFrontierSet::new();
        assert!(set.min_frontier().is_none());
    }

    #[test]
    fn majority_frontier_three_authorities() {
        let mut set = AckFrontierSet::new();
        // 3 authorities total, majority = 2
        set.update(make_frontier("auth-1", 100, 0, "user/"));
        set.update(make_frontier("auth-2", 200, 0, "user/"));
        set.update(make_frontier("auth-3", 150, 0, "user/"));

        // Sorted: [100, 150, 200]. majority=2, index = 3-2 = 1 → 150
        let mf = set.majority_frontier(3).unwrap();
        assert_eq!(mf.physical, 150);
    }

    #[test]
    fn majority_frontier_five_authorities() {
        let mut set = AckFrontierSet::new();
        // 5 authorities total, majority = 3
        set.update(make_frontier("auth-1", 100, 0, "user/"));
        set.update(make_frontier("auth-2", 200, 0, "user/"));
        set.update(make_frontier("auth-3", 150, 0, "user/"));
        set.update(make_frontier("auth-4", 300, 0, "user/"));
        set.update(make_frontier("auth-5", 250, 0, "user/"));

        // Sorted: [100, 150, 200, 250, 300]. majority=3, index = 5-3 = 2 → 200
        let mf = set.majority_frontier(5).unwrap();
        assert_eq!(mf.physical, 200);
    }

    #[test]
    fn majority_frontier_insufficient_reports() {
        let mut set = AckFrontierSet::new();
        // 3 authorities total, majority = 2, but only 1 has reported
        set.update(make_frontier("auth-1", 100, 0, "user/"));

        assert!(set.majority_frontier(3).is_none());
    }

    #[test]
    fn is_certified_at_below_majority() {
        let mut set = AckFrontierSet::new();
        set.update(make_frontier("auth-1", 100, 0, "user/"));
        set.update(make_frontier("auth-2", 200, 0, "user/"));
        set.update(make_frontier("auth-3", 150, 0, "user/"));

        // majority frontier = 150
        let ts_below = make_ts(120, 0, "client");
        assert!(set.is_certified_at(&ts_below, 3));
    }

    #[test]
    fn is_certified_at_equal_to_majority() {
        let mut set = AckFrontierSet::new();
        set.update(make_frontier("auth-1", 100, 0, "user/"));
        set.update(make_frontier("auth-2", 200, 0, "user/"));
        set.update(make_frontier("auth-3", 150, 0, "user/"));

        // majority frontier = 150 at auth-3, so ts with physical=150 from auth-3 matches
        let ts_equal = make_ts(150, 0, "auth-3");
        assert!(set.is_certified_at(&ts_equal, 3));
    }

    #[test]
    fn is_certified_at_above_majority() {
        let mut set = AckFrontierSet::new();
        set.update(make_frontier("auth-1", 100, 0, "user/"));
        set.update(make_frontier("auth-2", 200, 0, "user/"));
        set.update(make_frontier("auth-3", 150, 0, "user/"));

        // majority frontier = 150; 180 is above it
        let ts_above = make_ts(180, 0, "client");
        assert!(!set.is_certified_at(&ts_above, 3));
    }

    #[test]
    fn is_certified_at_insufficient_authorities() {
        let mut set = AckFrontierSet::new();
        set.update(make_frontier("auth-1", 100, 0, "user/"));

        // Only 1 out of 3 reported → no majority → nothing is certified
        let ts = make_ts(50, 0, "client");
        assert!(!set.is_certified_at(&ts, 3));
    }

    #[test]
    fn update_with_older_frontier_does_not_regress() {
        let mut set = AckFrontierSet::new();
        set.update(make_frontier("auth-1", 200, 5, "user/"));
        set.update(make_frontier("auth-1", 100, 0, "user/"));

        let got = set.get(&NodeId("auth-1".into())).unwrap();
        assert_eq!(got.frontier_hlc.physical, 200);
        assert_eq!(got.frontier_hlc.logical, 5);
    }

    #[test]
    fn update_with_newer_frontier_advances() {
        let mut set = AckFrontierSet::new();
        set.update(make_frontier("auth-1", 100, 0, "user/"));
        set.update(make_frontier("auth-1", 200, 0, "user/"));

        let got = set.get(&NodeId("auth-1".into())).unwrap();
        assert_eq!(got.frontier_hlc.physical, 200);
    }

    #[test]
    fn serde_roundtrip_ack_frontier() {
        let f = make_frontier("auth-1", 1_700_000_000_000, 42, "user/");
        let json = serde_json::to_string(&f).expect("serialize");
        let back: AckFrontier = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(f, back);
    }

    #[test]
    fn default_creates_empty_set() {
        let set = AckFrontierSet::default();
        assert!(set.all().is_empty());
        assert!(set.min_frontier().is_none());
    }

    #[test]
    fn majority_frontier_two_authorities() {
        let mut set = AckFrontierSet::new();
        // 2 authorities, majority = 2
        set.update(make_frontier("auth-1", 100, 0, "user/"));
        set.update(make_frontier("auth-2", 200, 0, "user/"));

        // Sorted: [100, 200]. majority=2, index = 2-2 = 0 → 100
        let mf = set.majority_frontier(2).unwrap();
        assert_eq!(mf.physical, 100);
    }

    #[test]
    fn majority_frontier_single_authority() {
        let mut set = AckFrontierSet::new();
        // 1 authority, majority = 1
        set.update(make_frontier("auth-1", 100, 0, "user/"));

        let mf = set.majority_frontier(1).unwrap();
        assert_eq!(mf.physical, 100);
    }
}
