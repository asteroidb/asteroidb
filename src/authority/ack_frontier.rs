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

/// Composite key that scopes a frontier entry to a specific key range,
/// policy version, and authority node.
///
/// Prevents frontier contamination: updates for one key range or policy
/// version cannot overwrite frontiers belonging to a different scope.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FrontierScope {
    /// The key range this frontier covers.
    pub key_range: KeyRange,
    /// The placement policy version in effect.
    pub policy_version: PolicyVersion,
    /// The Authority node.
    pub authority_id: NodeId,
}

impl FrontierScope {
    /// Create a new scope from individual components.
    pub fn new(key_range: KeyRange, policy_version: PolicyVersion, authority_id: NodeId) -> Self {
        Self {
            key_range,
            policy_version,
            authority_id,
        }
    }

    /// Extract scope from an `AckFrontier`.
    pub fn from_frontier(frontier: &AckFrontier) -> Self {
        Self {
            key_range: frontier.key_range.clone(),
            policy_version: frontier.policy_version,
            authority_id: frontier.authority_id.clone(),
        }
    }
}

/// Manages ack_frontiers for a set of Authority nodes, scoped by
/// `{key_range, policy_version, authority_id}`.
///
/// Provides queries for compaction safety (`min_frontier`) and certified read
/// eligibility (`majority_frontier`, `is_certified_at`).  Both unscoped
/// (all entries) and scoped (filtered by key_range + policy_version)
/// query variants are available.
#[derive(Debug, Clone)]
pub struct AckFrontierSet {
    frontiers: HashMap<FrontierScope, AckFrontier>,
}

impl AckFrontierSet {
    /// Create an empty frontier set.
    pub fn new() -> Self {
        Self {
            frontiers: HashMap::new(),
        }
    }

    /// Update the frontier for a scoped authority.
    ///
    /// The scope key `{key_range, policy_version, authority_id}` is derived
    /// from the frontier itself. Only advances the frontier forward within
    /// its scope; an older `frontier_hlc` is ignored to prevent regression.
    pub fn update(&mut self, frontier: AckFrontier) {
        let scope = FrontierScope::from_frontier(&frontier);
        match self.frontiers.get(&scope) {
            Some(existing) if existing.frontier_hlc >= frontier.frontier_hlc => {
                // Existing frontier is same or newer; ignore the update.
            }
            _ => {
                self.frontiers.insert(scope, frontier);
            }
        }
    }

    /// Get the frontier for a specific authority by `NodeId`.
    ///
    /// Searches all scopes and returns the first match. Suitable for
    /// single-scope sets or when the authority appears in only one scope.
    pub fn get(&self, authority_id: &NodeId) -> Option<&AckFrontier> {
        self.frontiers
            .values()
            .find(|f| &f.authority_id == authority_id)
    }

    /// Get the frontier for a fully-scoped key.
    pub fn get_scoped(&self, scope: &FrontierScope) -> Option<&AckFrontier> {
        self.frontiers.get(scope)
    }

    /// Return all tracked frontiers across all scopes.
    pub fn all(&self) -> Vec<&AckFrontier> {
        self.frontiers.values().collect()
    }

    /// Return all frontiers for a specific key range and policy version.
    pub fn all_for_scope(
        &self,
        key_range: &KeyRange,
        policy_version: &PolicyVersion,
    ) -> Vec<&AckFrontier> {
        self.frontiers
            .iter()
            .filter(|(scope, _)| {
                &scope.key_range == key_range && &scope.policy_version == policy_version
            })
            .map(|(_, f)| f)
            .collect()
    }

    // ---------------------------------------------------------------
    // Unscoped queries (operate on ALL entries in the set)
    // ---------------------------------------------------------------

    /// The lowest frontier across all authorities and scopes.
    ///
    /// Updates at or below this timestamp have been consumed by every known
    /// authority, making them safe candidates for compaction.
    pub fn min_frontier(&self) -> Option<&HlcTimestamp> {
        self.frontiers.values().map(|f| &f.frontier_hlc).min()
    }

    /// The frontier that at least a majority of authorities have reached,
    /// considering all entries regardless of scope.
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

        Some(timestamps[timestamps.len() - majority])
    }

    /// Check whether a given timestamp is certified across all scopes.
    pub fn is_certified_at(&self, timestamp: &HlcTimestamp, total_authorities: usize) -> bool {
        match self.majority_frontier(total_authorities) {
            Some(mf) => timestamp <= mf,
            None => false,
        }
    }

    // ---------------------------------------------------------------
    // Scoped queries (filtered by key_range + policy_version)
    // ---------------------------------------------------------------

    /// The lowest frontier for a specific key range and policy version.
    pub fn min_frontier_for_scope(
        &self,
        key_range: &KeyRange,
        policy_version: &PolicyVersion,
    ) -> Option<&HlcTimestamp> {
        self.all_for_scope(key_range, policy_version)
            .iter()
            .map(|f| &f.frontier_hlc)
            .min()
    }

    /// The majority frontier for a specific key range and policy version.
    ///
    /// Only considers frontiers that match the given scope when computing
    /// the majority threshold.
    pub fn majority_frontier_for_scope(
        &self,
        key_range: &KeyRange,
        policy_version: &PolicyVersion,
        total_authorities: usize,
    ) -> Option<HlcTimestamp> {
        let majority = total_authorities / 2 + 1;
        let scoped: Vec<&AckFrontier> = self.all_for_scope(key_range, policy_version);
        if scoped.len() < majority {
            return None;
        }

        let mut timestamps: Vec<&HlcTimestamp> = scoped.iter().map(|f| &f.frontier_hlc).collect();
        timestamps.sort();

        Some(timestamps[timestamps.len() - majority].clone())
    }

    /// Check whether a given timestamp is certified within a specific scope.
    pub fn is_certified_at_for_scope(
        &self,
        timestamp: &HlcTimestamp,
        key_range: &KeyRange,
        policy_version: &PolicyVersion,
        total_authorities: usize,
    ) -> bool {
        match self.majority_frontier_for_scope(key_range, policy_version, total_authorities) {
            Some(ref mf) => timestamp <= mf,
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

    fn make_frontier_v(
        authority: &str,
        physical: u64,
        logical: u32,
        prefix: &str,
        version: u64,
    ) -> AckFrontier {
        AckFrontier {
            authority_id: NodeId(authority.into()),
            frontier_hlc: make_ts(physical, logical, authority),
            key_range: KeyRange {
                prefix: prefix.into(),
            },
            policy_version: PolicyVersion(version),
            digest_hash: format!("{authority}-{physical}-{logical}"),
        }
    }

    fn kr(prefix: &str) -> KeyRange {
        KeyRange {
            prefix: prefix.into(),
        }
    }

    fn pv(v: u64) -> PolicyVersion {
        PolicyVersion(v)
    }

    // ---------------------------------------------------------------
    // Existing tests (adapted for scoped storage)
    // ---------------------------------------------------------------

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

    // ---------------------------------------------------------------
    // New tests: scoped storage prevents frontier contamination
    // ---------------------------------------------------------------

    #[test]
    fn different_key_ranges_are_independent() {
        let mut set = AckFrontierSet::new();

        // Same authority, different key ranges
        set.update(make_frontier("auth-1", 100, 0, "user/"));
        set.update(make_frontier("auth-1", 500, 0, "order/"));

        // Both entries exist independently
        assert_eq!(set.all().len(), 2);

        let scope_user = FrontierScope::new(kr("user/"), pv(1), NodeId("auth-1".into()));
        let scope_order = FrontierScope::new(kr("order/"), pv(1), NodeId("auth-1".into()));

        let user_f = set.get_scoped(&scope_user).unwrap();
        assert_eq!(user_f.frontier_hlc.physical, 100);

        let order_f = set.get_scoped(&scope_order).unwrap();
        assert_eq!(order_f.frontier_hlc.physical, 500);
    }

    #[test]
    fn different_policy_versions_are_independent() {
        let mut set = AckFrontierSet::new();

        // Same authority and key range, different policy versions
        set.update(make_frontier_v("auth-1", 100, 0, "user/", 1));
        set.update(make_frontier_v("auth-1", 500, 0, "user/", 2));

        // Both entries exist independently
        assert_eq!(set.all().len(), 2);

        let scope_v1 = FrontierScope::new(kr("user/"), pv(1), NodeId("auth-1".into()));
        let scope_v2 = FrontierScope::new(kr("user/"), pv(2), NodeId("auth-1".into()));

        let v1_f = set.get_scoped(&scope_v1).unwrap();
        assert_eq!(v1_f.frontier_hlc.physical, 100);

        let v2_f = set.get_scoped(&scope_v2).unwrap();
        assert_eq!(v2_f.frontier_hlc.physical, 500);
    }

    #[test]
    fn update_key_range_does_not_overwrite_different_range() {
        let mut set = AckFrontierSet::new();

        set.update(make_frontier("auth-1", 100, 0, "user/"));
        set.update(make_frontier("auth-1", 999, 0, "order/"));

        // user/ frontier must remain at 100, not overwritten by order/ update
        let scope_user = FrontierScope::new(kr("user/"), pv(1), NodeId("auth-1".into()));
        let user_f = set.get_scoped(&scope_user).unwrap();
        assert_eq!(user_f.frontier_hlc.physical, 100);
    }

    #[test]
    fn update_policy_version_does_not_overwrite_different_version() {
        let mut set = AckFrontierSet::new();

        set.update(make_frontier_v("auth-1", 100, 0, "user/", 1));
        set.update(make_frontier_v("auth-1", 999, 0, "user/", 2));

        // v1 frontier must remain at 100, not overwritten by v2 update
        let scope_v1 = FrontierScope::new(kr("user/"), pv(1), NodeId("auth-1".into()));
        let v1_f = set.get_scoped(&scope_v1).unwrap();
        assert_eq!(v1_f.frontier_hlc.physical, 100);
    }

    #[test]
    fn monotonic_advancement_per_scope() {
        let mut set = AckFrontierSet::new();

        // Advance frontier in user/ scope
        set.update(make_frontier("auth-1", 200, 0, "user/"));
        // Try to regress in user/ scope
        set.update(make_frontier("auth-1", 100, 0, "user/"));

        let scope = FrontierScope::new(kr("user/"), pv(1), NodeId("auth-1".into()));
        assert_eq!(set.get_scoped(&scope).unwrap().frontier_hlc.physical, 200);

        // But order/ scope can be set independently at a lower value
        set.update(make_frontier("auth-1", 50, 0, "order/"));
        let scope_order = FrontierScope::new(kr("order/"), pv(1), NodeId("auth-1".into()));
        assert_eq!(
            set.get_scoped(&scope_order).unwrap().frontier_hlc.physical,
            50
        );
    }

    #[test]
    fn all_for_scope_filters_correctly() {
        let mut set = AckFrontierSet::new();

        // 3 authorities in user/ scope
        set.update(make_frontier("auth-1", 100, 0, "user/"));
        set.update(make_frontier("auth-2", 200, 0, "user/"));
        set.update(make_frontier("auth-3", 150, 0, "user/"));

        // 2 authorities in order/ scope
        set.update(make_frontier("auth-1", 300, 0, "order/"));
        set.update(make_frontier("auth-2", 400, 0, "order/"));

        assert_eq!(set.all().len(), 5);
        assert_eq!(set.all_for_scope(&kr("user/"), &pv(1)).len(), 3);
        assert_eq!(set.all_for_scope(&kr("order/"), &pv(1)).len(), 2);
        assert_eq!(set.all_for_scope(&kr("data/"), &pv(1)).len(), 0);
    }

    #[test]
    fn majority_frontier_for_scope_independent() {
        let mut set = AckFrontierSet::new();

        // user/ scope: 3 authorities
        set.update(make_frontier("auth-1", 100, 0, "user/"));
        set.update(make_frontier("auth-2", 200, 0, "user/"));
        set.update(make_frontier("auth-3", 150, 0, "user/"));

        // order/ scope: 3 authorities (higher values)
        set.update(make_frontier("auth-1", 1000, 0, "order/"));
        set.update(make_frontier("auth-2", 2000, 0, "order/"));
        set.update(make_frontier("auth-3", 1500, 0, "order/"));

        // user/ majority: sorted [100, 150, 200], majority=2, idx=1 → 150
        let mf_user = set
            .majority_frontier_for_scope(&kr("user/"), &pv(1), 3)
            .unwrap();
        assert_eq!(mf_user.physical, 150);

        // order/ majority: sorted [1000, 1500, 2000], majority=2, idx=1 → 1500
        let mf_order = set
            .majority_frontier_for_scope(&kr("order/"), &pv(1), 3)
            .unwrap();
        assert_eq!(mf_order.physical, 1500);
    }

    #[test]
    fn is_certified_at_for_scope_independent() {
        let mut set = AckFrontierSet::new();

        // user/ scope: majority frontier = 150
        set.update(make_frontier("auth-1", 100, 0, "user/"));
        set.update(make_frontier("auth-2", 200, 0, "user/"));
        set.update(make_frontier("auth-3", 150, 0, "user/"));

        // order/ scope: majority frontier = 1500
        set.update(make_frontier("auth-1", 1000, 0, "order/"));
        set.update(make_frontier("auth-2", 2000, 0, "order/"));
        set.update(make_frontier("auth-3", 1500, 0, "order/"));

        let ts_180 = make_ts(180, 0, "client");

        // 180 is above user/ majority (150) → not certified in user/
        assert!(!set.is_certified_at_for_scope(&ts_180, &kr("user/"), &pv(1), 3));

        // 180 is below order/ majority (1500) → certified in order/
        assert!(set.is_certified_at_for_scope(&ts_180, &kr("order/"), &pv(1), 3));
    }

    #[test]
    fn min_frontier_for_scope_independent() {
        let mut set = AckFrontierSet::new();

        set.update(make_frontier("auth-1", 100, 0, "user/"));
        set.update(make_frontier("auth-2", 200, 0, "user/"));
        set.update(make_frontier("auth-1", 1000, 0, "order/"));
        set.update(make_frontier("auth-2", 2000, 0, "order/"));

        assert_eq!(
            set.min_frontier_for_scope(&kr("user/"), &pv(1))
                .unwrap()
                .physical,
            100
        );
        assert_eq!(
            set.min_frontier_for_scope(&kr("order/"), &pv(1))
                .unwrap()
                .physical,
            1000
        );
        // Global min is still 100
        assert_eq!(set.min_frontier().unwrap().physical, 100);
    }

    #[test]
    fn scoped_majority_insufficient_for_one_scope() {
        let mut set = AckFrontierSet::new();

        // user/ has 2 of 3 authorities
        set.update(make_frontier("auth-1", 100, 0, "user/"));
        set.update(make_frontier("auth-2", 200, 0, "user/"));

        // order/ has only 1 of 3 authorities
        set.update(make_frontier("auth-1", 1000, 0, "order/"));

        // user/ has majority
        assert!(
            set.majority_frontier_for_scope(&kr("user/"), &pv(1), 3)
                .is_some()
        );

        // order/ does not have majority
        assert!(
            set.majority_frontier_for_scope(&kr("order/"), &pv(1), 3)
                .is_none()
        );
    }

    #[test]
    fn frontier_scope_serde_roundtrip() {
        let scope = FrontierScope::new(kr("user/"), pv(2), NodeId("auth-1".into()));
        let json = serde_json::to_string(&scope).expect("serialize");
        let back: FrontierScope = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(scope, back);
    }

    #[test]
    fn get_scoped_returns_none_for_missing() {
        let set = AckFrontierSet::new();
        let scope = FrontierScope::new(kr("user/"), pv(1), NodeId("auth-1".into()));
        assert!(set.get_scoped(&scope).is_none());
    }

    #[test]
    fn get_by_authority_returns_first_match_across_scopes() {
        let mut set = AckFrontierSet::new();

        set.update(make_frontier("auth-1", 100, 0, "user/"));
        set.update(make_frontier("auth-1", 500, 0, "order/"));

        // get() returns some entry for auth-1 (implementation-defined which one)
        let got = set.get(&NodeId("auth-1".into()));
        assert!(got.is_some());
        let got = got.unwrap();
        assert_eq!(got.authority_id, NodeId("auth-1".into()));
    }

    #[test]
    fn mixed_scopes_global_majority_counts_all_entries() {
        let mut set = AckFrontierSet::new();

        // 2 entries: auth-1 in user/, auth-1 in order/
        set.update(make_frontier("auth-1", 100, 0, "user/"));
        set.update(make_frontier("auth-1", 200, 0, "order/"));

        // Global: 2 entries, majority=2 for total=2 → 100
        let mf = set.majority_frontier(2).unwrap();
        assert_eq!(mf.physical, 100);
    }

    #[test]
    fn policy_version_scoping_in_same_key_range() {
        let mut set = AckFrontierSet::new();

        // v1 authorities
        set.update(make_frontier_v("auth-1", 100, 0, "user/", 1));
        set.update(make_frontier_v("auth-2", 150, 0, "user/", 1));
        set.update(make_frontier_v("auth-3", 120, 0, "user/", 1));

        // v2 authorities (fresh start, lower values)
        set.update(make_frontier_v("auth-1", 10, 0, "user/", 2));
        set.update(make_frontier_v("auth-2", 20, 0, "user/", 2));
        set.update(make_frontier_v("auth-3", 15, 0, "user/", 2));

        // v1 scope: majority = 120
        let mf_v1 = set
            .majority_frontier_for_scope(&kr("user/"), &pv(1), 3)
            .unwrap();
        assert_eq!(mf_v1.physical, 120);

        // v2 scope: majority = 15
        let mf_v2 = set
            .majority_frontier_for_scope(&kr("user/"), &pv(2), 3)
            .unwrap();
        assert_eq!(mf_v2.physical, 15);

        // v1 entries not contaminated by v2
        assert!(set.is_certified_at_for_scope(&make_ts(100, 0, "c"), &kr("user/"), &pv(1), 3));
        assert!(!set.is_certified_at_for_scope(&make_ts(100, 0, "c"), &kr("user/"), &pv(2), 3));
    }
}
