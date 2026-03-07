use crate::authority::ack_frontier::{AckFrontier, FrontierScope};
use crate::control_plane::system_namespace::SystemNamespace;
use crate::hlc::{Hlc, HlcTimestamp};
use crate::types::{NodeId, PolicyVersion};

/// Generates frontier reports for authority scopes managed by this node.
///
/// An Authority node uses `FrontierReporter` to determine which key-range
/// scopes it is responsible for and to produce `AckFrontier` values based
/// on the current HLC time. The generated frontiers can then be fed into
/// `AckFrontierSet::update()` locally and pushed to peers.
///
/// Frontier regression is inherently prevented because:
/// 1. `Hlc::now()` is monotonic.
/// 2. `AckFrontierSet::update()` ignores older timestamps.
pub struct FrontierReporter {
    node_id: NodeId,
    /// Scopes this node is authority for (derived from SystemNamespace).
    authority_scopes: Vec<FrontierScope>,
}

impl FrontierReporter {
    /// Create a new reporter for the given node.
    ///
    /// Discovers which authority scopes this node is responsible for by
    /// scanning all authority definitions in the system namespace.
    pub fn new(node_id: NodeId, namespace: &SystemNamespace) -> Self {
        let authority_scopes = Self::discover_scopes(&node_id, namespace);
        Self {
            node_id,
            authority_scopes,
        }
    }

    /// Return the scopes this reporter is authority for.
    pub fn authority_scopes(&self) -> &[FrontierScope] {
        &self.authority_scopes
    }

    /// Return true if this node is an authority for at least one scope.
    pub fn is_authority(&self) -> bool {
        !self.authority_scopes.is_empty()
    }

    /// Return a reference to the node ID.
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Generate frontier reports for all authority scopes.
    ///
    /// Each scope receives a frontier at the current HLC time. The returned
    /// `AckFrontier` values can be applied via `AckFrontierSet::update()`.
    ///
    /// Because `Hlc::now()` is monotonic, successive calls will never
    /// produce timestamps that go backwards.
    pub fn report_frontiers(&self, clock: &mut Hlc) -> Vec<AckFrontier> {
        let now = clock.now();
        self.report_frontiers_at(&now)
    }

    /// Generate frontier reports for all authority scopes at a specific timestamp.
    ///
    /// This is useful for testing or when the caller already has a timestamp.
    pub fn report_frontiers_at(&self, timestamp: &HlcTimestamp) -> Vec<AckFrontier> {
        self.authority_scopes
            .iter()
            .map(|scope| AckFrontier {
                authority_id: self.node_id.clone(),
                frontier_hlc: timestamp.clone(),
                key_range: scope.key_range.clone(),
                policy_version: scope.policy_version,
                digest_hash: format!(
                    "{}-{}-{}",
                    self.node_id.0, timestamp.physical, timestamp.logical
                ),
            })
            .collect()
    }

    /// Re-discover authority scopes from the system namespace.
    ///
    /// Call this when the namespace changes (e.g., policy version bump or
    /// authority set reconfiguration).
    pub fn refresh_scopes(&mut self, namespace: &SystemNamespace) {
        self.authority_scopes = Self::discover_scopes(&self.node_id, namespace);
    }

    /// Discover which scopes this node is authority for.
    fn discover_scopes(node_id: &NodeId, namespace: &SystemNamespace) -> Vec<FrontierScope> {
        let mut scopes = Vec::new();
        for def in namespace.all_authority_definitions() {
            if def.authority_nodes.contains(node_id) {
                let policy_version = namespace
                    .get_placement_policy(&def.key_range.prefix)
                    .map(|p| p.version)
                    .unwrap_or(PolicyVersion(1));

                scopes.push(FrontierScope::new(
                    def.key_range.clone(),
                    policy_version,
                    node_id.clone(),
                ));
            }
        }
        scopes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authority::ack_frontier::AckFrontierSet;
    use crate::control_plane::system_namespace::AuthorityDefinition;
    use crate::placement::PlacementPolicy;
    use crate::types::KeyRange;

    fn node(name: &str) -> NodeId {
        NodeId(name.into())
    }

    fn kr(prefix: &str) -> KeyRange {
        KeyRange {
            prefix: prefix.into(),
        }
    }

    fn make_namespace(prefix: &str, authorities: &[&str]) -> SystemNamespace {
        let mut ns = SystemNamespace::new();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr(prefix),
            authority_nodes: authorities.iter().map(|a| node(a)).collect(),
            auto_generated: false,
        });
        ns
    }

    fn make_ts(physical: u64, logical: u32, node_id: &str) -> HlcTimestamp {
        HlcTimestamp {
            physical,
            logical,
            node_id: node_id.into(),
        }
    }

    // ---------------------------------------------------------------
    // Construction and scope discovery
    // ---------------------------------------------------------------

    #[test]
    fn discovers_scopes_for_authority_node() {
        let ns = make_namespace("user/", &["auth-1", "auth-2", "auth-3"]);
        let reporter = FrontierReporter::new(node("auth-1"), &ns);

        assert!(reporter.is_authority());
        assert_eq!(reporter.authority_scopes().len(), 1);
        assert_eq!(reporter.authority_scopes()[0].key_range, kr("user/"));
        assert_eq!(
            reporter.authority_scopes()[0].policy_version,
            PolicyVersion(1)
        );
    }

    #[test]
    fn non_authority_node_has_no_scopes() {
        let ns = make_namespace("user/", &["auth-1", "auth-2", "auth-3"]);
        let reporter = FrontierReporter::new(node("store-node"), &ns);

        assert!(!reporter.is_authority());
        assert!(reporter.authority_scopes().is_empty());
    }

    #[test]
    fn discovers_multiple_scopes() {
        let mut ns = SystemNamespace::new();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr("user/"),
            authority_nodes: vec![node("auth-1"), node("auth-2")],
            auto_generated: false,
        });
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr("order/"),
            authority_nodes: vec![node("auth-1"), node("auth-3")],
            auto_generated: false,
        });

        let reporter = FrontierReporter::new(node("auth-1"), &ns);
        assert_eq!(reporter.authority_scopes().len(), 2);

        let prefixes: Vec<&str> = reporter
            .authority_scopes()
            .iter()
            .map(|s| s.key_range.prefix.as_str())
            .collect();
        assert!(prefixes.contains(&"user/"));
        assert!(prefixes.contains(&"order/"));
    }

    #[test]
    fn respects_policy_version() {
        let mut ns = SystemNamespace::new();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr("data/"),
            authority_nodes: vec![node("auth-1")],
            auto_generated: false,
        });
        ns.set_placement_policy(
            PlacementPolicy::new(PolicyVersion(3), kr("data/"), 2).with_certified(true),
        );

        let reporter = FrontierReporter::new(node("auth-1"), &ns);
        assert_eq!(
            reporter.authority_scopes()[0].policy_version,
            PolicyVersion(3)
        );
    }

    // ---------------------------------------------------------------
    // Frontier generation
    // ---------------------------------------------------------------

    #[test]
    fn report_frontiers_at_generates_correct_frontiers() {
        let ns = make_namespace("user/", &["auth-1", "auth-2", "auth-3"]);
        let reporter = FrontierReporter::new(node("auth-1"), &ns);
        let ts = make_ts(1000, 5, "auth-1");

        let frontiers = reporter.report_frontiers_at(&ts);

        assert_eq!(frontiers.len(), 1);
        assert_eq!(frontiers[0].authority_id, node("auth-1"));
        assert_eq!(frontiers[0].frontier_hlc.physical, 1000);
        assert_eq!(frontiers[0].frontier_hlc.logical, 5);
        assert_eq!(frontiers[0].key_range, kr("user/"));
        assert_eq!(frontiers[0].policy_version, PolicyVersion(1));
    }

    #[test]
    fn report_frontiers_uses_hlc_clock() {
        let ns = make_namespace("user/", &["auth-1"]);
        let reporter = FrontierReporter::new(node("auth-1"), &ns);
        let mut clock = Hlc::new("auth-1".into());

        let frontiers = reporter.report_frontiers(&mut clock);
        assert_eq!(frontiers.len(), 1);
        // HLC clock should produce a valid timestamp.
        assert!(frontiers[0].frontier_hlc.physical > 0);
    }

    #[test]
    fn successive_reports_produce_monotonic_timestamps() {
        let ns = make_namespace("user/", &["auth-1"]);
        let reporter = FrontierReporter::new(node("auth-1"), &ns);
        let mut clock = Hlc::new("auth-1".into());

        let f1 = reporter.report_frontiers(&mut clock);
        let f2 = reporter.report_frontiers(&mut clock);

        assert!(
            f2[0].frontier_hlc > f1[0].frontier_hlc,
            "successive reports must produce monotonically increasing timestamps"
        );
    }

    // ---------------------------------------------------------------
    // Frontier regression prevention
    // ---------------------------------------------------------------

    #[test]
    fn old_frontier_does_not_regress_set() {
        let ns = make_namespace("user/", &["auth-1"]);
        let reporter = FrontierReporter::new(node("auth-1"), &ns);
        let mut set = AckFrontierSet::new();

        // Report at t=1000
        let ts_new = make_ts(1000, 0, "auth-1");
        let frontiers = reporter.report_frontiers_at(&ts_new);
        for f in &frontiers {
            set.update(f.clone());
        }

        // Try to apply an older frontier at t=500
        let ts_old = make_ts(500, 0, "auth-1");
        let old_frontiers = reporter.report_frontiers_at(&ts_old);
        for f in &old_frontiers {
            set.update(f.clone());
        }

        // Frontier should still be at t=1000 (monotonicity preserved)
        let scope = &reporter.authority_scopes()[0];
        let current = set.get_scoped(scope).unwrap();
        assert_eq!(
            current.frontier_hlc.physical, 1000,
            "frontier must not regress to older timestamp"
        );
    }

    // ---------------------------------------------------------------
    // Duplicate elimination
    // ---------------------------------------------------------------

    #[test]
    fn duplicate_frontier_is_idempotent() {
        let ns = make_namespace("user/", &["auth-1"]);
        let reporter = FrontierReporter::new(node("auth-1"), &ns);
        let mut set = AckFrontierSet::new();

        let ts = make_ts(1000, 0, "auth-1");
        let frontiers = reporter.report_frontiers_at(&ts);

        // Apply the same frontier twice
        for f in &frontiers {
            set.update(f.clone());
        }
        for f in &frontiers {
            set.update(f.clone());
        }

        // Set should still contain exactly one entry for this scope
        assert_eq!(set.all().len(), 1);
        let scope = &reporter.authority_scopes()[0];
        assert_eq!(set.get_scoped(scope).unwrap().frontier_hlc.physical, 1000);
    }

    // ---------------------------------------------------------------
    // Refresh scopes
    // ---------------------------------------------------------------

    #[test]
    fn refresh_scopes_detects_new_authority() {
        let mut ns = SystemNamespace::new();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr("user/"),
            authority_nodes: vec![node("auth-1")],
            auto_generated: false,
        });

        let mut reporter = FrontierReporter::new(node("auth-1"), &ns);
        assert_eq!(reporter.authority_scopes().len(), 1);

        // Add a new authority definition that includes auth-1.
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr("order/"),
            authority_nodes: vec![node("auth-1"), node("auth-2")],
            auto_generated: false,
        });

        reporter.refresh_scopes(&ns);
        assert_eq!(reporter.authority_scopes().len(), 2);
    }

    #[test]
    fn refresh_scopes_removes_revoked_authority() {
        let mut ns = SystemNamespace::new();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr("user/"),
            authority_nodes: vec![node("auth-1"), node("auth-2")],
            auto_generated: false,
        });

        let mut reporter = FrontierReporter::new(node("auth-1"), &ns);
        assert_eq!(reporter.authority_scopes().len(), 1);

        // Reconfigure: auth-1 is no longer an authority.
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr("user/"),
            authority_nodes: vec![node("auth-2"), node("auth-3")],
            auto_generated: false,
        });

        reporter.refresh_scopes(&ns);
        assert!(!reporter.is_authority());
    }

    // ---------------------------------------------------------------
    // Non-authority node produces no frontiers
    // ---------------------------------------------------------------

    #[test]
    fn non_authority_produces_empty_report() {
        let ns = make_namespace("user/", &["auth-1", "auth-2", "auth-3"]);
        let reporter = FrontierReporter::new(node("store-node"), &ns);
        let mut clock = Hlc::new("store-node".into());

        let frontiers = reporter.report_frontiers(&mut clock);
        assert!(frontiers.is_empty());
    }

    // ---------------------------------------------------------------
    // Integration: reporter → AckFrontierSet → certification check
    // ---------------------------------------------------------------

    #[test]
    fn frontier_reporter_drives_certification() {
        let ns = make_namespace("user/", &["auth-1", "auth-2", "auth-3"]);
        let mut set = AckFrontierSet::new();

        // Create reporters for all 3 authorities.
        let r1 = FrontierReporter::new(node("auth-1"), &ns);
        let r2 = FrontierReporter::new(node("auth-2"), &ns);
        let r3 = FrontierReporter::new(node("auth-3"), &ns);

        let ts = make_ts(500, 0, "client");

        // Only auth-1 and auth-2 report at t=1000 (above client write).
        let report_ts = make_ts(1000, 0, "auth-1");
        for f in r1.report_frontiers_at(&report_ts) {
            set.update(f);
        }
        let report_ts = make_ts(1000, 0, "auth-2");
        for f in r2.report_frontiers_at(&report_ts) {
            set.update(f);
        }

        // Majority (2 of 3) reached → ts=500 should be certified.
        assert!(
            set.is_certified_at_for_scope(&ts, &kr("user/"), &PolicyVersion(1), 3),
            "write at t=500 should be certified after 2-of-3 authorities report at t=1000"
        );

        // auth-3 hasn't reported yet; adding its frontier at t=200 shouldn't break anything.
        let report_ts = make_ts(200, 0, "auth-3");
        for f in r3.report_frontiers_at(&report_ts) {
            set.update(f);
        }

        // Still certified (majority frontier is min of top-2: min(1000, 1000) = 1000 >= 500).
        assert!(set.is_certified_at_for_scope(&ts, &kr("user/"), &PolicyVersion(1), 3));
    }
}
