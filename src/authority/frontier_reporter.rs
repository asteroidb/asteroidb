use crate::authority::ack_frontier::{AckFrontier, FrontierScope};
use crate::control_plane::system_namespace::SystemNamespace;
use crate::error::HlcError;
use crate::hlc::{Hlc, HlcTimestamp};
use crate::store::digest::{DIGEST_LEN, DIGEST_SCHEME_VERSION};
use crate::types::NodeId;

/// Sentinel `digest_hash` reported while the store's digest cache is still
/// cold (warm-up pending): the report binds no content claim for this tick.
/// Distinct from every real digest (`sd2:<64 hex>`), and constant within a
/// tick like any other digest string, so it can never produce a
/// self-conflicting pair.
pub const SD_COLD: &str = "sd2:cold";

/// Sentinel `digest_hash` reported when no eventual store is attached to
/// the runner (certified-only wiring): content binding is unavailable.
pub const SD_UNAVAILABLE: &str = "sd2:unavailable";

/// Format an M-7 store root digest as a frontier `digest_hash` string:
/// `"sd{DIGEST_SCHEME_VERSION}:{hex(root)}"` (e.g. `sd2:3fa9…`, 64 hex).
///
/// The scheme-version prefix makes the binding diagnosable on the wire and
/// keeps a future scheme bump (v3 canonical form) visually distinct; the
/// string is opaque to verification and comparison, so old nodes handle it
/// unchanged (signature covers it byte-for-byte, detector compares equality).
pub fn format_store_digest_hash(root: &[u8; DIGEST_LEN]) -> String {
    format!("sd{DIGEST_SCHEME_VERSION}:{}", hex::encode(root))
}

/// The legacy placeholder `digest_hash`: a deterministic function of
/// `(node_id, HLC)`. Still emitted while the store-digest form is inactive
/// (kill switch off, or no clock-floor persistence configured). Note that a
/// floorless-boot activation grace does NOT emit placeholders — it
/// suppresses reporting entirely (see `NodeRunner`'s
/// `DIGEST_ACTIVATION_GRACE`): placeholder determinism only protects
/// against re-issued HLCs whose earlier head was ALSO placeholder-format,
/// and a floorless boot cannot know which format its previous incarnation
/// was signing.
pub fn placeholder_digest_hash(node_id: &NodeId, ts: &HlcTimestamp) -> String {
    format!("{}-{}-{}", node_id.0, ts.physical, ts.logical)
}

/// Whether a frontier `digest_hash` string actually binds store content:
/// `sd<version>:<64 lowercase hex>` (any scheme version). Placeholder
/// strings, the `sd2:cold` / `sd2:unavailable` sentinels and arbitrary
/// garbage are all non-binding.
///
/// This is an OBSERVABILITY predicate, never a validity check: the
/// detector deliberately treats the digest as an opaque string (old nodes
/// keep detecting new-format conflicts, and format validation would add a
/// rejection path that a malicious authority could aim at). Its purpose is
/// the receive-side metric `frontier_nonbinding_digest_total` — a
/// compromised authority can permanently opt out of content binding by
/// signing placeholder-shaped or sentinel strings forever, which no
/// detector flags as misbehaviour; the metric makes that visible to
/// operators instead of leaving it silent.
pub fn is_binding_store_digest(digest_hash: &str) -> bool {
    let Some(rest) = digest_hash.strip_prefix("sd") else {
        return false;
    };
    let Some((version, hex)) = rest.split_once(':') else {
        return false;
    };
    !version.is_empty()
        && version.bytes().all(|b| b.is_ascii_digit())
        && hex.len() == DIGEST_LEN * 2
        && hex
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

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
    /// Whether this node is a member of at least one authority definition,
    /// including definitions whose range has no placement policy yet.
    /// Kept separate from `authority_scopes` (which only contains
    /// reportable scopes) so that a node seeded into a definition before
    /// any policy exists still registers as an authority — and starts
    /// reporting as soon as `refresh_scopes` sees the first policy.
    is_definition_member: bool,
}

impl FrontierReporter {
    /// Create a new reporter for the given node.
    ///
    /// Discovers which authority scopes this node is responsible for by
    /// scanning all authority definitions in the system namespace.
    pub fn new(node_id: NodeId, namespace: &SystemNamespace) -> Self {
        let (authority_scopes, is_definition_member) = Self::discover_scopes(&node_id, namespace);
        Self {
            node_id,
            authority_scopes,
            is_definition_member,
        }
    }

    /// Return the scopes this reporter is authority for.
    pub fn authority_scopes(&self) -> &[FrontierScope] {
        &self.authority_scopes
    }

    /// Return true if this node is a member of at least one authority
    /// definition (whether or not the range has a placement policy yet).
    pub fn is_authority(&self) -> bool {
        self.is_definition_member
    }

    /// Return a reference to the node ID.
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Generate frontier reports for all authority scopes.
    ///
    /// Each scope receives a frontier at the current HLC time with the
    /// given `digest_hash` (see [`report_frontiers_at`](Self::report_frontiers_at)
    /// for the digest contract). The returned `AckFrontier` values can be
    /// applied via `AckFrontierSet::update()`.
    ///
    /// Because `Hlc::now()` is monotonic, successive calls will never
    /// produce timestamps that go backwards.
    pub fn report_frontiers(
        &self,
        clock: &mut Hlc,
        digest_hash: &str,
    ) -> Result<Vec<AckFrontier>, HlcError> {
        let now = clock.now()?;
        Ok(self.report_frontiers_at(&now, digest_hash))
    }

    /// Generate frontier reports for all authority scopes at a specific
    /// timestamp, binding `digest_hash` verbatim into every scope's report.
    ///
    /// **Digest contract (equivocation safety)**: the caller must NEVER
    /// call this twice with the same `timestamp` but a different
    /// `digest_hash`. A signed frontier pair with identical
    /// `(authority, key_range, policy_version, frontier_hlc)` and
    /// differing digests IS the definition of an equivocation
    /// (`EquivocationDetector::observe`) — violating this rule makes an
    /// honest node accuse itself. The only production caller is
    /// `NodeRunner::report_frontiers`, which computes the digest exactly
    /// once per freshly issued (strictly monotonic, restart-floored) HLC.
    pub fn report_frontiers_at(
        &self,
        timestamp: &HlcTimestamp,
        digest_hash: &str,
    ) -> Vec<AckFrontier> {
        self.authority_scopes
            .iter()
            .map(|scope| AckFrontier {
                authority_id: self.node_id.clone(),
                frontier_hlc: timestamp.clone(),
                key_range: scope.key_range.clone(),
                policy_version: scope.policy_version,
                digest_hash: digest_hash.to_string(),
            })
            .collect()
    }

    /// Re-discover authority scopes from the system namespace.
    ///
    /// Call this when the namespace changes (e.g., policy version bump or
    /// authority set reconfiguration).
    pub fn refresh_scopes(&mut self, namespace: &SystemNamespace) {
        let (scopes, is_member) = Self::discover_scopes(&self.node_id, namespace);
        self.authority_scopes = scopes;
        self.is_definition_member = is_member;
    }

    /// Discover which scopes this node is authority for, and whether it is
    /// a member of any authority definition at all.
    ///
    /// Definitions WITHOUT a placement policy are membership-relevant but
    /// never reported: a range without a policy cannot certify any write
    /// (`resolve_scope` requires both), and every receiving node would
    /// reject the report at admission (`NoPolicy`) — notably the
    /// auto-seeded catch-all `""` definition on signed deployments would
    /// otherwise produce a WARN per authority per checkpoint tick, forever,
    /// while permanently ratcheting the flood-signal rejection counter.
    fn discover_scopes(
        node_id: &NodeId,
        namespace: &SystemNamespace,
    ) -> (Vec<FrontierScope>, bool) {
        let mut scopes = Vec::new();
        let mut is_member = false;
        for def in namespace.all_authority_definitions() {
            if !def.authority_nodes.contains(node_id) {
                continue;
            }
            is_member = true;
            let Some(policy) = namespace.get_placement_policy(&def.key_range.prefix) else {
                continue;
            };
            scopes.push(FrontierScope::new(
                def.key_range.clone(),
                policy.version,
                node_id.clone(),
            ));
        }
        (scopes, is_member)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authority::ack_frontier::AckFrontierSet;
    use crate::control_plane::system_namespace::AuthorityDefinition;
    use crate::placement::PlacementPolicy;
    use crate::types::{KeyRange, PolicyVersion};

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
        add_scope(&mut ns, prefix, authorities);
        ns
    }

    /// Add an authority definition WITH a placement policy (v1): only
    /// ranges with a policy are reportable.
    fn add_scope(ns: &mut SystemNamespace, prefix: &str, authorities: &[&str]) {
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr(prefix),
            authority_nodes: authorities.iter().map(|a| node(a)).collect(),
            auto_generated: false,
        });
        ns.set_placement_policy(PlacementPolicy::new(
            PolicyVersion(1),
            kr(prefix),
            authorities.len(),
        ))
        .unwrap();
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
        add_scope(&mut ns, "user/", &["auth-1", "auth-2"]);
        add_scope(&mut ns, "order/", &["auth-1", "auth-3"]);

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
        )
        .unwrap();

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

        let frontiers =
            reporter.report_frontiers_at(&ts, &placeholder_digest_hash(reporter.node_id(), &ts));

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

        let frontiers = reporter.report_frontiers(&mut clock, "sd-test").unwrap();
        assert_eq!(frontiers.len(), 1);
        // HLC clock should produce a valid timestamp.
        assert!(frontiers[0].frontier_hlc.physical > 0);
    }

    #[test]
    fn report_frontiers_at_propagates_digest_verbatim_to_all_scopes() {
        // The digest string is bound byte-for-byte into EVERY scope's
        // report of the tick: scope count must never fan the tick out
        // into per-scope digest variants (single-computation contract).
        let mut ns = SystemNamespace::new();
        add_scope(&mut ns, "user/", &["auth-1", "auth-2"]);
        add_scope(&mut ns, "order/", &["auth-1", "auth-3"]);
        let reporter = FrontierReporter::new(node("auth-1"), &ns);
        let ts = make_ts(2_000, 3, "auth-1");

        let digest = format_store_digest_hash(&[0xAB; DIGEST_LEN]);
        let frontiers = reporter.report_frontiers_at(&ts, &digest);
        assert_eq!(frontiers.len(), 2);
        for f in &frontiers {
            assert_eq!(f.digest_hash, digest, "digest must propagate verbatim");
        }
    }

    #[test]
    fn digest_hash_format_helpers() {
        let digest = format_store_digest_hash(&[0u8; DIGEST_LEN]);
        assert_eq!(
            digest,
            format!("sd{DIGEST_SCHEME_VERSION}:{}", "0".repeat(64))
        );
        // The sentinels share the live scheme prefix (diagnosability) but
        // can never collide with a real digest (never 64 hex chars).
        for sentinel in [SD_COLD, SD_UNAVAILABLE] {
            assert!(sentinel.starts_with(&format!("sd{DIGEST_SCHEME_VERSION}:")));
            assert_ne!(sentinel.len(), digest.len());
        }
        // The placeholder is a pure function of (node_id, HLC): two
        // kill-switch-era (or no-floor-path) reports at one HLC are
        // byte-identical and therefore Consistent on any peer.
        let ts = make_ts(1_234, 7, "auth-1");
        assert_eq!(
            placeholder_digest_hash(&node("auth-1"), &ts),
            "auth-1-1234-7"
        );
        assert_eq!(
            placeholder_digest_hash(&node("auth-1"), &ts),
            placeholder_digest_hash(&node("auth-1"), &ts.clone())
        );
    }

    #[test]
    fn is_binding_store_digest_classifies_formats() {
        // Binding: real root digests, any scheme version.
        assert!(is_binding_store_digest(&format_store_digest_hash(
            &[0xAB; DIGEST_LEN]
        )));
        assert!(is_binding_store_digest(&format!("sd3:{}", "0".repeat(64))));
        // Non-binding: sentinels, placeholders, malformed variants.
        assert!(!is_binding_store_digest(SD_COLD));
        assert!(!is_binding_store_digest(SD_UNAVAILABLE));
        assert!(!is_binding_store_digest("auth-1-1234-0"));
        assert!(!is_binding_store_digest(""));
        assert!(!is_binding_store_digest("sd:0"));
        assert!(!is_binding_store_digest(&format!("sd2:{}", "0".repeat(63))));
        assert!(!is_binding_store_digest(&format!("sd2:{}", "0".repeat(65))));
        // Uppercase hex is not the canonical `hex::encode` output.
        assert!(!is_binding_store_digest(&format!("sd2:{}", "A".repeat(64))));
        assert!(!is_binding_store_digest(&format!("sdX:{}", "0".repeat(64))));
    }

    #[test]
    fn successive_reports_produce_monotonic_timestamps() {
        let ns = make_namespace("user/", &["auth-1"]);
        let reporter = FrontierReporter::new(node("auth-1"), &ns);
        let mut clock = Hlc::new("auth-1".into());

        let f1 = reporter.report_frontiers(&mut clock, "sd-test").unwrap();
        let f2 = reporter.report_frontiers(&mut clock, "sd-test").unwrap();

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
        let frontiers = reporter.report_frontiers_at(&ts_new, "d-new");
        for f in &frontiers {
            set.update(f.clone());
        }

        // Try to apply an older frontier at t=500
        let ts_old = make_ts(500, 0, "auth-1");
        let old_frontiers = reporter.report_frontiers_at(&ts_old, "d-old");
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
        let frontiers =
            reporter.report_frontiers_at(&ts, &placeholder_digest_hash(reporter.node_id(), &ts));

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
        add_scope(&mut ns, "user/", &["auth-1"]);

        let mut reporter = FrontierReporter::new(node("auth-1"), &ns);
        assert_eq!(reporter.authority_scopes().len(), 1);

        // Add a new authority definition that includes auth-1.
        add_scope(&mut ns, "order/", &["auth-1", "auth-2"]);

        reporter.refresh_scopes(&ns);
        assert_eq!(reporter.authority_scopes().len(), 2);
    }

    #[test]
    fn refresh_scopes_removes_revoked_authority() {
        let mut ns = SystemNamespace::new();
        add_scope(&mut ns, "user/", &["auth-1", "auth-2"]);

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
        assert!(reporter.authority_scopes().is_empty());
    }

    // ---------------------------------------------------------------
    // Definitions without a placement policy are never reported
    // ---------------------------------------------------------------

    #[test]
    fn definition_without_policy_is_member_but_not_reported() {
        // The auto-seeded catch-all "" definition has no placement policy:
        // it must count for authority membership (the node keeps its
        // reporter and starts reporting once a policy appears) but must not
        // generate frontier reports — every receiver would reject them at
        // admission (NoPolicy), warn once per tick per authority forever,
        // and ratchet the flood-signal rejection counter.
        let mut ns = SystemNamespace::new();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr(""),
            authority_nodes: vec![node("auth-1"), node("auth-2"), node("auth-3")],
            auto_generated: false,
        });

        let mut reporter = FrontierReporter::new(node("auth-1"), &ns);
        assert!(
            reporter.is_authority(),
            "definition membership must hold even without a policy"
        );
        assert!(
            reporter.authority_scopes().is_empty(),
            "a range without a placement policy must not be reported"
        );

        // Once the operator creates a policy, the scope becomes reportable.
        ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(7), kr(""), 3))
            .unwrap();
        reporter.refresh_scopes(&ns);
        assert_eq!(reporter.authority_scopes().len(), 1);
        assert_eq!(
            reporter.authority_scopes()[0].policy_version,
            PolicyVersion(7)
        );
    }

    // ---------------------------------------------------------------
    // Non-authority node produces no frontiers
    // ---------------------------------------------------------------

    #[test]
    fn non_authority_produces_empty_report() {
        let ns = make_namespace("user/", &["auth-1", "auth-2", "auth-3"]);
        let reporter = FrontierReporter::new(node("store-node"), &ns);
        let mut clock = Hlc::new("store-node".into());

        let frontiers = reporter.report_frontiers(&mut clock, "sd-test").unwrap();
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
        for f in r1.report_frontiers_at(&report_ts, "d1") {
            set.update(f);
        }
        let report_ts = make_ts(1000, 0, "auth-2");
        for f in r2.report_frontiers_at(&report_ts, "d2") {
            set.update(f);
        }

        // Majority (2 of 3) reached → ts=500 should be certified.
        assert!(
            set.is_certified_at_for_scope(&ts, &kr("user/"), &PolicyVersion(1), 3),
            "write at t=500 should be certified after 2-of-3 authorities report at t=1000"
        );

        // auth-3 hasn't reported yet; adding its frontier at t=200 shouldn't break anything.
        let report_ts = make_ts(200, 0, "auth-3");
        for f in r3.report_frontiers_at(&report_ts, "d3") {
            set.update(f);
        }

        // Still certified (majority frontier is min of top-2: min(1000, 1000) = 1000 >= 500).
        assert!(set.is_certified_at_for_scope(&ts, &kr("user/"), &PolicyVersion(1), 3));
    }
}
