use serde::Serialize;

use crate::api::certified::CertifiedApi;
use crate::authority::ack_frontier::AckFrontierSet;
use crate::compaction::CompactionEngine;
use crate::hlc::HlcTimestamp;
use crate::types::CertificationStatus;

/// Per-scope frontier state for diagnostics.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct FrontierScopeSummary {
    /// Key range prefix.
    pub key_range: String,
    /// Policy version.
    pub policy_version: u64,
    /// Authority ID.
    pub authority_id: String,
    /// Frontier HLC timestamp.
    pub frontier_hlc: HlcTimestamp,
}

/// Aggregated frontier state across all scopes.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct FrontierSummary {
    /// Total number of tracked frontier entries.
    pub total_entries: usize,
    /// Per-scope frontier details.
    pub scopes: Vec<FrontierScopeSummary>,
}

/// Certification status counts.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct CertificationSummary {
    /// Number of writes in `Pending` status.
    pub pending: usize,
    /// Number of writes in `Certified` status.
    pub certified: usize,
    /// Number of writes in `Rejected` status.
    pub rejected: usize,
    /// Number of writes in `Timeout` status.
    pub timeout: usize,
    /// Total tracked writes.
    pub total: usize,
}

/// Summary of a single checkpoint.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct CheckpointSummary {
    /// Key range prefix this checkpoint covers.
    pub key_range: String,
    /// Timestamp when the checkpoint was created.
    pub timestamp: HlcTimestamp,
    /// Digest hash at checkpoint time.
    pub digest_hash: String,
    /// Policy version at checkpoint time.
    pub policy_version: u64,
    /// Operations processed since the previous checkpoint.
    pub ops_since_last: u64,
}

/// Compaction engine state.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct CompactionSummary {
    /// Total number of checkpoints across all key ranges.
    pub checkpoint_count: usize,
    /// Per-prefix checkpoint details.
    pub checkpoints: Vec<CheckpointSummary>,
    /// Number of revalidation events logged.
    pub revalidation_count: usize,
}

/// Retention and eviction state.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RetentionSummary {
    /// Cumulative count of pending writes evicted due to capacity pressure.
    pub evicted_count: u64,
    /// Current number of tracked pending writes.
    pub pending_writes_count: usize,
    /// Configured maximum age in milliseconds.
    pub max_age_ms: u64,
    /// Configured maximum entries.
    pub max_entries: usize,
}

/// Aggregated diagnostics snapshot for a node.
///
/// Captures the current state of all major subsystems for debugging
/// and operational monitoring. All fields are `Serialize` for JSON output.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct NodeDiagnostics {
    /// Frontier state across all scopes.
    pub frontier_summary: FrontierSummary,
    /// Certification status distribution.
    pub certification_summary: CertificationSummary,
    /// Compaction engine state.
    pub compaction_summary: CompactionSummary,
    /// Retention and eviction metrics.
    pub retention_summary: RetentionSummary,
}

/// Collect frontier diagnostics from an `AckFrontierSet`.
pub fn collect_frontier_summary(frontiers: &AckFrontierSet) -> FrontierSummary {
    let all = frontiers.all();
    let scopes = all
        .iter()
        .map(|f| FrontierScopeSummary {
            key_range: f.key_range.prefix.clone(),
            authority_id: f.authority_id.0.clone(),
            policy_version: f.policy_version.0,
            frontier_hlc: f.frontier_hlc.clone(),
        })
        .collect();

    FrontierSummary {
        total_entries: all.len(),
        scopes,
    }
}

/// Collect certification status counts from a `CertifiedApi`.
pub fn collect_certification_summary(api: &CertifiedApi) -> CertificationSummary {
    let writes = api.pending_writes();
    let mut pending = 0;
    let mut certified = 0;
    let mut rejected = 0;
    let mut timeout = 0;

    for pw in writes {
        match pw.status {
            CertificationStatus::Pending => pending += 1,
            CertificationStatus::Certified => certified += 1,
            CertificationStatus::Rejected => rejected += 1,
            CertificationStatus::Timeout => timeout += 1,
        }
    }

    CertificationSummary {
        pending,
        certified,
        rejected,
        timeout,
        total: writes.len(),
    }
}

/// Collect compaction state from a `CompactionEngine`.
///
/// The `prefixes` parameter specifies which key range prefixes to inspect.
/// Pass all known prefixes to get a complete picture.
pub fn collect_compaction_summary(
    engine: &CompactionEngine,
    prefixes: &[&str],
) -> CompactionSummary {
    let mut checkpoints = Vec::new();

    for prefix in prefixes {
        if let Some(cp) = engine.get_checkpoint(prefix) {
            checkpoints.push(CheckpointSummary {
                key_range: cp.key_range.prefix.clone(),
                timestamp: cp.timestamp.clone(),
                digest_hash: cp.digest_hash.clone(),
                policy_version: cp.policy_version.0,
                ops_since_last: cp.ops_since_last,
            });
        }
    }

    CompactionSummary {
        checkpoint_count: checkpoints.len(),
        checkpoints,
        revalidation_count: engine.revalidation_log().len(),
    }
}

/// Collect retention/eviction metrics from a `CertifiedApi`.
pub fn collect_retention_summary(api: &CertifiedApi) -> RetentionSummary {
    let policy = api.retention_policy();
    RetentionSummary {
        evicted_count: api.evicted_count(),
        pending_writes_count: api.pending_writes().len(),
        max_age_ms: policy.max_age_ms,
        max_entries: policy.max_entries,
    }
}

/// Build a full `NodeDiagnostics` snapshot.
///
/// Aggregates frontier, certification, compaction, and retention state
/// into a single diagnostics struct suitable for JSON serialization.
pub fn collect_node_diagnostics(
    frontiers: &AckFrontierSet,
    certified_api: &CertifiedApi,
    compaction_engine: &CompactionEngine,
    compaction_prefixes: &[&str],
) -> NodeDiagnostics {
    NodeDiagnostics {
        frontier_summary: collect_frontier_summary(frontiers),
        certification_summary: collect_certification_summary(certified_api),
        compaction_summary: collect_compaction_summary(compaction_engine, compaction_prefixes),
        retention_summary: collect_retention_summary(certified_api),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::certified::{CertifiedApi, OnTimeout, RetentionPolicy};
    use crate::authority::ack_frontier::{AckFrontier, AckFrontierSet};
    use crate::compaction::{CompactionEngine, RevalidationTrigger};
    use crate::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
    use crate::crdt::pn_counter::PnCounter;
    use crate::store::kv::CrdtValue;
    use crate::types::{KeyRange, NodeId, PolicyVersion};

    fn node(name: &str) -> NodeId {
        NodeId(name.into())
    }

    fn kr(prefix: &str) -> KeyRange {
        KeyRange {
            prefix: prefix.into(),
        }
    }

    fn ts(physical: u64, logical: u32, node: &str) -> HlcTimestamp {
        HlcTimestamp {
            physical,
            logical,
            node_id: node.into(),
        }
    }

    fn make_frontier(authority: &str, physical: u64, logical: u32, prefix: &str) -> AckFrontier {
        AckFrontier {
            authority_id: NodeId(authority.into()),
            frontier_hlc: ts(physical, logical, authority),
            key_range: kr(prefix),
            policy_version: PolicyVersion(1),
            digest_hash: format!("{authority}-{physical}-{logical}"),
        }
    }

    fn counter_value(n: i64) -> CrdtValue {
        let mut counter = PnCounter::new();
        for _ in 0..n {
            counter.increment(&node("writer"));
        }
        CrdtValue::Counter(counter)
    }

    fn default_namespace() -> SystemNamespace {
        let mut ns = SystemNamespace::new();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr(""),
            authority_nodes: vec![node("auth-1"), node("auth-2"), node("auth-3")],
        });
        ns
    }

    // ---------------------------------------------------------------
    // FrontierSummary tests
    // ---------------------------------------------------------------

    #[test]
    fn frontier_summary_empty() {
        let set = AckFrontierSet::new();
        let summary = collect_frontier_summary(&set);
        assert_eq!(summary.total_entries, 0);
        assert!(summary.scopes.is_empty());
    }

    #[test]
    fn frontier_summary_with_entries() {
        let mut set = AckFrontierSet::new();
        set.update(make_frontier("auth-1", 100, 0, "user/"));
        set.update(make_frontier("auth-2", 200, 0, "user/"));
        set.update(make_frontier("auth-1", 300, 0, "order/"));

        let summary = collect_frontier_summary(&set);
        assert_eq!(summary.total_entries, 3);
        assert_eq!(summary.scopes.len(), 3);

        // Verify all scopes are captured.
        let user_scopes: Vec<_> = summary
            .scopes
            .iter()
            .filter(|s| s.key_range == "user/")
            .collect();
        assert_eq!(user_scopes.len(), 2);

        let order_scopes: Vec<_> = summary
            .scopes
            .iter()
            .filter(|s| s.key_range == "order/")
            .collect();
        assert_eq!(order_scopes.len(), 1);
    }

    // ---------------------------------------------------------------
    // CertificationSummary tests
    // ---------------------------------------------------------------

    #[test]
    fn certification_summary_empty() {
        let api = CertifiedApi::new(node("node-1"), default_namespace());
        let summary = collect_certification_summary(&api);

        assert_eq!(summary.pending, 0);
        assert_eq!(summary.certified, 0);
        assert_eq!(summary.rejected, 0);
        assert_eq!(summary.timeout, 0);
        assert_eq!(summary.total, 0);
    }

    #[test]
    fn certification_summary_mixed_statuses() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());

        // Create 3 pending writes.
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        api.certified_write("key2".into(), counter_value(2), OnTimeout::Pending)
            .unwrap();
        api.certified_write("key3".into(), counter_value(3), OnTimeout::Pending)
            .unwrap();

        let write_ts = api.pending_writes()[0].timestamp.physical;

        // Certify key1 via frontier advancement.
        api.update_frontier(make_frontier("auth-1", write_ts + 1, 0, ""));
        api.update_frontier(make_frontier("auth-2", write_ts + 1, 0, ""));
        api.process_certifications();

        let summary = collect_certification_summary(&api);

        // key1 is certified, key2 and key3 may or may not be depending on timing.
        assert!(summary.certified >= 1);
        assert_eq!(summary.total, 3);
        assert_eq!(
            summary.pending + summary.certified + summary.rejected + summary.timeout,
            summary.total
        );
    }

    // ---------------------------------------------------------------
    // CompactionSummary tests
    // ---------------------------------------------------------------

    #[test]
    fn compaction_summary_empty() {
        let engine = CompactionEngine::with_defaults();
        let summary = collect_compaction_summary(&engine, &[]);
        assert_eq!(summary.checkpoint_count, 0);
        assert!(summary.checkpoints.is_empty());
        assert_eq!(summary.revalidation_count, 0);
    }

    #[test]
    fn compaction_summary_with_checkpoints() {
        let mut engine = CompactionEngine::with_defaults();

        for _ in 0..50 {
            engine.record_op(&kr("user/"));
        }
        engine.create_checkpoint(
            kr("user/"),
            ts(1000, 0, "node-a"),
            "hash-user".into(),
            PolicyVersion(1),
        );

        for _ in 0..30 {
            engine.record_op(&kr("order/"));
        }
        engine.create_checkpoint(
            kr("order/"),
            ts(2000, 0, "node-a"),
            "hash-order".into(),
            PolicyVersion(2),
        );

        engine.trigger_revalidation(RevalidationTrigger::Manual, ts(3000, 0, "node-a"));

        let summary = collect_compaction_summary(&engine, &["user/", "order/", "data/"]);
        assert_eq!(summary.checkpoint_count, 2);
        assert_eq!(summary.revalidation_count, 1);

        let user_cp = summary
            .checkpoints
            .iter()
            .find(|cp| cp.key_range == "user/")
            .unwrap();
        assert_eq!(user_cp.digest_hash, "hash-user");
        assert_eq!(user_cp.ops_since_last, 50);
        assert_eq!(user_cp.policy_version, 1);

        let order_cp = summary
            .checkpoints
            .iter()
            .find(|cp| cp.key_range == "order/")
            .unwrap();
        assert_eq!(order_cp.digest_hash, "hash-order");
        assert_eq!(order_cp.ops_since_last, 30);
        assert_eq!(order_cp.policy_version, 2);
    }

    // ---------------------------------------------------------------
    // RetentionSummary tests
    // ---------------------------------------------------------------

    #[test]
    fn retention_summary_defaults() {
        let api = CertifiedApi::new(node("node-1"), default_namespace());
        let summary = collect_retention_summary(&api);

        assert_eq!(summary.evicted_count, 0);
        assert_eq!(summary.pending_writes_count, 0);
        assert_eq!(summary.max_age_ms, 60_000);
        assert_eq!(summary.max_entries, 10_000);
    }

    #[test]
    fn retention_summary_with_custom_policy() {
        let policy = RetentionPolicy {
            max_age_ms: 5_000,
            max_entries: 3,
        };
        let api = CertifiedApi::with_retention(node("node-1"), default_namespace(), policy);
        let summary = collect_retention_summary(&api);

        assert_eq!(summary.max_age_ms, 5_000);
        assert_eq!(summary.max_entries, 3);
    }

    #[test]
    fn retention_summary_tracks_evictions() {
        let policy = RetentionPolicy {
            max_age_ms: 60_000,
            max_entries: 2,
        };
        let mut api = CertifiedApi::with_retention(node("node-1"), default_namespace(), policy);

        // Fill to capacity.
        api.certified_write("a".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        api.certified_write("b".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();

        // This triggers eviction of the oldest.
        api.certified_write("c".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();

        let summary = collect_retention_summary(&api);
        assert!(summary.evicted_count > 0);
        assert!(summary.pending_writes_count <= 2);
    }

    // ---------------------------------------------------------------
    // NodeDiagnostics integration test
    // ---------------------------------------------------------------

    #[test]
    fn node_diagnostics_integration() {
        let mut frontiers = AckFrontierSet::new();
        frontiers.update(make_frontier("auth-1", 100, 0, ""));
        frontiers.update(make_frontier("auth-2", 200, 0, ""));

        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();

        let mut engine = CompactionEngine::with_defaults();
        engine.create_checkpoint(kr(""), ts(50, 0, "node-a"), "hash".into(), PolicyVersion(1));

        let diag = collect_node_diagnostics(&frontiers, &api, &engine, &[""]);

        assert_eq!(diag.frontier_summary.total_entries, 2);
        assert_eq!(diag.certification_summary.total, 1);
        assert_eq!(diag.compaction_summary.checkpoint_count, 1);
        assert_eq!(diag.retention_summary.evicted_count, 0);
    }

    #[test]
    fn node_diagnostics_json_serialization() {
        let frontiers = AckFrontierSet::new();
        let api = CertifiedApi::new(node("node-1"), default_namespace());
        let engine = CompactionEngine::with_defaults();

        let diag = collect_node_diagnostics(&frontiers, &api, &engine, &[]);

        let json = serde_json::to_string_pretty(&diag).expect("should serialize to JSON");
        assert!(json.contains("frontier_summary"));
        assert!(json.contains("certification_summary"));
        assert!(json.contains("compaction_summary"));
        assert!(json.contains("retention_summary"));

        // Roundtrip: verify it's valid JSON.
        let _: serde_json::Value = serde_json::from_str(&json).expect("should be valid JSON");
    }

    #[test]
    fn compaction_summary_skips_missing_prefixes() {
        let engine = CompactionEngine::with_defaults();
        let summary = collect_compaction_summary(&engine, &["nonexistent/", "also/missing/"]);
        assert_eq!(summary.checkpoint_count, 0);
        assert!(summary.checkpoints.is_empty());
    }

    #[test]
    fn frontier_summary_scope_details_are_accurate() {
        let mut set = AckFrontierSet::new();
        set.update(make_frontier("auth-1", 500, 3, "data/"));

        let summary = collect_frontier_summary(&set);
        assert_eq!(summary.scopes.len(), 1);
        let scope = &summary.scopes[0];
        assert_eq!(scope.key_range, "data/");
        assert_eq!(scope.authority_id, "auth-1");
        assert_eq!(scope.policy_version, 1);
        assert_eq!(scope.frontier_hlc.physical, 500);
        assert_eq!(scope.frontier_hlc.logical, 3);
    }
}
