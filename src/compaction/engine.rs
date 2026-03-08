use std::collections::{HashMap, VecDeque};

use serde::{Deserialize, Serialize};

use crate::authority::ack_frontier::AckFrontierSet;
use crate::hlc::HlcTimestamp;
use crate::types::{KeyRange, PolicyVersion};

use super::tuner::AdaptiveCompactionConfig;

/// Configuration for compaction triggers (FR-010).
///
/// Controls when checkpoints are created using a hybrid approach:
/// either a time threshold or an operations count threshold, whichever
/// is reached first.
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Time threshold in milliseconds before triggering a checkpoint (default: 30,000 ms = 30s).
    pub time_threshold_ms: u64,
    /// Number of operations before triggering a checkpoint (default: 10,000).
    pub ops_threshold: u64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            time_threshold_ms: 30_000,
            ops_threshold: 10_000,
        }
    }
}

/// A checkpoint snapshot for a key range.
///
/// Captures the state at a point in time for compaction verification
/// and digest-based consistency checks.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Checkpoint {
    /// The key range this checkpoint covers.
    pub key_range: KeyRange,
    /// The HLC timestamp when this checkpoint was created.
    pub timestamp: HlcTimestamp,
    /// Hex-encoded digest hash of the data at this checkpoint.
    pub digest_hash: String,
    /// The placement policy version in effect at checkpoint time.
    pub policy_version: PolicyVersion,
    /// Number of operations processed since the previous checkpoint.
    pub ops_since_last: u64,
}

/// Reason why revalidation was triggered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RevalidationTrigger {
    /// Digest hash mismatch detected between expected and actual values.
    DigestMismatch { expected: String, actual: String },
    /// Policy version changed, requiring revalidation.
    PolicyVersionChange {
        old: PolicyVersion,
        new: PolicyVersion,
    },
    /// Authority set composition changed.
    AuthorityChange,
    /// Manual revalidation requested via API.
    Manual,
}

/// Compaction engine managing checkpoints and compaction eligibility (FR-010).
///
/// Tracks per-key-range operation counts, creates periodic checkpoints,
/// determines compaction eligibility based on Authority ack_frontiers,
/// and manages revalidation triggers.
pub struct CompactionEngine {
    config: CompactionConfig,
    checkpoints: HashMap<String, Checkpoint>,
    /// Full checkpoint history per key range prefix (newest last).
    checkpoint_history: HashMap<String, VecDeque<Checkpoint>>,
    ops_count: HashMap<String, u64>,
    revalidation_log: Vec<(HlcTimestamp, RevalidationTrigger)>,
    /// Optional adaptive tuning configuration.
    adaptive_config: Option<AdaptiveCompactionConfig>,
    /// Timestamp (ms since epoch) when this engine was created, used as the
    /// base for the time threshold when no prior checkpoint exists.
    created_at_ms: u64,
}

impl CompactionEngine {
    fn now_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    /// Create a new compaction engine with the given configuration.
    pub fn new(config: CompactionConfig) -> Self {
        Self {
            config,
            checkpoints: HashMap::new(),
            checkpoint_history: HashMap::new(),
            ops_count: HashMap::new(),
            revalidation_log: Vec::new(),
            adaptive_config: None,
            created_at_ms: Self::now_ms(),
        }
    }

    /// Create a new compaction engine with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(CompactionConfig::default())
    }

    /// Create a new compaction engine with adaptive tuning enabled.
    pub fn with_adaptive(adaptive: AdaptiveCompactionConfig) -> Self {
        let config = adaptive.effective().clone();
        Self {
            config,
            checkpoints: HashMap::new(),
            checkpoint_history: HashMap::new(),
            ops_count: HashMap::new(),
            revalidation_log: Vec::new(),
            adaptive_config: Some(adaptive),
            created_at_ms: Self::now_ms(),
        }
    }

    /// Return a reference to the adaptive config, if enabled.
    pub fn adaptive_config(&self) -> Option<&AdaptiveCompactionConfig> {
        self.adaptive_config.as_ref()
    }

    /// Return a mutable reference to the adaptive config, if enabled.
    pub fn adaptive_config_mut(&mut self) -> Option<&mut AdaptiveCompactionConfig> {
        self.adaptive_config.as_mut()
    }

    /// Run a tuning cycle. Call this periodically (e.g. every 30s).
    ///
    /// Updates the effective config from the adaptive tuner. Returns
    /// `true` if thresholds changed.
    pub fn tune(&mut self, now_ms: u64, avg_frontier_lag_ms: Option<u64>) -> bool {
        if let Some(ref mut adaptive) = self.adaptive_config {
            let changed = adaptive.tune(now_ms, avg_frontier_lag_ms);
            if changed {
                self.config = adaptive.effective().clone();
            }
            changed
        } else {
            false
        }
    }

    /// Produce a diagnostic snapshot of the current tuning state.
    ///
    /// Returns `None` if adaptive tuning is not enabled.
    pub fn tuning_snapshot(&self, now_ms: u64) -> Option<super::tuner::TuningSnapshot> {
        self.adaptive_config
            .as_ref()
            .map(|a| a.tuning_snapshot(now_ms))
    }

    /// Record an operation for the given key range, incrementing its ops counter.
    ///
    /// Uses the system clock to feed the adaptive write rate tracker (if enabled).
    /// Prefer [`record_op_at`] when an explicit timestamp is available for
    /// deterministic testing.
    pub fn record_op(&mut self, key_range: &KeyRange) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.record_op_at(key_range, now_ms);
    }

    /// Record an operation for the given key range at a specific timestamp.
    ///
    /// Increments the ops counter and feeds the adaptive write rate tracker
    /// (if enabled) so that `tune()` can observe real write rates.
    pub fn record_op_at(&mut self, key_range: &KeyRange, now_ms: u64) {
        *self.ops_count.entry(key_range.prefix.clone()).or_insert(0) += 1;
        if let Some(ref mut adaptive) = self.adaptive_config {
            adaptive.record_ops(&key_range.prefix, now_ms, 1);
        }
    }

    /// Check whether a checkpoint should be created for the given key range.
    ///
    /// Returns `true` if either the operations count threshold or the time
    /// threshold has been reached since the last checkpoint.
    ///
    /// When no prior checkpoint exists and at least one operation has been
    /// recorded, the time threshold is evaluated against the engine's
    /// creation time to ensure the first checkpoint is eventually created.
    pub fn should_checkpoint(&self, key_range: &KeyRange, now: &HlcTimestamp) -> bool {
        let prefix = &key_range.prefix;

        // Check ops threshold
        let ops = self.ops_count.get(prefix).copied().unwrap_or(0);
        if ops >= self.config.ops_threshold {
            return true;
        }

        // Check time threshold
        if let Some(cp) = self.checkpoints.get(prefix) {
            let elapsed = now.physical.saturating_sub(cp.timestamp.physical);
            if elapsed >= self.config.time_threshold_ms {
                return true;
            }
        } else if ops > 0 {
            // No prior checkpoint exists but there are pending ops.
            // Use the engine creation time as the base so the first
            // checkpoint is created once the time threshold elapses.
            let elapsed = now.physical.saturating_sub(self.created_at_ms);
            if elapsed >= self.config.time_threshold_ms {
                return true;
            }
        }

        false
    }

    /// Create a checkpoint for the given key range.
    ///
    /// Records the current state, resets the operations counter for this range,
    /// and applies the retention policy (evicting the oldest checkpoint if the
    /// history exceeds `max_checkpoint_history`).
    pub fn create_checkpoint(
        &mut self,
        key_range: KeyRange,
        now: HlcTimestamp,
        digest_hash: String,
        policy_version: PolicyVersion,
    ) -> Checkpoint {
        let prefix = key_range.prefix.clone();
        let ops_since_last = self.ops_count.get(&prefix).copied().unwrap_or(0);

        let checkpoint = Checkpoint {
            key_range,
            timestamp: now,
            digest_hash,
            policy_version,
            ops_since_last,
        };

        // Store in history with retention enforcement.
        let history = self.checkpoint_history.entry(prefix.clone()).or_default();
        history.push_back(checkpoint.clone());

        let max_history = self
            .adaptive_config
            .as_ref()
            .map(|a| a.max_checkpoint_history)
            .unwrap_or(usize::MAX);
        while history.len() > max_history {
            history.pop_front();
        }

        self.checkpoints.insert(prefix.clone(), checkpoint.clone());
        self.ops_count.insert(prefix, 0);

        checkpoint
    }

    /// Get the checkpoint history for a key range prefix.
    pub fn checkpoint_history(&self, prefix: &str) -> Option<&VecDeque<Checkpoint>> {
        self.checkpoint_history.get(prefix)
    }

    /// Get the latest checkpoint for a key range prefix.
    pub fn get_checkpoint(&self, prefix: &str) -> Option<&Checkpoint> {
        self.checkpoints.get(prefix)
    }

    /// Check whether data for a key range can be compacted.
    ///
    /// Compaction is safe only when the majority of authorities **within the
    /// same key_range and policy_version scope** have consumed updates past
    /// the checkpoint's timestamp (FR-010).  Frontiers from other key ranges
    /// or policy versions are excluded to prevent cross-scope contamination.
    pub fn is_compactable(
        &self,
        prefix: &str,
        frontiers: &AckFrontierSet,
        total_authorities: usize,
    ) -> bool {
        let checkpoint = match self.checkpoints.get(prefix) {
            Some(cp) => cp,
            None => return false,
        };

        frontiers.is_certified_at_for_scope(
            &checkpoint.timestamp,
            &checkpoint.key_range,
            &checkpoint.policy_version,
            total_authorities,
        )
    }

    /// Verify the digest hash for a key range against the stored checkpoint.
    ///
    /// Returns `Ok(())` if the digests match, or `Err(RevalidationTrigger::DigestMismatch)`
    /// if they differ.
    pub fn verify_digest(
        &self,
        prefix: &str,
        actual_hash: &str,
    ) -> Result<(), RevalidationTrigger> {
        match self.checkpoints.get(prefix) {
            Some(cp) if cp.digest_hash == actual_hash => Ok(()),
            Some(cp) => Err(RevalidationTrigger::DigestMismatch {
                expected: cp.digest_hash.clone(),
                actual: actual_hash.to_string(),
            }),
            None => Ok(()),
        }
    }

    /// Maximum number of revalidation log entries to retain.
    const MAX_REVALIDATION_LOG: usize = 1000;

    /// Log a revalidation event with the given trigger and timestamp.
    pub fn trigger_revalidation(&mut self, trigger: RevalidationTrigger, now: HlcTimestamp) {
        self.revalidation_log.push((now, trigger));
        if self.revalidation_log.len() > Self::MAX_REVALIDATION_LOG {
            self.revalidation_log
                .drain(..self.revalidation_log.len() - Self::MAX_REVALIDATION_LOG);
        }
    }

    /// Get the full revalidation log.
    pub fn revalidation_log(&self) -> &[(HlcTimestamp, RevalidationTrigger)] {
        &self.revalidation_log
    }

    /// Request a manual revalidation (FR-010 manual revalidation API).
    pub fn request_manual_revalidation(&mut self, now: HlcTimestamp) {
        self.trigger_revalidation(RevalidationTrigger::Manual, now);
    }

    /// Run a full compaction cycle for a key range:
    /// 1. Create a checkpoint (if the threshold is met).
    /// 2. Check compaction eligibility (majority of authorities past the checkpoint).
    /// 3. Prune old operation-log timestamps from the store.
    ///
    /// Returns the number of timestamp entries pruned, or 0 if compaction
    /// was not eligible or no checkpoint existed.
    #[allow(clippy::too_many_arguments)]
    pub fn run_compaction(
        &mut self,
        key_range: &KeyRange,
        now: HlcTimestamp,
        digest_hash: String,
        policy_version: PolicyVersion,
        frontiers: &AckFrontierSet,
        total_authorities: usize,
        store: &mut crate::store::kv::Store,
    ) -> usize {
        let prefix = &key_range.prefix;

        // Step 1: create checkpoint if threshold is reached.
        if self.should_checkpoint(key_range, &now) {
            self.create_checkpoint(key_range.clone(), now.clone(), digest_hash, policy_version);
        }

        // Step 2: check if compaction is safe (majority of authorities are past
        // the checkpoint frontier).
        if !self.is_compactable(prefix, frontiers, total_authorities) {
            return 0;
        }

        // Step 3: prune old timestamps from the store up to the checkpoint frontier.
        let checkpoint = match self.checkpoints.get(prefix) {
            Some(cp) => cp.clone(),
            None => return 0,
        };

        store.prune_timestamps_before(prefix, &checkpoint.timestamp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authority::ack_frontier::{AckFrontier, AckFrontierSet};
    use crate::compaction::tuner::AdaptiveCompactionConfig;
    use crate::types::NodeId;

    fn make_ts(physical: u64, logical: u32, node: &str) -> HlcTimestamp {
        HlcTimestamp {
            physical,
            logical,
            node_id: node.into(),
        }
    }

    fn make_key_range(prefix: &str) -> KeyRange {
        KeyRange {
            prefix: prefix.into(),
        }
    }

    fn make_frontier(authority: &str, physical: u64, prefix: &str) -> AckFrontier {
        AckFrontier {
            authority_id: NodeId(authority.into()),
            frontier_hlc: make_ts(physical, 0, authority),
            key_range: make_key_range(prefix),
            policy_version: PolicyVersion(1),
            digest_hash: format!("{authority}-{physical}"),
        }
    }

    #[test]
    fn record_op_increments_count() {
        let mut engine = CompactionEngine::with_defaults();
        let kr = make_key_range("user/");

        engine.record_op(&kr);
        engine.record_op(&kr);
        engine.record_op(&kr);

        assert_eq!(engine.ops_count.get("user/"), Some(&3));
    }

    #[test]
    fn should_checkpoint_below_threshold_returns_false() {
        let mut engine = CompactionEngine::with_defaults();
        let kr = make_key_range("user/");
        let now = make_ts(1000, 0, "node-a");

        // Record a few ops, well below the 10,000 threshold
        for _ in 0..100 {
            engine.record_op(&kr);
        }

        assert!(!engine.should_checkpoint(&kr, &now));
    }

    #[test]
    fn should_checkpoint_ops_threshold_reached() {
        let mut engine = CompactionEngine::new(CompactionConfig {
            time_threshold_ms: 30_000,
            ops_threshold: 5,
        });
        let kr = make_key_range("user/");
        let now = make_ts(1000, 0, "node-a");

        for _ in 0..5 {
            engine.record_op(&kr);
        }

        assert!(engine.should_checkpoint(&kr, &now));
    }

    #[test]
    fn should_checkpoint_time_threshold_reached() {
        let mut engine = CompactionEngine::new(CompactionConfig {
            time_threshold_ms: 1_000,
            ops_threshold: 10_000,
        });
        let kr = make_key_range("user/");

        // Create an initial checkpoint at t=1000
        engine.create_checkpoint(
            kr.clone(),
            make_ts(1000, 0, "node-a"),
            "hash1".into(),
            PolicyVersion(1),
        );

        // Record 1 op (below ops threshold)
        engine.record_op(&kr);

        // Now is 2001ms later (past the 1000ms time threshold)
        let now = make_ts(2001, 0, "node-a");
        assert!(engine.should_checkpoint(&kr, &now));
    }

    #[test]
    fn create_checkpoint_resets_ops_count() {
        let mut engine = CompactionEngine::with_defaults();
        let kr = make_key_range("user/");

        for _ in 0..50 {
            engine.record_op(&kr);
        }

        let cp = engine.create_checkpoint(
            kr.clone(),
            make_ts(1000, 0, "node-a"),
            "digest-abc".into(),
            PolicyVersion(1),
        );

        assert_eq!(cp.ops_since_last, 50);
        assert_eq!(engine.ops_count.get("user/"), Some(&0));
    }

    #[test]
    fn get_checkpoint_returns_latest() {
        let mut engine = CompactionEngine::with_defaults();
        let kr = make_key_range("user/");

        engine.create_checkpoint(
            kr.clone(),
            make_ts(1000, 0, "node-a"),
            "hash1".into(),
            PolicyVersion(1),
        );
        engine.create_checkpoint(
            kr.clone(),
            make_ts(2000, 0, "node-a"),
            "hash2".into(),
            PolicyVersion(2),
        );

        let cp = engine.get_checkpoint("user/").unwrap();
        assert_eq!(cp.digest_hash, "hash2");
        assert_eq!(cp.policy_version, PolicyVersion(2));
    }

    #[test]
    fn is_compactable_with_majority_ahead() {
        let mut engine = CompactionEngine::with_defaults();
        let kr = make_key_range("user/");

        // Checkpoint at t=100
        engine.create_checkpoint(
            kr.clone(),
            make_ts(100, 0, "node-a"),
            "hash".into(),
            PolicyVersion(1),
        );

        // 3 authorities, all past the checkpoint
        let mut frontiers = AckFrontierSet::new();
        frontiers.update(make_frontier("auth-1", 200, "user/"));
        frontiers.update(make_frontier("auth-2", 300, "user/"));
        frontiers.update(make_frontier("auth-3", 150, "user/"));

        assert!(engine.is_compactable("user/", &frontiers, 3));
    }

    #[test]
    fn is_compactable_with_frontiers_behind() {
        let mut engine = CompactionEngine::with_defaults();
        let kr = make_key_range("user/");

        // Checkpoint at t=500
        engine.create_checkpoint(
            kr.clone(),
            make_ts(500, 0, "node-a"),
            "hash".into(),
            PolicyVersion(1),
        );

        // 3 authorities, most behind the checkpoint
        let mut frontiers = AckFrontierSet::new();
        frontiers.update(make_frontier("auth-1", 100, "user/"));
        frontiers.update(make_frontier("auth-2", 200, "user/"));
        frontiers.update(make_frontier("auth-3", 600, "user/"));

        // majority frontier with 3 authorities: sorted [100, 200, 600], majority=2, index=1 → 200
        // 200 < 500, so not compactable
        assert!(!engine.is_compactable("user/", &frontiers, 3));
    }

    #[test]
    fn is_compactable_no_checkpoint() {
        let engine = CompactionEngine::with_defaults();
        let frontiers = AckFrontierSet::new();

        assert!(!engine.is_compactable("user/", &frontiers, 3));
    }

    #[test]
    fn verify_digest_match() {
        let mut engine = CompactionEngine::with_defaults();
        let kr = make_key_range("user/");

        engine.create_checkpoint(
            kr,
            make_ts(1000, 0, "node-a"),
            "abc123".into(),
            PolicyVersion(1),
        );

        assert!(engine.verify_digest("user/", "abc123").is_ok());
    }

    #[test]
    fn verify_digest_mismatch() {
        let mut engine = CompactionEngine::with_defaults();
        let kr = make_key_range("user/");

        engine.create_checkpoint(
            kr,
            make_ts(1000, 0, "node-a"),
            "abc123".into(),
            PolicyVersion(1),
        );

        let result = engine.verify_digest("user/", "xyz789");
        assert_eq!(
            result,
            Err(RevalidationTrigger::DigestMismatch {
                expected: "abc123".into(),
                actual: "xyz789".into(),
            })
        );
    }

    #[test]
    fn verify_digest_no_checkpoint() {
        let engine = CompactionEngine::with_defaults();
        assert!(engine.verify_digest("user/", "anything").is_ok());
    }

    #[test]
    fn trigger_revalidation_logs_events() {
        let mut engine = CompactionEngine::with_defaults();

        engine.trigger_revalidation(
            RevalidationTrigger::AuthorityChange,
            make_ts(1000, 0, "node-a"),
        );
        engine.trigger_revalidation(
            RevalidationTrigger::PolicyVersionChange {
                old: PolicyVersion(1),
                new: PolicyVersion(2),
            },
            make_ts(2000, 0, "node-a"),
        );

        let log = engine.revalidation_log();
        assert_eq!(log.len(), 2);
        assert_eq!(log[0].1, RevalidationTrigger::AuthorityChange);
        assert_eq!(
            log[1].1,
            RevalidationTrigger::PolicyVersionChange {
                old: PolicyVersion(1),
                new: PolicyVersion(2),
            }
        );
    }

    #[test]
    fn request_manual_revalidation() {
        let mut engine = CompactionEngine::with_defaults();

        engine.request_manual_revalidation(make_ts(5000, 0, "node-a"));

        let log = engine.revalidation_log();
        assert_eq!(log.len(), 1);
        assert_eq!(log[0].1, RevalidationTrigger::Manual);
        assert_eq!(log[0].0.physical, 5000);
    }

    #[test]
    fn is_compactable_not_affected_by_other_key_range() {
        let mut engine = CompactionEngine::with_defaults();
        let kr_user = make_key_range("user/");

        // Checkpoint at t=100 for user/
        engine.create_checkpoint(
            kr_user.clone(),
            make_ts(100, 0, "node-a"),
            "hash".into(),
            PolicyVersion(1),
        );

        // 3 authorities, but all frontiers are for order/ (different key range)
        let mut frontiers = AckFrontierSet::new();
        frontiers.update(make_frontier("auth-1", 200, "order/"));
        frontiers.update(make_frontier("auth-2", 300, "order/"));
        frontiers.update(make_frontier("auth-3", 400, "order/"));

        // Should NOT be compactable: no user/ frontiers exist
        assert!(!engine.is_compactable("user/", &frontiers, 3));
    }

    #[test]
    fn is_compactable_not_affected_by_other_policy_version() {
        let mut engine = CompactionEngine::with_defaults();
        let kr = make_key_range("user/");

        // Checkpoint at t=100 for user/ with policy_version=1
        engine.create_checkpoint(
            kr.clone(),
            make_ts(100, 0, "node-a"),
            "hash".into(),
            PolicyVersion(1),
        );

        // 3 authorities, but all frontiers are for policy_version=2
        let mut frontiers = AckFrontierSet::new();
        frontiers.update(AckFrontier {
            authority_id: NodeId("auth-1".into()),
            frontier_hlc: make_ts(200, 0, "auth-1"),
            key_range: make_key_range("user/"),
            policy_version: PolicyVersion(2),
            digest_hash: "auth-1-200".into(),
        });
        frontiers.update(AckFrontier {
            authority_id: NodeId("auth-2".into()),
            frontier_hlc: make_ts(300, 0, "auth-2"),
            key_range: make_key_range("user/"),
            policy_version: PolicyVersion(2),
            digest_hash: "auth-2-300".into(),
        });
        frontiers.update(AckFrontier {
            authority_id: NodeId("auth-3".into()),
            frontier_hlc: make_ts(400, 0, "auth-3"),
            key_range: make_key_range("user/"),
            policy_version: PolicyVersion(2),
            digest_hash: "auth-3-400".into(),
        });

        // Should NOT be compactable: no policy_version=1 frontiers
        assert!(!engine.is_compactable("user/", &frontiers, 3));
    }

    #[test]
    fn is_compactable_scoped_majority_only() {
        let mut engine = CompactionEngine::with_defaults();
        let kr = make_key_range("user/");

        // Checkpoint at t=100 for user/
        engine.create_checkpoint(
            kr.clone(),
            make_ts(100, 0, "node-a"),
            "hash".into(),
            PolicyVersion(1),
        );

        let mut frontiers = AckFrontierSet::new();
        // 1 authority in user/ scope (insufficient for majority of 3)
        frontiers.update(make_frontier("auth-1", 200, "user/"));
        // 2 authorities in order/ scope (irrelevant)
        frontiers.update(make_frontier("auth-2", 300, "order/"));
        frontiers.update(make_frontier("auth-3", 400, "order/"));

        // Should NOT be compactable: only 1 of 3 in user/ scope
        assert!(!engine.is_compactable("user/", &frontiers, 3));
    }

    #[test]
    fn is_compactable_with_matching_scope() {
        let mut engine = CompactionEngine::with_defaults();
        let kr = make_key_range("user/");

        // Checkpoint at t=100 for user/ with policy_version=1
        engine.create_checkpoint(
            kr.clone(),
            make_ts(100, 0, "node-a"),
            "hash".into(),
            PolicyVersion(1),
        );

        let mut frontiers = AckFrontierSet::new();
        // 2 of 3 authorities in correct scope (user/, policy_version=1)
        frontiers.update(make_frontier("auth-1", 200, "user/"));
        frontiers.update(make_frontier("auth-2", 300, "user/"));
        // 1 authority in wrong scope
        frontiers.update(make_frontier("auth-3", 400, "order/"));

        // Should be compactable: 2 of 3 in user/ scope past t=100
        assert!(engine.is_compactable("user/", &frontiers, 3));
    }

    #[test]
    fn compaction_config_defaults() {
        let config = CompactionConfig::default();
        assert_eq!(config.time_threshold_ms, 30_000);
        assert_eq!(config.ops_threshold, 10_000);
    }

    #[test]
    fn checkpoint_serde_roundtrip() {
        let cp = Checkpoint {
            key_range: make_key_range("order/"),
            timestamp: make_ts(1_700_000_000_000, 42, "node-x"),
            digest_hash: "deadbeef".into(),
            policy_version: PolicyVersion(3),
            ops_since_last: 1234,
        };

        let json = serde_json::to_string(&cp).expect("serialize");
        let back: Checkpoint = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(cp, back);
    }

    #[test]
    fn multiple_key_ranges_independent() {
        let mut engine = CompactionEngine::new(CompactionConfig {
            time_threshold_ms: 30_000,
            ops_threshold: 3,
        });

        let kr_user = make_key_range("user/");
        let kr_order = make_key_range("order/");

        engine.record_op(&kr_user);
        engine.record_op(&kr_user);
        engine.record_op(&kr_order);

        let now = make_ts(1000, 0, "node-a");

        // user/ has 2 ops (below 3), order/ has 1 op (below 3)
        assert!(!engine.should_checkpoint(&kr_user, &now));
        assert!(!engine.should_checkpoint(&kr_order, &now));

        // Push user/ over threshold
        engine.record_op(&kr_user);
        assert!(engine.should_checkpoint(&kr_user, &now));
        assert!(!engine.should_checkpoint(&kr_order, &now));
    }

    // ---------------------------------------------------------------
    // Checkpoint history and retention policy tests
    // ---------------------------------------------------------------

    #[test]
    fn checkpoint_history_accumulated() {
        let mut engine = CompactionEngine::with_defaults();
        let kr = make_key_range("user/");

        for i in 0..5 {
            engine.create_checkpoint(
                kr.clone(),
                make_ts(1000 * (i + 1), 0, "node-a"),
                format!("hash-{i}"),
                PolicyVersion(1),
            );
        }

        let history = engine.checkpoint_history("user/").unwrap();
        assert_eq!(history.len(), 5);
        assert_eq!(history.front().unwrap().digest_hash, "hash-0");
        assert_eq!(history.back().unwrap().digest_hash, "hash-4");
    }

    #[test]
    fn retention_policy_evicts_oldest() {
        let base = CompactionConfig {
            time_threshold_ms: 30_000,
            ops_threshold: 10_000,
        };
        let mut adaptive = AdaptiveCompactionConfig::new(base);
        adaptive.max_checkpoint_history = 3;

        let mut engine = CompactionEngine::with_adaptive(adaptive);
        let kr = make_key_range("user/");

        // Create 5 checkpoints; only last 3 should remain.
        for i in 0..5 {
            engine.create_checkpoint(
                kr.clone(),
                make_ts(1000 * (i + 1), 0, "node-a"),
                format!("hash-{i}"),
                PolicyVersion(1),
            );
        }

        let history = engine.checkpoint_history("user/").unwrap();
        assert_eq!(history.len(), 3);
        // Oldest surviving should be hash-2
        assert_eq!(history.front().unwrap().digest_hash, "hash-2");
        assert_eq!(history.back().unwrap().digest_hash, "hash-4");
    }

    #[test]
    fn no_retention_limit_without_adaptive() {
        let mut engine = CompactionEngine::with_defaults();
        let kr = make_key_range("user/");

        // Without adaptive, history is unlimited.
        for i in 0..20 {
            engine.create_checkpoint(
                kr.clone(),
                make_ts(1000 * (i + 1), 0, "node-a"),
                format!("hash-{i}"),
                PolicyVersion(1),
            );
        }

        let history = engine.checkpoint_history("user/").unwrap();
        assert_eq!(history.len(), 20);
    }

    // ---------------------------------------------------------------
    // Adaptive engine integration tests
    // ---------------------------------------------------------------

    #[test]
    fn adaptive_engine_tune_updates_config() {
        let base = CompactionConfig {
            time_threshold_ms: 30_000,
            ops_threshold: 10_000,
        };
        let mut adaptive = AdaptiveCompactionConfig::with_write_rate_window(base, 10_000);
        adaptive.set_tuning_interval_ms(0);

        let mut engine = CompactionEngine::with_adaptive(adaptive);

        // Record high write rate (> 750 ops/sec dead zone boundary).
        if let Some(ac) = engine.adaptive_config_mut() {
            ac.record_ops("user/", 1_000, 800);
        }

        let changed = engine.tune(2_000, None);
        assert!(changed);

        // Config should be updated.
        let kr = make_key_range("user/");
        let now = make_ts(2_000, 0, "node-a");
        // With ops_threshold now halved to 5000, 5000 ops should trigger.
        for _ in 0..5_000 {
            engine.record_op(&kr);
        }
        assert!(engine.should_checkpoint(&kr, &now));
    }

    #[test]
    fn tuning_snapshot_returns_none_without_adaptive() {
        let engine = CompactionEngine::with_defaults();
        assert!(engine.tuning_snapshot(1_000).is_none());
    }

    #[test]
    fn tuning_snapshot_returns_some_with_adaptive() {
        let base = CompactionConfig::default();
        let adaptive = AdaptiveCompactionConfig::new(base);
        let engine = CompactionEngine::with_adaptive(adaptive);

        let snap = engine.tuning_snapshot(1_000);
        assert!(snap.is_some());
        let snap = snap.unwrap();
        assert_eq!(snap.effective_ops_threshold, 10_000);
        assert_eq!(snap.effective_time_threshold_ms, 30_000);
    }

    // ---------------------------------------------------------------
    // record_op feeds adaptive write rate tracker
    // ---------------------------------------------------------------

    #[test]
    fn record_op_at_feeds_write_rate_tracker() {
        let base = CompactionConfig {
            time_threshold_ms: 30_000,
            ops_threshold: 10_000,
        };
        let adaptive = AdaptiveCompactionConfig::with_write_rate_window(base, 10_000);
        let mut engine = CompactionEngine::with_adaptive(adaptive);
        let kr = make_key_range("user/");

        // Record 100 ops at known timestamps via record_op_at
        for i in 0..100 {
            engine.record_op_at(&kr, 1_000 + i * 10);
        }

        // The write rate tracker should now have data
        let ac = engine.adaptive_config().unwrap();
        let rate = ac.write_rate("user/", 2_000);
        assert!(rate > 0.0, "write rate should be positive, got {rate}");
    }

    #[test]
    fn record_op_at_does_not_feed_tracker_without_adaptive() {
        let mut engine = CompactionEngine::with_defaults();
        let kr = make_key_range("user/");

        // Should not panic even without adaptive config
        engine.record_op_at(&kr, 1_000);
        assert_eq!(engine.ops_count.get("user/"), Some(&1));
    }

    #[test]
    fn record_op_at_drives_tuning() {
        let base = CompactionConfig {
            time_threshold_ms: 30_000,
            ops_threshold: 10_000,
        };
        let mut adaptive = AdaptiveCompactionConfig::with_write_rate_window(base, 10_000);
        adaptive.set_tuning_interval_ms(0);

        let mut engine = CompactionEngine::with_adaptive(adaptive);
        let kr = make_key_range("user/");

        // Record enough ops via record_op_at to push rate above 750 ops/sec.
        // 800 ops in 1 second = 800 ops/sec > 750.
        for i in 0..800 {
            engine.record_op_at(&kr, 1_000 + i);
        }

        // Tune should detect the high rate and halve ops_threshold.
        let changed = engine.tune(2_000, None);
        assert!(changed);
        assert_eq!(
            engine.adaptive_config().unwrap().effective().ops_threshold,
            5_000
        );
    }

    // ---------------------------------------------------------------
    // run_compaction tests (#253)
    // ---------------------------------------------------------------

    #[test]
    fn run_compaction_creates_checkpoint_and_prunes() {
        let mut engine = CompactionEngine::new(CompactionConfig {
            time_threshold_ms: 30_000,
            ops_threshold: 3,
        });
        let kr = make_key_range("user/");

        // Record enough ops to trigger checkpoint.
        for _ in 0..5 {
            engine.record_op(&kr);
        }

        // Build a store with timestamps.
        let mut store = crate::store::kv::Store::new();
        let counter = crate::crdt::pn_counter::PnCounter::new();
        store.put(
            "user/a".into(),
            crate::store::kv::CrdtValue::Counter(counter.clone()),
        );
        store.record_change("user/a", make_ts(50, 0, "n"));
        store.put(
            "user/b".into(),
            crate::store::kv::CrdtValue::Counter(counter.clone()),
        );
        store.record_change("user/b", make_ts(200, 0, "n"));

        // Build frontiers: all 3 authorities past t=100.
        let mut frontiers = AckFrontierSet::new();
        frontiers.update(make_frontier("auth-1", 200, "user/"));
        frontiers.update(make_frontier("auth-2", 300, "user/"));
        frontiers.update(make_frontier("auth-3", 150, "user/"));

        let pruned = engine.run_compaction(
            &kr,
            make_ts(100, 0, "node-a"),
            "digest-100".into(),
            PolicyVersion(1),
            &frontiers,
            3,
            &mut store,
        );

        // Checkpoint at t=100; user/a (ts=50) should be pruned, user/b (ts=200) kept.
        assert_eq!(pruned, 1);
        assert!(engine.get_checkpoint("user/").is_some());
    }

    #[test]
    fn run_compaction_not_eligible_returns_zero() {
        let mut engine = CompactionEngine::new(CompactionConfig {
            time_threshold_ms: 30_000,
            ops_threshold: 3,
        });
        let kr = make_key_range("user/");

        for _ in 0..5 {
            engine.record_op(&kr);
        }

        let mut store = crate::store::kv::Store::new();
        let counter = crate::crdt::pn_counter::PnCounter::new();
        store.put(
            "user/a".into(),
            crate::store::kv::CrdtValue::Counter(counter),
        );
        store.record_change("user/a", make_ts(50, 0, "n"));

        // Frontiers behind the checkpoint — not eligible.
        let mut frontiers = AckFrontierSet::new();
        frontiers.update(make_frontier("auth-1", 10, "user/"));
        frontiers.update(make_frontier("auth-2", 20, "user/"));
        frontiers.update(make_frontier("auth-3", 30, "user/"));

        let pruned = engine.run_compaction(
            &kr,
            make_ts(100, 0, "node-a"),
            "digest-100".into(),
            PolicyVersion(1),
            &frontiers,
            3,
            &mut store,
        );

        assert_eq!(pruned, 0);
        // Checkpoint should still have been created.
        assert!(engine.get_checkpoint("user/").is_some());
    }

    #[test]
    fn run_compaction_no_checkpoint_returns_zero() {
        let mut engine = CompactionEngine::with_defaults();
        let kr = make_key_range("user/");

        let mut store = crate::store::kv::Store::new();
        let frontiers = AckFrontierSet::new();

        // No ops recorded, no checkpoint => 0.
        let pruned = engine.run_compaction(
            &kr,
            make_ts(100, 0, "node-a"),
            "digest-100".into(),
            PolicyVersion(1),
            &frontiers,
            3,
            &mut store,
        );

        assert_eq!(pruned, 0);
    }
}
