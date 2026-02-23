use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::{Mutex, watch};

use crate::api::certified::CertifiedApi;
use crate::api::eventual::EventualApi;
use crate::authority::frontier_reporter::FrontierReporter;
use crate::compaction::CompactionEngine;
use crate::hlc::Hlc;
use crate::network::sync::SyncClient;
use crate::ops::metrics::RuntimeMetrics;
use crate::types::{CertificationStatus, NodeId};

/// Configuration for the background processing intervals of [`NodeRunner`].
#[derive(Debug, Clone)]
pub struct NodeRunnerConfig {
    /// How often to re-evaluate pending writes against authority frontiers.
    pub certification_interval: Duration,
    /// How often to run cleanup (expire + remove completed pending writes).
    pub cleanup_interval: Duration,
    /// How often to check compaction eligibility and create checkpoints.
    pub compaction_check_interval: Duration,
    /// How often Authority nodes report their frontier and push to peers.
    /// Default: 1 second. Only effective when this node is an authority.
    pub frontier_report_interval: Duration,
    /// How often to run anti-entropy sync with peers.
    /// Set to `None` to disable sync (e.g. when no peers are configured).
    pub sync_interval: Option<Duration>,
}

impl Default for NodeRunnerConfig {
    fn default() -> Self {
        Self {
            certification_interval: Duration::from_secs(1),
            cleanup_interval: Duration::from_secs(5),
            compaction_check_interval: Duration::from_secs(10),
            frontier_report_interval: Duration::from_secs(1),
            sync_interval: Some(Duration::from_secs(2)),
        }
    }
}

/// Node execution loop that drives background processing.
///
/// Owns the `CertifiedApi` and `CompactionEngine` and periodically runs:
/// - `process_certifications`: re-evaluates pending writes against frontiers
/// - `cleanup`: expires old pending writes and removes completed entries
/// - compaction checkpoint checks
/// - **frontier reporting**: if this node is an Authority, automatically
///   generates and applies frontier updates (removing the need for manual
///   `update_frontier` calls)
///
/// Supports graceful shutdown via a watch channel.
pub struct NodeRunner {
    node_id: NodeId,
    certified_api: CertifiedApi,
    compaction_engine: CompactionEngine,
    clock: Hlc,
    config: NodeRunnerConfig,
    frontier_reporter: Option<FrontierReporter>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
    /// Optional sync client for anti-entropy replication.
    sync_client: Option<SyncClient>,
    /// Shared reference to the eventual API for reading store state during sync.
    eventual_api: Option<Arc<Mutex<EventualApi>>>,
    /// Runtime metrics for operational monitoring.
    metrics: Arc<RuntimeMetrics>,
}

/// Counters returned after the run loop exits, useful for testing and observability.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunLoopStats {
    /// Number of certification processing ticks executed.
    pub certification_ticks: u64,
    /// Number of cleanup ticks executed.
    pub cleanup_ticks: u64,
    /// Number of compaction check ticks executed.
    pub compaction_check_ticks: u64,
    /// Number of frontier report ticks executed.
    pub frontier_report_ticks: u64,
    /// Number of anti-entropy sync ticks executed.
    pub sync_ticks: u64,
}

impl NodeRunner {
    /// Create a new `NodeRunner` without anti-entropy sync.
    ///
    /// Automatically discovers whether this node is an authority and
    /// configures the frontier reporter accordingly.
    pub fn new(
        node_id: NodeId,
        certified_api: CertifiedApi,
        compaction_engine: CompactionEngine,
        config: NodeRunnerConfig,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        let reporter = {
            let ns = certified_api.namespace().read().unwrap();
            FrontierReporter::new(node_id.clone(), &ns)
        };
        let frontier_reporter = if reporter.is_authority() {
            Some(reporter)
        } else {
            None
        };

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Self {
            clock: Hlc::new(node_id.0.clone()),
            node_id,
            certified_api,
            compaction_engine,
            config,
            frontier_reporter,
            shutdown_tx,
            shutdown_rx,
            sync_client: None,
            eventual_api: None,
            metrics,
        }
    }

    /// Create a new `NodeRunner` with anti-entropy sync enabled.
    ///
    /// The `eventual_api` must be the same `Arc<Mutex<EventualApi>>` shared
    /// with the HTTP handlers so that sync reads the latest store state.
    pub fn with_sync(
        node_id: NodeId,
        certified_api: CertifiedApi,
        compaction_engine: CompactionEngine,
        config: NodeRunnerConfig,
        sync_client: SyncClient,
        eventual_api: Arc<Mutex<EventualApi>>,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        let reporter = {
            let ns = certified_api.namespace().read().unwrap();
            FrontierReporter::new(node_id.clone(), &ns)
        };
        let frontier_reporter = if reporter.is_authority() {
            Some(reporter)
        } else {
            None
        };

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Self {
            clock: Hlc::new(node_id.0.clone()),
            node_id,
            certified_api,
            compaction_engine,
            config,
            frontier_reporter,
            shutdown_tx,
            shutdown_rx,
            sync_client: Some(sync_client),
            eventual_api: Some(eventual_api),
            metrics,
        }
    }

    /// Return a shutdown handle that can be used to signal graceful shutdown.
    ///
    /// Sending `true` on the returned sender causes `run()` to exit after the
    /// current tick completes.
    pub fn shutdown_handle(&self) -> watch::Sender<bool> {
        self.shutdown_tx.clone()
    }

    /// Return a reference to the node ID.
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Return a reference to the `CertifiedApi`.
    pub fn certified_api(&self) -> &CertifiedApi {
        &self.certified_api
    }

    /// Return a mutable reference to the `CertifiedApi`.
    pub fn certified_api_mut(&mut self) -> &mut CertifiedApi {
        &mut self.certified_api
    }

    /// Return a reference to the `CompactionEngine`.
    pub fn compaction_engine(&self) -> &CompactionEngine {
        &self.compaction_engine
    }

    /// Return a mutable reference to the `CompactionEngine`.
    pub fn compaction_engine_mut(&mut self) -> &mut CompactionEngine {
        &mut self.compaction_engine
    }

    /// Return whether this node has an active frontier reporter (is an authority).
    pub fn is_authority(&self) -> bool {
        self.frontier_reporter.is_some()
    }

    /// Return a reference to the frontier reporter, if this node is an authority.
    pub fn frontier_reporter(&self) -> Option<&FrontierReporter> {
        self.frontier_reporter.as_ref()
    }

    /// Return a reference to the runtime metrics.
    pub fn metrics(&self) -> &Arc<RuntimeMetrics> {
        &self.metrics
    }

    /// Run the node event loop until shutdown is signalled.
    ///
    /// This drives four periodic tasks using `tokio::time::interval`:
    /// 1. **Certification processing** -- calls `process_certifications()` on the
    ///    `CertifiedApi` to promote pending writes whose frontiers have advanced.
    /// 2. **Cleanup** -- calls `cleanup()` to expire old pending writes and
    ///    remove completed entries.
    /// 3. **Compaction check** -- evaluates whether checkpoints should be created
    ///    for tracked key ranges.
    /// 4. **Frontier reporting** -- if this node is an authority, generates
    ///    frontier updates from the current HLC time and applies them locally.
    ///    This drives the automatic frontier pipeline so callers never need
    ///    to call `update_frontier` manually.
    ///
    /// Returns [`RunLoopStats`] with tick counters after shutdown completes.
    pub async fn run(&mut self) -> RunLoopStats {
        // Use interval_at so the first tick fires after the configured delay,
        // rather than immediately on startup. This avoids all background tasks
        // firing simultaneously at t=0.
        let start = tokio::time::Instant::now();
        let mut cert_interval = tokio::time::interval_at(
            start + self.config.certification_interval,
            self.config.certification_interval,
        );
        let mut cleanup_interval = tokio::time::interval_at(
            start + self.config.cleanup_interval,
            self.config.cleanup_interval,
        );
        let mut compaction_interval = tokio::time::interval_at(
            start + self.config.compaction_check_interval,
            self.config.compaction_check_interval,
        );
        let mut frontier_interval = tokio::time::interval_at(
            start + self.config.frontier_report_interval,
            self.config.frontier_report_interval,
        );

        // Sync interval: only create if sync is configured.
        let sync_duration = self
            .config
            .sync_interval
            .unwrap_or(Duration::from_secs(3600));
        let sync_enabled = self.config.sync_interval.is_some()
            && self.sync_client.is_some()
            && self.eventual_api.is_some();
        let mut sync_interval = tokio::time::interval_at(start + sync_duration, sync_duration);

        let mut stats = RunLoopStats::default();
        let mut shutdown_rx = self.shutdown_rx.clone();

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
                _ = cert_interval.tick() => {
                    self.process_certifications();
                    stats.certification_ticks += 1;
                }
                _ = cleanup_interval.tick() => {
                    self.run_cleanup();
                    stats.cleanup_ticks += 1;
                }
                _ = compaction_interval.tick() => {
                    self.check_compaction();
                    stats.compaction_check_ticks += 1;
                }
                _ = frontier_interval.tick(), if self.frontier_reporter.is_some() => {
                    self.report_frontiers();
                    stats.frontier_report_ticks += 1;
                }
                _ = sync_interval.tick(), if sync_enabled => {
                    self.run_sync().await;
                    stats.sync_ticks += 1;
                }
            }
        }

        stats
    }

    /// Run the node event loop until shutdown is signalled or ctrl-c is received.
    ///
    /// This is a convenience wrapper around [`run`](Self::run) that also listens
    /// for `SIGINT` (ctrl-c) to trigger graceful shutdown.
    pub async fn run_with_signal(&mut self) -> RunLoopStats {
        let shutdown_tx = self.shutdown_tx.clone();

        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                let _ = shutdown_tx.send(true);
            }
        });

        self.run().await
    }

    fn process_certifications(&mut self) {
        let now = self.clock.now();
        let now_ms = now.physical;

        // Snapshot pending write timestamps before processing.
        let pre_statuses: Vec<(CertificationStatus, u64)> = self
            .certified_api
            .pending_writes()
            .iter()
            .map(|pw| (pw.status, pw.timestamp.physical))
            .collect();

        self.certified_api
            .process_certifications_with_timeout(now_ms);

        // Compute metrics after processing.
        let writes = self.certified_api.pending_writes();
        let mut pending = 0u64;
        let mut newly_certified = 0u64;
        let mut latency_sum = 0u64;

        for (i, pw) in writes.iter().enumerate() {
            if pw.status == CertificationStatus::Pending {
                pending += 1;
            }
            // Detect newly certified writes by comparing pre/post status.
            if pw.status == CertificationStatus::Certified {
                let was_pending = pre_statuses
                    .get(i)
                    .is_some_and(|(s, _)| *s == CertificationStatus::Pending);
                if was_pending {
                    newly_certified += 1;
                    latency_sum += now_ms.saturating_sub(pw.timestamp.physical) * 1000;
                }
            }
        }

        self.metrics.pending_count.store(pending, Ordering::Relaxed);

        if newly_certified > 0 {
            self.metrics
                .certified_total
                .fetch_add(newly_certified, Ordering::Relaxed);
            self.metrics
                .certification_latency_sum_us
                .fetch_add(latency_sum, Ordering::Relaxed);
            self.metrics
                .certification_latency_count
                .fetch_add(newly_certified, Ordering::Relaxed);
        }
    }

    fn run_cleanup(&mut self) {
        let now_ms = self.clock.now().physical;
        self.certified_api.cleanup(now_ms);
    }

    /// Generate and apply frontier reports for this authority node.
    fn report_frontiers(&mut self) {
        if let Some(reporter) = &self.frontier_reporter {
            let frontiers = reporter.report_frontiers(&mut self.clock);
            for f in frontiers {
                self.certified_api.update_frontier(f);
            }
        }

        // Compute frontier skew: for each scope, find max and min frontier
        // HLC among authorities, and report the maximum skew across all scopes.
        self.update_frontier_skew();
    }

    /// Compute and store the maximum frontier skew across all authority scopes.
    fn update_frontier_skew(&self) {
        use std::collections::HashMap;

        let all_frontiers = self.certified_api.all_frontiers();
        if all_frontiers.is_empty() {
            return;
        }

        // Group frontiers by key range prefix.
        let mut by_scope: HashMap<&str, (u64, u64)> = HashMap::new();
        for f in &all_frontiers {
            let entry = by_scope
                .entry(f.key_range.prefix.as_str())
                .or_insert((u64::MAX, 0));
            entry.0 = entry.0.min(f.frontier_hlc.physical);
            entry.1 = entry.1.max(f.frontier_hlc.physical);
        }

        let max_skew_ms = by_scope
            .values()
            .map(|(min_p, max_p)| max_p.saturating_sub(*min_p))
            .max()
            .unwrap_or(0);

        self.metrics
            .frontier_skew_ms
            .store(max_skew_ms, Ordering::Relaxed);
    }

    /// Run one cycle of anti-entropy push sync.
    async fn run_sync(&self) {
        let Some(sync_client) = &self.sync_client else {
            return;
        };
        let Some(eventual_api) = &self.eventual_api else {
            return;
        };

        self.metrics
            .sync_attempt_total
            .fetch_add(1, Ordering::Relaxed);

        let entries = {
            let api = eventual_api.lock().await;
            api.store()
                .all_entries()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        };

        let synced = sync_client.push_all_keys(entries, &self.node_id.0).await;

        if synced == 0 && sync_client.peer_registry().peer_count() > 0 {
            self.metrics
                .sync_failure_total
                .fetch_add(1, Ordering::Relaxed);
        }

        tracing::debug!(
            node = %self.node_id.0,
            peers_synced = synced,
            "anti-entropy sync cycle completed"
        );
    }

    fn check_compaction(&mut self) {
        let now = self.clock.now();

        let ns = self.certified_api.namespace().read().unwrap();

        // Iterate over all authority definitions to check each key range.
        let defs: Vec<_> = ns
            .all_authority_definitions()
            .into_iter()
            .map(|def| (def.key_range.clone(), def.authority_nodes.len()))
            .collect();

        for (key_range, _total_authorities) in &defs {
            if self.compaction_engine.should_checkpoint(key_range, &now) {
                // Create a checkpoint with a placeholder digest.
                // In a full implementation this would compute an actual digest
                // over the store data for this key range.
                let policy_version = ns
                    .get_placement_policy(&key_range.prefix)
                    .map(|p| p.version)
                    .unwrap_or(crate::types::PolicyVersion(1));

                let digest = format!("digest-{}-{}", key_range.prefix, now.physical);
                self.compaction_engine.create_checkpoint(
                    key_range.clone(),
                    now.clone(),
                    digest,
                    policy_version,
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::certified::{OnTimeout, RetentionPolicy};
    use crate::authority::ack_frontier::AckFrontier;
    use crate::compaction::CompactionConfig;
    use crate::control_plane::system_namespace::{AuthorityDefinition, SystemNamespace};
    use crate::crdt::pn_counter::PnCounter;
    use crate::hlc::HlcTimestamp;
    use crate::ops::metrics::RuntimeMetrics;
    use crate::store::kv::CrdtValue;
    use crate::types::{CertificationStatus, KeyRange, NodeId, PolicyVersion};
    use std::sync::{Arc, RwLock};

    fn default_metrics() -> Arc<RuntimeMetrics> {
        Arc::new(RuntimeMetrics::default())
    }

    fn node_id(s: &str) -> NodeId {
        NodeId(s.into())
    }

    fn kr(prefix: &str) -> KeyRange {
        KeyRange {
            prefix: prefix.into(),
        }
    }

    fn wrap_ns(ns: SystemNamespace) -> Arc<RwLock<SystemNamespace>> {
        Arc::new(RwLock::new(ns))
    }

    fn default_namespace() -> Arc<RwLock<SystemNamespace>> {
        let mut ns = SystemNamespace::new();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr(""),
            authority_nodes: vec![node_id("auth-1"), node_id("auth-2"), node_id("auth-3")],
        });
        wrap_ns(ns)
    }

    fn counter_value(n: i64) -> CrdtValue {
        let mut counter = PnCounter::new();
        for _ in 0..n {
            counter.increment(&node_id("writer"));
        }
        CrdtValue::Counter(counter)
    }

    fn make_frontier(authority: &str, physical: u64, prefix: &str) -> AckFrontier {
        AckFrontier {
            authority_id: NodeId(authority.into()),
            frontier_hlc: HlcTimestamp {
                physical,
                logical: 0,
                node_id: authority.into(),
            },
            key_range: KeyRange {
                prefix: prefix.into(),
            },
            policy_version: PolicyVersion(1),
            digest_hash: format!("{authority}-{physical}"),
        }
    }

    #[tokio::test]
    async fn node_runner_starts_and_stops() {
        let api = CertifiedApi::new(node_id("node-1"), default_namespace());
        let engine = CompactionEngine::with_defaults();
        let config = NodeRunnerConfig {
            certification_interval: Duration::from_millis(10),
            cleanup_interval: Duration::from_millis(50),
            compaction_check_interval: Duration::from_millis(100),
            frontier_report_interval: Duration::from_millis(100),
            sync_interval: None,
        };

        let mut runner = NodeRunner::new(node_id("node-1"), api, engine, config, default_metrics());
        let handle = runner.shutdown_handle();

        // Shut down after a brief delay.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(80)).await;
            let _ = handle.send(true);
        });

        let stats = runner.run().await;

        // At least one certification tick should have fired in ~80ms with 10ms interval.
        assert!(
            stats.certification_ticks >= 1,
            "expected at least 1 cert tick, got {}",
            stats.certification_ticks
        );
        assert!(
            stats.cleanup_ticks >= 1,
            "expected at least 1 cleanup tick, got {}",
            stats.cleanup_ticks
        );
    }

    #[tokio::test]
    async fn node_runner_processes_certifications() {
        let mut api = CertifiedApi::new(node_id("node-1"), default_namespace());

        // Write a pending entry.
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        let write_ts = api.pending_writes()[0].timestamp.physical;

        // Advance majority of frontiers past the write.
        api.update_frontier(make_frontier("auth-1", write_ts + 100, ""));
        api.update_frontier(make_frontier("auth-2", write_ts + 200, ""));

        let engine = CompactionEngine::with_defaults();
        let config = NodeRunnerConfig {
            certification_interval: Duration::from_millis(10),
            cleanup_interval: Duration::from_secs(60),
            compaction_check_interval: Duration::from_secs(60),
            frontier_report_interval: Duration::from_secs(60),
            sync_interval: None,
        };

        let mut runner = NodeRunner::new(node_id("node-1"), api, engine, config, default_metrics());
        let handle = runner.shutdown_handle();

        // Run long enough for at least one certification tick.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = handle.send(true);
        });

        runner.run().await;

        // The pending write should now be certified.
        assert_eq!(
            runner.certified_api().pending_writes()[0].status,
            CertificationStatus::Certified
        );
    }

    #[tokio::test]
    async fn node_runner_runs_cleanup() {
        let retention = RetentionPolicy {
            max_age_ms: 10,
            max_entries: 10_000,
        };
        let mut api =
            CertifiedApi::with_retention(node_id("node-1"), default_namespace(), retention);

        // Write a pending entry.
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();

        assert_eq!(api.pending_writes().len(), 1);

        let engine = CompactionEngine::with_defaults();
        let config = NodeRunnerConfig {
            certification_interval: Duration::from_secs(60),
            cleanup_interval: Duration::from_millis(10),
            compaction_check_interval: Duration::from_secs(60),
            frontier_report_interval: Duration::from_secs(60),
            sync_interval: None,
        };

        let mut runner = NodeRunner::new(node_id("node-1"), api, engine, config, default_metrics());
        let handle = runner.shutdown_handle();

        // Run long enough for cleanup to expire the 10ms-TTL write.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(80)).await;
            let _ = handle.send(true);
        });

        runner.run().await;

        // The expired write should have been cleaned up.
        assert_eq!(
            runner.certified_api().pending_writes().len(),
            0,
            "expired writes should be cleaned up"
        );
    }

    #[tokio::test]
    async fn node_runner_checks_compaction() {
        let mut ns = SystemNamespace::new();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr("data/"),
            authority_nodes: vec![node_id("auth-1"), node_id("auth-2"), node_id("auth-3")],
        });

        let api = CertifiedApi::new(node_id("node-1"), wrap_ns(ns));

        let compaction_config = CompactionConfig {
            time_threshold_ms: 10,
            ops_threshold: 1,
        };
        let mut engine = CompactionEngine::new(compaction_config);
        // Record an op to trigger checkpoint on first check.
        engine.record_op(&kr("data/"));

        let config = NodeRunnerConfig {
            certification_interval: Duration::from_secs(60),
            cleanup_interval: Duration::from_secs(60),
            compaction_check_interval: Duration::from_millis(10),
            frontier_report_interval: Duration::from_secs(60),
            sync_interval: None,
        };

        let mut runner = NodeRunner::new(node_id("node-1"), api, engine, config, default_metrics());
        let handle = runner.shutdown_handle();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = handle.send(true);
        });

        runner.run().await;

        // A checkpoint should have been created for data/.
        assert!(
            runner.compaction_engine().get_checkpoint("data/").is_some(),
            "compaction check should create checkpoint when threshold is reached"
        );
    }

    #[tokio::test]
    async fn shutdown_handle_is_cloneable() {
        let api = CertifiedApi::new(node_id("node-1"), default_namespace());
        let engine = CompactionEngine::with_defaults();
        let runner = NodeRunner::new(
            node_id("node-1"),
            api,
            engine,
            NodeRunnerConfig::default(),
            default_metrics(),
        );

        let handle1 = runner.shutdown_handle();
        let handle2 = runner.shutdown_handle();

        // Both handles should work.
        let _ = handle1.send(true);
        let _ = handle2.send(true);
    }

    #[tokio::test]
    async fn node_runner_default_config() {
        let config = NodeRunnerConfig::default();
        assert_eq!(config.certification_interval, Duration::from_secs(1));
        assert_eq!(config.cleanup_interval, Duration::from_secs(5));
        assert_eq!(config.compaction_check_interval, Duration::from_secs(10));
        assert_eq!(config.frontier_report_interval, Duration::from_secs(1));
        assert_eq!(config.sync_interval, Some(Duration::from_secs(2)));
    }

    #[tokio::test]
    async fn node_runner_accessors() {
        let api = CertifiedApi::new(node_id("node-1"), default_namespace());
        let engine = CompactionEngine::with_defaults();
        let mut runner = NodeRunner::new(
            node_id("node-1"),
            api,
            engine,
            NodeRunnerConfig::default(),
            default_metrics(),
        );

        assert_eq!(runner.node_id(), &node_id("node-1"));

        // Mutable access.
        runner
            .certified_api_mut()
            .certified_write("test".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        assert_eq!(runner.certified_api().pending_writes().len(), 1);

        runner.compaction_engine_mut().record_op(&kr("test/"));
    }

    #[tokio::test]
    async fn immediate_shutdown() {
        let api = CertifiedApi::new(node_id("node-1"), default_namespace());
        let engine = CompactionEngine::with_defaults();
        let config = NodeRunnerConfig {
            certification_interval: Duration::from_secs(60),
            cleanup_interval: Duration::from_secs(60),
            compaction_check_interval: Duration::from_secs(60),
            frontier_report_interval: Duration::from_secs(60),
            sync_interval: None,
        };

        let mut runner = NodeRunner::new(node_id("node-1"), api, engine, config, default_metrics());

        // Signal shutdown before run starts.
        let _ = runner.shutdown_handle().send(true);

        let stats = runner.run().await;

        // With long intervals and immediate shutdown, minimal ticks expected.
        // The initial tick fires immediately for each interval, so we may get
        // 0 or 1 depending on select! ordering. The key point is it exits.
        assert!(
            stats.certification_ticks <= 1,
            "expected at most 1 cert tick on immediate shutdown"
        );
    }

    // ---------------------------------------------------------------
    // Frontier auto-report tests
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn authority_node_has_frontier_reporter() {
        // node-1 is NOT in the authority set → no reporter
        let api = CertifiedApi::new(node_id("node-1"), default_namespace());
        let engine = CompactionEngine::with_defaults();
        let runner = NodeRunner::new(
            node_id("node-1"),
            api,
            engine,
            NodeRunnerConfig::default(),
            default_metrics(),
        );
        assert!(!runner.is_authority());
        assert!(runner.frontier_reporter().is_none());

        // auth-1 IS in the authority set → has reporter
        let api = CertifiedApi::new(node_id("auth-1"), default_namespace());
        let engine = CompactionEngine::with_defaults();
        let runner = NodeRunner::new(
            node_id("auth-1"),
            api,
            engine,
            NodeRunnerConfig::default(),
            default_metrics(),
        );
        assert!(runner.is_authority());
        assert!(runner.frontier_reporter().is_some());
    }

    #[tokio::test]
    async fn frontier_auto_report_advances_local_frontier() {
        // Create a namespace where auth-1 is an authority.
        let ns = default_namespace();
        let api = CertifiedApi::new(node_id("auth-1"), ns);
        let engine = CompactionEngine::with_defaults();

        let config = NodeRunnerConfig {
            certification_interval: Duration::from_secs(60),
            cleanup_interval: Duration::from_secs(60),
            compaction_check_interval: Duration::from_secs(60),
            frontier_report_interval: Duration::from_millis(10),
            sync_interval: None,
        };

        let mut runner = NodeRunner::new(node_id("auth-1"), api, engine, config, default_metrics());
        assert!(runner.is_authority());

        let handle = runner.shutdown_handle();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(80)).await;
            let _ = handle.send(true);
        });

        let stats = runner.run().await;

        // Frontier report ticks should have fired.
        assert!(
            stats.frontier_report_ticks >= 1,
            "expected at least 1 frontier report tick, got {}",
            stats.frontier_report_ticks
        );

        // The frontier should have been applied locally.
        let frontiers = runner.certified_api().all_frontiers();
        assert!(
            !frontiers.is_empty(),
            "authority node should have auto-reported frontiers"
        );

        // Verify the frontier is from auth-1.
        assert!(
            frontiers
                .iter()
                .any(|f| f.authority_id == node_id("auth-1")),
            "frontier should be from auth-1"
        );
    }

    #[tokio::test]
    async fn non_authority_does_not_report_frontiers() {
        let ns = default_namespace();
        let api = CertifiedApi::new(node_id("store-node"), ns);
        let engine = CompactionEngine::with_defaults();

        let config = NodeRunnerConfig {
            certification_interval: Duration::from_secs(60),
            cleanup_interval: Duration::from_secs(60),
            compaction_check_interval: Duration::from_secs(60),
            frontier_report_interval: Duration::from_millis(10),
            sync_interval: None,
        };

        let mut runner = NodeRunner::new(
            node_id("store-node"),
            api,
            engine,
            config,
            default_metrics(),
        );
        assert!(!runner.is_authority());

        let handle = runner.shutdown_handle();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(80)).await;
            let _ = handle.send(true);
        });

        let stats = runner.run().await;

        // Non-authority should not have any frontier report ticks.
        assert_eq!(
            stats.frontier_report_ticks, 0,
            "non-authority node should not report frontiers"
        );

        // No frontiers should have been applied.
        let frontiers = runner.certified_api().all_frontiers();
        assert!(
            frontiers.is_empty(),
            "non-authority node should have no frontiers"
        );
    }

    #[tokio::test]
    async fn auto_frontier_certifies_pending_write() {
        // This is the key integration test: a pending write on an authority
        // node should eventually be certified by the auto-frontier pipeline,
        // without any manual update_frontier calls.
        //
        // Setup: 1-authority system where auth-1 is the only authority.
        let mut ns = SystemNamespace::new();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr(""),
            authority_nodes: vec![node_id("auth-1")],
        });

        let mut api = CertifiedApi::new(node_id("auth-1"), wrap_ns(ns));
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        assert_eq!(api.pending_writes()[0].status, CertificationStatus::Pending);

        let engine = CompactionEngine::with_defaults();
        let config = NodeRunnerConfig {
            certification_interval: Duration::from_millis(10),
            cleanup_interval: Duration::from_secs(60),
            compaction_check_interval: Duration::from_secs(60),
            frontier_report_interval: Duration::from_millis(10),
            sync_interval: None,
        };

        let mut runner = NodeRunner::new(node_id("auth-1"), api, engine, config, default_metrics());
        let handle = runner.shutdown_handle();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = handle.send(true);
        });

        runner.run().await;

        // The pending write should have been auto-certified.
        assert_eq!(
            runner.certified_api().pending_writes()[0].status,
            CertificationStatus::Certified,
            "pending write should be auto-certified by frontier pipeline"
        );
    }

    #[tokio::test]
    async fn auto_frontier_regression_prevented() {
        // Verify that the auto-frontier pipeline never regresses.
        // We'll manually insert a high frontier, then let the auto-reporter
        // run. The frontier should not go backwards.
        let mut ns = SystemNamespace::new();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr(""),
            authority_nodes: vec![node_id("auth-1")],
        });

        let mut api = CertifiedApi::new(node_id("auth-1"), wrap_ns(ns));

        // Set a very high initial frontier manually.
        api.update_frontier(AckFrontier {
            authority_id: node_id("auth-1"),
            frontier_hlc: HlcTimestamp {
                physical: u64::MAX - 1000,
                logical: 0,
                node_id: "auth-1".into(),
            },
            key_range: kr(""),
            policy_version: PolicyVersion(1),
            digest_hash: "high-frontier".into(),
        });

        let engine = CompactionEngine::with_defaults();
        let config = NodeRunnerConfig {
            certification_interval: Duration::from_secs(60),
            cleanup_interval: Duration::from_secs(60),
            compaction_check_interval: Duration::from_secs(60),
            frontier_report_interval: Duration::from_millis(10),
            sync_interval: None,
        };

        let mut runner = NodeRunner::new(node_id("auth-1"), api, engine, config, default_metrics());
        let handle = runner.shutdown_handle();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(80)).await;
            let _ = handle.send(true);
        });

        runner.run().await;

        // The frontier should still be at the high value (not regressed).
        let frontiers = runner.certified_api().all_frontiers();
        assert!(!frontiers.is_empty());
        assert!(
            frontiers[0].frontier_hlc.physical >= u64::MAX - 1000,
            "frontier must not regress below the manually-set high value"
        );
    }
}
