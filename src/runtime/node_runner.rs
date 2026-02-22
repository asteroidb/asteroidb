use std::time::Duration;

use tokio::sync::watch;

use crate::api::certified::CertifiedApi;
use crate::compaction::CompactionEngine;
use crate::hlc::Hlc;
use crate::types::NodeId;

/// Configuration for the background processing intervals of [`NodeRunner`].
#[derive(Debug, Clone)]
pub struct NodeRunnerConfig {
    /// How often to re-evaluate pending writes against authority frontiers.
    pub certification_interval: Duration,
    /// How often to run cleanup (expire + remove completed pending writes).
    pub cleanup_interval: Duration,
    /// How often to check compaction eligibility and create checkpoints.
    pub compaction_check_interval: Duration,
}

impl Default for NodeRunnerConfig {
    fn default() -> Self {
        Self {
            certification_interval: Duration::from_secs(1),
            cleanup_interval: Duration::from_secs(5),
            compaction_check_interval: Duration::from_secs(10),
        }
    }
}

/// Node execution loop that drives background processing.
///
/// Owns the `CertifiedApi` and `CompactionEngine` and periodically runs:
/// - `process_certifications`: re-evaluates pending writes against frontiers
/// - `cleanup`: expires old pending writes and removes completed entries
/// - compaction checkpoint checks
///
/// Supports graceful shutdown via a watch channel.
pub struct NodeRunner {
    node_id: NodeId,
    certified_api: CertifiedApi,
    compaction_engine: CompactionEngine,
    clock: Hlc,
    config: NodeRunnerConfig,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
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
}

impl NodeRunner {
    /// Create a new `NodeRunner`.
    pub fn new(
        node_id: NodeId,
        certified_api: CertifiedApi,
        compaction_engine: CompactionEngine,
        config: NodeRunnerConfig,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Self {
            clock: Hlc::new(node_id.0.clone()),
            node_id,
            certified_api,
            compaction_engine,
            config,
            shutdown_tx,
            shutdown_rx,
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

    /// Run the node event loop until shutdown is signalled.
    ///
    /// This drives three periodic tasks using `tokio::time::interval`:
    /// 1. **Certification processing** — calls `process_certifications()` on the
    ///    `CertifiedApi` to promote pending writes whose frontiers have advanced.
    /// 2. **Cleanup** — calls `cleanup()` to expire old pending writes and
    ///    remove completed entries.
    /// 3. **Compaction check** — evaluates whether checkpoints should be created
    ///    for tracked key ranges.
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
        self.certified_api.process_certifications();
    }

    fn run_cleanup(&mut self) {
        let now_ms = self.clock.now().physical;
        self.certified_api.cleanup(now_ms);
    }

    fn check_compaction(&mut self) {
        let now = self.clock.now();

        // Iterate over all authority definitions to check each key range.
        let defs: Vec<_> = self
            .certified_api
            .namespace()
            .all_authority_definitions()
            .into_iter()
            .map(|def| (def.key_range.clone(), def.authority_nodes.len()))
            .collect();

        for (key_range, _total_authorities) in &defs {
            if self.compaction_engine.should_checkpoint(key_range, &now) {
                // Create a checkpoint with a placeholder digest.
                // In a full implementation this would compute an actual digest
                // over the store data for this key range.
                let policy_version = self
                    .certified_api
                    .namespace()
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
    use crate::store::kv::CrdtValue;
    use crate::types::{CertificationStatus, KeyRange, NodeId, PolicyVersion};

    fn node_id(s: &str) -> NodeId {
        NodeId(s.into())
    }

    fn kr(prefix: &str) -> KeyRange {
        KeyRange {
            prefix: prefix.into(),
        }
    }

    fn default_namespace() -> SystemNamespace {
        let mut ns = SystemNamespace::new();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr(""),
            authority_nodes: vec![node_id("auth-1"), node_id("auth-2"), node_id("auth-3")],
        });
        ns
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
        };

        let mut runner = NodeRunner::new(node_id("node-1"), api, engine, config);
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
        };

        let mut runner = NodeRunner::new(node_id("node-1"), api, engine, config);
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
        };

        let mut runner = NodeRunner::new(node_id("node-1"), api, engine, config);
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

        let api = CertifiedApi::new(node_id("node-1"), ns);

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
        };

        let mut runner = NodeRunner::new(node_id("node-1"), api, engine, config);
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
        let runner = NodeRunner::new(node_id("node-1"), api, engine, NodeRunnerConfig::default());

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
    }

    #[tokio::test]
    async fn node_runner_accessors() {
        let api = CertifiedApi::new(node_id("node-1"), default_namespace());
        let engine = CompactionEngine::with_defaults();
        let mut runner =
            NodeRunner::new(node_id("node-1"), api, engine, NodeRunnerConfig::default());

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
        };

        let mut runner = NodeRunner::new(node_id("node-1"), api, engine, config);

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
}
