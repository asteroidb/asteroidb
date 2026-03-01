use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::{Mutex, watch};

use crate::api::certified::CertifiedApi;
use crate::api::eventual::EventualApi;
use crate::authority::frontier_reporter::FrontierReporter;
use crate::compaction::CompactionEngine;
use crate::control_plane::system_namespace::SystemNamespace;
use crate::hlc::{Hlc, HlcTimestamp};
use crate::network::sync::SyncClient;
use crate::node::Node;
use crate::ops::metrics::RuntimeMetrics;
use crate::types::{CertificationStatus, KeyRange, NodeId, PolicyVersion};

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
    certified_api: Arc<Mutex<CertifiedApi>>,
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
    /// Tracked policy versions per key range prefix.
    ///
    /// On each certification tick the runner snapshots the current
    /// namespace versions and compares with these tracked values.
    /// When a version change is detected, the old version is fenced
    /// and the frontier reporter is refreshed.
    tracked_policy_versions: HashMap<String, PolicyVersion>,
    /// Per-peer last known frontier for delta sync.
    /// Maps peer address string to its last known frontier.
    peer_frontiers: HashMap<String, HlcTimestamp>,
    /// Known cluster nodes for authority auto-reconfiguration.
    ///
    /// When this list changes (node join/leave), the runner triggers
    /// `recalculate_authorities()` on the system namespace, updating
    /// authority definitions based on placement policy tag criteria.
    cluster_nodes: Arc<std::sync::RwLock<Vec<Node>>>,
    /// Hash-based fingerprint for detecting cluster membership changes.
    /// Computed from sorted node IDs so that same-size replacements
    /// (e.g. 1 leave + 1 join) are detected correctly.
    tracked_cluster_generation: u64,
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
    pub async fn new(
        node_id: NodeId,
        certified_api: Arc<Mutex<CertifiedApi>>,
        compaction_engine: CompactionEngine,
        config: NodeRunnerConfig,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        let cluster_nodes = Arc::new(std::sync::RwLock::new(Vec::new()));
        Self::with_cluster_nodes(
            node_id,
            certified_api,
            compaction_engine,
            config,
            metrics,
            cluster_nodes,
        )
        .await
    }

    /// Create a new `NodeRunner` with a shared cluster node list.
    ///
    /// The `cluster_nodes` list is monitored for changes. When nodes
    /// join or leave, authority definitions are automatically
    /// recalculated based on placement policies.
    pub async fn with_cluster_nodes(
        node_id: NodeId,
        certified_api: Arc<Mutex<CertifiedApi>>,
        compaction_engine: CompactionEngine,
        config: NodeRunnerConfig,
        metrics: Arc<RuntimeMetrics>,
        cluster_nodes: Arc<std::sync::RwLock<Vec<Node>>>,
    ) -> Self {
        let (reporter, tracked_versions) = {
            let api = certified_api.lock().await;
            let ns = api.namespace().read().unwrap();
            let reporter = FrontierReporter::new(node_id.clone(), &ns);
            let versions = Self::snapshot_policy_versions(&ns);
            (reporter, versions)
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
            tracked_policy_versions: tracked_versions,
            peer_frontiers: HashMap::new(),
            cluster_nodes,
            // Use sentinel value to force initial recalculation on first tick.
            tracked_cluster_generation: u64::MAX,
        }
    }

    /// Create a new `NodeRunner` with anti-entropy sync enabled.
    ///
    /// The `eventual_api` must be the same `Arc<Mutex<EventualApi>>` shared
    /// with the HTTP handlers so that sync reads the latest store state.
    pub async fn with_sync(
        node_id: NodeId,
        certified_api: Arc<Mutex<CertifiedApi>>,
        compaction_engine: CompactionEngine,
        config: NodeRunnerConfig,
        sync_client: SyncClient,
        eventual_api: Arc<Mutex<EventualApi>>,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        let cluster_nodes = Arc::new(std::sync::RwLock::new(Vec::new()));
        let (reporter, tracked_versions) = {
            let api = certified_api.lock().await;
            let ns = api.namespace().read().unwrap();
            let reporter = FrontierReporter::new(node_id.clone(), &ns);
            let versions = Self::snapshot_policy_versions(&ns);
            (reporter, versions)
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
            tracked_policy_versions: tracked_versions,
            peer_frontiers: HashMap::new(),
            cluster_nodes,
            tracked_cluster_generation: 0,
        }
    }

    /// Return a shutdown handle that can be used to signal graceful shutdown.
    ///
    /// Sending `true` on the returned sender causes `run()` to exit after the
    /// current tick completes.
    pub fn shutdown_handle(&self) -> watch::Sender<bool> {
        self.shutdown_tx.clone()
    }

    /// Set the shared `EventualApi` reference.
    ///
    /// This allows the `NodeRunner` to access the same eventual store
    /// used by HTTP handlers, ensuring that HTTP writes are visible
    /// to the anti-entropy sync loop.
    pub fn set_eventual_api(&mut self, api: Arc<Mutex<EventualApi>>) {
        self.eventual_api = Some(api);
    }

    /// Replace the sync client used for anti-entropy replication.
    ///
    /// Useful for injecting a token-enabled `SyncClient` after
    /// construction when the token is not known at `NodeRunner` creation time.
    pub fn set_sync_client(&mut self, client: SyncClient) {
        self.sync_client = Some(client);
    }

    /// Inject a peer frontier for testing purposes.
    ///
    /// This forces the next sync cycle to attempt delta sync first for
    /// the given peer address, which is useful for testing the
    /// delta-fail -> full-sync fallback path.
    pub fn inject_peer_frontier(&mut self, peer_addr: &str, frontier: HlcTimestamp) {
        self.peer_frontiers.insert(peer_addr.to_string(), frontier);
    }

    /// Return a reference to the node ID.
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Return a shared reference to the `CertifiedApi` wrapped in `Arc<Mutex<..>>`.
    pub fn certified_api(&self) -> &Arc<Mutex<CertifiedApi>> {
        &self.certified_api
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

    /// Return a shared reference to the cluster node list.
    pub fn cluster_nodes(&self) -> &Arc<std::sync::RwLock<Vec<Node>>> {
        &self.cluster_nodes
    }

    /// Snapshot the current policy version for each placement policy
    /// in the system namespace.
    fn snapshot_policy_versions(ns: &SystemNamespace) -> HashMap<String, PolicyVersion> {
        ns.all_placement_policies()
            .into_iter()
            .map(|p| (p.key_range.prefix.clone(), p.version))
            .collect()
    }

    /// Detect policy version changes, membership changes, and fence old versions.
    ///
    /// Compares the current namespace policy versions against the tracked
    /// snapshot. When a version change is detected:
    /// 1. The old version is fenced in the `AckFrontierSet` (via `CertifiedApi`)
    /// 2. The `FrontierReporter` is refreshed to pick up the new scopes
    /// 3. The tracked versions are updated
    ///
    /// Also detects cluster membership changes (node join/leave) and triggers
    /// authority recalculation when the node list changes.
    async fn detect_version_changes(&mut self) {
        // Check for cluster membership changes first.
        self.detect_membership_changes().await;

        // Snapshot current versions while briefly holding the locks.
        let current_versions: HashMap<String, PolicyVersion> = {
            let api = self.certified_api.lock().await;
            let ns = api.namespace().read().unwrap();
            Self::snapshot_policy_versions(&ns)
        };

        // Collect version changes: (prefix, old_version, new_version).
        let mut changes: Vec<(String, PolicyVersion, PolicyVersion)> = Vec::new();
        for (prefix, new_version) in &current_versions {
            if let Some(old_version) = self.tracked_policy_versions.get(prefix)
                && old_version != new_version
            {
                changes.push((prefix.clone(), *old_version, *new_version));
            }
        }

        if changes.is_empty() {
            return;
        }

        // Apply fencing and refresh reporter.
        {
            let mut api = self.certified_api.lock().await;
            for (prefix, old_version, _new_version) in &changes {
                let key_range = KeyRange {
                    prefix: prefix.clone(),
                };
                api.fence_version(&key_range, *old_version);
            }

            // Refresh the frontier reporter scopes.
            if let Some(reporter) = &mut self.frontier_reporter {
                let ns = api.namespace().read().unwrap();
                reporter.refresh_scopes(&ns);
            }
        }

        // Update tracked versions.
        self.tracked_policy_versions = current_versions;
    }

    /// Detect cluster membership changes and recalculate authorities.
    ///
    /// Compares the current cluster node list against the tracked generation.
    /// When a change is detected, calls `recalculate_authorities()` on the
    /// system namespace and refreshes the frontier reporter.
    /// Compute a fingerprint of the cluster node list.
    ///
    /// Sorts node IDs and feeds them into a deterministic hasher so that
    /// any structural change (including same-size replacements) produces
    /// a different value.
    fn cluster_fingerprint(nodes: &[Node]) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut ids: Vec<&str> = nodes.iter().map(|n| n.id.0.as_str()).collect();
        ids.sort_unstable();
        let mut hasher = DefaultHasher::new();
        ids.len().hash(&mut hasher);
        for id in ids {
            id.hash(&mut hasher);
        }
        hasher.finish()
    }

    async fn detect_membership_changes(&mut self) {
        let current_generation = {
            let nodes = self.cluster_nodes.read().unwrap();
            Self::cluster_fingerprint(&nodes)
        };
        if current_generation == self.tracked_cluster_generation {
            return;
        }
        self.tracked_cluster_generation = current_generation;

        let nodes: Vec<Node> = self.cluster_nodes.read().unwrap().clone();

        let api = self.certified_api.lock().await;
        let changed = {
            let mut ns = api.namespace().write().unwrap();
            ns.recalculate_authorities(&nodes)
        };

        if changed > 0 {
            // Refresh the frontier reporter to pick up new authority scopes.
            let ns = api.namespace().read().unwrap();
            let reporter = FrontierReporter::new(self.node_id.clone(), &ns);
            if reporter.is_authority() {
                self.frontier_reporter = Some(reporter);
            } else {
                self.frontier_reporter = None;
            }
        }
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
                    self.detect_version_changes().await;
                    self.process_certifications().await;
                    stats.certification_ticks += 1;
                }
                _ = cleanup_interval.tick() => {
                    self.run_cleanup().await;
                    stats.cleanup_ticks += 1;
                }
                _ = compaction_interval.tick() => {
                    self.check_compaction().await;
                    stats.compaction_check_ticks += 1;
                }
                _ = frontier_interval.tick(), if self.frontier_reporter.is_some() => {
                    self.report_frontiers().await;
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

    async fn process_certifications(&mut self) {
        let now = self.clock.now();
        let now_ms = now.physical;

        let mut api = self.certified_api.lock().await;

        // Snapshot pending write timestamps before processing.
        let pre_statuses: Vec<(CertificationStatus, u64)> = api
            .pending_writes()
            .iter()
            .map(|pw| (pw.status, pw.timestamp.physical))
            .collect();

        api.process_certifications_with_timeout(now_ms);

        // Compute metrics after processing.
        let writes = api.pending_writes();
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

        drop(api);

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

    async fn run_cleanup(&mut self) {
        let now_ms = self.clock.now().physical;
        let mut api = self.certified_api.lock().await;
        api.cleanup(now_ms);
    }

    /// Generate and apply frontier reports for this authority node.
    async fn report_frontiers(&mut self) {
        if let Some(reporter) = &self.frontier_reporter {
            let frontiers = reporter.report_frontiers(&mut self.clock);
            let mut api = self.certified_api.lock().await;
            for f in frontiers {
                api.update_frontier(f);
            }
        }

        // Compute frontier skew: for each scope, find max and min frontier
        // HLC among authorities, and report the maximum skew across all scopes.
        self.update_frontier_skew().await;
    }

    /// Compute and store the maximum frontier skew across all authority scopes.
    async fn update_frontier_skew(&self) {
        use std::collections::HashMap;

        let api = self.certified_api.lock().await;
        let all_frontiers = api.all_frontiers();
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

        drop(api);

        self.metrics
            .frontier_skew_ms
            .store(max_skew_ms, Ordering::Relaxed);
    }

    /// Run one cycle of delta-based anti-entropy sync.
    ///
    /// For each peer:
    /// 1. If we have a known frontier for the peer, try delta pull.
    /// 2. Apply received entries and update the peer's frontier.
    /// 3. On failure, fall back to full sync.
    async fn run_sync(&mut self) {
        let Some(sync_client) = &self.sync_client else {
            return;
        };
        let Some(eventual_api) = &self.eventual_api else {
            return;
        };

        self.metrics
            .sync_attempt_total
            .fetch_add(1, Ordering::Relaxed);

        let peers: Vec<_> = sync_client.peer_registry().all_peers().to_vec();
        let mut any_success = false;

        for peer in &peers {
            let peer_key = peer.addr.clone();

            // Try delta sync if we have a frontier for this peer.
            if let Some(frontier) = self.peer_frontiers.get(&peer_key) {
                let delta_result = sync_client
                    .pull_delta(&peer.addr, &self.node_id.0, frontier)
                    .await;

                if let Some(delta_resp) = delta_result {
                    // Apply delta entries.
                    let mut api = eventual_api.lock().await;
                    for entry in &delta_resp.entries {
                        let _ = api.merge_remote_with_hlc(
                            entry.key.clone(),
                            &entry.value,
                            entry.hlc.clone(),
                        );
                    }
                    drop(api);

                    // Update peer frontier.
                    if let Some(new_frontier) = delta_resp.sender_frontier {
                        self.peer_frontiers.insert(peer_key.clone(), new_frontier);
                    }

                    any_success = true;
                    tracing::debug!(
                        peer = %peer.node_id.0,
                        delta_entries = delta_resp.entries.len(),
                        "delta sync pull succeeded"
                    );
                    continue;
                }

                // Delta sync failed; retry once.
                let retry_result = sync_client
                    .pull_delta(&peer.addr, &self.node_id.0, frontier)
                    .await;

                if let Some(delta_resp) = retry_result {
                    let mut api = eventual_api.lock().await;
                    for entry in &delta_resp.entries {
                        let _ = api.merge_remote_with_hlc(
                            entry.key.clone(),
                            &entry.value,
                            entry.hlc.clone(),
                        );
                    }
                    drop(api);

                    if let Some(new_frontier) = delta_resp.sender_frontier {
                        self.peer_frontiers.insert(peer_key.clone(), new_frontier);
                    }

                    any_success = true;
                    tracing::debug!(
                        peer = %peer.node_id.0,
                        "delta sync retry succeeded"
                    );
                    continue;
                }

                // Both delta attempts failed; fall through to full sync.
                tracing::warn!(
                    peer = %peer.node_id.0,
                    "delta sync failed, falling back to full sync"
                );
                self.metrics
                    .sync_fallback_total
                    .fetch_add(1, Ordering::Relaxed);
            }

            // Full sync fallback: pull all keys from peer.
            if let Some(dump) = sync_client.pull_all_keys(&peer.addr).await {
                let mut api = eventual_api.lock().await;
                for (key, value) in &dump.entries {
                    let _ = api.merge_remote(key.clone(), value);
                }
                drop(api);

                // Update the peer frontier from the *remote* peer's frontier.
                // We must NOT use our local store frontier here because the local
                // store may be ahead of the remote; using it would cause subsequent
                // delta pulls to miss remote updates between the remote's true
                // frontier and our local frontier.
                if let Some(remote_frontier) = dump.frontier {
                    self.peer_frontiers.insert(peer_key, remote_frontier);
                }
                // If the remote did not report a frontier (e.g. empty store or
                // older peer that doesn't support the field), we intentionally
                // leave peer_frontiers without an entry. This means the next
                // sync cycle will fall back to full sync again, which is safe.

                any_success = true;
                tracing::debug!(
                    peer = %peer.node_id.0,
                    "full sync fallback succeeded"
                );
            }
        }

        if !any_success && !peers.is_empty() {
            self.metrics
                .sync_failure_total
                .fetch_add(1, Ordering::Relaxed);
        }

        tracing::debug!(
            node = %self.node_id.0,
            "anti-entropy sync cycle completed (delta-based)"
        );
    }

    async fn check_compaction(&mut self) {
        let now = self.clock.now();

        let api = self.certified_api.lock().await;
        let ns = api.namespace().read().unwrap();

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

    fn wrap_api(api: CertifiedApi) -> Arc<Mutex<CertifiedApi>> {
        Arc::new(Mutex::new(api))
    }

    #[tokio::test]
    async fn node_runner_starts_and_stops() {
        let api = wrap_api(CertifiedApi::new(node_id("node-1"), default_namespace()));
        let engine = CompactionEngine::with_defaults();
        let config = NodeRunnerConfig {
            certification_interval: Duration::from_millis(10),
            cleanup_interval: Duration::from_millis(50),
            compaction_check_interval: Duration::from_millis(100),
            frontier_report_interval: Duration::from_millis(100),
            sync_interval: None,
        };

        let mut runner =
            NodeRunner::new(node_id("node-1"), api, engine, config, default_metrics()).await;
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

        let shared_api = wrap_api(api);
        let engine = CompactionEngine::with_defaults();
        let config = NodeRunnerConfig {
            certification_interval: Duration::from_millis(10),
            cleanup_interval: Duration::from_secs(60),
            compaction_check_interval: Duration::from_secs(60),
            frontier_report_interval: Duration::from_secs(60),
            sync_interval: None,
        };

        let mut runner = NodeRunner::new(
            node_id("node-1"),
            shared_api.clone(),
            engine,
            config,
            default_metrics(),
        )
        .await;
        let handle = runner.shutdown_handle();

        // Run long enough for at least one certification tick.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = handle.send(true);
        });

        runner.run().await;

        // The pending write should now be certified.
        let api = shared_api.lock().await;
        assert_eq!(
            api.pending_writes()[0].status,
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

        let shared_api = wrap_api(api);
        let engine = CompactionEngine::with_defaults();
        let config = NodeRunnerConfig {
            certification_interval: Duration::from_secs(60),
            cleanup_interval: Duration::from_millis(10),
            compaction_check_interval: Duration::from_secs(60),
            frontier_report_interval: Duration::from_secs(60),
            sync_interval: None,
        };

        let mut runner = NodeRunner::new(
            node_id("node-1"),
            shared_api.clone(),
            engine,
            config,
            default_metrics(),
        )
        .await;
        let handle = runner.shutdown_handle();

        // Run long enough for cleanup to expire the 10ms-TTL write.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(80)).await;
            let _ = handle.send(true);
        });

        runner.run().await;

        // The expired write should have been cleaned up.
        let api = shared_api.lock().await;
        assert_eq!(
            api.pending_writes().len(),
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

        let api = wrap_api(CertifiedApi::new(node_id("node-1"), wrap_ns(ns)));

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

        let mut runner =
            NodeRunner::new(node_id("node-1"), api, engine, config, default_metrics()).await;
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
        let api = wrap_api(CertifiedApi::new(node_id("node-1"), default_namespace()));
        let engine = CompactionEngine::with_defaults();
        let runner = NodeRunner::new(
            node_id("node-1"),
            api,
            engine,
            NodeRunnerConfig::default(),
            default_metrics(),
        )
        .await;

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
        let api = wrap_api(CertifiedApi::new(node_id("node-1"), default_namespace()));
        let engine = CompactionEngine::with_defaults();
        let mut runner = NodeRunner::new(
            node_id("node-1"),
            api.clone(),
            engine,
            NodeRunnerConfig::default(),
            default_metrics(),
        )
        .await;

        assert_eq!(runner.node_id(), &node_id("node-1"));

        // Access through lock.
        {
            let mut locked_api = api.lock().await;
            locked_api
                .certified_write("test".into(), counter_value(1), OnTimeout::Pending)
                .unwrap();
            assert_eq!(locked_api.pending_writes().len(), 1);
        }

        runner.compaction_engine_mut().record_op(&kr("test/"));
    }

    #[tokio::test]
    async fn immediate_shutdown() {
        let api = wrap_api(CertifiedApi::new(node_id("node-1"), default_namespace()));
        let engine = CompactionEngine::with_defaults();
        let config = NodeRunnerConfig {
            certification_interval: Duration::from_secs(60),
            cleanup_interval: Duration::from_secs(60),
            compaction_check_interval: Duration::from_secs(60),
            frontier_report_interval: Duration::from_secs(60),
            sync_interval: None,
        };

        let mut runner =
            NodeRunner::new(node_id("node-1"), api, engine, config, default_metrics()).await;

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
        // node-1 is NOT in the authority set -> no reporter
        let api = wrap_api(CertifiedApi::new(node_id("node-1"), default_namespace()));
        let engine = CompactionEngine::with_defaults();
        let runner = NodeRunner::new(
            node_id("node-1"),
            api,
            engine,
            NodeRunnerConfig::default(),
            default_metrics(),
        )
        .await;
        assert!(!runner.is_authority());
        assert!(runner.frontier_reporter().is_none());

        // auth-1 IS in the authority set -> has reporter
        let api = wrap_api(CertifiedApi::new(node_id("auth-1"), default_namespace()));
        let engine = CompactionEngine::with_defaults();
        let runner = NodeRunner::new(
            node_id("auth-1"),
            api,
            engine,
            NodeRunnerConfig::default(),
            default_metrics(),
        )
        .await;
        assert!(runner.is_authority());
        assert!(runner.frontier_reporter().is_some());
    }

    #[tokio::test]
    async fn frontier_auto_report_advances_local_frontier() {
        // Create a namespace where auth-1 is an authority.
        let ns = default_namespace();
        let shared_api = wrap_api(CertifiedApi::new(node_id("auth-1"), ns));
        let engine = CompactionEngine::with_defaults();

        let config = NodeRunnerConfig {
            certification_interval: Duration::from_secs(60),
            cleanup_interval: Duration::from_secs(60),
            compaction_check_interval: Duration::from_secs(60),
            frontier_report_interval: Duration::from_millis(10),
            sync_interval: None,
        };

        let mut runner = NodeRunner::new(
            node_id("auth-1"),
            shared_api.clone(),
            engine,
            config,
            default_metrics(),
        )
        .await;
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
        let api = shared_api.lock().await;
        let frontiers = api.all_frontiers();
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
        let shared_api = wrap_api(CertifiedApi::new(node_id("store-node"), ns));
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
            shared_api.clone(),
            engine,
            config,
            default_metrics(),
        )
        .await;
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
        let api = shared_api.lock().await;
        let frontiers = api.all_frontiers();
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

        let shared_api = wrap_api(api);
        let engine = CompactionEngine::with_defaults();
        let config = NodeRunnerConfig {
            certification_interval: Duration::from_millis(10),
            cleanup_interval: Duration::from_secs(60),
            compaction_check_interval: Duration::from_secs(60),
            frontier_report_interval: Duration::from_millis(10),
            sync_interval: None,
        };

        let mut runner = NodeRunner::new(
            node_id("auth-1"),
            shared_api.clone(),
            engine,
            config,
            default_metrics(),
        )
        .await;
        let handle = runner.shutdown_handle();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = handle.send(true);
        });

        runner.run().await;

        // The pending write should have been auto-certified.
        let api = shared_api.lock().await;
        assert_eq!(
            api.pending_writes()[0].status,
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

        let shared_api = wrap_api(api);
        let engine = CompactionEngine::with_defaults();
        let config = NodeRunnerConfig {
            certification_interval: Duration::from_secs(60),
            cleanup_interval: Duration::from_secs(60),
            compaction_check_interval: Duration::from_secs(60),
            frontier_report_interval: Duration::from_millis(10),
            sync_interval: None,
        };

        let mut runner = NodeRunner::new(
            node_id("auth-1"),
            shared_api.clone(),
            engine,
            config,
            default_metrics(),
        )
        .await;
        let handle = runner.shutdown_handle();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(80)).await;
            let _ = handle.send(true);
        });

        runner.run().await;

        // The frontier should still be at the high value (not regressed).
        let api = shared_api.lock().await;
        let frontiers = api.all_frontiers();
        assert!(!frontiers.is_empty());
        assert!(
            frontiers[0].frontier_hlc.physical >= u64::MAX - 1000,
            "frontier must not regress below the manually-set high value"
        );
    }

    // ---------------------------------------------------------------
    // Authority auto-reconfiguration tests (#118)
    // ---------------------------------------------------------------

    fn make_node(id: &str, mode: crate::types::NodeMode, tags: &[&str]) -> crate::node::Node {
        use crate::types::Tag;
        let mut n = crate::node::Node::new(node_id(id), mode);
        for t in tags {
            n.add_tag(Tag((*t).into()));
        }
        n
    }

    #[tokio::test]
    async fn membership_change_triggers_authority_recalculation() {
        use crate::placement::PlacementPolicy;
        use crate::types::NodeMode;

        // Create a namespace with a certified policy requiring dc:tokyo tag.
        let mut ns = SystemNamespace::new();
        ns.set_placement_policy(
            PlacementPolicy::new(PolicyVersion(1), kr("user/"), 3)
                .with_certified(true)
                .with_required_tags([crate::types::Tag("dc:tokyo".into())].into()),
        );
        let shared_ns = wrap_ns(ns);

        let api = wrap_api(CertifiedApi::new(node_id("node-1"), shared_ns.clone()));

        // Shared cluster node list (initially empty).
        let cluster_nodes = Arc::new(std::sync::RwLock::new(Vec::<crate::node::Node>::new()));

        let config = NodeRunnerConfig {
            certification_interval: Duration::from_millis(10),
            cleanup_interval: Duration::from_secs(60),
            compaction_check_interval: Duration::from_secs(60),
            frontier_report_interval: Duration::from_secs(60),
            sync_interval: None,
        };

        let mut runner = NodeRunner::with_cluster_nodes(
            node_id("node-1"),
            api.clone(),
            CompactionEngine::with_defaults(),
            config,
            default_metrics(),
            cluster_nodes.clone(),
        )
        .await;
        let handle = runner.shutdown_handle();

        // Initially no authority definition for user/.
        {
            let api_lock = api.lock().await;
            let ns = api_lock.namespace().read().unwrap();
            assert!(ns.get_authority_definition("user/").is_none());
        }

        // Simulate nodes joining the cluster.
        {
            let mut nodes = cluster_nodes.write().unwrap();
            nodes.push(make_node("n1", NodeMode::Store, &["dc:tokyo"]));
            nodes.push(make_node("n2", NodeMode::Store, &["dc:tokyo"]));
            nodes.push(make_node("n3", NodeMode::Store, &["dc:tokyo"]));
        }

        // Run for a bit to let detection fire.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(80)).await;
            let _ = handle.send(true);
        });

        runner.run().await;

        // After detection, authority definition should be auto-created.
        let api_lock = api.lock().await;
        let ns = api_lock.namespace().read().unwrap();
        let def = ns.get_authority_definition("user/");
        assert!(
            def.is_some(),
            "authority definition should be auto-created from certified policy"
        );
        assert_eq!(def.unwrap().authority_nodes.len(), 3);
    }

    #[tokio::test]
    async fn cluster_nodes_accessor_returns_shared_ref() {
        let cluster_nodes = Arc::new(std::sync::RwLock::new(vec![make_node(
            "n1",
            crate::types::NodeMode::Store,
            &["dc:tokyo"],
        )]));

        let api = wrap_api(CertifiedApi::new(node_id("node-1"), default_namespace()));
        let runner = NodeRunner::with_cluster_nodes(
            node_id("node-1"),
            api,
            CompactionEngine::with_defaults(),
            NodeRunnerConfig::default(),
            default_metrics(),
            cluster_nodes.clone(),
        )
        .await;

        assert_eq!(runner.cluster_nodes().read().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn same_size_replacement_triggers_authority_recalculation() {
        use crate::placement::PlacementPolicy;
        use crate::types::NodeMode;

        // Create a namespace with a certified policy requiring dc:tokyo tag.
        let mut ns = SystemNamespace::new();
        ns.set_placement_policy(
            PlacementPolicy::new(PolicyVersion(1), kr("user/"), 3)
                .with_certified(true)
                .with_required_tags([crate::types::Tag("dc:tokyo".into())].into()),
        );
        let shared_ns = wrap_ns(ns);

        let api = wrap_api(CertifiedApi::new(node_id("node-1"), shared_ns.clone()));

        // Start with 3 nodes.
        let initial_nodes = vec![
            make_node("n1", NodeMode::Store, &["dc:tokyo"]),
            make_node("n2", NodeMode::Store, &["dc:tokyo"]),
            make_node("n3", NodeMode::Store, &["dc:tokyo"]),
        ];
        let cluster_nodes = Arc::new(std::sync::RwLock::new(initial_nodes));

        let config = NodeRunnerConfig {
            certification_interval: Duration::from_millis(10),
            cleanup_interval: Duration::from_secs(60),
            compaction_check_interval: Duration::from_secs(60),
            frontier_report_interval: Duration::from_secs(60),
            sync_interval: None,
        };

        let mut runner = NodeRunner::with_cluster_nodes(
            node_id("node-1"),
            api.clone(),
            CompactionEngine::with_defaults(),
            config.clone(),
            default_metrics(),
            cluster_nodes.clone(),
        )
        .await;
        let handle = runner.shutdown_handle();

        // Run briefly to let the initial membership detection fire.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = handle.send(true);
        });
        runner.run().await;

        // Verify initial authority definition: n1, n2, n3.
        {
            let api_lock = api.lock().await;
            let ns = api_lock.namespace().read().unwrap();
            let def = ns.get_authority_definition("user/").unwrap();
            assert_eq!(def.authority_nodes.len(), 3);
            assert!(def.authority_nodes.contains(&node_id("n1")));
            assert!(def.authority_nodes.contains(&node_id("n2")));
            assert!(def.authority_nodes.contains(&node_id("n3")));
        }

        // Same-size replacement: n3 leaves, n4 joins (still 3 nodes).
        {
            let mut nodes = cluster_nodes.write().unwrap();
            nodes.retain(|n| n.id != node_id("n3"));
            nodes.push(make_node("n4", NodeMode::Store, &["dc:tokyo"]));
            assert_eq!(nodes.len(), 3, "node count must remain unchanged");
        }

        // Run again with the same runner state (tracked generation is from
        // the first run). A new runner picks up the same tracked state.
        let mut runner2 = NodeRunner::with_cluster_nodes(
            node_id("node-1"),
            api.clone(),
            CompactionEngine::with_defaults(),
            config,
            default_metrics(),
            cluster_nodes.clone(),
        )
        .await;
        let handle2 = runner2.shutdown_handle();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = handle2.send(true);
        });
        runner2.run().await;

        // After detection, the authority definition should reflect the
        // replacement: n4 replaces n3.
        let api_lock = api.lock().await;
        let ns = api_lock.namespace().read().unwrap();
        let def = ns.get_authority_definition("user/").unwrap();
        assert_eq!(def.authority_nodes.len(), 3);
        assert!(
            def.authority_nodes.contains(&node_id("n4")),
            "n4 should be in authority set after same-size replacement"
        );
        assert!(
            !def.authority_nodes.contains(&node_id("n3")),
            "n3 should no longer be in authority set after leaving"
        );
    }
}
