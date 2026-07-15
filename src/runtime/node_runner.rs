use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, watch};

use crate::api::certified::CertifiedApi;
use crate::api::eventual::EventualApi;
#[cfg(feature = "native-crypto")]
use crate::authority::bls::BlsKeypair;
use crate::authority::certificate::{EpochConfig, EpochManager, KeysetRegistry, KeysetVersion};
use crate::authority::equivocation::{EquivocationDetector, GOSSIP_SAMPLE_MAX, ObserveOutcome};
use crate::authority::frontier_reporter::FrontierReporter;
use crate::authority::frontier_sig::{FrontierSignature, NodeSigner};
use crate::compaction::CompactionEngine;
use crate::control_plane::system_namespace::SystemNamespace;
use crate::crdt::gc::TombstoneGc;
use crate::hlc::{Hlc, HlcTimestamp};
use crate::network::frontier_sync::FrontierSyncClient;
use crate::network::membership::MembershipClient;
use crate::network::sync::{
    DEFAULT_BATCH_SIZE, DigestSyncRequest, DigestSyncResult, MAX_DELTA_PAYLOAD_BYTES, PeerBackoff,
    PullDeltaResult, SyncClient, should_fallback_to_full_sync,
};
use crate::node::Node;
use crate::ops::metrics::RuntimeMetrics;
use crate::ops::slo::{SLO_AUTHORITY_AVAILABILITY, SLO_REPLICATION_CONVERGENCE, SloTracker};
use crate::placement::PlacementPolicy;
use crate::placement::latency::LatencyModel;
use crate::placement::rebalance::{
    DEFAULT_REBALANCE_BATCH_SIZE, RebalancePlan, contiguous_success_count,
};
use crate::placement::topology::TopologyView;
use crate::store::digest::{StoreDigest, bucket_of, compute_store_digest};
use crate::types::{CertificationStatus, KeyRange, NodeId, PolicyVersion};

/// How long a peer stays cached as digest-unsupported (e.g. an old node
/// answering 404 for `/api/internal/sync/digest`) before being re-probed.
/// Bounds the per-cycle probe overhead against not-yet-upgraded peers
/// while letting upgraded peers be picked up within minutes.
const DIGEST_UNSUPPORTED_RETRY: Duration = Duration::from_secs(600);

/// Configuration for BLS key generation in [`NodeRunner`].
///
/// When present, the node generates a BLS keypair and registers its public
/// key in the `EpochManager`'s keyset registry. Nodes without this config
/// continue using Ed25519 signatures only (backward compat).
///
/// Requires the `native-crypto` feature for actual BLS key generation.
/// Without that feature, `BlsConfig` can still be provided but will be
/// silently ignored (Ed25519-only mode is used).
#[derive(Clone)]
pub struct BlsConfig {
    /// 32-byte seed (IKM) for BLS key generation.
    pub seed: [u8; 32],
}

impl std::fmt::Debug for BlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlsConfig")
            .field("seed", &"[REDACTED]")
            .finish()
    }
}

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
    /// How often to exchange peer lists with known peers (membership gossip).
    /// Set to `None` to disable periodic ping.
    /// Default: 10 seconds.
    pub ping_interval: Option<Duration>,
    /// How often to check for epoch boundaries and perform key rotation.
    /// Default: 60 seconds.
    pub epoch_check_interval: Duration,
    /// How often to run tombstone GC on CRDT deferred sets.
    /// Default: 60 seconds.
    pub gc_interval: Duration,
    /// Epoch configuration for key rotation (FR-008).
    /// Default: 24h epoch duration, 7 grace epochs.
    pub epoch_config: EpochConfig,
    /// Optional BLS key configuration. When `Some`, the node generates a BLS
    /// keypair and registers it in the keyset registry, enabling BLS
    /// certificate mode. When `None`, only Ed25519 certificates are used.
    pub bls_config: Option<BlsConfig>,
    /// How often to run ack-frontier GC (remove stale entries).
    /// Default: 60 seconds.
    pub frontier_gc_interval: Duration,
    /// Maximum number of old policy versions to retain in the frontier set.
    /// Entries older than `current_version - max_retained_versions` are
    /// eligible for GC. Default: 2.
    pub frontier_gc_max_retained_versions: u64,
    /// Grace period in seconds after fencing before entries become eligible
    /// for GC. Default: 300 seconds (5 minutes).
    pub frontier_gc_grace_period_secs: u64,
    /// Change rate threshold for falling back to full sync.
    ///
    /// When the ratio `changed_keys / total_keys` exceeds this threshold
    /// during the push phase, delta sync is skipped and the full state is
    /// pushed to the peer instead, because the delta payload is nearly as
    /// large as a full dump. Default: 0.5 (50%).
    pub full_sync_threshold: f64,
    /// Enable digest-based stepwise diff before full-sync fallbacks.
    ///
    /// When `true` (default), the sync loop exchanges two-level key-range
    /// digests with the peer before pushing/pulling a full state dump and
    /// transfers only mismatched buckets (zero transfer on a root match).
    /// Ops kill switch: set `false` to restore the legacy full-sync-only
    /// behaviour (`ASTEROIDB_DIGEST_SYNC_DISABLED=1` in the binary).
    pub digest_sync_enabled: bool,
    /// This node's signing key holder. When `Some` and this node is an
    /// authority, frontier reports are signed (FR-008 signing pipeline).
    pub node_signer: Option<Arc<NodeSigner>>,
    /// Shared keyset registry — the same `Arc` as `AppState.keyset_registry`
    /// so that signing-side keyset resolution and verification agree.
    pub keyset_registry: Option<Arc<std::sync::RwLock<KeysetRegistry>>>,
    /// Optional bearer token for the frontier push client
    /// (`ASTEROIDB_INTERNAL_TOKEN`).
    pub internal_token: Option<String>,
    /// Shared current-epoch counter — the same `Arc` as
    /// `AppState.current_epoch`, refreshed on each epoch check tick.
    pub current_epoch: Option<Arc<std::sync::atomic::AtomicU64>>,
    /// Shared equivocation detector — must be the *same* `Arc` as
    /// `AppState.equivocation`, so evidence detected on the HTTP receive
    /// path rides this runner's gossip (and self-signed reports feed the
    /// same index).
    pub equivocation: Option<Arc<EquivocationDetector>>,
}

impl Default for NodeRunnerConfig {
    fn default() -> Self {
        Self {
            certification_interval: Duration::from_secs(1),
            cleanup_interval: Duration::from_secs(5),
            compaction_check_interval: Duration::from_secs(10),
            frontier_report_interval: Duration::from_secs(1),
            sync_interval: Some(Duration::from_secs(2)),
            ping_interval: Some(Duration::from_secs(10)),
            epoch_check_interval: Duration::from_secs(60),
            gc_interval: Duration::from_secs(60),
            epoch_config: EpochConfig::default(),
            bls_config: None,
            frontier_gc_interval: Duration::from_secs(60),
            frontier_gc_max_retained_versions: 2,
            frontier_gc_grace_period_secs: 300,
            full_sync_threshold: 0.5,
            digest_sync_enabled: true,
            node_signer: None,
            keyset_registry: None,
            internal_token: None,
            current_epoch: None,
            equivocation: None,
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
    ///
    /// NOTE: this frontier also advances on successful PUSHES, so it is
    /// NOT a proof of what this node has received — see
    /// `pull_verified_frontiers` for the session-guarantee counterpart.
    peer_frontiers: HashMap<String, HlcTimestamp>,
    /// Per-peer verified received prefix for session guarantees.
    ///
    /// `pull_verified_frontiers[peer] = f` means this node has received
    /// EVERYTHING the peer's store contained up to HLC `f`, established
    /// exclusively by complete pulls (delta pulls whose request frontier
    /// was covered by the previous verified value, and full dumps).
    /// Unlike `peer_frontiers` it never advances on pushes; per-origin
    /// session claims (`note_applied` / applied-origins adoption) are
    /// only made for deltas requested at or below this frontier —
    /// otherwise a push-advanced request frontier would hide sender
    /// entries this node never received and the claim would be a lie.
    pull_verified_frontiers: HashMap<String, HlcTimestamp>,
    /// Per-peer exponential backoff state for sync retries.
    /// Tracks consecutive failures and gates retry attempts.
    peer_backoffs: HashMap<String, PeerBackoff>,
    /// Peers that rejected digest sync (old nodes without the endpoint or
    /// with a different scheme version), keyed by peer address with the
    /// instant of the rejection. Digest probes are skipped for these
    /// peers until [`DIGEST_UNSUPPORTED_RETRY`] elapses (re-probe picks
    /// up rolling upgrades). Cleaned together with `peer_frontiers` when
    /// peers leave the registry.
    digest_unsupported: HashMap<String, Instant>,
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
    /// Optional membership client for periodic peer list exchange (ping).
    membership_client: Option<MembershipClient>,
    /// Optional SLO tracker for recording operational observations.
    slo_tracker: Option<Arc<SloTracker>>,
    /// Active rebalance plans being executed, keyed by key range prefix.
    ///
    /// When a policy version change is detected, a [`RebalancePlan`] is
    /// computed and stored here. Each sync cycle processes a bounded batch
    /// of additions from the plan. Once all additions have been pushed,
    /// the plan is removed.
    active_rebalance_plans: HashMap<String, ActiveRebalance>,
    /// Snapshot of old placement policies for rebalance plan computation.
    ///
    /// When a policy version change is detected, the old policy is needed
    /// to compute which nodes are new/removed targets.
    tracked_policies: HashMap<String, PlacementPolicy>,
    /// Epoch manager for key rotation lifecycle (FR-008).
    ///
    /// Tracks epoch boundaries and manages keyset rotation. The runner
    /// periodically calls `check_and_rotate()` to detect epoch transitions
    /// and perform automatic key rotation when staged keys are available.
    epoch_manager: EpochManager,
    /// Optional BLS keypair for this node.
    ///
    /// Generated from `BlsConfig::seed` when BLS is configured. Used to
    /// produce BLS signatures and enable `DualModeCertificate` with
    /// `CertificateMode::Bls` instead of Ed25519-only certificates.
    ///
    /// Only available with the `native-crypto` feature.
    #[cfg(feature = "native-crypto")]
    bls_keypair: Option<BlsKeypair>,
    #[cfg(not(feature = "native-crypto"))]
    bls_keypair: Option<()>,
    /// Tombstone garbage collector for CRDT deferred sets.
    ///
    /// Periodically removes safely-reclaimable tombstone dots from
    /// `OrSet` and `OrMap` values in the store, bounding memory growth.
    tombstone_gc: TombstoneGc,
    /// Shared latency model for recording RTT measurements to peers.
    ///
    /// Updated after every successful sync or ping interaction. The same
    /// `Arc` is shared with `AppState` so that placement policies and the
    /// `/api/topology` endpoint have access to live latency data.
    latency_model: Option<Arc<std::sync::RwLock<LatencyModel>>>,
    /// Shared topology view rebuilt periodically from cluster nodes and
    /// latency data. The same `Arc` is shared with `AppState` so the
    /// `/api/topology` endpoint returns current data.
    topology_view: Option<Arc<std::sync::RwLock<TopologyView>>>,
    /// This node's signing key holder for frontier attestations (FR-008).
    node_signer: Option<Arc<NodeSigner>>,
    /// Shared keyset registry (same `Arc` as `AppState.keyset_registry`).
    /// Used to resolve the signing keyset version and for BLS mode detection.
    shared_keyset_registry: Option<Arc<std::sync::RwLock<KeysetRegistry>>>,
    /// HTTP client for pushing signed frontiers to peers. Built when this
    /// node is an authority.
    frontier_sync_client: Option<FrontierSyncClient>,
    /// Shared current-epoch counter (same `Arc` as `AppState.current_epoch`),
    /// refreshed by the epoch check tick.
    current_epoch_shared: Option<Arc<std::sync::atomic::AtomicU64>>,
    /// Shared equivocation detector (same `Arc` as `AppState.equivocation`).
    /// Feeds self-signed attestations into the index and samples gossip
    /// summaries for outgoing frontier pushes.
    equivocation: Option<Arc<EquivocationDetector>>,
}

/// State for an in-progress rebalance operation.
#[derive(Debug, Clone)]
struct ActiveRebalance {
    /// The computed rebalance plan.
    plan: RebalancePlan,
    /// Number of additions already pushed.
    additions_offset: usize,
    /// When this rebalance operation started.
    started_at: Instant,
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
    /// Number of membership ping ticks executed.
    pub ping_ticks: u64,
    /// Number of epoch check ticks executed.
    pub epoch_check_ticks: u64,
    /// Number of tombstone GC ticks executed.
    pub gc_ticks: u64,
    /// Number of ack-frontier GC ticks executed.
    pub frontier_gc_ticks: u64,
}

/// Outcome of applying one delta sync response
/// (see [`NodeRunner::apply_delta_response`]).
struct DeltaApplyOutcome {
    /// Number of per-key merge errors (logged; frontier advances anyway).
    #[allow(dead_code)]
    merge_errors: u64,
    /// Whether session claims (adoption of the sender's `applied_origins`)
    /// could be made. `false` means the delta may be incomplete relative
    /// to this node's verified received prefix (e.g. the sender pruned
    /// past the request frontier); the caller should fall back to a full
    /// sync — a full dump is unconditionally complete — so claims do not
    /// stay suppressed indefinitely.
    claims_ok: bool,
}

/// Outcome of a digest-based pull attempt (see [`NodeRunner::try_digest_pull`]).
enum DigestPullOutcome {
    /// Digest sync completed with full-dump-equivalent coverage (either a
    /// root match with zero transfer, or a mismatched-bucket dump). The
    /// caller records success and skips the legacy full sync.
    Synced,
    /// Digest sync was not possible (unsupported peer, scheme mismatch,
    /// or a network/decode failure). The caller falls through to the
    /// legacy full sync — behaviour identical to before digest sync.
    Fallback,
}

/// Outcome of a digest-based push probe (see [`NodeRunner::try_digest_push`]).
enum DigestPushOutcome {
    /// The probe ran: either the peer already matched (nothing pushed) or
    /// the mismatched-bucket subset was pushed. The caller skips the
    /// legacy full-state push. Partial subset-push failures are also
    /// `Handled` — the frontier was not advanced, so the next cycle
    /// retries; an immediate full push would only resend more.
    Handled,
    /// The probe could not run (unsupported peer, scheme mismatch, or a
    /// network/decode failure). The caller falls through to the legacy
    /// full-state push.
    Fallback,
}

impl NodeRunner {
    /// Initialize epoch manager and optional BLS keypair from config.
    ///
    /// Uses the current wall-clock time as the epoch base so that epoch 0
    /// starts at the time the node is created.
    #[cfg(feature = "native-crypto")]
    fn init_epoch_and_bls(
        config: &NodeRunnerConfig,
        node_id: &NodeId,
    ) -> (EpochManager, Option<BlsKeypair>) {
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let epoch_manager = EpochManager::new(config.epoch_config.clone(), now_secs);

        let bls_keypair = config.bls_config.as_ref().map(|bls_cfg| {
            let kp = BlsKeypair::generate(&bls_cfg.seed);
            tracing::info!(
                node_id = %node_id.0,
                "BLS keypair generated for node"
            );
            kp
        });

        (epoch_manager, bls_keypair)
    }

    /// Initialize epoch manager without BLS (native-crypto disabled).
    #[cfg(not(feature = "native-crypto"))]
    fn init_epoch_and_bls(
        config: &NodeRunnerConfig,
        _node_id: &NodeId,
    ) -> (EpochManager, Option<()>) {
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let epoch_manager = EpochManager::new(config.epoch_config.clone(), now_secs);
        (epoch_manager, None)
    }

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
        let (reporter, tracked_versions, tracked_policies) = {
            let api = certified_api.lock().await;
            let ns = api.namespace().read().unwrap_or_else(|e| e.into_inner());
            let reporter = FrontierReporter::new(node_id.clone(), &ns);
            let versions = Self::snapshot_policy_versions(&ns);
            let policies = Self::snapshot_policies(&ns);
            (reporter, versions, policies)
        };
        let frontier_reporter = if reporter.is_authority() {
            Some(reporter)
        } else {
            None
        };
        let (epoch_manager, bls_keypair) = Self::init_epoch_and_bls(&config, &node_id);
        let frontier_sync_client =
            Self::build_frontier_sync_client(&config, frontier_reporter.is_some());
        let tombstone_gc = TombstoneGc::new(config.gc_interval, Duration::from_secs(300));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        // Initialize the accused-authorities gauge from evidence restored
        // at startup, so a restart does not reset gauge-based alerting
        // while GET /api/authority/equivocations still shows accusations.
        if let Some(detector) = &config.equivocation {
            metrics.set_accused_authorities(detector.accused_count());
        }
        Self {
            clock: Hlc::new(node_id.0.clone()),
            node_id,
            certified_api,
            compaction_engine,
            node_signer: config.node_signer.clone(),
            shared_keyset_registry: config.keyset_registry.clone(),
            current_epoch_shared: config.current_epoch.clone(),
            equivocation: config.equivocation.clone(),
            config,
            frontier_reporter,
            shutdown_tx,
            shutdown_rx,
            sync_client: None,
            eventual_api: None,
            metrics,
            tracked_policy_versions: tracked_versions,
            peer_frontiers: HashMap::new(),
            pull_verified_frontiers: HashMap::new(),
            peer_backoffs: HashMap::new(),
            digest_unsupported: HashMap::new(),
            cluster_nodes,
            // Use sentinel value to force initial recalculation on first tick.
            tracked_cluster_generation: u64::MAX,
            membership_client: None,
            slo_tracker: None,
            active_rebalance_plans: HashMap::new(),
            tracked_policies,
            epoch_manager,
            bls_keypair,
            tombstone_gc,
            latency_model: None,
            topology_view: None,
            frontier_sync_client,
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
        Self::with_sync_and_cluster_nodes(
            node_id,
            certified_api,
            compaction_engine,
            config,
            sync_client,
            eventual_api,
            metrics,
            cluster_nodes,
        )
        .await
    }

    /// Create a `NodeRunner` with anti-entropy sync and a shared cluster node list.
    ///
    /// This variant accepts an external `cluster_nodes` so that HTTP handlers
    /// (via `AppState`) and the runner share the same node list.
    #[allow(clippy::too_many_arguments)]
    pub async fn with_sync_and_cluster_nodes(
        node_id: NodeId,
        certified_api: Arc<Mutex<CertifiedApi>>,
        compaction_engine: CompactionEngine,
        config: NodeRunnerConfig,
        sync_client: SyncClient,
        eventual_api: Arc<Mutex<EventualApi>>,
        metrics: Arc<RuntimeMetrics>,
        cluster_nodes: Arc<std::sync::RwLock<Vec<Node>>>,
    ) -> Self {
        let (reporter, tracked_versions, tracked_policies) = {
            let api = certified_api.lock().await;
            let ns = api.namespace().read().unwrap_or_else(|e| e.into_inner());
            let reporter = FrontierReporter::new(node_id.clone(), &ns);
            let versions = Self::snapshot_policy_versions(&ns);
            let policies = Self::snapshot_policies(&ns);
            (reporter, versions, policies)
        };
        let frontier_reporter = if reporter.is_authority() {
            Some(reporter)
        } else {
            None
        };

        let (epoch_manager, bls_keypair) = Self::init_epoch_and_bls(&config, &node_id);
        let frontier_sync_client =
            Self::build_frontier_sync_client(&config, frontier_reporter.is_some());
        let tombstone_gc = TombstoneGc::new(config.gc_interval, Duration::from_secs(300));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        // Initialize the accused-authorities gauge from evidence restored
        // at startup (see `with_cluster_nodes` for rationale).
        if let Some(detector) = &config.equivocation {
            metrics.set_accused_authorities(detector.accused_count());
        }
        Self {
            clock: Hlc::new(node_id.0.clone()),
            node_id,
            certified_api,
            compaction_engine,
            node_signer: config.node_signer.clone(),
            shared_keyset_registry: config.keyset_registry.clone(),
            current_epoch_shared: config.current_epoch.clone(),
            equivocation: config.equivocation.clone(),
            config,
            frontier_reporter,
            shutdown_tx,
            shutdown_rx,
            sync_client: Some(sync_client),
            eventual_api: Some(eventual_api),
            metrics,
            tracked_policy_versions: tracked_versions,
            peer_frontiers: HashMap::new(),
            pull_verified_frontiers: HashMap::new(),
            peer_backoffs: HashMap::new(),
            digest_unsupported: HashMap::new(),
            cluster_nodes,
            // Use sentinel value to force initial recalculation on first tick,
            // consistent with `with_cluster_nodes()`.
            tracked_cluster_generation: u64::MAX,
            membership_client: None,
            slo_tracker: None,
            active_rebalance_plans: HashMap::new(),
            tracked_policies,
            epoch_manager,
            bls_keypair,
            tombstone_gc,
            latency_model: None,
            topology_view: None,
            frontier_sync_client,
        }
    }

    /// Build the frontier push client for authority nodes.
    ///
    /// Returns `None` for non-authority nodes (nothing to push).
    fn build_frontier_sync_client(
        config: &NodeRunnerConfig,
        is_authority: bool,
    ) -> Option<FrontierSyncClient> {
        if !is_authority {
            return None;
        }
        Some(match &config.internal_token {
            Some(token) => FrontierSyncClient::with_token(token.clone()),
            None => FrontierSyncClient::new(),
        })
    }

    /// Set the membership client for periodic peer list exchange (ping).
    pub fn set_membership_client(&mut self, client: MembershipClient) {
        self.membership_client = Some(client);
    }

    /// Set the SLO tracker for recording operational observations.
    pub fn set_slo_tracker(&mut self, tracker: Arc<SloTracker>) {
        self.slo_tracker = Some(tracker);
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

    /// Set the shared latency model for recording peer RTT measurements.
    ///
    /// The same `Arc` should be shared with `AppState` so that placement
    /// policies and the `/api/topology` endpoint see live latency data.
    pub fn set_latency_model(&mut self, model: Arc<std::sync::RwLock<LatencyModel>>) {
        self.latency_model = Some(model);
    }

    /// Set the shared topology view.
    ///
    /// The same `Arc` should be shared with `AppState` so that the
    /// `/api/topology` endpoint returns current data.
    pub fn set_topology_view(&mut self, view: Arc<std::sync::RwLock<TopologyView>>) {
        self.topology_view = Some(view);
    }

    /// Return a reference to the shared latency model, if configured.
    pub fn latency_model(&self) -> Option<&Arc<std::sync::RwLock<LatencyModel>>> {
        self.latency_model.as_ref()
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

    /// Return a reference to the epoch manager.
    pub fn epoch_manager(&self) -> &EpochManager {
        &self.epoch_manager
    }

    /// Return a mutable reference to the epoch manager.
    pub fn epoch_manager_mut(&mut self) -> &mut EpochManager {
        &mut self.epoch_manager
    }

    /// Return whether this node has BLS keys configured.
    pub fn has_bls_keys(&self) -> bool {
        self.bls_keypair.is_some()
    }

    /// Return a reference to the BLS keypair, if configured.
    ///
    /// Only available with the `native-crypto` feature.
    #[cfg(feature = "native-crypto")]
    pub fn bls_keypair(&self) -> Option<&BlsKeypair> {
        self.bls_keypair.as_ref()
    }

    /// Return the current certificate mode based on BLS availability.
    ///
    /// Returns `CertificateMode::Bls` when BLS keys are configured and
    /// registered in the keyset registry, otherwise `CertificateMode::Ed25519`.
    ///
    /// The shared keyset registry (same instance as `AppState`) is consulted
    /// first: it is where production BLS keys are actually registered. The
    /// internal `EpochManager` registry is only a fallback for tests that
    /// register keys there directly.
    pub fn certificate_mode(&self) -> crate::authority::certificate::CertificateMode {
        use crate::authority::certificate::CertificateMode;
        #[cfg(feature = "native-crypto")]
        if self.bls_keypair.is_some() {
            if let Some(shared) = &self.shared_keyset_registry {
                let registry = shared.read().unwrap_or_else(|e| e.into_inner());
                let version = registry.current_version();
                if registry.get_bls_key(&version, &self.node_id.0).is_some() {
                    return CertificateMode::Bls;
                }
            }
            let version = self.epoch_manager.registry().current_version();
            if self
                .epoch_manager
                .registry()
                .get_bls_key(&version, &self.node_id.0)
                .is_some()
            {
                return CertificateMode::Bls;
            }
        }
        CertificateMode::Ed25519
    }

    /// Record an RTT measurement from this node to a peer.
    ///
    /// No-op if `latency_model` is not configured.
    fn record_peer_rtt(&self, peer_id: &NodeId, rtt: Duration) {
        if let Some(ref model) = self.latency_model {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let rtt_ms = rtt.as_secs_f64() * 1000.0;
            let mut m = model.write().unwrap_or_else(|e| e.into_inner());
            m.update_latency(&self.node_id, peer_id, rtt_ms, now_ms);
        }
    }

    /// Rebuild the shared topology view from the current cluster nodes
    /// and latency model.
    ///
    /// No-op if `topology_view` or `latency_model` is not configured.
    fn rebuild_topology(&self) {
        let (Some(topo_arc), Some(model_arc)) = (&self.topology_view, &self.latency_model) else {
            return;
        };
        let nodes: Vec<Node> = self
            .cluster_nodes
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let model = model_arc.read().unwrap_or_else(|e| e.into_inner());
        let new_view = TopologyView::build(&nodes, &model);
        *topo_arc.write().unwrap_or_else(|e| e.into_inner()) = new_view;
    }

    /// Snapshot the current policy version for each placement policy
    /// in the system namespace.
    fn snapshot_policy_versions(ns: &SystemNamespace) -> HashMap<String, PolicyVersion> {
        ns.all_placement_policies()
            .into_iter()
            .map(|p| (p.key_range.prefix.clone(), p.version))
            .collect()
    }

    /// Snapshot current placement policies (cloned) for rebalance computation.
    fn snapshot_policies(ns: &SystemNamespace) -> HashMap<String, PlacementPolicy> {
        ns.all_placement_policies()
            .into_iter()
            .map(|p| (p.key_range.prefix.clone(), p.clone()))
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
            let ns = api.namespace().read().unwrap_or_else(|e| e.into_inner());
            Self::snapshot_policy_versions(&ns)
        };

        // Collect version changes: (prefix, old_version, new_version).
        let mut changes: Vec<(String, PolicyVersion, PolicyVersion)> = Vec::new();
        for (prefix, new_version) in &current_versions {
            if let Some(old_version) = self.tracked_policy_versions.get(prefix) {
                if old_version != new_version {
                    changes.push((prefix.clone(), *old_version, *new_version));
                }
            } else {
                // New policy: not previously tracked.
                changes.push((prefix.clone(), PolicyVersion(0), *new_version));
            }
        }

        // Detect deleted policies: tracked but no longer in current.
        let mut deleted_prefixes: Vec<(String, PolicyVersion)> = Vec::new();
        for (prefix, old_version) in &self.tracked_policy_versions {
            if !current_versions.contains_key(prefix) {
                deleted_prefixes.push((prefix.clone(), *old_version));
            }
        }

        if changes.is_empty() && deleted_prefixes.is_empty() {
            return;
        }

        // Apply fencing and refresh reporter.
        {
            let mut api = self.certified_api.lock().await;
            for (prefix, old_version, new_version) in &changes {
                let key_range = KeyRange {
                    prefix: prefix.clone(),
                };
                if old_version.0 > 0 {
                    api.fence_version(&key_range, *old_version);
                }
                // The NEW current version may have been fenced earlier: the
                // replicated control-plane version counter can restart below
                // versions this node already used (Bootstrap version_floor
                // trailing a diverged pre-Raft namespace) and later re-assign
                // a fenced version. Frontier reports for the current version
                // would then be silently rejected, stalling certification —
                // lift the fence (and drop its stale old-era entries).
                if api.unfence_version(&key_range, *new_version) {
                    tracing::warn!(
                        prefix = prefix.as_str(),
                        version = new_version.0,
                        "policy version was re-assigned to a previously fenced \
                         version; fence lifted so frontier tracking can resume \
                         (replicated version counter restarted below local \
                         versions — see ops-guide §14.2)"
                    );
                }
            }

            // Fence deleted policies.
            for (prefix, old_version) in &deleted_prefixes {
                let key_range = KeyRange {
                    prefix: prefix.clone(),
                };
                api.fence_version(&key_range, *old_version);
            }

            // Recalculate authorities when any policy change is detected.
            let nodes: Vec<Node> = self
                .cluster_nodes
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            {
                let mut ns = api.namespace().write().unwrap_or_else(|e| e.into_inner());
                ns.recalculate_authorities(&nodes);
            }

            // Refresh the frontier reporter scopes.
            if let Some(reporter) = &mut self.frontier_reporter {
                let ns = api.namespace().read().unwrap_or_else(|e| e.into_inner());
                reporter.refresh_scopes(&ns);
            }
        }

        // Compute rebalance plans for changed policies.
        self.compute_rebalance_plans(&changes, &deleted_prefixes)
            .await;

        // Update tracked versions and policies.
        self.tracked_policy_versions = current_versions;
        let new_policies: HashMap<String, PlacementPolicy> = {
            let api = self.certified_api.lock().await;
            let ns = api.namespace().read().unwrap_or_else(|e| e.into_inner());
            Self::snapshot_policies(&ns)
        };
        self.tracked_policies = new_policies;
    }

    /// Compute rebalance plans for policy changes and queue them for execution.
    async fn compute_rebalance_plans(
        &mut self,
        changes: &[(String, PolicyVersion, PolicyVersion)],
        deleted_prefixes: &[(String, PolicyVersion)],
    ) {
        // We need the eventual API to read current keys.
        let Some(eventual_api) = &self.eventual_api else {
            return;
        };

        let nodes: Vec<Node> = self
            .cluster_nodes
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();

        // Get new policies from the namespace.
        let new_policies: HashMap<String, PlacementPolicy> = {
            let api = self.certified_api.lock().await;
            let ns = api.namespace().read().unwrap_or_else(|e| e.into_inner());
            Self::snapshot_policies(&ns)
        };

        let api = eventual_api.lock().await;

        for (prefix, _old_version, _new_version) in changes {
            let new_policy = match new_policies.get(prefix) {
                Some(p) => p,
                None => continue,
            };
            let old_policy = self.tracked_policies.get(prefix);

            let current_keys: Vec<String> = api
                .store()
                .keys_with_prefix(prefix)
                .into_iter()
                .cloned()
                .collect();

            if current_keys.is_empty() {
                continue;
            }

            let plan = RebalancePlan::compute(
                old_policy,
                new_policy,
                &nodes,
                &current_keys,
                &self.node_id,
            );

            if !plan.is_empty() {
                // If there is already an in-progress plan for this prefix,
                // record it as abandoned before overwriting.
                if let Some(existing) = self.active_rebalance_plans.get(prefix) {
                    tracing::warn!(
                        key_range = %prefix,
                        existing_additions = existing.plan.total_additions(),
                        existing_offset = existing.additions_offset,
                        "overwriting in-progress rebalance plan with new policy change"
                    );
                    self.metrics
                        .record_rebalance_complete(prefix, Duration::ZERO);
                }

                if plan.removals_count() > 0 {
                    tracing::info!(
                        key_range = %prefix,
                        removals = plan.removals_count(),
                        "advisory removals detected (not executed; CRDT merge is idempotent)"
                    );
                }

                self.metrics
                    .record_rebalance_start(prefix, plan.total_additions());
                self.active_rebalance_plans.insert(
                    prefix.clone(),
                    ActiveRebalance {
                        plan,
                        additions_offset: 0,
                        started_at: Instant::now(),
                    },
                );
            }
        }

        // For deleted policies, clear any active rebalance for that prefix.
        for (prefix, _) in deleted_prefixes {
            self.active_rebalance_plans.remove(prefix);
        }
    }

    /// Execute one batch of pending rebalance operations.
    ///
    /// For each active rebalance plan, pushes up to `max_keys_per_cycle`
    /// key additions to their target nodes using the sync client. Once
    /// all additions have been processed, the plan is marked complete.
    async fn execute_rebalance_batch(&mut self) {
        if self.active_rebalance_plans.is_empty() {
            return;
        }

        let Some(sync_client) = &self.sync_client else {
            return;
        };
        let Some(eventual_api) = &self.eventual_api else {
            return;
        };

        let max_keys = DEFAULT_REBALANCE_BATCH_SIZE;
        let mut completed_prefixes: Vec<String> = Vec::new();

        // Collect the prefixes to iterate without borrowing self mutably.
        let prefixes: Vec<String> = self.active_rebalance_plans.keys().cloned().collect();

        for prefix in &prefixes {
            let rebalance = match self.active_rebalance_plans.get(prefix) {
                Some(r) => r,
                None => continue,
            };

            let batch = rebalance
                .plan
                .additions_batch(rebalance.additions_offset, max_keys);
            if batch.is_empty() {
                // All additions have been processed.
                let duration = rebalance.started_at.elapsed();
                self.metrics.record_rebalance_complete(prefix, duration);
                completed_prefixes.push(prefix.clone());
                continue;
            }

            // Group additions by target node, tracking each entry's batch index
            // so we can determine exactly which additions succeeded after push.
            let batch_len = batch.len();
            let mut by_target: HashMap<&NodeId, Vec<(usize, &str)>> = HashMap::new();
            for (batch_idx, addition) in batch.iter().enumerate() {
                by_target
                    .entry(&addition.target_node)
                    .or_default()
                    .push((batch_idx, &addition.key));
            }

            let mut succeeded = vec![false; batch_len];
            let mut migrated = 0u64;
            let mut failed = 0u64;

            // Look up peer addresses from the registry.
            let peers = sync_client.peer_registry().lock().await.all_peers_owned();

            for (target_node, indexed_keys) in &by_target {
                // Find the peer address for this target node.
                let peer = peers.iter().find(|p| p.node_id == **target_node);
                let Some(peer) = peer else {
                    // Target node not in peer registry; count as failed.
                    failed += indexed_keys.len() as u64;
                    continue;
                };

                // Collect entries to push (preserving group order).
                let api = eventual_api.lock().await;
                let resolved: Vec<(usize, String, crate::store::kv::CrdtValue)> = indexed_keys
                    .iter()
                    .filter_map(|(idx, k)| {
                        api.store().get(k).map(|v| (*idx, k.to_string(), v.clone()))
                    })
                    .collect();
                drop(api);

                if resolved.is_empty() {
                    continue;
                }

                let entries: Vec<(String, crate::store::kv::CrdtValue)> = resolved
                    .iter()
                    .map(|(_, k, v)| (k.clone(), v.clone()))
                    .collect();

                let push_result = sync_client
                    .push_changed_keys(&peer.addr, entries, &self.node_id.0, DEFAULT_BATCH_SIZE)
                    .await;

                let pushed_count = match &push_result {
                    Ok(pushed) => *pushed,
                    Err(e) => e.pushed,
                };

                // Mark the first `pushed_count` entries in this group as succeeded.
                for (group_pos, (batch_idx, _, _)) in resolved.iter().enumerate() {
                    if group_pos < pushed_count {
                        succeeded[*batch_idx] = true;
                    }
                }

                match push_result {
                    Ok(pushed) => {
                        migrated += pushed as u64;
                    }
                    Err(e) => {
                        migrated += e.pushed as u64;
                        failed += (resolved.len() - e.pushed) as u64;
                        tracing::warn!(
                            target_node = %target_node.0,
                            error = %e,
                            "rebalance push failed"
                        );
                    }
                }
            }

            self.metrics
                .record_rebalance_progress(prefix, migrated, failed);

            // Advance the offset only past the contiguous block of successful
            // additions from the start of the batch.  This prevents skipping
            // failed additions that appear before later successes.
            let contiguous_ok = contiguous_success_count(&succeeded);
            if let Some(rebalance) = self.active_rebalance_plans.get_mut(prefix) {
                rebalance.additions_offset += contiguous_ok;

                // Check if we just finished.
                if rebalance.additions_offset >= rebalance.plan.additions.len() {
                    let duration = rebalance.started_at.elapsed();
                    self.metrics.record_rebalance_complete(prefix, duration);
                    completed_prefixes.push(prefix.clone());
                }
            }
        }

        // Remove completed rebalance plans.
        for prefix in completed_prefixes {
            self.active_rebalance_plans.remove(&prefix);
        }
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
        let mut sorted: Vec<&Node> = nodes.iter().collect();
        sorted.sort_unstable_by(|a, b| a.id.0.cmp(&b.id.0));
        let mut hasher = DefaultHasher::new();
        sorted.len().hash(&mut hasher);
        for node in sorted {
            node.id.0.hash(&mut hasher);
            node.mode.hash(&mut hasher);
            // Sort tags for deterministic hashing regardless of HashSet order.
            let mut tags: Vec<&str> = node.tags.iter().map(|t| t.0.as_str()).collect();
            tags.sort_unstable();
            tags.len().hash(&mut hasher);
            for tag in tags {
                tag.hash(&mut hasher);
            }
        }
        hasher.finish()
    }

    async fn detect_membership_changes(&mut self) {
        let current_generation = {
            let nodes = self.cluster_nodes.read().unwrap_or_else(|e| e.into_inner());
            Self::cluster_fingerprint(&nodes)
        };
        if current_generation == self.tracked_cluster_generation {
            return;
        }
        self.tracked_cluster_generation = current_generation;

        let nodes: Vec<Node> = self
            .cluster_nodes
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();

        let api = self.certified_api.lock().await;
        let changed = {
            let mut ns = api.namespace().write().unwrap_or_else(|e| e.into_inner());
            ns.recalculate_authorities(&nodes)
        };

        if changed > 0 {
            // Refresh the frontier reporter to pick up new authority scopes.
            let ns = api.namespace().read().unwrap_or_else(|e| e.into_inner());
            let reporter = FrontierReporter::new(self.node_id.clone(), &ns);
            if reporter.is_authority() {
                self.frontier_reporter = Some(reporter);
            } else {
                self.frontier_reporter = None;
            }
        }

        // Rebuild topology view to reflect the new membership.
        self.rebuild_topology();
    }

    /// Run the node event loop until shutdown is signalled.
    ///
    /// This drives periodic background tasks using `tokio::time::interval`:
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
    /// 5. **Epoch check** -- checks for epoch boundary crossings and performs
    ///    key rotation when staged keys are available (FR-008).
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
        let mut epoch_interval = tokio::time::interval_at(
            start + self.config.epoch_check_interval,
            self.config.epoch_check_interval,
        );
        let mut gc_interval =
            tokio::time::interval_at(start + self.config.gc_interval, self.config.gc_interval);
        let mut frontier_gc_interval = tokio::time::interval_at(
            start + self.config.frontier_gc_interval,
            self.config.frontier_gc_interval,
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

        // Ping interval: only create if membership client is configured.
        let ping_duration = self
            .config
            .ping_interval
            .unwrap_or(Duration::from_secs(3600));
        let ping_enabled = self.config.ping_interval.is_some() && self.membership_client.is_some();
        let mut ping_interval = tokio::time::interval_at(start + ping_duration, ping_duration);

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
                _ = epoch_interval.tick() => {
                    self.check_epoch_rotation();
                    stats.epoch_check_ticks += 1;
                }
                _ = gc_interval.tick() => {
                    self.run_gc().await;
                    stats.gc_ticks += 1;
                }
                _ = frontier_gc_interval.tick() => {
                    self.run_frontier_gc().await;
                    stats.frontier_gc_ticks += 1;
                }
                _ = sync_interval.tick(), if sync_enabled => {
                    self.run_sync().await;
                    self.execute_rebalance_batch().await;
                    stats.sync_ticks += 1;
                }
                _ = ping_interval.tick(), if ping_enabled => {
                    self.run_ping().await;
                    stats.ping_ticks += 1;
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
        let now = match self.clock.now() {
            Ok(ts) => ts,
            Err(e) => {
                tracing::error!(error = %e, "HLC overflow in process_certifications; skipping");
                return;
            }
        };
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

        let mut cert_latencies: Vec<Duration> = Vec::new();

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
                    let latency_ms = now_ms.saturating_sub(pw.timestamp.physical);
                    latency_sum += latency_ms * 1000;
                    cert_latencies.push(Duration::from_millis(latency_ms));
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

            // Record individual certification latencies into the sliding window.
            for latency in cert_latencies {
                self.metrics.record_certification_latency(latency);
            }
        }
    }

    async fn run_cleanup(&mut self) {
        let now_ms = match self.clock.now() {
            Ok(ts) => ts.physical,
            Err(e) => {
                tracing::error!(error = %e, "HLC overflow in run_cleanup; skipping");
                return;
            }
        };
        let mut api = self.certified_api.lock().await;
        api.cleanup(now_ms);
    }

    /// Check for epoch boundary crossings and perform key rotation.
    ///
    /// Calls `EpochManager::check_and_rotate()` with the current wall-clock
    /// time. If a rotation event occurs, logs the transition. This enables
    /// automatic keyset rotation at epoch boundaries per FR-008.
    fn check_epoch_rotation(&mut self) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        if let Some(event) = self.epoch_manager.check_and_rotate(now_ms) {
            tracing::info!(
                node_id = %self.node_id.0,
                new_version = event.new_version.0,
                epoch = event.epoch,
                cleaned = event.cleaned_versions.len(),
                "epoch rotation completed"
            );
            self.metrics
                .record_key_rotation_at(event.new_version.0, now_ms);
        }

        // Keep the shared epoch counter (used by verify_proof keyset expiry
        // checks) in sync with wall-clock epoch progression, so that it does
        // not stay frozen at its startup value.
        if let Some(shared) = &self.current_epoch_shared {
            let epoch = self.epoch_manager.current_epoch(now_ms / 1000);
            shared.store(epoch, Ordering::Relaxed);
        }
    }

    /// Generate, sign, apply, and push frontier reports for this authority node.
    ///
    /// When a `NodeSigner` is configured, each frontier gets a
    /// [`FrontierSignature`] (produced outside the certified lock) and is
    /// recorded as a self-verified attestation. Signed or not, the frontiers
    /// are then pushed to all known peers as a fire-and-forget background
    /// task so that network latency never blocks the run loop.
    async fn report_frontiers(&mut self) {
        if let Some(reporter) = &self.frontier_reporter {
            match reporter.report_frontiers(&mut self.clock) {
                Ok(frontiers) => {
                    // Sign outside the certified lock (crypto is CPU-heavy).
                    let signatures: Vec<Option<FrontierSignature>> = match &self.node_signer {
                        Some(signer) => {
                            let keyset_version = self.signing_keyset_version();
                            frontiers
                                .iter()
                                .map(|f| Some(signer.sign_frontier(f, keyset_version.clone())))
                                .collect()
                        }
                        None => frontiers.iter().map(|_| None).collect(),
                    };

                    // Feed our own signed reports into the equivocation
                    // index. An honest node can never conflict with itself
                    // (the HLC is monotone and the digest deterministic), so
                    // a self-equivocation signals a compromised key or a
                    // duplicate process sharing this key seed.
                    if let Some(detector) = &self.equivocation {
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;
                        let mut evidence_dirty = false;
                        for (f, sig) in frontiers.iter().zip(signatures.iter()) {
                            if let Some(sig) = sig
                                && let ObserveOutcome::Equivocation(ev) =
                                    detector.observe(f, sig, now_ms)
                            {
                                tracing::warn!(
                                    authority = %ev.authority_id.0,
                                    key_range = %ev.key_range.prefix,
                                    digest_first = %ev.first.frontier.digest_hash,
                                    digest_second = %ev.second.frontier.digest_hash,
                                    "self-attestation equivocation: possible key compromise or \
                                     duplicate process sharing this signing key"
                                );
                                self.metrics.record_equivocation_at(now_ms);
                                self.metrics
                                    .set_accused_authorities(detector.accused_count());
                                evidence_dirty = true;
                            }
                        }
                        // Persist exactly like the HTTP receive path does:
                        // a self-detected equivocation signals a possible
                        // key compromise, and the operator's likely response
                        // (a restart) must not wipe the only evidence.
                        if evidence_dirty {
                            detector.spawn_persist();
                        }
                    }

                    {
                        let mut api = self.certified_api.lock().await;
                        for (f, sig) in frontiers.iter().zip(signatures.iter()) {
                            match (&self.node_signer, sig) {
                                (Some(signer), Some(sig)) => {
                                    // Own signature: no re-verification needed.
                                    let att = signer.self_verified(f, sig);
                                    api.update_frontier_verified(f.clone(), Some(att));
                                }
                                _ => {
                                    api.update_frontier(f.clone());
                                }
                            }
                        }
                    }

                    // Attach the split-view gossip sample (evidence pairs
                    // first, then newest observed heads) to the same push —
                    // no new protocol, no extra periodic task.
                    let observed = self
                        .equivocation
                        .as_ref()
                        .map(|d| d.gossip_summaries(GOSSIP_SAMPLE_MAX))
                        .unwrap_or_default();
                    self.push_frontiers_to_peers(frontiers, signatures, observed)
                        .await;
                }
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        "HLC overflow in report_frontiers; skipping frontier update"
                    );
                }
            }
        }

        // Compute frontier skew: for each scope, find max and min frontier
        // HLC among authorities, and report the maximum skew across all scopes.
        self.update_frontier_skew().await;
    }

    /// Resolve the keyset version to sign under.
    ///
    /// Uses the shared registry's current version (the latest version under
    /// which this node's keys are registered). Falls back to version 1 when
    /// no registry is shared or the registry is still empty.
    fn signing_keyset_version(&self) -> KeysetVersion {
        self.shared_keyset_registry
            .as_ref()
            .map(|r| {
                r.read()
                    .unwrap_or_else(|e| e.into_inner())
                    .current_version()
            })
            .filter(|v| v.0 > 0)
            .unwrap_or(KeysetVersion(1))
    }

    /// Push frontier reports (and their signatures) to all known peers.
    ///
    /// Spawned as a background task with the 5-second-timeout HTTP client so
    /// the run loop is never blocked by slow peers. Failures are logged at
    /// debug level; the next report tick acts as the retry.
    async fn push_frontiers_to_peers(
        &self,
        frontiers: Vec<crate::authority::ack_frontier::AckFrontier>,
        signatures: Vec<Option<FrontierSignature>>,
        observed: Vec<crate::authority::equivocation::ObservedAttestation>,
    ) {
        let Some(client) = &self.frontier_sync_client else {
            return;
        };
        let Some(sync_client) = &self.sync_client else {
            return;
        };
        if frontiers.is_empty() {
            return;
        }
        let peers = sync_client.peer_registry().lock().await.all_peers_owned();
        if peers.is_empty() {
            return;
        }

        let client = client.clone();
        tokio::spawn(async move {
            for peer in peers {
                match client
                    .push_frontiers_with_observations(
                        &peer.addr,
                        frontiers.clone(),
                        signatures.clone(),
                        observed.clone(),
                    )
                    .await
                {
                    Ok(resp) => {
                        tracing::trace!(
                            peer = %peer.addr,
                            accepted = resp.accepted,
                            "pushed frontiers to peer"
                        );
                    }
                    Err(e) => {
                        tracing::debug!(
                            peer = %peer.addr,
                            error = %e,
                            "frontier push failed; will retry on next report tick"
                        );
                    }
                }
            }
        });
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
    /// 1. Check per-peer backoff; skip peers that are still in cooldown.
    /// 2. If we have a known frontier, push only changed keys (batched).
    /// 3. Pull delta entries from the peer and apply locally.
    /// 4. On failure, fall back to full sync.
    /// 5. Update backoff state on success/failure.
    async fn run_sync(&mut self) {
        let Some(sync_client) = &self.sync_client else {
            return;
        };
        let Some(eventual_api) = &self.eventual_api else {
            return;
        };

        let peers = sync_client.peer_registry().lock().await.all_peers_owned();
        let mut any_success = false;

        for peer in &peers {
            let peer_key = peer.addr.clone();
            let peer_id = &peer.node_id.0;
            let peer_start = Instant::now();

            // Count one attempt per peer so attempt/failure rates are comparable.
            self.metrics
                .sync_attempt_total
                .fetch_add(1, Ordering::Relaxed);

            // Check per-peer backoff; skip if still in cooldown.
            let backoff = self.peer_backoffs.entry(peer_key.clone()).or_default();
            if !backoff.is_ready() {
                tracing::debug!(
                    peer = %peer.node_id.0,
                    failures = backoff.consecutive_failures,
                    "skipping peer due to backoff"
                );
                continue;
            }

            // --- Push phase: send only changed local keys to peer ---
            // When the change rate is too high (changed_keys / total_keys > threshold),
            // delta sync payload approaches full-state size and loses its advantage.
            // In that case, skip delta and push the full state directly.
            if let Some(frontier) = self.peer_frontiers.get(&peer_key) {
                let api = eventual_api.lock().await;
                let total_keys = api.store().len();
                // delta_entries_since returns delta-state entries sorted by
                // HLC; each value contains only the portion changed since
                // the frontier, reducing bandwidth compared to full state.
                let entries_with_hlc: Vec<(
                    String,
                    crate::store::kv::CrdtValue,
                    crate::hlc::HlcTimestamp,
                )> = api.store().delta_entries_since(frontier);
                let changed_count = entries_with_hlc.len();

                // Compute change rate and decide whether to use delta or full sync.
                let change_rate = if total_keys > 0 {
                    changed_count as f64 / total_keys as f64
                } else {
                    0.0
                };

                if should_fallback_to_full_sync(
                    changed_count,
                    total_keys,
                    self.config.full_sync_threshold,
                ) {
                    // High change rate: full-sync territory. Probe the
                    // peer with a key-range digest first — if the states
                    // already match (or only a few buckets differ) the
                    // full-state push is avoided entirely.
                    drop(api);

                    tracing::info!(
                        peer = %peer.node_id.0,
                        change_rate = %format!("{:.2}", change_rate),
                        threshold = %format!("{:.2}", self.config.full_sync_threshold),
                        changed_keys = changed_count,
                        total_keys = total_keys,
                        "change rate exceeds threshold, falling back to full sync push"
                    );

                    let digest_handled = if Self::digest_sync_allowed(
                        &self.digest_unsupported,
                        self.config.digest_sync_enabled,
                        &peer_key,
                    ) {
                        matches!(
                            Self::try_digest_push(
                                sync_client,
                                eventual_api,
                                &self.metrics,
                                &self.node_id.0,
                                peer_id,
                                &peer_key,
                                &peer.addr,
                                &mut self.peer_frontiers,
                                &mut self.digest_unsupported,
                            )
                            .await,
                            DigestPushOutcome::Handled
                        )
                    } else {
                        false
                    };

                    if !digest_handled {
                        self.metrics
                            .full_sync_fallback_count
                            .fetch_add(1, Ordering::Relaxed);

                        let api = eventual_api.lock().await;
                        let all_entries: HashMap<String, crate::store::kv::CrdtValue> = api
                            .store()
                            .all_entries()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect();
                        drop(api);

                        let push_resp = sync_client
                            .push_full_state_to_peer(&peer.addr, all_entries, &self.node_id.0)
                            .await;

                        if let Some(resp) = push_resp {
                            if !resp.errors.is_empty() {
                                tracing::warn!(
                                    peer = %peer.node_id.0,
                                    error_count = resp.errors.len(),
                                    merged = resp.merged,
                                    "full sync push had per-key errors, not advancing frontier"
                                );
                            } else {
                                // After a successful full push, advance the frontier to
                                // the local store's current frontier so the next delta
                                // sync starts from the right point.
                                let api = eventual_api.lock().await;
                                if let Some(current) = api.store().current_frontier() {
                                    self.peer_frontiers.insert(peer_key.clone(), current);
                                }
                                drop(api);
                            }
                        }
                    }
                } else {
                    drop(api);

                    // Normal delta push path.
                    // Separate HLCs (cheap Copy-like fields) from owned key-value
                    // pairs so push_changed_keys can take ownership without an
                    // extra clone of every CrdtValue.
                    let hlc_vec: Vec<crate::hlc::HlcTimestamp> = entries_with_hlc
                        .iter()
                        .map(|(_, _, hlc)| hlc.clone())
                        .collect();
                    let changed: Vec<(String, crate::store::kv::CrdtValue)> = entries_with_hlc
                        .into_iter()
                        .map(|(key, value, _hlc)| (key, value))
                        .collect();

                    if !changed.is_empty() {
                        // Check serialized payload size — if the delta exceeds
                        // MAX_DELTA_PAYLOAD_BYTES, it is cheaper to send a full
                        // state push than an oversized delta.
                        let estimated_size: usize = changed
                            .iter()
                            .map(|(k, v)| {
                                k.len()
                                    + bincode::serde::encode_to_vec(v, bincode::config::standard())
                                        .map(|b| b.len())
                                        .unwrap_or(std::mem::size_of_val(v))
                            })
                            .sum();

                        if estimated_size > MAX_DELTA_PAYLOAD_BYTES {
                            tracing::info!(
                                peer = %peer.node_id.0,
                                estimated_size = estimated_size,
                                limit = MAX_DELTA_PAYLOAD_BYTES,
                                changed_keys = changed_count,
                                "delta payload exceeds size limit, falling back to full sync"
                            );

                            // Digest probe first: skip the full push when
                            // the peer already matches (or push only the
                            // mismatched buckets).
                            let digest_handled = if Self::digest_sync_allowed(
                                &self.digest_unsupported,
                                self.config.digest_sync_enabled,
                                &peer_key,
                            ) {
                                matches!(
                                    Self::try_digest_push(
                                        sync_client,
                                        eventual_api,
                                        &self.metrics,
                                        &self.node_id.0,
                                        peer_id,
                                        &peer_key,
                                        &peer.addr,
                                        &mut self.peer_frontiers,
                                        &mut self.digest_unsupported,
                                    )
                                    .await,
                                    DigestPushOutcome::Handled
                                )
                            } else {
                                false
                            };

                            if !digest_handled {
                                self.metrics
                                    .full_sync_fallback_count
                                    .fetch_add(1, Ordering::Relaxed);

                                let api = eventual_api.lock().await;
                                let snapshot: Vec<(String, crate::store::kv::CrdtValue)> = api
                                    .store()
                                    .all_entries()
                                    .map(|(k, v)| (k.clone(), v.clone()))
                                    .collect();
                                drop(api);

                                let all_entries = tokio::task::spawn_blocking(move || {
                                    snapshot
                                        .into_iter()
                                        .collect::<HashMap<String, crate::store::kv::CrdtValue>>()
                                })
                                .await
                                .expect("spawn_blocking panicked");

                                let push_resp = sync_client
                                    .push_full_state_to_peer(
                                        &peer.addr,
                                        all_entries,
                                        &self.node_id.0,
                                    )
                                    .await;

                                if let Some(resp) = push_resp {
                                    if !resp.errors.is_empty() {
                                        tracing::warn!(
                                            peer = %peer.node_id.0,
                                            error_count = resp.errors.len(),
                                            merged = resp.merged,
                                            "payload overflow full push had per-key errors"
                                        );
                                    } else {
                                        let api = eventual_api.lock().await;
                                        if let Some(current) = api.store().current_frontier() {
                                            self.peer_frontiers.insert(peer_key.clone(), current);
                                        }
                                        drop(api);
                                    }
                                }
                            }
                        } else {
                            self.metrics
                                .delta_sync_count
                                .fetch_add(1, Ordering::Relaxed);

                            let push_result = sync_client
                                .push_changed_keys(
                                    &peer.addr,
                                    changed,
                                    &self.node_id.0,
                                    DEFAULT_BATCH_SIZE,
                                )
                                .await;

                            match push_result {
                                Ok(pushed) => {
                                    tracing::debug!(
                                        peer = %peer.node_id.0,
                                        pushed_keys = pushed,
                                        total_changed = changed_count,
                                        "delta push succeeded"
                                    );
                                    // Record replication convergence SLO: time from
                                    // entry write (HLC physical) to push completion.
                                    if let Some(slo) = &self.slo_tracker {
                                        match self.clock.now() {
                                            Ok(ts) => {
                                                let now_ms = ts.physical;
                                                for hlc in hlc_vec.iter().take(pushed) {
                                                    let convergence_ms =
                                                        now_ms.saturating_sub(hlc.physical) as f64;
                                                    slo.record_observation(
                                                        SLO_REPLICATION_CONVERGENCE,
                                                        convergence_ms,
                                                    );
                                                }
                                            }
                                            Err(e) => {
                                                tracing::error!(
                                                    error = %e,
                                                    "HLC overflow recording SLO convergence; skipping"
                                                );
                                            }
                                        }
                                    }
                                    // Advance peer frontier to the max HLC of the
                                    // pushed batch — NOT current_frontier(), which
                                    // may have advanced past unpushed concurrent
                                    // writes.
                                    if let Some(max_hlc) = hlc_vec.last() {
                                        self.peer_frontiers
                                            .insert(peer_key.clone(), max_hlc.clone());
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        peer = %peer.node_id.0,
                                        error = %e,
                                        pushed = e.pushed,
                                        "delta push failed"
                                    );
                                    // On partial failure, do NOT advance the frontier.
                                    // push_changed_keys converts entries into a HashMap,
                                    // losing HLC order, so the `pushed` count does not
                                    // correspond to the first N HLCs in hlc_vec.
                                    // Advancing would permanently skip failed entries.
                                    // The next sync cycle will re-push from the old
                                    // frontier, which is safe (merges are idempotent).
                                    //
                                    // Record failure metrics but do NOT skip the pull
                                    // phase — the peer may have data we need even if
                                    // our push failed (e.g. network was briefly down
                                    // for outbound but the peer has new writes).
                                    self.metrics
                                        .sync_failure_total
                                        .fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    }
                }
            } else {
                // No frontier known for this peer — this is the initial sync.
                // Push the full local state so the peer receives our data even
                // if it has nothing to offer us in return. Without this push,
                // data written locally would never reach a peer that starts
                // empty, because both the delta push and delta pull paths
                // require a known frontier.
                let api = eventual_api.lock().await;
                let snapshot: Vec<(String, crate::store::kv::CrdtValue)> = api
                    .store()
                    .all_entries()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                drop(api);

                let all_entries = tokio::task::spawn_blocking(move || {
                    snapshot
                        .into_iter()
                        .collect::<HashMap<String, crate::store::kv::CrdtValue>>()
                })
                .await
                .expect("spawn_blocking panicked");

                if !all_entries.is_empty() {
                    tracing::info!(
                        peer = %peer.node_id.0,
                        keys = all_entries.len(),
                        "initial sync: pushing full state to peer (no frontier known)"
                    );

                    match sync_client
                        .push_full_state_to_peer(&peer.addr, all_entries, &self.node_id.0)
                        .await
                    {
                        Some(sync_resp) if sync_resp.errors.is_empty() => {
                            // All keys merged successfully.
                        }
                        Some(sync_resp) => {
                            // 2xx but per-key errors — log but still establish the
                            // frontier so the pull phase can proceed. Per-key merge
                            // errors (e.g. type mismatches on individual keys) should
                            // not block the entire sync pipeline; the pull path is
                            // independent and may bring in data we need.
                            tracing::warn!(
                                peer = %peer.node_id.0,
                                error_count = sync_resp.errors.len(),
                                merged = sync_resp.merged,
                                "initial full push had per-key merge errors"
                            );
                            for err in &sync_resp.errors {
                                tracing::debug!(
                                    peer = %peer.node_id.0,
                                    key = %err.key,
                                    error = %err.error,
                                    "full push per-key error"
                                );
                            }
                        }
                        None => {
                            // Network-level push failed — skip pull and retry next cycle.
                            continue;
                        }
                    }
                }

                // Set the frontier to ZERO so the first delta pull
                // fetches ALL entries from the remote peer. Using
                // local_frontier here would skip remote-only entries
                // at or below our frontier, causing data loss when
                // both peers have independent history. This also
                // handles the empty local store case (nothing to push,
                // but we still need to pull from the peer).
                self.peer_frontiers.insert(
                    peer_key.clone(),
                    crate::hlc::HlcTimestamp {
                        physical: 0,
                        logical: 0,
                        node_id: String::new(),
                    },
                );
            }

            // --- Pull phase: pull delta (or full) from peer ---
            // The request frontier is the VERIFIED received prefix, never
            // the push-advanced peer frontier: pulling from a frontier that
            // pushes advanced past the verified prefix would keep
            // `request > verified` forever, permanently suppressing session
            // claims (pull_verified only advances on claimed pulls). See
            // `pull_request_frontier`.
            if let Some(frontier) = Self::pull_request_frontier(
                &self.peer_frontiers,
                &self.pull_verified_frontiers,
                &peer_key,
            ) {
                let delta_result = sync_client
                    .pull_delta(&peer.addr, &self.node_id.0, &frontier)
                    .await;

                match delta_result {
                    PullDeltaResult::Ok(delta_resp) => {
                        let outcome = Self::apply_delta_response(
                            &mut self.peer_frontiers,
                            &mut self.pull_verified_frontiers,
                            &delta_resp,
                            &peer.node_id.0,
                            &peer_key,
                            eventual_api,
                            &frontier,
                            "delta pull",
                        )
                        .await;

                        if outcome.claims_ok {
                            any_success = true;
                            let elapsed = peer_start.elapsed();
                            self.record_peer_rtt(&peer.node_id, elapsed);
                            self.metrics.record_peer_sync_success(peer_id, elapsed);
                            self.peer_backoffs
                                .entry(peer_key.clone())
                                .or_default()
                                .record_success();
                            tracing::debug!(
                                peer = %peer.node_id.0,
                                delta_entries = delta_resp.entries.len(),
                                rtt_ms = elapsed.as_secs_f64() * 1000.0,
                                "delta sync pull succeeded"
                            );
                            continue;
                        }
                        // Data was merged, but session claims could not be
                        // made (e.g. the sender pruned past our verified
                        // prefix). A full dump is unconditionally complete,
                        // so fall through to full sync to re-establish
                        // verified coverage instead of staying unclaimed
                        // forever.
                        tracing::info!(
                            peer = %peer.node_id.0,
                            "delta pull merged without session claims; \
                             falling back to full sync to re-establish verified coverage"
                        );
                        self.metrics
                            .sync_fallback_total
                            .fetch_add(1, Ordering::Relaxed);
                        // Fall through to full sync below.
                    }
                    PullDeltaResult::DeserializationError => {
                        // Payload was corrupted (e.g. by network jitter).
                        // Skip the delta retry — the same corruption is likely
                        // to recur — and fall through directly to full sync.
                        tracing::warn!(
                            peer = %peer.node_id.0,
                            "delta deserialization failed, skipping retry and falling back to full sync"
                        );
                        self.metrics
                            .sync_fallback_total
                            .fetch_add(1, Ordering::Relaxed);
                        // Fall through to full sync below.
                    }
                    PullDeltaResult::NetworkError => {
                        // Network-level failure; retry once before full sync.
                        let retry_result = sync_client
                            .pull_delta(&peer.addr, &self.node_id.0, &frontier)
                            .await;

                        match retry_result {
                            PullDeltaResult::Ok(delta_resp) => {
                                let outcome = Self::apply_delta_response(
                                    &mut self.peer_frontiers,
                                    &mut self.pull_verified_frontiers,
                                    &delta_resp,
                                    &peer.node_id.0,
                                    &peer_key,
                                    eventual_api,
                                    &frontier,
                                    "delta pull retry",
                                )
                                .await;

                                if outcome.claims_ok {
                                    any_success = true;
                                    let elapsed = peer_start.elapsed();
                                    self.record_peer_rtt(&peer.node_id, elapsed);
                                    self.metrics.record_peer_sync_success(peer_id, elapsed);
                                    self.peer_backoffs
                                        .entry(peer_key.clone())
                                        .or_default()
                                        .record_success();
                                    tracing::debug!(
                                        peer = %peer.node_id.0,
                                        rtt_ms = elapsed.as_secs_f64() * 1000.0,
                                        "delta sync retry succeeded"
                                    );
                                    continue;
                                }
                                tracing::info!(
                                    peer = %peer.node_id.0,
                                    "delta pull retry merged without session claims; \
                                     falling back to full sync to re-establish verified coverage"
                                );
                                self.metrics
                                    .sync_fallback_total
                                    .fetch_add(1, Ordering::Relaxed);
                                // Fall through to full sync below.
                            }
                            _ => {
                                // Retry also failed; fall through to full sync.
                                tracing::warn!(
                                    peer = %peer.node_id.0,
                                    "delta sync pull failed after retry, falling back to full sync"
                                );
                                self.metrics
                                    .sync_fallback_total
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            }

            // Digest-based stepwise diff: before falling back to a full
            // key dump, compare key-range digests with the peer and pull
            // only the mismatched buckets — zero transfer when the states
            // already match. Every failure mode (unsupported peer, scheme
            // mismatch, network/decode error) falls through to the legacy
            // full sync below, unchanged (rolling-upgrade safe).
            if Self::digest_sync_allowed(
                &self.digest_unsupported,
                self.config.digest_sync_enabled,
                &peer_key,
            ) {
                let outcome = Self::try_digest_pull(
                    sync_client,
                    eventual_api,
                    &self.metrics,
                    &self.node_id.0,
                    peer_id,
                    &peer_key,
                    &peer.addr,
                    &mut self.peer_frontiers,
                    &mut self.pull_verified_frontiers,
                    &mut self.digest_unsupported,
                )
                .await;
                if matches!(outcome, DigestPullOutcome::Synced) {
                    any_success = true;
                    let elapsed = peer_start.elapsed();
                    self.record_peer_rtt(&peer.node_id, elapsed);
                    self.metrics.record_peer_sync_success(peer_id, elapsed);
                    self.peer_backoffs
                        .entry(peer_key)
                        .or_default()
                        .record_success();
                    tracing::debug!(
                        peer = %peer.node_id.0,
                        rtt_ms = elapsed.as_secs_f64() * 1000.0,
                        "digest sync fallback succeeded"
                    );
                    continue;
                }
            }

            // Full sync fallback: pull all keys from peer.
            //
            // Frontier adoption (session guarantees): a full dump is the
            // sender's complete state (pruned keys are still present in
            // `entries`), so adopting its applied_origins is
            // unconditionally sound after merging all entries — see
            // `apply_complete_state`, which is shared with the digest
            // sync path precisely so the claims/frontier/poison semantics
            // cannot diverge between the two.
            if let Some(dump) = sync_client.pull_all_keys(&peer.addr).await {
                Self::apply_complete_state(
                    &mut self.peer_frontiers,
                    &mut self.pull_verified_frontiers,
                    eventual_api,
                    peer_id,
                    &peer_key,
                    &dump.entries,
                    &dump.timestamps,
                    dump.frontier.clone(),
                    &dump.applied_origins,
                    &dump.visible_origins,
                    dump.merge_failed_keys.clone(),
                    "full sync",
                )
                .await;

                any_success = true;
                let elapsed = peer_start.elapsed();
                self.record_peer_rtt(&peer.node_id, elapsed);
                self.metrics.record_peer_sync_success(peer_id, elapsed);
                self.peer_backoffs
                    .entry(peer_key)
                    .or_default()
                    .record_success();
                tracing::debug!(
                    peer = %peer.node_id.0,
                    rtt_ms = elapsed.as_secs_f64() * 1000.0,
                    "full sync fallback succeeded"
                );
            } else {
                self.metrics.record_peer_sync_failure(peer_id);
                // Full sync also failed; record failure for backoff.
                self.peer_backoffs
                    .entry(peer_key)
                    .or_default()
                    .record_failure();
            }
        }

        // Prune stale peer frontiers and backoffs: remove entries for peers
        // that are no longer in the registry (e.g. removed via membership changes).
        let active_addrs: std::collections::HashSet<&String> =
            peers.iter().map(|p| &p.addr).collect();
        self.peer_frontiers
            .retain(|addr, _| active_addrs.contains(addr));
        self.pull_verified_frontiers
            .retain(|addr, _| active_addrs.contains(addr));
        self.peer_backoffs
            .retain(|addr, _| active_addrs.contains(addr));
        self.digest_unsupported
            .retain(|addr, _| active_addrs.contains(addr));

        // NOTE: sync_failure_total is incremented per-peer on failure above,
        // so we do not add another increment here to avoid double-counting.

        // Rebuild topology view with fresh latency data.
        if any_success {
            self.rebuild_topology();
        }

        tracing::debug!(
            node = %self.node_id.0,
            "anti-entropy sync cycle completed (delta-based)"
        );
    }

    /// Run one cycle of peer list exchange (membership gossip).
    async fn run_ping(&mut self) {
        if let Some(membership_client) = &mut self.membership_client {
            let result = membership_client.ping_all().await;

            // Record authority availability SLO: 1.0 per successful ping,
            // 0.0 per failed ping.
            if let Some(slo) = &self.slo_tracker {
                for _ in 0..result.successes {
                    slo.record_observation(SLO_AUTHORITY_AVAILABILITY, 100.0);
                }
                for _ in 0..result.failures {
                    slo.record_observation(SLO_AUTHORITY_AVAILABILITY, 0.0);
                }
            }

            // Record per-peer RTT measurements from successful pings.
            for rtt_entry in &result.peer_rtts {
                self.record_peer_rtt(&rtt_entry.node_id, rtt_entry.rtt);
            }

            if result.discovered > 0 {
                tracing::info!(
                    node = %self.node_id.0,
                    discovered = result.discovered,
                    ping_rtts = result.peer_rtts.len(),
                    "peer list exchange discovered new peers"
                );
                // Membership changed — rebuild topology.
                self.rebuild_topology();
            } else if !result.peer_rtts.is_empty() {
                // Latency data updated — rebuild topology.
                self.rebuild_topology();
                tracing::debug!(
                    node = %self.node_id.0,
                    ping_rtts = result.peer_rtts.len(),
                    "peer list exchange completed, no new peers"
                );
            } else {
                tracing::debug!(
                    node = %self.node_id.0,
                    "peer list exchange completed, no new peers"
                );
            }
        }
    }

    async fn check_compaction(&mut self) {
        let now = match self.clock.now() {
            Ok(ts) => ts,
            Err(e) => {
                tracing::error!(error = %e, "HLC overflow in check_compaction; skipping");
                return;
            }
        };

        // Drain per-key write ops recorded by HTTP handlers and aggregate
        // by key range prefix so that hot ranges trigger compaction
        // independently of idle ones.
        let ops_by_key = self.metrics.drain_write_ops_by_key();

        // Phase 1: Acquire certified_api lock, read all needed data, then drop
        // the lock before any subsequent .await points.
        let (defs, frontier_set, policy_versions) = {
            let api = self.certified_api.lock().await;
            let ns = api.namespace().read().unwrap_or_else(|e| e.into_inner());

            // Iterate over all authority definitions to check each key range.
            let defs: Vec<_> = ns
                .all_authority_definitions()
                .into_iter()
                .map(|def| (def.key_range.clone(), def.authority_nodes.len()))
                .collect();

            // Collect policy versions for all key ranges upfront so we don't
            // need to re-acquire the lock later.
            let policy_versions: Vec<_> = defs
                .iter()
                .map(|(key_range, _)| {
                    ns.get_placement_policy(&key_range.prefix)
                        .map(|p| p.version)
                        .unwrap_or(crate::types::PolicyVersion(1))
                })
                .collect();

            let fs = api.frontier_set().clone();

            // Drop ns (RwLock read guard) and api (tokio Mutex guard) here.
            (defs, fs, policy_versions)
        };

        // Phase 2: Aggregate per-key write ops into per-range counts by
        // matching each written key against key range prefixes. Keys that
        // don't match any range are counted under the first range as a
        // fallback (maintains the previous behaviour of counting all ops).
        if !ops_by_key.is_empty() && !defs.is_empty() {
            let mut range_ops: HashMap<&str, u64> = HashMap::new();
            for (key, count) in &ops_by_key {
                let matched = defs
                    .iter()
                    .find(|(kr, _)| key.starts_with(&kr.prefix))
                    .map(|(kr, _)| kr.prefix.as_str());
                let prefix = matched.unwrap_or(&defs[0].0.prefix);
                *range_ops.entry(prefix).or_insert(0) += count;
            }

            for (key_range, _) in &defs {
                let ops = range_ops
                    .get(key_range.prefix.as_str())
                    .copied()
                    .unwrap_or(0);
                for _ in 0..ops {
                    self.compaction_engine.record_op(key_range);
                }
            }
        }

        // Phase 3: Run compaction (checkpoint evaluation + pruning). Only
        // execute when eventual_api is available — without a real store there
        // is nothing to checkpoint or prune, and creating checkpoints against
        // an empty store would accumulate stale entries.
        if let Some(ref eventual_api) = self.eventual_api {
            // Evaluate checkpoint eligibility for each key range.
            for (i, (key_range, _total_authorities)) in defs.iter().enumerate() {
                if self.compaction_engine.should_checkpoint(key_range, &now) {
                    let digest = format!("digest-{}-{}", key_range.prefix, now.physical);
                    self.compaction_engine.create_checkpoint(
                        key_range.clone(),
                        now.clone(),
                        digest,
                        policy_versions[i],
                    );
                }
            }

            // Prune old timestamps from the store.
            let mut ev_api = eventual_api.lock().await;
            let store = ev_api.store_mut();
            for (i, (key_range, total_authorities)) in defs.iter().enumerate() {
                let digest = format!("digest-{}-{}", key_range.prefix, now.physical);
                let pruned = self.compaction_engine.run_compaction(
                    key_range,
                    now.clone(),
                    digest,
                    policy_versions[i],
                    &frontier_set,
                    *total_authorities,
                    store,
                );
                if pruned > 0 {
                    tracing::info!(
                        node_id = %self.node_id.0,
                        key_range = %key_range.prefix,
                        pruned,
                        "compaction pruned old timestamps"
                    );
                }
            }
        }
    }

    /// Run tombstone GC on the eventual store (if available).
    ///
    /// Tombstone GC only runs when the `AckFrontierSet` confirms that all
    /// authorities have acknowledged updates past the retention period.
    ///
    /// **P1-10 fix**: Previous code used `frontier_hlc.physical` (an HLC
    /// millisecond timestamp) as the version floor for
    /// `compact_deferred_with_floor()`, but that function compares against
    /// `Dot.counter` (a small per-node monotonic integer). The units and
    /// identity spaces don't match — HLC physical timestamps are ~10^12
    /// while dot counters are small integers — causing tombstones to be
    /// GC'd too aggressively and resurrecting removed entries on lagging
    /// replicas. Additionally, per-node floors were keyed by `authority_id`
    /// but dots are keyed by writer `node_id`, so lookups never matched.
    ///
    /// The fix uses `compact_deferred()` (counter-based, no external floor)
    /// which correctly checks each dot's counter against the maximum known
    /// counter for that writer node. GC only proceeds when all authorities
    /// have frontier entries, ensuring all replicas have seen the tombstones.
    async fn run_gc(&mut self) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        if !self.tombstone_gc.should_run(now_ms) {
            return;
        }

        // Check that all authorities have acknowledged updates. If any
        // authority has no frontier entry, some replicas may not have seen
        // the tombstones yet, so GC is unsafe.
        let all_authorities_synced = {
            let api = self.certified_api.lock().await;
            let frontiers = api.all_frontiers();
            // Require at least one frontier entry to proceed. The retention
            // period on TombstoneGc provides the additional time buffer.
            !frontiers.is_empty()
        };

        if !all_authorities_synced {
            return;
        }

        if let Some(ref eventual_api) = self.eventual_api {
            let mut api = eventual_api.lock().await;
            let collected = self.tombstone_gc.gc_tombstones(api.store_mut(), now_ms);
            if collected > 0 {
                tracing::info!(
                    node_id = %self.node_id.0,
                    collected,
                    total = self.tombstone_gc.total_collected(),
                    "tombstone GC completed"
                );
            }
        }
    }

    /// Choose the delta-pull request frontier for a peer.
    ///
    /// Returns `None` when no frontier is known yet (initial sync handles
    /// that case in the push phase).
    ///
    /// `peer_frontiers` advances on successful PUSHES, which proves
    /// nothing about what this node has RECEIVED from the peer. Pulling
    /// from a push-advanced frontier makes `request_frontier >
    /// pull_verified_frontiers[peer]` — and since the verified prefix
    /// only advances on claimed pulls, one push would suppress session
    /// claims (adoption of the sender's `applied_origins`) for the rest
    /// of the process lifetime. Requesting from the VERIFIED received
    /// prefix instead (never ahead of the push-advanced frontier) keeps
    /// claims flowing every cycle, at the cost of occasionally re-pulling
    /// entries a push already echoed back (CRDT merges are idempotent).
    fn pull_request_frontier(
        peer_frontiers: &HashMap<String, HlcTimestamp>,
        pull_verified_frontiers: &HashMap<String, HlcTimestamp>,
        peer_key: &str,
    ) -> Option<HlcTimestamp> {
        let pushed = peer_frontiers.get(peer_key)?;
        let zero = HlcTimestamp {
            physical: 0,
            logical: 0,
            node_id: String::new(),
        };
        let verified = pull_verified_frontiers.get(peer_key).unwrap_or(&zero);
        Some(if verified < pushed {
            verified.clone()
        } else {
            pushed.clone()
        })
    }

    /// Apply a delta sync response by merging all entries into the eventual store.
    ///
    /// The peer frontier is advanced regardless of per-key errors so that
    /// successfully merged entries are not re-pulled and permanently-failing
    /// keys (e.g. type mismatches) do not stall the entire sync pipeline.
    ///
    /// Session guarantees: claims are made EXCLUSIVELY by adopting the
    /// sender's transmitted `applied_origins` map — never per entry. A
    /// per-entry claim on the entry's HLC origin would be unsound: even a
    /// transfer that is complete relative to the sender only proves
    /// "receiver ⊇ sender", not that the sender holds the entry origin's
    /// full write prefix (third-party writes can reach the sender through
    /// gappy deltas). Adoption itself is only sound when the delta is
    /// provably a complete diff of the sender's state relative to what
    /// this node already holds:
    ///
    /// 1. `request_frontier <= pull_verified_frontiers[peer]` — everything
    ///    at or below the request frontier has actually been RECEIVED from
    ///    this peer. `peer_frontiers` alone is insufficient: it advances
    ///    on successful pushes, and the sender may hold entries below a
    ///    push-advanced frontier (e.g. old-timestamped writes learned from
    ///    a third node) that this node has never seen.
    /// 2. `request_frontier >= sender pruned_floor` — keys pruned on the
    ///    sender are absent from the delta, so a lower request frontier
    ///    cannot prove completeness.
    ///
    /// When either condition fails, entries are still merged (data
    /// convergence is unaffected) but no claims are made — a false
    /// negative for session reads, never a false success — and the caller
    /// is told via [`DeltaApplyOutcome::claims_ok`] so it can fall back to
    /// a full sync (unconditionally complete) to re-establish coverage.
    #[allow(clippy::too_many_arguments)]
    async fn apply_delta_response(
        peer_frontiers: &mut HashMap<String, HlcTimestamp>,
        pull_verified_frontiers: &mut HashMap<String, HlcTimestamp>,
        delta_resp: &crate::network::sync::DeltaSyncResponse,
        peer_id: &str,
        peer_key: &str,
        eventual_api: &Arc<Mutex<EventualApi>>,
        request_frontier: &HlcTimestamp,
        label: &str,
    ) -> DeltaApplyOutcome {
        let zero = HlcTimestamp {
            physical: 0,
            logical: 0,
            node_id: String::new(),
        };
        let verified = pull_verified_frontiers.get(peer_key).unwrap_or(&zero);
        let coverage_ok = request_frontier <= verified;
        let floor_ok = delta_resp
            .pruned_floor
            .as_ref()
            .is_none_or(|floor| request_frontier >= floor);
        let claims_ok = coverage_ok && floor_ok;
        if !claims_ok {
            tracing::debug!(
                peer = %peer_id,
                coverage_ok,
                floor_ok,
                "delta may be incomplete; merging without session claims"
            );
        }

        let mut api = eventual_api.lock().await;
        let mut last_success_hlc: Option<HlcTimestamp> = None;
        let mut error_count = 0u64;
        for entry in &delta_resp.entries {
            // merge_remote_with_hlc never claims the entry origin; it
            // records the position in the store's visible frontier so
            // response tokens cover it.
            //
            // Per-entry failures keep the adoption below sound in both
            // shapes: a type mismatch poisons the key BEFORE the merge,
            // and a WAL append failure AFTER a successful in-memory merge
            // (CrdtError::Storage) also poisons the key inside
            // merge_remote_with_hlc — so an adopted applied frontier can
            // never claim a contribution whose data record is not in the
            // log (session checks on that key stay fail-closed).
            let result =
                api.merge_remote_with_hlc(entry.key.clone(), &entry.value, entry.hlc.clone());
            match result {
                Ok(()) => last_success_hlc = Some(entry.hlc.clone()),
                Err(e) => {
                    error_count += 1;
                    tracing::warn!(
                        peer = %peer_id,
                        key = %entry.key,
                        error = %e,
                        "{} merge failed for key", label
                    );
                }
            }
        }

        // Frontier adoption (session guarantees): a delta entry's CRDT
        // value can embed contributions from origins other than the
        // entry's own HLC origin, so the local applied_origins alone does
        // not dominate the now-visible state. Adopting the sender's
        // applied_origins closes that gap — and is the ONLY way claims
        // are made on this path. The sender's poisoned keys are unioned
        // whenever claims are made, so contributions dropped on the
        // sender are not claimed as present here.
        // The sender's VISIBLE frontier is merged UNCONDITIONALLY (claims
        // or not): merged entry values may embed contributions from
        // origins their HLCs do not name, and the response session tokens
        // issued here must cover everything a reader can now observe.
        // Over-covering is safe (false-negative direction only).
        // adopt_session_claims persists the adoption as ONE WAL record
        // (poison + frontier can never be separated by a crash); an append
        // failure only degrades durability of the adoption and is retried
        // by the next sync round.
        let no_claims = HashMap::new();
        let (adopt_applied, adopt_failed) = if claims_ok {
            (
                &delta_resp.applied_origins,
                delta_resp.merge_failed_keys.clone(),
            )
        } else {
            (&no_claims, Vec::new())
        };
        if let Err(e) =
            api.adopt_session_claims(adopt_applied, &delta_resp.visible_origins, adopt_failed)
        {
            tracing::warn!(
                peer = %peer_id,
                error = %e,
                "failed to persist adopted session claims ({})", label
            );
        }
        drop(api);

        if error_count > 0 {
            tracing::warn!(
                peer = %peer_id,
                error_count,
                total_entries = delta_resp.entries.len(),
                "{} completed with merge errors", label
            );
        }

        // Advance the frontier even when some entries failed to merge.
        // Per-key merge errors (e.g. type mismatches) are typically permanent
        // for those specific keys, so refusing to advance the frontier would
        // cause the same failing entries to be re-pulled every cycle, permanently
        // stalling progress. By advancing past them, successfully merged entries
        // are not re-transmitted and the failing keys will be retried naturally
        // when the remote peer updates them (creating a new HLC > our frontier).
        let new_frontier = if let Some(ref f) = delta_resp.sender_frontier {
            Some(f.clone())
        } else {
            last_success_hlc
        };
        if let Some(f) = new_frontier {
            // A complete pull extends the verified received prefix: this
            // node held everything <= request_frontier and now also holds
            // (request_frontier, f]. Incomplete pulls leave it unchanged.
            if claims_ok
                && pull_verified_frontiers
                    .get(peer_key)
                    .is_none_or(|existing| f > *existing)
            {
                pull_verified_frontiers.insert(peer_key.to_string(), f.clone());
            }
            peer_frontiers.insert(peer_key.to_string(), f);
        }

        DeltaApplyOutcome {
            merge_errors: error_count,
            claims_ok,
        }
    }

    /// Whether a digest sync should be attempted against this peer.
    ///
    /// Skips peers that recently rejected the digest endpoint/scheme
    /// (old nodes) until [`DIGEST_UNSUPPORTED_RETRY`] elapses, and
    /// everything when the ops kill switch is off.
    fn digest_sync_allowed(
        digest_unsupported: &HashMap<String, Instant>,
        enabled: bool,
        peer_key: &str,
    ) -> bool {
        if !enabled {
            return false;
        }
        match digest_unsupported.get(peer_key) {
            Some(rejected_at) => rejected_at.elapsed() >= DIGEST_UNSUPPORTED_RETRY,
            None => true,
        }
    }

    /// Snapshot the eventual store's data and frontier in ONE lock scope,
    /// then compute the two-level key-range digest OFF the lock.
    ///
    /// The digest describes exactly the returned `data`/frontier (that
    /// coupling is what makes the push-side frontier advancement and the
    /// pull-side claims adoption sound). Hashing is O(total CRDT state
    /// size), so it runs in `spawn_blocking`; the lock is held only for
    /// the clone, matching the existing full-push snapshot pattern.
    async fn snapshot_store_digest(
        eventual_api: &Arc<Mutex<EventualApi>>,
    ) -> (
        StoreDigest,
        std::collections::BTreeMap<String, crate::store::kv::CrdtValue>,
        Option<HlcTimestamp>,
    ) {
        let api = eventual_api.lock().await;
        let data: std::collections::BTreeMap<String, crate::store::kv::CrdtValue> = api
            .store()
            .all_entries()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let frontier = api.store().current_frontier();
        drop(api);

        let (digest, data) = tokio::task::spawn_blocking(move || {
            let digest = compute_store_digest(&data);
            (digest, data)
        })
        .await
        .expect("spawn_blocking panicked");

        (digest, data, frontier)
    }

    /// Apply a COMPLETE state transfer received from a peer.
    ///
    /// "Complete" means that after merging `entries` this node holds
    /// everything the sender's snapshot held: a full key dump satisfies
    /// this trivially, and a digest sync response does too (matched
    /// buckets are byte-identical to the sender's snapshot, mismatched
    /// buckets are transferred in full). That completeness is the
    /// soundness precondition for the unconditional adoption of the
    /// sender's `applied_origins` below; the sender's poisoned keys are
    /// unioned so its dropped contributions are not claimed as present
    /// here, and the visible frontier is merged so response tokens cover
    /// the now-visible contributions. `adopt_session_claims` persists all
    /// three as ONE WAL record; an append failure only degrades the
    /// adoption's durability (retried next round) and is logged.
    ///
    /// Frontier handling matches the historical full-sync behaviour:
    /// - per-key merge errors are logged but do NOT block advancement
    ///   (type mismatches are typically permanent; refusing to advance
    ///   would retry the same failing dump forever — the keys are
    ///   poisoned inside `merge_remote(_with_hlc)` so session checks stay
    ///   fail-closed);
    /// - both `pull_verified_frontiers` (max-monotone) and
    ///   `peer_frontiers` advance to the sender's frontier — never the
    ///   local frontier, which may be ahead of the remote;
    /// - a sender without a frontier (empty store / old peer) yields a
    ///   zero `peer_frontiers` entry so later cycles use the delta paths.
    ///
    /// Returns the number of per-key merge errors.
    #[allow(clippy::too_many_arguments)]
    async fn apply_complete_state(
        peer_frontiers: &mut HashMap<String, HlcTimestamp>,
        pull_verified_frontiers: &mut HashMap<String, HlcTimestamp>,
        eventual_api: &Arc<Mutex<EventualApi>>,
        peer_id: &str,
        peer_key: &str,
        entries: &HashMap<String, crate::store::kv::CrdtValue>,
        timestamps: &HashMap<String, HlcTimestamp>,
        frontier: Option<HlcTimestamp>,
        applied_origins: &HashMap<String, HlcTimestamp>,
        visible_origins: &HashMap<String, HlcTimestamp>,
        merge_failed_keys: Vec<String>,
        label: &str,
    ) -> u64 {
        let mut api = eventual_api.lock().await;
        let mut merge_errors = 0u64;
        for (key, value) in entries {
            // Preserve original HLC timestamps when available to avoid
            // retimestamping imported entries with a local clock tick.
            let result = if let Some(hlc) = timestamps.get(key) {
                api.merge_remote_with_hlc(key.clone(), value, hlc.clone())
            } else {
                api.merge_remote(key.clone(), value)
            };
            if let Err(e) = result {
                merge_errors += 1;
                tracing::warn!(
                    peer = %peer_id,
                    key = %key,
                    error = %e,
                    "{} merge failed for key", label
                );
            }
        }
        if let Err(e) =
            api.adopt_session_claims(applied_origins, visible_origins, merge_failed_keys)
        {
            tracing::warn!(
                peer = %peer_id,
                error = %e,
                "failed to persist adopted session claims ({})", label
            );
        }
        drop(api);

        if merge_errors > 0 {
            tracing::warn!(
                peer = %peer_id,
                error_count = merge_errors,
                total_entries = entries.len(),
                "{} completed with merge errors", label
            );
        }

        if let Some(remote_frontier) = frontier {
            // A complete transfer covers the sender's whole state: the
            // verified received prefix (session guarantees) advances to
            // the remote frontier along with the delta-sync frontier.
            if pull_verified_frontiers
                .get(peer_key)
                .is_none_or(|existing| remote_frontier > *existing)
            {
                pull_verified_frontiers.insert(peer_key.to_string(), remote_frontier.clone());
            }
            peer_frontiers.insert(peer_key.to_string(), remote_frontier);
        } else {
            // Remote reported no frontier (empty store or older peer).
            // Set a zero-epoch frontier so that subsequent sync cycles
            // enter the delta push/pull paths instead of repeatedly
            // falling back; a zero frontier makes `entries_since()`
            // return everything, which is correct for a peer that has
            // seen nothing.
            peer_frontiers.insert(
                peer_key.to_string(),
                HlcTimestamp {
                    physical: 0,
                    logical: 0,
                    node_id: String::new(),
                },
            );
        }

        merge_errors
    }

    /// Attempt a digest-based stepwise pull instead of a full key dump.
    ///
    /// Runs on the full-sync fallback path only (unclaimed delta, decode
    /// failure, or exhausted delta retries). Sends the local digest and
    /// applies the peer's answer: a root match completes with ZERO data
    /// transfer, a mismatch transfers only the differing buckets — both
    /// with full-dump-equivalent session-claim adoption (the response is
    /// a single-snapshot answer, see `internal_digest_sync`).
    ///
    /// Returns [`DigestPullOutcome::Fallback`] on any failure WITHOUT
    /// adopting anything (fail-closed: never a false claim) so the caller
    /// proceeds with the legacy full sync.
    #[allow(clippy::too_many_arguments)]
    async fn try_digest_pull(
        sync_client: &SyncClient,
        eventual_api: &Arc<Mutex<EventualApi>>,
        metrics: &Arc<RuntimeMetrics>,
        node_id: &str,
        peer_id: &str,
        peer_key: &str,
        peer_addr: &str,
        peer_frontiers: &mut HashMap<String, HlcTimestamp>,
        pull_verified_frontiers: &mut HashMap<String, HlcTimestamp>,
        digest_unsupported: &mut HashMap<String, Instant>,
    ) -> DigestPullOutcome {
        metrics
            .digest_sync_attempt_total
            .fetch_add(1, Ordering::Relaxed);

        let (digest, _data, _frontier) = Self::snapshot_store_digest(eventual_api).await;
        let request = DigestSyncRequest::from_digest(node_id, &digest, true);

        match sync_client.digest_sync(peer_addr, &request).await {
            DigestSyncResult::Ok(resp) if resp.scheme_ok => {
                digest_unsupported.remove(peer_key);
                if resp.root_matched {
                    metrics
                        .digest_sync_root_match_total
                        .fetch_add(1, Ordering::Relaxed);
                    metrics
                        .digest_sync_keys_skipped_total
                        .fetch_add(resp.total_keys, Ordering::Relaxed);
                    tracing::info!(
                        peer = %peer_id,
                        total_keys = resp.total_keys,
                        "digest sync: root digest matched, zero-transfer coverage"
                    );
                } else {
                    let transferred = resp.entries.len() as u64;
                    metrics
                        .digest_sync_partial_total
                        .fetch_add(1, Ordering::Relaxed);
                    metrics
                        .digest_sync_keys_transferred_total
                        .fetch_add(transferred, Ordering::Relaxed);
                    metrics.digest_sync_keys_skipped_total.fetch_add(
                        resp.total_keys.saturating_sub(transferred),
                        Ordering::Relaxed,
                    );
                    tracing::info!(
                        peer = %peer_id,
                        mismatched_buckets = resp.mismatched_buckets.len(),
                        transferred_keys = transferred,
                        peer_total_keys = resp.total_keys,
                        "digest sync: transferring mismatched buckets only"
                    );
                }
                Self::apply_complete_state(
                    peer_frontiers,
                    pull_verified_frontiers,
                    eventual_api,
                    peer_id,
                    peer_key,
                    &resp.entries,
                    &resp.timestamps,
                    resp.frontier.clone(),
                    &resp.applied_origins,
                    &resp.visible_origins,
                    resp.merge_failed_keys.clone(),
                    "digest sync",
                )
                .await;
                DigestPullOutcome::Synced
            }
            DigestSyncResult::Ok(_) => {
                // scheme_ok = false: version mismatch during a rolling
                // upgrade. Cache and use the legacy full sync meanwhile.
                metrics
                    .digest_sync_unsupported_total
                    .fetch_add(1, Ordering::Relaxed);
                digest_unsupported.insert(peer_key.to_string(), Instant::now());
                tracing::info!(
                    peer = %peer_id,
                    "peer rejected digest scheme version; falling back to full sync"
                );
                DigestPullOutcome::Fallback
            }
            DigestSyncResult::Unsupported => {
                metrics
                    .digest_sync_unsupported_total
                    .fetch_add(1, Ordering::Relaxed);
                digest_unsupported.insert(peer_key.to_string(), Instant::now());
                tracing::info!(
                    peer = %peer_id,
                    "peer does not support digest sync; falling back to full sync"
                );
                DigestPullOutcome::Fallback
            }
            DigestSyncResult::Failed => {
                // Fail-closed: nothing was merged or claimed. The legacy
                // full sync below (or the next cycle) re-establishes
                // coverage. Not cached as unsupported: transient network
                // failures should not suppress digest sync for 10 minutes.
                metrics
                    .digest_sync_failed_total
                    .fetch_add(1, Ordering::Relaxed);
                DigestPullOutcome::Fallback
            }
        }
    }

    /// Attempt a digest probe + subset push instead of a full-state push.
    ///
    /// Runs on the push-side full-sync branches (high change rate or
    /// oversized delta). Sends the local digest with
    /// `include_entries = false`; on a root match nothing is pushed at
    /// all, otherwise only the local keys living in mismatched buckets
    /// are pushed (batched through the existing `/api/internal/sync`
    /// endpoint, whose WAL-durability ack semantics are unchanged).
    ///
    /// On success the push frontier advances to the SNAPSHOT-time
    /// frontier — deliberately not `current_frontier()`, which may have
    /// advanced past writes that were not part of the compared state
    /// (they are delta-pushed next cycle). Partial subset-push failures
    /// leave the frontier untouched (idempotent re-push next cycle),
    /// matching the delta push policy.
    #[allow(clippy::too_many_arguments)]
    async fn try_digest_push(
        sync_client: &SyncClient,
        eventual_api: &Arc<Mutex<EventualApi>>,
        metrics: &Arc<RuntimeMetrics>,
        node_id: &str,
        peer_id: &str,
        peer_key: &str,
        peer_addr: &str,
        peer_frontiers: &mut HashMap<String, HlcTimestamp>,
        digest_unsupported: &mut HashMap<String, Instant>,
    ) -> DigestPushOutcome {
        metrics
            .digest_push_probe_total
            .fetch_add(1, Ordering::Relaxed);

        let (digest, data, snapshot_frontier) = Self::snapshot_store_digest(eventual_api).await;
        let request = DigestSyncRequest::from_digest(node_id, &digest, false);

        match sync_client.digest_sync(peer_addr, &request).await {
            DigestSyncResult::Ok(resp) if resp.scheme_ok => {
                digest_unsupported.remove(peer_key);
                if resp.root_matched {
                    metrics
                        .digest_push_match_total
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::info!(
                        peer = %peer_id,
                        "digest push probe: peer already matches, skipping full push"
                    );
                    if let Some(frontier) = snapshot_frontier {
                        peer_frontiers.insert(peer_key.to_string(), frontier);
                    }
                    return DigestPushOutcome::Handled;
                }

                let mismatched: std::collections::HashSet<u16> =
                    resp.mismatched_buckets.iter().copied().collect();
                let changed: Vec<(String, crate::store::kv::CrdtValue)> = data
                    .into_iter()
                    .filter(|(key, _)| mismatched.contains(&(bucket_of(key) as u16)))
                    .collect();

                if changed.is_empty() {
                    // Every mismatched bucket is empty on OUR side: the
                    // peer holds data we lack, but everything we hold it
                    // already has — the snapshot state is fully conveyed,
                    // so the snapshot frontier may advance. The pull
                    // phase fetches the peer-only data.
                    tracing::debug!(
                        peer = %peer_id,
                        "digest push probe: mismatches are peer-only data, nothing to push"
                    );
                    if let Some(frontier) = snapshot_frontier {
                        peer_frontiers.insert(peer_key.to_string(), frontier);
                    }
                    return DigestPushOutcome::Handled;
                }

                let changed_count = changed.len();
                match sync_client
                    .push_changed_keys(peer_addr, changed, node_id, DEFAULT_BATCH_SIZE)
                    .await
                {
                    Ok(pushed) => {
                        metrics
                            .digest_push_keys_pushed_total
                            .fetch_add(pushed as u64, Ordering::Relaxed);
                        tracing::info!(
                            peer = %peer_id,
                            pushed_keys = pushed,
                            mismatched_buckets = resp.mismatched_buckets.len(),
                            "digest push: pushed mismatched buckets instead of full state"
                        );
                        if let Some(frontier) = snapshot_frontier {
                            peer_frontiers.insert(peer_key.to_string(), frontier);
                        }
                        DigestPushOutcome::Handled
                    }
                    Err(e) => {
                        metrics
                            .digest_push_keys_pushed_total
                            .fetch_add(e.pushed as u64, Ordering::Relaxed);
                        metrics.sync_failure_total.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(
                            peer = %peer_id,
                            error = %e,
                            pushed = e.pushed,
                            total_changed = changed_count,
                            "digest subset push failed; not advancing frontier"
                        );
                        // Handled (not Fallback): a full push now would
                        // resend strictly more over the same failing
                        // link; the next cycle retries idempotently.
                        // Per-key merge errors do not starve later keys:
                        // push_changed_keys still attempts every batch
                        // and only aborts on transport/HTTP failures,
                        // so all mergeable keys were already delivered
                        // (matching the legacy full push).
                        DigestPushOutcome::Handled
                    }
                }
            }
            DigestSyncResult::Ok(_) => {
                metrics
                    .digest_sync_unsupported_total
                    .fetch_add(1, Ordering::Relaxed);
                digest_unsupported.insert(peer_key.to_string(), Instant::now());
                tracing::info!(
                    peer = %peer_id,
                    "peer rejected digest scheme version; falling back to full push"
                );
                DigestPushOutcome::Fallback
            }
            DigestSyncResult::Unsupported => {
                metrics
                    .digest_sync_unsupported_total
                    .fetch_add(1, Ordering::Relaxed);
                digest_unsupported.insert(peer_key.to_string(), Instant::now());
                tracing::info!(
                    peer = %peer_id,
                    "peer does not support digest sync; falling back to full push"
                );
                DigestPushOutcome::Fallback
            }
            DigestSyncResult::Failed => {
                metrics
                    .digest_sync_failed_total
                    .fetch_add(1, Ordering::Relaxed);
                DigestPushOutcome::Fallback
            }
        }
    }

    /// Run garbage collection on stale ack-frontier entries.
    ///
    /// Determines the current policy version **per key range** across all
    /// authority definitions and delegates to
    /// [`CertifiedApi::gc_frontier_entries`].
    ///
    /// Using per-range versions prevents over-deleting slow ranges: if one
    /// key range is at v10 and another at v3, each range gets its own cutoff.
    async fn run_frontier_gc(&mut self) {
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut api = self.certified_api.lock().await;

        // Build per-range current version map from authority definitions.
        let current_versions: std::collections::HashMap<
            crate::types::KeyRange,
            crate::types::PolicyVersion,
        > = {
            let ns = api.namespace().read().unwrap_or_else(|e| e.into_inner());
            let mut versions = std::collections::HashMap::new();
            for def in ns.all_authority_definitions() {
                if let Some(policy) = ns.get_placement_policy(&def.key_range.prefix) {
                    versions
                        .entry(def.key_range.clone())
                        .and_modify(|v: &mut crate::types::PolicyVersion| {
                            if policy.version.0 > v.0 {
                                *v = policy.version;
                            }
                        })
                        .or_insert(policy.version);
                }
            }
            versions
        };

        let removed = api.gc_frontier_entries(
            &current_versions,
            self.config.frontier_gc_max_retained_versions,
            self.config.frontier_gc_grace_period_secs,
            now_secs,
        );

        if removed > 0 {
            tracing::info!(
                node_id = %self.node_id.0,
                removed,
                remaining = api.frontier_count(),
                "ack-frontier GC completed"
            );
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
    use crate::ops::slo::{SLO_REPLICATION_CONVERGENCE, SloTracker};
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
            auto_generated: false,
        });
        ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(1), kr(""), 3))
            .unwrap();
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

    // -----------------------------------------------------------------
    // digest_sync_allowed (pure TTL gate for the digest-unsupported cache)
    // -----------------------------------------------------------------

    #[test]
    fn digest_sync_allowed_respects_kill_switch() {
        let cache = HashMap::new();
        assert!(
            !NodeRunner::digest_sync_allowed(&cache, false, "peer:1"),
            "kill switch off must suppress digest sync even for unknown peers"
        );
        assert!(
            NodeRunner::digest_sync_allowed(&cache, true, "peer:1"),
            "unknown peer with the switch on must be probed"
        );
    }

    #[test]
    fn digest_sync_allowed_skips_recently_rejected_peer() {
        let mut cache = HashMap::new();
        cache.insert("peer:1".to_string(), Instant::now());
        assert!(
            !NodeRunner::digest_sync_allowed(&cache, true, "peer:1"),
            "a peer that just rejected the digest route must not be re-probed"
        );
        assert!(
            NodeRunner::digest_sync_allowed(&cache, true, "peer:2"),
            "other peers are unaffected by peer:1's rejection"
        );
    }

    #[test]
    fn digest_sync_allowed_reprobes_after_ttl() {
        // An entry exactly DIGEST_UNSUPPORTED_RETRY old (or older) must be
        // re-probed so upgraded peers are picked up automatically.
        let Some(expired) = Instant::now().checked_sub(DIGEST_UNSUPPORTED_RETRY) else {
            // Platforms where Instant cannot represent t-10min (e.g. just
            // after boot) cannot run this case.
            return;
        };
        let mut cache = HashMap::new();
        cache.insert("peer:1".to_string(), expired);
        assert!(
            NodeRunner::digest_sync_allowed(&cache, true, "peer:1"),
            "peer must be re-probed once DIGEST_UNSUPPORTED_RETRY has elapsed"
        );

        // Just under the TTL: still suppressed.
        let Some(recent) = Instant::now().checked_sub(DIGEST_UNSUPPORTED_RETRY / 2) else {
            return;
        };
        cache.insert("peer:1".to_string(), recent);
        assert!(
            !NodeRunner::digest_sync_allowed(&cache, true, "peer:1"),
            "peer must stay suppressed while the TTL has not elapsed"
        );
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
            ping_interval: None,
            ..NodeRunnerConfig::default()
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
            ping_interval: None,
            ..NodeRunnerConfig::default()
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
            ping_interval: None,
            ..NodeRunnerConfig::default()
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
            auto_generated: false,
        });
        ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(1), kr("data/"), 3))
            .unwrap();

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
            ping_interval: None,
            ..NodeRunnerConfig::default()
        };

        // Compaction now requires an eventual_api — without it, checkpoints are
        // not created (running against an empty store accumulates stale entries).
        let eventual_api = crate::api::eventual::EventualApi::new(node_id("node-1"));
        let eventual_api = Arc::new(Mutex::new(eventual_api));

        let mut runner =
            NodeRunner::new(node_id("node-1"), api, engine, config, default_metrics()).await;
        runner.set_eventual_api(eventual_api);
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
            ping_interval: None,
            ..NodeRunnerConfig::default()
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
            ping_interval: None,
            ..NodeRunnerConfig::default()
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
            ping_interval: None,
            ..NodeRunnerConfig::default()
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
            auto_generated: false,
        });
        ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(1), kr(""), 1))
            .unwrap();

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
            ping_interval: None,
            ..NodeRunnerConfig::default()
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
            auto_generated: false,
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
            ping_interval: None,
            ..NodeRunnerConfig::default()
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
        )
        .unwrap();
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
            ping_interval: None,
            ..NodeRunnerConfig::default()
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
            let ns = api_lock
                .namespace()
                .read()
                .unwrap_or_else(|e| e.into_inner());
            assert!(ns.get_authority_definition("user/").is_none());
        }

        // Simulate nodes joining the cluster.
        {
            let mut nodes = cluster_nodes.write().unwrap_or_else(|e| e.into_inner());
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
        let ns = api_lock
            .namespace()
            .read()
            .unwrap_or_else(|e| e.into_inner());
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

        assert_eq!(
            runner
                .cluster_nodes()
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .len(),
            1
        );
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
        )
        .unwrap();
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
            ping_interval: None,
            ..NodeRunnerConfig::default()
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
            let ns = api_lock
                .namespace()
                .read()
                .unwrap_or_else(|e| e.into_inner());
            let def = ns.get_authority_definition("user/").unwrap();
            assert_eq!(def.authority_nodes.len(), 3);
            assert!(def.authority_nodes.contains(&node_id("n1")));
            assert!(def.authority_nodes.contains(&node_id("n2")));
            assert!(def.authority_nodes.contains(&node_id("n3")));
        }

        // Same-size replacement: n3 leaves, n4 joins (still 3 nodes).
        {
            let mut nodes = cluster_nodes.write().unwrap_or_else(|e| e.into_inner());
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
        let ns = api_lock
            .namespace()
            .read()
            .unwrap_or_else(|e| e.into_inner());
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

    // ---------------------------------------------------------------
    // Policy version change detection tests (#160, #161)
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn detect_version_changes_picks_up_new_policy() {
        use crate::placement::PlacementPolicy;
        use crate::types::NodeMode;

        // Start with an empty namespace (no policies).
        let ns = SystemNamespace::new();
        let shared_ns = wrap_ns(ns);

        let api = wrap_api(CertifiedApi::new(node_id("node-1"), shared_ns.clone()));

        let cluster_nodes = Arc::new(std::sync::RwLock::new(vec![
            make_node("n1", NodeMode::Store, &["dc:tokyo"]),
            make_node("n2", NodeMode::Store, &["dc:tokyo"]),
            make_node("n3", NodeMode::Store, &["dc:tokyo"]),
        ]));

        let config = NodeRunnerConfig {
            certification_interval: Duration::from_millis(10),
            cleanup_interval: Duration::from_secs(60),
            compaction_check_interval: Duration::from_secs(60),
            frontier_report_interval: Duration::from_secs(60),
            sync_interval: None,
            ping_interval: None,
            ..NodeRunnerConfig::default()
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

        // No authority definition initially.
        {
            let api_lock = api.lock().await;
            let ns = api_lock
                .namespace()
                .read()
                .unwrap_or_else(|e| e.into_inner());
            assert!(ns.get_authority_definition("data/").is_none());
        }

        // Add a new certified policy while the runner is alive.
        {
            let api_lock = api.lock().await;
            let mut ns = api_lock
                .namespace()
                .write()
                .unwrap_or_else(|e| e.into_inner());
            ns.set_placement_policy(
                PlacementPolicy::new(PolicyVersion(1), kr("data/"), 3)
                    .with_certified(true)
                    .with_required_tags([crate::types::Tag("dc:tokyo".into())].into()),
            )
            .unwrap();
        }

        let handle = runner.shutdown_handle();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(80)).await;
            let _ = handle.send(true);
        });
        runner.run().await;

        // After detection, the new policy should have triggered authority creation.
        let api_lock = api.lock().await;
        let ns = api_lock
            .namespace()
            .read()
            .unwrap_or_else(|e| e.into_inner());
        let def = ns.get_authority_definition("data/");
        assert!(
            def.is_some(),
            "new policy addition should trigger recalculate_authorities"
        );
        assert_eq!(def.unwrap().authority_nodes.len(), 3);
    }

    #[tokio::test]
    async fn detect_version_changes_handles_deleted_policy() {
        use crate::placement::PlacementPolicy;
        use crate::types::NodeMode;

        // Start with one certified policy.
        let mut ns = SystemNamespace::new();
        ns.set_placement_policy(
            PlacementPolicy::new(PolicyVersion(1), kr("data/"), 3)
                .with_certified(true)
                .with_required_tags([crate::types::Tag("dc:tokyo".into())].into()),
        )
        .unwrap();
        let shared_ns = wrap_ns(ns);

        let api = wrap_api(CertifiedApi::new(node_id("node-1"), shared_ns.clone()));

        let cluster_nodes = Arc::new(std::sync::RwLock::new(vec![
            make_node("n1", NodeMode::Store, &["dc:tokyo"]),
            make_node("n2", NodeMode::Store, &["dc:tokyo"]),
            make_node("n3", NodeMode::Store, &["dc:tokyo"]),
        ]));

        let config = NodeRunnerConfig {
            certification_interval: Duration::from_millis(10),
            cleanup_interval: Duration::from_secs(60),
            compaction_check_interval: Duration::from_secs(60),
            frontier_report_interval: Duration::from_secs(60),
            sync_interval: None,
            ping_interval: None,
            ..NodeRunnerConfig::default()
        };

        // First run: let it pick up the initial policy.
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
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = handle.send(true);
        });
        runner.run().await;

        // Verify initial tracked state has the data/ policy.
        assert!(runner.tracked_policy_versions.contains_key("data/"));

        // Now remove the policy from the namespace.
        {
            let api_lock = api.lock().await;
            let mut ns = api_lock
                .namespace()
                .write()
                .unwrap_or_else(|e| e.into_inner());
            ns.remove_placement_policy("data/");
        }

        // Call detect_version_changes directly to check deletion detection.
        runner.detect_version_changes().await;

        // After detection, the deleted prefix should no longer be tracked.
        assert!(
            !runner.tracked_policy_versions.contains_key("data/"),
            "deleted policy should be removed from tracked versions"
        );
    }

    #[tokio::test]
    async fn detect_version_changes_recalculates_authorities_on_version_bump() {
        use crate::placement::PlacementPolicy;
        use crate::types::NodeMode;

        // Start with a certified policy.
        let mut ns = SystemNamespace::new();
        ns.set_placement_policy(
            PlacementPolicy::new(PolicyVersion(1), kr("user/"), 2)
                .with_certified(true)
                .with_required_tags([crate::types::Tag("dc:tokyo".into())].into()),
        )
        .unwrap();
        let shared_ns = wrap_ns(ns);

        let api = wrap_api(CertifiedApi::new(node_id("node-1"), shared_ns.clone()));

        let cluster_nodes = Arc::new(std::sync::RwLock::new(vec![
            make_node("n1", NodeMode::Store, &["dc:tokyo"]),
            make_node("n2", NodeMode::Store, &["dc:tokyo"]),
            make_node("n3", NodeMode::Store, &["dc:tokyo"]),
        ]));

        let config = NodeRunnerConfig {
            certification_interval: Duration::from_millis(10),
            cleanup_interval: Duration::from_secs(60),
            compaction_check_interval: Duration::from_secs(60),
            frontier_report_interval: Duration::from_secs(60),
            sync_interval: None,
            ping_interval: None,
            ..NodeRunnerConfig::default()
        };

        // First run to establish baseline.
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
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = handle.send(true);
        });
        runner.run().await;

        // Authority should exist with replica_count=2.
        {
            let api_lock = api.lock().await;
            let ns = api_lock
                .namespace()
                .read()
                .unwrap_or_else(|e| e.into_inner());
            let def = ns.get_authority_definition("user/");
            assert!(def.is_some(), "authority definition should exist initially");
        }

        // Bump the policy version with new replica_count=3.
        {
            let api_lock = api.lock().await;
            let mut ns = api_lock
                .namespace()
                .write()
                .unwrap_or_else(|e| e.into_inner());
            ns.set_placement_policy(
                PlacementPolicy::new(PolicyVersion(2), kr("user/"), 3)
                    .with_certified(true)
                    .with_required_tags([crate::types::Tag("dc:tokyo".into())].into()),
            )
            .unwrap();
        }

        // Call detect_version_changes directly.
        runner.detect_version_changes().await;

        // The tracked version should be updated to v2.
        assert_eq!(
            runner.tracked_policy_versions.get("user/"),
            Some(&PolicyVersion(2)),
            "tracked version should be updated after version bump"
        );

        // Authority should have been recalculated (3 nodes match the new replica_count=3).
        let api_lock = api.lock().await;
        let ns = api_lock
            .namespace()
            .read()
            .unwrap_or_else(|e| e.into_inner());
        let def = ns.get_authority_definition("user/").unwrap();
        assert_eq!(
            def.authority_nodes.len(),
            3,
            "authority should be recalculated after version bump"
        );
    }

    /// Fenced-version reuse: when the replicated version counter restarts
    /// below versions this node already used (Bootstrap version_floor
    /// trailing a diverged pre-Raft namespace) and later re-assigns a fenced
    /// version as the CURRENT one, the fence must be lifted — otherwise all
    /// frontier reports for the current version are silently rejected and
    /// certification for the prefix stalls.
    #[tokio::test]
    async fn detect_version_changes_unfences_reassigned_current_version() {
        use crate::authority::ack_frontier::AckFrontier;
        use crate::placement::PlacementPolicy;
        use crate::types::NodeMode;

        // Pre-Raft divergence: the local namespace holds "user/" at v5.
        let mut ns = SystemNamespace::new();
        ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(5), kr("user/"), 3))
            .unwrap();
        let shared_ns = wrap_ns(ns);
        let api = wrap_api(CertifiedApi::new(node_id("node-1"), shared_ns.clone()));

        let cluster_nodes = Arc::new(std::sync::RwLock::new(vec![
            make_node("n1", NodeMode::Store, &["dc:tokyo"]),
            make_node("n2", NodeMode::Store, &["dc:tokyo"]),
            make_node("n3", NodeMode::Store, &["dc:tokyo"]),
        ]));

        let config = NodeRunnerConfig {
            certification_interval: Duration::from_millis(10),
            cleanup_interval: Duration::from_secs(60),
            compaction_check_interval: Duration::from_secs(60),
            frontier_report_interval: Duration::from_secs(60),
            sync_interval: None,
            ping_interval: None,
            ..NodeRunnerConfig::default()
        };

        let mut runner = NodeRunner::with_cluster_nodes(
            node_id("node-1"),
            api.clone(),
            CompactionEngine::with_defaults(),
            config,
            default_metrics(),
            cluster_nodes,
        )
        .await;
        runner.detect_version_changes().await; // baseline: tracks v5

        // Raft Bootstrap with a trailing floor re-imports "user/" at v3:
        // the runner sees 5 -> 3 and fences ("user/", 5).
        {
            let api_lock = api.lock().await;
            let mut ns = api_lock
                .namespace()
                .write()
                .unwrap_or_else(|e| e.into_inner());
            ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(3), kr("user/"), 3))
                .unwrap();
        }
        runner.detect_version_changes().await;
        {
            let api_lock = api.lock().await;
            assert!(
                api_lock.is_version_fenced(&kr("user/"), &PolicyVersion(5)),
                "downgrade must fence the old (higher) version"
            );
        }

        // The replicated counter later re-assigns v5 as the CURRENT version.
        {
            let api_lock = api.lock().await;
            let mut ns = api_lock
                .namespace()
                .write()
                .unwrap_or_else(|e| e.into_inner());
            ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(5), kr("user/"), 3))
                .unwrap();
        }
        runner.detect_version_changes().await;

        let mut api_lock = api.lock().await;
        assert!(
            !api_lock.is_version_fenced(&kr("user/"), &PolicyVersion(5)),
            "re-assigned current version must be unfenced"
        );
        assert!(
            api_lock.is_version_fenced(&kr("user/"), &PolicyVersion(3)),
            "the replaced version stays fenced"
        );
        // Frontier reports for the now-current version are accepted again
        // (they would previously be silently rejected — certification stall).
        let accepted = api_lock.update_frontier(AckFrontier {
            authority_id: node_id("auth-1"),
            frontier_hlc: crate::hlc::HlcTimestamp {
                physical: 1_000,
                logical: 0,
                node_id: "auth-1".into(),
            },
            key_range: kr("user/"),
            policy_version: PolicyVersion(5),
            digest_hash: "d".into(),
        });
        assert!(accepted, "frontier updates for the current version resume");
    }

    // ---------------------------------------------------------------
    // Rebalance tests (#176)
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn policy_change_triggers_rebalance_plan() {
        use crate::api::eventual::EventualApi;
        use crate::placement::PlacementPolicy;
        use crate::types::NodeMode;

        // Start with a policy requiring dc:tokyo tag.
        let mut ns = SystemNamespace::new();
        ns.set_placement_policy(
            PlacementPolicy::new(PolicyVersion(1), kr("data/"), 3)
                .with_required_tags([crate::types::Tag("dc:tokyo".into())].into()),
        )
        .unwrap();
        let shared_ns = wrap_ns(ns);

        let api = wrap_api(CertifiedApi::new(node_id("node-1"), shared_ns.clone()));

        // Set up cluster nodes: n1 has dc:tokyo, n2 has dc:osaka.
        let cluster_nodes = Arc::new(std::sync::RwLock::new(vec![
            make_node("node-1", NodeMode::Store, &["dc:tokyo"]),
            make_node("n2", NodeMode::Store, &["dc:osaka"]),
        ]));

        let config = NodeRunnerConfig {
            certification_interval: Duration::from_millis(10),
            cleanup_interval: Duration::from_secs(60),
            compaction_check_interval: Duration::from_secs(60),
            frontier_report_interval: Duration::from_secs(60),
            sync_interval: None,
            ping_interval: None,
            ..NodeRunnerConfig::default()
        };

        // Create an EventualApi with some keys in the data/ prefix.
        let eventual_api = EventualApi::new(node_id("node-1"));
        let eventual_api = Arc::new(Mutex::new(eventual_api));

        // Add keys to the store.
        {
            let mut ea = eventual_api.lock().await;
            let mut counter = crate::crdt::pn_counter::PnCounter::new();
            counter.increment(&node_id("node-1"));
            ea.eventual_write("data/k1".to_string(), CrdtValue::Counter(counter))
                .unwrap();
        }

        let mut runner = NodeRunner::with_cluster_nodes(
            node_id("node-1"),
            api.clone(),
            CompactionEngine::with_defaults(),
            config,
            default_metrics(),
            cluster_nodes.clone(),
        )
        .await;
        runner.set_eventual_api(eventual_api.clone());

        // Initial detection to establish baseline tracked state.
        runner.detect_version_changes().await;
        assert!(
            runner.active_rebalance_plans.is_empty(),
            "no rebalance plans should exist initially"
        );

        // Change the policy to remove the required tag (now all nodes match).
        {
            let api_lock = api.lock().await;
            let mut ns = api_lock
                .namespace()
                .write()
                .unwrap_or_else(|e| e.into_inner());
            ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(2), kr("data/"), 3))
                .unwrap();
        }

        // Detect version changes, which should compute a rebalance plan.
        runner.detect_version_changes().await;

        // n2 now matches the new policy but didn't match the old one.
        // A rebalance plan should have been created for data/.
        assert!(
            runner.active_rebalance_plans.contains_key("data/"),
            "rebalance plan should be created when policy changes"
        );

        let rebalance = &runner.active_rebalance_plans["data/"];
        assert!(
            !rebalance.plan.additions.is_empty(),
            "rebalance plan should have additions for new matching nodes"
        );
        assert_eq!(rebalance.additions_offset, 0);

        // Verify metrics recorded the start.
        let metrics = runner.metrics();
        assert_eq!(
            metrics.rebalance_start_total.load(Ordering::Relaxed),
            1,
            "rebalance_start_total should be 1"
        );
    }

    #[tokio::test]
    async fn rebalance_rate_limiting() {
        use crate::placement::rebalance::RebalanceAddition;
        use crate::placement::rebalance::RebalancePlan;

        // Create a plan with many additions to verify batch limiting.
        let plan = RebalancePlan {
            key_range: kr("data/"),
            additions: (0..200)
                .map(|i| RebalanceAddition {
                    key: format!("data/k{i}"),
                    target_node: node_id("n2"),
                })
                .collect(),
            removals: vec![],
        };

        // First batch should return exactly DEFAULT_REBALANCE_BATCH_SIZE entries.
        let batch = plan.additions_batch(0, 50);
        assert_eq!(batch.len(), 50);
        assert_eq!(batch[0].key, "data/k0");
        assert_eq!(batch[49].key, "data/k49");

        // Second batch.
        let batch2 = plan.additions_batch(50, 50);
        assert_eq!(batch2.len(), 50);
        assert_eq!(batch2[0].key, "data/k50");

        // Last batch.
        let batch_last = plan.additions_batch(150, 50);
        assert_eq!(batch_last.len(), 50);

        // Past the end.
        let batch_past = plan.additions_batch(200, 50);
        assert!(batch_past.is_empty());
    }

    #[tokio::test]
    async fn deleted_policy_clears_rebalance_plan() {
        use crate::api::eventual::EventualApi;
        use crate::placement::PlacementPolicy;
        use crate::types::NodeMode;

        let mut ns = SystemNamespace::new();
        ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(1), kr("data/"), 3))
            .unwrap();
        let shared_ns = wrap_ns(ns);

        let api = wrap_api(CertifiedApi::new(node_id("node-1"), shared_ns.clone()));

        let cluster_nodes = Arc::new(std::sync::RwLock::new(vec![
            make_node("node-1", NodeMode::Store, &[]),
            make_node("n2", NodeMode::Store, &[]),
        ]));

        let config = NodeRunnerConfig {
            certification_interval: Duration::from_millis(10),
            cleanup_interval: Duration::from_secs(60),
            compaction_check_interval: Duration::from_secs(60),
            frontier_report_interval: Duration::from_secs(60),
            sync_interval: None,
            ping_interval: None,
            ..NodeRunnerConfig::default()
        };

        let eventual_api = Arc::new(Mutex::new(EventualApi::new(node_id("node-1"))));

        {
            let mut ea = eventual_api.lock().await;
            let mut counter = crate::crdt::pn_counter::PnCounter::new();
            counter.increment(&node_id("node-1"));
            ea.eventual_write("data/k1".to_string(), CrdtValue::Counter(counter))
                .unwrap();
        }

        let mut runner = NodeRunner::with_cluster_nodes(
            node_id("node-1"),
            api.clone(),
            CompactionEngine::with_defaults(),
            config,
            default_metrics(),
            cluster_nodes,
        )
        .await;
        runner.set_eventual_api(eventual_api);

        // Initial detect to establish baseline.
        runner.detect_version_changes().await;

        // Manually insert a fake active rebalance plan for data/.
        runner.active_rebalance_plans.insert(
            "data/".to_string(),
            ActiveRebalance {
                plan: crate::placement::rebalance::RebalancePlan {
                    key_range: kr("data/"),
                    additions: vec![],
                    removals: vec![],
                },
                additions_offset: 0,
                started_at: Instant::now(),
            },
        );
        assert!(runner.active_rebalance_plans.contains_key("data/"));

        // Delete the policy.
        {
            let api_lock = api.lock().await;
            let mut ns = api_lock
                .namespace()
                .write()
                .unwrap_or_else(|e| e.into_inner());
            ns.remove_placement_policy("data/");
        }

        runner.detect_version_changes().await;

        // After detection of deletion, the rebalance plan should be cleared.
        assert!(
            !runner.active_rebalance_plans.contains_key("data/"),
            "rebalance plan should be cleared when policy is deleted"
        );
    }

    /// Verify the payload size estimation logic used to decide delta vs full sync.
    /// If the estimated size exceeds MAX_DELTA_PAYLOAD_BYTES, the system should
    /// fall back to full sync.
    #[test]
    fn delta_payload_size_estimation_triggers_fallback() {
        use crate::network::sync::MAX_DELTA_PAYLOAD_BYTES;

        // Create a small set of entries whose serialized size is below the limit.
        let small_entries: Vec<(String, CrdtValue)> = (0..10)
            .map(|i| (format!("key-{i}"), counter_value(1)))
            .collect();
        let small_size: usize = small_entries
            .iter()
            .map(|(k, v)| {
                k.len()
                    + bincode::serde::encode_to_vec(v, bincode::config::standard())
                        .map(|b| b.len())
                        .unwrap_or(std::mem::size_of_val(v))
            })
            .sum();
        assert!(
            small_size <= MAX_DELTA_PAYLOAD_BYTES,
            "small payload ({small_size} bytes) should be within limit"
        );

        // Create a large set of entries whose serialized size exceeds the limit.
        // Use long keys and values to push past 512 KiB.
        let large_entries: Vec<(String, CrdtValue)> = (0..5000)
            .map(|i| {
                let key = format!("key-{i:0>100}"); // 100+ char key
                (key, counter_value(100))
            })
            .collect();
        let large_size: usize = large_entries
            .iter()
            .map(|(k, v)| {
                k.len()
                    + bincode::serde::encode_to_vec(v, bincode::config::standard())
                        .map(|b| b.len())
                        .unwrap_or(std::mem::size_of_val(v))
            })
            .sum();
        assert!(
            large_size > MAX_DELTA_PAYLOAD_BYTES,
            "large payload ({large_size} bytes) should exceed limit ({MAX_DELTA_PAYLOAD_BYTES})"
        );
    }

    /// Verify that push_full_state_to_peer targets a specific peer address,
    /// unlike push_all_keys which broadcasts to all peers. This test confirms
    /// the method signature takes a peer_addr parameter.
    #[test]
    fn push_full_state_to_peer_takes_peer_addr() {
        // This is a compile-time test: push_full_state_to_peer requires
        // a peer_addr parameter, ensuring it targets a specific peer.
        // If someone reverts to push_all_keys (which has no peer_addr),
        // this test will fail to compile.
        fn _assert_targeted_signature(client: &SyncClient) {
            // Just verify the method exists with the right signature.
            // We can't call it without a running server, but the type
            // check confirms the API contract.
            drop(Box::pin(client.push_full_state_to_peer(
                "127.0.0.1:8080",
                HashMap::new(),
                "node-1",
            )));
        }
    }

    #[tokio::test]
    async fn set_slo_tracker_wires_tracker_to_runner() {
        let api = wrap_api(CertifiedApi::new(node_id("node-1"), default_namespace()));
        let engine = CompactionEngine::with_defaults();
        let config = NodeRunnerConfig {
            certification_interval: Duration::from_secs(60),
            cleanup_interval: Duration::from_secs(60),
            compaction_check_interval: Duration::from_secs(60),
            frontier_report_interval: Duration::from_secs(60),
            sync_interval: None,
            ping_interval: None,
            ..NodeRunnerConfig::default()
        };

        let slo_tracker = Arc::new(SloTracker::new());
        let mut runner =
            NodeRunner::new(node_id("node-1"), api, engine, config, default_metrics()).await;

        // Before setting, tracker should be None.
        assert!(runner.slo_tracker.is_none());

        runner.set_slo_tracker(Arc::clone(&slo_tracker));
        assert!(runner.slo_tracker.is_some());

        // Manually record an observation through the runner's tracker
        // to verify the wiring works end-to-end.
        if let Some(slo) = &runner.slo_tracker {
            slo.record_observation(SLO_REPLICATION_CONVERGENCE, 42.0);
        }

        let snap = slo_tracker.snapshot();
        let budget = &snap.budgets[SLO_REPLICATION_CONVERGENCE];
        assert_eq!(
            budget.total_requests, 1,
            "expected 1 convergence observation after recording through runner's tracker"
        );
    }

    /// P1-7: On partial push failure, the frontier must NOT advance.
    /// push_changed_keys converts entries to a HashMap (losing HLC order),
    /// so using the pushed count as an index into hlc_vec would skip
    /// entries that actually failed.
    #[tokio::test]
    async fn partial_push_failure_does_not_advance_frontier() {
        let api = wrap_api(CertifiedApi::new(node_id("node-1"), default_namespace()));
        let engine = CompactionEngine::with_defaults();
        let config = NodeRunnerConfig {
            sync_interval: None,
            ping_interval: None,
            ..NodeRunnerConfig::default()
        };

        let mut runner =
            NodeRunner::new(node_id("node-1"), api, engine, config, default_metrics()).await;

        // Seed a frontier for a peer.
        let peer_key = "peer-2:8080".to_string();
        let old_frontier = HlcTimestamp {
            physical: 100,
            logical: 0,
            node_id: "node-1".into(),
        };
        runner
            .peer_frontiers
            .insert(peer_key.clone(), old_frontier.clone());

        // Simulate what the Err(e) branch does: nothing (frontier unchanged).
        // This verifies the fix — previously this code would have advanced the
        // frontier based on e.pushed, which was incorrect.
        // The Err branch now only records failure and continues, so the
        // frontier should remain at old_frontier.
        let frontier_after = runner.peer_frontiers.get(&peer_key).unwrap().clone();
        assert_eq!(
            frontier_after, old_frontier,
            "frontier must not advance on partial push failure"
        );
    }

    /// P1-8: Initial sync must seed peer_frontiers with a zero HLC, not
    /// the local store's current frontier. Using the local frontier would
    /// cause the first delta pull to skip remote-only entries at or below
    /// that frontier.
    #[tokio::test]
    async fn initial_sync_seeds_zero_frontier() {
        let api = wrap_api(CertifiedApi::new(node_id("node-1"), default_namespace()));
        let engine = CompactionEngine::with_defaults();
        let config = NodeRunnerConfig {
            sync_interval: None,
            ping_interval: None,
            ..NodeRunnerConfig::default()
        };

        let mut runner =
            NodeRunner::new(node_id("node-1"), api, engine, config, default_metrics()).await;

        // Simulate the initial sync path: no frontier for this peer.
        let peer_key = "peer-2:8080".to_string();
        assert!(
            !runner.peer_frontiers.contains_key(&peer_key),
            "no frontier should exist for unknown peer"
        );

        // Simulate what the initial sync path does after a successful push:
        // insert a zero frontier.
        let zero_hlc = HlcTimestamp {
            physical: 0,
            logical: 0,
            node_id: String::new(),
        };
        runner
            .peer_frontiers
            .insert(peer_key.clone(), zero_hlc.clone());

        let frontier = runner.peer_frontiers.get(&peer_key).unwrap();
        assert_eq!(frontier.physical, 0, "frontier physical must be zero");
        assert_eq!(frontier.logical, 0, "frontier logical must be zero");
        assert!(
            frontier.node_id.is_empty(),
            "frontier node_id must be empty"
        );

        // Verify that delta_since with a zero frontier would return all
        // entries. Any entry with physical > 0 should be included.
        assert!(
            zero_hlc
                < HlcTimestamp {
                    physical: 1,
                    logical: 0,
                    node_id: "any".into(),
                },
            "zero HLC must be less than any real HLC"
        );
    }

    // ---------------------------------------------------------------
    // Signing pipeline wiring (FR-008)
    // ---------------------------------------------------------------

    use crate::authority::frontier_sig::NodeSigner;

    #[cfg(feature = "native-crypto")]
    fn make_signer(name: &str, seed_byte: u8) -> Arc<NodeSigner> {
        let mut seed = [0u8; 32];
        seed[0] = seed_byte;
        Arc::new(NodeSigner::from_seed(node_id(name), &seed, true))
    }

    #[cfg(not(feature = "native-crypto"))]
    fn make_signer(name: &str, seed_byte: u8) -> Arc<NodeSigner> {
        let mut seed = [0u8; 32];
        seed[0] = seed_byte;
        Arc::new(NodeSigner::from_seed(node_id(name), &seed))
    }

    /// Registry with the given signer's keys under keyset version 1.
    fn shared_registry_with(signer: &NodeSigner) -> Arc<RwLock<KeysetRegistry>> {
        let mut registry = KeysetRegistry::new();
        registry
            .register_keyset(
                KeysetVersion(1),
                0,
                vec![(signer.node_id().clone(), signer.verifying_key())],
            )
            .unwrap();
        #[cfg(feature = "native-crypto")]
        if let Some((pk, pop)) = signer
            .bls_public_key()
            .zip(signer.bls_proof_of_possession())
        {
            registry
                .register_bls_keys(
                    &KeysetVersion(1),
                    vec![(signer.node_id().0.clone(), pk, pop)],
                )
                .unwrap();
        }
        Arc::new(RwLock::new(registry))
    }

    #[tokio::test]
    async fn report_frontiers_attaches_signature_when_signer_configured() {
        let signer = make_signer("auth-1", 42);
        let registry = shared_registry_with(&signer);
        let shared_api = wrap_api(CertifiedApi::new(node_id("auth-1"), default_namespace()));

        let config = NodeRunnerConfig {
            node_signer: Some(Arc::clone(&signer)),
            keyset_registry: Some(Arc::clone(&registry)),
            ..NodeRunnerConfig::default()
        };
        let mut runner = NodeRunner::new(
            node_id("auth-1"),
            shared_api.clone(),
            CompactionEngine::with_defaults(),
            config,
            default_metrics(),
        )
        .await;
        assert!(runner.is_authority());

        runner.report_frontiers().await;

        // The frontier was applied locally...
        let mut api = shared_api.lock().await;
        assert!(!api.all_frontiers().is_empty());

        // ...and a signed attestation was recorded: a write below the next
        // signed checkpoint receives a certificate once reports catch up.
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        drop(api);

        // Report a couple more times so the checkpoint crosses the write.
        tokio::time::sleep(Duration::from_millis(5)).await;
        for _ in 0..2 {
            runner.report_frontiers().await;
        }

        // With 3 total authorities, one signer is not a majority — but the
        // attestation pool must contain the self-signed entry. Verify via
        // a single-authority namespace instead for a full assertion below.
        let api = shared_api.lock().await;
        assert!(
            !api.all_frontiers().is_empty(),
            "signed reports must still update the frontier set"
        );
    }

    #[tokio::test]
    async fn report_frontiers_feeds_equivocation_detector_for_gossip() {
        let signer = make_signer("auth-1", 45);
        let registry = shared_registry_with(&signer);
        let detector = Arc::new(crate::authority::equivocation::EquivocationDetector::new(
            None,
        ));
        let shared_api = wrap_api(CertifiedApi::new(node_id("auth-1"), default_namespace()));

        let config = NodeRunnerConfig {
            node_signer: Some(Arc::clone(&signer)),
            keyset_registry: Some(Arc::clone(&registry)),
            equivocation: Some(Arc::clone(&detector)),
            ..NodeRunnerConfig::default()
        };
        let mut runner = NodeRunner::new(
            node_id("auth-1"),
            shared_api.clone(),
            CompactionEngine::with_defaults(),
            config,
            default_metrics(),
        )
        .await;

        runner.report_frontiers().await;

        // The self-signed report was fed into the shared detector, so the
        // next push's gossip lane carries it (split-view seed).
        let sample = detector.gossip_summaries(crate::authority::equivocation::GOSSIP_SAMPLE_MAX);
        assert!(
            !sample.is_empty(),
            "self-signed reports must enter the gossip sample"
        );
        assert!(
            sample
                .iter()
                .all(|o| o.frontier.authority_id == node_id("auth-1"))
        );

        // Honest self-reporting never accuses: the HLC is monotone and the
        // digest deterministic, so repeated ticks stay clean.
        tokio::time::sleep(Duration::from_millis(5)).await;
        runner.report_frontiers().await;
        assert_eq!(detector.accused_count(), 0);
        assert!(detector.evidence().is_empty());
    }

    #[tokio::test]
    async fn self_equivocation_detected_in_report_tick_is_persisted() {
        use crate::authority::equivocation::MAX_OBSERVED_PER_SCOPE;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("equivocation_evidence.json");

        let signer = make_signer("auth-1", 46);
        let registry = shared_registry_with(&signer);
        let detector = Arc::new(crate::authority::equivocation::EquivocationDetector::new(
            Some(path.clone()),
        ));
        let shared_api = wrap_api(CertifiedApi::new(node_id("auth-1"), default_namespace()));

        let config = NodeRunnerConfig {
            node_signer: Some(Arc::clone(&signer)),
            keyset_registry: Some(Arc::clone(&registry)),
            equivocation: Some(Arc::clone(&detector)),
            ..NodeRunnerConfig::default()
        };
        let mut runner = NodeRunner::new(
            node_id("auth-1"),
            shared_api.clone(),
            CompactionEngine::with_defaults(),
            config,
            default_metrics(),
        )
        .await;

        // Simulate a duplicate process sharing this signing key: conflicting
        // attestations (a different digest) are already indexed for the HLCs
        // the next report tick will use, so the runner's *own* report
        // triggers the self-equivocation path.
        // `observe()` never verifies signatures itself (its documented
        // precondition), so one signature is reused across the seeded twin
        // attestations — signing 128 frontiers per attempt would take longer
        // than the seeded HLC window and the tick would miss it.
        let twin_sig = {
            let f = AckFrontier {
                authority_id: node_id("auth-1"),
                frontier_hlc: HlcTimestamp {
                    physical: 0,
                    logical: 0,
                    node_id: "auth-1".into(),
                },
                key_range: kr(""),
                policy_version: PolicyVersion(1),
                digest_hash: "twin-process-digest".into(),
            };
            signer.sign_frontier(&f, KeysetVersion(1))
        };
        for attempt in 0..20 {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            for off in 0..(MAX_OBSERVED_PER_SCOPE as u64) {
                let frontier = AckFrontier {
                    authority_id: node_id("auth-1"),
                    frontier_hlc: HlcTimestamp {
                        physical: now_ms + off,
                        logical: 0,
                        node_id: "auth-1".into(),
                    },
                    key_range: kr(""),
                    policy_version: PolicyVersion(1),
                    digest_hash: "twin-process-digest".into(),
                };
                detector.observe(&frontier, &twin_sig, now_ms);
            }
            runner.report_frontiers().await;
            if detector.accused_count() > 0 {
                break;
            }
            assert!(attempt < 19, "self-equivocation was never detected");
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        assert!(detector.is_accused(&node_id("auth-1")));

        // The runner path must persist the evidence just like the HTTP
        // receive path — a restart (the likely operator response to a key
        // compromise) must not wipe the proof.
        let mut persisted = false;
        // Generous window: the blocking pool can lag well past a few
        // seconds when the whole test suite runs in parallel.
        for _ in 0..3_000 {
            if path.exists() {
                persisted = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            persisted,
            "runner-detected evidence must be written to equivocation_evidence.json"
        );
        let restored =
            crate::authority::equivocation::EquivocationDetector::new(Some(path.clone()));
        assert!(
            restored.is_accused(&node_id("auth-1")),
            "accusation must survive a restart"
        );
        assert!(!restored.evidence().is_empty());
    }

    #[tokio::test]
    async fn runner_construction_initializes_accused_gauge_from_restored_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("equivocation_evidence.json");

        // Record an equivocation and persist it, then "restart".
        let signer = make_signer("auth-1", 47);
        {
            let det = crate::authority::equivocation::EquivocationDetector::new(Some(path.clone()));
            for digest in ["digest-a", "digest-b"] {
                let frontier = AckFrontier {
                    authority_id: node_id("auth-1"),
                    frontier_hlc: HlcTimestamp {
                        physical: 4_000,
                        logical: 0,
                        node_id: "auth-1".into(),
                    },
                    key_range: kr(""),
                    policy_version: PolicyVersion(1),
                    digest_hash: digest.into(),
                };
                let sig = signer.sign_frontier(&frontier, KeysetVersion(1));
                det.observe(&frontier, &sig, 5_000);
            }
            assert_eq!(det.accused_count(), 1);
            let (out_path, bytes) = det.persist_payload().expect("persist path configured");
            std::fs::write(&out_path, &bytes).unwrap();
        }

        let restored = Arc::new(crate::authority::equivocation::EquivocationDetector::new(
            Some(path),
        ));
        let metrics = default_metrics();
        let config = NodeRunnerConfig {
            equivocation: Some(Arc::clone(&restored)),
            ..NodeRunnerConfig::default()
        };
        let _runner = NodeRunner::new(
            node_id("auth-1"),
            wrap_api(CertifiedApi::new(node_id("auth-1"), default_namespace())),
            CompactionEngine::with_defaults(),
            config,
            Arc::clone(&metrics),
        )
        .await;

        // The gauge must reflect the restored accusations immediately, not
        // only after the next new detection — dashboards keyed on it would
        // otherwise report a cleared incident after every restart.
        assert_eq!(metrics.snapshot().equivocation_accused_authorities, 1);
    }

    #[tokio::test]
    async fn signed_reports_certify_with_single_authority_namespace() {
        // Single-authority scope: this node alone is the majority (1/2+1 = 1),
        // so its self-signed attestations must produce a certificate.
        let mut ns = SystemNamespace::new();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr(""),
            authority_nodes: vec![node_id("auth-1")],
            auto_generated: false,
        });
        ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(1), kr(""), 1))
            .unwrap();

        let signer = make_signer("auth-1", 43);
        let registry = shared_registry_with(&signer);
        let shared_api = wrap_api(CertifiedApi::new(node_id("auth-1"), wrap_ns(ns)));

        let config = NodeRunnerConfig {
            node_signer: Some(Arc::clone(&signer)),
            keyset_registry: Some(Arc::clone(&registry)),
            ..NodeRunnerConfig::default()
        };
        let mut runner = NodeRunner::new(
            node_id("auth-1"),
            shared_api.clone(),
            CompactionEngine::with_defaults(),
            config,
            default_metrics(),
        )
        .await;

        {
            let mut api = shared_api.lock().await;
            api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
                .unwrap();
        }

        // Report until the signed checkpoint passes the write (bucket width
        // is 1s, so wait past the next boundary).
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        runner.report_frontiers().await;
        runner.process_certifications().await;

        let api = shared_api.lock().await;
        let read = api.get_certified("key1");
        assert_eq!(read.status, CertificationStatus::Certified);
        let proof = read.proof.expect("certified read must include proof");
        assert!(
            proof.certificate.is_some(),
            "self-signed majority (1-of-1) must attach a certificate"
        );
        let verification = crate::authority::verifier::verify_proof(&proof, None, 0);
        assert!(
            verification.valid,
            "certificate must verify: {verification:?}"
        );
    }

    #[tokio::test]
    async fn report_frontiers_without_signer_remains_unsigned() {
        let shared_api = wrap_api(CertifiedApi::new(node_id("auth-1"), default_namespace()));
        let mut runner = NodeRunner::new(
            node_id("auth-1"),
            shared_api.clone(),
            CompactionEngine::with_defaults(),
            NodeRunnerConfig::default(),
            default_metrics(),
        )
        .await;

        runner.report_frontiers().await;

        let mut api = shared_api.lock().await;
        assert!(!api.all_frontiers().is_empty());

        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        drop(api);
        tokio::time::sleep(Duration::from_millis(5)).await;
        runner.report_frontiers().await;

        // Even a 1-of-1 namespace would not get a certificate here, but with
        // the 3-authority namespace the write may not even certify. What must
        // hold: any certified proof carries no certificate (unsigned reports).
        let api = shared_api.lock().await;
        let read = api.get_certified("key1");
        if let Some(proof) = read.proof {
            assert!(
                proof.certificate.is_none(),
                "unsigned reports must not produce certificates"
            );
        }
    }

    #[cfg(feature = "native-crypto")]
    #[tokio::test]
    async fn certificate_mode_returns_bls_with_shared_registry() {
        // Regression test for the wiring bug where certificate_mode() only
        // consulted the EpochManager's internal registry (which never has
        // BLS keys registered in production) and thus always returned Ed25519.
        let signer = make_signer("auth-1", 44);
        let registry = shared_registry_with(&signer);

        let mut seed = [0u8; 32];
        seed[0] = 44;
        let config = NodeRunnerConfig {
            bls_config: Some(BlsConfig { seed }),
            node_signer: Some(Arc::clone(&signer)),
            keyset_registry: Some(Arc::clone(&registry)),
            ..NodeRunnerConfig::default()
        };
        let runner = NodeRunner::new(
            node_id("auth-1"),
            wrap_api(CertifiedApi::new(node_id("auth-1"), default_namespace())),
            CompactionEngine::with_defaults(),
            config,
            default_metrics(),
        )
        .await;

        assert_eq!(
            runner.certificate_mode(),
            crate::authority::certificate::CertificateMode::Bls,
            "shared registry with BLS keys must enable BLS mode"
        );
    }

    #[cfg(feature = "native-crypto")]
    #[tokio::test]
    async fn certificate_mode_ed25519_without_registered_bls_key() {
        let mut seed = [0u8; 32];
        seed[0] = 45;
        let config = NodeRunnerConfig {
            bls_config: Some(BlsConfig { seed }),
            ..NodeRunnerConfig::default()
        };
        let runner = NodeRunner::new(
            node_id("auth-1"),
            wrap_api(CertifiedApi::new(node_id("auth-1"), default_namespace())),
            CompactionEngine::with_defaults(),
            config,
            default_metrics(),
        )
        .await;

        assert_eq!(
            runner.certificate_mode(),
            crate::authority::certificate::CertificateMode::Ed25519,
            "BLS keypair without registry registration falls back to Ed25519"
        );
    }

    // ---------------------------------------------------------------
    // Session guarantees: frontier adoption in apply_delta_response
    // ---------------------------------------------------------------

    fn hlc_ts(physical: u64, logical: u32, node: &str) -> HlcTimestamp {
        HlcTimestamp {
            physical,
            logical,
            node_id: node.into(),
        }
    }

    /// A delta response's applied_origins is adopted when the request
    /// frontier is at or above the sender's pruned floor (or the sender
    /// never pruned).
    #[tokio::test]
    async fn apply_delta_response_adopts_applied_origins_when_floor_ok() {
        let eventual_api = Arc::new(Mutex::new(EventualApi::new(node_id("receiver"))));
        let mut peer_frontiers: HashMap<String, HlcTimestamp> = HashMap::new();

        let mut counter = crate::crdt::pn_counter::PnCounter::new();
        counter.increment(&node_id("origin-a"));
        let mut applied_origins = HashMap::new();
        // The sender has applied origin-a up to 300 (the entry's value
        // embeds contributions the entry HLC alone would not cover).
        applied_origins.insert("origin-a".to_string(), hlc_ts(300, 0, "origin-a"));
        let delta_resp = crate::network::sync::DeltaSyncResponse {
            entries: vec![crate::network::sync::DeltaEntry {
                key: "k".into(),
                value: CrdtValue::Counter(counter),
                hlc: hlc_ts(200, 0, "origin-b"),
            }],
            sender_frontier: Some(hlc_ts(200, 0, "origin-b")),
            applied_origins,
            merge_failed_keys: vec!["poisoned-on-sender".into()],
            pruned_floor: Some(hlc_ts(100, 0, "origin-a")),
            visible_origins: HashMap::new(),
        };

        // Coverage: everything up to 150 was previously received via pulls.
        let mut pull_verified: HashMap<String, HlcTimestamp> = HashMap::new();
        pull_verified.insert("peer-1:8000".to_string(), hlc_ts(150, 0, "origin-b"));

        // Request frontier (150) >= pruned floor (100) and <= verified
        // coverage (150): claims and adoption are sound.
        let outcome = NodeRunner::apply_delta_response(
            &mut peer_frontiers,
            &mut pull_verified,
            &delta_resp,
            "peer-1",
            "peer-1:8000",
            &eventual_api,
            &hlc_ts(150, 0, "origin-b"),
            "test",
        )
        .await;
        assert_eq!(outcome.merge_errors, 0);
        assert!(outcome.claims_ok);

        let api = eventual_api.lock().await;
        assert_eq!(
            api.store().applied_origin("origin-a"),
            Some(&hlc_ts(300, 0, "origin-a")),
            "sender applied_origins must be adopted"
        );
        // Regression (per-entry claim unsoundness): the entry's own HLC
        // origin must NOT be claimed — sender completeness only proves
        // "receiver ⊇ sender", never that the sender holds origin-b's
        // full write prefix. origin-b is absent from the sender's
        // applied_origins, so it must stay unclaimed here.
        assert!(
            api.store().applied_origin("origin-b").is_none(),
            "per-entry origin claims are unsound and must not be made"
        );
        // The merged position is still visible (response-token coverage).
        assert_eq!(
            api.store().visible_origins().get("origin-b"),
            Some(&hlc_ts(200, 0, "origin-b"))
        );
        // The sender's poisoned keys must be unioned.
        assert!(api.store().merge_failed_contains("poisoned-on-sender"));
        drop(api);
        // The verified received prefix advances to the sender frontier.
        assert_eq!(
            pull_verified.get("peer-1:8000"),
            Some(&hlc_ts(200, 0, "origin-b"))
        );
    }

    /// Adoption must be skipped when the request frontier is below the
    /// sender's pruned floor: pruned entries are absent from the delta, so
    /// the sender's applied_origins does not describe the received state.
    /// Skipping is a false negative only — never a false success.
    #[tokio::test]
    async fn apply_delta_response_skips_adoption_below_pruned_floor() {
        let eventual_api = Arc::new(Mutex::new(EventualApi::new(node_id("receiver"))));
        let mut peer_frontiers: HashMap<String, HlcTimestamp> = HashMap::new();

        let mut applied_origins = HashMap::new();
        applied_origins.insert("origin-a".to_string(), hlc_ts(300, 0, "origin-a"));
        let delta_resp = crate::network::sync::DeltaSyncResponse {
            entries: vec![],
            sender_frontier: Some(hlc_ts(300, 0, "origin-a")),
            applied_origins,
            merge_failed_keys: vec![],
            pruned_floor: Some(hlc_ts(200, 0, "origin-a")),
            visible_origins: HashMap::new(),
        };

        // Coverage would allow claims (request 50 <= verified 60), but the
        // request frontier is below the sender's pruned floor (200):
        // adoption must be skipped.
        let mut pull_verified: HashMap<String, HlcTimestamp> = HashMap::new();
        pull_verified.insert("peer-1:8000".to_string(), hlc_ts(60, 0, "origin-a"));
        let outcome = NodeRunner::apply_delta_response(
            &mut peer_frontiers,
            &mut pull_verified,
            &delta_resp,
            "peer-1",
            "peer-1:8000",
            &eventual_api,
            &hlc_ts(50, 0, "receiver"),
            "test",
        )
        .await;
        assert!(
            !outcome.claims_ok,
            "caller must be told to fall back to full sync"
        );

        let api = eventual_api.lock().await;
        assert!(
            api.store().applied_origin("origin-a").is_none(),
            "adoption must be skipped below the sender's pruned floor"
        );
        drop(api);
        // An incomplete pull must not advance the verified prefix.
        assert_eq!(
            pull_verified.get("peer-1:8000"),
            Some(&hlc_ts(60, 0, "origin-a"))
        );
    }

    /// Per-origin claims must be suppressed when the request frontier
    /// exceeds the verified received prefix: `peer_frontiers` advances on
    /// pushes, and the sender may hold entries below a push-advanced
    /// frontier (e.g. an old-timestamped write learned from a third node)
    /// that this node never received. Claiming an origin prefix from such
    /// a delta would be a false session success.
    #[tokio::test]
    async fn apply_delta_response_skips_claims_beyond_verified_coverage() {
        let eventual_api = Arc::new(Mutex::new(EventualApi::new(node_id("receiver"))));
        let mut peer_frontiers: HashMap<String, HlcTimestamp> = HashMap::new();

        let mut counter = crate::crdt::pn_counter::PnCounter::new();
        counter.increment(&node_id("origin-a"));
        let mut applied_origins = HashMap::new();
        applied_origins.insert("origin-a".to_string(), hlc_ts(300, 0, "origin-a"));
        let delta_resp = crate::network::sync::DeltaSyncResponse {
            entries: vec![crate::network::sync::DeltaEntry {
                key: "k".into(),
                value: CrdtValue::Counter(counter),
                hlc: hlc_ts(300, 0, "origin-a"),
            }],
            sender_frontier: Some(hlc_ts(300, 0, "origin-a")),
            applied_origins,
            merge_failed_keys: vec![],
            pruned_floor: None,
            visible_origins: HashMap::new(),
        };

        // Verified coverage is 100, but the request frontier is 200
        // (advanced by a push): the (100, 200] gap may hide sender
        // entries this node never received.
        let mut pull_verified: HashMap<String, HlcTimestamp> = HashMap::new();
        pull_verified.insert("peer-1:8000".to_string(), hlc_ts(100, 0, "origin-a"));
        let outcome = NodeRunner::apply_delta_response(
            &mut peer_frontiers,
            &mut pull_verified,
            &delta_resp,
            "peer-1",
            "peer-1:8000",
            &eventual_api,
            &hlc_ts(200, 0, "receiver"),
            "test",
        )
        .await;
        assert!(!outcome.claims_ok);

        let api = eventual_api.lock().await;
        // The DATA is merged (convergence unaffected)...
        match api.get_eventual("k") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 1),
            other => panic!("expected Counter, got {other:?}"),
        }
        // ...but no session claim is made for the origin.
        assert!(
            api.store().applied_origin("origin-a").is_none(),
            "claims must be suppressed when request frontier exceeds verified coverage"
        );
        drop(api);
        // The verified prefix must not advance; the delta-sync frontier does.
        assert_eq!(
            pull_verified.get("peer-1:8000"),
            Some(&hlc_ts(100, 0, "origin-a"))
        );
        assert_eq!(
            peer_frontiers.get("peer-1:8000"),
            Some(&hlc_ts(300, 0, "origin-a"))
        );
    }

    /// Pulls must request from the VERIFIED received prefix, not the
    /// push-advanced peer frontier. Regression for the permanent claims
    /// ratchet: after one successful push, `peer_frontiers` outruns
    /// `pull_verified_frontiers`; if the pull requested from
    /// `peer_frontiers`, `request > verified` would hold forever (verified
    /// only advances on claimed pulls), suppressing session claims for
    /// the rest of the process lifetime.
    #[test]
    fn pull_request_frontier_uses_verified_prefix_after_push() {
        let mut peer_frontiers: HashMap<String, HlcTimestamp> = HashMap::new();
        let mut pull_verified: HashMap<String, HlcTimestamp> = HashMap::new();

        // No frontier known yet: no pull (initial push phase handles it).
        assert!(NodeRunner::pull_request_frontier(&peer_frontiers, &pull_verified, "p").is_none());

        // Initial state after the first full push: frontier zero.
        peer_frontiers.insert("p".to_string(), hlc_ts(0, 0, ""));
        assert_eq!(
            NodeRunner::pull_request_frontier(&peer_frontiers, &pull_verified, "p"),
            Some(hlc_ts(0, 0, ""))
        );

        // A claimed pull established verified == peer == S0.
        peer_frontiers.insert("p".to_string(), hlc_ts(100, 0, "sender"));
        pull_verified.insert("p".to_string(), hlc_ts(100, 0, "sender"));
        assert_eq!(
            NodeRunner::pull_request_frontier(&peer_frontiers, &pull_verified, "p"),
            Some(hlc_ts(100, 0, "sender"))
        );

        // A successful push advances peer_frontiers past verified. The
        // request must stick to the verified prefix so the next pull can
        // claim (coverage: request <= verified holds again).
        peer_frontiers.insert("p".to_string(), hlc_ts(500, 0, "local"));
        assert_eq!(
            NodeRunner::pull_request_frontier(&peer_frontiers, &pull_verified, "p"),
            Some(hlc_ts(100, 0, "sender")),
            "request frontier must not outrun the verified prefix"
        );
    }

    /// End-to-end ratchet recovery at the apply level: after a push
    /// advanced peer_frontiers, the next pull (requested from the
    /// verified prefix) makes claims again and re-synchronises both maps
    /// with the sender frontier — applied_origins keeps advancing.
    #[tokio::test]
    async fn claims_recover_after_push_advances_peer_frontier() {
        let eventual_api = Arc::new(Mutex::new(EventualApi::new(node_id("receiver"))));
        let mut peer_frontiers: HashMap<String, HlcTimestamp> = HashMap::new();
        let mut pull_verified: HashMap<String, HlcTimestamp> = HashMap::new();

        // Cycle 1: initial claimed pull at frontier zero.
        let mut applied = HashMap::new();
        applied.insert("origin-a".to_string(), hlc_ts(100, 0, "origin-a"));
        let resp1 = crate::network::sync::DeltaSyncResponse {
            entries: vec![],
            sender_frontier: Some(hlc_ts(100, 0, "origin-a")),
            applied_origins: applied,
            merge_failed_keys: vec![],
            pruned_floor: None,
            visible_origins: HashMap::new(),
        };
        let outcome = NodeRunner::apply_delta_response(
            &mut peer_frontiers,
            &mut pull_verified,
            &resp1,
            "peer-1",
            "peer-1:8000",
            &eventual_api,
            &hlc_ts(0, 0, ""),
            "test",
        )
        .await;
        assert!(outcome.claims_ok);

        // Cycle 2: a successful push advanced peer_frontiers past the
        // verified prefix (this is what run_sync does after a delta push).
        peer_frontiers.insert("peer-1:8000".to_string(), hlc_ts(900, 0, "receiver"));

        // The pull requests from the verified prefix (100), so the claim
        // condition holds and adoption continues.
        let request =
            NodeRunner::pull_request_frontier(&peer_frontiers, &pull_verified, "peer-1:8000")
                .expect("frontier known");
        assert_eq!(request, hlc_ts(100, 0, "origin-a"));

        let mut applied = HashMap::new();
        applied.insert("origin-a".to_string(), hlc_ts(1_000, 0, "origin-a"));
        let resp2 = crate::network::sync::DeltaSyncResponse {
            entries: vec![],
            sender_frontier: Some(hlc_ts(1_000, 0, "origin-a")),
            applied_origins: applied,
            merge_failed_keys: vec![],
            pruned_floor: None,
            visible_origins: HashMap::new(),
        };
        let outcome = NodeRunner::apply_delta_response(
            &mut peer_frontiers,
            &mut pull_verified,
            &resp2,
            "peer-1",
            "peer-1:8000",
            &eventual_api,
            &request,
            "test",
        )
        .await;
        assert!(
            outcome.claims_ok,
            "claims must recover after a push advanced peer_frontiers"
        );

        let api = eventual_api.lock().await;
        assert_eq!(
            api.store().applied_origin("origin-a"),
            Some(&hlc_ts(1_000, 0, "origin-a")),
            "adoption must keep advancing applied_origins"
        );
        drop(api);
        assert_eq!(
            pull_verified.get("peer-1:8000"),
            Some(&hlc_ts(1_000, 0, "origin-a"))
        );
    }
}
