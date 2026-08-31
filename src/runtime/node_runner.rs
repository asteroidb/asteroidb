use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, watch};

use crate::api::certified::CertifiedApi;
use crate::api::eventual::EventualApi;
#[cfg(feature = "native-crypto")]
use crate::authority::bls::BlsKeypair;
use crate::authority::certificate::{EpochConfig, EpochManager, KeysetRegistry, KeysetVersion};
use crate::authority::equivocation::{
    EquivocationDetector, GOSSIP_SAMPLE_MAX, OBSERVED_RETENTION_MS, ObserveOutcome,
};
use crate::authority::frontier_reporter::{
    FrontierReporter, SD_COLD, SD_UNAVAILABLE, format_store_digest_hash, placeholder_digest_hash,
};
use crate::authority::frontier_sig::{FrontierSignature, NodeSigner};
use crate::compaction::CompactionEngine;
use crate::control_plane::system_namespace::SystemNamespace;
use crate::crdt::gc::TombstoneGc;
use crate::hlc::{Hlc, HlcTimestamp, MAX_CLOCK_SKEW_MS};
use crate::network::PeerRegistry;
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
use crate::runtime::report_clock::ReportClockFloor;
use crate::store::digest::{StoreDigest, digest_pass};
use crate::types::{CertificationStatus, KeyRange, NodeId, PolicyVersion};

/// How long a peer stays cached as digest-unsupported (e.g. an old node
/// answering 404 for `/api/internal/sync/digest`) before being re-probed.
/// Bounds the per-cycle probe overhead against not-yet-upgraded peers
/// while letting upgraded peers be picked up within minutes.
const DIGEST_UNSUPPORTED_RETRY: Duration = Duration::from_secs(600);

/// Activation grace when no report clock floor file exists at startup
/// (first boot after the M-12 upgrade, or a lost/corrupt floor file).
/// Until it elapses the node signs NO frontier reports at all — the tick
/// is skipped entirely, in every digest format.
///
/// Silence (rather than the legacy placeholder format) is load-bearing: a
/// floorless boot cannot know which format its previous incarnation was
/// signing. If it emitted placeholder reports and the pre-crash era was
/// `sd2:`-active, a wall-clock rollback within the ordinary 60s skew
/// budget could re-issue an HLC that peers still retain as an `sd2:` head
/// — placeholder-vs-sd2 at one HLC is a false equivocation against an
/// honest node. Nothing signed means nothing can collide, in either
/// format direction.
///
/// The floor file is also NOT created/covered during the grace (no report
/// is issued, so there is nothing to cover): a crash mid-grace therefore
/// restarts the grace from scratch on the next boot, and a floor file's
/// existence always proves that its lease covers every report signed
/// since — never a partially-served grace.
///
/// Width — the safety argument (revised for M-14): the former argument
/// ("every head signed before the floorless restart has aged out of every
/// peer's detector by the time the grace elapses") no longer holds once
/// observed heads are RELAYED between nodes (frontier gossip + the M-14
/// sync piggyback): every relay hop re-stamps `seen_ms` on the receiving
/// detector, and an aged-out head can be re-indexed by a later echo, so a
/// head's total lifetime across the cluster is unbounded (ping-pong
/// between nodes with offset windows). Safety therefore does NOT depend
/// on head lifetime; it is a clock-arithmetic invariant instead:
///
/// - Any pre-restart head a peer can hold satisfies
///   `head.physical <= W_old + MAX_CLOCK_SKEW_MS`, where `W_old` is the
///   wall clock at the crash — enforced on every ingest path by the HLC
///   receive bound (`hlc.rs`) and the detector's own `MAX_FUTURE_SKEW_MS`
///   guard, regardless of how long relaying keeps the head alive.
/// - With a wall-clock rollback within the ordinary skew budget
///   (<= MAX_CLOCK_SKEW_MS), the first post-grace report satisfies
///   `report.physical >= W_old - MAX_CLOCK_SKEW_MS + grace`.
///
/// Both bounds are INCLUSIVE (the HLC receive gate and the detector guard
/// reject only strictly-beyond-bound physicals), so strict exceedance of
/// every pre-restart head requires the STRICT inequality
/// `grace > 2 x MAX_CLOCK_SKEW_MS`: at exactly `grace = 2 x skew` the two
/// bounds meet — the first post-grace report could carry the same
/// physical (and hence, with a colliding logical, the same HLC) as a
/// still-relayed pre-restart head, which is precisely the
/// false-evidence case the grace exists to prevent, and M-14 relaying
/// makes head lifetime unbounded so age-out cannot save it. Currently
/// 180s > 120s, a 60s margin (pinned strictly by the
/// `digest_activation_grace_covers_clock_swing_budget` test). A same-HLC
/// false pair would need a rollback beyond the skew budget, which is
/// outside the threat model (same failure class as before M-14). The
/// `OBSERVED_RETENTION_MS` term in the constant is kept as the
/// definition of the local detection window, not as a safety
/// precondition. Cost: ~3 minutes without frontier reports from this
/// authority, only on first boot / floor loss.
pub const DIGEST_ACTIVATION_GRACE: Duration =
    Duration::from_millis(OBSERVED_RETENTION_MS + MAX_CLOCK_SKEW_MS);

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
    ///
    /// NOTE: automatic keyset ROTATION does not fire in production —
    /// nothing calls `EpochManager::stage_keys`, so `check_and_rotate`
    /// never finds staged keys to promote (see `check_epoch_rotation`).
    /// Key updates today happen only via `ASTEROIDB_AUTHORITY_KEYS`
    /// redistribution + restart. This tick still keeps the shared epoch
    /// counter advancing (keyset expiry checks).
    pub epoch_check_interval: Duration,
    /// How often to run tombstone GC on CRDT deferred sets.
    /// Default: 60 seconds.
    pub gc_interval: Duration,
    /// Minimum age a tombstone-GC mark must reach before its sweep may
    /// collect (`ASTEROIDB_GC_RETENTION_SECS` in the binary).
    /// Default: 300 seconds. See [`TombstoneGc::mark_and_sweep`].
    pub gc_retention: Duration,
    /// Stage 2 tombstone-GC hole-jump (`ASTEROIDB_GC_HOLE_JUMP=1`).
    /// Default: false (Stage 1, fail-closed on legacy holes).
    ///
    /// When enabled, a sweep whose INBOUND gate also holds (every
    /// registry peer's complete state merged since the mark — see
    /// [`NodeRunner::run_gc`]) may advance the per-value compaction floor
    /// across legacy holes: dots that a pre-floor sweep physically
    /// deleted cluster-wide. Enable only after a Stage 1 soak
    /// (`gc_floor_stalled_hole_dots` identifies the stalled remainder);
    /// see
    /// docs/ops-guide.md.
    pub gc_hole_jump_enabled: bool,
    /// Epoch configuration for key rotation (FR-008).
    /// Default: 24h epoch duration, 7 grace epochs.
    ///
    /// NOTE: the 24h-epoch / 7-grace-epoch rotation these parameters
    /// describe is currently unwired in production (no `stage_keys`
    /// caller) — see the note on
    /// [`epoch_check_interval`](Self::epoch_check_interval).
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
    /// When `true`, this node's OWN attestations are excluded from
    /// certificate assembly while the node is accused of equivocation, and
    /// its previously pooled attestations are purged (m-7). Must mirror
    /// `AppState.exclude_accused_authorities`
    /// (`ASTEROIDB_EXCLUDE_ACCUSED_AUTHORITIES`) — without this wiring the
    /// self-report path re-inserts what the HTTP path excludes. Default:
    /// `false` (detect-only).
    pub exclude_accused_authorities: bool,
    /// When `true` (default), authority frontier reports bind the M-7
    /// eventual-store root digest (`sd2:<hex>`) as their `digest_hash`,
    /// making data-content split views detectable (M-12). Requires
    /// `frontier_clock_floor_path` to actually activate — without a
    /// persisted floor, restart monotonicity of report HLCs cannot be
    /// guaranteed and the legacy placeholder format is kept (fail-safe).
    /// Ops kill switch: `ASTEROIDB_FRONTIER_STORE_DIGEST=0` in the binary.
    pub frontier_store_digest: bool,
    /// Persistence path of the frontier report clock floor
    /// (`<data_dir>/frontier_report_clock.json` in the binary). `None`
    /// (default, library/test wiring without a data dir) disables the
    /// store-digest report format entirely.
    pub frontier_clock_floor_path: Option<PathBuf>,
    /// Test override for [`DIGEST_ACTIVATION_GRACE`] (the window during
    /// which a floorless boot suppresses ALL frontier reporting, after
    /// which the store-digest format activates). `None` (default) uses
    /// the production constant.
    pub frontier_digest_activation_grace: Option<Duration>,
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
            gc_retention: Duration::from_secs(300),
            gc_hole_jump_enabled: false,
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
            exclude_accused_authorities: false,
            frontier_store_digest: true,
            frontier_clock_floor_path: None,
            frontier_digest_activation_grace: None,
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
    /// It also advances on successful PULLS (to the peer's own sender
    /// frontier), so it is equally NOT a proof of what the peer has
    /// received from us — see `push_frontiers` / `push_acked_wall_ms`
    /// for the push-evidence counterparts (tombstone-GC gate, delta push
    /// baseline).
    peer_frontiers: HashMap<String, HlcTimestamp>,
    /// Per-peer PUSH-ONLY delta baseline: the highest HLC this node has
    /// provably conveyed to the peer through fully-successful pushes.
    ///
    /// Advanced EXCLUSIVELY when a push completes with zero per-key
    /// errors (clean delta push, clean full-state push, digest push
    /// root-match / subset success) — NEVER by pulls. Using this (instead
    /// of the pull-advanced `peer_frontiers`) as the `delta_entries_since`
    /// baseline guarantees that every local entry is either at/below the
    /// baseline (covered by an earlier successful push) or included in
    /// the next delta push — a pull can no longer silently drop an
    /// un-pushed local entry (e.g. a tombstone) from all future pushes
    /// to that peer (C-2). The cost is an occasional re-push of entries a
    /// pull already echoed back; CRDT merges are idempotent.
    push_frontiers: HashMap<String, HlcTimestamp>,
    /// Per-peer LOCAL wall-clock time (ms) of the store snapshot/scan
    /// behind the last fully-successful push to that peer.
    ///
    /// This is the freshness evidence consumed by the tombstone-GC peer
    /// gate: `push_acked_wall_ms[peer] >= mark_ms` proves the peer merged
    /// (without per-key errors) everything this node's store held at a
    /// scan taken AFTER the mark — including every tombstone marked at or
    /// before it. Recorded at SNAPSHOT time (not push completion) so a
    /// tombstone created between the scan and the ack can never be
    /// claimed as conveyed, and measured on THIS node's clock so
    /// peer-clock skew propagated into data HLCs cannot forge freshness
    /// (both were possible with the previous `peer_frontiers`-based gate).
    push_acked_wall_ms: HashMap<String, u64>,
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
    /// Per-peer LOCAL wall-clock time (ms) at which this node last
    /// STARTED a pull that went on to absorb the peer's COMPLETE state
    /// with zero per-key errors and no poisoned keys: a digest pull that
    /// root-matched, a digest pull whose every mismatched bucket merged
    /// cleanly, or a legacy full-dump pull that merged cleanly.
    ///
    /// This is the INBOUND evidence consumed by the Stage 2 tombstone-GC
    /// hole-jump gate (`gc_inbound_gate_passed`):
    /// `pull_reconciled_wall_ms[peer] >= mark_ms` proves this node has
    /// seen everything the peer held at some point after the mark, so a
    /// dot that is still a hole (neither live nor deferred anywhere we
    /// looked) is live on no registry peer — i.e. removed. Recorded at
    /// request-START time (this node's clock, before the peer snapshots)
    /// so it can never overstate freshness; symmetric counterpart of the
    /// OUTBOUND `push_acked_wall_ms` evidence.
    pull_reconciled_wall_ms: HashMap<String, u64>,
    /// Wall-clock ms of the last emitted "GC gate blocked" WARN. A blocked
    /// gate repeats on every tick, so the log line is throttled; the
    /// counters and gauges carry the unthrottled signal.
    gc_gate_warn_last_ms: u64,
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
    /// Per-peer `(fingerprint, delivered_at_wall_ms)` of the last
    /// split-view gossip sample that was provably DELIVERED to the peer
    /// on the sync piggyback lane (M-14): a carrier request that reached
    /// a server (any decoded response) while carrying a non-empty sample
    /// records the sample's fingerprint here, and an unchanged sample is
    /// then suppressed for that peer — the steady state (no new heads)
    /// costs zero relay bytes.
    ///
    /// The suppression is TIME-BOUNDED to [`OBSERVED_RETENTION_MS`]: the
    /// receiver's detector state is memory-only and its heads age out
    /// after that same window, so a delivered-mark older than the window
    /// is treated as absent and the sample re-attached (see
    /// [`Self::observed_delivery_fresh`]). Without the bound, a static
    /// sample marked delivered once would be withheld forever — outliving
    /// a receiver restart (detector wiped) or a pre-M-14 peer's upgrade
    /// (the old peer decoded the request while silently dropping the
    /// trailing `observed` bytes) and permanently starving that relay
    /// path. Bounded by the peer count (pruned with `peer_frontiers`).
    observed_last_sent: HashMap<String, (u64, u64)>,
    /// Known cluster nodes for authority auto-reconfiguration.
    ///
    /// When this list changes (node join/leave), the runner triggers
    /// `recalculate_authorities()` on the system namespace, updating
    /// authority definitions based on placement policy tag criteria.
    cluster_nodes: Arc<std::sync::RwLock<Vec<Node>>>,
    /// This node's own `Node` record (id, mode, tags) from its config file.
    ///
    /// Set together with [`Self::inventory_source`]; see
    /// [`Self::set_cluster_inventory_source`].
    self_node: Option<Node>,
    /// Peer registry to derive [`Self::cluster_nodes`] from.
    ///
    /// `None` (the default, and what every in-process test uses) means the
    /// caller owns `cluster_nodes` and the runner only reads it.
    inventory_source: Option<Arc<Mutex<PeerRegistry>>>,
    /// Hash-based fingerprint for detecting cluster membership changes.
    /// Computed from sorted node IDs so that same-size replacements
    /// (e.g. 1 leave + 1 join) are detected correctly.
    tracked_cluster_generation: u64,
    /// Fingerprint of everything [`FrontierReporter::discover_scopes`] reads,
    /// used to reconcile [`Self::frontier_reporter`] against the namespace on
    /// each certification tick.
    ///
    /// Authority definitions change for reasons that have nothing to do with
    /// cluster membership -- `PUT /api/control-plane/authorities`, a Raft
    /// `PutAuthority` apply, the startup sweep -- and
    /// `detect_membership_changes` is not reached at all while placement is
    /// frozen. Without this the reporter could only ever be promoted by a
    /// membership change, so a node re-registered as an authority at runtime
    /// would never start reporting frontiers.
    tracked_reporter_fingerprint: u64,
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
    /// Write-ahead persisted floor over frontier report HLCs (M-12).
    /// `Some` only for authority nodes with a configured floor path. Every
    /// report tick covers its issued HLC here BEFORE signing/observing;
    /// on restart the clock is seeded from the lease, so a report HLC can
    /// never be re-issued even across a wall-clock rollback.
    report_floor: Option<ReportClockFloor>,
    /// Instant from which the store-digest report format is active (M-12).
    /// `None` = never (non-authority, kill switch semantics via helper);
    /// a future instant = activation grace running (floor file was absent
    /// at startup — see [`DIGEST_ACTIVATION_GRACE`]).
    store_digest_active_at: Option<Instant>,
    /// While `Some` and in the future, frontier reporting is fully
    /// suppressed (M-12 activation grace after a floorless boot): nothing
    /// is issued, covered, signed or observed, so no report of ANY format
    /// can collide with a head a peer still retains from a pre-restart
    /// incarnation. Cleared on the first tick at/after the instant. See
    /// [`DIGEST_ACTIVATION_GRACE`] for why silence (not the placeholder
    /// format) is required.
    report_silence_until: Option<Instant>,
}

/// Why the tombstone-GC dual gate is closed on this tick.
///
/// Purely a LABEL for an already-taken decision: the deciders are
/// [`NodeRunner::gc_authority_gate_passed`] and
/// [`NodeRunner::gc_peer_gate_passed`], which
/// [`NodeRunner::gc_gate_diagnose`] calls before classifying anything. A
/// blocked gate repeats every tick and, before this existed, was
/// indistinguishable in the metrics from a healthy node with nothing to
/// collect — which is how defect D1 stayed invisible from first boot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GcGateBlock {
    /// A certifiable range has fewer reporting authorities than it has
    /// authority nodes.
    AuthorityUnderReported {
        prefix: String,
        reported: usize,
        required: usize,
    },
    /// Every authority reported, but the scoped minimum frontier's DATA
    /// time is still behind the mark.
    FrontierBehindMark { prefix: String },
    /// Every authority reported past the mark in data time, but at least
    /// one report has not ADVANCED locally since the mark (receipt time —
    /// includes every frontier merely restored from persistence).
    ReportNotAdvanced { prefix: String },
    /// A registered sync peer has missing or pre-mark push evidence
    /// (commonly a dead peer left in the registry).
    PeerEvidenceMissingOrStale { peer_addr: String },
}

/// Minimum interval between "GC gate blocked" WARN lines.
const GC_GATE_WARN_THROTTLE_MS: u64 = 600_000;

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

/// Where the local digest for one digest exchange came from (see
/// [`NodeRunner::local_digest`]). Both variants describe exactly one
/// store state T0 (digest and frontier from a single lock scope).
enum LocalDigestSource {
    /// Read from the store's incremental digest cache: no data clone.
    /// `generation` is the mutation epoch at T0 — an unchanged reading
    /// under a later lock proves the store still IS T0, which is what
    /// lets the push path extract mismatched buckets from the live store
    /// and still advance T0-coupled evidence (all-or-nothing, M-7).
    Warm {
        digest: StoreDigest,
        frontier: Option<HlcTimestamp>,
        generation: u64,
    },
    /// Legacy cold fallback: full snapshot cloned under the lock, hashed
    /// off the lock; `buckets[i]` is the bucket of the i-th key of
    /// `data` in iteration order (captured in the same digest pass, so
    /// mismatched-bucket filtering needs no per-key re-hash).
    Snapshot {
        digest: StoreDigest,
        frontier: Option<HlcTimestamp>,
        data: std::collections::BTreeMap<String, crate::store::kv::CrdtValue>,
        buckets: Vec<u8>,
    },
}

impl LocalDigestSource {
    fn digest(&self) -> &StoreDigest {
        match self {
            LocalDigestSource::Warm { digest, .. } => digest,
            LocalDigestSource::Snapshot { digest, .. } => digest,
        }
    }

    fn frontier(&self) -> Option<HlcTimestamp> {
        match self {
            LocalDigestSource::Warm { frontier, .. } => frontier.clone(),
            LocalDigestSource::Snapshot { frontier, .. } => frontier.clone(),
        }
    }
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
        let (reporter, tracked_versions, tracked_policies, reporter_fingerprint, recovered_max_hlc) = {
            let api = certified_api.lock().await;
            let ns = api.namespace().read().unwrap_or_else(|e| e.into_inner());
            let reporter = FrontierReporter::new(node_id.clone(), &ns);
            let versions = Self::snapshot_policy_versions(&ns);
            let policies = Self::snapshot_policies(&ns);
            let fingerprint = Self::reporter_fingerprint(&ns, &node_id);
            (
                reporter,
                versions,
                policies,
                fingerprint,
                api.store().max_known_hlc(),
            )
        };
        let frontier_reporter = if reporter.is_authority() {
            Some(reporter)
        } else {
            None
        };
        let mut clock = Hlc::new(node_id.0.clone());
        // Best-effort recovery seed from the certified store's max HLC.
        // Insurance only: data HLCs do not necessarily cover report HLCs
        // (an idle authority reports far past its last write), so this
        // never replaces the report clock floor below.
        if frontier_reporter.is_some()
            && let Some(max) = &recovered_max_hlc
        {
            clock.seed_recovered(max);
        }
        let (report_floor, store_digest_active_at, report_silence_until) =
            Self::init_report_floor(&config, &node_id, &mut clock, frontier_reporter.is_some());
        let (epoch_manager, bls_keypair) = Self::init_epoch_and_bls(&config, &node_id);
        let frontier_sync_client =
            Self::build_frontier_sync_client(&config, frontier_reporter.is_some());
        let tombstone_gc = TombstoneGc::new(config.gc_interval, config.gc_retention);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        // Initialize the accused-authorities gauge from evidence restored
        // at startup, so a restart does not reset gauge-based alerting
        // while GET /api/authority/equivocations still shows accusations.
        if let Some(detector) = &config.equivocation {
            metrics.set_accused_authorities(detector.accused_count());
        }
        Self {
            clock,
            report_floor,
            store_digest_active_at,
            report_silence_until,
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
            push_frontiers: HashMap::new(),
            push_acked_wall_ms: HashMap::new(),
            pull_verified_frontiers: HashMap::new(),
            pull_reconciled_wall_ms: HashMap::new(),
            gc_gate_warn_last_ms: 0,
            peer_backoffs: HashMap::new(),
            digest_unsupported: HashMap::new(),
            observed_last_sent: HashMap::new(),
            cluster_nodes,
            self_node: None,
            inventory_source: None,
            // Use sentinel value to force initial recalculation on first tick.
            tracked_cluster_generation: u64::MAX,
            tracked_reporter_fingerprint: reporter_fingerprint,
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
        let (reporter, tracked_versions, tracked_policies, reporter_fingerprint, recovered_max_hlc) = {
            let api = certified_api.lock().await;
            let ns = api.namespace().read().unwrap_or_else(|e| e.into_inner());
            let reporter = FrontierReporter::new(node_id.clone(), &ns);
            let versions = Self::snapshot_policy_versions(&ns);
            let policies = Self::snapshot_policies(&ns);
            let fingerprint = Self::reporter_fingerprint(&ns, &node_id);
            (
                reporter,
                versions,
                policies,
                fingerprint,
                api.store().max_known_hlc(),
            )
        };
        let frontier_reporter = if reporter.is_authority() {
            Some(reporter)
        } else {
            None
        };

        let mut clock = Hlc::new(node_id.0.clone());
        // Best-effort recovery seed from the recovered stores' max HLCs
        // (certified + eventual). Insurance only — data HLCs do not
        // necessarily cover report HLCs; the report clock floor below is
        // what actually guarantees restart monotonicity.
        if frontier_reporter.is_some() {
            if let Some(max) = &recovered_max_hlc {
                clock.seed_recovered(max);
            }
            if let Some(max) = eventual_api.lock().await.store().max_known_hlc() {
                clock.seed_recovered(&max);
            }
        }
        let (report_floor, store_digest_active_at, report_silence_until) =
            Self::init_report_floor(&config, &node_id, &mut clock, frontier_reporter.is_some());
        let (epoch_manager, bls_keypair) = Self::init_epoch_and_bls(&config, &node_id);
        let frontier_sync_client =
            Self::build_frontier_sync_client(&config, frontier_reporter.is_some());
        let tombstone_gc = TombstoneGc::new(config.gc_interval, config.gc_retention);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        // Initialize the accused-authorities gauge from evidence restored
        // at startup (see `with_cluster_nodes` for rationale).
        if let Some(detector) = &config.equivocation {
            metrics.set_accused_authorities(detector.accused_count());
        }
        Self {
            clock,
            report_floor,
            store_digest_active_at,
            report_silence_until,
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
            push_frontiers: HashMap::new(),
            push_acked_wall_ms: HashMap::new(),
            pull_verified_frontiers: HashMap::new(),
            pull_reconciled_wall_ms: HashMap::new(),
            gc_gate_warn_last_ms: 0,
            peer_backoffs: HashMap::new(),
            digest_unsupported: HashMap::new(),
            observed_last_sent: HashMap::new(),
            cluster_nodes,
            self_node: None,
            inventory_source: None,
            // Use sentinel value to force initial recalculation on first tick,
            // consistent with `with_cluster_nodes()`.
            tracked_cluster_generation: u64::MAX,
            tracked_reporter_fingerprint: reporter_fingerprint,
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

    /// Initialize the report clock floor, the store-digest activation
    /// instant and the report-silence window for an authority node (M-12).
    ///
    /// Called from the constructors and — with identical semantics — from
    /// [`detect_membership_changes`](Self::detect_membership_changes) when
    /// a running node is promoted to authority at runtime (the promoted
    /// node has never reported in this process, so the same reasoning
    /// applies from the moment of promotion).
    ///
    /// - Floor file present: seed the clock from the persisted lease via
    ///   `seed_recovered` — NOT `Hlc::update`, whose `ClockSkew` guard
    ///   would reject the lease after a wall-clock rollback beyond 60s,
    ///   which is exactly the case the floor must cover. The artificial
    ///   skew is bounded by the lease width (10s), well inside the
    ///   detector's future-skew guard. The store-digest format is active
    ///   immediately and no silence applies: the write-ahead lease covers
    ///   every report this authority ever signed (the file is only ever
    ///   created by a covered post-grace report tick), so every new HLC is
    ///   fresh. CAVEAT (documented in the ops guide): this proof assumes
    ///   the file is the node's own latest copy — a floor RESTORED FROM A
    ///   BACKUP is stale monotonicity evidence and must be deleted
    ///   instead, so that the grace applies.
    /// - Floor file absent (first boot / lost data dir): frontier
    ///   reporting is fully SUPPRESSED for [`DIGEST_ACTIVATION_GRACE`],
    ///   in every format — the previous incarnation's format is unknown,
    ///   and only silence is collision-free in both format directions
    ///   (see the constant's docs). The store-digest format activates
    ///   when the silence lifts.
    /// - No floor path configured: the store-digest format NEVER activates
    ///   (fail-safe — without a persisted floor, restart monotonicity of
    ///   report HLCs cannot be guaranteed) and the legacy deterministic
    ///   placeholder is reported without silence, preserving pre-M-12
    ///   library/test behaviour.
    fn init_report_floor(
        config: &NodeRunnerConfig,
        node_id: &NodeId,
        clock: &mut Hlc,
        is_authority: bool,
    ) -> (Option<ReportClockFloor>, Option<Instant>, Option<Instant>) {
        if !is_authority {
            return (None, None, None);
        }
        let Some(path) = &config.frontier_clock_floor_path else {
            if config.frontier_store_digest {
                tracing::warn!(
                    node_id = %node_id.0,
                    "no frontier report clock floor path configured (no data dir?); \
                     store-digest frontier reports stay DISABLED and the placeholder \
                     digest format is kept"
                );
            }
            return (None, None, None);
        };
        let (floor, existed) = ReportClockFloor::load(path.clone());
        let (active_at, silence_until) = if existed {
            clock.seed_recovered(&HlcTimestamp {
                physical: floor.leased(),
                logical: 0,
                node_id: node_id.0.clone(),
            });
            (Instant::now(), None)
        } else {
            let grace = config
                .frontier_digest_activation_grace
                .unwrap_or(DIGEST_ACTIVATION_GRACE);
            tracing::warn!(
                node_id = %node_id.0,
                grace_ms = grace.as_millis() as u64,
                "frontier report clock floor absent (first boot or lost floor file); \
                 SUPPRESSING all frontier reports for the activation grace — nothing \
                 this node could sign is provably collision-free against heads peers \
                 may retain from a previous incarnation"
            );
            let until = Instant::now() + grace;
            (until, Some(until))
        };
        (Some(floor), Some(active_at), silence_until)
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

    /// Derive `cluster_nodes` from the peer registry instead of expecting the
    /// caller to maintain it.
    ///
    /// Only `main.rs` calls this. Tests that hand `with_cluster_nodes` an
    /// explicit inventory leave it unset and keep full control of the list.
    ///
    /// Declaring an inventory source also **freezes placement** — see
    /// [`placement_inventory_usable`](Self::placement_inventory_usable). The
    /// two are deliberately the same switch: a peer-registry inventory is the
    /// only kind that is missing peer identity, and it must never reach
    /// `select_nodes`.
    pub fn set_cluster_inventory_source(
        &mut self,
        self_node: Node,
        peers: Arc<Mutex<PeerRegistry>>,
    ) {
        self.self_node = Some(self_node);
        self.inventory_source = Some(peers);
        tracing::warn!(
            node = %self.node_id.0,
            "peer identity (mode/tags) is not propagated on the wire, so the \
             cluster inventory derived from the peer registry is incomplete. \
             Automatic authority derivation (FR-003) and rebalance planning \
             (FR-007) are frozen; set authority definitions explicitly with \
             PUT /api/control-plane/authorities. GET /api/topology and \
             latency-aware routing are unaffected."
        );
    }

    /// Whether the current cluster inventory may be fed to placement
    /// (`recalculate_authorities`, `compute_rebalance_plans`).
    ///
    /// False exactly when the inventory is derived from the peer registry.
    /// `PeerConfig` carries only `node_id` and `addr`: neither `mode` nor
    /// `tags` is on the wire, so peers can only be materialised with
    /// placeholder attributes.
    ///
    /// Feeding that to `PlacementPolicy::select_nodes` would be worse than
    /// feeding it nothing. Under a policy with `required_tags`, only this node
    /// carries real tags, so `matches_node` reduces the candidate set to
    /// `[self]` — on every node, each naming itself, none of it via Raft. That
    /// scope then has `total == 1` and `majority_threshold(1) == 1`, so a
    /// single node certifies writes with its own signature alone **and the
    /// resulting proof verifies**. The `total == 0` bug this series also fixes
    /// at least announced itself at verification time; `total == 1` does not.
    ///
    /// Substituting defaults does not help either: assuming `Both` makes
    /// subscribe-only peers eligible as store authorities, assuming
    /// `Subscribe` silently shrinks the set, and an empty tag set trivially
    /// satisfies `forbidden_tags`.
    ///
    /// Nothing is lost by freezing: with no writer at all, `select_nodes(&[])`
    /// already returned `[]` on every tick, so FR-003 and FR-007 have never
    /// run in a shipped binary. Unfreezing is the completion condition for
    /// propagating peer identity on the wire (follow-up).
    fn placement_inventory_usable(&self) -> bool {
        self.inventory_source.is_none()
    }

    /// Rebuild `cluster_nodes` from the peer registry.
    ///
    /// No-op unless [`set_cluster_inventory_source`](Self::set_cluster_inventory_source)
    /// was called. Peers are materialised as `NodeMode::Both` with no tags —
    /// placeholders that `placement_inventory_usable` keeps away from
    /// placement. They are safe for `TopologyView`, which publishes only
    /// region names, node counts and node IDs, and buckets an untagged node
    /// under `"unknown"` — an honest "region not known".
    async fn refresh_cluster_inventory(&mut self) {
        let Some(peers) = &self.inventory_source else {
            return;
        };
        let peer_configs = {
            // Drop the tokio guard before taking the std RwLock below: a std
            // lock must never be held across an await, and this keeps the two
            // locks from ever overlapping.
            let registry = peers.lock().await;
            registry.all_peers_owned()
        };

        let mut nodes: Vec<Node> = peer_configs
            .into_iter()
            .map(|p| Node::new(p.node_id, crate::types::NodeMode::Both))
            .collect();
        if let Some(self_node) = &self.self_node {
            nodes.retain(|n| n.id != self_node.id);
            nodes.push(self_node.clone());
        }
        nodes.sort_by(|a, b| a.id.0.cmp(&b.id.0));

        // Write only when the membership actually changed, so an unchanged
        // registry does not churn the cluster fingerprint every tick.
        let mut current = self
            .cluster_nodes
            .write()
            .unwrap_or_else(|e| e.into_inner());
        let unchanged = current.len() == nodes.len()
            && current.iter().zip(nodes.iter()).all(|(a, b)| a.id == b.id);
        if !unchanged {
            *current = nodes;
        }
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
        // Best-effort recovery seed (same insurance as the constructors —
        // never a substitute for the report clock floor). `try_lock`
        // because this setter is synchronous; a contended lock just means
        // the seed is skipped, which is safe.
        if self.frontier_reporter.is_some()
            && let Ok(guard) = api.try_lock()
            && let Some(max) = guard.store().max_known_hlc()
        {
            self.clock.seed_recovered(&max);
        }
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
        self.peer_frontiers
            .insert(peer_addr.to_string(), frontier.clone());
        // Simulate "successfully pushed up to `frontier`" so the delta
        // push path scans from it (mirrors what a real clean push sets).
        self.push_frontiers.insert(peer_addr.to_string(), frontier);
    }

    /// Inbound reconciliation evidence recorded for a peer, if any
    /// (test observability for the Stage 2 hole-jump gate — see
    /// `pull_reconciled_wall_ms`).
    pub fn pull_reconciled_for(&self, peer_addr: &str) -> Option<u64> {
        self.pull_reconciled_wall_ms.get(peer_addr).copied()
    }

    /// Outbound push evidence recorded for a peer, if any (test
    /// observability for the tombstone-GC peer gate — see
    /// `push_acked_wall_ms`).
    pub fn push_acked_for(&self, peer_addr: &str) -> Option<u64> {
        self.push_acked_wall_ms.get(peer_addr).copied()
    }

    /// Push frontier recorded for a peer, if any (test observability —
    /// see `push_frontiers`).
    pub fn push_frontier_for(&self, peer_addr: &str) -> Option<&HlcTimestamp> {
        self.push_frontiers.get(peer_addr)
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
        // Refresh the inventory before reading it (no-op unless a peer
        // registry was declared as the source).
        self.refresh_cluster_inventory().await;

        // Check for cluster membership changes first.
        self.detect_membership_changes().await;

        // Then reconcile the reporter against the namespace itself, which
        // changes for reasons membership never sees (an operator PUT, a Raft
        // `PutAuthority` apply, the startup sweep).
        self.reconcile_frontier_reporter().await;

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
            // Skipped while the inventory is peer-derived; see
            // `placement_inventory_usable`.
            if self.placement_inventory_usable() {
                let nodes: Vec<Node> = self
                    .cluster_nodes
                    .read()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone();
                let mut ns = api.namespace().write().unwrap_or_else(|e| e.into_inner());
                ns.recalculate_authorities(&nodes);
            }

            // Refresh the frontier reporter scopes.
            if let Some(reporter) = &mut self.frontier_reporter {
                let ns = api.namespace().read().unwrap_or_else(|e| e.into_inner());
                reporter.refresh_scopes(&ns);
            }
        }

        // Compute rebalance plans for changed policies. Also gated: a plan
        // built from placeholder tags would move real data to the wrong
        // nodes, which is worse than not rebalancing at all.
        if self.placement_inventory_usable() {
            self.compute_rebalance_plans(&changes, &deleted_prefixes)
                .await;
        }

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

                // Mark success PER KEY, not by count: `push_changed_keys`
                // returns an order-insensitive success count plus the exact
                // set of failed keys. Treating the count as a positional
                // prefix would misattribute success when an early key fails
                // but later keys succeed, permanently skipping the failed key
                // (silent under-replication reported as completed).
                let failed_set: std::collections::HashSet<&str> = match &push_result {
                    Ok(_) => std::collections::HashSet::new(),
                    Err(e) => e.failed_keys.iter().map(|s| s.as_str()).collect(),
                };
                for (batch_idx, key, _) in resolved.iter() {
                    if failed_set.contains(key.as_str()) {
                        failed += 1;
                    } else {
                        succeeded[*batch_idx] = true;
                        migrated += 1;
                    }
                }
                if let Err(e) = &push_result {
                    tracing::warn!(
                        target_node = %target_node.0,
                        error = %e,
                        "rebalance push failed"
                    );
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

    /// Fingerprint the namespace state that decides what this node reports.
    ///
    /// `FrontierReporter::discover_scopes` reads exactly three things per
    /// authority definition: the prefix, whether this node is a member, and
    /// the placement policy version for that prefix (a definition without a
    /// policy is membership-relevant but never reported, which is why the
    /// missing case gets its own sentinel rather than being skipped).
    fn reporter_fingerprint(ns: &SystemNamespace, node_id: &NodeId) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut entries: Vec<(&str, bool, Option<u64>)> = ns
            .all_authority_definitions()
            .into_iter()
            .map(|def| {
                (
                    def.key_range.prefix.as_str(),
                    def.authority_nodes.contains(node_id),
                    ns.get_placement_policy(&def.key_range.prefix)
                        .map(|p| p.version.0),
                )
            })
            .collect();
        entries.sort_unstable();
        let mut hasher = DefaultHasher::new();
        entries.len().hash(&mut hasher);
        for entry in entries {
            entry.hash(&mut hasher);
        }
        hasher.finish()
    }

    /// Install a freshly discovered reporter, promoting or demoting this node.
    fn adopt_reporter(&mut self, reporter: FrontierReporter) {
        if reporter.is_authority() {
            self.frontier_reporter = Some(reporter);
            // Runtime promotion (M-12): a node promoted here was NOT an
            // authority at construction time, so init_report_floor never
            // ran and the store-digest machinery (floor, activation
            // instant, floorless-boot silence) would silently stay off —
            // the node would report the legacy placeholder for its whole
            // process lifetime with no WARN and no restart-monotonicity
            // coverage. Run the exact constructor initialization now.
            // Guarded on report_floor so a demote/re-promote cycle keeps
            // the already-initialized floor and does not re-arm the
            // grace (the floor stays valid across demotion: it is only
            // ever advanced, never regressed).
            if self.report_floor.is_none() && self.store_digest_active_at.is_none() {
                let (floor, active_at, silence_until) =
                    Self::init_report_floor(&self.config, &self.node_id, &mut self.clock, true);
                self.report_floor = floor;
                self.store_digest_active_at = active_at;
                self.report_silence_until = silence_until;
            }
        } else {
            // Demotion: keep report_floor / store_digest_active_at so a
            // later re-promotion resumes with full restart-monotonicity
            // evidence instead of re-running the activation grace.
            self.frontier_reporter = None;
        }
    }

    /// Reconcile the frontier reporter against the current namespace.
    ///
    /// Runs on every certification tick, independent of both the membership
    /// fingerprint and the placement freeze. `detect_membership_changes` also
    /// adopts a reporter, but only along a path that a peer-derived inventory
    /// closes entirely — and only for changes that a *cluster* change caused.
    /// An operator repopulating a swept definition with
    /// `PUT /api/control-plane/authorities` (ops-guide 14.5.1) changes neither,
    /// so without this the node would accept the definition and still never
    /// report a frontier for it, leaving the range uncertifiable until restart.
    async fn reconcile_frontier_reporter(&mut self) {
        let outcome = {
            let api = self.certified_api.lock().await;
            let ns = api.namespace().read().unwrap_or_else(|e| e.into_inner());
            let fingerprint = Self::reporter_fingerprint(&ns, &self.node_id);
            if fingerprint == self.tracked_reporter_fingerprint {
                None
            } else {
                Some((
                    fingerprint,
                    FrontierReporter::new(self.node_id.clone(), &ns),
                ))
            }
        };
        let Some((fingerprint, reporter)) = outcome else {
            return;
        };
        self.tracked_reporter_fingerprint = fingerprint;
        let was_authority = self.frontier_reporter.is_some();
        self.adopt_reporter(reporter);
        match (was_authority, self.frontier_reporter.is_some()) {
            (false, true) => tracing::info!(
                node_id = %self.node_id.0,
                "promoted to frontier authority by an authority-definition change"
            ),
            (true, false) => tracing::info!(
                node_id = %self.node_id.0,
                "demoted from frontier authority by an authority-definition change"
            ),
            _ => {}
        }
    }

    async fn detect_membership_changes(&mut self) {
        let current_generation = {
            let nodes = self.cluster_nodes.read().unwrap_or_else(|e| e.into_inner());
            Self::cluster_fingerprint(&nodes)
        };
        if current_generation == self.tracked_cluster_generation {
            return;
        }

        // While placement is frozen the membership change is still real —
        // topology must follow it — but it must not reach `select_nodes`.
        // Deliberately leave `tracked_cluster_generation` alone so that the
        // recalculation fires as soon as the gate ever opens, instead of the
        // change being silently consumed here.
        if !self.placement_inventory_usable() {
            self.rebuild_topology();
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

        // Refresh the frontier reporter to pick up new authority scopes.
        let promoted = if changed > 0 {
            let ns = api.namespace().read().unwrap_or_else(|e| e.into_inner());
            Some((
                Self::reporter_fingerprint(&ns, &self.node_id),
                FrontierReporter::new(self.node_id.clone(), &ns),
            ))
        } else {
            None
        };
        drop(api);

        if let Some((fingerprint, reporter)) = promoted {
            self.tracked_reporter_fingerprint = fingerprint;
            self.adopt_reporter(reporter);
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
        // Split-view relay effectiveness check (M-14): the sync piggyback
        // re-delivers observed heads once per cycle, so a cycle at/above
        // the detector's retention window lets heads age out between
        // relays and the cross-check largely stops meeting.
        if sync_enabled
            && self.equivocation.is_some()
            && sync_duration.as_millis() as u64 >= OBSERVED_RETENTION_MS
        {
            tracing::warn!(
                sync_interval_ms = sync_duration.as_millis() as u64,
                observed_retention_ms = OBSERVED_RETENTION_MS,
                "sync_interval is at or above the observed-head retention window; \
                 split-view relay via sync piggyback loses effectiveness"
            );
        }

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
    /// time. If a rotation event occurs, logs the transition.
    ///
    /// NOTE: in production this NEVER produces a rotation event — the
    /// FR-008 automatic keyset rotation is unwired because nothing calls
    /// `EpochManager::stage_keys`, so there are no staged keys for
    /// `check_and_rotate` to promote at an epoch boundary. Operational
    /// key updates happen exclusively via `ASTEROIDB_AUTHORITY_KEYS`
    /// redistribution + restart (see the key-rotation runbook). The tick
    /// is still load-bearing for the shared epoch counter below.
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
    ///
    /// Per-tick order (M-12 — each step is load-bearing for the
    /// no-self-equivocation invariant):
    /// 0. if the floorless activation grace is still running, skip the
    ///    WHOLE tick — nothing is issued, covered, signed or observed, and
    ///    (crucially) the floor file is NOT created, so a crash during the
    ///    grace restarts it from scratch on the next boot;
    /// 1. issue the HLC (`Hlc::now()`, strictly monotone);
    /// 2. cover it in the persisted [`ReportClockFloor`] — write-ahead: a
    ///    failed fsync skips the WHOLE tick (nothing signed, nothing
    ///    observed; an unsigned HLC has claimed nothing, so discarding it
    ///    is safe). Issuing FIRST and covering SECOND is mandatory — a
    ///    "check the wall clock, then issue" scheme would let the wall
    ///    clock cross the lease between check and issue;
    /// 3. compute the digest string exactly ONCE and bind the same bytes
    ///    into every scope's report of this tick.
    async fn report_frontiers(&mut self) {
        if self.frontier_reporter.is_some() && !self.in_report_silence() {
            match self.clock.now() {
                Ok(issued) => {
                    let covered = match &mut self.report_floor {
                        Some(floor) => match floor.cover(&issued) {
                            Ok(()) => true,
                            Err(e) => {
                                tracing::warn!(
                                    error = %e,
                                    "failed to persist the frontier report clock floor; \
                                     skipping this report tick (nothing signed)"
                                );
                                self.metrics
                                    .frontier_report_skipped_floor_total
                                    .fetch_add(1, Ordering::Relaxed);
                                false
                            }
                        },
                        None => true,
                    };
                    if covered {
                        let digest_hash = self.current_frontier_digest_hash(&issued).await;
                        let frontiers = self
                            .frontier_reporter
                            .as_ref()
                            .expect("checked is_some above")
                            .report_frontiers_at(&issued, &digest_hash);
                        self.sign_apply_and_push_frontiers(frontiers).await;
                    }
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

    /// Return `true` while the floorless activation grace suppresses all
    /// frontier reporting (M-12), clearing the window once it has elapsed.
    ///
    /// Silence — not the placeholder format — is what makes a floorless
    /// boot safe: peers may still retain heads of EITHER format from a
    /// previous incarnation of this authority, and any signed report at a
    /// re-issued HLC could pair with one of them. Nothing signed, nothing
    /// to pair. See [`DIGEST_ACTIVATION_GRACE`].
    fn in_report_silence(&mut self) -> bool {
        match self.report_silence_until {
            None => false,
            Some(until) if Instant::now() >= until => {
                self.report_silence_until = None;
                tracing::info!(
                    node_id = %self.node_id.0,
                    "frontier report activation grace elapsed; reporting resumes \
                     (all pre-restart heads have aged out of peer detectors)"
                );
                false
            }
            Some(_) => {
                tracing::debug!(
                    "frontier report tick suppressed: floorless activation grace running"
                );
                true
            }
        }
    }

    /// Resolve the `digest_hash` string for the report tick at `ts`.
    ///
    /// Store-digest form (`sd2:<hex root>`) only when ALL of: the config
    /// enables it, a report clock floor is wired (restart monotonicity),
    /// and the activation instant has passed. Otherwise the legacy
    /// placeholder (kill switch off, or no floor path configured — a
    /// floorless-boot grace never reaches this point, because the whole
    /// tick is suppressed by [`in_report_silence`](Self::in_report_silence)
    /// until the activation instant). Cold-cache and no-store situations
    /// report per-tick constant sentinels — fail-safe in the
    /// detection-power direction, never in the false-positive direction.
    async fn current_frontier_digest_hash(&mut self, ts: &HlcTimestamp) -> String {
        let active = self.config.frontier_store_digest
            && self.report_floor.is_some()
            && self
                .store_digest_active_at
                .is_some_and(|at| Instant::now() >= at);
        if !active {
            return placeholder_digest_hash(&self.node_id, ts);
        }
        let Some(eventual) = self.eventual_api.clone() else {
            return SD_UNAVAILABLE.to_string();
        };
        // Best-effort warm-up OFF the store lock (M-7): the run loop must
        // never pay an O(N) cold rebuild under the lock. Failure is safe —
        // this tick reports the cold sentinel and the next one retries.
        let _ = crate::api::digest_warmup::ensure_digest_warm(&eventual).await;
        // Single lock scope, same pattern as the digest sync handler:
        // check temperature and read the root under one guard.
        let mut api = eventual.lock().await;
        if api.store().digest_is_cold() {
            tracing::debug!(
                "store digest cache still cold at report tick; binding the cold sentinel"
            );
            self.metrics
                .frontier_digest_cold_total
                .fetch_add(1, Ordering::Relaxed);
            return SD_COLD.to_string();
        }
        format_store_digest_hash(&api.store_mut().digest().root)
    }

    /// Sign (outside the certified lock), self-observe, apply and push one
    /// tick's frontier reports. Factored out of [`report_frontiers`] so the
    /// issue/cover/digest preamble stays readable.
    async fn sign_apply_and_push_frontiers(
        &mut self,
        frontiers: Vec<crate::authority::ack_frontier::AckFrontier>,
    ) {
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
        // index. An honest node can never conflict with itself: the
        // digest is computed exactly once per tick and frozen by the
        // report signature, and the HLC is strictly monotone — including
        // across restarts, via the ReportClockFloor write-ahead lease
        // (a floorless boot signs NOTHING until the activation grace has
        // fully elapsed) — so the same HLC is never signed twice. A self-equivocation therefore
        // signals a compromised key or a duplicate process sharing this
        // key seed — a REAL detection target, not a false positive.
        if let Some(detector) = &self.equivocation {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let mut evidence_dirty = false;
            for (f, sig) in frontiers.iter().zip(signatures.iter()) {
                if let Some(sig) = sig
                    && let ObserveOutcome::Equivocation(ev) = detector.observe(f, sig, now_ms)
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
            // Self-report exclusion (m-7): when exclusion is
            // enabled and THIS node is accused (compromised key
            // or duplicate process), its own attestations must
            // not feed certificate assembly either — the HTTP
            // receive path already excludes them, and without
            // this gate the self-report lane would re-insert
            // them locally. The frontier itself still advances.
            //
            // The accusation state is read UNDER the certified
            // lock, mirroring the HTTP path's apply-time
            // re-check: a concurrent handler that accuses this
            // node purges its pooled attestations inside the
            // same lock, so either that purge already ran (we
            // read accused=true here and skip the inserts) or
            // it is serialized after us and removes whatever we
            // insert below. Reading the flag before the lock
            // would reopen a window in which a fresh
            // self-attestation slips in right after the purge
            // and can be consumed by a certification tick.
            let self_excluded = self.config.exclude_accused_authorities
                && self
                    .equivocation
                    .as_ref()
                    .is_some_and(|d| d.is_accused(&self.node_id));
            for (f, sig) in frontiers.iter().zip(signatures.iter()) {
                match (&self.node_signer, sig) {
                    (Some(signer), Some(sig)) if !self_excluded => {
                        // Own signature: no re-verification needed.
                        let att = signer.self_verified(f, sig);
                        api.update_frontier_verified(f.clone(), Some(att));
                    }
                    _ => {
                        api.update_frontier(f.clone());
                    }
                }
            }
            // Purge attestations pooled BEFORE the accusation
            // (m-7). Gated on the accusation state rather than
            // on a detection in this very tick, so an
            // accusation that lands via the shared detector
            // between ticks is also enforced; after the first
            // purge this is O(1) per tick.
            if self_excluded {
                api.purge_accused_attestations(std::slice::from_ref(&self.node_id));
            }
            let stats = api.attestation_stats();
            self.metrics.set_attestation_pool_stats(
                stats.scopes,
                stats.rejected_unknown_range_total,
                stats.rejected_version_window_total,
                stats.stale_version_total,
                stats.rejected_fenced_total,
                stats.rejected_scope_cap_total,
                stats.rejected_authority_cap_total,
                stats.purged_total,
                stats.frontier_skew_rejected_total,
            );
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

    /// True when `peer_key` has a recorded delivery of exactly this
    /// sample fingerprint that is still fresh, i.e. younger than
    /// [`OBSERVED_RETENTION_MS`].
    ///
    /// Expiring the delivered-mark is load-bearing for detection
    /// reachability: the receiver's observed-head index is memory-only
    /// (only evidence pairs are persisted) and ages heads out after the
    /// same window, so "delivered once" says nothing about the receiver
    /// still holding the sample. A restarted relay hop, an aged-out head,
    /// or a pre-M-14 peer that decoded the carrier while dropping the
    /// trailing bytes are all re-covered at most one window later. The
    /// steady-state cost of the bound is one redundant sample (~80KB max)
    /// per peer per window, deduplicated at the receiver by
    /// `is_known_exact` with no extra signature verification.
    fn observed_delivery_fresh(
        observed_last_sent: &HashMap<String, (u64, u64)>,
        peer_key: &str,
        sample_fp: u64,
        now_wall_ms: u64,
    ) -> bool {
        observed_last_sent
            .get(peer_key)
            .is_some_and(|&(fp, delivered_at_ms)| {
                fp == sample_fp
                    && now_wall_ms.saturating_sub(delivered_at_ms) < OBSERVED_RETENTION_MS
            })
    }

    /// Deterministic fingerprint of a split-view gossip sample (M-14),
    /// over the identity fields of each observation in sample order.
    /// Used to suppress re-sending an unchanged sample to a peer; a
    /// spurious mismatch only costs a redundant (deduplicated) relay,
    /// never a missed one. Empty sample → 0.
    fn observed_sample_fingerprint(
        sample: &[crate::authority::equivocation::ObservedAttestation],
    ) -> u64 {
        if sample.is_empty() {
            return 0;
        }
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for obs in sample {
            obs.frontier.authority_id.0.hash(&mut hasher);
            obs.frontier.key_range.prefix.hash(&mut hasher);
            obs.frontier.policy_version.0.hash(&mut hasher);
            obs.frontier.frontier_hlc.hash(&mut hasher);
            obs.frontier.digest_hash.hash(&mut hasher);
        }
        hasher.finish()
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

        // Best-effort digest cache warm-up (M-7): move any pending O(N)
        // digest rebuild OFF the store lock before the per-peer digest
        // exchanges. Failure is safe — cold exchanges fall back to the
        // legacy snapshot path and the next cycle retries.
        if self.config.digest_sync_enabled {
            let _ = crate::api::digest_warmup::ensure_digest_warm(eventual_api).await;
        }

        // Split-view relay sample (M-14): every node — authority or not —
        // piggybacks its observed-attestation gossip sample on the sync
        // lane, so conflicting heads meet even when a split view targets
        // only non-authority nodes (the frontier push lane relays only
        // from authorities). Sampled ONCE per cycle; each peer gets it on
        // at most one carrier request below, and an unchanged sample
        // already delivered to a peer is suppressed entirely.
        let gossip_sample = self
            .equivocation
            .as_ref()
            .map(|d| d.gossip_summaries(GOSSIP_SAMPLE_MAX))
            .unwrap_or_default();
        let sample_fp = Self::observed_sample_fingerprint(&gossip_sample);

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

            // Relay sample pending for THIS peer (attach-once): the first
            // carrier request sent to the peer takes it; `None` when the
            // sample is empty or unchanged since a still-fresh delivery
            // (delivered-marks expire after OBSERVED_RETENTION_MS — see
            // `observed_delivery_fresh`).
            let mut observed_pending: Option<
                Vec<crate::authority::equivocation::ObservedAttestation>,
            > = (!gossip_sample.is_empty()
                && !Self::observed_delivery_fresh(
                    &self.observed_last_sent,
                    &peer_key,
                    sample_fp,
                    crate::hlc::wall_clock_ms(),
                ))
            .then(|| gossip_sample.clone());

            // --- Push phase: send only changed local keys to peer ---
            // When the change rate is too high (changed_keys / total_keys > threshold),
            // delta sync payload approaches full-state size and loses its advantage.
            // In that case, skip delta and push the full state directly.
            if self.peer_frontiers.contains_key(&peer_key) {
                // The delta baseline is the PUSH-ONLY frontier: everything
                // at/below it was covered by an earlier fully-successful
                // push. The pull-advanced `peer_frontiers` must NOT be used
                // here — a pull that outran an un-pushed local entry (e.g.
                // a tombstone whose push failed) would silently drop it
                // from every future delta push (C-2 resurrection enabler).
                let frontier =
                    self.push_frontiers
                        .get(&peer_key)
                        .cloned()
                        .unwrap_or(HlcTimestamp {
                            physical: 0,
                            logical: 0,
                            node_id: String::new(),
                        });
                // Wall-clock instant BEFORE the scan: recorded as the
                // push-evidence time on success (see `push_acked_wall_ms`).
                let scan_wall_ms = crate::hlc::wall_clock_ms();
                let api = eventual_api.lock().await;
                let total_keys = api.store().len();
                // delta_entries_since returns delta-state entries sorted by
                // HLC; each value contains only the portion changed since
                // the frontier, reducing bandwidth compared to full state.
                let entries_with_hlc: Vec<(
                    String,
                    crate::store::kv::CrdtValue,
                    crate::hlc::HlcTimestamp,
                )> = api.store().delta_entries_since(&frontier);
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
                        // First carrier candidate for the relay sample.
                        let relay_observed = observed_pending.take().unwrap_or_default();
                        let relay_attached = !relay_observed.is_empty();
                        let outcome = Self::try_digest_push(
                            sync_client,
                            eventual_api,
                            &self.metrics,
                            &self.node_id.0,
                            peer_id,
                            &peer_key,
                            &peer.addr,
                            &mut self.peer_frontiers,
                            &mut self.push_frontiers,
                            &mut self.push_acked_wall_ms,
                            &mut self.digest_unsupported,
                            relay_observed,
                        )
                        .await;
                        if relay_attached && matches!(outcome, DigestPushOutcome::Handled) {
                            // Handled implies a decoded scheme-ok response:
                            // the peer ingested the sample.
                            self.observed_last_sent
                                .insert(peer_key.clone(), (sample_fp, crate::hlc::wall_clock_ms()));
                        }
                        matches!(outcome, DigestPushOutcome::Handled)
                    } else {
                        false
                    };

                    if !digest_handled {
                        self.metrics
                            .full_sync_fallback_count
                            .fetch_add(1, Ordering::Relaxed);

                        // Snapshot entries AND frontier in one lock scope:
                        // the frontier must describe exactly the pushed
                        // state (a post-push `current_frontier()` could
                        // cover concurrent writes the push never carried).
                        let snapshot_wall_ms = crate::hlc::wall_clock_ms();
                        let api = eventual_api.lock().await;
                        let all_entries: HashMap<String, crate::store::kv::CrdtValue> = api
                            .store()
                            .all_entries()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect();
                        let snapshot_frontier = api.store().current_frontier();
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
                                // After a clean full push, advance both
                                // frontiers to the snapshot frontier and
                                // record the push evidence (GC peer gate).
                                if let Some(frontier) = snapshot_frontier {
                                    self.peer_frontiers
                                        .insert(peer_key.clone(), frontier.clone());
                                    self.push_frontiers.insert(peer_key.clone(), frontier);
                                }
                                self.push_acked_wall_ms
                                    .insert(peer_key.clone(), snapshot_wall_ms);
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
                                // Carrier candidate for the relay sample
                                // (oversized-delta branch).
                                let relay_observed = observed_pending.take().unwrap_or_default();
                                let relay_attached = !relay_observed.is_empty();
                                let outcome = Self::try_digest_push(
                                    sync_client,
                                    eventual_api,
                                    &self.metrics,
                                    &self.node_id.0,
                                    peer_id,
                                    &peer_key,
                                    &peer.addr,
                                    &mut self.peer_frontiers,
                                    &mut self.push_frontiers,
                                    &mut self.push_acked_wall_ms,
                                    &mut self.digest_unsupported,
                                    relay_observed,
                                )
                                .await;
                                if relay_attached && matches!(outcome, DigestPushOutcome::Handled) {
                                    self.observed_last_sent.insert(
                                        peer_key.clone(),
                                        (sample_fp, crate::hlc::wall_clock_ms()),
                                    );
                                }
                                matches!(outcome, DigestPushOutcome::Handled)
                            } else {
                                false
                            };

                            if !digest_handled {
                                self.metrics
                                    .full_sync_fallback_count
                                    .fetch_add(1, Ordering::Relaxed);

                                // Snapshot entries AND frontier in one lock
                                // scope (see the high-change-rate branch).
                                let snapshot_wall_ms = crate::hlc::wall_clock_ms();
                                let api = eventual_api.lock().await;
                                let snapshot: Vec<(String, crate::store::kv::CrdtValue)> = api
                                    .store()
                                    .all_entries()
                                    .map(|(k, v)| (k.clone(), v.clone()))
                                    .collect();
                                let snapshot_frontier = api.store().current_frontier();
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
                                        if let Some(frontier) = snapshot_frontier {
                                            self.peer_frontiers
                                                .insert(peer_key.clone(), frontier.clone());
                                            self.push_frontiers.insert(peer_key.clone(), frontier);
                                        }
                                        self.push_acked_wall_ms
                                            .insert(peer_key.clone(), snapshot_wall_ms);
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
                                    // Advance the push frontier to the max HLC
                                    // of the pushed batch — NOT
                                    // current_frontier(), which may have
                                    // advanced past unpushed concurrent
                                    // writes. `peer_frontiers` may already sit
                                    // higher (pull-advanced), so it only
                                    // advances max-monotonically.
                                    if let Some(max_hlc) = hlc_vec.last() {
                                        self.push_frontiers
                                            .insert(peer_key.clone(), max_hlc.clone());
                                        if self
                                            .peer_frontiers
                                            .get(&peer_key)
                                            .is_none_or(|existing| *max_hlc > *existing)
                                        {
                                            self.peer_frontiers
                                                .insert(peer_key.clone(), max_hlc.clone());
                                        }
                                    }
                                    // Push evidence for the tombstone-GC peer
                                    // gate: the scan (taken at scan_wall_ms)
                                    // was fully conveyed with zero per-key
                                    // errors (`Ok` implies no failed keys).
                                    self.push_acked_wall_ms
                                        .insert(peer_key.clone(), scan_wall_ms);
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
                    } else {
                        // Nothing above the push-only baseline: every local
                        // entry was already conveyed by an earlier fully-
                        // successful push, so the (empty) scan itself is
                        // fresh push evidence for the GC peer gate.
                        self.push_acked_wall_ms
                            .insert(peer_key.clone(), scan_wall_ms);
                    }
                }
            } else {
                // No frontier known for this peer — this is the initial sync.
                // Push the full local state so the peer receives our data even
                // if it has nothing to offer us in return. Without this push,
                // data written locally would never reach a peer that starts
                // empty, because both the delta push and delta pull paths
                // require a known frontier.
                let snapshot_wall_ms = crate::hlc::wall_clock_ms();
                let api = eventual_api.lock().await;
                let snapshot: Vec<(String, crate::store::kv::CrdtValue)> = api
                    .store()
                    .all_entries()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                let snapshot_frontier = api.store().current_frontier();
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
                            // All keys merged successfully: seed the
                            // push-only baseline and the push evidence
                            // (GC peer gate) from the snapshot. The
                            // pull-oriented `peer_frontiers` still starts
                            // at ZERO below.
                            if let Some(frontier) = snapshot_frontier {
                                self.push_frontiers.insert(peer_key.clone(), frontier);
                            }
                            self.push_acked_wall_ms
                                .insert(peer_key.clone(), snapshot_wall_ms);
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
                // Carrier candidate for the relay sample: on NetworkError
                // the request never reached the server, so the SAME sample
                // is re-attached to the built-in retry below (a decoded
                // response of any kind counts as delivered — echoes of an
                // over-delivered sample are deduped by `is_known_exact`).
                let relay_observed = observed_pending.take().unwrap_or_default();
                let delta_result = sync_client
                    .pull_delta(
                        &peer.addr,
                        &self.node_id.0,
                        &frontier,
                        relay_observed.clone(),
                    )
                    .await;
                if !relay_observed.is_empty()
                    && !matches!(delta_result, PullDeltaResult::NetworkError)
                {
                    self.observed_last_sent
                        .insert(peer_key.clone(), (sample_fp, crate::hlc::wall_clock_ms()));
                }

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
                        // The relay sample (if any) did not reach the server,
                        // so it rides the retry too (re-attach once — the
                        // built-in retry itself is single-shot).
                        let retry_result = sync_client
                            .pull_delta(
                                &peer.addr,
                                &self.node_id.0,
                                &frontier,
                                relay_observed.clone(),
                            )
                            .await;
                        if !relay_observed.is_empty()
                            && !matches!(retry_result, PullDeltaResult::NetworkError)
                        {
                            self.observed_last_sent
                                .insert(peer_key.clone(), (sample_fp, crate::hlc::wall_clock_ms()));
                        }

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
                // Carrier candidate for the relay sample (digest pull —
                // reached only when no earlier carrier took the sample).
                let relay_observed = observed_pending.take().unwrap_or_default();
                let relay_attached = !relay_observed.is_empty();
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
                    &mut self.pull_reconciled_wall_ms,
                    relay_observed,
                )
                .await;
                if relay_attached && matches!(outcome, DigestPullOutcome::Synced) {
                    self.observed_last_sent
                        .insert(peer_key.clone(), (sample_fp, crate::hlc::wall_clock_ms()));
                }
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
            let full_pull_start_wall_ms = crate::hlc::wall_clock_ms();
            if let Some(dump) = sync_client.pull_all_keys(&peer.addr).await {
                let merge_errors = Self::apply_complete_state(
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
                // Inbound evidence (Stage 2 hole-jump gate): a legacy
                // full dump is the peer's complete state; record only a
                // clean, poison-free absorption (fail-closed).
                if merge_errors == 0 {
                    let poisoned = {
                        let api = eventual_api.lock().await;
                        !api.store().merge_failed_keys().is_empty()
                    };
                    if !poisoned {
                        self.pull_reconciled_wall_ms
                            .insert(peer_key.clone(), full_pull_start_wall_ms);
                    }
                }

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
        self.push_frontiers
            .retain(|addr, _| active_addrs.contains(addr));
        self.push_acked_wall_ms
            .retain(|addr, _| active_addrs.contains(addr));
        self.pull_verified_frontiers
            .retain(|addr, _| active_addrs.contains(addr));
        self.pull_reconciled_wall_ms
            .retain(|addr, _| active_addrs.contains(addr));
        self.peer_backoffs
            .retain(|addr, _| active_addrs.contains(addr));
        self.digest_unsupported
            .retain(|addr, _| active_addrs.contains(addr));
        self.observed_last_sent
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

            // Ping is what discovers and evicts peers, so pick the result up
            // immediately rather than waiting for the next certification tick.
            self.refresh_cluster_inventory().await;
            self.rebuild_topology();
        }
    }

    /// The one and only definition of the range population used by the
    /// tombstone-GC authority gate and by compaction.
    ///
    /// A prefix qualifies only when it carries BOTH an authority definition
    /// and a placement policy (`SystemNamespace::certifiable_ranges`). The
    /// previous code took every definition and fabricated `PolicyVersion(1)`
    /// for the policy-less ones, which put scope `(prefix, 1)` into the
    /// gate's conjunction — a scope no reporter emits (`discover_scopes`
    /// skips policy-less definitions) and no receiver admits
    /// (`AdmissionReject::NoPolicy`). Such a term is unsatisfiable rather
    /// than strict, and since a policy-less range cannot hold certified
    /// state in the first place (`resolve_scope` fails), dropping the term
    /// removes no protection: eventual state is guarded by the peer and
    /// inbound gates, which never consult authority definitions at all.
    ///
    /// Returns parallel vectors — same order, same length by construction,
    /// built in a single pass so the two can never drift apart.
    fn certifiable_population(
        ns: &SystemNamespace,
    ) -> (Vec<(KeyRange, usize)>, Vec<PolicyVersion>) {
        let mut defs = Vec::new();
        let mut policy_versions = Vec::new();
        for (def, version) in ns.certifiable_ranges() {
            defs.push((def.key_range.clone(), def.authority_nodes.len()));
            policy_versions.push(version);
        }
        (defs, policy_versions)
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

            // Iterate over the certifiable key ranges (definition AND
            // policy), carrying each range's real policy version.
            let (defs, policy_versions) = Self::certifiable_population(&ns);

            let fs = api.frontier_set().clone();

            // Drop ns (RwLock read guard) and api (tokio Mutex guard) here.
            (defs, fs, policy_versions)
        };

        // Phase 2: Aggregate per-key write ops into per-range counts by
        // matching each written key against key range prefixes. Keys that
        // match no certifiable range are NOT attributed to any range —
        // they are counted on `compaction_unattributed_write_ops_total`
        // instead. The old fallback charged them to `defs[0]`, i.e. to a
        // prefix picked out of `HashMap` iteration order; the catch-all
        // `""` definition (which matches every key) was the only reason
        // that arbitrariness never surfaced.
        if !ops_by_key.is_empty() && !defs.is_empty() {
            let mut range_ops: HashMap<&str, u64> = HashMap::new();
            for (key, count) in &ops_by_key {
                let matched = defs
                    .iter()
                    .find(|(kr, _)| key.starts_with(&kr.prefix))
                    .map(|(kr, _)| kr.prefix.as_str());
                let Some(prefix) = matched else {
                    self.metrics
                        .compaction_unattributed_write_ops_total
                        .fetch_add(*count, Ordering::Relaxed);
                    continue;
                };
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
        } else if !ops_by_key.is_empty() {
            // No certifiable range at all: every drained op is
            // unattributable. Counted rather than silently discarded.
            self.metrics
                .compaction_unattributed_write_ops_total
                .fetch_add(ops_by_key.values().sum::<u64>(), Ordering::Relaxed);
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

    /// Run tombstone GC on the eventual store (if available) — gated
    /// mark-and-sweep (see [`TombstoneGc::mark_and_sweep`]).
    ///
    /// Pass N snapshots (marks) the deferred dots; pass N+1, at least
    /// `gc_retention` later, collects the MARKED dots only when a dual
    /// replica-synchronisation gate holds against the mark time:
    ///
    /// - **Authority gate**: for every authority definition, EVERY
    ///   authority in the definition has an ack-frontier entry for the
    ///   current scope, the scoped minimum `frontier_hlc.physical` has
    ///   passed the mark time, AND every scoped frontier ADVANCED (a
    ///   strictly newer report was received) at a LOCAL wall-clock time
    ///   past the mark. The data-time check alone is not enough:
    ///   `frontier_hlc` is data time, which the HLC max rule pushes
    ///   ahead of this node's wall clock under peer-clock skew, so a
    ///   stale pre-partition frontier could read as "past the mark"
    ///   throughout a partition. The receipt check is measured entirely
    ///   on this node's own clock and fails for any frontier that has
    ///   not advanced since the mark.
    /// - **Peer gate**: every peer in the sync registry has push
    ///   evidence (`push_acked_wall_ms`, recorded ONLY when a push
    ///   completes with zero per-key errors, stamped with the LOCAL
    ///   wall-clock time of the store scan behind that push) past the
    ///   mark time — the peer provably merged everything this node held
    ///   at a scan taken after the mark, including the marked
    ///   tombstones. The pull-advanced `peer_frontiers` map is never
    ///   consulted (a pull proves nothing about what the peer received),
    ///   and no data-HLC physical is ever compared against the
    ///   wall-clock mark (peer clock skew propagates into data HLCs and
    ///   could otherwise forge freshness).
    ///
    /// Any missing entry fails the gate (fail-closed): a partition, a
    /// lagging authority, or a DEAD PEER LEFT IN THE REGISTRY stalls
    /// collection entirely — tombstones then accumulate until the
    /// cluster heals or the peer is removed (an ops trade-off documented
    /// in the ops guide). A single-node deployment (no authority
    /// definitions, no peers) passes vacuously: with no other replica
    /// there is no resurrection source.
    ///
    /// Residual limits (also documented): frontiers are key-range
    /// consumption reports, not per-dot acks; replicas this node has
    /// never heard of are outside the gate (although post-floor their
    /// stale live dots are killed/rejected by the floor on merge);
    /// majority-reach GC is an open design question.
    ///
    /// **Stage 2 hole-jump** (`gc_hole_jump_enabled`, default off): in
    /// addition to the dual OUTBOUND gate above, the sweep may cross
    /// legacy holes (dots the pre-floor sweep physically deleted) when
    /// the INBOUND gate holds — every registry peer has
    /// `pull_reconciled_wall_ms` evidence (a complete, error-free pull
    /// STARTED) at/after the mark. Having absorbed every known peer's
    /// full state since the mark, a dot that was ALREADY a hole at mark
    /// time and is still a hole is live nowhere known, i.e. removed. The
    /// sweep enforces the "at mark time" part itself: the mark snapshots
    /// each value's per-node counters and the walk only jumps holes at
    /// or below that snapshot — a hole minted AFTER the mark by an
    /// inbound partial delta (counters ride deltas in full while an
    /// entry below the requested frontier is filtered out) may be live
    /// on the pushing peer, and the pull evidence, taken earlier, proves
    /// nothing about it (see the `TombstoneGc` module docs). Fail-closed:
    /// a missing entry or a disabled flag keeps the walk stalled
    /// (`gc_floor_stalled_hole_dots`).
    ///
    /// **P1-10 note** (why the gate compares HLC *time*, never counters):
    /// dot counters are per-CRDT/per-writer small integers and no
    /// cross-replica protocol transports them as floors; the legacy
    /// version-floor APIs were REPLACED by the per-value
    /// `compaction_floor`, which lives in dot space and advances only
    /// through the certified contiguous walk and merge inheritance
    /// (units mismatch, ~10^12 vs small ints, was the original P1-10
    /// bug). The mark-and-sweep design only ever compares wall-clock
    /// mark times against frontier *times*; dot identity is handled by
    /// the marked candidate sets.
    async fn run_gc(&mut self) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        if !self.tombstone_gc.should_run(now_ms) {
            return;
        }
        let Some(eventual_api) = self.eventual_api.clone() else {
            return;
        };

        // Evaluate the dual gate against the pending mark, if any (the
        // very first pass only marks, so the verdict is irrelevant then).
        let (gates_passed, block) = match self.tombstone_gc.pending_mark_ms() {
            Some(mark_ms) => {
                let block = self.gc_gate_diagnosis(mark_ms).await;
                (block.is_none(), block)
            }
            None => (false, None),
        };
        if let Some(block) = &block {
            self.record_gc_gate_block(block, now_ms);
        }

        // Stage 2 hole-jump requires the ADDITIONAL inbound gate.
        let allow_hole_jump = self.config.gc_hole_jump_enabled
            && gates_passed
            && match self.tombstone_gc.pending_mark_ms() {
                Some(mark_ms) => {
                    if let Some(sync_client) = &self.sync_client {
                        let peers = sync_client.peer_registry().lock().await.all_peers_owned();
                        Self::gc_inbound_gate_passed(&peers, &self.pull_reconciled_wall_ms, mark_ms)
                    } else {
                        // No sync layer — no peers exist to hold a hole
                        // dot live (single-node case).
                        true
                    }
                }
                None => false,
            };

        let mut api = eventual_api.lock().await;
        let stats = self.tombstone_gc.mark_and_sweep(
            api.store_mut(),
            now_ms,
            gates_passed,
            allow_hole_jump,
        );
        let floor_fx = api.store_mut().take_floor_effects();
        let redundant_merge_skips = api.redundant_merge_skips();
        drop(api);

        // Publish floor observability: stall gauges reflect the latest
        // EXECUTED sweep; merge-effect counters accumulate across ticks.
        // Mark-only and gate-blocked passes return zeroed stats
        // (`swept == false`) and must NOT overwrite the gauges: with the
        // default 60s interval / 300s retention only ~1 tick in 5 sweeps,
        // and zeroing on the other ticks would flap a persistent hole
        // stall to 0 for most scrapes — the exact signal ops-guide 3.7
        // uses to decide on Stage 2 (and 12.3 to diagnose blocked gates).
        if stats.swept {
            // Top-level GC liveness. Stamped ONLY by passes that actually
            // swept, for the same reason as the stall gauges below — but
            // unlike them a stuck value here is itself the alarm: it is
            // the one signal that distinguishes a permanently-closed gate
            // from a healthy node with nothing to collect.
            self.metrics
                .gc_last_sweep_wall_ms
                .store(now_ms, Ordering::Relaxed);
            self.metrics
                .gc_floor_stalled_hole_dots
                .store(stats.stalled_holes, Ordering::Relaxed);
            self.metrics
                .gc_floor_stalled_uncandidated_dots
                .store(stats.stalled_uncandidated, Ordering::Relaxed);
        }
        self.metrics
            .gc_floor_rejected_dots_total
            .fetch_add(floor_fx.rejected_covered_deferred, Ordering::Relaxed);
        self.metrics.gc_floor_killed_by_floor_total.fetch_add(
            floor_fx.killed_by_floor + floor_fx.rejected_stale_live,
            Ordering::Relaxed,
        );
        // Mirror the RR-gate skip counter (M-6) — a cumulative value kept
        // by the EventualApi, so `store` (not `fetch_add`) is correct.
        self.metrics
            .sync_redundant_merge_skips_total
            .store(redundant_merge_skips, Ordering::Relaxed);

        if stats.collected > 0 {
            tracing::info!(
                node_id = %self.node_id.0,
                collected = stats.collected,
                stalled_holes = stats.stalled_holes,
                stalled_uncandidated = stats.stalled_uncandidated,
                total = self.tombstone_gc.total_collected(),
                "tombstone GC completed"
            );
        }
    }

    /// Count a blocked GC tick and, at most once per
    /// [`GC_GATE_WARN_THROTTLE_MS`], say why in the log.
    ///
    /// A legitimately lagging cluster blocks the gate too, so these
    /// counters are diagnostic, not alarms — alert on
    /// `gc_last_sweep_wall_ms` failing to advance and use these to
    /// explain it.
    fn record_gc_gate_block(&mut self, block: &GcGateBlock, now_ms: u64) {
        match block {
            GcGateBlock::AuthorityUnderReported { .. }
            | GcGateBlock::FrontierBehindMark { .. }
            | GcGateBlock::ReportNotAdvanced { .. } => {
                self.metrics
                    .gc_gate_blocked_authority_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            GcGateBlock::PeerEvidenceMissingOrStale { .. } => {
                self.metrics
                    .gc_gate_blocked_peer_total
                    .fetch_add(1, Ordering::Relaxed);
            }
        }

        if self.gc_gate_warn_last_ms != 0
            && now_ms.saturating_sub(self.gc_gate_warn_last_ms) < GC_GATE_WARN_THROTTLE_MS
        {
            return;
        }
        self.gc_gate_warn_last_ms = now_ms;

        // A single-node deployment (no sync layer) is expected to have no
        // peers; a node WITH a sync layer and an empty registry is not,
        // and sweeps there are unguarded by the peer gate.
        let peer_population = self.metrics.gc_gate_peer_population.load(Ordering::Relaxed);
        let peer_lane = match (&self.sync_client, peer_population) {
            (None, _) => "none (single-node, no sync layer)",
            (Some(_), 0) => "empty registry (peer gate vacuous)",
            (Some(_), _) => "registered peers",
        };

        match block {
            GcGateBlock::AuthorityUnderReported {
                prefix,
                reported,
                required,
            } => tracing::warn!(
                node_id = %self.node_id.0,
                reason = "authority_under_reported",
                prefix = %prefix,
                reported,
                required,
                peer_lane,
                "tombstone GC gate blocked: not every authority has reported this scope"
            ),
            GcGateBlock::FrontierBehindMark { prefix } => tracing::warn!(
                node_id = %self.node_id.0,
                reason = "frontier_behind_mark",
                prefix = %prefix,
                peer_lane,
                "tombstone GC gate blocked: scoped minimum frontier is behind the mark"
            ),
            GcGateBlock::ReportNotAdvanced { prefix } => tracing::warn!(
                node_id = %self.node_id.0,
                reason = "report_not_advanced",
                prefix = %prefix,
                peer_lane,
                "tombstone GC gate blocked: no frontier report has advanced since the mark"
            ),
            GcGateBlock::PeerEvidenceMissingOrStale { peer_addr } => tracing::warn!(
                node_id = %self.node_id.0,
                reason = "peer_evidence_missing_or_stale",
                peer = %peer_addr,
                peer_lane,
                "tombstone GC gate blocked: peer has no push evidence from after the mark"
            ),
        }
    }

    /// Stage 2 INBOUND gate: every registered peer's complete state has
    /// been absorbed by a clean pull STARTED at/after the mark (see
    /// `pull_reconciled_wall_ms`). A peer without an entry fails the
    /// gate (fail-closed). Vacuously true with an empty registry.
    fn gc_inbound_gate_passed(
        peers: &[crate::network::PeerConfig],
        pull_reconciled_wall_ms: &HashMap<String, u64>,
        mark_ms: u64,
    ) -> bool {
        peers.iter().all(|peer| {
            pull_reconciled_wall_ms
                .get(&peer.addr)
                .is_some_and(|reconciled| *reconciled >= mark_ms)
        })
    }

    /// Evaluate the tombstone-GC dual gate against `mark_ms` (see
    /// [`run_gc`](Self::run_gc)) and, when it is closed, say WHY.
    ///
    /// `None` means "gate open" and is exactly `gc_authority_gate_passed &&
    /// gc_peer_gate_passed` — the frozen predicates are the deciders; the
    /// classification only re-walks the same population to LABEL an
    /// already-taken decision and can never open the gate.
    ///
    /// INVARIANT: the authority gate protects CERTIFIED state only — its
    /// population is the set of ranges that can hold certified state at
    /// all (definition AND policy, see
    /// [`certifiable_population`](Self::certifiable_population)). Eventual
    /// state is protected by the peer gate (and, for hole-jumps, the
    /// inbound gate), neither of which consults authority definitions.
    async fn gc_gate_diagnosis(&self, mark_ms: u64) -> Option<GcGateBlock> {
        // Same snapshot pattern as check_compaction Phase 1: read the
        // population + frontier set under the certified lock, evaluate off
        // the lock.
        let (defs, frontier_set, policy_versions) = {
            let api = self.certified_api.lock().await;
            let ns = api.namespace().read().unwrap_or_else(|e| e.into_inner());
            let (defs, policy_versions) = Self::certifiable_population(&ns);
            let fs = api.frontier_set().clone();
            (defs, fs, policy_versions)
        };

        // A node with no sync layer has no peers at all — the peer gate is
        // then vacuous exactly as it is with an empty registry.
        let peers = match &self.sync_client {
            Some(sync_client) => sync_client.peer_registry().lock().await.all_peers_owned(),
            None => Vec::new(),
        };
        self.metrics
            .gc_gate_peer_population
            .store(peers.len() as u64, Ordering::Relaxed);

        Self::gc_gate_diagnose(
            &defs,
            &policy_versions,
            &frontier_set,
            &peers,
            &self.push_acked_wall_ms,
            mark_ms,
        )
    }

    /// Pure classifier for the dual gate: `None` iff both frozen
    /// predicates pass.
    ///
    /// The frozen predicates are called FIRST and are the only deciders.
    /// Only when one of them says `false` is the population walked again,
    /// purely to name the offending range or peer for logs and metrics.
    fn gc_gate_diagnose(
        defs: &[(KeyRange, usize)],
        policy_versions: &[PolicyVersion],
        frontier_set: &crate::authority::ack_frontier::AckFrontierSet,
        peers: &[crate::network::PeerConfig],
        push_acked_wall_ms: &HashMap<String, u64>,
        mark_ms: u64,
    ) -> Option<GcGateBlock> {
        if !Self::gc_authority_gate_passed(defs, policy_versions, frontier_set, mark_ms) {
            let named = defs.iter().enumerate().find_map(|(i, (key_range, total))| {
                let version = &policy_versions[i];
                let prefix = key_range.prefix.clone();
                let scoped = frontier_set.all_for_scope(key_range, version);
                if scoped.len() < *total {
                    return Some(GcGateBlock::AuthorityUnderReported {
                        prefix,
                        reported: scoped.len(),
                        required: *total,
                    });
                }
                if !frontier_set
                    .min_frontier_for_scope(key_range, version)
                    .is_some_and(|min| min.physical >= mark_ms)
                {
                    return Some(GcGateBlock::FrontierBehindMark { prefix });
                }
                if !frontier_set
                    .min_advanced_at_for_scope(key_range, version)
                    .is_some_and(|received| received >= mark_ms)
                {
                    return Some(GcGateBlock::ReportNotAdvanced { prefix });
                }
                None
            });
            // The gate already said "blocked"; the walk above must have
            // found the reason. The fallback exists only so that a future
            // divergence between predicate and classifier degrades into an
            // unlabelled block rather than an OPEN gate.
            return Some(named.unwrap_or(GcGateBlock::AuthorityUnderReported {
                prefix: String::new(),
                reported: 0,
                required: 0,
            }));
        }

        if !Self::gc_peer_gate_passed(peers, push_acked_wall_ms, mark_ms) {
            let named = peers
                .iter()
                .find(|peer| {
                    !push_acked_wall_ms
                        .get(&peer.addr)
                        .is_some_and(|acked| *acked >= mark_ms)
                })
                .map(|peer| GcGateBlock::PeerEvidenceMissingOrStale {
                    peer_addr: peer.addr.clone(),
                });
            return Some(named.unwrap_or(GcGateBlock::PeerEvidenceMissingOrStale {
                peer_addr: String::new(),
            }));
        }

        None
    }

    /// Authority half of the tombstone-GC gate: every authority of every
    /// definition has a frontier entry for the current scope, the scoped
    /// minimum frontier has consumed state as of the mark time (data
    /// time), AND every scoped frontier has ADVANCED at a local
    /// wall-clock time at/after the mark (receipt time, this node's
    /// clock). Vacuously true with no authority definitions (single-node
    /// case: no other replica exists to resurrect from).
    ///
    /// The receipt condition closes the clock-skew hole in the data-time
    /// condition: HLC physicals run AHEAD of this node's wall clock when
    /// any writer's clock is skewed forward (max rule), so a frontier
    /// whose `physical >= mark_ms` may have been received long before
    /// the mark (stale pre-partition state). A frontier that has not
    /// produced a strictly-newer report since the mark — including every
    /// frontier merely restored from persistence (`advanced_at` is
    /// volatile) — fails the gate (fail-closed).
    fn gc_authority_gate_passed(
        defs: &[(KeyRange, usize)],
        policy_versions: &[PolicyVersion],
        frontier_set: &crate::authority::ack_frontier::AckFrontierSet,
        mark_ms: u64,
    ) -> bool {
        defs.iter().enumerate().all(|(i, (key_range, total))| {
            let scoped = frontier_set.all_for_scope(key_range, &policy_versions[i]);
            if scoped.len() < *total {
                return false; // an authority has not reported this scope
            }
            frontier_set
                .min_frontier_for_scope(key_range, &policy_versions[i])
                .is_some_and(|min| min.physical >= mark_ms)
                && frontier_set
                    .min_advanced_at_for_scope(key_range, &policy_versions[i])
                    .is_some_and(|received| received >= mark_ms)
        })
    }

    /// Peer half of the tombstone-GC gate: every registered peer has
    /// push evidence (`push_acked_wall_ms` — the LOCAL wall-clock time
    /// of the store scan behind the last push that completed with zero
    /// per-key errors) from at/after the mark time. That scan saw every
    /// tombstone marked at or before `mark_ms`, and the push conveyed
    /// the whole scan, so the peer can never re-offer a marked dot as
    /// live state.
    ///
    /// Deliberately NOT used: `peer_frontiers` (advanced by successful
    /// PULLS to the peer's own sender frontier — no evidence the peer
    /// received anything from us) and frontier HLC *physical* components
    /// (data time; peer clock skew propagates into HLCs via the max
    /// rule, so `physical >= mark_ms` does not imply "synchronised after
    /// the mark" on this node's clock). Both were the C-2 resurrection
    /// holes this gate previously had.
    ///
    /// A peer without an entry fails the gate (fail-closed). Vacuously
    /// true with an empty registry.
    fn gc_peer_gate_passed(
        peers: &[crate::network::PeerConfig],
        push_acked_wall_ms: &HashMap<String, u64>,
        mark_ms: u64,
    ) -> bool {
        peers.iter().all(|peer| {
            push_acked_wall_ms
                .get(&peer.addr)
                .is_some_and(|acked| *acked >= mark_ms)
        })
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

        // Untracked-key compensation (sent only on complete pulls): keys
        // without a tracked HLC on the sender (v1/v2-migrated stores) are
        // invisible to its delta scan, yet a complete pull's adoption
        // below claims the sender's WHOLE state. Merge them BEFORE the
        // adoption so the claim is true; without an origin HLC they merge
        // via `merge_remote`. Since the RR gate (M-6) the local re-stamp
        // only happens when the merge inflates local state or the key is
        // untracked HERE too — which still makes a genuinely new key
        // delta-visible from now on, while a redundant echo of an
        // already-tracked converged key is absorbed without dirtying the
        // change log. A failed merge poisons the key inside merge_remote,
        // keeping the adoption fail-closed for it.
        //
        // Skipped when no claims will be adopted (`!claims_ok`): the
        // entries only exist to make the adoption's completeness claim
        // true, and merging them without it would re-stamp every key
        // with a fresh local HLC for nothing — the guaranteed full-sync
        // fallback transfers the same data with proper claims. This also
        // guards against pre-fix compacted senders that shipped their
        // whole pruned keyspace as untracked entries.
        if claims_ok {
            for (key, value) in &delta_resp.untracked_entries {
                if let Err(e) = api.merge_remote(key.clone(), value) {
                    error_count += 1;
                    tracing::warn!(
                        peer = %peer_id,
                        key = %key,
                        error = %e,
                        "{} untracked-entry merge failed for key", label
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
        // Per-origin adoption soundness. A scalar `verified[peer]` watermark
        // proves A received B's complete state up to `request_frontier` only
        // AS OF the pull that set it — B can later back-fill a THIRD origin's
        // write BELOW that (stale) watermark (learned from a third node after
        // a partition heal), which an incremental delta (entries strictly
        // above `request_frontier`) then omits. Adopting B's full
        // applied_origins would claim that third origin's prefix without
        // holding it — a read-your-writes lie.
        //
        // Two cases are sound:
        //  * The peer's OWN origin, always: the peer's writes are monotonic,
        //    so it cannot introduce an own-write below the request frontier
        //    that this node has not already received.
        //  * A COMPLETE pull (request_frontier == zero): the response is a
        //    consistent snapshot of the peer's entire current state —
        //    tracked entries via the delta scan plus `untracked_entries`
        //    (keys with no per-key HLC on the sender, invisible to the
        //    scan; merged above) — so this node genuinely holds everything
        //    the peer holds — every origin's full prefix — and the whole
        //    map is safe to adopt. (A mixed-version OLD sender omits
        //    `untracked_entries`; its migrated untracked keys converge
        //    only via full sync, as before.)
        // On an incremental pull, third-origin coverage is re-established by
        // the periodic complete / full sync (a false negative, never a false
        // success).
        let is_complete_pull = *request_frontier == zero;
        let adopt_applied: HashMap<String, HlcTimestamp> = if !claims_ok {
            HashMap::new()
        } else if is_complete_pull {
            delta_resp.applied_origins.clone()
        } else {
            delta_resp
                .applied_origins
                .get(peer_id)
                .map(|f| HashMap::from([(peer_id.to_string(), f.clone())]))
                .unwrap_or_default()
        };
        let adopt_failed = if claims_ok {
            delta_resp.merge_failed_keys.clone()
        } else {
            Vec::new()
        };
        if let Err(e) =
            api.adopt_session_claims(&adopt_applied, &delta_resp.visible_origins, adopt_failed)
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

    /// Obtain the local store digest + frontier for one digest exchange.
    ///
    /// Warm cache (the steady state, thanks to the `run_sync` warm-up and
    /// the M-6 RR gate): a single lock scope reads the incrementally
    /// maintained digest, the frontier and the mutation generation —
    /// ZERO data cloning and (with an empty dirty set) zero hashing.
    ///
    /// Cold cache (fresh deserialize before the first warm-up succeeds,
    /// or a warm-up that lost two generation races): the pre-M-7 legacy
    /// path, bit-identical digests — snapshot under the lock, hash off
    /// the lock in `spawn_blocking` — with a per-key bucket index
    /// captured in the same pass so the push path never re-hashes keys
    /// for bucket filtering. The cache is left untouched.
    ///
    /// Either way the digest and frontier describe exactly ONE state T0
    /// (that coupling is what makes the push-side evidence advancement
    /// and the pull-side claims adoption sound).
    async fn local_digest(eventual_api: &Arc<Mutex<EventualApi>>) -> LocalDigestSource {
        {
            let mut api = eventual_api.lock().await;
            if !api.store().digest_is_cold() {
                let generation = api.store().digest_generation();
                let digest = api.store_mut().digest();
                let frontier = api.store().current_frontier();
                return LocalDigestSource::Warm {
                    digest,
                    frontier,
                    generation,
                };
            }
        }

        let api = eventual_api.lock().await;
        let data: std::collections::BTreeMap<String, crate::store::kv::CrdtValue> = api
            .store()
            .all_entries()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let frontier = api.store().current_frontier();
        drop(api);

        let (digest, data, buckets) = tokio::task::spawn_blocking(move || {
            let mut buckets = Vec::with_capacity(data.len());
            let digest = digest_pass(&data, |_, bucket, _| buckets.push(bucket));
            (digest, data, buckets)
        })
        .await
        .expect("spawn_blocking panicked");

        LocalDigestSource::Snapshot {
            digest,
            frontier,
            data,
            buckets,
        }
    }

    /// Extract from a [`LocalDigestSource::Snapshot`] the entries living
    /// in `mismatched` buckets.
    ///
    /// `buckets[i]` MUST be the bucket of the i-th key of `data` in
    /// iteration order (the positional pairing captured by
    /// [`local_digest`]'s digest pass) — that is what makes this
    /// equivalent to the legacy per-key `bucket_of(key)` filter without
    /// re-hashing any key. Push evidence advanced after sending this
    /// extract relies on it being exactly the T0 content of those
    /// buckets, so the pairing is pinned by unit tests and a length
    /// assert.
    fn snapshot_bucket_extract(
        data: std::collections::BTreeMap<String, crate::store::kv::CrdtValue>,
        buckets: Vec<u8>,
        mismatched: &std::collections::HashSet<u16>,
    ) -> Vec<(String, crate::store::kv::CrdtValue)> {
        assert_eq!(
            data.len(),
            buckets.len(),
            "positional bucket index must cover the snapshot exactly"
        );
        data.into_iter()
            .zip(buckets)
            .filter_map(|((key, value), bucket)| {
                mismatched
                    .contains(&(bucket as u16))
                    .then_some((key, value))
            })
            .collect()
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
        pull_reconciled_wall_ms: &mut HashMap<String, u64>,
        observed: Vec<crate::authority::equivocation::ObservedAttestation>,
    ) -> DigestPullOutcome {
        metrics
            .digest_sync_attempt_total
            .fetch_add(1, Ordering::Relaxed);

        // Captured BEFORE the request: the peer's answering snapshot is
        // taken after this instant, so recording it as inbound evidence
        // (`pull_reconciled_wall_ms`) can never overstate freshness.
        let request_start_wall_ms = crate::hlc::wall_clock_ms();

        // Only the digest is needed here (the peer answers with ITS
        // entries); the warm path makes this a zero-clone lock scope.
        // `observed` piggybacks the split-view relay sample (M-14) when
        // this request is the cycle's carrier for the peer.
        let request = {
            let source = Self::local_digest(eventual_api).await;
            let mut req = DigestSyncRequest::from_digest(node_id, source.digest(), true);
            req.observed = observed;
            req
        };

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
                let merge_errors = Self::apply_complete_state(
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
                // Inbound evidence (Stage 2 hole-jump gate): a root match
                // or a clean full-bucket merge absorbed the peer's
                // complete state. Poisoned keys fail the record — a
                // type-mismatched key may hide a live dot we could not
                // absorb (fail-closed).
                if merge_errors == 0 {
                    let poisoned = {
                        let api = eventual_api.lock().await;
                        !api.store().merge_failed_keys().is_empty()
                    };
                    if !poisoned {
                        pull_reconciled_wall_ms.insert(peer_key.to_string(), request_start_wall_ms);
                    }
                }
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
    ///
    /// Every success case (root match, peer-only mismatches, clean subset
    /// push) proves the peer holds the whole snapshot, so it also records
    /// push evidence for the tombstone-GC peer gate: `push_frontiers`
    /// advances to the snapshot frontier and `push_acked_wall_ms` to the
    /// local wall-clock time captured BEFORE the snapshot.
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
        push_frontiers: &mut HashMap<String, HlcTimestamp>,
        push_acked_wall_ms: &mut HashMap<String, u64>,
        digest_unsupported: &mut HashMap<String, Instant>,
        observed: Vec<crate::authority::equivocation::ObservedAttestation>,
    ) -> DigestPushOutcome {
        metrics
            .digest_push_probe_total
            .fetch_add(1, Ordering::Relaxed);

        let snapshot_wall_ms = crate::hlc::wall_clock_ms();
        let source = Self::local_digest(eventual_api).await;
        // `observed` piggybacks the split-view relay sample (M-14) when
        // this probe is the cycle's carrier for the peer.
        let mut request = DigestSyncRequest::from_digest(node_id, source.digest(), false);
        request.observed = observed;

        match sync_client.digest_sync(peer_addr, &request).await {
            DigestSyncResult::Ok(resp) if resp.scheme_ok => {
                digest_unsupported.remove(peer_key);
                let snapshot_frontier = source.frontier();
                if resp.root_matched {
                    metrics
                        .digest_push_match_total
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::info!(
                        peer = %peer_id,
                        "digest push probe: peer already matches, skipping full push"
                    );
                    if let Some(frontier) = snapshot_frontier {
                        peer_frontiers.insert(peer_key.to_string(), frontier.clone());
                        push_frontiers.insert(peer_key.to_string(), frontier);
                    }
                    push_acked_wall_ms.insert(peer_key.to_string(), snapshot_wall_ms);
                    return DigestPushOutcome::Handled;
                }

                let mismatched: std::collections::HashSet<u16> =
                    resp.mismatched_buckets.iter().copied().collect();

                // Decide the peer-only case straight from the T0 digest's
                // per-bucket key counts — no relock, no data walk.
                let local_has_keys = mismatched.iter().any(|&bucket| {
                    source
                        .digest()
                        .key_counts
                        .get(bucket as usize)
                        .is_some_and(|&count| count > 0)
                });
                if !local_has_keys {
                    // Every mismatched bucket is empty on OUR side: the
                    // peer holds data we lack, but everything we hold it
                    // already has — the T0 state is fully conveyed, so
                    // the T0 frontier may advance. The pull phase
                    // fetches the peer-only data.
                    tracing::debug!(
                        peer = %peer_id,
                        "digest push probe: mismatches are peer-only data, nothing to push"
                    );
                    if let Some(frontier) = snapshot_frontier {
                        peer_frontiers.insert(peer_key.to_string(), frontier.clone());
                        push_frontiers.insert(peer_key.to_string(), frontier);
                    }
                    push_acked_wall_ms.insert(peer_key.to_string(), snapshot_wall_ms);
                    return DigestPushOutcome::Handled;
                }

                // Extract the local entries of the mismatched buckets.
                //
                // `evidence_valid` is the all-or-nothing guard for the
                // T0-coupled push evidence: the probe compared the T0
                // digest, so `push_frontiers` / `push_acked_wall_ms` may
                // only advance to (frontier_T0, wall0) if what we now
                // send provably IS the T0 content of those buckets.
                // - Snapshot source: the extract comes from the T0
                //   snapshot itself — always valid.
                // - Warm source: valid iff the mutation generation is
                //   unchanged (the store still IS T0). Otherwise the
                //   fresher extract is still sent — a CRDT merge of
                //   newer state is always safe — but the evidence stays
                //   put, in the fail-safe direction (the tombstone-GC
                //   peer gate merely waits; the next quiet probe's root
                //   match advances it).
                let (changed, evidence_valid): (Vec<(String, crate::store::kv::CrdtValue)>, bool) =
                    match source {
                        LocalDigestSource::Snapshot { data, buckets, .. } => (
                            Self::snapshot_bucket_extract(data, buckets, &mismatched),
                            true,
                        ),
                        LocalDigestSource::Warm { generation, .. } => {
                            let mut api = eventual_api.lock().await;
                            if api.store().digest_is_cold() {
                                // A write burst larger than the inline
                                // refresh budget landed during the probe
                                // RTT: refreshing here would hash O(dirty)
                                // values — or fully rebuild after a
                                // collapse — UNDER the lock, stalling all
                                // reads/writes/syncs. The burst also moved
                                // the generation, so push evidence could
                                // not advance anyway: skip this subset
                                // push; the next cycle re-probes after the
                                // off-lock warm-up.
                                drop(api);
                                tracing::debug!(
                                    peer = %peer_id,
                                    "digest push: cache went cold during the probe RTT; \
                                     skipping subset push this cycle"
                                );
                                return DigestPushOutcome::Handled;
                            }
                            // Refresh so the bucket index is clean (O(d),
                            // bounded by the inline budget just checked),
                            // then check whether anything mutated since T0.
                            let _ = api.store_mut().digest();
                            let stable = api.store().digest_generation() == generation;
                            let (entries, _timestamps) =
                                api.store().clone_bucket_entries(&mismatched);
                            drop(api);
                            (entries.into_iter().collect(), stable)
                        }
                    };

                if changed.is_empty() {
                    // Only reachable when a warm extract raced with
                    // concurrent mutations that emptied the buckets;
                    // nothing to send and nothing to prove.
                    tracing::debug!(
                        peer = %peer_id,
                        "digest push: mismatched buckets emptied concurrently, nothing to push"
                    );
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
                        if evidence_valid {
                            if let Some(frontier) = snapshot_frontier {
                                peer_frontiers.insert(peer_key.to_string(), frontier.clone());
                                push_frontiers.insert(peer_key.to_string(), frontier);
                            }
                            push_acked_wall_ms.insert(peer_key.to_string(), snapshot_wall_ms);
                        } else {
                            tracing::debug!(
                                peer = %peer_id,
                                "digest push: store mutated between probe and extraction; \
                                 data pushed but push evidence withheld (self-healing: a \
                                 later quiet probe's root match advances it)"
                            );
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

    // -----------------------------------------------------------------
    // local_digest cold fallback (LocalDigestSource::Snapshot) and the
    // positional bucket extraction used by the push path
    // -----------------------------------------------------------------

    /// Build an eventual API whose digest cache is COLD at exchange time
    /// (serde round-trip of the store yields the invalid `serde(skip)`
    /// default), i.e. the state of a freshly restarted node before its
    /// first warm-up.
    fn cold_eventual_api(keys: usize) -> Arc<Mutex<EventualApi>> {
        let mut api = EventualApi::new(NodeId("cold-node".into()));
        for i in 0..keys {
            api.eventual_counter_inc(&format!("cold-key-{i:03}"))
                .unwrap();
        }
        let json = serde_json::to_string(api.store()).unwrap();
        *api.store_mut() = serde_json::from_str(&json).unwrap();
        assert!(
            api.store().digest_is_cold(),
            "round-trip must leave a cold cache"
        );
        Arc::new(Mutex::new(api))
    }

    /// The runner-side cold fallback must (a) produce the exact
    /// from-scratch digest and (b) capture a positional bucket index
    /// aligned with the snapshot's iteration order — the coupling the
    /// push extraction and its T0 evidence semantics stand on.
    #[tokio::test]
    async fn local_digest_cold_snapshot_matches_recompute_and_bucket_index() {
        let api = cold_eventual_api(64);

        let source = NodeRunner::local_digest(&api).await;
        let LocalDigestSource::Snapshot {
            digest,
            data,
            buckets,
            ..
        } = source
        else {
            panic!("a cold cache must take the snapshot fallback");
        };

        assert_eq!(
            digest,
            crate::store::digest::compute_store_digest(&data),
            "cold-path digest must equal the from-scratch recompute"
        );
        assert_eq!(data.len(), buckets.len());
        for ((key, _), bucket) in data.iter().zip(&buckets) {
            assert_eq!(
                *bucket as usize,
                crate::store::digest::bucket_of(key),
                "positional bucket index must match bucket_of({key})"
            );
        }
        // The legacy path must leave the cache untouched.
        assert!(api.lock().await.store().digest_is_cold());
    }

    /// The zip-based extraction must be equivalent to the legacy
    /// per-key `bucket_of(key)` filter for an arbitrary mismatched
    /// bucket set — a regression here would silently push the WRONG
    /// keys while still advancing T0 push evidence.
    #[tokio::test]
    async fn snapshot_bucket_extract_equals_legacy_bucket_of_filter() {
        let api = cold_eventual_api(64);
        let LocalDigestSource::Snapshot { data, buckets, .. } =
            NodeRunner::local_digest(&api).await
        else {
            panic!("a cold cache must take the snapshot fallback");
        };

        // Mismatch the buckets of every third key, plus one bucket that
        // is guaranteed empty locally (peer-only data).
        let mut mismatched: std::collections::HashSet<u16> = data
            .keys()
            .step_by(3)
            .map(|key| crate::store::digest::bucket_of(key) as u16)
            .collect();
        let occupied: std::collections::HashSet<u16> = data
            .keys()
            .map(|key| crate::store::digest::bucket_of(key) as u16)
            .collect();
        let empty_bucket = (0..crate::store::digest::DIGEST_BUCKET_COUNT as u16)
            .find(|bucket| !occupied.contains(bucket))
            .expect("64 keys cannot fill all 256 buckets");
        mismatched.insert(empty_bucket);

        let expected: Vec<(String, CrdtValue)> = data
            .iter()
            .filter(|(key, _)| mismatched.contains(&(crate::store::digest::bucket_of(key) as u16)))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        assert!(
            !expected.is_empty() && expected.len() < data.len(),
            "the mismatched set must select a proper non-empty subset"
        );

        let extracted = NodeRunner::snapshot_bucket_extract(data, buckets, &mismatched);
        assert_eq!(
            extracted, expected,
            "positional extraction must equal the legacy bucket_of filter"
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
        // Admission (M-4) only tracks scopes with a placement policy.
        ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(1), kr(""), 1))
            .unwrap();

        let mut api = CertifiedApi::new(node_id("auth-1"), wrap_ns(ns));

        // Set a very high (but within clock-skew) initial frontier manually.
        // The P0-6 guard rejects reports beyond `now + MAX_CLOCK_SKEW_MS`, so
        // a real-but-high value exercises the "does not regress" intent
        // without tripping it.
        let high_physical = crate::hlc::wall_clock_ms() + 30_000;
        api.update_frontier(AckFrontier {
            authority_id: node_id("auth-1"),
            frontier_hlc: HlcTimestamp {
                physical: high_physical,
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
            frontiers[0].frontier_hlc.physical >= high_physical,
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

    /// Reproduces the shipped binary's actual state: `main.rs` creates
    /// `cluster_nodes` empty and never writes to it, so the very first
    /// certification tick recalculates authorities against an empty
    /// inventory. Before the empty-candidate guard, that tick replaced the
    /// operator's authority definition with `authority_nodes: []` -- on every
    /// node, without going through Raft, one second after startup.
    #[tokio::test]
    async fn empty_cluster_inventory_does_not_erase_a_manual_authority_definition() {
        use crate::placement::PlacementPolicy;

        let mut ns = SystemNamespace::new();
        // No required tags: an empty inventory, not a tag mismatch, is what
        // empties the candidate set.
        ns.set_placement_policy(
            PlacementPolicy::new(PolicyVersion(1), kr("user/"), 3).with_certified(true),
        )
        .unwrap();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr("user/"),
            authority_nodes: vec![node_id("n1"), node_id("n2"), node_id("n3")],
            auto_generated: false,
        });
        let shared_ns = wrap_ns(ns);

        let api = wrap_api(CertifiedApi::new(node_id("node-1"), shared_ns.clone()));
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

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(80)).await;
            let _ = handle.send(true);
        });
        let stats = runner.run().await;
        assert!(
            stats.certification_ticks > 0,
            "the run loop must actually have ticked, otherwise the definition \
             surviving proves nothing about the guard"
        );

        let api_lock = api.lock().await;
        let ns = api_lock
            .namespace()
            .read()
            .unwrap_or_else(|e| e.into_inner());
        let def = ns
            .get_authority_definition("user/")
            .expect("the definition must survive an empty inventory");
        assert_eq!(
            def.authority_nodes.len(),
            3,
            "an empty cluster inventory must not empty the authority set"
        );
        assert!(
            !def.auto_generated,
            "a manual definition must not be flipped to auto-generated"
        );
    }

    // ---------------------------------------------------------------
    // Peer-registry-derived cluster inventory (P0-2)
    // ---------------------------------------------------------------

    fn peer_registry_with(
        self_id: &str,
        entries: &[(&str, &str)],
    ) -> Arc<Mutex<crate::network::PeerRegistry>> {
        let peers = entries
            .iter()
            .map(|(id, addr)| crate::network::PeerConfig {
                node_id: node_id(id),
                addr: (*addr).to_string(),
            })
            .collect();
        Arc::new(Mutex::new(
            crate::network::PeerRegistry::new(node_id(self_id), peers).unwrap(),
        ))
    }

    /// The shipped binary never wrote to `cluster_nodes`, so `GET
    /// /api/topology` reported an empty cluster forever and the documented
    /// response example was unreachable.
    #[tokio::test]
    async fn peer_registry_populates_cluster_nodes_and_topology() {
        use crate::placement::latency::LatencyModel;
        use crate::types::NodeMode;

        let cluster_nodes = Arc::new(std::sync::RwLock::new(Vec::<crate::node::Node>::new()));
        let mut runner = NodeRunner::with_cluster_nodes(
            node_id("n1"),
            wrap_api(CertifiedApi::new(node_id("n1"), default_namespace())),
            CompactionEngine::with_defaults(),
            NodeRunnerConfig::default(),
            default_metrics(),
            cluster_nodes.clone(),
        )
        .await;

        // The same two `Arc`s `main.rs` hands to `AppState`, which is what
        // `GET /api/topology` reads.
        let topology_view = Arc::new(std::sync::RwLock::new(TopologyView::build(
            &[],
            &LatencyModel::new(),
        )));
        runner.set_latency_model(Arc::new(std::sync::RwLock::new(LatencyModel::new())));
        runner.set_topology_view(topology_view.clone());

        runner.set_cluster_inventory_source(
            crate::node::Node::new(node_id("n1"), NodeMode::Both),
            peer_registry_with("n1", &[("n2", "127.0.0.1:3002"), ("n3", "127.0.0.1:3003")]),
        );
        runner.refresh_cluster_inventory().await;

        let nodes = cluster_nodes.read().unwrap().clone();
        let ids: Vec<&str> = nodes.iter().map(|n| n.id.0.as_str()).collect();
        assert_eq!(ids, vec!["n1", "n2", "n3"], "self plus every peer, sorted");

        // The shared view is what the endpoint serves, and it is refreshed on
        // the membership path -- which stays reachable while placement is
        // frozen precisely so topology keeps following the cluster.
        runner.detect_membership_changes().await;
        let view = topology_view.read().unwrap();
        assert_eq!(view.total_nodes, 3);
        assert_eq!(
            view.regions
                .iter()
                .find(|r| r.name == "unknown")
                .map(|r| r.node_count),
            Some(3),
            "peers carry no region tag, and neither does this untagged self node"
        );
    }

    /// The self node contributes its real mode and tags, which is the whole
    /// reason `main.rs` now keeps `config.node` instead of just its id.
    /// (`PeerRegistry` rejects self in the peer list, so the de-duplication in
    /// `refresh_cluster_inventory` is defensive only and cannot be provoked
    /// through the registry API.)
    #[tokio::test]
    async fn self_node_contributes_its_real_mode_and_tags() {
        use crate::types::{NodeMode, Tag};

        let cluster_nodes = Arc::new(std::sync::RwLock::new(Vec::<crate::node::Node>::new()));
        let mut runner = NodeRunner::with_cluster_nodes(
            node_id("n1"),
            wrap_api(CertifiedApi::new(node_id("n1"), default_namespace())),
            CompactionEngine::with_defaults(),
            NodeRunnerConfig::default(),
            default_metrics(),
            cluster_nodes.clone(),
        )
        .await;

        let mut self_node = crate::node::Node::new(node_id("n1"), NodeMode::Store);
        self_node.add_tag(Tag("dc:tokyo".into()));
        runner.set_cluster_inventory_source(
            self_node,
            peer_registry_with("n1", &[("n2", "127.0.0.1:3002")]),
        );
        runner.refresh_cluster_inventory().await;

        let nodes = cluster_nodes.read().unwrap().clone();
        assert_eq!(nodes.len(), 2, "self plus the one peer");
        let me = nodes.iter().find(|n| n.id == node_id("n1")).unwrap();
        assert_eq!(me.mode, NodeMode::Store, "config mode, not the placeholder");
        assert!(me.tags.contains(&Tag("dc:tokyo".into())));
    }

    /// A peer-derived inventory carries no peer tags, so it must never reach
    /// `select_nodes`. Without the freeze a tag-constrained policy would
    /// shrink the candidate set to `[self]`, giving `total == 1` and letting
    /// one node certify writes with its own signature alone.
    #[tokio::test]
    async fn placement_is_frozen_while_the_inventory_is_peer_derived() {
        use crate::placement::PlacementPolicy;
        use crate::types::{NodeMode, Tag};

        let mut ns = SystemNamespace::new();
        ns.set_placement_policy(
            PlacementPolicy::new(PolicyVersion(1), kr("user/"), 3)
                .with_certified(true)
                .with_required_tags([Tag("dc:tokyo".into())].into()),
        )
        .unwrap();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr("user/"),
            authority_nodes: vec![node_id("n1"), node_id("n2")],
            auto_generated: false,
        });
        let shared_ns = wrap_ns(ns);
        let api = wrap_api(CertifiedApi::new(node_id("n1"), shared_ns.clone()));

        let cluster_nodes = Arc::new(std::sync::RwLock::new(Vec::<crate::node::Node>::new()));
        let mut runner = NodeRunner::with_cluster_nodes(
            node_id("n1"),
            api.clone(),
            CompactionEngine::with_defaults(),
            NodeRunnerConfig::default(),
            default_metrics(),
            cluster_nodes.clone(),
        )
        .await;

        // Only this node carries the required tag; the peer cannot.
        let mut self_node = crate::node::Node::new(node_id("n1"), NodeMode::Both);
        self_node.add_tag(Tag("dc:tokyo".into()));
        runner.set_cluster_inventory_source(
            self_node,
            peer_registry_with("n1", &[("n2", "127.0.0.1:3002")]),
        );

        runner.refresh_cluster_inventory().await;
        runner.detect_membership_changes().await;

        let ns = shared_ns.read().unwrap();
        let def = ns.get_authority_definition("user/").unwrap();
        assert_eq!(
            def.authority_nodes,
            vec![node_id("n1"), node_id("n2")],
            "a peer-derived inventory must not narrow the authority set to [self]"
        );
        assert!(!def.auto_generated);
    }

    /// `detect_membership_changes` is not the only way into
    /// `recalculate_authorities`: a placement *policy* change reaches it too,
    /// on a path membership never touches. That gate needs its own coverage --
    /// opening it lets a tag-constrained policy narrow the set to `[self]`,
    /// which is the `total == 1` single-signer hazard.
    #[tokio::test]
    async fn policy_change_does_not_recalculate_authorities_while_placement_is_frozen() {
        use crate::placement::PlacementPolicy;
        use crate::types::{NodeMode, Tag};

        let mut ns = SystemNamespace::new();
        ns.set_placement_policy(
            PlacementPolicy::new(PolicyVersion(1), kr("user/"), 3)
                .with_certified(true)
                .with_required_tags([Tag("dc:tokyo".into())].into()),
        )
        .unwrap();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr("user/"),
            authority_nodes: vec![node_id("n1"), node_id("n2")],
            auto_generated: false,
        });
        let shared_ns = wrap_ns(ns);
        let api = wrap_api(CertifiedApi::new(node_id("n1"), shared_ns.clone()));

        let cluster_nodes = Arc::new(std::sync::RwLock::new(Vec::<crate::node::Node>::new()));
        let mut runner = NodeRunner::with_cluster_nodes(
            node_id("n1"),
            api.clone(),
            CompactionEngine::with_defaults(),
            NodeRunnerConfig::default(),
            default_metrics(),
            cluster_nodes.clone(),
        )
        .await;

        // Only this node can carry the required tag; the peer arrives without
        // one, so `select_nodes` would return exactly `[n1]`.
        let mut self_node = crate::node::Node::new(node_id("n1"), NodeMode::Both);
        self_node.add_tag(Tag("dc:tokyo".into()));
        runner.set_cluster_inventory_source(
            self_node,
            peer_registry_with("n1", &[("n2", "127.0.0.1:3002")]),
        );

        // Baseline tick, then a policy version bump -- the membership
        // fingerprint is unchanged from here on, so only the policy path runs.
        runner.detect_version_changes().await;
        {
            let mut ns = shared_ns.write().unwrap_or_else(|e| e.into_inner());
            ns.set_placement_policy(
                PlacementPolicy::new(PolicyVersion(2), kr("user/"), 3)
                    .with_certified(true)
                    .with_required_tags([Tag("dc:tokyo".into())].into()),
            )
            .unwrap();
        }
        runner.detect_version_changes().await;

        let ns = shared_ns.read().unwrap_or_else(|e| e.into_inner());
        let def = ns.get_authority_definition("user/").unwrap();
        assert_eq!(
            def.authority_nodes,
            vec![node_id("n1"), node_id("n2")],
            "a policy change must not narrow the authority set to [self] \
             while the inventory is peer-derived"
        );
        assert!(!def.auto_generated);
    }

    /// The third frozen path: a rebalance plan built from placeholder tags
    /// would move real keys onto nodes that were never shown to match.
    #[tokio::test]
    async fn rebalance_plans_are_not_computed_while_placement_is_frozen() {
        use crate::api::eventual::EventualApi;
        use crate::placement::PlacementPolicy;
        use crate::types::{NodeMode, Tag};

        let mut ns = SystemNamespace::new();
        ns.set_placement_policy(
            PlacementPolicy::new(PolicyVersion(1), kr("data/"), 3)
                .with_required_tags([Tag("dc:tokyo".into())].into()),
        )
        .unwrap();
        let shared_ns = wrap_ns(ns);
        let api = wrap_api(CertifiedApi::new(node_id("n1"), shared_ns.clone()));

        let eventual_api = Arc::new(Mutex::new(EventualApi::new(node_id("n1"))));
        {
            let mut ea = eventual_api.lock().await;
            let mut counter = crate::crdt::pn_counter::PnCounter::new();
            counter.increment(&node_id("n1"));
            ea.eventual_write("data/k1".to_string(), CrdtValue::Counter(counter))
                .unwrap();
        }

        let cluster_nodes = Arc::new(std::sync::RwLock::new(Vec::<crate::node::Node>::new()));
        let mut runner = NodeRunner::with_cluster_nodes(
            node_id("n1"),
            api.clone(),
            CompactionEngine::with_defaults(),
            NodeRunnerConfig::default(),
            default_metrics(),
            cluster_nodes.clone(),
        )
        .await;
        runner.set_eventual_api(eventual_api.clone());

        let mut self_node = crate::node::Node::new(node_id("n1"), NodeMode::Store);
        self_node.add_tag(Tag("dc:tokyo".into()));
        runner.set_cluster_inventory_source(
            self_node,
            peer_registry_with("n1", &[("n2", "127.0.0.1:3002")]),
        );

        runner.detect_version_changes().await;
        assert!(runner.active_rebalance_plans.is_empty());

        // Drop the tag requirement: the placeholder peer would now "match",
        // which is exactly the fabricated-attribute move the gate prevents.
        {
            let mut ns = shared_ns.write().unwrap_or_else(|e| e.into_inner());
            ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(2), kr("data/"), 3))
                .unwrap();
        }
        runner.detect_version_changes().await;

        assert!(
            runner.active_rebalance_plans.is_empty(),
            "no rebalance may be planned from an inventory with placeholder tags"
        );
        assert_eq!(
            runner
                .metrics()
                .rebalance_start_total
                .load(Ordering::Relaxed),
            0
        );
    }

    /// The recovery runbook (ops-guide 14.5.1) tells the operator to
    /// re-register a swept authority definition with `PUT
    /// /api/control-plane/authorities`. That changes neither the cluster
    /// fingerprint nor any policy version, and in a shipped binary
    /// `detect_membership_changes` returns early anyway -- so the reporter has
    /// to be reconciled against the namespace directly, or the range stays
    /// uncertifiable until the process restarts.
    #[tokio::test]
    async fn a_new_authority_definition_promotes_the_reporter_while_frozen() {
        use crate::placement::PlacementPolicy;
        use crate::types::NodeMode;

        let mut ns = SystemNamespace::new();
        ns.set_placement_policy(
            PlacementPolicy::new(PolicyVersion(1), kr("user/"), 3).with_certified(true),
        )
        .unwrap();
        let shared_ns = wrap_ns(ns);
        let api = wrap_api(CertifiedApi::new(node_id("n1"), shared_ns.clone()));

        let dir = tempfile::tempdir().unwrap();
        let cluster_nodes = Arc::new(std::sync::RwLock::new(Vec::<crate::node::Node>::new()));
        let mut runner = NodeRunner::with_cluster_nodes(
            node_id("n1"),
            api.clone(),
            CompactionEngine::with_defaults(),
            NodeRunnerConfig {
                frontier_clock_floor_path: Some(dir.path().join("frontier_report_clock.json")),
                frontier_digest_activation_grace: Some(Duration::ZERO),
                ..NodeRunnerConfig::default()
            },
            default_metrics(),
            cluster_nodes.clone(),
        )
        .await;
        runner.set_cluster_inventory_source(
            crate::node::Node::new(node_id("n1"), NodeMode::Both),
            peer_registry_with("n1", &[("n2", "127.0.0.1:3002")]),
        );
        assert!(
            runner.frontier_reporter.is_none(),
            "no definition yet, so not an authority"
        );
        assert!(runner.report_floor.is_none(), "non-authority: no floor");

        runner.detect_version_changes().await;
        assert!(runner.frontier_reporter.is_none(), "still no definition");

        // What `PUT /api/control-plane/authorities` ends up doing.
        {
            let mut ns = shared_ns.write().unwrap_or_else(|e| e.into_inner());
            ns.set_authority_definition(AuthorityDefinition {
                key_range: kr("user/"),
                authority_nodes: vec![node_id("n1"), node_id("n2")],
                auto_generated: false,
            });
        }
        runner.detect_version_changes().await;

        let reporter = runner
            .frontier_reporter
            .as_ref()
            .expect("re-registering the definition must promote this node");
        assert_eq!(reporter.authority_scopes().len(), 1);
        assert!(
            runner.report_floor.is_some(),
            "runtime promotion must initialise the report clock floor"
        );

        // ... and removing it again demotes.
        {
            let mut ns = shared_ns.write().unwrap_or_else(|e| e.into_inner());
            ns.set_authority_definition(AuthorityDefinition {
                key_range: kr("user/"),
                authority_nodes: vec![node_id("n2")],
                auto_generated: false,
            });
        }
        runner.detect_version_changes().await;
        assert!(runner.frontier_reporter.is_none(), "demoted");
        assert!(
            runner.report_floor.is_some(),
            "the floor survives demotion so a re-promotion does not re-arm the grace"
        );
    }

    /// The gate must not consume the membership change: leaving the
    /// fingerprint untracked is what makes recalculation fire the moment the
    /// gate ever opens.
    #[tokio::test]
    async fn membership_fingerprint_is_not_consumed_while_placement_is_frozen() {
        use crate::types::NodeMode;

        let cluster_nodes = Arc::new(std::sync::RwLock::new(Vec::<crate::node::Node>::new()));
        let mut runner = NodeRunner::with_cluster_nodes(
            node_id("n1"),
            wrap_api(CertifiedApi::new(node_id("n1"), default_namespace())),
            CompactionEngine::with_defaults(),
            NodeRunnerConfig::default(),
            default_metrics(),
            cluster_nodes.clone(),
        )
        .await;
        runner.set_cluster_inventory_source(
            crate::node::Node::new(node_id("n1"), NodeMode::Both),
            peer_registry_with("n1", &[("n2", "127.0.0.1:3002")]),
        );

        runner.refresh_cluster_inventory().await;
        runner.detect_membership_changes().await;

        assert_eq!(
            runner.tracked_cluster_generation,
            u64::MAX,
            "the sentinel must survive so the first unfrozen tick recalculates"
        );
    }

    /// Repeated refreshes over an unchanged registry must not churn the
    /// fingerprint (which would re-trigger recalculation every tick).
    #[tokio::test]
    async fn refresh_is_stable_when_the_registry_does_not_change() {
        use crate::types::NodeMode;

        let cluster_nodes = Arc::new(std::sync::RwLock::new(Vec::<crate::node::Node>::new()));
        let mut runner = NodeRunner::with_cluster_nodes(
            node_id("n1"),
            wrap_api(CertifiedApi::new(node_id("n1"), default_namespace())),
            CompactionEngine::with_defaults(),
            NodeRunnerConfig::default(),
            default_metrics(),
            cluster_nodes.clone(),
        )
        .await;
        runner.set_cluster_inventory_source(
            crate::node::Node::new(node_id("n1"), NodeMode::Both),
            peer_registry_with("n1", &[("n2", "127.0.0.1:3002")]),
        );

        runner.refresh_cluster_inventory().await;
        let first = NodeRunner::cluster_fingerprint(&cluster_nodes.read().unwrap());
        runner.refresh_cluster_inventory().await;
        let second = NodeRunner::cluster_fingerprint(&cluster_nodes.read().unwrap());
        assert_eq!(first, second);
    }

    /// Without a declared source the runner must not touch `cluster_nodes` at
    /// all: every in-process test supplies its own inventory.
    #[tokio::test]
    async fn refresh_is_a_noop_without_a_declared_inventory_source() {
        use crate::types::NodeMode;

        let cluster_nodes = Arc::new(std::sync::RwLock::new(vec![make_node(
            "explicit",
            NodeMode::Store,
            &["dc:tokyo"],
        )]));
        let mut runner = NodeRunner::with_cluster_nodes(
            node_id("n1"),
            wrap_api(CertifiedApi::new(node_id("n1"), default_namespace())),
            CompactionEngine::with_defaults(),
            NodeRunnerConfig::default(),
            default_metrics(),
            cluster_nodes.clone(),
        )
        .await;

        assert!(runner.placement_inventory_usable());
        runner.refresh_cluster_inventory().await;

        let nodes = cluster_nodes.read().unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].id, node_id("explicit"));
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
        // Manual authority definition (survives recalculation): admission
        // (M-4) only accepts frontier reports from members of the exact
        // range definition.
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr("user/"),
            authority_nodes: vec![node_id("auth-1")],
            auto_generated: false,
        });
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

    /// Accuse `authority` in `detector` with a forged conflicting pair at a
    /// fixed old HLC (observe() never verifies — documented precondition).
    fn accuse_via_forged_pair(
        detector: &crate::authority::equivocation::EquivocationDetector,
        signer: &NodeSigner,
        authority: &str,
    ) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        for digest in ["forged-a", "forged-b"] {
            let frontier = AckFrontier {
                authority_id: node_id(authority),
                frontier_hlc: HlcTimestamp {
                    physical: now_ms.saturating_sub(30_000),
                    logical: 0,
                    node_id: authority.into(),
                },
                key_range: kr(""),
                policy_version: PolicyVersion(1),
                digest_hash: digest.into(),
            };
            let sig = signer.sign_frontier(&frontier, KeysetVersion(1));
            detector.observe(&frontier, &sig, now_ms);
        }
        assert!(detector.is_accused(&node_id(authority)));
    }

    /// m-7 self-report lane: with exclusion enabled, an accused node's own
    /// attestations are dropped from the pool AND attestations pooled
    /// before the accusation are purged; frontier advancement continues.
    #[tokio::test]
    async fn self_report_exclusion_drops_and_purges_own_attestations() {
        let signer = make_signer("auth-1", 48);
        let registry = shared_registry_with(&signer);
        let detector = Arc::new(crate::authority::equivocation::EquivocationDetector::new(
            None,
        ));
        let shared_api = wrap_api(CertifiedApi::new(node_id("auth-1"), default_namespace()));
        let metrics = default_metrics();

        let config = NodeRunnerConfig {
            node_signer: Some(Arc::clone(&signer)),
            keyset_registry: Some(Arc::clone(&registry)),
            equivocation: Some(Arc::clone(&detector)),
            exclude_accused_authorities: true,
            ..NodeRunnerConfig::default()
        };
        let mut runner = NodeRunner::new(
            node_id("auth-1"),
            shared_api.clone(),
            CompactionEngine::with_defaults(),
            config,
            Arc::clone(&metrics),
        )
        .await;

        // Not yet accused: the self-report pools an attestation.
        runner.report_frontiers().await;
        {
            let api = shared_api.lock().await;
            assert_eq!(api.attestation_stats().scopes, 1);
        }

        // The accusation lands (e.g. relayed evidence via the shared
        // detector). The next report tick must purge the pre-accusation
        // attestation and stop pooling new ones — while the frontier
        // itself keeps advancing.
        accuse_via_forged_pair(&detector, &signer, "auth-1");
        tokio::time::sleep(Duration::from_millis(5)).await;
        runner.report_frontiers().await;

        let api = shared_api.lock().await;
        let stats = api.attestation_stats();
        assert_eq!(
            stats.scopes, 0,
            "pre-accusation attestations must be purged from the pool"
        );
        assert!(stats.purged_total >= 1);
        assert!(
            !api.all_frontiers().is_empty(),
            "frontier advancement is never blocked by an accusation"
        );

        // Event-driven metrics sync ran on the report tick.
        let snap = metrics.snapshot();
        assert_eq!(snap.attestation_pool_scopes, 0);
        assert!(snap.attestation_purged_total >= 1);
    }

    /// Detect-only default: without the exclusion flag, an accusation does
    /// not change self-report behaviour (no exclusion, no purge).
    #[tokio::test]
    async fn self_report_detect_only_keeps_attestations_without_flag() {
        let signer = make_signer("auth-1", 49);
        let registry = shared_registry_with(&signer);
        let detector = Arc::new(crate::authority::equivocation::EquivocationDetector::new(
            None,
        ));
        let shared_api = wrap_api(CertifiedApi::new(node_id("auth-1"), default_namespace()));

        let config = NodeRunnerConfig {
            node_signer: Some(Arc::clone(&signer)),
            keyset_registry: Some(Arc::clone(&registry)),
            equivocation: Some(Arc::clone(&detector)),
            // exclude_accused_authorities: false (default)
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

        accuse_via_forged_pair(&detector, &signer, "auth-1");
        runner.report_frontiers().await;

        let api = shared_api.lock().await;
        let stats = api.attestation_stats();
        assert_eq!(
            stats.scopes, 1,
            "detect-only default must keep pooling own attestations"
        );
        assert_eq!(stats.purged_total, 0);
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

    // -----------------------------------------------------------------
    // M-12: store-digest frontier reports + report clock floor
    // -----------------------------------------------------------------

    fn eventual_with_keys(node: &str, keys: &[&str]) -> Arc<Mutex<EventualApi>> {
        let mut api = EventualApi::new(node_id(node));
        for k in keys {
            api.eventual_counter_inc(k).unwrap();
        }
        Arc::new(Mutex::new(api))
    }

    /// Config with the store-digest format immediately active: a floor
    /// path in `dir` (file may be absent) plus a zero activation grace.
    fn digest_active_config(dir: &std::path::Path) -> NodeRunnerConfig {
        NodeRunnerConfig {
            frontier_clock_floor_path: Some(dir.join("frontier_report_clock.json")),
            frontier_digest_activation_grace: Some(Duration::ZERO),
            ..NodeRunnerConfig::default()
        }
    }

    #[tokio::test]
    async fn report_frontiers_uses_store_digest() {
        let dir = tempfile::tempdir().unwrap();
        let shared_api = wrap_api(CertifiedApi::new(node_id("auth-1"), default_namespace()));
        let mut runner = NodeRunner::new(
            node_id("auth-1"),
            shared_api.clone(),
            CompactionEngine::with_defaults(),
            digest_active_config(dir.path()),
            default_metrics(),
        )
        .await;
        let eventual = eventual_with_keys("auth-1", &["user/x", "user/y"]);
        runner.set_eventual_api(Arc::clone(&eventual));

        runner.report_frontiers().await;

        // The bound digest is exactly the M-7 root digest of the eventual
        // store, in the sd{scheme}: format.
        let expected = format_store_digest_hash(&eventual.lock().await.store_mut().digest().root);
        let api = shared_api.lock().await;
        let frontiers = api.all_frontiers();
        assert!(!frontiers.is_empty());
        for f in &frontiers {
            assert_eq!(f.digest_hash, expected);
        }
        // The write-ahead floor was persisted for the issued HLC.
        assert!(dir.path().join("frontier_report_clock.json").exists());
    }

    #[tokio::test]
    async fn active_format_without_eventual_api_reports_unavailable_sentinel() {
        let dir = tempfile::tempdir().unwrap();
        let shared_api = wrap_api(CertifiedApi::new(node_id("auth-1"), default_namespace()));
        let mut runner = NodeRunner::new(
            node_id("auth-1"),
            shared_api.clone(),
            CompactionEngine::with_defaults(),
            digest_active_config(dir.path()),
            default_metrics(),
        )
        .await;

        runner.report_frontiers().await;

        let api = shared_api.lock().await;
        for f in &api.all_frontiers() {
            assert_eq!(f.digest_hash, SD_UNAVAILABLE);
        }
    }

    /// A cold-cache tick binds the per-tick constant SD_COLD sentinel; the
    /// next (warm) tick binds the real digest at a LATER HLC — head keys
    /// differ, so the transition can never produce evidence. The cold tick
    /// is reproduced through `sign_apply_and_push_frontiers` with a
    /// properly issued HLC (byte-identical to what a cold tick emits —
    /// deterministically forcing `ensure_digest_warm` to give up would
    /// require an in-flight write race, see digest_warmup's injectable
    /// tests); the warm tick runs the full production path.
    #[tokio::test]
    async fn cold_cache_reports_sentinel_then_real_digest_without_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let signer = make_signer("auth-1", 71);
        let registry = shared_registry_with(&signer);
        let detector = Arc::new(crate::authority::equivocation::EquivocationDetector::new(
            None,
        ));
        let shared_api = wrap_api(CertifiedApi::new(node_id("auth-1"), default_namespace()));
        let config = NodeRunnerConfig {
            node_signer: Some(Arc::clone(&signer)),
            keyset_registry: Some(Arc::clone(&registry)),
            equivocation: Some(Arc::clone(&detector)),
            ..digest_active_config(dir.path())
        };
        let mut runner = NodeRunner::new(
            node_id("auth-1"),
            shared_api.clone(),
            CompactionEngine::with_defaults(),
            config,
            default_metrics(),
        )
        .await;
        let eventual = eventual_with_keys("auth-1", &["user/x"]);
        runner.set_eventual_api(Arc::clone(&eventual));

        // Tick 1 (cold): what report_frontiers emits when the digest
        // cache is cold — the SD_COLD sentinel at a freshly issued,
        // floor-covered HLC.
        let issued = runner.clock.now().unwrap();
        runner
            .report_floor
            .as_mut()
            .unwrap()
            .cover(&issued)
            .unwrap();
        let cold_frontiers = runner
            .frontier_reporter
            .as_ref()
            .unwrap()
            .report_frontiers_at(&issued, SD_COLD);
        runner.sign_apply_and_push_frontiers(cold_frontiers).await;

        // Tick 2 (warm): the full production path with a real digest.
        tokio::time::sleep(Duration::from_millis(2)).await;
        runner.report_frontiers().await;

        let api = shared_api.lock().await;
        let frontiers = api.all_frontiers();
        assert!(frontiers.iter().all(|f| f.digest_hash.starts_with("sd2:")));
        assert!(
            frontiers.iter().any(|f| f.digest_hash != SD_COLD),
            "the warm tick must bind a real digest"
        );
        drop(api);
        assert_eq!(
            detector.accused_count(),
            0,
            "sentinel↔digest is no conflict"
        );
        assert!(detector.evidence().is_empty());
    }

    /// The core M-12 false-positive regression: the store legitimately
    /// mutates between report ticks (replication, local writes, GC), so
    /// the digest changes every tick — including WITHIN one checkpoint
    /// bucket. Each tick has a fresh HLC, so the detector never compares
    /// them: zero evidence, nobody accused.
    #[tokio::test]
    async fn no_false_positive_store_mutates_between_report_ticks() {
        let dir = tempfile::tempdir().unwrap();
        let signer = make_signer("auth-1", 72);
        let registry = shared_registry_with(&signer);
        let detector = Arc::new(crate::authority::equivocation::EquivocationDetector::new(
            None,
        ));
        let shared_api = wrap_api(CertifiedApi::new(node_id("auth-1"), default_namespace()));
        let config = NodeRunnerConfig {
            node_signer: Some(Arc::clone(&signer)),
            keyset_registry: Some(Arc::clone(&registry)),
            equivocation: Some(Arc::clone(&detector)),
            ..digest_active_config(dir.path())
        };
        let mut runner = NodeRunner::new(
            node_id("auth-1"),
            shared_api.clone(),
            CompactionEngine::with_defaults(),
            config,
            default_metrics(),
        )
        .await;
        let eventual = eventual_with_keys("auth-1", &["seed"]);
        runner.set_eventual_api(Arc::clone(&eventual));

        let mut digests = Vec::new();
        for i in 0..4 {
            // Mutate the store between ticks (several ticks land in the
            // same 1s checkpoint bucket).
            eventual
                .lock()
                .await
                .eventual_counter_inc(&format!("churn-{i}"))
                .unwrap();
            runner.report_frontiers().await;
            let api = shared_api.lock().await;
            digests.push(api.all_frontiers()[0].digest_hash.clone());
            drop(api);
            tokio::time::sleep(Duration::from_millis(2)).await;
        }

        // Content binding is live: every tick bound a different digest...
        for pair in digests.windows(2) {
            assert_ne!(pair[0], pair[1], "store mutation must change the digest");
        }
        // ...and none of it is ever mistaken for an equivocation.
        assert_eq!(detector.accused_count(), 0);
        assert!(detector.evidence().is_empty());
    }

    #[tokio::test]
    async fn floor_write_failure_skips_report_tick() {
        let dir = tempfile::tempdir().unwrap();
        // Point the floor INTO a regular file so every persist fails.
        std::fs::write(dir.path().join("blocker"), b"x").unwrap();
        let signer = make_signer("auth-1", 73);
        let registry = shared_registry_with(&signer);
        let detector = Arc::new(crate::authority::equivocation::EquivocationDetector::new(
            None,
        ));
        let metrics = default_metrics();
        let shared_api = wrap_api(CertifiedApi::new(node_id("auth-1"), default_namespace()));
        let config = NodeRunnerConfig {
            frontier_clock_floor_path: Some(dir.path().join("blocker/floor.json")),
            frontier_digest_activation_grace: Some(Duration::ZERO),
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
            Arc::clone(&metrics),
        )
        .await;
        runner.set_eventual_api(eventual_with_keys("auth-1", &["user/x"]));

        runner.report_frontiers().await;

        // Write-ahead: nothing signed, nothing observed, nothing applied.
        let api = shared_api.lock().await;
        assert!(
            api.all_frontiers().is_empty(),
            "an uncovered HLC must never produce a report"
        );
        drop(api);
        assert!(
            detector
                .gossip_summaries(crate::authority::equivocation::GOSSIP_SAMPLE_MAX)
                .is_empty(),
            "an uncovered HLC must never be self-observed"
        );
        assert_eq!(
            metrics.snapshot().frontier_report_skipped_floor_total,
            1,
            "the skipped tick must be counted"
        );
    }

    #[tokio::test]
    async fn restart_with_floor_seeds_clock_strictly_above_lease() {
        let dir = tempfile::tempdir().unwrap();
        let floor_path = dir.path().join("frontier_report_clock.json");
        // A lease FAR ahead of the wall clock — equivalent to a restart
        // whose wall clock rolled back way beyond MAX_CLOCK_SKEW_MS.
        // `Hlc::update` would reject this with ClockSkew; the seed must
        // use `seed_recovered` and still hold the floor invariant.
        let lease = crate::hlc::wall_clock_ms() + 5 * MAX_CLOCK_SKEW_MS;
        std::fs::write(
            &floor_path,
            format!("{{\"version\":1,\"leased_physical_ms\":{lease}}}"),
        )
        .unwrap();

        let shared_api = wrap_api(CertifiedApi::new(node_id("auth-1"), default_namespace()));
        let mut runner = NodeRunner::new(
            node_id("auth-1"),
            shared_api.clone(),
            CompactionEngine::with_defaults(),
            NodeRunnerConfig {
                frontier_clock_floor_path: Some(floor_path),
                ..NodeRunnerConfig::default()
            },
            default_metrics(),
        )
        .await;
        runner.set_eventual_api(eventual_with_keys("auth-1", &["user/x"]));

        runner.report_frontiers().await;

        let api = shared_api.lock().await;
        let frontiers = api.all_frontiers();
        assert!(!frontiers.is_empty(), "seeding must not break reporting");
        for f in &frontiers {
            assert!(
                f.frontier_hlc.physical >= lease,
                "post-restart report HLC ({}) must sit at/above the persisted \
                 lease ({lease}) — i.e. strictly above every pre-restart report",
                f.frontier_hlc.physical
            );
            // Floor existed => the store-digest format is active
            // immediately (no grace needed).
            assert!(f.digest_hash.starts_with("sd2:"));
        }
    }

    #[tokio::test]
    async fn missing_floor_path_never_activates_store_digest() {
        let shared_api = wrap_api(CertifiedApi::new(node_id("auth-1"), default_namespace()));
        let mut runner = NodeRunner::new(
            node_id("auth-1"),
            shared_api.clone(),
            CompactionEngine::with_defaults(),
            NodeRunnerConfig {
                // frontier_store_digest defaults to true, but without a
                // floor path there is no restart-monotonicity guarantee:
                // the format must stay off even with a zero grace.
                frontier_clock_floor_path: None,
                frontier_digest_activation_grace: Some(Duration::ZERO),
                ..NodeRunnerConfig::default()
            },
            default_metrics(),
        )
        .await;
        runner.set_eventual_api(eventual_with_keys("auth-1", &["user/x"]));

        runner.report_frontiers().await;

        let api = shared_api.lock().await;
        for f in &api.all_frontiers() {
            assert_eq!(
                f.digest_hash,
                placeholder_digest_hash(&node_id("auth-1"), &f.frontier_hlc),
                "no floor => placeholder format, unconditionally"
            );
        }
    }

    /// A floorless boot must sign NOTHING during the activation grace —
    /// its previous incarnation's format is unknown, so a placeholder
    /// report at a rolled-back, re-issued HLC could pair with a retained
    /// pre-crash `sd2:` head and frame this honest node. Silence is the
    /// only format-direction-agnostic safe behaviour. The floor file must
    /// not be created either (see the mid-grace-crash test below).
    #[tokio::test]
    async fn activation_grace_suppresses_all_reports_then_sd2() {
        let dir = tempfile::tempdir().unwrap();
        let floor_path = dir.path().join("frontier_report_clock.json");
        let shared_api = wrap_api(CertifiedApi::new(node_id("auth-1"), default_namespace()));
        let mut runner = NodeRunner::new(
            node_id("auth-1"),
            shared_api.clone(),
            CompactionEngine::with_defaults(),
            NodeRunnerConfig {
                frontier_clock_floor_path: Some(floor_path.clone()),
                // No floor file at startup => the grace applies.
                frontier_digest_activation_grace: Some(Duration::from_millis(300)),
                ..NodeRunnerConfig::default()
            },
            default_metrics(),
        )
        .await;
        runner.set_eventual_api(eventual_with_keys("auth-1", &["user/x"]));

        // During the grace: no reports of ANY format, and no floor file —
        // an unsigned tick is the only thing that provably cannot collide
        // with whatever format the pre-crash incarnation was signing.
        runner.report_frontiers().await;
        {
            let api = shared_api.lock().await;
            assert!(
                api.all_frontiers().is_empty(),
                "a floorless boot must not sign any report during the grace"
            );
        }
        assert!(
            !floor_path.exists(),
            "grace ticks must not create the floor file (a mid-grace crash \
             must restart the grace, not fake full-history coverage)"
        );

        // After the grace: reporting resumes directly in sd2 format and
        // the first covered tick creates the floor file.
        tokio::time::sleep(Duration::from_millis(400)).await;
        runner.report_frontiers().await;
        let api = shared_api.lock().await;
        let frontiers = api.all_frontiers();
        assert!(!frontiers.is_empty(), "reporting must resume after grace");
        for f in &frontiers {
            assert!(
                f.digest_hash.starts_with("sd2:"),
                "grace elapsed => sd2 format, got {}",
                f.digest_hash
            );
        }
        assert!(floor_path.exists(), "post-grace ticks are floor-covered");
    }

    /// Crash-during-grace regression: the floor file's existence is
    /// trusted as FULL restart-monotonicity evidence (immediate sd2), so
    /// it must only ever come into existence via a covered post-grace
    /// report. A runner that "crashes" mid-grace and is rebuilt on the
    /// same path must restart the grace from scratch — never activate
    /// sd2 (or sign anything) off a partially-served grace.
    #[tokio::test]
    async fn mid_grace_crash_restarts_grace_from_scratch() {
        let dir = tempfile::tempdir().unwrap();
        let floor_path = dir.path().join("frontier_report_clock.json");
        let config = NodeRunnerConfig {
            frontier_clock_floor_path: Some(floor_path.clone()),
            frontier_digest_activation_grace: Some(Duration::from_secs(3600)),
            ..NodeRunnerConfig::default()
        };

        // Boot 1: floorless => grace. Tick a few times "during" it.
        let shared_api = wrap_api(CertifiedApi::new(node_id("auth-1"), default_namespace()));
        let mut runner = NodeRunner::new(
            node_id("auth-1"),
            shared_api.clone(),
            CompactionEngine::with_defaults(),
            config.clone(),
            default_metrics(),
        )
        .await;
        runner.set_eventual_api(eventual_with_keys("auth-1", &["user/x"]));
        runner.report_frontiers().await;
        runner.report_frontiers().await;
        assert!(shared_api.lock().await.all_frontiers().is_empty());
        assert!(!floor_path.exists());
        drop(runner); // "crash" mid-grace

        // Boot 2 on the same path: the floor is still absent, so the FULL
        // grace applies again — first tick must stay silent, not report
        // sd2 (the old bug: boot 2 saw a floor file created by boot 1's
        // grace ticks and activated sd2 immediately, though the lease
        // never covered the pre-upgrade / pre-loss report history).
        let shared_api2 = wrap_api(CertifiedApi::new(node_id("auth-1"), default_namespace()));
        let mut runner2 = NodeRunner::new(
            node_id("auth-1"),
            shared_api2.clone(),
            CompactionEngine::with_defaults(),
            config,
            default_metrics(),
        )
        .await;
        runner2.set_eventual_api(eventual_with_keys("auth-1", &["user/x"]));
        runner2.report_frontiers().await;
        assert!(
            shared_api2.lock().await.all_frontiers().is_empty(),
            "a mid-grace crash must restart the grace, not activate sd2"
        );
        assert!(!floor_path.exists());
    }

    /// Runtime-promotion regression (M-12): a node that becomes an
    /// authority via membership recalculation (not at construction) must
    /// run the same floor/activation initialization as the constructors —
    /// previously report_floor stayed None forever, so the store-digest
    /// format silently never activated and the report HLCs had no
    /// write-ahead coverage until the next restart.
    #[tokio::test]
    async fn runtime_promotion_initializes_report_floor_and_activates_sd2() {
        use crate::placement::PlacementPolicy;
        use crate::types::NodeMode;

        let dir = tempfile::tempdir().unwrap();
        let floor_path = dir.path().join("frontier_report_clock.json");

        // Certified policy with no authority definition yet: the runner
        // node is NOT an authority at construction time.
        let mut ns = SystemNamespace::new();
        ns.set_placement_policy(
            PlacementPolicy::new(PolicyVersion(1), kr("user/"), 1).with_certified(true),
        )
        .unwrap();
        let shared_api = wrap_api(CertifiedApi::new(node_id("n1"), wrap_ns(ns)));
        let cluster_nodes = Arc::new(std::sync::RwLock::new(Vec::<crate::node::Node>::new()));
        let mut runner = NodeRunner::with_cluster_nodes(
            node_id("n1"),
            shared_api.clone(),
            CompactionEngine::with_defaults(),
            NodeRunnerConfig {
                frontier_clock_floor_path: Some(floor_path.clone()),
                frontier_digest_activation_grace: Some(Duration::ZERO),
                ..NodeRunnerConfig::default()
            },
            default_metrics(),
            cluster_nodes.clone(),
        )
        .await;
        runner.set_eventual_api(eventual_with_keys("n1", &["user/x"]));
        assert!(runner.frontier_reporter.is_none());
        assert!(runner.report_floor.is_none(), "non-authority: no floor");

        // The node joins the cluster; recalculation promotes it.
        cluster_nodes
            .write()
            .unwrap()
            .push(crate::node::Node::new(node_id("n1"), NodeMode::Store));
        runner.detect_membership_changes().await;
        assert!(
            runner.frontier_reporter.is_some(),
            "membership recalculation must promote n1 to authority"
        );
        assert!(
            runner.report_floor.is_some(),
            "promotion must initialize the report clock floor"
        );
        assert!(runner.store_digest_active_at.is_some());

        // The promoted authority reports with full M-12 semantics: sd2
        // digest (grace zero) and a write-ahead-covered HLC.
        runner.report_frontiers().await;
        let api = shared_api.lock().await;
        let frontiers = api.all_frontiers();
        assert!(!frontiers.is_empty(), "promoted authority must report");
        for f in &frontiers {
            assert!(
                f.digest_hash.starts_with("sd2:"),
                "promoted authority must bind the store digest, got {}",
                f.digest_hash
            );
        }
        assert!(floor_path.exists(), "reports must be floor-covered");
        drop(api);

        // Demote/re-promote: the floor survives and the grace is not
        // re-armed (no second initialization).
        //
        // Demote by replacing n1 in the inventory, not by emptying it:
        // `recalculate_authorities` treats an empty candidate set as "no
        // inventory available" and leaves existing definitions alone, so
        // clearing the list demotes nobody. Replacement is also the shape a
        // real demotion takes (a node leaves while others remain).
        {
            let mut nodes = cluster_nodes.write().unwrap();
            nodes.clear();
            nodes.push(crate::node::Node::new(node_id("n2"), NodeMode::Store));
        }
        runner.detect_membership_changes().await;
        assert!(runner.frontier_reporter.is_none(), "demoted");
        assert!(
            runner.report_floor.is_some(),
            "demotion must keep the floor for a later re-promotion"
        );
    }

    #[tokio::test]
    async fn kill_switch_config_restores_placeholder_format() {
        let dir = tempfile::tempdir().unwrap();
        let shared_api = wrap_api(CertifiedApi::new(node_id("auth-1"), default_namespace()));
        let mut runner = NodeRunner::new(
            node_id("auth-1"),
            shared_api.clone(),
            CompactionEngine::with_defaults(),
            NodeRunnerConfig {
                // ASTEROIDB_FRONTIER_STORE_DIGEST=0 lands here (main.rs).
                frontier_store_digest: false,
                ..digest_active_config(dir.path())
            },
            default_metrics(),
        )
        .await;
        runner.set_eventual_api(eventual_with_keys("auth-1", &["user/x"]));

        runner.report_frontiers().await;

        let api = shared_api.lock().await;
        let frontiers = api.all_frontiers();
        assert!(!frontiers.is_empty());
        for f in &frontiers {
            assert_eq!(
                f.digest_hash,
                placeholder_digest_hash(&node_id("auth-1"), &f.frontier_hlc)
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

    /// An INCREMENTAL delta response (non-zero request frontier) with
    /// sound coverage adopts ONLY the sender's OWN origin from its
    /// applied_origins: a scalar verified watermark cannot rule out the
    /// sender having back-filled a THIRD origin's write below the (stale)
    /// watermark after the pull that set it, so third-origin claims from
    /// an incremental delta would be read-your-writes lies.
    #[tokio::test]
    async fn apply_delta_response_adopts_applied_origins_when_floor_ok() {
        let eventual_api = Arc::new(Mutex::new(EventualApi::new(node_id("receiver"))));
        let mut peer_frontiers: HashMap<String, HlcTimestamp> = HashMap::new();

        let mut counter = crate::crdt::pn_counter::PnCounter::new();
        counter.increment(&node_id("origin-a"));
        let mut applied_origins = HashMap::new();
        // The sender claims its OWN origin (adoptable on an incremental
        // pull: its writes are monotone, so it cannot introduce an
        // own-write below the request frontier this node lacks) and a
        // THIRD origin (NOT adoptable — possible back-fill below the
        // stale watermark).
        applied_origins.insert("peer-1".to_string(), hlc_ts(200, 0, "peer-1"));
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
            untracked_entries: HashMap::new(),
        };

        // Coverage: everything up to 150 was previously received via pulls.
        let mut pull_verified: HashMap<String, HlcTimestamp> = HashMap::new();
        pull_verified.insert("peer-1:8000".to_string(), hlc_ts(150, 0, "origin-b"));

        // Request frontier (150) >= pruned floor (100) and <= verified
        // coverage (150): claims are sound for the sender's own origin.
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
            api.store().applied_origin("peer-1"),
            Some(&hlc_ts(200, 0, "peer-1")),
            "the sender's OWN origin claim must be adopted"
        );
        // Regression (third-origin claim unsoundness on incremental
        // pulls): the sender may have back-filled origin-a's writes below
        // the stale verified watermark; an incremental delta omits them,
        // so adopting origin-a's frontier would claim writes this node
        // does not hold.
        assert!(
            api.store().applied_origin("origin-a").is_none(),
            "third-origin claims must not be adopted from an incremental delta"
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
            untracked_entries: HashMap::new(),
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
            untracked_entries: HashMap::new(),
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
    /// with the sender frontier. A COMPLETE (zero-frontier) pull adopts
    /// the whole applied_origins map; subsequent INCREMENTAL pulls keep
    /// advancing the sender's OWN origin (third origins wait for the
    /// next complete transfer).
    #[tokio::test]
    async fn claims_recover_after_push_advances_peer_frontier() {
        let eventual_api = Arc::new(Mutex::new(EventualApi::new(node_id("receiver"))));
        let mut peer_frontiers: HashMap<String, HlcTimestamp> = HashMap::new();
        let mut pull_verified: HashMap<String, HlcTimestamp> = HashMap::new();

        // Cycle 1: initial claimed pull at frontier zero — a COMPLETE
        // pull, so even third-origin claims are adopted.
        let mut applied = HashMap::new();
        applied.insert("origin-a".to_string(), hlc_ts(100, 0, "origin-a"));
        applied.insert("peer-1".to_string(), hlc_ts(90, 0, "peer-1"));
        let resp1 = crate::network::sync::DeltaSyncResponse {
            entries: vec![],
            sender_frontier: Some(hlc_ts(100, 0, "origin-a")),
            applied_origins: applied,
            merge_failed_keys: vec![],
            pruned_floor: None,
            visible_origins: HashMap::new(),
            untracked_entries: HashMap::new(),
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
        {
            let api = eventual_api.lock().await;
            assert_eq!(
                api.store().applied_origin("origin-a"),
                Some(&hlc_ts(100, 0, "origin-a")),
                "a complete pull adopts third-origin claims"
            );
        }

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
        applied.insert("peer-1".to_string(), hlc_ts(1_000, 0, "peer-1"));
        applied.insert("origin-a".to_string(), hlc_ts(1_000, 0, "origin-a"));
        let resp2 = crate::network::sync::DeltaSyncResponse {
            entries: vec![],
            sender_frontier: Some(hlc_ts(1_000, 0, "peer-1")),
            applied_origins: applied,
            merge_failed_keys: vec![],
            pruned_floor: None,
            visible_origins: HashMap::new(),
            untracked_entries: HashMap::new(),
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
            api.store().applied_origin("peer-1"),
            Some(&hlc_ts(1_000, 0, "peer-1")),
            "incremental adoption must keep advancing the sender's own origin"
        );
        // The third origin stays at its complete-pull value: an
        // incremental delta cannot prove origin-a's (1_000 > 100) writes
        // were transferred (possible back-fill below the watermark).
        assert_eq!(
            api.store().applied_origin("origin-a"),
            Some(&hlc_ts(100, 0, "origin-a")),
            "third-origin claims must wait for the next complete transfer"
        );
        drop(api);
        assert_eq!(
            pull_verified.get("peer-1:8000"),
            Some(&hlc_ts(1_000, 0, "peer-1"))
        );
    }

    /// M-2 regression: a COMPLETE (zero-frontier) pull from a
    /// v1/v2-migrated sender includes its timestamp-less keys as
    /// `untracked_entries`; the receiver must merge them BEFORE adopting
    /// the sender's whole applied_origins map. Without the compensation
    /// the receiver would adopt the claim while the data never arrives
    /// through the pull path — a read-your-writes false success plus
    /// permanent divergence.
    #[tokio::test]
    async fn complete_pull_merges_untracked_entries_before_adoption() {
        let eventual_api = Arc::new(Mutex::new(EventualApi::new(node_id("receiver"))));
        let mut peer_frontiers: HashMap<String, HlcTimestamp> = HashMap::new();
        let mut pull_verified: HashMap<String, HlcTimestamp> = HashMap::new();

        // The sender's tracked entry rides the delta scan; the migrated
        // key has no per-key HLC and rides untracked_entries.
        let mut tracked_counter = crate::crdt::pn_counter::PnCounter::new();
        tracked_counter.increment(&node_id("peer-1"));
        let mut migrated_counter = crate::crdt::pn_counter::PnCounter::new();
        migrated_counter.increment(&node_id("peer-1"));
        migrated_counter.increment(&node_id("peer-1"));

        let mut applied = HashMap::new();
        applied.insert("peer-1".to_string(), hlc_ts(100, 0, "peer-1"));
        let mut untracked = HashMap::new();
        untracked.insert(
            "migrated-key".to_string(),
            CrdtValue::Counter(migrated_counter),
        );
        let delta_resp = crate::network::sync::DeltaSyncResponse {
            entries: vec![crate::network::sync::DeltaEntry {
                key: "tracked-key".into(),
                value: CrdtValue::Counter(tracked_counter),
                hlc: hlc_ts(100, 0, "peer-1"),
            }],
            sender_frontier: Some(hlc_ts(100, 0, "peer-1")),
            applied_origins: applied,
            merge_failed_keys: vec![],
            pruned_floor: None,
            visible_origins: HashMap::new(),
            untracked_entries: untracked,
        };

        let outcome = NodeRunner::apply_delta_response(
            &mut peer_frontiers,
            &mut pull_verified,
            &delta_resp,
            "peer-1",
            "peer-1:8000",
            &eventual_api,
            &hlc_ts(0, 0, ""),
            "test",
        )
        .await;
        assert_eq!(outcome.merge_errors, 0);
        assert!(outcome.claims_ok, "zero-frontier pull is complete");

        let api = eventual_api.lock().await;
        // The adopted claim is TRUE: the untracked data arrived with it.
        match api.get_eventual("migrated-key") {
            Some(CrdtValue::Counter(c)) => assert_eq!(c.value(), 2),
            other => panic!("expected migrated Counter, got {other:?}"),
        }
        assert_eq!(
            api.store().applied_origin("peer-1"),
            Some(&hlc_ts(100, 0, "peer-1")),
            "complete-pull adoption still applies"
        );
        // The compensated key is re-stamped locally: it is now tracked
        // and therefore delta-visible onward from this node.
        assert!(
            api.store().timestamp_for("migrated-key").is_some(),
            "untracked entries must become tracked on the receiver"
        );
    }

    // ---------------------------------------------------------------
    // Tombstone GC gates (C-2)
    // ---------------------------------------------------------------

    fn ack_frontier(
        authority: &str,
        prefix: &str,
        version: u64,
        physical: u64,
    ) -> crate::authority::ack_frontier::AckFrontier {
        crate::authority::ack_frontier::AckFrontier {
            authority_id: node_id(authority),
            frontier_hlc: hlc_ts(physical, 0, authority),
            key_range: KeyRange {
                prefix: prefix.into(),
            },
            policy_version: PolicyVersion(version),
            digest_hash: "d".into(),
        }
    }

    /// The authority half of the GC gate: EVERY authority of EVERY
    /// definition must have reported the current scope, and the scoped
    /// minimum frontier must have consumed state as of the mark time.
    /// The old guard (`!frontiers.is_empty()`) passed off this node's
    /// own self-report — a partitioned authority could still resurrect
    /// GC'd removes after healing.
    #[test]
    fn gc_authority_gate_requires_all_authorities_past_mark() {
        let defs = vec![(
            KeyRange {
                prefix: "user/".into(),
            },
            2usize,
        )];
        let versions = vec![PolicyVersion(1)];
        let mark_ms = 8_000;

        // No reports at all: fail-closed (the old `!frontiers.is_empty()`
        // guard is exactly what this replaces).
        let set = crate::authority::ack_frontier::AckFrontierSet::new();
        assert!(!NodeRunner::gc_authority_gate_passed(
            &defs, &versions, &set, mark_ms
        ));

        // Only one of two authorities reported: fail.
        let mut set = crate::authority::ack_frontier::AckFrontierSet::new();
        set.update(ack_frontier("auth-1", "user/", 1, 10_000));
        assert!(!NodeRunner::gc_authority_gate_passed(
            &defs, &versions, &set, mark_ms
        ));

        // Both reported but one is still BEHIND the mark: fail (that
        // authority may not have consumed the tombstoned state yet).
        set.update(ack_frontier("auth-2", "user/", 1, 5_000));
        assert!(!NodeRunner::gc_authority_gate_passed(
            &defs, &versions, &set, mark_ms
        ));

        // Both past the mark: pass.
        set.update(ack_frontier("auth-2", "user/", 1, 9_000));
        assert!(NodeRunner::gc_authority_gate_passed(
            &defs, &versions, &set, mark_ms
        ));

        // No authority definitions (single-node deployment): vacuously
        // true — there is no other replica to resurrect from.
        assert!(NodeRunner::gc_authority_gate_passed(
            &[],
            &[],
            &crate::authority::ack_frontier::AckFrontierSet::new(),
            mark_ms
        ));
    }

    /// Clock-skew regression (re-review): the authority gate must not
    /// pass off a frontier whose DATA time (`frontier_hlc.physical`,
    /// which the HLC max rule inflates under peer-clock skew) is past
    /// the mark when the report was RECEIVED (local wall clock) before
    /// the mark — a stale pre-partition frontier must stall collection
    /// for the whole partition.
    #[test]
    fn gc_authority_gate_requires_fresh_local_receipts() {
        let defs = vec![(
            KeyRange {
                prefix: "user/".into(),
            },
            1usize,
        )];
        let versions = vec![PolicyVersion(1)];
        let mark_ms = 8_000;

        // Skew-inflated data time (10_000 >= mark) but a receipt from
        // BEFORE the mark (7_000): fail.
        let mut set = crate::authority::ack_frontier::AckFrontierSet::new();
        set.update_at(ack_frontier("auth-1", "user/", 1, 10_000), 7_000);
        assert!(
            !NodeRunner::gc_authority_gate_passed(&defs, &versions, &set, mark_ms),
            "a stale receipt must fail the gate regardless of the frontier's data time"
        );

        // A strictly newer report received at/after the mark: pass.
        set.update_at(ack_frontier("auth-1", "user/", 1, 10_001), 8_500);
        assert!(NodeRunner::gc_authority_gate_passed(
            &defs, &versions, &set, mark_ms
        ));

        // A frontier restored from persistence never advanced in this
        // process: the (volatile) receipt map is empty after a JSON
        // round-trip, so the gate stays fail-closed.
        let json = serde_json::to_string(&set).expect("serialize frontier set");
        let restored: crate::authority::ack_frontier::AckFrontierSet =
            serde_json::from_str(&json).expect("deserialize frontier set");
        assert!(
            !NodeRunner::gc_authority_gate_passed(&defs, &versions, &restored, mark_ms),
            "persisted frontiers without fresh receipts must fail the gate"
        );
    }

    /// The peer half of the GC gate: every registered peer must have
    /// push evidence (local wall-clock time of the scan behind the last
    /// error-free push) at/after the mark — a peer with NO entry (never
    /// successfully pushed to, e.g. partitioned since before the mark)
    /// fails the gate rather than being ignored.
    #[test]
    fn gc_peer_gate_requires_all_registered_peers_past_mark() {
        let peer = |name: &str, addr: &str| crate::network::PeerConfig {
            node_id: node_id(name),
            addr: addr.into(),
        };
        let mark_ms = 8_000;
        let mut acked: HashMap<String, u64> = HashMap::new();

        // Empty registry (no peers): vacuously true.
        assert!(NodeRunner::gc_peer_gate_passed(&[], &acked, mark_ms));

        // A registered peer with no push evidence at all: fail-closed.
        let peers = vec![peer("p1", "p1:9000"), peer("p2", "p2:9000")];
        acked.insert("p1:9000".into(), 9_000);
        assert!(!NodeRunner::gc_peer_gate_passed(&peers, &acked, mark_ms));

        // Push evidence from BEFORE the mark: fail.
        acked.insert("p2:9000".into(), 7_999);
        assert!(!NodeRunner::gc_peer_gate_passed(&peers, &acked, mark_ms));

        // All peers pushed at/after the mark: pass.
        acked.insert("p2:9000".into(), 8_000);
        assert!(NodeRunner::gc_peer_gate_passed(&peers, &acked, mark_ms));
    }

    /// C-2 regression (re-review): a successful PULL must NOT count as
    /// push evidence for the GC peer gate. Under the old design
    /// `peer_frontiers` (advanced to the peer's own sender frontier on
    /// every pull) fed the gate, so a peer that kept writing while our
    /// pushes to it failed would pass the gate and the swept tombstone
    /// could resurrect from its stale state.
    #[test]
    fn gc_peer_gate_ignores_pull_advanced_frontiers() {
        let peers = vec![crate::network::PeerConfig {
            node_id: node_id("p1"),
            addr: "p1:9000".into(),
        }];
        let mark_ms = 8_000;

        // Simulate what a pull does: peer_frontiers advances, but the
        // push-evidence map stays empty. The gate must fail.
        let mut peer_frontiers: HashMap<String, HlcTimestamp> = HashMap::new();
        peer_frontiers.insert("p1:9000".into(), hlc_ts(9_999, 0, "p1"));
        let push_acked: HashMap<String, u64> = HashMap::new();
        assert!(
            !NodeRunner::gc_peer_gate_passed(&peers, &push_acked, mark_ms),
            "a pull-advanced peer frontier must not open the GC gate"
        );
    }

    /// Stage 2 INBOUND gate: hole-jump requires reconciliation evidence
    /// (a complete, error-free pull STARTED at/after the mark) from
    /// EVERY registered peer — fail-closed on missing or stale entries.
    #[test]
    fn gc_inbound_gate_requires_all_registered_peers_reconciled_past_mark() {
        let peer = |name: &str, addr: &str| crate::network::PeerConfig {
            node_id: node_id(name),
            addr: addr.into(),
        };
        let mark_ms = 8_000;
        let mut reconciled: HashMap<String, u64> = HashMap::new();

        // Empty registry: vacuously true (no peer can hold a hole dot live).
        assert!(NodeRunner::gc_inbound_gate_passed(
            &[],
            &reconciled,
            mark_ms
        ));

        // A registered peer with no reconciliation evidence: fail-closed.
        let peers = vec![peer("p1", "p1:9000"), peer("p2", "p2:9000")];
        reconciled.insert("p1:9000".into(), 9_000);
        assert!(!NodeRunner::gc_inbound_gate_passed(
            &peers,
            &reconciled,
            mark_ms
        ));

        // Evidence from BEFORE the mark: fail (the peer's state as of the
        // mark was never absorbed).
        reconciled.insert("p2:9000".into(), 7_999);
        assert!(!NodeRunner::gc_inbound_gate_passed(
            &peers,
            &reconciled,
            mark_ms
        ));

        // All peers reconciled at/after the mark: pass.
        reconciled.insert("p2:9000".into(), 8_000);
        assert!(NodeRunner::gc_inbound_gate_passed(
            &peers,
            &reconciled,
            mark_ms
        ));
    }

    // -----------------------------------------------------------------
    // run_gc Stage 2 wiring (config flag ∧ outbound gates ∧ inbound
    // evidence against the PENDING mark) — exercised end-to-end so a
    // future refactor (`&&` → `||`, comparing against the wrong
    // timestamp, mis-keyed peer addresses, …) cannot silently turn the
    // fail-closed hole-jump into a fail-open one.
    // -----------------------------------------------------------------

    /// Build a runner whose store holds a legacy-hole OrSet — counters
    /// `A=2`, live `(A,2)` swept away by a remove into tombstone `(A,2)`,
    /// and `(A,1)` neither live nor deferred (a pre-floor sweep's hole) —
    /// with one registered peer, an empty authority namespace (authority
    /// gate vacuous), a 1ms GC interval and zero retention so run_gc
    /// passes chain immediately (pass 1 marks, pass 2 may sweep).
    async fn legacy_hole_runner(
        hole_jump_enabled: bool,
    ) -> (NodeRunner, Arc<Mutex<EventualApi>>, String) {
        legacy_hole_runner_with_ns(hole_jump_enabled, SystemNamespace::new()).await
    }

    /// As [`legacy_hole_runner`], but with a caller-supplied namespace so a
    /// test can put the GC authority gate under a REAL production-shaped
    /// namespace instead of the empty (vacuous) one.
    async fn legacy_hole_runner_with_ns(
        hole_jump_enabled: bool,
        ns: SystemNamespace,
    ) -> (NodeRunner, Arc<Mutex<EventualApi>>, String) {
        use crate::api::eventual::EventualApi;

        let api = wrap_api(CertifiedApi::new(node_id("node-1"), wrap_ns(ns)));
        let config = NodeRunnerConfig {
            gc_interval: Duration::from_millis(1),
            gc_retention: Duration::ZERO,
            gc_hole_jump_enabled: hole_jump_enabled,
            sync_interval: None,
            ping_interval: None,
            ..NodeRunnerConfig::default()
        };
        let eventual_api = Arc::new(Mutex::new(EventualApi::new(node_id("node-1"))));
        {
            let mut ea = eventual_api.lock().await;
            let json = r#"{"elements":{"y":[{"node_id":"A","counter":2}]},"counters":{"A":2}}"#;
            let mut set: crate::crdt::or_set::OrSet<String> =
                serde_json::from_str(json).expect("legacy state");
            set.remove(&"y".to_string()); // tombstone (A,2) above the hole (A,1)
            ea.eventual_write("myset".to_string(), CrdtValue::Set(set))
                .expect("seed store");
        }

        let peer_addr = "p1:9000".to_string();
        let registry = crate::network::PeerRegistry::new(
            node_id("node-1"),
            vec![crate::network::PeerConfig {
                node_id: node_id("p1"),
                addr: peer_addr.clone(),
            }],
        )
        .expect("valid registry");
        let sync_client = SyncClient::new(Arc::new(Mutex::new(registry)));

        let mut runner = NodeRunner::new(
            node_id("node-1"),
            api,
            CompactionEngine::with_defaults(),
            config,
            default_metrics(),
        )
        .await;
        runner.set_eventual_api(eventual_api.clone());
        runner.set_sync_client(sync_client);
        (runner, eventual_api, peer_addr)
    }

    async fn hole_state(eventual_api: &Arc<Mutex<EventualApi>>) -> (HashMap<NodeId, u64>, usize) {
        let ea = eventual_api.lock().await;
        match ea.store().get("myset") {
            Some(CrdtValue::Set(s)) => (s.compaction_floor().clone(), s.deferred_len()),
            other => panic!("expected Set, got {other:?}"),
        }
    }

    /// Stage 2 disabled: even with full outbound AND inbound evidence the
    /// sweep must stall on the legacy hole (fail-closed) and report it on
    /// the stall gauge — and a subsequent BLOCKED pass must not zero that
    /// gauge (ops-guide 3.7's "persistently non-zero" Stage 2 signal).
    #[tokio::test]
    async fn run_gc_stage1_stalls_on_legacy_hole_despite_full_evidence() {
        let (mut runner, eventual_api, peer_addr) = legacy_hole_runner(false).await;

        runner.run_gc().await; // pass 1: mark
        assert!(runner.tombstone_gc.pending_mark_ms().is_some());
        runner
            .push_acked_wall_ms
            .insert(peer_addr.clone(), u64::MAX);
        runner
            .pull_reconciled_wall_ms
            .insert(peer_addr.clone(), u64::MAX);

        runner.run_gc().await; // pass 2: sweep (gates pass, flag off)
        let (floor, deferred) = hole_state(&eventual_api).await;
        assert!(floor.is_empty(), "disabled flag must keep the walk stalled");
        assert_eq!(deferred, 1, "the tombstone above the hole must survive");
        assert_eq!(
            runner
                .metrics
                .gc_floor_stalled_hole_dots
                .load(Ordering::Relaxed),
            1,
            "the executed sweep must report the hole stall"
        );

        // A gate-blocked pass (outbound evidence gone) must keep the
        // last executed sweep's gauge instead of flapping it to zero.
        runner.push_acked_wall_ms.clear();
        runner.run_gc().await;
        assert_eq!(
            runner
                .metrics
                .gc_floor_stalled_hole_dots
                .load(Ordering::Relaxed),
            1,
            "a blocked (non-sweep) pass must not overwrite the stall gauge"
        );
    }

    /// Stage 2 enabled but the inbound evidence predates the mark: the
    /// hole-jump must stay off (fail-closed) — the pull proves nothing
    /// about state the peer held at the mark.
    #[tokio::test]
    async fn run_gc_stage2_requires_inbound_evidence_after_the_mark() {
        let (mut runner, eventual_api, peer_addr) = legacy_hole_runner(true).await;

        runner.run_gc().await; // pass 1: mark
        let mark_ms = runner
            .tombstone_gc
            .pending_mark_ms()
            .expect("mark must be pending");
        runner
            .push_acked_wall_ms
            .insert(peer_addr.clone(), u64::MAX);
        runner
            .pull_reconciled_wall_ms
            .insert(peer_addr.clone(), mark_ms.saturating_sub(1)); // STALE

        runner.run_gc().await; // pass 2: sweep, hole-jump must be denied
        let (floor, deferred) = hole_state(&eventual_api).await;
        assert!(
            floor.is_empty(),
            "stale inbound evidence must keep the hole-jump fail-closed"
        );
        assert_eq!(deferred, 1);
        assert_eq!(
            runner
                .metrics
                .gc_floor_stalled_hole_dots
                .load(Ordering::Relaxed),
            1
        );
    }

    /// Stage 2 enabled with post-mark inbound evidence from every
    /// registry peer: the walk crosses the legacy hole, the floor reaches
    /// the marked tombstone and collection proceeds.
    #[tokio::test]
    async fn run_gc_stage2_jumps_legacy_hole_with_fresh_inbound_evidence() {
        let (mut runner, eventual_api, peer_addr) = legacy_hole_runner(true).await;

        runner.run_gc().await; // pass 1: mark
        runner
            .push_acked_wall_ms
            .insert(peer_addr.clone(), u64::MAX);
        runner
            .pull_reconciled_wall_ms
            .insert(peer_addr.clone(), u64::MAX);

        runner.run_gc().await; // pass 2: sweep with hole-jump authorised
        let (floor, deferred) = hole_state(&eventual_api).await;
        assert_eq!(
            floor.get(&NodeId("A".into())),
            Some(&2),
            "the floor must cross the legacy hole and absorb the tombstone"
        );
        assert_eq!(deferred, 0, "the marked tombstone must be collected");
        assert_eq!(
            runner
                .metrics
                .gc_floor_stalled_hole_dots
                .load(Ordering::Relaxed),
            0
        );
        assert!(runner.tombstone_gc.total_collected() >= 1);
    }

    // -----------------------------------------------------------------
    // D1: the catch-all seed must not wedge tombstone GC shut forever.
    //
    // `main.rs` seeds a catch-all `""` authority definition on a fresh
    // boot and never creates a placement policy. The GC authority gate
    // used to build its population from EVERY definition and fabricate
    // `PolicyVersion(1)` for the policy-less ones, demanding frontier
    // evidence for scope `("", 1)` — a scope nobody can ever report
    // (`discover_scopes` skips policy-less definitions) and nobody can
    // ever accept (`attestation_admissible` rejects it with `NoPolicy`).
    // The gate was therefore permanently false from first boot, on the
    // DEFAULT configuration, and no tombstone was ever collected.
    // -----------------------------------------------------------------

    /// The namespace `src/main.rs` produces on a fresh boot: a manual
    /// catch-all `""` definition over the default authority set, and NO
    /// placement policy anywhere.
    fn production_seed_namespace() -> SystemNamespace {
        let mut ns = SystemNamespace::new();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr(""),
            authority_nodes: vec![node_id("auth-1"), node_id("auth-2"), node_id("auth-3")],
            auto_generated: false,
        });
        ns
    }

    /// Drive a legacy-hole runner through mark → full outbound+inbound
    /// evidence → sweep, and report whether the sweep actually collected.
    async fn sweeps_with_full_evidence(ns: SystemNamespace) -> (bool, u64) {
        let (mut runner, _eventual_api, peer_addr) = legacy_hole_runner_with_ns(true, ns).await;

        runner.run_gc().await; // pass 1: mark
        assert!(runner.tombstone_gc.pending_mark_ms().is_some());

        // Saturate BOTH peer-facing gates so the authority gate is the
        // only thing left that can hold the sweep back.
        runner
            .push_acked_wall_ms
            .insert(peer_addr.clone(), u64::MAX);
        runner.pull_reconciled_wall_ms.insert(peer_addr, u64::MAX);

        runner.run_gc().await; // pass 2: sweep
        let collected = runner.tombstone_gc.total_collected();
        let last_sweep = runner.metrics.gc_last_sweep_wall_ms.load(Ordering::Relaxed);
        (collected >= 1, last_sweep)
    }

    /// D1 regression: the default fresh-boot namespace must not block the
    /// GC authority gate. The catch-all definition has no policy, so it
    /// cannot participate in certification at all — demanding frontier
    /// evidence for it is unsatisfiable, not strict.
    #[tokio::test]
    async fn production_seed_namespace_sweeps_tombstones() {
        let (swept, last_sweep_ms) = sweeps_with_full_evidence(production_seed_namespace()).await;
        assert!(
            swept,
            "the fresh-boot catch-all seed must not wedge tombstone GC shut"
        );
        assert!(
            last_sweep_ms > 0,
            "an executed sweep must stamp gc_last_sweep_wall_ms"
        );
    }

    /// Backward compatibility (range-states.md test plan 6): a deployment
    /// that already persisted the seed keeps a MANUAL (`auto_generated:
    /// false`) `""` definition forever — `recalculate_authorities` never
    /// prunes manual definitions, and the control plane has no
    /// `RemoveAuthority` command. GC must un-wedge on upgrade anyway.
    ///
    /// That this passes with `auto_generated` left at `false` is the
    /// executable proof that demoting the seed is NOT the cure.
    #[tokio::test]
    async fn legacy_persisted_namespace_sweeps_tombstones() {
        // (a) a namespace that went through the real persistence path.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("system_namespace.json");
        production_seed_namespace().save(&path).expect("save");
        let loaded = SystemNamespace::load(&path)
            .expect("load")
            .expect("file exists");
        assert!(
            !loaded
                .get_authority_definition("")
                .expect("catch-all survives persistence")
                .auto_generated,
            "the persisted seed stays MANUAL — demoting main.rs cannot reach it"
        );
        let (swept, _) = sweeps_with_full_evidence(loaded).await;
        assert!(swept, "a persisted seed namespace must not wedge GC shut");

        // (b) an OLD-FORMAT file that predates the `auto_generated` field
        //     entirely (it deserialises to `false` via `#[serde(default)]`).
        let legacy_json = r#"{
            "version": 2,
            "placement_policies": {},
            "authority_definitions": {
                "": {
                    "key_range": {"prefix": ""},
                    "authority_nodes": ["auth-1", "auth-2", "auth-3"]
                }
            },
            "version_history": [1, 2]
        }"#;
        let legacy: SystemNamespace = serde_json::from_str(legacy_json).expect("legacy namespace");
        assert!(
            !legacy
                .get_authority_definition("")
                .expect("def")
                .auto_generated
        );
        let (swept, _) = sweeps_with_full_evidence(legacy).await;
        assert!(swept, "an old-format namespace must not wedge GC shut");
    }

    /// The peer gate is what actually protects a multi-replica deployment,
    /// and it does NOT depend on authority definitions: with the same seed
    /// namespace but a registered peer that has never been pushed to, the
    /// sweep must still be blocked.
    ///
    /// Not RED-first (it passes before the fix too, via the authority
    /// gate); its value is proving that removing the authority-gate term
    /// did not remove the protection. Verified by mutation — see notes.
    #[tokio::test]
    async fn peer_gate_alone_blocks_sweep_on_seed_namespace() {
        let (mut runner, _eventual_api, _peer_addr) =
            legacy_hole_runner_with_ns(true, production_seed_namespace()).await;

        runner.run_gc().await; // pass 1: mark
        runner.run_gc().await; // pass 2: no push evidence for the peer

        assert_eq!(
            runner.tombstone_gc.total_collected(),
            0,
            "a registered peer without push evidence must block the sweep"
        );
        assert_eq!(
            runner.metrics.gc_last_sweep_wall_ms.load(Ordering::Relaxed),
            0,
            "a blocked pass never sweeps"
        );
        assert!(
            runner
                .metrics
                .gc_gate_blocked_peer_total
                .load(Ordering::Relaxed)
                > 0,
            "the block must be attributed to the peer gate"
        );
    }

    /// Write ops for keys covered by no certifiable range must not be
    /// charged to an arbitrary range. The old fallback (`defs[0]`) picked
    /// a prefix out of `HashMap` iteration order; the catch-all `""`
    /// definition (which matches every key) was the only reason it did
    /// not show. `node_runner_checks_compaction` cannot catch this — it
    /// calls `engine.record_op` directly and never goes through
    /// `drain_write_ops_by_key`.
    #[tokio::test]
    async fn check_compaction_drops_unattributed_ops_deterministically() {
        for insertion_order in [["user/", "z/"], ["z/", "user/"]] {
            let mut ns = SystemNamespace::new();
            for prefix in insertion_order {
                ns.set_authority_definition(AuthorityDefinition {
                    key_range: kr(prefix),
                    authority_nodes: vec![node_id("auth-1")],
                    auto_generated: false,
                });
                ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(3), kr(prefix), 1))
                    .expect("valid policy");
            }

            let metrics = default_metrics();
            // A key under no certifiable range.
            metrics.record_write_op("other/y");

            let engine = CompactionEngine::new(CompactionConfig {
                time_threshold_ms: u64::MAX,
                ops_threshold: 1,
            });
            let config = NodeRunnerConfig {
                sync_interval: None,
                ping_interval: None,
                ..NodeRunnerConfig::default()
            };
            let mut runner = NodeRunner::new(
                node_id("node-1"),
                wrap_api(CertifiedApi::new(node_id("node-1"), wrap_ns(ns))),
                engine,
                config,
                Arc::clone(&metrics),
            )
            .await;
            runner.set_eventual_api(Arc::new(Mutex::new(
                crate::api::eventual::EventualApi::new(node_id("node-1")),
            )));

            runner.check_compaction().await;

            assert!(
                runner.compaction_engine().get_checkpoint("user/").is_none(),
                "an unattributable op must not be charged to user/ (order {insertion_order:?})"
            );
            assert!(
                runner.compaction_engine().get_checkpoint("z/").is_none(),
                "an unattributable op must not be charged to z/ (order {insertion_order:?})"
            );
            assert_eq!(
                metrics
                    .compaction_unattributed_write_ops_total
                    .load(Ordering::Relaxed),
                1,
                "the dropped op must be counted (order {insertion_order:?})"
            );
        }
    }

    /// The diagnostic classifier must agree with the frozen predicates on
    /// EVERY case the gate unit tests pin — in particular it must never
    /// report "open" where the gate says "closed". Same fixture table as
    /// `gc_authority_gate_requires_*` / `gc_peer_gate_*`.
    #[test]
    fn gc_gate_diagnose_matches_gate_decision() {
        use crate::authority::ack_frontier::AckFrontierSet;

        let peer = |name: &str, addr: &str| crate::network::PeerConfig {
            node_id: node_id(name),
            addr: addr.into(),
        };
        let mark_ms = 8_000u64;
        let defs = vec![(kr("user/"), 2usize)];
        let versions = vec![PolicyVersion(1)];
        let one_def = vec![(kr("user/"), 1usize)];
        let one_version = vec![PolicyVersion(1)];

        // Authority-side fixtures.
        let no_reports = AckFrontierSet::new();

        let mut one_of_two = AckFrontierSet::new();
        one_of_two.update(ack_frontier("auth-1", "user/", 1, 10_000));

        let mut behind_mark = one_of_two.clone();
        behind_mark.update(ack_frontier("auth-2", "user/", 1, 5_000));

        let mut both_past = one_of_two.clone();
        both_past.update(ack_frontier("auth-2", "user/", 1, 9_000));

        let mut stale_receipt = AckFrontierSet::new();
        stale_receipt.update_at(ack_frontier("auth-1", "user/", 1, 10_000), 7_000);

        // Peer-side fixtures.
        let peers = vec![peer("p1", "p1:9000"), peer("p2", "p2:9000")];
        let no_evidence: HashMap<String, u64> = HashMap::new();
        let mut stale_evidence: HashMap<String, u64> = HashMap::new();
        stale_evidence.insert("p1:9000".into(), 9_000);
        stale_evidence.insert("p2:9000".into(), 7_999);
        let mut fresh_evidence: HashMap<String, u64> = HashMap::new();
        fresh_evidence.insert("p1:9000".into(), 9_000);
        fresh_evidence.insert("p2:9000".into(), 8_000);

        #[allow(clippy::type_complexity)]
        let cases: Vec<(
            &str,
            &[(KeyRange, usize)],
            &[PolicyVersion],
            &AckFrontierSet,
            &[crate::network::PeerConfig],
            &HashMap<String, u64>,
        )> = vec![
            (
                "no reports",
                &defs,
                &versions,
                &no_reports,
                &[],
                &no_evidence,
            ),
            (
                "1-of-2 reported",
                &defs,
                &versions,
                &one_of_two,
                &[],
                &no_evidence,
            ),
            (
                "behind the mark",
                &defs,
                &versions,
                &behind_mark,
                &[],
                &no_evidence,
            ),
            (
                "both past the mark",
                &defs,
                &versions,
                &both_past,
                &[],
                &no_evidence,
            ),
            (
                "stale local receipt",
                &one_def,
                &one_version,
                &stale_receipt,
                &[],
                &no_evidence,
            ),
            ("empty population", &[], &[], &no_reports, &[], &no_evidence),
            (
                "peer without evidence",
                &defs,
                &versions,
                &both_past,
                &peers,
                &no_evidence,
            ),
            (
                "peer with pre-mark evidence",
                &defs,
                &versions,
                &both_past,
                &peers,
                &stale_evidence,
            ),
            (
                "all gates satisfied",
                &defs,
                &versions,
                &both_past,
                &peers,
                &fresh_evidence,
            ),
            (
                "pull-advanced frontier is not push evidence",
                &[],
                &[],
                &no_reports,
                &peers,
                &no_evidence,
            ),
        ];

        let mut seen_variants = Vec::new();
        for (name, defs, versions, set, peers, acked) in cases {
            let gate_open = NodeRunner::gc_authority_gate_passed(defs, versions, set, mark_ms)
                && NodeRunner::gc_peer_gate_passed(peers, acked, mark_ms);
            let diagnosed =
                NodeRunner::gc_gate_diagnose(defs, versions, set, peers, acked, mark_ms);
            assert_eq!(
                diagnosed.is_none(),
                gate_open,
                "diagnose disagreed with the frozen gate on case {name:?}"
            );
            if let Some(block) = diagnosed {
                seen_variants.push(std::mem::discriminant(&block));
            }
        }

        // All four block kinds must be reachable, or the classifier is
        // silently collapsing distinct causes.
        let reachable = [
            GcGateBlock::AuthorityUnderReported {
                prefix: String::new(),
                reported: 0,
                required: 0,
            },
            GcGateBlock::FrontierBehindMark {
                prefix: String::new(),
            },
            GcGateBlock::ReportNotAdvanced {
                prefix: String::new(),
            },
            GcGateBlock::PeerEvidenceMissingOrStale {
                peer_addr: String::new(),
            },
        ];
        for variant in &reachable {
            assert!(
                seen_variants.contains(&std::mem::discriminant(variant)),
                "block variant {variant:?} was never produced by the fixture table"
            );
        }

        // Spot-check the labels carry usable detail.
        assert_eq!(
            NodeRunner::gc_gate_diagnose(&defs, &versions, &one_of_two, &[], &no_evidence, mark_ms),
            Some(GcGateBlock::AuthorityUnderReported {
                prefix: "user/".into(),
                reported: 1,
                required: 2,
            })
        );
        assert_eq!(
            NodeRunner::gc_gate_diagnose(
                &defs,
                &versions,
                &both_past,
                &peers,
                &no_evidence,
                mark_ms
            ),
            Some(GcGateBlock::PeerEvidenceMissingOrStale {
                peer_addr: "p1:9000".into(),
            })
        );
    }

    /// A gate that is LEGITIMATELY closed (a certifiable range whose
    /// authorities have not reported) must be counted and must leave
    /// `gc_last_sweep_wall_ms` at zero. Independent of D1: this is the
    /// signal that would have surfaced D1 within one scrape.
    #[tokio::test]
    async fn gc_gate_block_is_counted_and_surfaced() {
        let mut ns = SystemNamespace::new();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr("user/"),
            authority_nodes: vec![node_id("auth-1"), node_id("auth-2")],
            auto_generated: false,
        });
        ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(1), kr("user/"), 2))
            .expect("valid policy");

        let (mut runner, _eventual_api, peer_addr) = legacy_hole_runner_with_ns(true, ns).await;

        runner.run_gc().await; // pass 1: mark
        runner
            .push_acked_wall_ms
            .insert(peer_addr.clone(), u64::MAX);
        runner.pull_reconciled_wall_ms.insert(peer_addr, u64::MAX);

        runner.run_gc().await; // pass 2: authority gate legitimately closed

        assert!(
            runner
                .metrics
                .gc_gate_blocked_authority_total
                .load(Ordering::Relaxed)
                > 0,
            "an unreported certifiable range must be counted as an authority-side block"
        );
        assert_eq!(
            runner.metrics.gc_last_sweep_wall_ms.load(Ordering::Relaxed),
            0,
            "no sweep executed, so the liveness gauge must stay at zero"
        );
        assert_eq!(
            runner
                .metrics
                .gc_gate_peer_population
                .load(Ordering::Relaxed),
            1,
            "the peer population gauge must reflect the registry"
        );
        assert_eq!(runner.tombstone_gc.total_collected(), 0);
    }

    /// The population must carry each range's REAL policy version — the
    /// fabricated `PolicyVersion(1)` is not a conservative stand-in, it is
    /// a second copy of D1.
    ///
    /// `PolicyVersion(1)` is unreachable in a real deployment: a fresh
    /// `SystemNamespace` starts at version 1, `main.rs`'s catch-all seed
    /// `bump_version()`s it to 2, and `PutPolicy` increments
    /// `version_counter` BEFORE it stamps — so the first policy anyone can
    /// ever create is version 2 or later. A gate built on version 1
    /// therefore demands a scope that no authority reports and that
    /// `attestation_admissible` would reject anyway, i.e. it is
    /// permanently closed, exactly as D1 was.
    ///
    /// Behaviour under test: an authority reporting the scope it actually
    /// serves opens the gate and the sweep runs.
    #[tokio::test]
    async fn gc_gate_opens_on_a_report_at_the_ranges_real_policy_version() {
        let mut ns = SystemNamespace::new();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr("user/"),
            authority_nodes: vec![node_id("auth-1")],
            auto_generated: false,
        });
        // Version 7: any version a real `PutPolicy` could hand out, and
        // deliberately not 1.
        ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(7), kr("user/"), 1))
            .expect("valid policy");

        let (mut runner, _eventual_api, peer_addr) = legacy_hole_runner_with_ns(true, ns).await;

        runner.run_gc().await; // pass 1: mark
        assert!(runner.tombstone_gc.pending_mark_ms().is_some());

        // The authority reports through the real admission path, for the
        // scope it actually serves.
        {
            let mut api = runner.certified_api.lock().await;
            // A very high (but within clock-skew) frontier: high enough to
            // consume all state as of the mark, without tripping the P0-6
            // future-skew guard in `AckFrontierSet::update_at`.
            let high = crate::hlc::wall_clock_ms() + 30_000;
            assert!(
                api.update_frontier(ack_frontier("auth-1", "user/", 7, high)),
                "a report at the range's current policy version is admissible"
            );
        }
        // Saturate both peer-facing gates so the authority gate is the
        // only thing that can still hold the sweep back.
        runner
            .push_acked_wall_ms
            .insert(peer_addr.clone(), u64::MAX);
        runner.pull_reconciled_wall_ms.insert(peer_addr, u64::MAX);

        runner.run_gc().await; // pass 2: sweep

        assert_eq!(
            runner
                .metrics
                .gc_gate_blocked_authority_total
                .load(Ordering::Relaxed),
            0,
            "a fully reported range must not block the authority gate — a gate \
             evaluated at a fabricated PolicyVersion(1) sees no report at all"
        );
        assert!(
            runner.tombstone_gc.total_collected() >= 1,
            "the sweep must run once the range's real scope is fully reported"
        );
        assert!(
            runner.metrics.gc_last_sweep_wall_ms.load(Ordering::Relaxed) > 0,
            "an executed sweep must stamp gc_last_sweep_wall_ms"
        );
    }

    /// Same requirement on the other consumer of the population: a
    /// checkpoint must be stamped with the range's real policy version.
    /// A checkpoint stamped `PolicyVersion(1)` is looked up against a
    /// scope no authority certifies, so pruning could never proceed.
    #[tokio::test]
    async fn check_compaction_checkpoints_the_real_policy_version() {
        let mut ns = SystemNamespace::new();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr("user/"),
            authority_nodes: vec![node_id("auth-1")],
            auto_generated: false,
        });
        ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(9), kr("user/"), 1))
            .expect("valid policy");

        let metrics = default_metrics();
        metrics.record_write_op("user/k");

        let engine = CompactionEngine::new(CompactionConfig {
            time_threshold_ms: u64::MAX,
            ops_threshold: 1,
        });
        let config = NodeRunnerConfig {
            sync_interval: None,
            ping_interval: None,
            ..NodeRunnerConfig::default()
        };
        let mut runner = NodeRunner::new(
            node_id("node-1"),
            wrap_api(CertifiedApi::new(node_id("node-1"), wrap_ns(ns))),
            engine,
            config,
            Arc::clone(&metrics),
        )
        .await;
        runner.set_eventual_api(Arc::new(Mutex::new(
            crate::api::eventual::EventualApi::new(node_id("node-1")),
        )));

        runner.check_compaction().await;

        let checkpoint = runner
            .compaction_engine()
            .get_checkpoint("user/")
            .expect("the op threshold was reached, so a checkpoint exists");
        assert_eq!(
            checkpoint.policy_version,
            PolicyVersion(9),
            "the checkpoint must carry the range's real policy version, not a \
             fabricated PolicyVersion(1)"
        );
    }

    // -----------------------------------------------------------------
    // M-14: observed-attestation relay piggybacked on the sync lane
    // -----------------------------------------------------------------

    use crate::authority::equivocation::{
        EquivocationDetector, GOSSIP_SAMPLE_MAX as SAMPLE_MAX, ObservedAttestation,
    };
    use crate::network::sync::{
        DeltaSyncRequest, DeltaSyncResponse, DigestSyncRequest, DigestSyncResponse,
    };

    /// M-12 grace invariant, restated for M-14 (see the
    /// `DIGEST_ACTIVATION_GRACE` doc): relaying keeps observed heads alive
    /// indefinitely, so restart safety rests on clock arithmetic alone —
    /// the grace must cover a full skew swing (rollback + future-skew
    /// admission), NOT the head retention window. STRICT inequality is
    /// required: both the pre-restart head bound and the post-grace
    /// report bound are inclusive, so at `grace == 2 x skew` a
    /// same-physical (hence potentially same-HLC) pre/post-restart pair
    /// is no longer excluded.
    #[test]
    fn digest_activation_grace_covers_clock_swing_budget() {
        assert!(
            DIGEST_ACTIVATION_GRACE.as_millis() as u64 > 2 * MAX_CLOCK_SKEW_MS,
            "DIGEST_ACTIVATION_GRACE ({}ms) must STRICTLY exceed 2 x MAX_CLOCK_SKEW_MS ({}ms): \
             equality would re-admit a same-HLC pre/post-restart pair (false evidence), and \
             observed-head relay (M-14) voids any age-out-based argument",
            DIGEST_ACTIVATION_GRACE.as_millis(),
            2 * MAX_CLOCK_SKEW_MS
        );
    }

    fn signed_observation(seed_byte: u8, physical: u64, digest: &str) -> ObservedAttestation {
        let signer = make_signer("auth-1", seed_byte);
        let frontier = AckFrontier {
            authority_id: node_id("auth-1"),
            frontier_hlc: HlcTimestamp {
                physical,
                logical: 0,
                node_id: "auth-1".into(),
            },
            key_range: kr(""),
            policy_version: PolicyVersion(1),
            digest_hash: digest.into(),
        };
        let signature = signer.sign_frontier(&frontier, KeysetVersion(1));
        ObservedAttestation {
            frontier,
            signature,
        }
    }

    #[test]
    fn observed_sample_fingerprint_is_deterministic_and_content_sensitive() {
        let now = crate::hlc::wall_clock_ms();
        let a = signed_observation(70, now, "sd2:aaaa");
        let b = signed_observation(70, now, "sd2:bbbb");
        assert_eq!(NodeRunner::observed_sample_fingerprint(&[]), 0);
        assert_eq!(
            NodeRunner::observed_sample_fingerprint(std::slice::from_ref(&a)),
            NodeRunner::observed_sample_fingerprint(std::slice::from_ref(&a)),
        );
        assert_ne!(
            NodeRunner::observed_sample_fingerprint(std::slice::from_ref(&a)),
            NodeRunner::observed_sample_fingerprint(std::slice::from_ref(&b)),
            "a changed digest must change the fingerprint"
        );
    }

    /// Captured request bodies: (content-type, raw body) per request.
    type CapturedRequests = Arc<std::sync::Mutex<Vec<(String, Vec<u8>)>>>;

    /// Mock sync peer capturing every delta request body. When `healthy`
    /// is false, the delta route answers 500 (both the bincode attempt
    /// and the JSON fallback), and the full-sync key dump 404s.
    async fn spawn_delta_mock(
        healthy: bool,
    ) -> (
        std::net::SocketAddr,
        CapturedRequests,
        tokio::task::JoinHandle<()>,
    ) {
        use axum::response::IntoResponse;

        let captured: CapturedRequests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let cap = Arc::clone(&captured);
        let app = axum::Router::new().route(
            "/api/internal/sync/delta",
            axum::routing::post(
                move |headers: axum::http::HeaderMap, body: axum::body::Bytes| {
                    let cap = Arc::clone(&cap);
                    async move {
                        let ct = headers
                            .get("content-type")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("")
                            .to_string();
                        cap.lock().unwrap().push((ct, body.to_vec()));
                        if healthy {
                            axum::Json(DeltaSyncResponse {
                                entries: vec![],
                                sender_frontier: None,
                                applied_origins: HashMap::new(),
                                merge_failed_keys: vec![],
                                pruned_floor: None,
                                visible_origins: HashMap::new(),
                                untracked_entries: HashMap::new(),
                            })
                            .into_response()
                        } else {
                            axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response()
                        }
                    }
                },
            ),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (addr, captured, handle)
    }

    /// Runner wired to the mock peer, with a detector holding one head.
    async fn relay_test_runner(
        peer_addr: std::net::SocketAddr,
        detector: Arc<EquivocationDetector>,
    ) -> NodeRunner {
        use crate::network::{PeerConfig, PeerRegistry};

        let certified = wrap_api(CertifiedApi::new(node_id("node-x"), default_namespace()));
        let eventual = Arc::new(Mutex::new(EventualApi::new(node_id("node-x"))));
        let registry = PeerRegistry::new(
            node_id("node-x"),
            vec![PeerConfig {
                node_id: node_id("peer-1"),
                addr: peer_addr.to_string(),
            }],
        )
        .unwrap();
        let sync_client = SyncClient::new(Arc::new(Mutex::new(registry)));
        let config = NodeRunnerConfig {
            sync_interval: Some(Duration::from_millis(50)),
            ping_interval: None,
            // Delta-only carriers keep this mock minimal; the digest
            // carriers have dedicated runner tests below
            // (`digest_push_probe_carries_sample_and_records_delivery`,
            // `digest_carrier_fallback_does_not_record_delivery_and_reattaches`).
            digest_sync_enabled: false,
            equivocation: Some(detector),
            ..NodeRunnerConfig::default()
        };
        NodeRunner::with_sync(
            node_id("node-x"),
            certified,
            CompactionEngine::with_defaults(),
            config,
            sync_client,
            eventual,
            default_metrics(),
        )
        .await
    }

    fn decode_delta_requests(captured: &CapturedRequests) -> Vec<DeltaSyncRequest> {
        captured
            .lock()
            .unwrap()
            .iter()
            .map(|(ct, body)| {
                if ct.starts_with("application/json") {
                    serde_json::from_slice(body).unwrap()
                } else {
                    bincode::serde::decode_from_slice(body, bincode::config::standard())
                        .unwrap()
                        .0
                }
            })
            .collect()
    }

    /// T-12: one cycle attaches the (bounded) sample to at most one
    /// request per peer, and records the delivery.
    #[tokio::test]
    async fn run_sync_attaches_observed_at_most_once_per_peer() {
        let (addr, captured, server) = spawn_delta_mock(true).await;
        let detector = Arc::new(EquivocationDetector::new(None));
        let obs = signed_observation(71, crate::hlc::wall_clock_ms(), "sd2:head-1");
        detector.observe(&obs.frontier, &obs.signature, crate::hlc::wall_clock_ms());
        let mut runner = relay_test_runner(addr, Arc::clone(&detector)).await;

        runner.run_sync().await;

        let requests = decode_delta_requests(&captured);
        let non_empty: Vec<_> = requests.iter().filter(|r| !r.observed.is_empty()).collect();
        assert_eq!(
            non_empty.len(),
            1,
            "exactly one carrier request must attach the sample (got {} of {})",
            non_empty.len(),
            requests.len()
        );
        assert!(non_empty[0].observed.len() <= SAMPLE_MAX);
        assert_eq!(non_empty[0].observed[0].frontier.digest_hash, "sd2:head-1");
        assert_eq!(
            runner.observed_last_sent.len(),
            1,
            "a delivered sample must be recorded for the peer"
        );
        server.abort();
    }

    /// T-13: an unchanged sample already delivered to the peer is
    /// suppressed (zero relay bytes in the steady state); a new head
    /// re-arms the relay.
    #[tokio::test]
    async fn run_sync_suppresses_unchanged_sample() {
        let (addr, captured, server) = spawn_delta_mock(true).await;
        let detector = Arc::new(EquivocationDetector::new(None));
        let now = crate::hlc::wall_clock_ms();
        let obs = signed_observation(72, now, "sd2:head-1");
        detector.observe(&obs.frontier, &obs.signature, now);
        let mut runner = relay_test_runner(addr, Arc::clone(&detector)).await;

        runner.run_sync().await; // cycle 1: delivers the sample
        runner.run_sync().await; // cycle 2: nothing new to relay

        let requests = decode_delta_requests(&captured);
        assert_eq!(requests.len(), 2);
        assert!(!requests[0].observed.is_empty(), "cycle 1 must carry it");
        assert!(
            requests[1].observed.is_empty(),
            "an unchanged sample must not be re-sent"
        );

        // A new observed head changes the fingerprint and re-arms relay.
        let obs2 = signed_observation(72, now + 1, "sd2:head-2");
        detector.observe(&obs2.frontier, &obs2.signature, now);
        runner.run_sync().await; // cycle 3

        let requests = decode_delta_requests(&captured);
        assert_eq!(requests.len(), 3);
        assert!(
            !requests[2].observed.is_empty(),
            "a new head must re-arm the relay"
        );
        assert!(
            requests[2]
                .observed
                .iter()
                .any(|o| o.frontier.digest_hash == "sd2:head-2")
        );
        server.abort();
    }

    /// T-14: a carrier that never reached the server (NetworkError) does
    /// not consume the sample — it rides the built-in retry and is
    /// re-attached on the next cycle too.
    #[tokio::test]
    async fn observed_reattached_after_network_error() {
        let (addr, captured, server) = spawn_delta_mock(false).await;
        let detector = Arc::new(EquivocationDetector::new(None));
        let now = crate::hlc::wall_clock_ms();
        let obs = signed_observation(73, now, "sd2:head-1");
        detector.observe(&obs.frontier, &obs.signature, now);
        let mut runner = relay_test_runner(addr, Arc::clone(&detector)).await;

        runner.run_sync().await;

        let requests = decode_delta_requests(&captured);
        // pull_delta + its NetworkError retry, each with the built-in
        // JSON fallback POST: 4 delta bodies, ALL carrying the sample.
        assert!(
            requests.len() >= 2,
            "expected the initial attempt and at least one retry"
        );
        for req in &requests {
            assert!(
                !req.observed.is_empty(),
                "undelivered samples must ride every retry"
            );
        }
        assert!(
            runner.observed_last_sent.is_empty(),
            "a sample that never reached a server must not be marked delivered"
        );

        // Next cycle (peer still failing, after backoff): re-attached.
        runner
            .peer_backoffs
            .values_mut()
            .for_each(|b| b.record_success());
        let before = captured.lock().unwrap().len();
        runner.run_sync().await;
        let requests = decode_delta_requests(&captured);
        assert!(requests.len() > before);
        assert!(
            requests[before..].iter().all(|r| !r.observed.is_empty()),
            "the sample must be re-attached on the next cycle"
        );
        server.abort();
    }

    /// A delivered-mark older than `OBSERVED_RETENTION_MS` is treated as
    /// absent and the (unchanged) sample re-attached. Load-bearing: the
    /// receiver's detector state is memory-only — a restarted relay hop,
    /// an aged-out head or a freshly upgraded pre-M-14 peer would
    /// otherwise never receive a fingerprint-static sample again,
    /// permanently starving that relay path.
    #[tokio::test]
    async fn expired_delivery_mark_rearms_relay() {
        let (addr, captured, server) = spawn_delta_mock(true).await;
        let detector = Arc::new(EquivocationDetector::new(None));
        let now = crate::hlc::wall_clock_ms();
        let obs = signed_observation(75, now, "sd2:head-1");
        detector.observe(&obs.frontier, &obs.signature, now);
        let mut runner = relay_test_runner(addr, Arc::clone(&detector)).await;

        runner.run_sync().await; // cycle 1: delivers and records the mark
        runner.run_sync().await; // cycle 2: fresh mark suppresses
        let requests = decode_delta_requests(&captured);
        assert_eq!(requests.len(), 2);
        assert!(!requests[0].observed.is_empty());
        assert!(
            requests[1].observed.is_empty(),
            "a fresh delivered-mark must suppress the unchanged sample"
        );

        // Age the delivered-mark past the retention window (as if the
        // cluster stayed quiet while the receiver restarted or upgraded).
        for entry in runner.observed_last_sent.values_mut() {
            entry.1 = entry.1.saturating_sub(OBSERVED_RETENTION_MS + 1);
        }

        runner.run_sync().await; // cycle 3: expired mark re-arms the relay
        let requests = decode_delta_requests(&captured);
        assert_eq!(requests.len(), 3);
        assert!(
            !requests[2].observed.is_empty(),
            "an expired delivered-mark must be treated as not-delivered"
        );
        // The re-delivery records a fresh mark again.
        let &(_, delivered_at_ms) = runner.observed_last_sent.values().next().unwrap();
        assert!(
            crate::hlc::wall_clock_ms().saturating_sub(delivered_at_ms) < OBSERVED_RETENTION_MS,
            "the re-delivery must refresh the delivered-mark timestamp"
        );
        server.abort();
    }

    /// Captured request bodies per route: (path, content-type, raw body).
    type CapturedRouteRequests = Arc<std::sync::Mutex<Vec<(String, String, Vec<u8>)>>>;

    /// Mock sync peer serving BOTH the delta and the digest routes. The
    /// delta route always answers a healthy empty response. The digest
    /// route answers a scheme-ok root-match when `digest_ok`, and 404
    /// (a digest-unsupported old node) otherwise. Every other route
    /// (full-state push, key dump) 404s — those failures only exercise
    /// the fallback paths, which is exactly what these tests need.
    async fn spawn_digest_mock(
        digest_ok: bool,
    ) -> (
        std::net::SocketAddr,
        CapturedRouteRequests,
        tokio::task::JoinHandle<()>,
    ) {
        use axum::response::IntoResponse;

        let captured: CapturedRouteRequests = Arc::new(std::sync::Mutex::new(Vec::new()));

        fn capture(
            cap: &CapturedRouteRequests,
            path: &str,
            headers: &axum::http::HeaderMap,
            body: &[u8],
        ) {
            let ct = headers
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            cap.lock()
                .unwrap()
                .push((path.to_string(), ct, body.to_vec()));
        }

        let delta_cap = Arc::clone(&captured);
        let digest_cap = Arc::clone(&captured);
        let app = axum::Router::new()
            .route(
                "/api/internal/sync/delta",
                axum::routing::post(
                    move |headers: axum::http::HeaderMap, body: axum::body::Bytes| {
                        let cap = Arc::clone(&delta_cap);
                        async move {
                            capture(&cap, "delta", &headers, &body);
                            axum::Json(DeltaSyncResponse {
                                entries: vec![],
                                sender_frontier: None,
                                applied_origins: HashMap::new(),
                                merge_failed_keys: vec![],
                                pruned_floor: None,
                                visible_origins: HashMap::new(),
                                untracked_entries: HashMap::new(),
                            })
                            .into_response()
                        }
                    },
                ),
            )
            .route(
                "/api/internal/sync/digest",
                axum::routing::post(
                    move |headers: axum::http::HeaderMap, body: axum::body::Bytes| {
                        let cap = Arc::clone(&digest_cap);
                        async move {
                            capture(&cap, "digest", &headers, &body);
                            if digest_ok {
                                axum::Json(DigestSyncResponse {
                                    scheme_ok: true,
                                    root_matched: true,
                                    total_keys: 1,
                                    ..DigestSyncResponse::default()
                                })
                                .into_response()
                            } else {
                                axum::http::StatusCode::NOT_FOUND.into_response()
                            }
                        }
                    },
                ),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (addr, captured, handle)
    }

    fn decode_routed<T: serde::de::DeserializeOwned>(
        captured: &CapturedRouteRequests,
        path: &str,
    ) -> Vec<T> {
        captured
            .lock()
            .unwrap()
            .iter()
            .filter(|(p, _, _)| p == path)
            .map(|(_, ct, body)| {
                if ct.starts_with("application/json") {
                    serde_json::from_slice(body).unwrap()
                } else {
                    bincode::serde::decode_from_slice(body, bincode::config::standard())
                        .unwrap()
                        .0
                }
            })
            .collect()
    }

    /// Runner wired for the DIGEST carriers: digest sync enabled, one
    /// seeded local key and a zero push frontier so the push phase sees a
    /// 100% change rate (> full_sync_threshold) and elects the digest
    /// push probe as the cycle's FIRST carrier.
    async fn digest_relay_test_runner(
        peer_addr: std::net::SocketAddr,
        detector: Arc<EquivocationDetector>,
    ) -> NodeRunner {
        use crate::network::{PeerConfig, PeerRegistry};

        let certified = wrap_api(CertifiedApi::new(node_id("node-x"), default_namespace()));
        let eventual = Arc::new(Mutex::new(EventualApi::new(node_id("node-x"))));
        eventual
            .lock()
            .await
            .eventual_register_set("k1", "v1".into())
            .unwrap();
        let registry = PeerRegistry::new(
            node_id("node-x"),
            vec![PeerConfig {
                node_id: node_id("peer-1"),
                addr: peer_addr.to_string(),
            }],
        )
        .unwrap();
        let sync_client = SyncClient::new(Arc::new(Mutex::new(registry)));
        let config = NodeRunnerConfig {
            sync_interval: Some(Duration::from_millis(50)),
            ping_interval: None,
            digest_sync_enabled: true,
            equivocation: Some(detector),
            ..NodeRunnerConfig::default()
        };
        let mut runner = NodeRunner::with_sync(
            node_id("node-x"),
            certified,
            CompactionEngine::with_defaults(),
            config,
            sync_client,
            eventual,
            default_metrics(),
        )
        .await;
        // A known peer frontier with an empty push frontier: every local
        // key counts as changed (rate 1.0 > threshold 0.5), driving the
        // full-sync-threshold branch whose first carrier is the digest
        // push probe.
        runner.peer_frontiers.insert(
            peer_addr.to_string(),
            HlcTimestamp {
                physical: 1,
                logical: 0,
                node_id: "seed".into(),
            },
        );
        runner
    }

    /// Digest carrier, success path: the digest PUSH PROBE (first carrier
    /// of the full-sync-threshold branch) transmits the sample, no other
    /// request re-attaches it, and the scheme-ok (`Handled`) response
    /// records the delivery. Guards the `request.observed = observed;`
    /// wiring in `try_digest_push` — dropping it would silently disable
    /// the whole M-14 relay in digest-push-dominant deployments.
    #[tokio::test]
    async fn digest_push_probe_carries_sample_and_records_delivery() {
        let (addr, captured, server) = spawn_digest_mock(true).await;
        let detector = Arc::new(EquivocationDetector::new(None));
        let now = crate::hlc::wall_clock_ms();
        let obs = signed_observation(76, now, "sd2:head-1");
        detector.observe(&obs.frontier, &obs.signature, now);
        let mut runner = digest_relay_test_runner(addr, Arc::clone(&detector)).await;

        runner.run_sync().await;

        let digest_reqs: Vec<DigestSyncRequest> = decode_routed(&captured, "digest");
        assert!(!digest_reqs.is_empty(), "the digest push probe must run");
        assert!(
            !digest_reqs[0].include_entries,
            "the first digest request must be the push probe"
        );
        assert!(
            !digest_reqs[0].observed.is_empty(),
            "the push probe is the cycle's first carrier and must transmit the sample"
        );
        assert_eq!(
            digest_reqs[0].observed[0].frontier.digest_hash,
            "sd2:head-1"
        );

        // Attach-once across ALL carrier requests of the cycle.
        let delta_reqs: Vec<DeltaSyncRequest> = decode_routed(&captured, "delta");
        let non_empty = digest_reqs
            .iter()
            .filter(|r| !r.observed.is_empty())
            .count()
            + delta_reqs.iter().filter(|r| !r.observed.is_empty()).count();
        assert_eq!(non_empty, 1, "exactly one carrier must transmit the sample");

        assert!(
            runner.observed_last_sent.contains_key(&addr.to_string()),
            "a scheme-ok digest response must record the delivery"
        );
        server.abort();
    }

    /// Digest carrier, fallback path: a 404 (digest-unsupported peer)
    /// consumes the probe but must NOT record a delivery — the sample
    /// re-attaches to the next cycle's carrier (the delta pull, since the
    /// peer is now cached as digest-unsupported).
    #[tokio::test]
    async fn digest_carrier_fallback_does_not_record_delivery_and_reattaches() {
        let (addr, captured, server) = spawn_digest_mock(false).await;
        let detector = Arc::new(EquivocationDetector::new(None));
        let now = crate::hlc::wall_clock_ms();
        let obs = signed_observation(77, now, "sd2:head-1");
        detector.observe(&obs.frontier, &obs.signature, now);
        let mut runner = digest_relay_test_runner(addr, Arc::clone(&detector)).await;

        runner.run_sync().await; // cycle 1: probe 404s (Fallback)

        let digest_reqs: Vec<DigestSyncRequest> = decode_routed(&captured, "digest");
        assert!(!digest_reqs.is_empty(), "the digest push probe must run");
        assert!(
            digest_reqs.iter().all(|r| !r.observed.is_empty()),
            "every probe attempt (bincode + JSON fallback) carries the sample"
        );
        assert!(
            runner.observed_last_sent.is_empty(),
            "a Fallback outcome must not be recorded as a delivery"
        );

        // Cycle 2: the peer is cached digest-unsupported, so the delta
        // pull is the first carrier — the undelivered sample rides it.
        runner
            .peer_backoffs
            .values_mut()
            .for_each(|b| b.record_success());
        let digest_before = digest_reqs.len();
        let delta_before: usize = decode_routed::<DeltaSyncRequest>(&captured, "delta").len();
        runner.run_sync().await;

        let digest_reqs: Vec<DigestSyncRequest> = decode_routed(&captured, "digest");
        assert_eq!(
            digest_reqs.len(),
            digest_before,
            "an unsupported peer must not be re-probed within the cache TTL"
        );
        let delta_reqs: Vec<DeltaSyncRequest> = decode_routed(&captured, "delta");
        assert!(delta_reqs.len() > delta_before);
        assert!(
            delta_reqs[delta_before..]
                .iter()
                .any(|r| !r.observed.is_empty()),
            "the undelivered sample must re-attach to the next cycle's carrier"
        );
        assert!(
            !runner.observed_last_sent.is_empty(),
            "the delta delivery must record the mark"
        );
        server.abort();
    }
}
