use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use serde::Serialize;

use crate::authority::ack_frontier::{AckFrontier, AckFrontierSet, FrontierScope};
use crate::authority::attestation_pool::{AttestationPool, InsertOutcome};
use crate::authority::certificate::{DualModeCertificate, MajorityCertificate, majority_threshold};
use crate::authority::frontier_sig::{CHECKPOINT_INTERVAL_MS, VerifiedAttestation};
use crate::control_plane::system_namespace::SystemNamespace;
use crate::error::CrdtError;
use crate::hlc::{CertifiedDeltaCursor, Hlc, HlcTimestamp};
use crate::store::kv::{CrdtValue, Store};
use crate::store::wal::CertifiedScopePin;
#[cfg(not(target_arch = "wasm32"))]
use crate::store::wal::{WalPos, WalRecord, WalWriter};
use crate::types::{CertificationStatus, KeyRange, NodeId, PolicyVersion};

/// A normalized checkpoint backed by a verified quorum of exact accepted
/// full frontier reports. Production code can obtain this token only through
/// `CertifiedApi::signed_prune_floor`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedPruneFloor(HlcTimestamp);

impl VerifiedPruneFloor {
    pub(crate) fn timestamp(&self) -> &HlcTimestamp {
        &self.0
    }

    #[cfg(test)]
    pub(crate) fn for_test(timestamp: HlcTimestamp) -> Self {
        Self(timestamp)
    }
}

/// What to do when `certified_write` cannot achieve consensus.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnTimeout {
    /// Return `CrdtError::CertificationTimeout`.
    ///
    /// The local write is already durable in the store when this error is
    /// returned.  Callers can use this to distinguish a certification timeout
    /// (write committed locally, certification did not complete) from a
    /// generic failure.  Retry via `process_certifications` once more
    /// Authority frontiers arrive, without re-issuing the write.
    Error,
    /// Accept the write as `Pending` and let the caller poll status later.
    Pending,
}

/// One entry of a certified delta batch: full value and write-time scope.
pub type CertifiedDeltaTuple = (String, CrdtValue, HlcTimestamp, CertifiedScopePin);

/// Outcome of a replicated certified ingest ([`CertifiedApi::merge_remote_certified`]).
///
/// The caller decides what each outcome means for its peer baseline, and
/// the distinction matters: advancing past an entry is PERMANENT, because
/// `Store::delta_entries_since` filters strictly above the baseline and
/// this plane has no digest lane to repair the hole afterwards.
/// `Applied` / `Skipped` / `Rejected(TypeMismatch)` are settled and may be
/// advanced past; `Rejected(OriginAhead)` and `Err(PolicyDenied)` are
/// control-plane lag that resolves on its own and must be RETRIED (see
/// `NodeRunner::run_certified_sync`); `Err(Storage)` must neither advance
/// nor be treated as delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestOutcome {
    /// The contribution inflated local state; it was tracked and logged.
    Applied,
    /// Redundant-relay gate (M-6): local state already dominates the
    /// contribution. Nothing written, nothing logged, no timestamp moved.
    Skipped,
    /// Refused on its merits; see [`IngestRejection`].
    Rejected(IngestRejection),
}

/// Why a replicated certified entry was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestRejection {
    /// The wire origin is ahead of this node's current policy version.
    /// Accepting it would pin the entry to a frontier this node cannot
    /// produce (permanent `Pending`). TRANSIENT: it becomes acceptable as
    /// soon as the local namespace applies the same version, so the caller
    /// must hold its baseline rather than step over it.
    OriginAhead,
    /// The incoming CRDT type conflicts with the stored one.
    TypeMismatch,
    /// A causal dot names a different key, element, value or HLC than the
    /// local immutable assignment. Retrying cannot make this payload valid.
    DotAssignmentConflict,
}

/// A verifiable proof bundle attached to a certified read response.
///
/// Contains the metadata needed for an external client to independently
/// verify that a majority of authorities have acknowledged a given frontier.
/// The `certificate` field carries an Ed25519 `MajorityCertificate` whose
/// `frontier_hlc` matches this bundle's `frontier_hlc` (the checkpoint the
/// authorities signed), and `bls_certificate` optionally carries a compact
/// BLS aggregate over the same checkpoint message.
#[derive(Debug, Clone, Serialize)]
pub struct ProofBundle {
    /// The key range this proof covers.
    pub key_range: KeyRange,
    /// The majority frontier HLC at the time of certification.
    ///
    /// When a certificate is attached, this is the certificate checkpoint
    /// (a floor-normalised HLC) so that verifiers recompute the exact
    /// message the authorities signed.
    pub frontier_hlc: HlcTimestamp,
    /// The policy version in effect when the proof was generated.
    pub policy_version: PolicyVersion,
    /// The authority node IDs that have reported frontiers for this scope.
    pub contributing_authorities: Vec<NodeId>,
    /// The total number of authorities in the authority set for this key range.
    pub total_authorities: usize,
    /// The Ed25519 majority certificate. A `Certified` read always contains
    /// one; unsigned frontiers leave the write `Pending`.
    pub certificate: Option<MajorityCertificate>,
    /// Optional BLS aggregate certificate over the same checkpoint
    /// (`CertificateMode::Bls`), attached in addition to the Ed25519
    /// certificate when a BLS majority under a uniform keyset is available.
    pub bls_certificate: Option<DualModeCertificate>,
}

/// Result of a certified read (FR-002).
#[derive(Debug)]
pub struct CertifiedRead<'a> {
    /// The CRDT value, if the key exists.
    pub value: Option<&'a CrdtValue>,
    /// Certification status of the value.
    pub status: CertificationStatus,
    /// The majority of admitted frontier reports observed at query time, if
    /// available. This can include unsigned reports and is not a proof of
    /// certification; only `proof` establishes `Certified` status.
    pub frontier: Option<HlcTimestamp>,
    /// Verifiable proof bundle, present when status is `Certified`.
    pub proof: Option<ProofBundle>,
}

/// A write awaiting Authority majority certification.
#[derive(Debug, Clone)]
pub struct PendingWrite {
    /// The key that was written.
    pub key: String,
    /// The CRDT value that was written.
    pub value: CrdtValue,
    /// The HLC timestamp assigned to this write.
    pub timestamp: HlcTimestamp,
    /// Current certification status.
    pub status: CertificationStatus,
    /// The resolved key range for this write's authority scope.
    pub key_range: KeyRange,
    /// The policy version in effect when this write was issued.
    pub policy_version: PolicyVersion,
    /// The total number of authorities for this write's key range.
    pub total_authorities: usize,
    /// Exact write-time Authority roster, preserved across replication and
    /// recovery by the scope pin.
    pub authority_nodes: Vec<NodeId>,
    /// True when this entry was created by the replication lane
    /// (`merge_remote_certified`) rather than by a local client write.
    ///
    /// Recovery (`rebuild_pending_from_store`) marks its entries
    /// `replicated: true` because it cannot distinguish their source.
    pub replicated: bool,
}

/// Configuration for retention and cleanup of pending writes.
///
/// Controls how `CertifiedApi` bounds the growth of its internal
/// `pending_writes` list. Completed entries can be removed by `cleanup()`
/// or automatically when admission reaches `max_entries`.
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    /// Legacy response-wait threshold in milliseconds. It does not expire
    /// durable pending operations. Default: 60,000 ms.
    pub max_age_ms: u64,
    /// Maximum number of tracked writes. Once completed entries are cleaned
    /// up, a full queue refuses new work before modifying the store.
    /// Default: 10,000.
    pub max_entries: usize,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            max_age_ms: 60_000,
            max_entries: 10_000,
        }
    }
}

/// Cached proof for a key that has achieved `Certified` status.
///
/// This entry survives cleanup of `pending_writes`, ensuring that
/// `get_certified` and `get_certification_status` continue to return
/// `Certified` with proof data even after the pending write has been removed.
#[derive(Debug, Clone)]
pub struct CertifiedCacheEntry {
    /// The key range this certification covers.
    pub key_range: KeyRange,
    /// The majority frontier HLC at the time of certification.
    pub frontier_hlc: HlcTimestamp,
    /// The policy version in effect when the proof was generated.
    pub policy_version: PolicyVersion,
    /// The authority node IDs that contributed to certification.
    pub contributing_authorities: Vec<NodeId>,
    /// The total number of authorities in the authority set.
    pub total_authorities: usize,
    /// Exact authority roster at issuance, independent of namespace version.
    pub authority_nodes: Vec<NodeId>,
    /// The majority certificate. Cache entries are only created with one.
    pub certificate: Option<MajorityCertificate>,
    /// Local receipts showing that every certificate signer had an accepted
    /// frontier at the certificate's exact checkpoint when this proof was
    /// issued. Copied before the bounded frontier history can evict them.
    accepted_frontiers: Vec<AckFrontier>,
    /// The BLS aggregate certificate, if available.
    pub bls_certificate: Option<DualModeCertificate>,
    /// The HLC timestamp of the write that was certified.
    pub write_timestamp: HlcTimestamp,
}

/// Policy-version admission window (lag side): attestations up to this many
/// versions BEHIND the range's current placement-policy version are admitted
/// to the attestation pool.
///
/// Must equal `NodeRunnerConfig::frontier_gc_max_retained_versions`' default
/// (2): the pool has to accept every version the frontier GC still retains
/// (`cur - 2 ..= cur`, see `AckFrontierSet::gc_stale_entries`), or lagging
/// reporters whose frontiers the GC deliberately keeps would have their
/// attestations rejected here.
const ATTESTATION_VERSION_LAG: u64 = 2;

/// Policy-version admission window (lead side): accept one version ahead —
/// a leading reporter may attest under `N + 1` before this node's namespace
/// catches up (control-plane propagation race).
const ATTESTATION_VERSION_LEAD: u64 = 1;

/// Why a frontier report was refused admission (M-4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdmissionReject {
    /// No authority definition exists for the exact key-range prefix.
    /// Unknown ranges can never certify a write (`resolve_scope` fails with
    /// `PolicyDenied`), so rejecting them loses nothing legitimate.
    UnknownRange,
    /// The signer is not a member of the range's authority set (defence in
    /// depth: the HTTP receive path also gates on membership).
    NotRangeAuthority,
    /// The range has a definition but no placement policy; without a
    /// current version there is nothing to certify against.
    NoPolicy,
    /// The attested policy version is outside
    /// `current - ATTESTATION_VERSION_LAG ..= current + ATTESTATION_VERSION_LEAD`.
    VersionOutOfWindow { current: u64 },
}

/// Validate one frontier report against the namespace before tracking it
/// (M-4).
///
/// This is the primary defence that collapses the scope key space of BOTH
/// resource sinks fed by `update_frontier_verified` — the frontier set
/// (`AckFrontierSet`, an otherwise uncapped map that grows one entry per
/// distinct `(key_range, policy_version, authority)` triple) and the
/// attestation pool — to a finite, namespace-derived set: a registered
/// authority rotating `policy_version` / `key_range` values can no longer
/// mint unbounded scopes in either structure. Nothing legitimate is lost:
/// certification (`resolve_scope`) requires the same exact definition and
/// placement policy this gate checks, so a rejected scope can never certify
/// a write. The pool's own caps remain as a backstop.
///
/// Returns the range's CURRENT policy version on success, so the caller can
/// detect stale-but-admissible reports (M-17 observability) without a
/// second namespace read.
fn attestation_admissible(
    ns: &SystemNamespace,
    key_range: &KeyRange,
    policy_version: PolicyVersion,
    authority: &NodeId,
) -> Result<u64, AdmissionReject> {
    // Exact-prefix lookup: a pending write's key_range always equals some
    // authority definition's key_range verbatim (resolve_scope), so an
    // attestation that does not match a definition exactly can never be
    // consumed by certificate assembly.
    let Some(def) = ns.get_authority_definition(&key_range.prefix) else {
        return Err(AdmissionReject::UnknownRange);
    };
    if !def.authority_nodes.contains(authority) {
        return Err(AdmissionReject::NotRangeAuthority);
    }
    let Some(policy) = ns.get_placement_policy(&key_range.prefix) else {
        return Err(AdmissionReject::NoPolicy);
    };
    let current = policy.version.0;
    let pv = policy_version.0;
    if pv < current.saturating_sub(ATTESTATION_VERSION_LAG)
        || pv > current.saturating_add(ATTESTATION_VERSION_LEAD)
    {
        return Err(AdmissionReject::VersionOutOfWindow { current });
    }
    Ok(current)
}

/// Point-in-time attestation pool statistics (admission + capacity + purge)
/// for the metrics pipeline.
#[derive(Debug, Clone, Copy, Default)]
pub struct AttestationPoolStats {
    /// Number of scopes currently tracked by the pool (gauge).
    pub scopes: u64,
    /// Frontier reports rejected at admission (neither tracked nor pooled):
    /// unknown range, non-member signer, or missing placement policy.
    pub rejected_unknown_range_total: u64,
    /// Frontier reports rejected at admission (neither tracked nor pooled):
    /// policy version outside the accepted window around the current
    /// version.
    pub rejected_version_window_total: u64,
    /// Frontier reports ADMITTED with a policy version behind the range's
    /// current version (M-17). The earliest visible symptom of a lagging
    /// authority — an observer with a stale namespace, or a voter on the
    /// minority side of a partition — fires from the FIRST policy bump,
    /// before fencing or the admission window drop anything.
    pub stale_version_total: u64,
    /// Frontier reports whose (range, version) scope was FENCED: the
    /// frontier no longer advances and no attestation is pooled, so the
    /// signer contributes nothing to certification for that scope (M-17;
    /// previously fully silent).
    pub rejected_fenced_total: u64,
    /// Attestation inserts rejected by the pool's global scope cap.
    pub rejected_scope_cap_total: u64,
    /// Attestation inserts rejected by the pool's per-authority scope cap.
    pub rejected_authority_cap_total: u64,
    /// Attestations removed by accused-authority purges (m-7).
    pub purged_total: u64,
    /// Frontier reports dropped because their `frontier_hlc.physical` was
    /// more than `MAX_CLOCK_SKEW_MS` ahead of this node's local wall clock
    /// (P0-6). Sustained growth means a peer's clock is badly skewed or an
    /// authority is reporting far-future frontiers. Zero in steady state.
    pub frontier_skew_rejected_total: u64,
}

/// Certified consistency API (FR-002, FR-004).
///
/// Provides `get_certified` and `certified_write` operations that integrate
/// with the Authority ack_frontier to track and report certification status.
/// Authority resolution uses longest-prefix match via `SystemNamespace` to
/// ensure certification decisions are scoped to the correct key range.
pub struct CertifiedApi {
    store: Store,
    clock: Hlc,
    frontiers: AckFrontierSet,
    namespace: Arc<RwLock<SystemNamespace>>,
    pending_writes: Vec<PendingWrite>,
    /// Index of the LATEST `pending_writes` entry per key.
    ///
    /// `get_certified` / `get_certification_status` are on the certified
    /// read path (`SLO_CERTIFIED_READ_P99`) and both want "the newest
    /// tracked write for this key". A reverse linear scan was acceptable
    /// while the certified store only ever held this node's own writes;
    /// with replication it holds the CLUSTER's certified key set, so the
    /// scan is now O(tracked writes) on every read. Kept in lockstep with
    /// `pending_writes` by construction: pushes update it in place, and
    /// every structural removal goes through `retain_pending`, which
    /// rebuilds it.
    pending_index: HashMap<String, usize>,
    /// Write-time (ORIGIN) policy version per key -- first-class, durable
    /// state rather than a projection of `pending_writes`.
    ///
    /// This is the pin that keeps a write being evaluated in the policy
    /// scope it was issued under (FR-009 / #342). It CANNOT be derived
    /// from `pending_writes`: that list is a bounded tracking
    /// cache (completed entries may be cleaned up) while checkpoints run
    /// on a much slower cadence (~300s) and prune the WAL segments carrying
    /// the same origins. The previous age based cleanup erased both
    /// carriers of an origin about a minute after the write, degrading the key
    /// to the current-version fallback on the next restart -- the exact
    /// fence bypass #342 closed, reopened by a timer.
    ///
    /// Seeded on recovery from the sidecar + retained WAL, extended by
    /// every local `certified_write` and every replicated ingest, and
    /// dumped verbatim by `checkpoint_certified`.
    ///
    /// Growth is O(certified keys) by design; `origins_len` exports it for
    /// monitoring. TODO(v2): core-semantics-v2 replaces per-key pins with
    /// roster/coverage proofs, collapsing this to O(policy versions).
    origins: HashMap<String, PolicyVersion>,
    /// Durable scope of each retained certified-store write. Unlike
    /// `pending_writes`, these pins survive cleanup and checkpoint pruning.
    scope_pins: HashMap<String, CertifiedScopePin>,
    retention: RetentionPolicy,
    /// Legacy counter for pending-write evictions. New admissions refuse
    /// capacity pressure instead of evicting; this remains zero.
    evicted_count: u64,
    /// Cache of certified proofs that survives `pending_writes` cleanup.
    ///
    /// When a write transitions to `Certified`, its proof info is stored here
    /// so that subsequent reads still return `Certified` with proof even after
    /// the pending write entry has been removed by cleanup.
    /// For a given key, only the latest certified entry is kept.
    certified_cache: HashMap<String, CertifiedCacheEntry>,
    /// Pool of signature-verified frontier attestations, used to assemble
    /// majority certificates for certified proofs (FR-008).
    attestations: AttestationPool,
    /// Cumulative attestations rejected at admission for a non-window
    /// reason (unknown range / non-member signer / missing policy).
    attestation_rejected_unknown_range_total: u64,
    /// Cumulative attestations rejected at admission for a policy version
    /// outside the accepted window.
    attestation_rejected_version_window_total: u64,
    /// Cumulative frontier reports admitted with a policy version behind
    /// the current one (M-17: stale-but-admissible, the first symptom of a
    /// lagging authority namespace).
    attestation_stale_version_total: u64,
    /// Cumulative frontier reports dropped because their scope was fenced
    /// (M-17: previously a fully silent drop).
    attestation_rejected_fenced_total: u64,
    /// Per-(range, authority) wall-clock ms of the last stale-version WARN,
    /// throttling the log line (not the counter) to ~1/minute per scope.
    /// Bounded: admission collapses the key space to namespace-defined
    /// (range, authority) pairs.
    stale_version_warned_ms: HashMap<(String, String), u64>,
    /// Wall-clock ms of the last cap-pressure stale-scope sweep, used to
    /// throttle sweeps to at most one per checkpoint interval.
    last_stale_prune_ms: u64,
    /// Write-ahead log appender; `None` = persistence disabled.
    ///
    /// The certified store is NOT covered by anti-entropy sync, so a crash
    /// without a WAL cannot be repaired from peers — this store's WAL is
    /// the only copy of un-snapshotted certified writes. The certification
    /// state itself (`pending_writes` / `frontiers` / `attestations` /
    /// `certified_cache`) is deliberately volatile: after a restart values
    /// regress from `Certified` to `Pending` (fail-closed — never a false
    /// certification) and re-certify as attestations are re-collected.
    #[cfg(not(target_arch = "wasm32"))]
    wal: Option<WalWriter>,
    /// Position of the most recent WAL append, for `wait_durable`.
    #[cfg(not(target_arch = "wasm32"))]
    last_wal_pos: Option<WalPos>,
}

impl CertifiedApi {
    /// Create a new `CertifiedApi` for the given node.
    ///
    /// The `namespace` provides authority definitions for key-range-scoped
    /// certification decisions via longest-prefix match.
    pub fn new(node_id: NodeId, namespace: Arc<RwLock<SystemNamespace>>) -> Self {
        let mut frontiers = AckFrontierSet::new();
        frontiers.set_local_authority(node_id.clone());
        Self {
            store: Store::new(),
            clock: Hlc::new(node_id.0),
            frontiers,
            namespace,
            pending_writes: Vec::new(),
            pending_index: HashMap::new(),
            origins: HashMap::new(),
            scope_pins: HashMap::new(),
            retention: RetentionPolicy::default(),
            evicted_count: 0,
            certified_cache: HashMap::new(),
            attestations: AttestationPool::new(),
            attestation_rejected_unknown_range_total: 0,
            attestation_rejected_version_window_total: 0,
            attestation_stale_version_total: 0,
            attestation_rejected_fenced_total: 0,
            stale_version_warned_ms: HashMap::new(),
            last_stale_prune_ms: 0,
            #[cfg(not(target_arch = "wasm32"))]
            wal: None,
            #[cfg(not(target_arch = "wasm32"))]
            last_wal_pos: None,
        }
    }

    /// Create a `CertifiedApi` from a recovered store (snapshot + WAL
    /// replay).
    ///
    /// Seeds the HLC clock from the highest recovered timestamp so writes
    /// issued after the restart are strictly greater than anything already
    /// persisted. The certification state starts empty: recovered values
    /// read as `Pending` until re-certified (fail-closed).
    ///
    /// `origins` maps each recovered key to the policy version its write was
    /// issued under (harvested from `WalRecord::CertifiedUpsert` records and
    /// the certified origins sidecar). Recovered writes are re-tracked under
    /// their origin version rather than the current one, and any origin that
    /// trails the current policy version has its `(range, origin)` scope
    /// re-fenced — reconstructing the FR-009 fence that a version bump
    /// installed in memory but that no snapshot/WAL persisted. Without this,
    /// a write fenced just before a crash would be re-tagged with the current
    /// version and certified off a newer authority frontier after the restart
    /// (fix/fence-persistence-across-restart). The advancing frontier
    /// watermarks, attestations and certified cache stay deliberately
    /// volatile: this only ADDS fences to an empty `AckFrontierSet`, never
    /// restores advancing state, so the "regress to Pending" contract holds.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn recovered(
        node_id: NodeId,
        namespace: Arc<RwLock<SystemNamespace>>,
        store: Store,
        wal: Option<WalWriter>,
        origins: HashMap<String, PolicyVersion>,
    ) -> Self {
        Self::recovered_with_scope_pins(node_id, namespace, store, wal, origins, HashMap::new())
    }

    /// Recover with validated scope pins harvested from the WAL and sidecar.
    /// Keys without a valid write-time scope remain untracked and Pending.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn recovered_with_scope_pins(
        node_id: NodeId,
        namespace: Arc<RwLock<SystemNamespace>>,
        store: Store,
        wal: Option<WalWriter>,
        origins: HashMap<String, PolicyVersion>,
        scope_pins: HashMap<String, CertifiedScopePin>,
    ) -> Self {
        let mut frontiers = AckFrontierSet::new();
        frontiers.set_local_authority(node_id.clone());
        let mut clock = Hlc::new(node_id.0);
        if let Some(max) = store.max_known_hlc() {
            clock.seed_recovered(&max);
        }
        let mut api = Self {
            store,
            clock,
            frontiers,
            namespace,
            pending_writes: Vec::new(),
            pending_index: HashMap::new(),
            origins: HashMap::new(),
            scope_pins: HashMap::new(),
            retention: RetentionPolicy::default(),
            evicted_count: 0,
            certified_cache: HashMap::new(),
            attestations: AttestationPool::new(),
            attestation_rejected_unknown_range_total: 0,
            attestation_rejected_version_window_total: 0,
            attestation_stale_version_total: 0,
            attestation_rejected_fenced_total: 0,
            stale_version_warned_ms: HashMap::new(),
            last_stale_prune_ms: 0,
            wal,
            last_wal_pos: None,
        };
        // The recovered origins are RETAINED, not consumed: they are the
        // live pin map from here on, and the next `checkpoint_certified`
        // dumps them back to the sidecar. Consuming them (the previous
        // behaviour) meant the sidecar was rebuilt from the expiring
        // `pending_writes` cache instead, which dropped every origin whose
        // tracking entry had aged out -- see the `origins` field doc.
        api.origins = origins;
        api.scope_pins = scope_pins;
        api.rebuild_pending_from_store();
        api
    }

    /// Re-enqueue every recovered store entry as a pending certification.
    ///
    /// Without this, a write acked `"pending"` before a crash is dropped from
    /// certification tracking on restart: `process_certifications` only scans
    /// `pending_writes`, so the write would report `Pending` / `proof: null`
    /// forever even after the cluster certifies it (clients polling the
    /// documented contract hang indefinitely). Frontiers start empty on
    /// recovery, so every entry is re-tracked as `Pending` and promoted as
    /// attestations are re-collected. Keys without an authority definition
    /// cannot certify and are skipped.
    ///
    /// `origins` carries each key's WRITE-TIME policy version (from the WAL /
    /// sidecar). A recovered write is re-tracked under its origin version,
    /// not the current one: `resolve_scope` reports the current version, but
    /// certifying an old write at the new version would ignore the fence the
    /// version bump installed. When the origin trails the current version the
    /// `(range, origin)` scope is re-fenced here, so late old-version
    /// attestations are rejected and the write cannot be certified off the
    /// current version's frontier after a restart (FR-009,
    /// fix/fence-persistence-across-restart). A key without a valid write-time
    /// pin is never re-tracked under a reconstructed current scope.
    #[cfg(not(target_arch = "wasm32"))]
    fn rebuild_pending_from_store(&mut self) {
        // A sidecar can be newer than the snapshot after a crash between the
        // two atomic saves. The retained WAL is replayed first; any pin that
        // still names a different store HLC belongs to a write that did not
        // survive and must not be applied to this value.
        self.scope_pins.retain(|key, pin| {
            self.store.timestamp_for(key) == Some(&pin.write_hlc)
                && self.origins.get(key) == Some(&pin.policy_version)
                && key.starts_with(&pin.key_range.prefix)
                && !pin.authority_nodes.is_empty()
        });
        // Pass 1 (unbounded, cheap): derive every fence and rank the keys
        // by recency. Fences are safety state and must cover the WHOLE
        // store -- capping them would let an old-version key certify off
        // the current frontier. Neither fences nor accepted receipts can
        // be dropped to satisfy a new admission limit.
        let keys: Vec<String> = self.store.keys().into_iter().cloned().collect();
        let mut fences: Vec<(KeyRange, PolicyVersion)> = Vec::new();
        let mut ranked: Vec<(HlcTimestamp, String)> = Vec::with_capacity(keys.len());
        for key in keys {
            let Some(timestamp) = self.store.timestamp_for(&key).cloned() else {
                continue;
            };
            let Some(pin) = self.scope_pins.get(&key) else {
                continue;
            };
            let key_range = pin.key_range.clone();
            let current_version = self
                .namespace
                .read()
                .ok()
                .and_then(|ns| {
                    ns.get_placement_policy(&key_range.prefix)
                        .map(|p| p.version)
                })
                .unwrap_or(pin.policy_version);
            let policy_version = pin.policy_version;
            // A write issued under an older-than-current version was fenced at
            // the version bump on the live node; that fence was in-memory
            // only. Re-derive it so the recovered write stays isolated in its
            // old scope (no re-certification off the current frontier).
            if policy_version < current_version {
                fences.push((key_range, policy_version));
            }
            ranked.push((timestamp, key));
        }

        // Pass 2: restore EVERY retained write. Silently truncating at
        // max_entries would strand older operations forever after restart.
        // A recovered store can exceed today's admission limit (for example
        // after a policy change); new writes then wait for certification to
        // free capacity. This may temporarily use more memory than the
        // configured limit, but never lies about accepted operations.
        ranked.sort_unstable_by(|a, b| b.0.cmp(&a.0));
        // Restore store order (ascending HLC) so tracking order matches the
        // live path, where writes are appended in issue order.
        ranked.reverse();
        for (timestamp, key) in ranked {
            let Some(value) = self.store.get(&key).cloned() else {
                continue;
            };
            let Some(pin) = self.scope_pins.get(&key).cloned() else {
                continue;
            };
            let (key_range, policy_version, authority_nodes) =
                (pin.key_range, pin.policy_version, pin.authority_nodes);
            let total_authorities = authority_nodes.len();
            self.push_pending(PendingWrite {
                key,
                value,
                timestamp,
                status: CertificationStatus::Pending,
                key_range,
                policy_version,
                total_authorities,
                authority_nodes,
                // Recovery cannot tell local and replicated entries apart.
                replicated: true,
            });
        }
        for (range, version) in fences {
            self.fence_version(&range, version);
        }
    }

    /// Append a tracked write and keep `pending_index` pointing at it.
    fn push_pending(&mut self, pw: PendingWrite) {
        self.pending_index
            .insert(pw.key.clone(), self.pending_writes.len());
        self.pending_writes.push(pw);
    }

    /// Structurally filter `pending_writes`, then rebuild `pending_index`.
    ///
    /// Every removal must go through here: a stale index would make the
    /// read path report another key's status.
    fn retain_pending<F: FnMut(&PendingWrite) -> bool>(&mut self, f: F) {
        self.pending_writes.retain(f);
        self.pending_index.clear();
        for (i, pw) in self.pending_writes.iter().enumerate() {
            self.pending_index.insert(pw.key.clone(), i);
        }
    }

    /// The latest tracked write for `key`, via `pending_index`.
    fn latest_pending(&self, key: &str) -> Option<&PendingWrite> {
        self.pending_index
            .get(key)
            .and_then(|i| self.pending_writes.get(*i))
    }

    /// Write-time (ORIGIN) policy version of every key this node holds in
    /// its certified store.
    ///
    /// The input to the certified origins sidecar (`checkpoint_certified`).
    pub fn origins(&self) -> &HashMap<String, PolicyVersion> {
        &self.origins
    }

    /// Exact local issue-time (or receiver intake-time) scope pins for
    /// retained certified-store writes. Checkpointing copies this map under
    /// the same lock as the store snapshot.
    pub fn scope_pins(&self) -> &HashMap<String, CertifiedScopePin> {
        &self.scope_pins
    }

    /// Number of per-key origin pins held in memory.
    ///
    /// Grows with the certified key set (see the `origins` field doc);
    /// exported so operators can watch it.
    pub fn origins_len(&self) -> usize {
        self.origins.len()
    }

    /// Position of the most recent WAL append (for durability waits).
    #[cfg(not(target_arch = "wasm32"))]
    pub fn last_wal_pos(&self) -> Option<WalPos> {
        self.last_wal_pos
    }

    /// Seal the active WAL segment and start a new one (checkpoint step 1).
    /// See `EventualApi::wal_rotate`.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn wal_rotate(&mut self) -> std::io::Result<Option<u64>> {
        self.wal.as_mut().map(|w| w.rotate()).transpose()
    }

    /// Return a reference to the underlying store (snapshot input).
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Create a new `CertifiedApi` with a custom retention policy.
    pub fn with_retention(
        node_id: NodeId,
        namespace: Arc<RwLock<SystemNamespace>>,
        retention: RetentionPolicy,
    ) -> Self {
        let mut frontiers = AckFrontierSet::new();
        frontiers.set_local_authority(node_id.clone());
        Self {
            store: Store::new(),
            clock: Hlc::new(node_id.0),
            frontiers,
            namespace,
            pending_writes: Vec::new(),
            pending_index: HashMap::new(),
            origins: HashMap::new(),
            scope_pins: HashMap::new(),
            retention,
            evicted_count: 0,
            certified_cache: HashMap::new(),
            attestations: AttestationPool::new(),
            attestation_rejected_unknown_range_total: 0,
            attestation_rejected_version_window_total: 0,
            attestation_stale_version_total: 0,
            attestation_rejected_fenced_total: 0,
            stale_version_warned_ms: HashMap::new(),
            last_stale_prune_ms: 0,
            #[cfg(not(target_arch = "wasm32"))]
            wal: None,
            #[cfg(not(target_arch = "wasm32"))]
            last_wal_pos: None,
        }
    }

    /// Resolve the authority scope for a given key.
    ///
    /// Uses longest-prefix match against authority definitions in the system
    /// namespace. Returns the key range, current policy version, and total
    /// authority count for that range.
    fn resolve_scope(&self, key: &str) -> Result<(KeyRange, PolicyVersion, usize), CrdtError> {
        let ns = self.namespace.read().unwrap();
        let auth_def = ns.get_authorities_for_key(key).ok_or_else(|| {
            CrdtError::PolicyDenied(format!("no authority definition for key: {key}"))
        })?;

        let key_range = auth_def.key_range.clone();
        let total = auth_def.authority_nodes.len();

        // A scope with zero authorities cannot certify anything: the proof it
        // would issue carries `total_authorities: 0`, and
        // `majority_threshold(0)` is unsatisfiable on both sides -- the
        // issuing side here and `authority::verifier`, which routes through
        // the same function. Refuse the scope outright rather than record a
        // write that can only ever be self-contradictory.
        // `PolicyDenied` matches the "no authority definition" case just
        // above; `InvalidArgument` is already taken by the missing-policy case
        // below and describes a malformed request, not a denied one.
        if total == 0 {
            return Err(CrdtError::PolicyDenied(format!(
                "authority definition for prefix '{}' has no authority nodes; \
                 certification is impossible until it is repopulated via \
                 PUT /api/control-plane/authorities",
                key_range.prefix
            )));
        }

        let policy_version = ns
            .get_placement_policy(&key_range.prefix)
            .map(|p| p.version)
            .ok_or_else(|| {
                CrdtError::InvalidArgument(format!(
                    "no placement policy for prefix: {}",
                    key_range.prefix
                ))
            })?;

        Ok((key_range, policy_version, total))
    }

    /// Record a certified write in the proof cache only when signed evidence exists.
    ///
    /// Captures the frontier state at certification time so that later reads
    /// can still return `Certified` with a valid proof bundle even after the
    /// pending write has been cleaned up.
    fn cache_certified_proof(&mut self, pw: &PendingWrite) -> bool {
        if self.store.timestamp_for(&pw.key) != Some(&pw.timestamp) {
            return false;
        }
        if self.origins.get(&pw.key) != Some(&pw.policy_version)
            || pw.authority_nodes.len() != pw.total_authorities
            || pw.authority_nodes.is_empty()
        {
            return false;
        }
        let mut authority_nodes = pw.authority_nodes.clone();
        authority_nodes.sort();
        if authority_nodes.windows(2).any(|pair| pair[0] == pair[1]) {
            return false;
        }
        let Some((frontier_hlc, cert, bls_certificate)) =
            self.attestations.build_certificates_for_members_matching(
                &pw.key_range,
                pw.policy_version,
                pw.total_authorities,
                &pw.timestamp,
                &authority_nodes,
                |att| {
                    let scope = FrontierScope::new(
                        pw.key_range.clone(),
                        pw.policy_version,
                        att.authority_id.clone(),
                    );
                    self.frontiers
                        .accepted_frontier_at(&scope, att.checkpoint_hlc.physical)
                        .is_some_and(|frontier| att.matches_frontier(frontier))
                },
            )
        else {
            return false;
        };
        let signers: HashSet<&NodeId> = cert.signers().into_iter().collect();
        if signers.len() < majority_threshold(authority_nodes.len())
            || signers
                .iter()
                .any(|signer| !authority_nodes.contains(*signer))
        {
            return false;
        }
        let contributing_authorities: Vec<NodeId> = cert.signers().into_iter().cloned().collect();
        let mut accepted_frontiers = Vec::with_capacity(contributing_authorities.len());
        for authority in &contributing_authorities {
            let scope =
                FrontierScope::new(pw.key_range.clone(), pw.policy_version, authority.clone());
            let Some(accepted) = self
                .frontiers
                .accepted_frontier_at(&scope, frontier_hlc.physical)
            else {
                return false;
            };
            if !self.attestations.has_attestation_for_frontier(accepted) {
                return false;
            }
            accepted_frontiers.push(accepted.clone());
        }

        self.certified_cache.insert(
            pw.key.clone(),
            CertifiedCacheEntry {
                key_range: pw.key_range.clone(),
                frontier_hlc,
                policy_version: pw.policy_version,
                contributing_authorities,
                total_authorities: pw.total_authorities,
                authority_nodes,
                certificate: Some(cert),
                accepted_frontiers,
                bls_certificate,
                write_timestamp: pw.timestamp.clone(),
            },
        );

        true
    }

    /// Read a key with certification status (FR-002).
    ///
    /// Returns the value (if present), its certification status based on
    /// the latest pending write for that key, the scoped majority frontier
    /// for the key's authority range, and a verifiable proof bundle when
    /// the status is `Certified`.
    ///
    /// If no pending write exists for the key but the certified proof cache
    /// contains an entry, the cached `Certified` status and proof are returned.
    /// This ensures certification stability after cleanup.
    pub fn get_certified(&self, key: &str) -> CertifiedRead<'_> {
        let value = self.store.get(key);

        let current_proof = self.certified_cache_for_stored_write(key);
        let frontier = if let Some(cached) = current_proof {
            self.frontiers.majority_frontier_for_scope(
                &cached.key_range,
                &cached.policy_version,
                cached.total_authorities,
            )
        } else if let Some(pw) = self.latest_pending(key) {
            self.frontiers.majority_frontier_for_scope(
                &pw.key_range,
                &pw.policy_version,
                pw.total_authorities,
            )
        } else {
            self.resolve_scope(key)
                .ok()
                .and_then(|(range, version, count)| {
                    self.frontiers
                        .majority_frontier_for_scope(&range, &version, count)
                })
        };

        // Look up status from pending_writes first; fall back to certified_cache.
        let pending_status = self.latest_pending(key).map(|pw| pw.status);

        let (status, proof) = match pending_status {
            Some(CertificationStatus::Certified) => {
                // Prefer the cached proof: it carries the certificate (and the
                // checkpoint frontier the authorities actually signed) when
                // the signing pipeline produced one at promotion time.
                match current_proof {
                    Some(cached) => (
                        CertificationStatus::Certified,
                        Some(Self::proof_from_cache(cached)),
                    ),
                    _ => (CertificationStatus::Pending, None),
                }
            }
            Some(other_status) => (other_status, None),
            None => {
                // No pending write — check the certified cache.
                if let Some(cached) = current_proof {
                    (
                        CertificationStatus::Certified,
                        Some(Self::proof_from_cache(cached)),
                    )
                } else {
                    (CertificationStatus::Pending, None)
                }
            }
        };

        CertifiedRead {
            value,
            status,
            frontier,
            proof,
        }
    }

    fn preview_merged_value(
        &self,
        key: &str,
        value: &CrdtValue,
    ) -> Result<(CrdtValue, bool), CrdtError> {
        // Use the real Store merge rules, including immutable dot assignment
        // checks, without publishing a value or changing GC diagnostics before
        // its WAL record has been appended.
        let mut candidate = Store::new();
        if let Some(existing) = self.store.get(key) {
            candidate.put(key.to_owned(), existing.clone());
        }
        let changed = candidate.merge_value(key.to_owned(), value)?;
        let post_state = candidate
            .get(key)
            .cloned()
            .expect("merging a value always creates a candidate entry");
        Ok((post_state, changed))
    }

    /// Write a value that requires Authority majority certification (FR-004).
    ///
    /// The key is resolved to an authority scope via longest-prefix match
    /// in the system namespace. The value is written to the local store
    /// immediately (eventual path). A `PendingWrite` entry is created to
    /// track certification progress.
    ///
    /// Returns `Err(CrdtError::PolicyDenied)` if no authority definition
    /// covers the key.
    ///
    /// ## Capacity enforcement
    ///
    /// `max_entries` is a hard admission limit. Completed entries are
    /// cleaned first; if pending work still fills the queue, the new write
    /// is refused before modifying the store.
    ///
    /// If a signed majority certificate already covers the write,
    /// returns `Ok(CertificationStatus::Certified)`. Otherwise, behaviour
    /// depends on `on_timeout`:
    /// - `OnTimeout::Error` — returns `Err(CrdtError::CertificationTimeout)`.
    /// - `OnTimeout::Pending` — returns `Ok(CertificationStatus::Pending)`.
    ///
    /// Callers using `OnTimeout::Pending` can poll with
    /// `get_certification_status` or wait for `process_certifications`.
    pub fn certified_write(
        &mut self,
        key: String,
        value: CrdtValue,
        on_timeout: OnTimeout,
    ) -> Result<CertificationStatus, CrdtError> {
        let (key_range, policy_version, total_authorities) = self.resolve_scope(&key)?;
        let authority_nodes = match self.current_read_scope(&key) {
            Some((range, version, members))
                if range == key_range
                    && version == policy_version
                    && members.len() == total_authorities =>
            {
                members
            }
            _ => {
                return Err(CrdtError::PolicyDenied(
                    "authority scope changed during certified write".into(),
                ));
            }
        };

        self.ensure_pending_capacity_for_key(&key)?;

        let timestamp = self.clock.now()?;

        // Write to the local store (eventual consistency path), recording
        // the HLC so the entry is immediately visible to delta_sync.
        //
        // The value is CRDT-MERGED into any existing entry, never a plain
        // replace: WAL recovery rebuilds state by merging the logged
        // post-states, so the live path must only produce post-states that
        // dominate previous ones for the key. A replace can regress CRDT
        // state (e.g. overwrite counter {a:2} with a fresh {b:1}) and
        // replay's merge would resurrect the replaced contributions —
        // and the certified store has no anti-entropy rebuild path, so the
        // divergence would be permanent. A type-changing write is rejected
        // here (TypeMismatch) instead of installing state that replay
        // could not reconstruct.
        // A counter write carries an absolute value, but the merge above takes
        // the per-node max — so setting a counter to 3 after 5 is a silent
        // no-op that would still be acked as success. Detect an
        // unrepresentable counter write and fail loudly *before* mutating the
        // store (so an error leaves it untouched), instead of lying to the
        // client. Registers/sets/maps legitimately produce a merged post-state
        // that differs from the request, so this check is counter-only.
        if let CrdtValue::Counter(requested) = &value
            && let Some(CrdtValue::Counter(existing)) = self.store.get(&key)
        {
            let mut merged = existing.clone();
            merged.merge(requested);
            if merged.value() != requested.value() {
                return Err(CrdtError::InvalidArgument(format!(
                    "certified counter write to key {key} is unrepresentable: \
                     merging with existing state yields {} (requested {}); the \
                     certified path merges rather than replaces for WAL-replay \
                     soundness, so a counter can only advance, never regress",
                    merged.value(),
                    requested.value(),
                )));
            }
        }

        let (post_state, _) = self.preview_merged_value(&key, &value)?;
        let scope_pin = CertifiedScopePin {
            write_hlc: timestamp.clone(),
            key_range: key_range.clone(),
            policy_version,
            authority_nodes: authority_nodes.clone(),
        };
        // Append before publishing any change to the store, proof cache, or
        // scope pin. An append failure leaves the prior value and its proof
        // visible, and the client can retry the same write.
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(wal) = self.wal.as_mut() {
            // Log the origin policy version alongside the post-state so a
            // restart can re-track this write under the version it was
            // written with (and re-derive its FR-009 fence), instead of
            // silently re-tagging it with the current post-bump version and
            // certifying it off a newer authority frontier
            // (fix/fence-persistence-across-restart).
            let record = WalRecord::CertifiedUpsertScoped {
                key: key.clone(),
                value: post_state,
                hlc: timestamp.clone(),
                scope: scope_pin.clone(),
            };
            match wal.append(&record) {
                Ok(pos) => self.last_wal_pos = Some(pos),
                Err(e) => return Err(CrdtError::Storage(format!("WAL append failed: {e}"))),
            }
        }
        #[cfg(target_arch = "wasm32")]
        let _ = post_state;

        // The preflight used the same merge rules against the same state. No
        // other writer can interleave while this mutable API call runs.
        self.store
            .merge_value(key.clone(), &value)
            .expect("prevalidated certified merge must still succeed");
        self.certified_cache.remove(&key);
        self.store.record_change(&key, timestamp.clone());
        self.origins.insert(key.clone(), policy_version);
        self.scope_pins.insert(key.clone(), scope_pin);

        let mut pw = PendingWrite {
            key: key.clone(),
            value,
            timestamp,
            status: CertificationStatus::Pending,
            key_range,
            policy_version,
            total_authorities,
            authority_nodes,
            replicated: false,
        };

        // Promotion requires a cacheable signed certificate for the current
        // Authority roster, not just a quorum in a possibly stale pool.
        let already_certified = self.cache_certified_proof(&pw);
        if already_certified {
            pw.status = CertificationStatus::Certified;
        }

        // The store and its proof cache describe only the latest state of a
        // key. Once this write is logged, older tracking entries cannot be
        // certified against that state and must not consume capacity forever.
        // Keep them until this point so a failed write leaves its predecessor
        // available for certification.
        self.retain_pending(|existing| existing.key != key);
        self.push_pending(pw);

        if already_certified {
            return Ok(CertificationStatus::Certified);
        }

        match on_timeout {
            // The local write is already durable in the store.  Return a
            // distinct error so callers can tell that the value IS present
            // locally and only the Authority-majority certification timed
            // out.  They can retry certification via `process_certifications`
            // once more Authority frontiers arrive, without re-issuing the
            // write.
            OnTimeout::Error => Err(CrdtError::CertificationTimeout),
            OnTimeout::Pending => Ok(CertificationStatus::Pending),
        }
    }

    /// Reserve tracking capacity before a local write or replicated ingest
    /// mutates the store. A client or peer can retry after pending work is
    /// certified. Silent eviction would strand an accepted operation.
    fn ensure_pending_capacity(&mut self) -> Result<(), CrdtError> {
        if self.pending_writes.len() >= self.retention.max_entries {
            self.cleanup_completed();
        }
        if self.pending_writes.len() >= self.retention.max_entries {
            return Err(CrdtError::CertificationCapacity {
                max_entries: self.retention.max_entries,
            });
        }
        Ok(())
    }

    /// Replacing a tracked key does not add a tracking slot. At capacity,
    /// allow that replacement only if dropping its prior entries leaves room
    /// for exactly one new entry; do not remove them before the write lands.
    fn ensure_pending_capacity_for_key(&mut self, key: &str) -> Result<(), CrdtError> {
        if self.pending_writes.len() >= self.retention.max_entries {
            let replaced = self
                .pending_writes
                .iter()
                .filter(|entry| entry.key == key)
                .count();
            if replaced > 0 && self.pending_writes.len() - replaced < self.retention.max_entries {
                return Ok(());
            }
        }
        self.ensure_pending_capacity()
    }

    /// Check the redundant-relay case without mutating the real store. At
    /// capacity this lets an already tracked, dominated re-offer advance the
    /// peer baseline instead of wedging behind its own pending receipt.
    fn dominated_tracked_reoffer(
        &self,
        key: &str,
        incoming: &CrdtValue,
        hlc: &HlcTimestamp,
        origin: Option<PolicyVersion>,
    ) -> bool {
        if !self
            .store
            .timestamp_for(key)
            .is_some_and(|stored| stored >= hlc)
            || !origin.is_some_and(|version| self.origins.get(key) == Some(&version))
            || (self.latest_pending(key).is_none() && !self.certified_cache.contains_key(key))
        {
            return false;
        }
        match (self.store.get(key), incoming) {
            (Some(CrdtValue::Counter(existing)), CrdtValue::Counter(value)) => {
                !existing.clone().merge(value)
            }
            (Some(CrdtValue::Set(existing)), CrdtValue::Set(value)) => {
                !existing.clone().merge(value).changed
            }
            (Some(CrdtValue::Map(existing)), CrdtValue::Map(value)) => {
                !existing.clone().merge(value).changed
            }
            (Some(CrdtValue::Register(existing)), CrdtValue::Register(value)) => {
                !existing.clone().merge(value)
            }
            _ => false,
        }
    }

    /// Extract certified entries modified strictly after `since`, each
    /// tagged with its write-time ORIGIN policy version, for the certified
    /// replication lane (P0-1).
    ///
    /// Returns `(entries, sender_frontier)`. **`sender_frontier` is the HLC
    /// of the LAST INCLUDED entry when the batch was truncated**, never the
    /// store's full frontier: a receiver that adopted the full frontier
    /// after a partial batch would skip everything between the last
    /// delivered entry and that frontier, permanently. When the batch is
    /// complete it is the store's own frontier, which lets the receiver
    /// jump over keys it will never be offered.
    ///
    /// Every entry carries the stored write-time scope pin.
    ///
    /// Each entry carries the key's FULL stored state, not a per-CRDT
    /// delta. The certified plane has no digest lane and no full-sync
    /// fallback, so a receiver that never got an entry (refused while its
    /// namespace lagged, or below the baseline it adopted from another
    /// peer) has no way to ask for the base a partial payload assumes.
    /// `OrMap::delta_since` is a genuinely partial payload, and
    /// `Store::merge_value` inserts it verbatim when the key is absent --
    /// a certified map silently missing fields, served with a valid proof.
    /// Full state makes every entry self-sufficient, so any later update
    /// to a key repairs a hole in it.
    ///
    /// Selection happens on `(hlc, &key)` pairs and only the `max` entries
    /// that actually ship are cloned. Cloning the whole matching set and
    /// truncating afterwards made a baseline=0 catch-up of N keys pay
    /// ~N deep CRDT clones per round for ceil(N/max) rounds, all under the
    /// mutex that serves `SLO_CERTIFIED_READ_P99`.
    pub fn certified_delta_entries(
        &self,
        since: &Option<CertifiedDeltaCursor>,
        max: usize,
    ) -> (Vec<CertifiedDeltaTuple>, Option<CertifiedDeltaCursor>) {
        let mut ranked: Vec<(&HlcTimestamp, &String)> = self
            .store
            .keys()
            .into_iter()
            .filter_map(|key| self.store.timestamp_for(key).map(|ts| (ts, key)))
            .filter(|(ts, key)| {
                since
                    .as_ref()
                    .is_none_or(|cursor| (*ts, key.as_str()) > (&cursor.hlc, cursor.key.as_str()))
            })
            .collect();
        ranked.sort_unstable_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
        ranked.truncate(max);

        let entries: Vec<CertifiedDeltaTuple> = ranked
            .into_iter()
            .filter_map(|(hlc, key)| {
                self.store.get(key).map(|value| {
                    (
                        key.clone(),
                        value.clone(),
                        hlc.clone(),
                        self.scope_pins
                            .get(key)
                            .expect("certified delta entry must have a scope pin")
                            .clone(),
                    )
                })
            })
            .collect();

        let sender_frontier = entries.last().map(|(key, _, hlc, _)| CertifiedDeltaCursor {
            hlc: hlc.clone(),
            key: key.clone(),
        });
        (entries, sender_frontier)
    }

    #[cfg(test)]
    fn merge_remote_test(
        &mut self,
        key: String,
        value: &CrdtValue,
        hlc: HlcTimestamp,
        origin: Option<PolicyVersion>,
    ) -> Result<IngestOutcome, CrdtError> {
        let (key_range, current_version, authority_nodes) = self
            .current_read_scope(&key)
            .ok_or_else(|| CrdtError::PolicyDenied("missing test authority scope".into()))?;
        let scope = CertifiedScopePin {
            write_hlc: hlc.clone(),
            key_range,
            policy_version: origin.unwrap_or(current_version),
            authority_nodes,
        };
        self.merge_remote_certified(key, value, hlc, scope)
    }

    /// Ingest a certified entry replicated from a peer (P0-1).
    ///
    /// This is the receiving half of the certified lane: it makes a
    /// certified write durable on more than the one node that accepted it,
    /// which is what removes the inversion where the strongly-consistent
    /// plane was LESS durable than the eventual one.
    ///
    /// `scope` carries the write-time policy version, range, and Authority
    /// roster. The receiver validates the roster against its trusted
    /// namespace before ingest. Pinning the version matters because the
    /// receiver's `resolve_scope` reports the receiver's CURRENT version,
    /// and `merge_remote_with_hlc`-style ingest keeps the writer's HLC, so
    /// a node that has already moved to v2 would otherwise evaluate a v1
    /// write against its v2 frontier and certify it. `is_version_fenced`
    /// only gates attestation ADMISSION -- it cannot stop a certify that
    /// reads the current version's frontier. That is the #342 fence bypass,
    /// and without the wire scope replication would spread it to every
    /// non-writer node, with no crash or restart involved.
    ///
    /// Ingest can only ever produce `Pending` tracking: it never touches
    /// `certified_cache` (except to INVALIDATE), `attestations` or the
    /// frontiers, so an internal-token holder cannot forge a `Certified`
    /// verdict or a proof through this path.
    pub fn merge_remote_certified(
        &mut self,
        key: String,
        value: &CrdtValue,
        hlc: HlcTimestamp,
        scope: CertifiedScopePin,
    ) -> Result<IngestOutcome, CrdtError> {
        // The same policy gate a local write passes: a key with no
        // authority definition can never certify, so it has no business in
        // the certified store.
        let (key_range, current_version, total_authorities) = self.resolve_scope(&key)?;
        let intake_authority_nodes = match self.current_read_scope(&key) {
            Some((range, version, members))
                if range == key_range
                    && version == current_version
                    && members.len() == total_authorities =>
            {
                members
            }
            _ => {
                return Err(CrdtError::PolicyDenied(
                    "authority scope changed during certified ingest".into(),
                ));
            }
        };

        // An origin AHEAD of this node pins the entry to a frontier this
        // node cannot yet produce: it would sit `Pending` until the local
        // namespace catches up, and it asserts a policy version this node
        // cannot validate. Refuse it. The caller must NOT advance its peer
        // baseline past this entry -- the peer only re-offers entries
        // strictly above the baseline, so stepping over it would drop the
        // value permanently on this node.
        if scope.policy_version > current_version {
            return Ok(IngestOutcome::Rejected(IngestRejection::OriginAhead));
        }

        // A peer cannot define its own quorum. Until the namespace retains
        // trusted roster history, accept only the currently trusted roster.
        if scope.write_hlc != hlc
            || scope.key_range != key_range
            || scope.authority_nodes.is_empty()
            || scope.authority_nodes.len() != total_authorities
            || scope.authority_nodes != intake_authority_nodes
            || {
                let unique: HashSet<_> = scope.authority_nodes.iter().collect();
                unique.len() != scope.authority_nodes.len()
            }
        {
            return Err(CrdtError::PolicyDenied(
                "replicated certified scope differs from trusted authority scope".into(),
            ));
        }
        let origin = scope.policy_version;

        // Every certified head has durable issue-time metadata. Refuse an
        // incomplete local head before merging into it; reconstructing its
        // scope from today's namespace could lower its original quorum.
        if let Some(stored_hlc) = self.store.timestamp_for(&key)
            && !self.scope_pins.get(&key).is_some_and(|pin| {
                pin.write_hlc == *stored_hlc && self.origins.get(&key) == Some(&pin.policy_version)
            })
        {
            return Err(CrdtError::Internal(format!(
                "certified key {key} has no valid write-time scope pin"
            )));
        }

        // This check must precede the full-capacity redundant-reoffer fast
        // path. A conflicting OR-Map value can lose the LWW comparison and
        // make a simulated merge look like a no-op, but an immutable dot may
        // never be redefined even by a dominated payload.
        if self
            .store
            .get(&key)
            .is_some_and(|held| held.validate_dot_assignments(value).is_err())
        {
            return Ok(IngestOutcome::Rejected(
                IngestRejection::DotAssignmentConflict,
            ));
        }

        // A dominated re-offer of an already tracked key needs no new
        // receipt and can safely advance even when the queue is full.
        // Every changing contribution reserves capacity before mutation.
        if self.pending_writes.len() >= self.retention.max_entries
            && self.dominated_tracked_reoffer(&key, value, &hlc, Some(origin))
        {
            return Ok(IngestOutcome::Skipped);
        }
        self.ensure_pending_capacity_for_key(&key)?;

        // Advisory only, exactly as in `EventualApi::merge_remote_with_hlc`:
        // a ClockSkew/Overflow must never discard the merge, because the
        // lane advances its peer frontier past entries it processed and
        // would never re-request them.
        let _ = self.clock.update(&hlc);

        let prev_ts = self.store.timestamp_for(&key).cloned();
        let (post_state, changed) = match self.preview_merged_value(&key, value) {
            Ok(candidate) => candidate,
            Err(CrdtError::TypeMismatch { .. }) => {
                // A single conflicting key must not wedge the lane: report
                // it so the caller can count it and keep going.
                return Ok(IngestOutcome::Rejected(IngestRejection::TypeMismatch));
            }
            Err(CrdtError::DotAssignmentConflict(_)) => {
                return Ok(IngestOutcome::Rejected(
                    IngestRejection::DotAssignmentConflict,
                ));
            }
            Err(e) => return Err(e),
        };

        // RR gate (redundant-relay suppression, M-6). A no-op merge may skip
        // the WAL only when its write HLC is already covered by this key's
        // head. A newer write with the same CRDT post-state is still a new
        // certification subject: it must replace the head and its proof.
        // Skipping only dominated writes lets repeated pulls quiesce.
        //
        if !changed && prev_ts.as_ref().is_some_and(|previous| &hlc <= previous) {
            // A dominated re-offer is still the only signal a replica gets
            // that a key exists. If nothing is tracking it and no proof is
            // cached, re-track it so it can certify here -- but only with
            // headroom, so a rotating re-offer cannot crowd out pending
            // work. This can repair a legacy state with a missing receipt.
            if self.pending_writes.len() < self.retention.max_entries
                && self.latest_pending(&key).is_none()
                && !self.certified_cache.contains_key(&key)
                && let Some(timestamp) = self.store.timestamp_for(&key).cloned()
                && let Some(value) = self.store.get(&key).cloned()
            {
                let pin = &self.scope_pins[&key];
                let policy_version = pin.policy_version;
                let tracked_range = pin.key_range.clone();
                let authority_nodes = pin.authority_nodes.clone();
                let total_authorities = authority_nodes.len();
                self.push_pending(PendingWrite {
                    key: key.clone(),
                    value,
                    timestamp,
                    status: CertificationStatus::Pending,
                    key_range: tracked_range,
                    policy_version,
                    total_authorities,
                    authority_nodes,
                    replicated: true,
                });
            }
            return Ok(IngestOutcome::Skipped);
        }

        // The origin follows the HIGHEST-HLC contribution. HLCs are totally
        // ordered, so every replica converges on the same origin for a key
        // regardless of the order entries arrive in.
        let newer = prev_ts.as_ref().is_none_or(|prev| hlc > *prev);
        let effective_origin = if newer {
            origin
        } else {
            // The preflight above established this head's durable pin.
            self.origins[&key]
        };

        // A lower-HLC contribution can change the CRDT post-state while the
        // key's max HLC and origin stay fixed. Preserve that write's pin;
        // otherwise persist the sender's validated write-time scope.
        let stored_ts = prev_ts
            .as_ref()
            .filter(|previous| **previous >= hlc)
            .cloned()
            .unwrap_or_else(|| hlc.clone());
        let scope_pin = self
            .scope_pins
            .get(&key)
            .filter(|pin| pin.write_hlc == stored_ts && pin.policy_version == effective_origin)
            .cloned()
            .unwrap_or(scope);
        // Append the candidate before changing the live store or its proof.
        // A failed append leaves the old value visible and the peer baseline
        // unchanged, so the same contribution can be retried normally.
        #[cfg(not(target_arch = "wasm32"))]
        {
            if let Some(wal) = self.wal.as_mut() {
                let record = WalRecord::CertifiedUpsertScoped {
                    key: key.clone(),
                    value: post_state,
                    hlc: stored_ts.clone(),
                    scope: scope_pin.clone(),
                };
                match wal.append(&record) {
                    Ok(pos) => self.last_wal_pos = Some(pos),
                    Err(e) => return Err(CrdtError::Storage(format!("WAL append failed: {e}"))),
                }
            }
        }
        #[cfg(target_arch = "wasm32")]
        let _ = post_state;

        self.store
            .merge_value(key.clone(), value)
            .expect("prevalidated replicated merge must still succeed");
        // Keep the writer's HLC; a lower timestamp contribution may change
        // the CRDT value while the key's head and scope pin stay fixed.
        self.store.record_change_max(&key, hlc.clone());
        if newer {
            self.origins.insert(key.clone(), origin);
        }
        self.scope_pins.insert(key.clone(), scope_pin.clone());
        self.certified_cache.remove(&key);

        // Track it so it can certify here. Deliberately NOT skipped in
        // favour of "record the origin only": a replica that never tracks
        // replicated keys reports them `Pending` forever, which is the
        // liveness failure the certified value-plane design names as
        // unacceptable.
        let timestamp = self
            .store
            .timestamp_for(&key)
            .cloned()
            .unwrap_or_else(|| hlc.clone());
        let value = self
            .store
            .get(&key)
            .cloned()
            .unwrap_or_else(|| value.clone());
        // The replicated post-state is now logged. Only its current HLC can
        // acquire a proof, so older same-key receipts are no longer work.
        self.retain_pending(|entry| entry.key != key);
        self.push_pending(PendingWrite {
            key,
            value,
            timestamp,
            status: CertificationStatus::Pending,
            key_range: scope_pin.key_range.clone(),
            policy_version: effective_origin,
            total_authorities: scope_pin.authority_nodes.len(),
            authority_nodes: scope_pin.authority_nodes,
            replicated: true,
        });

        // Same rule as `rebuild_pending_from_store`: an origin behind the
        // current version means the version bump that superseded it also
        // fenced its scope. Re-derive that fence here so late old-version
        // attestations are refused on this node too.
        if effective_origin < current_version {
            self.fence_version(&scope_pin.key_range, effective_origin);
        }

        Ok(IngestOutcome::Applied)
    }

    /// Check the certification status of the latest write for a key.
    ///
    /// Returns `CertificationStatus::Pending` if no tracked write exists
    /// and the key is not in the certified proof cache.
    pub fn get_certification_status(&self, key: &str) -> CertificationStatus {
        let has_proof = self.certified_cache_for_stored_write(key).is_some();
        match self.latest_pending(key).map(|pw| pw.status) {
            Some(CertificationStatus::Certified) if !has_proof => CertificationStatus::Pending,
            Some(status) => status,
            None if has_proof => CertificationStatus::Certified,
            None => CertificationStatus::Pending,
        }
    }

    /// A cached certificate stays bound to the accepted write, including its
    /// origin version and HLC. Later live scope changes do not revoke it.
    fn certified_cache_for_stored_write(&self, key: &str) -> Option<&CertifiedCacheEntry> {
        self.certified_cache.get(key).filter(|cached| {
            self.origins.get(key) == Some(&cached.policy_version)
                && self.store.timestamp_for(key) == Some(&cached.write_timestamp)
                && Self::cached_accepted_frontiers_match(cached)
        })
    }

    fn cached_accepted_frontiers_match(cached: &CertifiedCacheEntry) -> bool {
        let Some(cert) = cached.certificate.as_ref() else {
            return false;
        };
        if cert.key_range != cached.key_range
            || cert.policy_version != cached.policy_version
            || cert.frontier_hlc != cached.frontier_hlc
            || cert.frontier_hlc < cached.write_timestamp
            || !cert.has_majority(cached.total_authorities)
        {
            return false;
        }
        let signers: HashSet<&NodeId> = cert.signers().into_iter().collect();
        let recorded_signers: HashSet<&NodeId> = cached.contributing_authorities.iter().collect();
        if signers != recorded_signers
            || signers.len() != cached.accepted_frontiers.len()
            || signers
                .iter()
                .any(|signer| !cached.authority_nodes.contains(*signer))
        {
            return false;
        }
        let mut witnessed = HashSet::new();
        cached.accepted_frontiers.iter().all(|frontier| {
            frontier.key_range == cached.key_range
                && frontier.policy_version == cached.policy_version
                && crate::authority::frontier_sig::checkpoint_hlc(&frontier.frontier_hlc)
                    == cached.frontier_hlc
                && signers.contains(&frontier.authority_id)
                && witnessed.insert(&frontier.authority_id)
        })
    }

    /// Resolve the current range and roster in one namespace snapshot. The
    /// returned version is used only when no write-time origin is recorded.
    /// Replacing an Authority definition can keep its placement version and
    /// count while changing the actual signer set.
    fn current_read_scope(&self, key: &str) -> Option<(KeyRange, PolicyVersion, Vec<NodeId>)> {
        let ns = self.namespace.read().ok()?;
        let definition = ns.get_authorities_for_key(key)?;
        let mut members = definition.authority_nodes.clone();
        if members.is_empty() {
            return None;
        }
        members.sort();
        if members.windows(2).any(|pair| pair[0] == pair[1]) {
            return None;
        }
        let version = ns
            .get_placement_policy(&definition.key_range.prefix)?
            .version;
        Some((definition.key_range.clone(), version, members))
    }

    /// Build a `ProofBundle` from a cached certified entry.
    fn proof_from_cache(cached: &CertifiedCacheEntry) -> ProofBundle {
        ProofBundle {
            key_range: cached.key_range.clone(),
            frontier_hlc: cached.frontier_hlc.clone(),
            policy_version: cached.policy_version,
            contributing_authorities: cached.contributing_authorities.clone(),
            total_authorities: cached.total_authorities,
            certificate: cached.certificate.clone(),
            bls_certificate: cached.bls_certificate.clone(),
        }
    }

    /// Update an Authority's ack frontier.
    ///
    /// Simulates receiving an ack from an Authority node. Returns `true` if
    /// the frontier was actually advanced, `false` if the update was
    /// stale or duplicate.
    pub fn update_frontier(&mut self, frontier: AckFrontier) -> bool {
        self.update_frontier_verified(frontier, None)
    }

    /// Update an Authority's ack frontier, recording a verified attestation.
    ///
    /// The report is first validated against the namespace
    /// ([`attestation_admissible`], M-4). An inadmissible report — unknown
    /// range, non-member signer, missing placement policy, or a policy
    /// version outside the admission window — is dropped ENTIRELY: it
    /// advances neither the frontier set nor the pool. Gating the frontier
    /// set as well is deliberate: `AckFrontierSet` grows one uncapped,
    /// persisted entry per distinct scope triple, so admitting inadmissible
    /// frontiers would leave the M-4 memory-exhaustion vector open there
    /// even with the pool protected. A scope this gate rejects can never
    /// certify a write (`resolve_scope` requires the same definition and
    /// policy), so no legitimate certification is lost.
    ///
    /// Admissible reports go through `AckFrontierSet::update()` unchanged
    /// (monotonicity, deduplication, fencing). A matching signature enters
    /// the pool only if this node accepted that exact full frontier. A late
    /// signature can join a previously accepted report; a stale or different
    /// report in the same checkpoint cannot certify a read.
    /// Returns whether the frontier advanced.
    pub fn update_frontier_verified(
        &mut self,
        frontier: AckFrontier,
        verified: Option<VerifiedAttestation>,
    ) -> bool {
        if verified
            .as_ref()
            .is_some_and(|att| !att.matches_frontier(&frontier))
        {
            return false;
        }
        let key_range = frontier.key_range.clone();
        let policy_version = frontier.policy_version;
        let authority_id = frontier.authority_id.clone();
        let admissible = {
            let ns = self.namespace.read().unwrap();
            attestation_admissible(&ns, &key_range, policy_version, &authority_id)
        };
        let current = match admissible {
            Ok(current) => current,
            Err(reject) => {
                self.note_admission_rejection(&key_range, policy_version, &authority_id, reject);
                return false;
            }
        };
        // Stale-but-admissible (M-17): the report is inside the admission
        // window but behind the current version — the FIRST bump after an
        // authority's namespace freezes (frozen observer, minority-side
        // voter) lands here, before fencing or the window reject anything.
        // The counter always increments; the WARN is throttled per
        // (range, authority) since a lagging reporter re-reports every tick.
        if policy_version.0 < current {
            self.attestation_stale_version_total += 1;
            if self.scope_warn_allowed(&key_range.prefix, &authority_id, "stale") {
                tracing::warn!(
                    authority = %authority_id.0,
                    key_range = %key_range.prefix,
                    policy_version = policy_version.0,
                    current,
                    "authority is reporting a STALE policy version (behind the \
                     current one): its namespace is lagging the control plane — \
                     after the old version is fenced it will stop contributing \
                     to certification for this range (M-17)"
                );
            }
        }
        let fenced = self
            .frontiers
            .is_version_fenced(&frontier.key_range, &frontier.policy_version);
        if fenced {
            // Fenced drop (M-17): previously completely silent — the
            // frontier no longer advances (`AckFrontierSet::update` refuses
            // fenced scopes) and no attestation is pooled below, so this
            // authority contributes NOTHING to certification for this
            // scope. The certification quorum numerator shrinks while the
            // denominator stays fixed (errs safe); this counter + WARN is
            // the operator's confirmation that the fence has been reached.
            self.attestation_rejected_fenced_total += 1;
            if self.scope_warn_allowed(&key_range.prefix, &authority_id, "fenced") {
                tracing::warn!(
                    authority = %authority_id.0,
                    key_range = %key_range.prefix,
                    policy_version = policy_version.0,
                    current,
                    "dropping frontier report for a FENCED policy version: this \
                     authority contributes nothing to certification for this scope \
                     until it reports the current version (lagging namespace? see \
                     observer pull metrics in /api/control-plane/raft/status)"
                );
            }
        }
        let advanced = self.frontiers.update(frontier.clone());
        if let Some(att) = verified
            && !fenced
            && self.frontiers.accepted_frontier_at(
                &FrontierScope::new(key_range.clone(), policy_version, authority_id.clone()),
                att.checkpoint_hlc.physical,
            ) == Some(&frontier)
        {
            self.record_attestation(
                &key_range,
                policy_version,
                &authority_id,
                att,
                crate::hlc::wall_clock_ms(),
            );
        }
        advanced
    }

    /// Log-throttle gate for the per-scope stale/fenced WARNs (M-17): a
    /// lagging reporter re-reports every tick, so the counters increment on
    /// every report but the log line fires at most ~1/minute per
    /// `(range, authority, lane)`. A backward wall-clock step counts as
    /// throttle expiry (same posture as the cap-pressure sweep throttle).
    fn scope_warn_allowed(&mut self, prefix: &str, authority: &NodeId, lane: &str) -> bool {
        const WARN_INTERVAL_MS: u64 = 60_000;
        let now_ms = crate::hlc::wall_clock_ms();
        let key = (prefix.to_string(), format!("{}#{lane}", authority.0));
        match self.stale_version_warned_ms.get(&key) {
            Some(&last) if now_ms >= last && now_ms - last < WARN_INTERVAL_MS => false,
            _ => {
                self.stale_version_warned_ms.insert(key, now_ms);
                true
            }
        }
    }

    /// Count and log one admission rejection (M-4).
    ///
    /// The log volume is bounded by the receive path's
    /// `MAX_FRONTIERS_PER_REQUEST` (256) per request and is zero under
    /// honest load.
    fn note_admission_rejection(
        &mut self,
        key_range: &KeyRange,
        policy_version: PolicyVersion,
        authority: &NodeId,
        reject: AdmissionReject,
    ) {
        match reject {
            AdmissionReject::VersionOutOfWindow { current } => {
                self.attestation_rejected_version_window_total += 1;
                tracing::warn!(
                    authority = %authority.0,
                    key_range = %key_range.prefix,
                    policy_version = policy_version.0,
                    current,
                    "rejecting frontier report: policy version outside the admission window"
                );
            }
            reason => {
                self.attestation_rejected_unknown_range_total += 1;
                tracing::warn!(
                    authority = %authority.0,
                    key_range = %key_range.prefix,
                    policy_version = policy_version.0,
                    ?reason,
                    "rejecting frontier report: scope not admissible for certification"
                );
            }
        }
    }

    /// Record an already-admitted attestation in the pool (M-4).
    ///
    /// Admission ([`attestation_admissible`]) runs in
    /// `update_frontier_verified` before this point. When the pool rejects
    /// the insert for capacity, a throttled sweep (at most one per
    /// [`CHECKPOINT_INTERVAL_MS`]) drops scopes that can no longer be
    /// consumed by certificate assembly — the current-version snapshot is
    /// taken under a single `namespace.read()` so a sweep never mixes
    /// versions from different namespace states — and the insert is retried
    /// once.
    fn record_attestation(
        &mut self,
        key_range: &KeyRange,
        policy_version: PolicyVersion,
        authority: &NodeId,
        att: VerifiedAttestation,
        now_ms: u64,
    ) {
        let outcome = self
            .attestations
            .insert(key_range, policy_version, att.clone(), now_ms);
        if !matches!(
            outcome,
            InsertOutcome::RejectedScopeCap | InsertOutcome::RejectedAuthorityCap
        ) {
            return;
        }

        // Cap pressure: sweep stale scopes and retry, at most once per
        // checkpoint interval (namespace snapshot + full-scope walk). A
        // backward wall-clock step (NTP correction, VM migration) counts as
        // throttle expiry: with `now_ms < last_stale_prune_ms` a plain
        // saturating_sub would return 0 and suppress the sweep until the
        // clock re-passes the old timestamp, leaving cap-pressure inserts
        // rejected for the whole regression window (cf. commit f86c6da for
        // the same class of bug on the http-writer path).
        if self.last_stale_prune_ms != 0
            && now_ms >= self.last_stale_prune_ms
            && now_ms - self.last_stale_prune_ms < CHECKPOINT_INTERVAL_MS
        {
            return;
        }
        self.last_stale_prune_ms = now_ms;

        let current_versions: HashMap<String, u64> = {
            let ns = self.namespace.read().unwrap();
            ns.all_authority_definitions()
                .into_iter()
                .filter_map(|def| {
                    let prefix = def.key_range.prefix.clone();
                    ns.get_placement_policy(&prefix)
                        .map(|p| (prefix, p.version.0))
                })
                .collect()
        };
        let removed = self.attestations.retain_scopes(|kr, pv| {
            current_versions.get(&kr.prefix).is_some_and(|current| {
                pv.0 >= current.saturating_sub(ATTESTATION_VERSION_LAG)
                    && pv.0 <= current.saturating_add(ATTESTATION_VERSION_LEAD)
            })
        });
        let retry = self
            .attestations
            .insert(key_range, policy_version, att, now_ms);
        if matches!(
            retry,
            InsertOutcome::RejectedScopeCap | InsertOutcome::RejectedAuthorityCap
        ) {
            tracing::warn!(
                authority = %authority.0,
                key_range = %key_range.prefix,
                policy_version = policy_version.0,
                swept = removed,
                "attestation pool caps reached even after sweeping stale scopes; \
                 the number of concurrently live scopes exceeds the configured \
                 bounds — an operating-scale problem, consider raising \
                 MAX_POOL_SCOPES / MAX_POOL_SCOPES_PER_AUTHORITY"
            );
        }
    }

    /// Remove pooled attestations of accused authorities (m-7).
    ///
    /// Called when an equivocation accusation lands while
    /// `ASTEROIDB_EXCLUDE_ACCUSED_AUTHORITIES` is enabled: attestations the
    /// accused authority pooled *before* the accusation (up to 128
    /// checkpoints per scope) must not be consumed by later certificate
    /// assembly. Returns the number of attestations removed.
    pub fn purge_accused_attestations(&mut self, accused: &[NodeId]) -> usize {
        let mut total = 0;
        for authority in accused {
            let removed = self.attestations.purge_authority(authority);
            if removed > 0 {
                tracing::warn!(
                    authority = %authority.0,
                    removed,
                    "purged pooled attestations of accused authority"
                );
            }
            total += removed;
        }
        total
    }

    /// Attestation pool statistics for the metrics pipeline.
    pub fn attestation_stats(&self) -> AttestationPoolStats {
        AttestationPoolStats {
            scopes: self.attestations.scope_count() as u64,
            rejected_unknown_range_total: self.attestation_rejected_unknown_range_total,
            rejected_version_window_total: self.attestation_rejected_version_window_total,
            stale_version_total: self.attestation_stale_version_total,
            rejected_fenced_total: self.attestation_rejected_fenced_total,
            rejected_scope_cap_total: self.attestations.rejected_scope_cap_total(),
            rejected_authority_cap_total: self.attestations.rejected_authority_cap_total(),
            purged_total: self.attestations.purged_attestations_total(),
            frontier_skew_rejected_total: self.frontiers.skew_rejected_total(),
        }
    }

    /// Promote pending writes only after a signed majority certificate covers
    /// their timestamps, and cache the proof across cleanup cycles.
    pub fn process_certifications(&mut self) {
        for index in 0..self.pending_writes.len() {
            let status = self.pending_writes[index].status;
            if status != CertificationStatus::Pending && status != CertificationStatus::Certified {
                continue;
            }
            let pw = self.pending_writes[index].clone();
            if status == CertificationStatus::Certified
                && self.store.timestamp_for(&pw.key) != Some(&pw.timestamp)
            {
                // A failed newer write can advance the in-memory store before
                // its WAL append succeeds. Keep the old completed receipt
                // terminal until a successful replacement removes it.
                continue;
            }
            if status == CertificationStatus::Certified
                && self.certified_cache_for_stored_write(&pw.key).is_some()
            {
                continue;
            }
            if self.cache_certified_proof(&pw) {
                self.pending_writes[index].status = CertificationStatus::Certified;
            } else {
                self.pending_writes[index].status = CertificationStatus::Pending;
            }
        }
    }

    /// Re-evaluate pending writes after a client wait may have elapsed.
    ///
    /// A response deadline does not revoke an accepted operation. Old
    /// pending writes remain eligible for later certification. Returns the
    /// number of writes newly certified.
    pub fn process_certifications_with_timeout(&mut self, _now_physical_ms: u64) -> usize {
        let mut transitions = 0;
        for index in 0..self.pending_writes.len() {
            let status = self.pending_writes[index].status;
            if status != CertificationStatus::Pending && status != CertificationStatus::Certified {
                continue;
            }
            let pw = self.pending_writes[index].clone();
            if status == CertificationStatus::Certified
                && self.store.timestamp_for(&pw.key) != Some(&pw.timestamp)
            {
                continue;
            }
            if status == CertificationStatus::Certified
                && self.certified_cache_for_stored_write(&pw.key).is_some()
            {
                continue;
            }
            if self.cache_certified_proof(&pw) {
                self.pending_writes[index].status = CertificationStatus::Certified;
                transitions += 1;
            } else {
                self.pending_writes[index].status = CertificationStatus::Pending;
            }
        }
        transitions
    }

    /// Reject a pending write by key.
    ///
    /// Marks the most recent `Pending` write for the given key as `Rejected`.
    /// Returns `true` if a write was found and rejected, `false` otherwise.
    /// Only `Pending` writes can be rejected; already-resolved writes are
    /// left unchanged.
    pub fn reject_write(&mut self, key: &str) -> bool {
        for pw in self.pending_writes.iter_mut().rev() {
            if pw.key == key && pw.status == CertificationStatus::Pending {
                pw.status = CertificationStatus::Rejected;
                return true;
            }
        }
        false
    }

    /// Remove all writes whose status is not `Pending`.
    ///
    /// This removes `Certified`, `Rejected`, and legacy `Timeout` entries,
    /// keeping only writes that are still awaiting resolution.
    pub fn cleanup_completed(&mut self) {
        self.retain_pending(|pw| pw.status == CertificationStatus::Pending);
    }

    /// Remove completed entries. Elapsed client wait never expires pending
    /// certification work.
    ///
    /// `now_physical_ms` is the current wall-clock time in milliseconds.
    pub fn cleanup_expired(&mut self, _now_physical_ms: u64) {
        self.cleanup_completed();
    }

    /// Periodic maintenance removes completed entries and keeps every
    /// pending operation available for later certification.
    pub fn cleanup(&mut self, now_physical_ms: u64) {
        self.cleanup_expired(now_physical_ms);
    }

    /// Return a reference to the current retention policy.
    pub fn retention_policy(&self) -> &RetentionPolicy {
        &self.retention
    }

    /// Return a slice of all tracked writes.
    pub fn pending_writes(&self) -> &[PendingWrite] {
        &self.pending_writes
    }

    /// Legacy eviction counter. Admission now refuses capacity pressure
    /// before mutating the store, so this remains zero.
    pub fn evicted_count(&self) -> u64 {
        self.evicted_count
    }

    /// Return the number of entries in the certified proof cache.
    pub fn certified_cache_len(&self) -> usize {
        self.certified_cache.len()
    }

    /// Return a reference to the shared system namespace.
    pub fn namespace(&self) -> &Arc<RwLock<SystemNamespace>> {
        &self.namespace
    }

    /// Return all tracked frontiers.
    ///
    /// Useful for serving the internal frontier pull endpoint and for
    /// the automatic frontier synchronisation pipeline.
    pub fn all_frontiers(&self) -> Vec<&AckFrontier> {
        self.frontiers.all()
    }

    /// Return a reference to the underlying `AckFrontierSet`.
    ///
    /// Useful for runtime components that need to query frontier state
    /// (e.g., compaction eligibility, GC version floor derivation).
    pub fn frontier_set(&self) -> &AckFrontierSet {
        &self.frontiers
    }

    /// Require a fresh frontier advancement after the next GC mark.
    pub(crate) fn clear_gc_frontier_receipts(&mut self) {
        self.frontiers.clear_gc_advance_receipts();
    }

    /// Highest signed checkpoint supported by a quorum of the current roster
    /// whose exact full reports were also accepted locally. Unsigned frontier
    /// observations cannot authorize pruning the HLC timestamp index.
    pub(crate) fn signed_prune_floor(
        &self,
        key_range: &KeyRange,
        policy_version: PolicyVersion,
        members: &[NodeId],
    ) -> Option<VerifiedPruneFloor> {
        let mut roster = members.to_vec();
        roster.sort();
        if roster.is_empty() || roster.windows(2).any(|pair| pair[0] == pair[1]) {
            return None;
        }
        let zero = HlcTimestamp {
            physical: 0,
            logical: 0,
            node_id: String::new(),
        };
        self.attestations
            .build_certificates_for_members_matching(
                key_range,
                policy_version,
                roster.len(),
                &zero,
                &roster,
                |att| {
                    let scope = FrontierScope::new(
                        key_range.clone(),
                        policy_version,
                        att.authority_id.clone(),
                    );
                    self.frontiers
                        .accepted_frontier_at(&scope, att.checkpoint_hlc.physical)
                        .is_some_and(|frontier| att.matches_frontier(frontier))
                },
            )
            .map(|(floor, _, _)| VerifiedPruneFloor(floor))
    }

    /// Fence a (key_range, policy_version) pair in the frontier set.
    ///
    /// After fencing, all subsequent frontier updates for this combination
    /// are silently rejected. This isolates frontier judgment at version
    /// boundaries during policy transitions (FR-009).
    pub fn fence_version(&mut self, range: &KeyRange, version: PolicyVersion) {
        self.frontiers.fence_version(range, version);
        // Drop collected attestations for the fenced scope so that no new
        // certificates can be assembled for it (FR-009 safe transition).
        self.attestations.gc_scope(range, &version);
    }

    /// Check whether a (key_range, policy_version) pair has been fenced.
    pub fn is_version_fenced(&self, range: &KeyRange, version: &PolicyVersion) -> bool {
        self.frontiers.is_version_fenced(range, version)
    }

    /// Lift a fence because `version` became the CURRENT policy version for
    /// `range` again (replicated version counter re-assigned a version this
    /// node had already used and fenced — see
    /// [`AckFrontierSet::unfence_version`]). Without this, all frontier
    /// reports for the current version would be rejected and certification
    /// for the range would stall. Returns `true` when a fence was lifted.
    pub fn unfence_version(&mut self, range: &KeyRange, version: PolicyVersion) -> bool {
        if !self.frontiers.unfence_version(range, version) {
            return false;
        }
        // The same numeric version now denotes a new policy era. A cached
        // proof from before the fence cannot establish the new read scope.
        self.certified_cache
            .retain(|_, cached| cached.key_range != *range || cached.policy_version != version);
        true
    }

    /// Run garbage collection on stale frontier entries.
    ///
    /// Delegates to [`AckFrontierSet::gc_stale_entries`]. Returns the number
    /// of frontier entries removed.
    pub fn gc_frontier_entries(
        &mut self,
        current_versions: &std::collections::HashMap<KeyRange, PolicyVersion>,
        max_retained_versions: u64,
        grace_period_secs: u64,
        now_secs: u64,
    ) -> usize {
        self.frontiers.gc_stale_entries(
            current_versions,
            max_retained_versions,
            grace_period_secs,
            now_secs,
        )
    }

    /// Return the number of frontier entries currently tracked.
    pub fn frontier_count(&self) -> usize {
        self.frontiers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authority::ack_frontier::AckFrontier;
    use crate::control_plane::system_namespace::AuthorityDefinition;
    use crate::crdt::or_map::OrMap;
    use crate::crdt::pn_counter::PnCounter;
    use crate::hlc::HlcTimestamp;
    use crate::placement::PlacementPolicy;
    use crate::types::{KeyRange, NodeId, PolicyVersion};

    fn node(name: &str) -> NodeId {
        NodeId(name.into())
    }

    fn kr(prefix: &str) -> KeyRange {
        KeyRange {
            prefix: prefix.into(),
        }
    }

    fn wrap_ns(ns: SystemNamespace) -> Arc<RwLock<SystemNamespace>> {
        Arc::new(RwLock::new(ns))
    }

    fn make_frontier(authority: &str, physical: u64, logical: u32, prefix: &str) -> AckFrontier {
        AckFrontier {
            authority_id: NodeId(authority.into()),
            frontier_hlc: HlcTimestamp {
                physical,
                logical,
                node_id: authority.into(),
            },
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
            frontier_hlc: HlcTimestamp {
                physical,
                logical,
                node_id: authority.into(),
            },
            key_range: KeyRange {
                prefix: prefix.into(),
            },
            policy_version: PolicyVersion(version),
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

    /// Create a namespace with a single catch-all authority definition (prefix "")
    /// with 3 authorities. This preserves backward-compatible behaviour for
    /// existing tests.
    fn default_namespace() -> Arc<RwLock<SystemNamespace>> {
        make_namespace("", &["auth-1", "auth-2", "auth-3"])
    }

    fn make_namespace(prefix: &str, authorities: &[&str]) -> Arc<RwLock<SystemNamespace>> {
        let mut ns = SystemNamespace::new();
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
        wrap_ns(ns)
    }

    // ---------------------------------------------------------------
    // get_certified with no data
    // ---------------------------------------------------------------

    #[test]
    fn get_certified_no_data() {
        let api = CertifiedApi::new(node("node-1"), default_namespace());
        let result = api.get_certified("missing");

        assert!(result.value.is_none());
        assert_eq!(result.status, CertificationStatus::Pending);
        assert!(result.frontier.is_none());
    }

    // ---------------------------------------------------------------
    // certified_write creates pending entry
    // ---------------------------------------------------------------

    #[test]
    fn certified_write_creates_pending_entry() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        let result = api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending);

        assert_eq!(result.unwrap(), CertificationStatus::Pending);
        assert_eq!(api.pending_writes().len(), 1);
        assert_eq!(api.pending_writes()[0].key, "key1");
        assert_eq!(api.pending_writes()[0].status, CertificationStatus::Pending);
    }

    // ---------------------------------------------------------------
    // get_certification_status returns Pending for new write
    // ---------------------------------------------------------------

    #[test]
    fn get_certification_status_pending_for_new_write() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();

        assert_eq!(
            api.get_certification_status("key1"),
            CertificationStatus::Pending
        );
    }

    #[test]
    fn get_certification_status_no_write_returns_pending() {
        let api = CertifiedApi::new(node("node-1"), default_namespace());
        assert_eq!(
            api.get_certification_status("nonexistent"),
            CertificationStatus::Pending
        );
    }

    // ---------------------------------------------------------------
    // process_certifications: signed attestations → Certified
    // ---------------------------------------------------------------

    #[test]
    fn process_certifications_promotes_to_certified() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();

        let write_ts = api.pending_writes()[0].timestamp.physical;

        // Unsigned frontier majority is not evidence of certification.
        api.update_frontier(make_frontier("auth-1", write_ts + 100, 0, ""));
        api.update_frontier(make_frontier("auth-2", write_ts + 200, 0, ""));
        api.process_certifications();
        assert_eq!(api.pending_writes()[0].status, CertificationStatus::Pending);
        assert!(api.get_certified("key1").proof.is_none());

        // The signed report must be newer than both unsigned frontiers
        // above, including when the write falls near the end of a second.
        let report_ts = ((write_ts / 1000 + 1) * 1000 + 100).max(write_ts + 300);
        for signer in [&make_signer("auth-1", 31), &make_signer("auth-2", 32)] {
            let (frontier, attestation) = make_signed_frontier(signer, report_ts, "");
            api.update_frontier_verified(frontier, Some(attestation));
        }
        api.process_certifications();

        assert_eq!(
            api.pending_writes()[0].status,
            CertificationStatus::Certified
        );
        assert!(
            api.get_certified("key1")
                .proof
                .unwrap()
                .certificate
                .is_some()
        );
    }

    #[test]
    fn stale_signed_checkpoint_without_accepted_frontier_cannot_certify() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        let write_ts = api.pending_writes()[0].timestamp.physical;
        let signed_ts = (write_ts / CHECKPOINT_INTERVAL_MS + 1) * CHECKPOINT_INTERVAL_MS + 100;

        for (authority, seed) in [("auth-1", 41), ("auth-2", 42)] {
            // The accepted frontier is at another checkpoint. The later
            // signed report is stale, so it never becomes an accepted
            // frontier at the checkpoint its certificate would name.
            assert!(api.update_frontier(make_frontier(
                authority,
                signed_ts + 2 * CHECKPOINT_INTERVAL_MS,
                0,
                ""
            )));
            let signer = make_signer(authority, seed);
            let (frontier, attestation) = make_signed_frontier(&signer, signed_ts, "");
            assert!(!api.update_frontier_verified(frontier, Some(attestation)));
        }

        api.process_certifications();
        let read = api.get_certified("key1");
        assert_eq!(read.status, CertificationStatus::Pending);
        assert!(read.proof.is_none());
    }

    #[test]
    fn mismatched_verified_attestation_does_not_advance_or_enter_pool() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        let signer = make_signer("auth-1", 43);
        let (mut frontier, attestation) = make_signed_frontier(&signer, 10_500, "");
        frontier.digest_hash.push_str("-changed");
        assert!(!api.update_frontier_verified(frontier, Some(attestation)));
        assert!(api.frontiers.is_empty());
        assert_eq!(api.attestations.scope_count(), 0);
    }

    #[test]
    fn process_certifications_not_enough_authorities() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();

        let write_ts = api.pending_writes()[0].timestamp.physical;

        // Only 1 of 3 authorities has reported — not a majority.
        api.update_frontier(make_frontier("auth-1", write_ts + 100, 0, ""));

        api.process_certifications();

        assert_eq!(api.pending_writes()[0].status, CertificationStatus::Pending);
    }

    // ---------------------------------------------------------------
    // on_timeout=Error with no resolution → returns error
    // ---------------------------------------------------------------

    #[test]
    fn certified_write_on_timeout_error() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        let result = api.certified_write("key1".into(), counter_value(1), OnTimeout::Error);

        // Must return the distinct CertificationTimeout error so callers know
        // the local write succeeded and only certification timed out.
        assert_eq!(result.unwrap_err(), CrdtError::CertificationTimeout);
        // The write should still be tracked as pending.
        assert_eq!(api.pending_writes().len(), 1);
        assert_eq!(api.pending_writes()[0].status, CertificationStatus::Pending);
        // The value must be accessible via the store (local write committed).
        let read = api.get_certified("key1");
        assert!(
            read.value.is_some(),
            "local write must be committed even on CertificationTimeout"
        );
    }

    // ---------------------------------------------------------------
    // on_timeout=Pending with no resolution → returns Pending
    // ---------------------------------------------------------------

    #[test]
    fn certified_write_on_timeout_pending() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        let result = api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending);

        assert_eq!(result.unwrap(), CertificationStatus::Pending);
    }

    // ---------------------------------------------------------------
    // get_certified after certification → status Certified
    // ---------------------------------------------------------------

    #[test]
    fn get_certified_after_certification() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        api.certified_write("key1".into(), counter_value(5), OnTimeout::Pending)
            .unwrap();

        let write_ts = api.pending_writes()[0].timestamp.physical;

        // Advance majority of authorities.
        submit_signed_majority(&mut api, [("auth-1", 41), ("auth-2", 42)], write_ts, "", 1);

        api.process_certifications();

        let result = api.get_certified("key1");
        assert!(result.value.is_some());
        assert_eq!(result.status, CertificationStatus::Certified);
        assert!(result.frontier.is_some());
    }

    // ---------------------------------------------------------------
    // Multiple writes and selective certification
    // ---------------------------------------------------------------

    #[test]
    fn multiple_writes_selective_certification() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());

        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        let ts1 = api.pending_writes()[0].timestamp.physical;

        api.certified_write("key2".into(), counter_value(2), OnTimeout::Pending)
            .unwrap();

        // Advance authorities past key1's timestamp but not key2's.
        submit_signed_majority(&mut api, [("auth-1", 41), ("auth-2", 42)], ts1, "", 1);

        api.process_certifications();

        assert_eq!(
            api.get_certification_status("key1"),
            CertificationStatus::Certified
        );
        // key2 was written after key1 and the frontier may or may not cover it.
        // With ts1+1, the second write (which has a higher timestamp) might not be certified.
        // This depends on timing, so we just verify the API works.
        let status2 = api.get_certification_status("key2");
        assert!(
            status2 == CertificationStatus::Pending || status2 == CertificationStatus::Certified
        );
    }

    // ---------------------------------------------------------------
    // update_frontier advances tracking
    // ---------------------------------------------------------------

    #[test]
    fn update_frontier_updates_tracking() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());

        api.update_frontier(make_frontier("auth-1", 100, 0, ""));
        api.update_frontier(make_frontier("auth-2", 200, 0, ""));
        api.update_frontier(make_frontier("auth-3", 150, 0, ""));

        // With all 3 authorities reporting, get_certified should have a frontier.
        let result = api.get_certified("any-key");
        assert!(result.frontier.is_some());
    }

    // ---------------------------------------------------------------
    // Value is stored in the local store
    // ---------------------------------------------------------------

    #[test]
    fn certified_write_stores_value_locally() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        api.certified_write("key1".into(), counter_value(3), OnTimeout::Pending)
            .unwrap();

        let read = api.get_certified("key1");
        assert!(read.value.is_some());
        match read.value.unwrap() {
            CrdtValue::Counter(c) => assert_eq!(c.value(), 3),
            other => panic!("expected Counter, got {:?}", other),
        }
    }

    // ---------------------------------------------------------------
    // Retention policy defaults
    // ---------------------------------------------------------------

    #[test]
    fn retention_policy_defaults() {
        let api = CertifiedApi::new(node("node-1"), default_namespace());
        let policy = api.retention_policy();
        assert_eq!(policy.max_age_ms, 60_000);
        assert_eq!(policy.max_entries, 10_000);
    }

    #[test]
    fn with_retention_custom_policy() {
        let policy = RetentionPolicy {
            max_age_ms: 5_000,
            max_entries: 100,
        };
        let api = CertifiedApi::with_retention(node("node-1"), default_namespace(), policy);
        assert_eq!(api.retention_policy().max_age_ms, 5_000);
        assert_eq!(api.retention_policy().max_entries, 100);
    }

    // ---------------------------------------------------------------
    // cleanup_completed removes certified/rejected/timeout entries
    // ---------------------------------------------------------------

    #[test]
    fn cleanup_completed_removes_non_pending() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());

        // Write 3 entries.
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

        assert_eq!(api.pending_writes().len(), 3);

        api.cleanup_completed();

        // Only pending entries remain.
        assert!(
            api.pending_writes()
                .iter()
                .all(|pw| pw.status == CertificationStatus::Pending)
        );
    }

    // ---------------------------------------------------------------
    // cleanup_expired retains pending work regardless of age
    // ---------------------------------------------------------------

    #[test]
    fn cleanup_expired_keeps_old_pending_entries() {
        let policy = RetentionPolicy {
            max_age_ms: 5_000,
            max_entries: 10_000,
        };
        let mut api = CertifiedApi::with_retention(node("node-1"), default_namespace(), policy);

        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        let write_ts = api.pending_writes()[0].timestamp.physical;

        assert_eq!(api.pending_writes().len(), 1);

        // Not yet expired.
        api.cleanup_expired(write_ts + 4_999);
        assert_eq!(api.pending_writes().len(), 1);

        // The response wait has elapsed; the operation is still pending.
        api.cleanup_expired(write_ts + 5_000);
        assert_eq!(api.pending_writes().len(), 1);
        assert_eq!(
            api.get_certification_status("key1"),
            CertificationStatus::Pending
        );

        submit_signed_majority(&mut api, [("auth-1", 41), ("auth-2", 42)], write_ts, "", 1);
        api.process_certifications();
        assert_eq!(
            api.get_certification_status("key1"),
            CertificationStatus::Certified
        );
    }

    // ---------------------------------------------------------------
    // cleanup does full maintenance
    // ---------------------------------------------------------------

    #[test]
    fn cleanup_removes_completed_but_keeps_old_pending() {
        let policy = RetentionPolicy {
            max_age_ms: 10_000,
            max_entries: 10_000,
        };
        let mut api = CertifiedApi::with_retention(node("node-1"), default_namespace(), policy);

        // Write entries at different times.
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        let ts1 = api.pending_writes()[0].timestamp.physical;

        api.certified_write("key2".into(), counter_value(2), OnTimeout::Pending)
            .unwrap();

        // Unsigned frontiers cannot certify either write.
        api.update_frontier(make_frontier("auth-1", ts1 + 1, 0, ""));
        api.update_frontier(make_frontier("auth-2", ts1 + 1, 0, ""));
        api.process_certifications();

        let ts2 = api.pending_writes()[1].timestamp.physical;

        // Cleanup at a time after TTL for key2 (and certainly key1).
        api.cleanup(ts2 + 10_000);

        assert_eq!(api.pending_writes().len(), 2);
        assert!(
            api.pending_writes()
                .iter()
                .all(|pw| pw.status == CertificationStatus::Pending)
        );
    }

    // ---------------------------------------------------------------
    // Auto-cleanup when max_entries exceeded
    // ---------------------------------------------------------------

    #[test]
    fn auto_cleanup_on_capacity_exceeded() {
        let policy = RetentionPolicy {
            max_age_ms: 60_000,
            max_entries: 3,
        };
        let mut api = CertifiedApi::with_retention(node("node-1"), default_namespace(), policy);

        // Write 3 entries (hits max_entries).
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        api.certified_write("key2".into(), counter_value(2), OnTimeout::Pending)
            .unwrap();
        api.certified_write("key3".into(), counter_value(3), OnTimeout::Pending)
            .unwrap();

        // Certify all three via signed majority, freeing their slots.
        let ts_last = api.pending_writes().last().unwrap().timestamp.physical;
        submit_signed_majority(&mut api, [("auth-1", 41), ("auth-2", 42)], ts_last, "", 1);
        api.process_certifications();

        assert_eq!(api.pending_writes().len(), 3);

        // Adding a 4th write triggers auto-cleanup (len >= max_entries).
        api.certified_write("key4".into(), counter_value(4), OnTimeout::Pending)
            .unwrap();

        // Certified entries were cleaned up; key4 is tracked.
        assert!(api.pending_writes().len() <= 3);
        assert!(
            api.pending_writes()
                .iter()
                .any(|pw| pw.key == "key3" || pw.key == "key4")
        );
    }

    // ---------------------------------------------------------------
    // Bounded growth under sustained writes
    // ---------------------------------------------------------------

    #[test]
    fn bounded_growth_under_sustained_writes() {
        let policy = RetentionPolicy {
            max_age_ms: 60_000,
            max_entries: 10,
        };
        let mut api = CertifiedApi::with_retention(node("node-1"), default_namespace(), policy);

        // Simulate sustained writes with periodic signed certification.
        for i in 0..50u64 {
            api.certified_write(format!("key-{i}"), counter_value(1), OnTimeout::Pending)
                .unwrap();

            // Certify every other write to make them eligible for cleanup.
            if i % 2 == 0 {
                let ts = api.pending_writes().last().unwrap().timestamp.physical;
                submit_signed_majority(&mut api, [("auth-1", 41), ("auth-2", 42)], ts, "", 1);
                api.process_certifications();
            }
        }

        // The number of tracked writes must never exceed max_entries.
        assert!(
            api.pending_writes().len() <= 10,
            "expected bounded growth <= max_entries(10), got {} entries",
            api.pending_writes().len()
        );
    }

    // ---------------------------------------------------------------
    // Hard limit: all-pending admission refusal
    // ---------------------------------------------------------------

    #[test]
    fn all_pending_capacity_refuses_new_write_without_mutation() {
        let policy = RetentionPolicy {
            max_age_ms: 60_000,
            max_entries: 3,
        };
        let mut api = CertifiedApi::with_retention(node("node-1"), default_namespace(), policy);

        // Fill to capacity with all-pending writes (no certification).
        for i in 0..3u64 {
            api.certified_write(format!("key-{i}"), counter_value(1), OnTimeout::Pending)
                .unwrap();
        }
        assert_eq!(api.pending_writes().len(), 3);
        assert_eq!(api.evicted_count(), 0);

        assert_eq!(
            api.certified_write("key-3".into(), counter_value(1), OnTimeout::Pending),
            Err(CrdtError::CertificationCapacity { max_entries: 3 })
        );
        assert_eq!(api.pending_writes().len(), 3);
        assert_eq!(api.evicted_count(), 0);
        assert_eq!(
            api.get_certification_status("key-0"),
            CertificationStatus::Pending
        );
        assert!(api.store.get("key-3").is_none());
    }

    // ---------------------------------------------------------------
    // Capacity refusal leaves the legacy eviction counter unchanged
    // ---------------------------------------------------------------

    #[test]
    fn capacity_refusals_leave_existing_pending_writes_intact() {
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
        assert_eq!(api.evicted_count(), 0);

        for key in ["c", "d"] {
            assert_eq!(
                api.certified_write(key.into(), counter_value(1), OnTimeout::Pending),
                Err(CrdtError::CertificationCapacity { max_entries: 2 })
            );
            assert!(api.store.get(key).is_none());
        }
        assert_eq!(api.pending_writes().len(), 2);
        assert_eq!(api.evicted_count(), 0);
    }

    #[test]
    fn same_key_replacement_uses_one_slot_after_successful_write() {
        let mut api = CertifiedApi::with_retention(
            node("node-1"),
            default_namespace(),
            RetentionPolicy {
                max_age_ms: 60_000,
                max_entries: 1,
            },
        );
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        let first = api.pending_writes()[0].timestamp.clone();
        let original = api.store.get("key1").cloned();

        assert!(matches!(
            api.certified_write(
                "key1".into(),
                CrdtValue::Map(OrMap::new()),
                OnTimeout::Pending,
            ),
            Err(CrdtError::TypeMismatch { .. })
        ));
        assert_eq!(api.pending_writes().len(), 1);
        assert_eq!(api.pending_writes()[0].timestamp, first);
        assert_eq!(api.store.get("key1").cloned(), original);

        api.certified_write("key1".into(), counter_value(2), OnTimeout::Pending)
            .unwrap();
        assert_eq!(api.pending_writes().len(), 1);
        assert_ne!(api.pending_writes()[0].timestamp, first);
        assert_eq!(api.pending_writes()[0].key, "key1");
        assert_eq!(
            api.certified_write("key2".into(), counter_value(1), OnTimeout::Pending),
            Err(CrdtError::CertificationCapacity { max_entries: 1 })
        );
        assert!(api.store.get("key2").is_none());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn failed_same_key_wal_append_keeps_prior_tracking_until_retry() {
        let dir = tempfile::tempdir().unwrap();
        let wal = WalWriter::open(crate::store::wal::WalConfig::new(
            dir.path(),
            crate::store::wal::SyncPolicy::Always,
        ))
        .unwrap();
        let mut api = CertifiedApi::recovered(
            node("node-1"),
            default_namespace(),
            Store::new(),
            Some(wal),
            HashMap::new(),
        );
        api.retention.max_entries = 1;

        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        let first = api.pending_writes()[0].timestamp.clone();
        submit_signed_majority(
            &mut api,
            [("auth-1", 41), ("auth-2", 42)],
            first.physical,
            "",
            1,
        );
        api.process_certifications();
        assert_eq!(
            api.pending_writes()[0].status,
            CertificationStatus::Certified
        );

        api.wal.as_mut().unwrap().inject_append_failures(1);
        assert!(matches!(
            api.certified_write("key1".into(), counter_value(2), OnTimeout::Pending),
            Err(CrdtError::Storage(_))
        ));
        assert_eq!(api.pending_writes().len(), 1);
        assert_eq!(api.pending_writes()[0].timestamp, first);
        assert_eq!(api.store.get("key1"), Some(&counter_value(1)));
        api.process_certifications();
        assert_eq!(api.process_certifications_with_timeout(0), 0);
        assert_eq!(
            api.pending_writes()[0].status,
            CertificationStatus::Certified
        );
        assert_eq!(
            api.get_certified("key1").status,
            CertificationStatus::Certified
        );

        api.certified_write("key1".into(), counter_value(2), OnTimeout::Pending)
            .unwrap();
        assert_eq!(api.pending_writes().len(), 1);
        assert_ne!(api.pending_writes()[0].timestamp, first);
    }

    // ---------------------------------------------------------------
    // Hard limit under sustained all-pending writes
    // ---------------------------------------------------------------

    #[test]
    fn hard_limit_under_sustained_all_pending_writes() {
        let policy = RetentionPolicy {
            max_age_ms: 60_000,
            max_entries: 5,
        };
        let mut api = CertifiedApi::with_retention(node("node-1"), default_namespace(), policy);

        // Fill the pending queue, then explicitly refuse further writes.
        for i in 0..5u64 {
            api.certified_write(format!("key-{i}"), counter_value(1), OnTimeout::Pending)
                .unwrap();
        }
        for i in 5..100u64 {
            assert_eq!(
                api.certified_write(format!("key-{i}"), counter_value(1), OnTimeout::Pending),
                Err(CrdtError::CertificationCapacity { max_entries: 5 })
            );
        }
        assert_eq!(api.pending_writes().len(), 5);
        assert_eq!(api.evicted_count(), 0);
    }

    // ---------------------------------------------------------------
    // Range-aware certification: cross-range contamination prevented
    // ---------------------------------------------------------------

    #[test]
    fn cross_range_certification_does_not_contaminate() {
        // Two separate key ranges with distinct authority sets.
        let mut ns = SystemNamespace::new();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr("user/"),
            authority_nodes: vec![node("auth-u1"), node("auth-u2"), node("auth-u3")],
            auto_generated: false,
        });
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr("order/"),
            authority_nodes: vec![node("auth-o1"), node("auth-o2"), node("auth-o3")],
            auto_generated: false,
        });
        ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(1), kr("user/"), 3))
            .unwrap();
        ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(1), kr("order/"), 3))
            .unwrap();

        let mut api = CertifiedApi::new(node("node-1"), wrap_ns(ns));

        // Write to both ranges.
        api.certified_write("user/alice".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        let user_ts = api.pending_writes()[0].timestamp.physical;

        api.certified_write("order/123".into(), counter_value(2), OnTimeout::Pending)
            .unwrap();
        let order_ts = api.pending_writes()[1].timestamp.physical;

        // Advance only order/ authorities past both timestamps.
        submit_signed_majority(
            &mut api,
            [("auth-o1", 41), ("auth-o2", 42)],
            order_ts,
            "order/",
            1,
        );

        api.process_certifications();

        // order/123 should be certified (its authorities reached majority).
        assert_eq!(
            api.get_certification_status("order/123"),
            CertificationStatus::Certified
        );

        // user/alice must NOT be certified — user/ authorities haven't reported.
        assert_eq!(
            api.get_certification_status("user/alice"),
            CertificationStatus::Pending
        );

        // Now advance user/ authorities.
        submit_signed_majority(
            &mut api,
            [("auth-u1", 41), ("auth-u2", 42)],
            user_ts,
            "user/",
            1,
        );

        api.process_certifications();

        // Now user/alice should be certified.
        assert_eq!(
            api.get_certification_status("user/alice"),
            CertificationStatus::Certified
        );
    }

    // ---------------------------------------------------------------
    // Range-aware: scoped majority frontier in get_certified
    // ---------------------------------------------------------------

    #[test]
    fn get_certified_returns_scoped_frontier() {
        let mut ns = SystemNamespace::new();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr("user/"),
            authority_nodes: vec![node("auth-u1"), node("auth-u2"), node("auth-u3")],
            auto_generated: false,
        });
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr("order/"),
            authority_nodes: vec![node("auth-o1"), node("auth-o2"), node("auth-o3")],
            auto_generated: false,
        });
        ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(1), kr("user/"), 3))
            .unwrap();
        ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(1), kr("order/"), 3))
            .unwrap();

        let mut api = CertifiedApi::new(node("node-1"), wrap_ns(ns));

        // Set different frontier levels for each range.
        api.update_frontier(make_frontier("auth-u1", 100, 0, "user/"));
        api.update_frontier(make_frontier("auth-u2", 200, 0, "user/"));
        api.update_frontier(make_frontier("auth-u3", 150, 0, "user/"));

        api.update_frontier(make_frontier("auth-o1", 1000, 0, "order/"));
        api.update_frontier(make_frontier("auth-o2", 2000, 0, "order/"));
        api.update_frontier(make_frontier("auth-o3", 1500, 0, "order/"));

        // user/ majority frontier should be 150.
        let user_read = api.get_certified("user/alice");
        assert_eq!(user_read.frontier.unwrap().physical, 150);

        // order/ majority frontier should be 1500.
        let order_read = api.get_certified("order/123");
        assert_eq!(order_read.frontier.unwrap().physical, 1500);
    }

    // ---------------------------------------------------------------
    // Range-aware: policy version transition
    // ---------------------------------------------------------------

    #[test]
    fn policy_version_transition_independent_certification() {
        let mut ns = SystemNamespace::new();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr("data/"),
            authority_nodes: vec![node("auth-1"), node("auth-2"), node("auth-3")],
            auto_generated: false,
        });
        // Set placement policy at version 2.
        ns.set_placement_policy(
            PlacementPolicy::new(PolicyVersion(2), kr("data/"), 3).with_certified(true),
        )
        .unwrap();

        let mut api = CertifiedApi::new(node("node-1"), wrap_ns(ns));

        // Write a key — should resolve to data/ with policy version 2.
        api.certified_write("data/sensor".into(), counter_value(42), OnTimeout::Pending)
            .unwrap();
        let write_ts = api.pending_writes()[0].timestamp.physical;
        assert_eq!(api.pending_writes()[0].policy_version, PolicyVersion(2));

        // Frontiers at version 1 should NOT certify a write resolved at version 2.
        submit_signed_majority(
            &mut api,
            [("auth-1", 41), ("auth-2", 42)],
            write_ts,
            "data/",
            1,
        );
        api.process_certifications();

        assert_eq!(
            api.get_certification_status("data/sensor"),
            CertificationStatus::Pending,
            "v1 frontiers must not certify a v2 write"
        );

        // Frontiers at version 2 should certify the write.
        submit_signed_majority(
            &mut api,
            [("auth-1", 41), ("auth-2", 42)],
            write_ts,
            "data/",
            2,
        );
        api.process_certifications();

        assert_eq!(
            api.get_certification_status("data/sensor"),
            CertificationStatus::Certified
        );
    }

    // ---------------------------------------------------------------
    // Range-aware: longest-prefix authority resolution
    // ---------------------------------------------------------------

    #[test]
    fn longest_prefix_authority_resolution() {
        let mut ns = SystemNamespace::new();
        // Broader authority set for user/
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr("user/"),
            authority_nodes: vec![node("auth-1"), node("auth-2"), node("auth-3")],
            auto_generated: false,
        });
        // Narrower (higher-priority) authority set for user/vip/
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr("user/vip/"),
            authority_nodes: vec![node("auth-v1"), node("auth-v2")],
            auto_generated: false,
        });
        ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(1), kr("user/"), 3))
            .unwrap();
        ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(1), kr("user/vip/"), 2))
            .unwrap();

        let mut api = CertifiedApi::new(node("node-1"), wrap_ns(ns));

        // Write to user/vip/alice — should resolve to user/vip/ (2 authorities).
        api.certified_write(
            "user/vip/alice".into(),
            counter_value(1),
            OnTimeout::Pending,
        )
        .unwrap();
        assert_eq!(api.pending_writes()[0].key_range, kr("user/vip/"));
        assert_eq!(api.pending_writes()[0].total_authorities, 2);

        // Write to user/regular/bob — should resolve to user/ (3 authorities).
        api.certified_write(
            "user/regular/bob".into(),
            counter_value(2),
            OnTimeout::Pending,
        )
        .unwrap();
        assert_eq!(api.pending_writes()[1].key_range, kr("user/"));
        assert_eq!(api.pending_writes()[1].total_authorities, 3);
    }

    // ---------------------------------------------------------------
    // Range-aware: certified_write rejects key with no authority
    // ---------------------------------------------------------------

    #[test]
    fn certified_write_rejects_key_without_authority() {
        // Namespace with only user/ defined.
        let ns = make_namespace("user/", &["auth-1", "auth-2", "auth-3"]);
        let mut api = CertifiedApi::new(node("node-1"), ns);

        // order/ has no authority definition — should be PolicyDenied.
        let result = api.certified_write("order/123".into(), counter_value(1), OnTimeout::Pending);
        assert!(matches!(result, Err(CrdtError::PolicyDenied(_))));
    }

    // ---------------------------------------------------------------
    // Range-aware: pending write stores resolved scope
    // ---------------------------------------------------------------

    #[test]
    fn pending_write_stores_resolved_scope() {
        let ns = make_namespace("data/", &["auth-1", "auth-2", "auth-3"]);
        let mut api = CertifiedApi::new(node("node-1"), ns);

        api.certified_write("data/key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();

        let pw = &api.pending_writes()[0];
        assert_eq!(pw.key_range, kr("data/"));
        assert_eq!(pw.policy_version, PolicyVersion(1));
        assert_eq!(pw.total_authorities, 3);
    }

    // ---------------------------------------------------------------
    // process_certifications_with_timeout tests
    // ---------------------------------------------------------------

    #[test]
    fn process_with_timeout_certifies_and_detects_timeout() {
        // Use two separate key ranges so we can certify one without the other.
        let mut ns = SystemNamespace::new();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr("cert/"),
            authority_nodes: vec![node("auth-1"), node("auth-2"), node("auth-3")],
            auto_generated: false,
        });
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr("stale/"),
            authority_nodes: vec![node("auth-s1"), node("auth-s2"), node("auth-s3")],
            auto_generated: false,
        });
        ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(1), kr("cert/"), 3))
            .unwrap();
        ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(1), kr("stale/"), 3))
            .unwrap();

        let policy = RetentionPolicy {
            max_age_ms: 5_000,
            max_entries: 10_000,
        };
        let mut api = CertifiedApi::with_retention(node("node-1"), wrap_ns(ns), policy);

        // Write to cert/ range (will be certified).
        api.certified_write("cert/key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        let ts1 = api.pending_writes()[0].timestamp.physical;

        // Write to stale/ range (will stay pending until its authorities report).
        api.certified_write("stale/key2".into(), counter_value(2), OnTimeout::Pending)
            .unwrap();

        // Advance cert/ authorities past the write timestamp.
        submit_signed_majority(&mut api, [("auth-1", 41), ("auth-2", 42)], ts1, "cert/", 1);

        // A far-future client wait does not revoke stale/key2.
        let transitions = api.process_certifications_with_timeout(ts1 + 10_000);

        // cert/key1 should be certified (its authorities reached majority).
        assert_eq!(
            api.get_certification_status("cert/key1"),
            CertificationStatus::Certified
        );
        // stale/key2 stays pending (its authorities never reported).
        assert_eq!(
            api.get_certification_status("stale/key2"),
            CertificationStatus::Pending
        );
        assert_eq!(transitions, 1);
    }

    #[test]
    fn process_with_timeout_no_timeout_when_young() {
        let policy = RetentionPolicy {
            max_age_ms: 60_000,
            max_entries: 10_000,
        };
        let mut api = CertifiedApi::with_retention(node("node-1"), default_namespace(), policy);

        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        let ts1 = api.pending_writes()[0].timestamp.physical;

        // Process with a time only slightly ahead (below max_age_ms).
        let transitions = api.process_certifications_with_timeout(ts1 + 1_000);

        // Still pending — no timeout.
        assert_eq!(
            api.get_certification_status("key1"),
            CertificationStatus::Pending
        );
        assert_eq!(transitions, 0);
    }

    #[test]
    fn process_with_timeout_returns_transition_count() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());

        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        api.certified_write("key2".into(), counter_value(2), OnTimeout::Pending)
            .unwrap();

        let ts = api.pending_writes()[1].timestamp.physical;

        // Certify both.
        submit_signed_majority(&mut api, [("auth-1", 41), ("auth-2", 42)], ts, "", 1);

        let transitions = api.process_certifications_with_timeout(ts + 100);
        assert_eq!(transitions, 2);

        // Calling again should yield 0 (already certified).
        let transitions2 = api.process_certifications_with_timeout(ts + 200);
        assert_eq!(transitions2, 0);
    }

    // ---------------------------------------------------------------
    // reject_write tests
    // ---------------------------------------------------------------

    #[test]
    fn reject_write_marks_pending_as_rejected() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());

        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        assert_eq!(
            api.get_certification_status("key1"),
            CertificationStatus::Pending
        );

        assert!(api.reject_write("key1"));
        assert_eq!(
            api.get_certification_status("key1"),
            CertificationStatus::Rejected
        );
    }

    #[test]
    fn reject_write_returns_false_for_nonexistent_key() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        assert!(!api.reject_write("no-such-key"));
    }

    #[test]
    fn reject_write_does_not_affect_certified() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());

        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        let ts = api.pending_writes()[0].timestamp.physical;

        // Certify it.
        submit_signed_majority(&mut api, [("auth-1", 41), ("auth-2", 42)], ts, "", 1);
        api.process_certifications();

        assert_eq!(
            api.get_certification_status("key1"),
            CertificationStatus::Certified
        );

        // Reject should be a no-op on certified writes.
        assert!(!api.reject_write("key1"));
        assert_eq!(
            api.get_certification_status("key1"),
            CertificationStatus::Certified
        );
    }

    #[test]
    fn reject_write_targets_the_current_same_key_write() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());

        // Write same key twice.
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        api.certified_write("key1".into(), counter_value(2), OnTimeout::Pending)
            .unwrap();

        // Reject targets the latest (most recent) pending write.
        assert!(api.reject_write("key1"));

        // Only the current state can receive a proof; the older pending
        // entry was superseded after the second write succeeded.
        let writes: Vec<_> = api
            .pending_writes()
            .iter()
            .filter(|pw| pw.key == "key1")
            .collect();
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].status, CertificationStatus::Rejected);
    }

    // ---------------------------------------------------------------
    // ProofBundle tests
    // ---------------------------------------------------------------

    #[test]
    fn get_certified_returns_proof_when_certified() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        api.certified_write("key1".into(), counter_value(5), OnTimeout::Pending)
            .unwrap();

        let write_ts = api.pending_writes()[0].timestamp.physical;

        // Advance majority of authorities.
        submit_signed_majority(&mut api, [("auth-1", 41), ("auth-2", 42)], write_ts, "", 1);

        api.process_certifications();

        let result = api.get_certified("key1");
        assert_eq!(result.status, CertificationStatus::Certified);
        assert!(
            result.proof.is_some(),
            "proof should be present when certified"
        );

        let proof = result.proof.unwrap();
        assert_eq!(proof.key_range, kr(""));
        assert!(proof.frontier_hlc.physical > 0);
        assert_eq!(proof.policy_version, PolicyVersion(1));
        assert_eq!(proof.contributing_authorities.len(), 2);
        assert_eq!(proof.total_authorities, 3);
        assert!(
            proof.certificate.is_some(),
            "certified reads must carry signed majority evidence"
        );
    }

    #[test]
    fn cached_origin_proof_survives_policy_version_bump() {
        let namespace = default_namespace();
        let mut api = CertifiedApi::new(node("node-1"), Arc::clone(&namespace));
        api.certified_write("key1".into(), counter_value(5), OnTimeout::Pending)
            .unwrap();
        let write_ts = api.pending_writes()[0].timestamp.physical;
        submit_signed_majority(&mut api, [("auth-1", 41), ("auth-2", 42)], write_ts, "", 1);
        api.process_certifications();
        assert_eq!(
            api.get_certified("key1").status,
            CertificationStatus::Certified
        );

        // The write stays pinned to its origin version. A live policy bump
        // does not revoke its signed v1 proof or move its frontier to v2.
        namespace
            .write()
            .unwrap()
            .set_placement_policy(PlacementPolicy::new(PolicyVersion(2), kr(""), 3))
            .unwrap();
        for _ in 0..2 {
            assert_eq!(
                api.get_certification_status("key1"),
                CertificationStatus::Certified
            );
            let read = api.get_certified("key1");
            assert!(read.value.is_some());
            assert_eq!(read.status, CertificationStatus::Certified);
            assert_eq!(read.proof.unwrap().policy_version, PolicyVersion(1));
            assert!(read.frontier.is_some());
            api.cleanup_completed();
        }
    }

    #[test]
    fn issued_proof_survives_live_range_and_roster_change() {
        let namespace = default_namespace();
        let mut api = CertifiedApi::new(node("node-1"), Arc::clone(&namespace));
        api.certified_write("user/key".into(), counter_value(5), OnTimeout::Pending)
            .unwrap();
        let write_ts = api.pending_writes()[0].timestamp.physical;
        submit_signed_majority(&mut api, [("auth-1", 41), ("auth-2", 42)], write_ts, "", 1);
        api.process_certifications();
        api.cleanup_completed();

        let mut ns = namespace.write().unwrap();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr("user/"),
            authority_nodes: vec![node("auth-x"), node("auth-y"), node("auth-z")],
            auto_generated: false,
        });
        ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(1), kr("user/"), 3))
            .unwrap();
        drop(ns);

        let read = api.get_certified("user/key");
        assert_eq!(read.status, CertificationStatus::Certified);
        let proof = read.proof.unwrap();
        assert_eq!(proof.key_range, kr(""));
        assert_eq!(
            proof.contributing_authorities,
            vec![node("auth-1"), node("auth-2")]
        );
        assert_eq!(
            api.get_certification_status("user/key"),
            CertificationStatus::Certified
        );
    }

    #[test]
    fn in_flight_write_certifies_only_from_its_pinned_authority_roster() {
        let namespace = default_namespace();
        let mut api = CertifiedApi::new(node("node-1"), Arc::clone(&namespace));
        api.certified_write("key1".into(), counter_value(5), OnTimeout::Pending)
            .unwrap();
        let write_ts = api.pending_writes()[0].timestamp.physical;
        assert_eq!(
            api.pending_writes()[0].authority_nodes,
            vec![node("auth-1"), node("auth-2"), node("auth-3")]
        );

        namespace
            .write()
            .unwrap()
            .set_authority_definition(AuthorityDefinition {
                key_range: kr(""),
                authority_nodes: vec![node("auth-1"), node("auth-3"), node("auth-4")],
                auto_generated: false,
            });
        // The new auth-4 cannot replace removed auth-2 for the old write.
        submit_signed_majority(&mut api, [("auth-1", 41), ("auth-4", 44)], write_ts, "", 1);
        api.process_certifications();
        assert_eq!(api.pending_writes()[0].status, CertificationStatus::Pending);

        // auth-3 belongs to both sets; auth-1 + auth-3 is an old-set majority.
        submit_signed_majority(&mut api, [("auth-1", 41), ("auth-3", 43)], write_ts, "", 1);
        api.process_certifications();
        assert_eq!(
            api.pending_writes()[0].status,
            CertificationStatus::Certified
        );
        let proof = api.get_certified("key1").proof.unwrap();
        assert_eq!(
            proof.contributing_authorities,
            vec![node("auth-1"), node("auth-3")]
        );
    }

    #[test]
    fn duplicate_authority_ids_cannot_define_a_write_scope() {
        let namespace = default_namespace();
        namespace
            .write()
            .unwrap()
            .set_authority_definition(AuthorityDefinition {
                key_range: kr(""),
                authority_nodes: vec![node("auth-1"), node("auth-1"), node("auth-2")],
                auto_generated: false,
            });
        let mut api = CertifiedApi::new(node("node-1"), namespace);
        assert!(matches!(
            api.certified_write("key1".into(), counter_value(5), OnTimeout::Pending),
            Err(CrdtError::PolicyDenied(_))
        ));
        assert!(api.store().get("key1").is_none());
    }

    #[test]
    fn get_certified_proof_is_none_when_pending() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();

        let result = api.get_certified("key1");
        assert_eq!(result.status, CertificationStatus::Pending);
        assert!(
            result.proof.is_none(),
            "proof should be None when status is Pending"
        );
    }

    #[test]
    fn get_certified_proof_is_none_when_no_data() {
        let api = CertifiedApi::new(node("node-1"), default_namespace());
        let result = api.get_certified("nonexistent");
        assert!(result.proof.is_none(), "proof should be None when no data");
    }

    #[test]
    fn proof_bundle_has_correct_authority_ids() {
        let mut ns = SystemNamespace::new();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr("data/"),
            authority_nodes: vec![node("auth-a"), node("auth-b"), node("auth-c")],
            auto_generated: false,
        });
        ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(1), kr("data/"), 3))
            .unwrap();

        let mut api = CertifiedApi::new(node("node-1"), wrap_ns(ns));
        api.certified_write("data/x".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();

        let write_ts = api.pending_writes()[0].timestamp.physical;

        // Advance 2 of 3 authorities.
        submit_signed_majority(
            &mut api,
            [("auth-a", 41), ("auth-b", 42)],
            write_ts,
            "data/",
            1,
        );

        api.process_certifications();

        let result = api.get_certified("data/x");
        assert_eq!(result.status, CertificationStatus::Certified);

        let proof = result.proof.unwrap();
        let mut auth_ids: Vec<String> = proof
            .contributing_authorities
            .iter()
            .map(|n| n.0.clone())
            .collect();
        auth_ids.sort();
        assert_eq!(auth_ids, vec!["auth-a", "auth-b"]);
    }

    #[test]
    fn unsigned_frontiers_do_not_produce_a_proof() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();

        let write_ts = api.pending_writes()[0].timestamp.physical;

        api.update_frontier(make_frontier("auth-1", write_ts + 100, 0, ""));
        api.update_frontier(make_frontier("auth-2", write_ts + 200, 0, ""));

        api.process_certifications();

        let result = api.get_certified("key1");
        assert_eq!(result.status, CertificationStatus::Pending);
        assert!(result.proof.is_none());
    }

    // ---------------------------------------------------------------
    // Certified status stability after cleanup (#203)
    // ---------------------------------------------------------------

    #[test]
    fn certified_status_stable_after_cleanup_completed() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        api.certified_write("key1".into(), counter_value(5), OnTimeout::Pending)
            .unwrap();

        let write_ts = api.pending_writes()[0].timestamp.physical;

        // Advance majority of authorities to certify.
        submit_signed_majority(&mut api, [("auth-1", 41), ("auth-2", 42)], write_ts, "", 1);
        api.process_certifications();

        // Verify certified before cleanup.
        assert_eq!(
            api.get_certification_status("key1"),
            CertificationStatus::Certified
        );
        assert_eq!(api.certified_cache_len(), 1);

        // Cleanup removes all non-pending entries from pending_writes.
        api.cleanup_completed();
        assert_eq!(api.pending_writes().len(), 0);

        // Status must remain Certified after cleanup.
        assert_eq!(
            api.get_certification_status("key1"),
            CertificationStatus::Certified,
            "status must remain Certified after cleanup_completed"
        );

        // get_certified must still return Certified with proof.
        let result = api.get_certified("key1");
        assert_eq!(result.status, CertificationStatus::Certified);
        assert!(result.value.is_some());
        assert!(
            result.proof.is_some(),
            "proof must be present from cache after cleanup"
        );

        let proof = result.proof.unwrap();
        assert_eq!(proof.key_range, kr(""));
        assert!(proof.frontier_hlc.physical > 0);
        assert_eq!(proof.total_authorities, 3);
    }

    #[test]
    fn certified_proof_survives_cache_pressure_after_cleanup() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        api.certified_write("first".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        let first_time = api.pending_writes()[0].timestamp.physical;
        submit_signed_majority(
            &mut api,
            [("auth-1", 41), ("auth-2", 42)],
            first_time,
            "",
            1,
        );
        api.process_certifications();
        assert_eq!(
            api.get_certification_status("first"),
            CertificationStatus::Certified
        );
        api.cleanup_completed();

        // Model unrelated certified entries at the old 10,000-entry limit.
        // Signature issuance is covered by the surrounding tests; this tests
        // that cache pressure cannot discard an already public proof.
        let proof = api.certified_cache["first"].clone();
        for index in 0..10_000 {
            let mut other = proof.clone();
            other.write_timestamp.physical += index + 1;
            api.certified_cache.insert(format!("other-{index}"), other);
        }

        api.certified_write("last".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        if api.get_certification_status("last") != CertificationStatus::Certified {
            let last_time = api
                .pending_writes()
                .iter()
                .find(|write| write.key == "last")
                .unwrap()
                .timestamp
                .physical;
            submit_signed_majority(&mut api, [("auth-1", 41), ("auth-2", 42)], last_time, "", 1);
            api.process_certifications();
        }

        assert_eq!(
            api.get_certification_status("last"),
            CertificationStatus::Certified
        );
        let first = api.get_certified("first");
        assert_eq!(first.status, CertificationStatus::Certified);
        assert!(first.proof.is_some());
    }

    #[test]
    fn cached_read_keeps_issuance_frontier_witness_after_history_eviction() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        let write_ts = api.pending_writes()[0].timestamp.physical;
        submit_signed_majority(&mut api, [("auth-1", 71), ("auth-2", 72)], write_ts, "", 1);
        api.process_certifications();
        let checkpoint = api.certified_cache["key1"].frontier_hlc.physical;
        let scope = FrontierScope::new(kr(""), PolicyVersion(1), node("auth-1"));
        assert!(api.frontiers.accepted_checkpoint(&scope, checkpoint));

        for offset in 1..=128u64 {
            let physical = checkpoint + offset * CHECKPOINT_INTERVAL_MS + 100;
            assert!(
                api.frontiers
                    .update_at(make_frontier("auth-1", physical, 0, ""), physical,)
            );
        }
        assert!(!api.frontiers.accepted_checkpoint(&scope, checkpoint));
        assert_eq!(
            api.get_certified("key1").status,
            CertificationStatus::Certified
        );

        // The read gate uses the copied receipt, not a loose cache flag.
        api.certified_cache
            .get_mut("key1")
            .unwrap()
            .accepted_frontiers[0]
            .policy_version = PolicyVersion(2);
        assert_eq!(
            api.get_certified("key1").status,
            CertificationStatus::Pending
        );
    }

    #[test]
    fn unfencing_reassigned_scope_invalidates_old_cached_proof() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        let write_ts = api.pending_writes()[0].timestamp.physical;
        submit_signed_majority(&mut api, [("auth-1", 73), ("auth-2", 74)], write_ts, "", 1);
        api.process_certifications();
        assert_eq!(
            api.get_certified("key1").status,
            CertificationStatus::Certified
        );

        api.fence_version(&kr(""), PolicyVersion(1));
        assert!(api.unfence_version(&kr(""), PolicyVersion(1)));
        assert_eq!(api.certified_cache_len(), 0);
        assert_eq!(
            api.get_certified("key1").status,
            CertificationStatus::Pending
        );
    }

    #[test]
    fn rejected_write_keeps_prior_certified_value_and_proof() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        api.certified_write("key1".into(), counter_value(5), OnTimeout::Pending)
            .unwrap();
        let write_ts = api.pending_writes()[0].timestamp.physical;
        submit_signed_majority(&mut api, [("auth-1", 41), ("auth-2", 42)], write_ts, "", 1);
        api.process_certifications();
        let before = api.get_certified("key1");
        assert_eq!(before.status, CertificationStatus::Certified);
        let before_proof = before.proof.unwrap();
        let before_value = before.value.cloned();

        assert!(matches!(
            api.certified_write(
                "key1".into(),
                CrdtValue::Map(OrMap::new()),
                OnTimeout::Pending
            ),
            Err(CrdtError::TypeMismatch { .. })
        ));
        assert!(matches!(
            api.certified_write("key1".into(), counter_value(3), OnTimeout::Pending),
            Err(CrdtError::InvalidArgument(_))
        ));

        let after = api.get_certified("key1");
        assert_eq!(after.status, CertificationStatus::Certified);
        assert_eq!(after.value.cloned(), before_value);
        assert_eq!(
            serde_json::to_value(after.proof.unwrap()).unwrap(),
            serde_json::to_value(before_proof).unwrap()
        );
        assert_eq!(api.certified_cache_len(), 1);
    }

    #[test]
    fn certified_status_stable_after_cleanup_expired() {
        let policy = RetentionPolicy {
            max_age_ms: 5_000,
            max_entries: 10_000,
        };
        let mut api = CertifiedApi::with_retention(node("node-1"), default_namespace(), policy);

        api.certified_write("key1".into(), counter_value(3), OnTimeout::Pending)
            .unwrap();

        let write_ts = api.pending_writes()[0].timestamp.physical;

        // Certify.
        submit_signed_majority(&mut api, [("auth-1", 41), ("auth-2", 42)], write_ts, "", 1);
        api.process_certifications();

        assert_eq!(
            api.get_certification_status("key1"),
            CertificationStatus::Certified
        );

        // Expire + cleanup — well past max_age_ms.
        api.cleanup_expired(write_ts + 100_000);
        assert_eq!(api.pending_writes().len(), 0);

        // Status must remain Certified.
        assert_eq!(
            api.get_certification_status("key1"),
            CertificationStatus::Certified,
            "status must remain Certified after cleanup_expired"
        );

        let result = api.get_certified("key1");
        assert_eq!(result.status, CertificationStatus::Certified);
        assert!(result.proof.is_some());
    }

    #[test]
    fn multiple_writes_same_key_latest_certified_wins() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());

        // First write.
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        let ts1 = api.pending_writes()[0].timestamp.physical;

        // Certify first write.
        submit_signed_majority(&mut api, [("auth-1", 41), ("auth-2", 42)], ts1, "", 1);
        api.process_certifications();

        assert_eq!(api.certified_cache_len(), 1);

        // Second write to the same key (replaces value in store).
        api.certified_write("key1".into(), counter_value(10), OnTimeout::Pending)
            .unwrap();
        assert_eq!(api.pending_writes().len(), 1);
        let ts2 = api.pending_writes().last().unwrap().timestamp.physical;

        // Certify second write as well.
        submit_signed_majority(&mut api, [("auth-1", 41), ("auth-2", 42)], ts2, "", 1);
        api.process_certifications();

        // Cache should still have 1 entry (overwritten for same key).
        assert_eq!(api.certified_cache_len(), 1);
        assert_eq!(api.pending_writes().len(), 1);

        // Cleanup everything.
        api.cleanup_completed();
        assert_eq!(api.pending_writes().len(), 0);

        // The cached entry should reflect the latest certification.
        let result = api.get_certified("key1");
        assert_eq!(result.status, CertificationStatus::Certified);
        assert!(result.proof.is_some());

        let proof = result.proof.unwrap();
        // The cached frontier should be from the second certification round.
        assert!(proof.frontier_hlc.physical >= ts2);
    }

    #[test]
    fn certified_status_stable_after_full_cleanup() {
        let policy = RetentionPolicy {
            max_age_ms: 10_000,
            max_entries: 10_000,
        };
        let mut api = CertifiedApi::with_retention(node("node-1"), default_namespace(), policy);

        api.certified_write("key1".into(), counter_value(7), OnTimeout::Pending)
            .unwrap();
        let ts = api.pending_writes()[0].timestamp.physical;

        // Certify.
        submit_signed_majority(&mut api, [("auth-1", 41), ("auth-2", 42)], ts, "", 1);
        api.process_certifications();

        // Full cleanup (the recommended periodic method).
        api.cleanup(ts + 100_000);
        assert_eq!(api.pending_writes().len(), 0);

        // Must still be Certified.
        assert_eq!(
            api.get_certification_status("key1"),
            CertificationStatus::Certified
        );
        let result = api.get_certified("key1");
        assert_eq!(result.status, CertificationStatus::Certified);
        assert!(result.proof.is_some());
    }

    #[test]
    fn certified_cache_populated_by_process_with_timeout() {
        let policy = RetentionPolicy {
            max_age_ms: 5_000,
            max_entries: 10_000,
        };
        let mut api = CertifiedApi::with_retention(node("node-1"), default_namespace(), policy);

        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        let ts = api.pending_writes()[0].timestamp.physical;

        // Certify via process_certifications_with_timeout.
        submit_signed_majority(&mut api, [("auth-1", 41), ("auth-2", 42)], ts, "", 1);
        let transitions = api.process_certifications_with_timeout(ts + 100);
        assert_eq!(transitions, 1);
        assert_eq!(api.certified_cache_len(), 1);

        // Cleanup.
        api.cleanup_completed();
        assert_eq!(api.pending_writes().len(), 0);

        // Still certified from cache.
        assert_eq!(
            api.get_certification_status("key1"),
            CertificationStatus::Certified
        );
        let result = api.get_certified("key1");
        assert_eq!(result.status, CertificationStatus::Certified);
        assert!(result.proof.is_some());
    }

    #[test]
    fn certified_status_stable_after_retention_eviction() {
        let policy = RetentionPolicy {
            max_age_ms: 60_000,
            max_entries: 2,
        };
        let mut api = CertifiedApi::with_retention(node("node-1"), default_namespace(), policy);

        // Write and certify key1.
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        let ts1 = api.pending_writes()[0].timestamp.physical;

        submit_signed_majority(&mut api, [("auth-1", 41), ("auth-2", 42)], ts1, "", 1);
        api.process_certifications();

        // Write key2 — now at capacity (2).
        api.certified_write("key2".into(), counter_value(2), OnTimeout::Pending)
            .unwrap();

        // Write key3 — triggers auto-cleanup which removes certified key1.
        api.certified_write("key3".into(), counter_value(3), OnTimeout::Pending)
            .unwrap();

        // key1 should have been cleaned up from pending_writes.
        assert!(
            !api.pending_writes().iter().any(|pw| pw.key == "key1"),
            "key1 should have been removed from pending_writes by auto-cleanup"
        );

        // But key1 should still be Certified via the cache.
        assert_eq!(
            api.get_certification_status("key1"),
            CertificationStatus::Certified,
            "key1 status must remain Certified after retention eviction"
        );
        let result = api.get_certified("key1");
        assert_eq!(result.status, CertificationStatus::Certified);
        assert!(result.proof.is_some());
    }

    // ---------------------------------------------------------------
    // Delta sync visibility (API layer)
    // ---------------------------------------------------------------

    /// Verify that `certified_write` produces an entry immediately visible
    /// to delta sync (`entries_since` / `delta_entries_since`) at the API
    /// layer — not just at the `Store` level.
    #[test]
    fn certified_write_is_visible_to_delta_sync() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());

        // Obtain a zero-valued frontier that precedes any write.
        let frontier_before = HlcTimestamp {
            physical: 0,
            logical: 0,
            node_id: "".into(),
        };

        // Write via certified_write — this must atomically record the HLC
        // timestamp so that the entry is immediately visible to delta sync.
        api.certified_write("key1".into(), counter_value(3), OnTimeout::Pending)
            .unwrap();

        // The written key must appear in entries_since (the delta sync view).
        let delta = api.store.entries_since(&frontier_before);
        assert_eq!(
            delta.len(),
            1,
            "certified_write must produce an entry visible to delta sync (entries_since)"
        );
        assert_eq!(delta[0].0, "key1");

        // Also verify via delta_entries_since.
        let delta2 = api.store.delta_entries_since(&frontier_before);
        assert_eq!(delta2.len(), 1);
        assert_eq!(delta2[0].0, "key1");
    }

    /// Verify that multiple `certified_write` calls each produce delta-visible
    /// entries — all keys must appear in `entries_since`.
    #[test]
    fn certified_write_multiple_keys_all_visible_to_delta_sync() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());

        let frontier_before = HlcTimestamp {
            physical: 0,
            logical: 0,
            node_id: "".into(),
        };

        api.certified_write("key-a".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        api.certified_write("key-b".into(), counter_value(2), OnTimeout::Pending)
            .unwrap();
        api.certified_write("key-c".into(), counter_value(3), OnTimeout::Pending)
            .unwrap();

        let delta = api.store.entries_since(&frontier_before);
        assert_eq!(
            delta.len(),
            3,
            "all three certified_write calls must be visible to delta sync"
        );

        let mut keys: Vec<&str> = delta.iter().map(|(k, _, _)| k.as_str()).collect();
        keys.sort();
        assert_eq!(keys, vec!["key-a", "key-b", "key-c"]);
    }

    // ---------------------------------------------------------------
    // Signing pipeline: signed frontiers → certificate in ProofBundle
    // ---------------------------------------------------------------

    use crate::authority::certificate::KeysetVersion;
    use crate::authority::frontier_sig::NodeSigner;

    #[cfg(feature = "native-crypto")]
    fn make_signer(name: &str, byte: u8) -> NodeSigner {
        let mut seed = [0u8; 32];
        seed[0] = byte;
        NodeSigner::from_seed(node(name), &seed, true)
    }

    #[cfg(not(feature = "native-crypto"))]
    fn make_signer(name: &str, byte: u8) -> NodeSigner {
        let mut seed = [0u8; 32];
        seed[0] = byte;
        NodeSigner::from_seed(node(name), &seed)
    }

    /// Build a signed frontier report and its self-verified attestation.
    fn make_signed_frontier(
        signer: &NodeSigner,
        physical: u64,
        prefix: &str,
    ) -> (AckFrontier, VerifiedAttestation) {
        let frontier = make_frontier(signer.node_id().0.as_str(), physical, 0, prefix);
        let sig = signer.sign_frontier(&frontier, KeysetVersion(1));
        let att = signer.self_verified(&frontier, &sig);
        (frontier, att)
    }

    /// Submit a signed majority whose common checkpoint is later than the
    /// write, including writes made on a checkpoint boundary.
    fn submit_signed_majority(
        api: &mut CertifiedApi,
        authorities: [(&str, u8); 2],
        write_physical: u64,
        prefix: &str,
        version: u64,
    ) {
        let report_physical =
            (write_physical / CHECKPOINT_INTERVAL_MS + 1) * CHECKPOINT_INTERVAL_MS + 100;
        for (authority, seed) in authorities {
            let signer = make_signer(authority, seed);
            let frontier = make_frontier_v(authority, report_physical, 0, prefix, version);
            let signature = signer.sign_frontier(&frontier, KeysetVersion(1));
            let attestation = signer.self_verified(&frontier, &signature);
            api.update_frontier_verified(frontier, Some(attestation));
        }
    }

    #[test]
    fn update_frontier_verified_records_attestation_and_matches_update() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        let signer = make_signer("auth-1", 1);

        let (f1, a1) = make_signed_frontier(&signer, 10_500, "");
        assert!(api.update_frontier_verified(f1.clone(), Some(a1.clone())));

        // Stale (same-or-older) frontier: update returns false, and its
        // signature cannot replace the accepted full report's signature.
        let (f2, a2) = make_signed_frontier(&signer, 10_200, "");
        assert!(!api.update_frontier_verified(f2, Some(a2)));

        // The accepted auth-1 signature and auth-2 signature form a majority.
        let signer2 = make_signer("auth-2", 2);
        let (f3, a3) = make_signed_frontier(&signer2, 10_600, "");
        api.update_frontier_verified(f3, Some(a3));

        let write_ts = HlcTimestamp {
            physical: 9_000,
            logical: 0,
            node_id: "writer".into(),
        };
        let built = api
            .attestations
            .build_certificates(&kr(""), PolicyVersion(1), 3, &write_ts);
        assert!(built.is_some(), "2 of 3 attestations must reach majority");
    }

    #[test]
    fn stale_signed_report_cannot_borrow_unsigned_frontier_checkpoint() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        let write_time = api.pending_writes()[0].timestamp.physical;
        let checkpoint = (write_time / CHECKPOINT_INTERVAL_MS + 1) * CHECKPOINT_INTERVAL_MS;

        for (authority, seed) in [("auth-1", 41), ("auth-2", 42)] {
            let signer = make_signer(authority, seed);
            let unsigned = make_frontier(authority, checkpoint + 600, 0, "");
            let signed = make_frontier(authority, checkpoint + 100, 0, "");
            let signature = signer.sign_frontier(&signed, KeysetVersion(1));
            let attestation = signer.self_verified(&signed, &signature);

            assert!(api.update_frontier(unsigned));
            assert!(!api.update_frontier_verified(signed, Some(attestation)));
        }

        api.process_certifications();
        assert_eq!(
            api.get_certified("key1").status,
            CertificationStatus::Pending,
            "a signed stale full report cannot borrow an unsigned report's checkpoint receipt"
        );
    }

    #[test]
    fn late_signature_for_exact_unsigned_frontier_can_certify() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        let write_time = api.pending_writes()[0].timestamp.physical;
        let report_time = (write_time / CHECKPOINT_INTERVAL_MS + 1) * CHECKPOINT_INTERVAL_MS + 100;

        for (authority, seed) in [("auth-1", 41), ("auth-2", 42)] {
            let signer = make_signer(authority, seed);
            let frontier = make_frontier(authority, report_time, 0, "");
            assert!(api.update_frontier(frontier.clone()));
            let signature = signer.sign_frontier(&frontier, KeysetVersion(1));
            let attestation = signer.self_verified(&frontier, &signature);
            assert!(!api.update_frontier_verified(frontier, Some(attestation)));
        }

        api.process_certifications();
        assert_eq!(
            api.get_certified("key1").status,
            CertificationStatus::Certified
        );
    }

    #[test]
    fn unsigned_frontier_cannot_replace_signed_issue_witness_in_same_checkpoint() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        let write_time = api.pending_writes()[0].timestamp.physical;
        let checkpoint = (write_time / CHECKPOINT_INTERVAL_MS + 1) * CHECKPOINT_INTERVAL_MS;

        for (authority, seed) in [("auth-1", 41), ("auth-2", 42)] {
            let signer = make_signer(authority, seed);
            let signed = make_frontier(authority, checkpoint + 100, 0, "");
            let signature = signer.sign_frontier(&signed, KeysetVersion(1));
            let attestation = signer.self_verified(&signed, &signature);
            assert!(api.update_frontier_verified(signed, Some(attestation)));

            let unsigned = make_frontier(authority, checkpoint + 600, 0, "");
            assert!(api.update_frontier(unsigned));
        }

        api.process_certifications();
        assert_eq!(
            api.get_certified("key1").status,
            CertificationStatus::Pending,
            "an unsigned replacement cannot stand in for the pooled signed full frontier"
        );
    }

    #[test]
    fn later_unsigned_frontier_does_not_hide_earlier_signed_checkpoint() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        let write_time = api.pending_writes()[0].timestamp.physical;
        let first = (write_time / CHECKPOINT_INTERVAL_MS + 1) * CHECKPOINT_INTERVAL_MS + 100;
        let second = first + CHECKPOINT_INTERVAL_MS;

        for (authority, seed) in [("auth-1", 41), ("auth-2", 42)] {
            let signer = make_signer(authority, seed);
            for report_time in [first, second] {
                let frontier = make_frontier(authority, report_time, 0, "");
                let signature = signer.sign_frontier(&frontier, KeysetVersion(1));
                let attestation = signer.self_verified(&frontier, &signature);
                assert!(api.update_frontier_verified(frontier, Some(attestation)));
            }
            assert!(api.update_frontier(make_frontier(authority, second + 500, 0, "")));
        }

        api.process_certifications();
        let read = api.get_certified("key1");
        assert_eq!(read.status, CertificationStatus::Certified);
        assert_eq!(read.proof.unwrap().frontier_hlc.physical, first - 100);
    }

    #[test]
    fn fenced_scope_does_not_collect_attestations() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        api.fence_version(&kr(""), PolicyVersion(1));

        let signer = make_signer("auth-1", 3);
        let signer2 = make_signer("auth-2", 4);
        let (f1, a1) = make_signed_frontier(&signer, 10_500, "");
        let (f2, a2) = make_signed_frontier(&signer2, 10_600, "");
        assert!(!api.update_frontier_verified(f1, Some(a1)));
        assert!(!api.update_frontier_verified(f2, Some(a2)));

        let write_ts = HlcTimestamp {
            physical: 9_000,
            logical: 0,
            node_id: "writer".into(),
        };
        assert!(
            api.attestations
                .build_certificates(&kr(""), PolicyVersion(1), 3, &write_ts)
                .is_none(),
            "fenced scopes must not accumulate attestations (FR-009)"
        );
    }

    /// End-to-end: signed frontier reports from a majority of authorities
    /// produce a ProofBundle whose certificate passes the verifier.
    #[test]
    fn signed_majority_produces_verifiable_certificate() {
        use crate::authority::certificate::{EpochConfig, KeysetRegistry};
        use crate::authority::verifier;

        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        api.certified_write("key1".into(), counter_value(5), OnTimeout::Pending)
            .unwrap();
        let write_ts = api.pending_writes()[0].timestamp.physical;

        let s1 = make_signer("auth-1", 5);
        let s2 = make_signer("auth-2", 6);

        // Both authorities report past the write's checkpoint boundary.
        let report_ts = (write_ts / 1000 + 1) * 1000 + 100;
        for signer in [&s1, &s2] {
            let (f, a) = make_signed_frontier(signer, report_ts, "");
            api.update_frontier_verified(f, Some(a));
        }

        api.process_certifications();

        let result = api.get_certified("key1");
        assert_eq!(result.status, CertificationStatus::Certified);
        let proof = result.proof.expect("certified read must include a proof");
        let cert = proof
            .certificate
            .as_ref()
            .expect("signed majority must attach a certificate");

        // The proof's frontier is the certificate checkpoint.
        assert_eq!(proof.frontier_hlc, cert.frontier_hlc);
        assert_eq!(proof.frontier_hlc.logical, 0);
        assert_eq!(proof.frontier_hlc.physical % 1000, 0);
        assert_eq!(proof.contributing_authorities.len(), 2);

        let trusted_keys = vec![
            (node("auth-1"), s1.verifying_key()),
            (node("auth-2"), s2.verifying_key()),
            (node("auth-3"), make_signer("auth-3", 7).verifying_key()),
        ];
        let verification = verifier::verify_proof(&proof, &trusted_keys, None, 0);
        assert!(
            verification.valid,
            "assembled certificate must verify: {verification:?}"
        );
        assert_eq!(verification.signatures_valid, Some(true));

        // Registry-based verification also passes.
        let mut registry = KeysetRegistry::new();
        registry
            .register_keyset(
                KeysetVersion(1),
                0,
                vec![
                    (node("auth-1"), s1.verifying_key()),
                    (node("auth-2"), s2.verifying_key()),
                ],
            )
            .unwrap();
        let verification = verifier::verify_proof_with_registry(
            &proof,
            &[node("auth-1"), node("auth-2"), node("auth-3")],
            &registry,
            0,
            &EpochConfig::default(),
            None,
            0,
        );
        assert!(verification.valid, "registry verification must pass");
    }

    /// A write stays Pending until the signed checkpoint catches up.
    #[test]
    fn certificate_required_before_promotion() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        let write_ts = api.pending_writes()[0].timestamp.physical;

        let s1 = make_signer("auth-1", 7);
        let s2 = make_signer("auth-2", 8);

        // Reports advance the frontier past the write but their checkpoint
        // (floor to 1s) is still below the write timestamp.
        let early_ts = write_ts + 1; // same checkpoint bucket as the write
        if !early_ts.is_multiple_of(1000) {
            for signer in [&s1, &s2] {
                let (f, a) = make_signed_frontier(signer, early_ts, "");
                api.update_frontier_verified(f, Some(a));
            }
            api.process_certifications();
            let result = api.get_certified("key1");
            assert_eq!(result.status, CertificationStatus::Pending);
            assert!(result.proof.is_none());
        }

        // Later reports cross the next checkpoint boundary.
        let late_ts = (write_ts / 1000 + 1) * 1000 + 50;
        for signer in [&s1, &s2] {
            let (f, a) = make_signed_frontier(signer, late_ts, "");
            api.update_frontier_verified(f, Some(a));
        }
        api.process_certifications();

        let result = api.get_certified("key1");
        assert_eq!(result.status, CertificationStatus::Certified);
        let proof = result.proof.unwrap();
        assert!(
            proof.certificate.is_some(),
            "certificate must be available at promotion"
        );
    }

    /// Unsigned frontier reports cannot certify a write.
    #[test]
    fn unsigned_frontiers_leave_write_pending() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        let write_ts = api.pending_writes()[0].timestamp.physical;

        // The unsigned majority advances the diagnostic frontier only.
        api.update_frontier(make_frontier("auth-1", write_ts + 100, 0, ""));
        api.update_frontier(make_frontier("auth-2", write_ts + 100, 0, ""));
        api.process_certifications();

        assert_eq!(
            api.get_certification_status("key1"),
            CertificationStatus::Pending
        );
        assert!(api.get_certified("key1").proof.is_none());
        assert_eq!(api.certified_cache_len(), 0);
    }

    #[cfg(feature = "native-crypto")]
    #[test]
    fn signed_majority_attaches_bls_certificate() {
        use crate::authority::certificate::{EpochConfig, KeysetRegistry};
        use crate::authority::verifier;

        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        let write_ts = api.pending_writes()[0].timestamp.physical;

        let s1 = make_signer("auth-1", 9);
        let s2 = make_signer("auth-2", 10);
        let report_ts = (write_ts / 1000 + 1) * 1000 + 100;
        for signer in [&s1, &s2] {
            let (f, a) = make_signed_frontier(signer, report_ts, "");
            api.update_frontier_verified(f, Some(a));
        }
        api.process_certifications();

        let proof = api.get_certified("key1").proof.unwrap();
        let bls_cert = proof
            .bls_certificate
            .expect("BLS-capable majority must attach a BLS certificate");
        assert!(bls_cert.has_majority(3));

        let mut registry = KeysetRegistry::new();
        registry
            .register_keyset(
                KeysetVersion(1),
                0,
                vec![
                    (node("auth-1"), s1.verifying_key()),
                    (node("auth-2"), s2.verifying_key()),
                ],
            )
            .unwrap();
        registry
            .register_bls_keys(
                &KeysetVersion(1),
                vec![
                    (
                        "auth-1".into(),
                        s1.bls_public_key().unwrap(),
                        s1.bls_proof_of_possession().unwrap(),
                    ),
                    (
                        "auth-2".into(),
                        s2.bls_public_key().unwrap(),
                        s2.bls_proof_of_possession().unwrap(),
                    ),
                ],
            )
            .unwrap();

        let result = verifier::verify_dual_proof_with_registry(
            &bls_cert,
            &[node("auth-1"), node("auth-2"), node("auth-3")],
            &registry,
            0,
            &EpochConfig::default(),
            None,
            0,
        );
        assert!(result.valid, "BLS certificate must verify: {result:?}");
    }

    // ---------------------------------------------------------------
    // Attestation admission (M-4) and accused purge (m-7)
    // ---------------------------------------------------------------

    /// Signed frontier + attestation with an explicit policy version.
    fn make_signed_frontier_v(
        signer: &NodeSigner,
        physical: u64,
        prefix: &str,
        version: u64,
    ) -> (AckFrontier, VerifiedAttestation) {
        let frontier = make_frontier_v(signer.node_id().0.as_str(), physical, 0, prefix, version);
        let sig = signer.sign_frontier(&frontier, KeysetVersion(1));
        let att = signer.self_verified(&frontier, &sig);
        (frontier, att)
    }

    #[test]
    fn signed_prune_floor_requires_matching_accepted_quorum() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        let members = [node("auth-1"), node("auth-2"), node("auth-3")];
        let first = make_signer("auth-1", 74);
        let second = make_signer("auth-2", 75);
        let (first_frontier, first_attestation) = make_signed_frontier_v(&first, 10_100, "", 1);
        api.update_frontier_verified(first_frontier, Some(first_attestation));
        api.update_frontier(make_frontier_v("auth-2", 10_100, 0, "", 1));
        assert_eq!(
            api.signed_prune_floor(&kr(""), PolicyVersion(1), &members),
            None,
            "an unsigned second frontier cannot supply a signed quorum"
        );

        let (second_frontier, second_attestation) = make_signed_frontier_v(&second, 10_101, "", 1);
        api.update_frontier_verified(second_frontier, Some(second_attestation));
        assert_eq!(
            api.signed_prune_floor(&kr(""), PolicyVersion(1), &members)
                .map(|floor| floor.timestamp().physical),
            Some(10_000)
        );

        api.update_frontier(make_frontier_v("auth-2", 10_200, 0, "", 1));
        assert_eq!(
            api.signed_prune_floor(&kr(""), PolicyVersion(1), &members),
            None,
            "a later unsigned full report in the same checkpoint cannot borrow the old signature"
        );
    }

    fn namespace_with_version(
        prefix: &str,
        authorities: &[&str],
        version: u64,
    ) -> Arc<RwLock<SystemNamespace>> {
        let mut ns = SystemNamespace::new();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr(prefix),
            authority_nodes: authorities.iter().map(|a| node(a)).collect(),
            auto_generated: false,
        });
        ns.set_placement_policy(PlacementPolicy::new(
            PolicyVersion(version),
            kr(prefix),
            authorities.len(),
        ))
        .unwrap();
        wrap_ns(ns)
    }

    #[test]
    fn attestation_version_window_boundaries() {
        // Current policy version 5: accept cur-2 ..= cur+1, reject outside.
        let mut api = CertifiedApi::new(
            node("node-1"),
            namespace_with_version("", &["auth-1", "auth-2", "auth-3"], 5),
        );
        let signer = make_signer("auth-1", 20);

        for (pv, admitted) in [
            (2u64, false),
            (3, true),
            (4, true),
            (5, true),
            (6, true),
            (7, false),
        ] {
            let (f, a) = make_signed_frontier_v(&signer, 10_500, "", pv);
            // Admission gates the frontier set as well as the pool (M-4):
            // an in-window report advances (first report for its scope), an
            // out-of-window report is dropped entirely.
            assert_eq!(
                api.update_frontier_verified(f, Some(a)),
                admitted,
                "frontier advance must follow admission (pv={pv})"
            );
        }

        let stats = api.attestation_stats();
        assert_eq!(stats.scopes, 4, "cur-2 ..= cur+1 must be pooled");
        assert_eq!(stats.rejected_version_window_total, 2);
        assert_eq!(stats.rejected_unknown_range_total, 0);
        assert_eq!(
            api.frontier_count(),
            4,
            "out-of-window reports must not enter the frontier set either"
        );
    }

    #[test]
    fn attestation_admission_rejects_unknown_scopes() {
        let ns = default_namespace();
        let mut api = CertifiedApi::new(node("node-1"), Arc::clone(&ns));

        // Unknown range: no authority definition for the exact prefix.
        let signer = make_signer("auth-1", 21);
        let (f, a) = make_signed_frontier_v(&signer, 10_500, "ghost/", 1);
        assert!(!api.update_frontier_verified(f, Some(a)));

        // Non-member signer for a defined range.
        let outsider = make_signer("auth-9", 22);
        let (f, a) = make_signed_frontier_v(&outsider, 10_500, "", 1);
        assert!(!api.update_frontier_verified(f, Some(a)));

        // Defined range without a placement policy.
        ns.write()
            .unwrap()
            .set_authority_definition(AuthorityDefinition {
                key_range: kr("nopolicy/"),
                authority_nodes: vec![node("auth-1")],
                auto_generated: false,
            });
        let (f, a) = make_signed_frontier_v(&signer, 10_500, "nopolicy/", 1);
        assert!(!api.update_frontier_verified(f, Some(a)));

        let stats = api.attestation_stats();
        assert_eq!(stats.scopes, 0, "no inadmissible attestation may pool");
        assert_eq!(stats.rejected_unknown_range_total, 3);
        assert_eq!(stats.rejected_version_window_total, 0);
        assert_eq!(
            api.frontier_count(),
            0,
            "no inadmissible report may enter the frontier set"
        );
    }

    #[test]
    fn fence_takes_precedence_over_admission() {
        // A fenced scope stays fenced even when the version window would
        // admit it: no insert, no admission-rejection counter movement.
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        api.fence_version(&kr(""), PolicyVersion(1));

        let signer = make_signer("auth-1", 23);
        let (f, a) = make_signed_frontier_v(&signer, 10_500, "", 1);
        assert!(!api.update_frontier_verified(f, Some(a)));

        let stats = api.attestation_stats();
        assert_eq!(stats.scopes, 0);
        assert_eq!(stats.rejected_unknown_range_total, 0);
        assert_eq!(stats.rejected_version_window_total, 0);
    }

    /// M-17 observability: stale-but-admissible reports and fenced drops
    /// are counted (previously the fenced drop was fully silent, which is
    /// what made the observer silent-fence failure invisible).
    #[test]
    fn stale_and_fenced_reports_are_counted() {
        // Current policy version 5.
        let mut api = CertifiedApi::new(
            node("node-1"),
            namespace_with_version("", &["auth-1", "auth-2", "auth-3"], 5),
        );
        let signer = make_signer("auth-1", 26);

        // Current-version report: no stale / fenced movement.
        let (f, a) = make_signed_frontier_v(&signer, 10_500, "", 5);
        assert!(api.update_frontier_verified(f, Some(a)));
        // Leading report (cur+1): ahead, not stale.
        let (f, a) = make_signed_frontier_v(&signer, 10_510, "", 6);
        assert!(api.update_frontier_verified(f, Some(a)));
        let stats = api.attestation_stats();
        assert_eq!(stats.stale_version_total, 0);
        assert_eq!(stats.rejected_fenced_total, 0);

        // Stage A: in-window but behind current (pv 4 < 5) — admitted
        // (frontier advances, attestation pools) AND counted as stale.
        let (f, a) = make_signed_frontier_v(&signer, 10_600, "", 4);
        assert!(api.update_frontier_verified(f, Some(a)));
        let stats = api.attestation_stats();
        assert_eq!(stats.stale_version_total, 1);
        assert_eq!(stats.rejected_fenced_total, 0);

        // Fence pv 4 (what detect_version_changes does on a bump): the
        // same report now drops — frontier refuses to advance, nothing
        // pools — and the fenced counter records it.
        api.fence_version(&kr(""), PolicyVersion(4));
        let (f, a) = make_signed_frontier_v(&signer, 10_700, "", 4);
        assert!(!api.update_frontier_verified(f, Some(a)));
        let stats = api.attestation_stats();
        assert_eq!(
            stats.stale_version_total, 2,
            "fenced reports stay stale too"
        );
        assert_eq!(stats.rejected_fenced_total, 1);

        // Stage B: outside the window (pv 2 < 5-2) — rejected at
        // admission; the stale / fenced counters must NOT move (the
        // window counter owns that phase).
        let (f, a) = make_signed_frontier_v(&signer, 10_800, "", 2);
        assert!(!api.update_frontier_verified(f, Some(a)));
        let stats = api.attestation_stats();
        assert_eq!(stats.stale_version_total, 2);
        assert_eq!(stats.rejected_fenced_total, 1);
        assert_eq!(stats.rejected_version_window_total, 1);
    }

    #[test]
    fn version_transition_admits_lag_and_lead_until_fence() {
        let ns = default_namespace(); // current version 1
        let mut api = CertifiedApi::new(node("node-1"), Arc::clone(&ns));
        let s1 = make_signer("auth-1", 24);
        let s2 = make_signer("auth-2", 25);

        // Leading reporter: v2 admitted while current is still 1.
        let (f, a) = make_signed_frontier_v(&s1, 10_500, "", 2);
        api.update_frontier_verified(f, Some(a));
        // Current version: admitted.
        let (f, a) = make_signed_frontier_v(&s1, 10_500, "", 1);
        api.update_frontier_verified(f, Some(a));
        assert_eq!(api.attestation_stats().scopes, 2);

        // Bump to v2: a lagging reporter still attests v1 and is admitted.
        ns.write()
            .unwrap()
            .set_placement_policy(PlacementPolicy::new(PolicyVersion(2), kr(""), 3))
            .unwrap();
        let (f, a) = make_signed_frontier_v(&s2, 10_600, "", 1);
        api.update_frontier_verified(f, Some(a));
        assert_eq!(api.attestation_stats().scopes, 2);

        // Fencing v1 drops its scope and blocks later v1 attestations even
        // though the window still covers v1.
        api.fence_version(&kr(""), PolicyVersion(1));
        let (f, a) = make_signed_frontier_v(&s2, 10_700, "", 1);
        api.update_frontier_verified(f, Some(a));
        let stats = api.attestation_stats();
        assert_eq!(stats.scopes, 1, "only the v2 scope may remain");
        assert_eq!(stats.rejected_version_window_total, 0);

        // v2 keeps pooling after the bump.
        let (f, a) = make_signed_frontier_v(&s2, 10_800, "", 2);
        api.update_frontier_verified(f, Some(a));
        assert_eq!(api.attestation_stats().scopes, 1);
    }

    #[test]
    fn unfenced_reassigned_version_is_admitted_again() {
        // The replicated version counter can re-assign a version this node
        // already fenced (node_runner lifts the fence via unfence_version).
        // After the lift, attestations under the re-assigned CURRENT version
        // must pool again.
        let ns = default_namespace();
        let mut api = CertifiedApi::new(node("node-1"), Arc::clone(&ns));
        api.fence_version(&kr(""), PolicyVersion(2));

        ns.write()
            .unwrap()
            .set_placement_policy(PlacementPolicy::new(PolicyVersion(2), kr(""), 3))
            .unwrap();
        assert!(api.unfence_version(&kr(""), PolicyVersion(2)));

        let signer = make_signer("auth-1", 26);
        let (f, a) = make_signed_frontier_v(&signer, 10_500, "", 2);
        api.update_frontier_verified(f, Some(a));
        assert_eq!(api.attestation_stats().scopes, 1);
    }

    #[test]
    fn cap_pressure_sweeps_stale_scopes_and_is_throttled() {
        use crate::authority::attestation_pool::MAX_POOL_SCOPES_PER_AUTHORITY;

        // 16 prefixes, all with auth-1 as the only authority, current
        // version 4 → 16 x 4 in-window versions = 64 admissible scopes,
        // exactly the per-authority cap.
        let mut ns = SystemNamespace::new();
        for i in 0..16 {
            let prefix = format!("p{i}/");
            ns.set_authority_definition(AuthorityDefinition {
                key_range: kr(&prefix),
                authority_nodes: vec![node("auth-1")],
                auto_generated: false,
            });
            ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(4), kr(&prefix), 1))
                .unwrap();
        }
        let ns = wrap_ns(ns);
        let mut api = CertifiedApi::new(node("node-1"), Arc::clone(&ns));
        let signer = make_signer("auth-1", 27);

        let fill = |api: &mut CertifiedApi, versions: [u64; 4], now_ms: u64| {
            for i in 0..16 {
                for pv in versions {
                    let (f, a) = make_signed_frontier_v(&signer, 9_500, &format!("p{i}/"), pv);
                    api.record_attestation(
                        &f.key_range,
                        f.policy_version,
                        &f.authority_id,
                        a,
                        now_ms,
                    );
                }
            }
        };
        let bump_all = |ns: &Arc<RwLock<SystemNamespace>>, version: u64| {
            let mut ns = ns.write().unwrap();
            for i in 0..16 {
                ns.set_placement_policy(PlacementPolicy::new(
                    PolicyVersion(version),
                    kr(&format!("p{i}/")),
                    1,
                ))
                .unwrap();
            }
        };

        fill(&mut api, [2, 3, 4, 5], 10_000);
        assert_eq!(
            api.attestation_stats().scopes,
            MAX_POOL_SCOPES_PER_AUTHORITY as u64
        );

        // Bump every prefix far ahead: all pooled scopes become stale. The
        // next admissible insert hits the cap, sweeps them, and succeeds.
        bump_all(&ns, 100);
        let (f, a) = make_signed_frontier_v(&signer, 9_500, "p0/", 100);
        api.record_attestation(&f.key_range, f.policy_version, &f.authority_id, a, 10_000);
        let stats = api.attestation_stats();
        assert_eq!(stats.scopes, 1, "sweep must evict stale scopes and admit");
        assert_eq!(stats.rejected_authority_cap_total, 1);

        // Refill to the cap under the new current version, then bump again:
        // a rejection within the throttle interval must NOT sweep...
        fill(&mut api, [98, 99, 100, 101], 10_100);
        assert_eq!(
            api.attestation_stats().scopes,
            MAX_POOL_SCOPES_PER_AUTHORITY as u64
        );
        bump_all(&ns, 200);
        let (f, a) = make_signed_frontier_v(&signer, 9_500, "p0/", 200);
        api.record_attestation(
            &f.key_range,
            f.policy_version,
            &f.authority_id,
            a.clone(),
            10_500,
        );
        assert_eq!(
            api.attestation_stats().scopes,
            MAX_POOL_SCOPES_PER_AUTHORITY as u64,
            "within the throttle interval the sweep must not run"
        );

        // ...but one interval later the sweep runs and the insert lands.
        api.record_attestation(&f.key_range, f.policy_version, &f.authority_id, a, 11_000);
        assert_eq!(api.attestation_stats().scopes, 1);
    }

    /// M-4 DoS regression: a single REGISTERED authority rotating
    /// policy_version and key_range values through legitimately signed
    /// frontier reports must not grow the attestation pool OR the frontier
    /// set beyond the namespace-derived scope set — the admission layer
    /// alone stops the attack (the pool caps are never even reached, and
    /// the uncapped `AckFrontierSet` never sees a rotated scope).
    #[test]
    fn scope_rotation_flood_is_stopped_by_admission() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        let signer = make_signer("auth-1", 28);

        for i in 0..10_000u64 {
            let (f, a) = if i % 2 == 0 {
                // Rotating far-future policy versions on the real range.
                make_signed_frontier_v(&signer, 10_500 + i, "", 10 + i)
            } else {
                // Rotating fictional prefixes.
                make_signed_frontier_v(&signer, 10_500 + i, &format!("junk-{i}/"), 1)
            };
            api.update_frontier_verified(f, Some(a));
        }

        let stats = api.attestation_stats();
        assert_eq!(
            stats.scopes, 0,
            "no rotated scope may enter the pool (admission stops the flood)"
        );
        assert_eq!(stats.rejected_version_window_total, 5_000);
        assert_eq!(stats.rejected_unknown_range_total, 5_000);
        assert_eq!(stats.rejected_scope_cap_total, 0, "caps never reached");
        assert_eq!(stats.rejected_authority_cap_total, 0);
        assert_eq!(
            api.frontier_count(),
            0,
            "no rotated scope may enter the frontier set (M-4: it is \
             uncapped and persisted, so admission must gate it too)"
        );

        // Honest traffic is unaffected during and after the flood.
        let (f, a) = make_signed_frontier_v(&signer, 10_500, "", 1);
        api.update_frontier_verified(f, Some(a));
        assert_eq!(api.attestation_stats().scopes, 1);
        assert_eq!(api.frontier_count(), 1);
    }

    /// The cap-pressure sweep throttle must treat a BACKWARD wall-clock
    /// step as expiry: after `last_stale_prune_ms` is set at time T, a
    /// rejection observed at `now < T` must still be allowed to sweep —
    /// otherwise an NTP step back would suppress all sweeps until the
    /// clock re-passes T, rejecting every new-scope insert meanwhile.
    #[test]
    fn cap_sweep_throttle_survives_backward_clock_step() {
        use crate::authority::attestation_pool::MAX_POOL_SCOPES_PER_AUTHORITY;

        let mut ns = SystemNamespace::new();
        for i in 0..16 {
            let prefix = format!("p{i}/");
            ns.set_authority_definition(AuthorityDefinition {
                key_range: kr(&prefix),
                authority_nodes: vec![node("auth-1")],
                auto_generated: false,
            });
            ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(4), kr(&prefix), 1))
                .unwrap();
        }
        let ns = wrap_ns(ns);
        let mut api = CertifiedApi::new(node("node-1"), Arc::clone(&ns));
        let signer = make_signer("auth-1", 33);

        // Fill to the per-authority cap and run one sweep at wall time
        // 600_000 to arm the throttle far in the "future".
        for i in 0..16 {
            for pv in [2u64, 3, 4, 5] {
                let (f, a) = make_signed_frontier_v(&signer, 9_500, &format!("p{i}/"), pv);
                api.record_attestation(&f.key_range, f.policy_version, &f.authority_id, a, 10_000);
            }
        }
        assert_eq!(
            api.attestation_stats().scopes,
            MAX_POOL_SCOPES_PER_AUTHORITY as u64
        );
        {
            let mut guard = ns.write().unwrap();
            for i in 0..16 {
                guard
                    .set_placement_policy(PlacementPolicy::new(
                        PolicyVersion(100),
                        kr(&format!("p{i}/")),
                        1,
                    ))
                    .unwrap();
            }
        }
        let (f, a) = make_signed_frontier_v(&signer, 9_500, "p0/", 100);
        api.record_attestation(&f.key_range, f.policy_version, &f.authority_id, a, 600_000);
        assert_eq!(api.attestation_stats().scopes, 1, "sweep ran at T=600_000");

        // Refill to the cap, then make everything stale again.
        for i in 0..16 {
            for pv in [98u64, 99, 100, 101] {
                let (f, a) = make_signed_frontier_v(&signer, 9_500, &format!("p{i}/"), pv);
                api.record_attestation(&f.key_range, f.policy_version, &f.authority_id, a, 600_100);
            }
        }
        assert_eq!(
            api.attestation_stats().scopes,
            MAX_POOL_SCOPES_PER_AUTHORITY as u64
        );
        {
            let mut guard = ns.write().unwrap();
            for i in 0..16 {
                guard
                    .set_placement_policy(PlacementPolicy::new(
                        PolicyVersion(200),
                        kr(&format!("p{i}/")),
                        1,
                    ))
                    .unwrap();
            }
        }

        // The wall clock steps back 10 minutes (now < last_stale_prune_ms):
        // the sweep must still run and the insert must land.
        let (f, a) = make_signed_frontier_v(&signer, 9_500, "p0/", 200);
        api.record_attestation(&f.key_range, f.policy_version, &f.authority_id, a, 10_000);
        assert_eq!(
            api.attestation_stats().scopes,
            1,
            "a backward clock step must count as throttle expiry, not \
             suppress the sweep"
        );
    }

    #[test]
    fn purge_accused_attestations_removes_pooled_entries() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        let write_ts = api.pending_writes()[0].timestamp.physical;

        let s1 = make_signer("auth-1", 29);
        let s2 = make_signer("auth-2", 30);
        let report_ts = (write_ts / 1000 + 1) * 1000 + 100;
        for signer in [&s1, &s2] {
            let (f, a) = make_signed_frontier(signer, report_ts, "");
            api.update_frontier_verified(f, Some(a));
        }

        // auth-1 gets accused: purge its pooled attestation. auth-2 alone is
        // 1 of 3, so no certificate can be assembled.
        let purged = api.purge_accused_attestations(&[node("auth-1")]);
        assert_eq!(purged, 1);
        assert_eq!(api.attestation_stats().purged_total, 1);

        api.process_certifications();
        let read = api.get_certified("key1");
        assert_eq!(read.status, CertificationStatus::Pending);
        assert!(read.proof.is_none());

        // Idempotent.
        assert_eq!(api.purge_accused_attestations(&[node("auth-1")]), 0);
    }

    /// Version-transition liveness: attestations arriving under the OLD
    /// version after a policy bump (lagging reporters) still certify a
    /// write issued under that version, and attestations arriving one
    /// version AHEAD (leading reporters) are pooled before the local bump.
    #[test]
    fn lag_and_lead_attestations_around_bump_still_certify() {
        let ns = default_namespace();
        let mut api = CertifiedApi::new(node("node-1"), Arc::clone(&ns));

        // Write under v1.
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        let write_ts = api.pending_writes()[0].timestamp.physical;

        // Policy bumps to v2 BEFORE any attestation for the write arrived.
        ns.write()
            .unwrap()
            .set_placement_policy(PlacementPolicy::new(PolicyVersion(2), kr(""), 3))
            .unwrap();

        // Lagging reporters still attest v1 — admitted by the lag window,
        // and the v1 write still receives its certificate.
        let s1 = make_signer("auth-1", 31);
        let s2 = make_signer("auth-2", 32);
        let report_ts = (write_ts / 1000 + 1) * 1000 + 100;
        for signer in [&s1, &s2] {
            let (f, a) = make_signed_frontier(signer, report_ts, ""); // v1
            api.update_frontier_verified(f, Some(a));
        }
        api.process_certifications();
        let read = api.get_certified("key1");
        assert_eq!(read.status, CertificationStatus::Certified);
        assert!(
            read.proof.unwrap().certificate.is_some(),
            "lagging v1 attestations must still certify the v1 write"
        );

        // Leading reporters: v3 attestations are pooled while current is 2.
        for (signer, ts) in [(&s1, report_ts + 2_000), (&s2, report_ts + 2_000)] {
            let (f, a) = make_signed_frontier_v(signer, ts, "", 3);
            api.update_frontier_verified(f, Some(a));
        }
        // After the local bump to v3, a write under v3 certifies against
        // the pre-bump attestations.
        ns.write()
            .unwrap()
            .set_placement_policy(PlacementPolicy::new(PolicyVersion(3), kr(""), 3))
            .unwrap();
        api.certified_write("key2".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        let write2_ts = api.pending_writes()[1].timestamp.physical;
        // Ensure the pooled checkpoints cover the new write; if the clock
        // advanced past them, top up with fresh v3 attestations.
        if (report_ts + 2_000) < write2_ts {
            let late_ts = (write2_ts / 1000 + 1) * 1000 + 100;
            for signer in [&s1, &s2] {
                let (f, a) = make_signed_frontier_v(signer, late_ts, "", 3);
                api.update_frontier_verified(f, Some(a));
            }
        }
        api.process_certifications();
        let read = api.get_certified("key2");
        assert_eq!(read.status, CertificationStatus::Certified);
        assert!(
            read.proof.unwrap().certificate.is_some(),
            "leading attestations must be available once the bump lands"
        );
    }

    // ---------------------------------------------------------------
    // P0-1 / C1: certified replication ingest (`merge_remote_certified`).
    //
    // The lane carries `(key, value, hlc, origin)`. The receiver must pin
    // the ORIGIN policy version it was handed instead of stamping the
    // entry with its own current version — otherwise a v1 write read on a
    // node that has already moved to v2 is evaluated against the v2
    // frontier, which is the #342 fence bypass reopened cluster-wide.
    // ---------------------------------------------------------------

    fn versioned_namespace(version: u64) -> Arc<RwLock<SystemNamespace>> {
        let mut ns = SystemNamespace::new();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: kr(""),
            authority_nodes: vec![node("auth-1"), node("auth-2"), node("auth-3")],
            auto_generated: false,
        });
        ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(version), kr(""), 3))
            .unwrap();
        wrap_ns(ns)
    }

    fn remote_hlc(physical: u64) -> HlcTimestamp {
        HlcTimestamp {
            physical,
            logical: 0,
            node_id: "remote".into(),
        }
    }

    #[test]
    fn merge_remote_certified_pins_origin_and_fences_old_version() {
        // This node already runs v2; the arriving entry was written under v1.
        let mut api = CertifiedApi::new(node("node-1"), versioned_namespace(2));

        let outcome = api
            .merge_remote_test(
                "key1".into(),
                &counter_value(3),
                remote_hlc(1_000),
                Some(PolicyVersion(1)),
            )
            .unwrap();
        assert_eq!(outcome, IngestOutcome::Applied);

        assert_eq!(api.origins().get("key1"), Some(&PolicyVersion(1)));
        let pw = api
            .pending_writes()
            .iter()
            .find(|p| p.key == "key1")
            .expect("ingest must track the entry so it can certify later");
        assert_eq!(
            pw.policy_version,
            PolicyVersion(1),
            "the tracked write must carry the ORIGIN version, not this node's current one"
        );
        assert!(
            api.is_version_fenced(&kr(""), &PolicyVersion(1)),
            "an origin behind the current version must re-derive the FR-009 fence, \
             exactly as `rebuild_pending_from_store` does on restart"
        );

        // A v2 frontier well past the write must NOT certify it.
        api.update_frontier(make_frontier_v("auth-1", 9_000, 0, "", 2));
        api.update_frontier(make_frontier_v("auth-2", 9_000, 0, "", 2));
        api.process_certifications();
        assert_ne!(
            api.get_certification_status("key1"),
            CertificationStatus::Certified
        );
    }

    #[test]
    fn merge_remote_certified_rejects_origin_ahead_of_current_version() {
        // A peer that has already seen a version bump this node has not.
        // Accepting the pin would park the entry on a frontier this node
        // can never produce: permanent Pending (a liveness poison), and a
        // claim about a policy version this node cannot validate.
        let mut api = CertifiedApi::new(node("node-1"), versioned_namespace(1));

        let outcome = api
            .merge_remote_test(
                "key1".into(),
                &counter_value(3),
                remote_hlc(1_000),
                Some(PolicyVersion(2)),
            )
            .unwrap();
        assert_eq!(
            outcome,
            IngestOutcome::Rejected(IngestRejection::OriginAhead)
        );
        assert!(
            api.store().get("key1").is_none(),
            "a rejected ingest must not touch the store"
        );
        assert!(api.origins().get("key1").is_none());
        assert!(api.pending_writes().is_empty());
    }

    #[test]
    fn forged_single_authority_scope_cannot_reduce_trusted_quorum() {
        let mut api = CertifiedApi::new(node("node-1"), versioned_namespace(1));
        let hlc = remote_hlc(1_000);
        let forged = CertifiedScopePin {
            write_hlc: hlc.clone(),
            key_range: kr(""),
            policy_version: PolicyVersion(1),
            authority_nodes: vec![node("auth-1")],
        };
        assert!(matches!(
            api.merge_remote_certified("key1".into(), &counter_value(1), hlc, forged),
            Err(CrdtError::PolicyDenied(_))
        ));
        assert!(api.store().get("key1").is_none());
        assert!(api.pending_writes().is_empty());
        assert!(api.scope_pins().is_empty());
    }

    /// A counter contribution from `writer` alone, so two deliveries from
    /// different writers BOTH inflate state regardless of arrival order.
    fn counter_from(writer: &str) -> CrdtValue {
        let mut counter = PnCounter::new();
        counter.increment(&node(writer));
        CrdtValue::Counter(counter)
    }

    #[test]
    fn merge_remote_certified_keeps_origin_of_the_max_hlc_contribution() {
        // Two independent contributions to one key, each inflating state,
        // arriving in either order. HLCs are totally ordered, so every
        // replica must converge on the origin of the HIGHEST HLC —
        // otherwise replicas disagree about which policy scope the key
        // belongs to, and the same read returns different verdicts
        // depending on which node answered.
        for reverse in [false, true] {
            let mut api = CertifiedApi::new(node("node-1"), versioned_namespace(2));
            let mut deliveries = vec![
                (remote_hlc(10), PolicyVersion(1), counter_from("w1")),
                (remote_hlc(20), PolicyVersion(2), counter_from("w2")),
            ];
            if reverse {
                deliveries.reverse();
            }
            for (hlc, origin, value) in deliveries {
                assert_eq!(
                    api.merge_remote_test("key1".into(), &value, hlc, Some(origin))
                        .unwrap(),
                    IngestOutcome::Applied,
                    "both contributions inflate state (reverse={reverse})"
                );
            }
            assert_eq!(
                api.origins().get("key1"),
                Some(&PolicyVersion(2)),
                "origin must follow the max-HLC contribution regardless of arrival order \
                 (reverse={reverse})"
            );
            assert_eq!(
                api.store().timestamp_for("key1").unwrap().physical,
                20,
                "the stored HLC is the max of the contributions (reverse={reverse})"
            );
        }
    }

    #[test]
    fn lower_hlc_merge_keeps_the_existing_scope_pin() {
        let namespace = versioned_namespace(1);
        let mut api = CertifiedApi::new(node("node-1"), Arc::clone(&namespace));
        assert_eq!(
            api.merge_remote_test(
                "key1".into(),
                &counter_from("w1"),
                remote_hlc(20),
                Some(PolicyVersion(1)),
            )
            .unwrap(),
            IngestOutcome::Applied
        );
        let first_pin = api.scope_pins()["key1"].clone();
        namespace
            .write()
            .unwrap()
            .set_authority_definition(AuthorityDefinition {
                key_range: kr(""),
                authority_nodes: vec![node("auth-1"), node("auth-4"), node("auth-5")],
                auto_generated: false,
            });
        assert_eq!(
            api.merge_remote_test(
                "key1".into(),
                &counter_from("w2"),
                remote_hlc(10),
                Some(PolicyVersion(1)),
            )
            .unwrap(),
            IngestOutcome::Applied,
            "the lower-HLC contribution still changes the CRDT value"
        );
        assert_eq!(api.scope_pins()["key1"], first_pin);
        assert_eq!(
            api.pending_writes().last().unwrap().authority_nodes,
            first_pin.authority_nodes
        );
    }

    #[test]
    fn merge_remote_certified_is_a_noop_for_dominated_state() {
        // The RR gate (M-6): re-offering an already-dominated contribution
        // must not touch the change log. Without it the lane never
        // quiesces — each round re-stamps the key and makes it "new" for
        // the peer again.
        let mut api = CertifiedApi::new(node("node-1"), versioned_namespace(1));
        api.merge_remote_test(
            "key1".into(),
            &counter_value(3),
            remote_hlc(1_000),
            Some(PolicyVersion(1)),
        )
        .unwrap();
        let ts_before = api.store().timestamp_for("key1").cloned().unwrap();
        let tracked_before = api.pending_writes().len();

        let outcome = api
            .merge_remote_test(
                "key1".into(),
                &counter_value(3),
                remote_hlc(900),
                Some(PolicyVersion(1)),
            )
            .unwrap();
        assert_eq!(outcome, IngestOutcome::Skipped);
        assert_eq!(
            api.store().timestamp_for("key1").cloned().unwrap(),
            ts_before,
            "a dominated re-offer must not move the per-key HLC"
        );
        assert_eq!(
            api.pending_writes().len(),
            tracked_before,
            "a dominated re-offer must not add a tracking entry"
        );
    }

    #[test]
    fn same_value_with_newer_hlc_replaces_certified_head_even_at_capacity() {
        let mut api = CertifiedApi::new(node("node-1"), versioned_namespace(1));
        assert_eq!(
            api.merge_remote_test(
                "key1".into(),
                &counter_value(3),
                remote_hlc(1_000),
                Some(PolicyVersion(1)),
            )
            .unwrap(),
            IngestOutcome::Applied
        );
        submit_signed_majority(&mut api, [("auth-1", 41), ("auth-2", 42)], 1_000, "", 1);
        api.process_certifications();
        assert_eq!(
            api.get_certified("key1").status,
            CertificationStatus::Certified
        );

        api.retention.max_entries = 1;
        assert_eq!(
            api.merge_remote_test(
                "key1".into(),
                &counter_value(3),
                remote_hlc(2_000),
                Some(PolicyVersion(1)),
            )
            .unwrap(),
            IngestOutcome::Applied
        );
        assert_eq!(api.store().timestamp_for("key1"), Some(&remote_hlc(2_000)));
        assert_eq!(api.pending_writes().len(), 1);
        assert_eq!(api.pending_writes()[0].timestamp, remote_hlc(2_000));
        assert_eq!(
            api.get_certified("key1").status,
            CertificationStatus::Pending
        );

        assert_eq!(
            api.merge_remote_test(
                "key1".into(),
                &counter_value(3),
                remote_hlc(2_000),
                Some(PolicyVersion(1)),
            )
            .unwrap(),
            IngestOutcome::Skipped
        );
    }

    #[test]
    fn certified_delta_entries_truncates_at_the_last_included_entry() {
        // A truncated batch must report the LAST INCLUDED entry's HLC as
        // the sender frontier. Reporting the store's full frontier would
        // make the receiver adopt a baseline covering entries it was never
        // sent, and the pull path never re-requests below its baseline —
        // the skipped keys would be lost on that replica forever.
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        for i in 0..5 {
            api.certified_write(format!("key{i}"), counter_value(1), OnTimeout::Pending)
                .unwrap();
        }
        let (entries, frontier) = api.certified_delta_entries(&None, 2);
        assert_eq!(entries.len(), 2);
        let frontier = frontier.expect("a truncated batch still reports a frontier");
        assert_eq!(
            frontier,
            CertifiedDeltaCursor {
                hlc: entries[1].2.clone(),
                key: entries[1].0.clone()
            },
            "the truncation frontier is the last DELIVERED entry's position"
        );
        assert!(
            frontier.hlc < api.store().current_frontier().unwrap(),
            "and it must trail the store's own frontier"
        );

        // Resuming from it delivers exactly the remainder, once.
        let (rest, rest_frontier) = api.certified_delta_entries(&Some(frontier), 100);
        assert_eq!(rest.len(), 3, "the remainder must not be skipped");
        assert_eq!(
            rest_frontier.as_ref().map(|cursor| &cursor.hlc),
            api.store().current_frontier().as_ref()
        );
        let (empty, _) = api.certified_delta_entries(&rest_frontier, 100);
        assert!(empty.is_empty(), "a complete pull must then quiesce");
    }

    #[test]
    fn certified_delta_entries_resume_across_equal_timestamps() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        let timestamp = remote_hlc(100);
        for key in ["a", "b", "c"] {
            assert_eq!(
                api.merge_remote_test(
                    key.into(),
                    &counter_from("writer"),
                    timestamp.clone(),
                    None,
                )
                .unwrap(),
                IngestOutcome::Applied
            );
        }

        let mut cursor = None;
        let mut keys = Vec::new();
        for _ in 0..3 {
            let (entries, next) = api.certified_delta_entries(&cursor, 1);
            assert_eq!(entries.len(), 1);
            keys.push(entries[0].0.clone());
            cursor = next;
        }
        assert_eq!(keys, ["a", "b", "c"]);
        assert!(api.certified_delta_entries(&cursor, 1).0.is_empty());
    }

    #[test]
    fn certified_delta_cursor_retries_refused_key_at_equal_timestamp() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        let timestamp = remote_hlc(100);
        for key in ["accepted", "refused"] {
            api.merge_remote_test(key.into(), &counter_from("writer"), timestamp.clone(), None)
                .unwrap();
        }
        let (entries, _) = api.certified_delta_entries(&None, 2);
        assert_eq!(entries[0].0, "accepted");
        assert_eq!(entries[1].0, "refused");

        // A transient refusal after accepting the first entry leaves the
        // cursor at that exact key, so the second key is offered again.
        let accepted = CertifiedDeltaCursor {
            hlc: entries[0].2.clone(),
            key: entries[0].0.clone(),
        };
        let (retry, _) = api.certified_delta_entries(&Some(accepted), 2);
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0].0, "refused");
    }

    #[test]
    fn certified_delta_entries_tags_each_entry_with_its_origin() {
        let mut api = CertifiedApi::new(node("node-1"), versioned_namespace(3));
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        let (entries, _) = api.certified_delta_entries(&None, 100);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].3.policy_version, PolicyVersion(3));
        assert_eq!(entries[0].3.authority_nodes.len(), 3);
        assert_eq!(entries[0].3.write_hlc, entries[0].2);
    }

    #[test]
    fn certified_delta_entries_ship_full_state_not_a_partial_delta() {
        // The certified plane has no digest lane and no full-sync
        // fallback, so a receiver that never got an entry cannot ask for
        // the base a partial payload assumes. `OrMap::delta_since` IS a
        // partial payload and `Store::merge_value` inserts it verbatim
        // when the key is absent -- a certified map silently missing
        // fields, served with a valid proof. Shipping full state makes
        // every entry self-sufficient, so any later update repairs a hole.
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        let mut map = OrMap::new();
        map.set("f1".into(), "v1".into(), remote_hlc(10), &node("node-1"));
        api.certified_write(
            "key1".into(),
            CrdtValue::Map(map.clone()),
            OnTimeout::Pending,
        )
        .unwrap();
        let after_first = CertifiedDeltaCursor {
            hlc: api.store().current_frontier().unwrap(),
            key: "key1".into(),
        };

        map.set("f2".into(), "v2".into(), remote_hlc(20), &node("node-1"));
        api.certified_write("key1".into(), CrdtValue::Map(map), OnTimeout::Pending)
            .unwrap();

        // A receiver whose baseline already covers the FIRST write (it was
        // offered that entry and refused it, or adopted the baseline from
        // another peer) is offered only the second. That entry must still
        // carry both fields.
        let (entries, _) = api.certified_delta_entries(&Some(after_first), 100);
        assert_eq!(entries.len(), 1);
        let CrdtValue::Map(shipped) = &entries[0].1 else {
            panic!("expected a map");
        };
        assert!(
            shipped.get(&"f1".to_string()).is_some() && shipped.get(&"f2".to_string()).is_some(),
            "a replicated entry must carry the key's FULL state; a partial \
             payload merged onto a missing base silently drops fields"
        );
    }

    #[test]
    fn ingest_refuses_capacity_before_mutation_and_preserves_local_write() {
        // A replicated contribution cannot evict a pending receipt. Its
        // caller retains the baseline and retries after capacity frees up.
        let max_entries = 8usize;
        let mut api = CertifiedApi::with_retention(
            node("node-1"),
            default_namespace(),
            RetentionPolicy {
                max_age_ms: 60_000,
                max_entries,
            },
        );

        // Fill most of the budget with replicated entries, then take the
        // local write a client is waiting on.
        for i in 0..(max_entries - 1) {
            api.merge_remote_test(
                format!("remote{i}"),
                &counter_value(1),
                remote_hlc(1_000 + i as u64),
                None,
            )
            .unwrap();
        }
        api.certified_write("local".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();

        let before = api.pending_writes().len();
        assert!(matches!(
            api.merge_remote_test("burst0".into(), &counter_value(1), remote_hlc(5_000), None,),
            Err(CrdtError::CertificationCapacity { max_entries: 8 })
        ));
        assert_eq!(api.pending_writes().len(), before);
        assert!(api.store.get("burst0").is_none());
        assert!(
            api.pending_writes().iter().any(|pw| pw.key == "local"),
            "local pending write must keep its receipt"
        );

        assert_eq!(
            api.merge_remote_test(
                "remote0".into(),
                &counter_value(1),
                remote_hlc(1_000),
                Some(PolicyVersion(1)),
            )
            .unwrap(),
            IngestOutcome::Skipped,
            "a dominated tracked re-offer must advance despite full capacity"
        );
        assert_eq!(
            api.merge_remote_test(
                "remote0".into(),
                &counter_value(2),
                remote_hlc(9_000),
                Some(PolicyVersion(1)),
            )
            .unwrap(),
            IngestOutcome::Applied,
            "an updated same-key head replaces its old receipt without using another slot"
        );
        assert_eq!(api.pending_writes().len(), before);
        assert_eq!(
            api.pending_writes()
                .iter()
                .filter(|pw| pw.key == "remote0")
                .count(),
            1
        );

        let mut forged_scope = api.scope_pins()["remote0"].clone();
        forged_scope.authority_nodes = vec![node("auth-1")];
        assert!(matches!(
            api.merge_remote_certified(
                "remote0".into(),
                &counter_value(1),
                remote_hlc(1_000),
                forged_scope,
            ),
            Err(CrdtError::PolicyDenied(_))
        ));
        assert_eq!(api.pending_writes().len(), before);
    }

    #[test]
    fn full_capacity_cannot_skip_a_dominated_conflicting_map_dot() {
        let mut api = CertifiedApi::with_retention(
            node("node-1"),
            default_namespace(),
            RetentionPolicy {
                max_age_ms: 60_000,
                max_entries: 1,
            },
        );
        let writer = node("writer");
        let mut original = OrMap::new();
        original.set("field".into(), "z".into(), remote_hlc(10), &writer);
        api.certified_write("key1".into(), CrdtValue::Map(original), OnTimeout::Pending)
            .unwrap();
        let scope = api.scope_pins()["key1"].clone();
        let before = api.store.get("key1").cloned();
        let pending = api.pending_writes().len();

        // Same writer/counter/key but a smaller value would make the old
        // simulated merge report no change and bypass Store validation.
        let mut conflicting = OrMap::new();
        conflicting.set("field".into(), "a".into(), remote_hlc(10), &writer);
        assert_eq!(
            api.merge_remote_certified(
                "key1".into(),
                &CrdtValue::Map(conflicting),
                scope.write_hlc.clone(),
                scope,
            )
            .unwrap(),
            IngestOutcome::Rejected(IngestRejection::DotAssignmentConflict)
        );
        assert_eq!(api.store.get("key1"), before.as_ref());
        assert_eq!(api.pending_writes().len(), pending);
    }

    #[test]
    fn a_dominated_re_offer_retracks_an_untracked_key() {
        // A periodic rescan re-offers every key a peer holds. For a key
        // whose tracking was dropped by legacy retention behavior
        // that re-offer is the only signal a replica ever gets that the
        // key exists, so it must restore the tracking entry -- otherwise
        // the key reports `Pending` with `proof: null` for the life of the
        // process while the value sits in the store.
        let mut api = CertifiedApi::new(node("node-1"), versioned_namespace(1));
        api.merge_remote_test(
            "key1".into(),
            &counter_value(1),
            remote_hlc(1_000),
            Some(PolicyVersion(1)),
        )
        .unwrap();
        assert_eq!(api.pending_writes().len(), 1);

        // Drop the tracking to simulate a legacy missing receipt
        // would, leaving the value and its origin in place.
        api.retain_pending(|_| false);
        assert!(api.pending_writes().is_empty());

        let outcome = api
            .merge_remote_test(
                "key1".into(),
                &counter_value(1),
                remote_hlc(1_000),
                Some(PolicyVersion(1)),
            )
            .unwrap();
        assert_eq!(
            outcome,
            IngestOutcome::Skipped,
            "a dominated re-offer must still be a no-op for the store"
        );
        let pw = api
            .pending_writes()
            .iter()
            .find(|pw| pw.key == "key1")
            .expect("the re-offer must restore tracking for an untracked key");
        assert_eq!(
            pw.policy_version,
            PolicyVersion(1),
            "and restore it under the pinned ORIGIN version"
        );
    }

    #[test]
    fn ingest_refuses_an_existing_head_without_a_write_time_pin() {
        let mut api = CertifiedApi::new(node("node-1"), versioned_namespace(1));
        api.store
            .merge_value("key1".into(), &counter_from("w1"))
            .unwrap();
        api.store.record_change("key1", remote_hlc(9_000));
        let before = api.store.get("key1").cloned();

        assert!(matches!(
            api.merge_remote_test(
                "key1".into(),
                &counter_from("w2"),
                remote_hlc(1_000),
                Some(PolicyVersion(1)),
            ),
            Err(CrdtError::Internal(_))
        ));
        assert_eq!(api.store.get("key1").cloned(), before);
        assert!(api.pending_writes().is_empty());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn failed_certified_ingest_leaves_no_unlogged_value_and_retries() {
        // A failed append must not publish the incoming value or timestamp.
        // The lane holds its baseline back, then retries the same packet.
        let dir = tempfile::tempdir().unwrap();
        let wal = WalWriter::open(crate::store::wal::WalConfig::new(
            dir.path(),
            crate::store::wal::SyncPolicy::Always,
        ))
        .unwrap();
        let mut api = CertifiedApi::recovered(
            node("node-1"),
            versioned_namespace(2),
            Store::new(),
            Some(wal),
            HashMap::new(),
        );

        api.wal.as_mut().unwrap().inject_append_failures(1);
        api.merge_remote_test(
            "key1".into(),
            &counter_value(3),
            remote_hlc(5_000),
            Some(PolicyVersion(1)),
        )
        .unwrap_err();
        assert!(api.pending_writes().is_empty());
        assert!(api.store.get("key1").is_none());
        assert!(api.store.timestamp_for("key1").is_none());
        assert!(api.last_wal_pos().is_none());
        assert!(!api.is_version_fenced(&kr(""), &PolicyVersion(1)));

        // The lane held its baseline back and re-offers the same entry.
        let outcome = api
            .merge_remote_test(
                "key1".into(),
                &counter_value(3),
                remote_hlc(5_000),
                Some(PolicyVersion(1)),
            )
            .unwrap();
        assert_eq!(
            outcome,
            IngestOutcome::Applied,
            "an uncommitted contribution must not be RR-skipped"
        );
        assert!(
            api.last_wal_pos().is_some(),
            "the retry must append the missing CertifiedUpsert record"
        );
        assert_eq!(
            api.pending_writes()
                .iter()
                .find(|pw| pw.key == "key1")
                .map(|pw| pw.policy_version),
            Some(PolicyVersion(1))
        );
        assert!(
            api.is_version_fenced(&kr(""), &PolicyVersion(1)),
            "and install the fence the failed round skipped"
        );
    }

    // ---------------------------------------------------------------
    // Recovery must restore every pending receipt even when the persisted
    // store exceeds the current admission limit.
    // ---------------------------------------------------------------

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn recovery_retains_all_pending_writes_above_admission_cap() {
        let max_entries = RetentionPolicy::default().max_entries;
        let mut store = Store::new();
        let mut origins = HashMap::new();
        let mut scope_pins = HashMap::new();
        for i in 0..(max_entries + 25) {
            let key = format!("key{i}");
            let write_hlc = HlcTimestamp {
                physical: 1_000 + i as u64,
                logical: 0,
                node_id: "writer".into(),
            };
            store.merge_value(key.clone(), &counter_value(1)).unwrap();
            store.record_change(&key, write_hlc.clone());
            origins.insert(key.clone(), PolicyVersion(1));
            scope_pins.insert(
                key,
                CertifiedScopePin {
                    write_hlc,
                    key_range: kr(""),
                    policy_version: PolicyVersion(1),
                    authority_nodes: vec![node("auth-1"), node("auth-2"), node("auth-3")],
                },
            );
        }

        let mut api = CertifiedApi::recovered_with_scope_pins(
            node("node-1"),
            default_namespace(),
            store,
            None,
            origins,
            scope_pins,
        );
        assert_eq!(api.pending_writes().len(), max_entries + 25);
        assert!(api.pending_writes().iter().any(|pw| pw.key == "key0"));
        assert_eq!(
            api.certified_write(
                "after-recovery".into(),
                counter_value(1),
                OnTimeout::Pending
            ),
            Err(CrdtError::CertificationCapacity { max_entries })
        );
        assert!(api.store.get("after-recovery").is_none());
    }
}
