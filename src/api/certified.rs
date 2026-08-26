use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use serde::Serialize;

use crate::authority::ack_frontier::{AckFrontier, AckFrontierSet};
use crate::authority::attestation_pool::{AttestationPool, InsertOutcome};
use crate::authority::certificate::{DualModeCertificate, MajorityCertificate};
use crate::authority::frontier_sig::{CHECKPOINT_INTERVAL_MS, VerifiedAttestation};
use crate::control_plane::system_namespace::SystemNamespace;
use crate::error::CrdtError;
use crate::hlc::{Hlc, HlcTimestamp};
use crate::store::kv::{CrdtValue, Store};
#[cfg(not(target_arch = "wasm32"))]
use crate::store::wal::{WalPos, WalRecord, WalWriter};
use crate::types::{CertificationStatus, KeyRange, NodeId, PolicyVersion};

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

/// A verifiable proof bundle attached to a certified read response.
///
/// Contains the metadata needed for an external client to independently
/// verify that a majority of authorities have acknowledged a given frontier.
/// When the signing pipeline is active (signed frontier reports), the
/// `certificate` field carries an Ed25519 `MajorityCertificate` whose
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
    /// The Ed25519 majority certificate, when signed attestations reached
    /// a majority. `None` when frontiers were reported unsigned.
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
    /// The majority frontier at query time, if available.
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
}

/// Configuration for retention and cleanup of pending writes.
///
/// Controls how `CertifiedApi` bounds the growth of its internal
/// `pending_writes` list. Cleanup can be triggered explicitly via
/// `cleanup()` or automatically when `max_entries` is exceeded
/// during `certified_write`.
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    /// Maximum age in milliseconds for pending writes before they are
    /// marked as `Timeout` and eligible for removal. Default: 60,000 ms.
    pub max_age_ms: u64,
    /// Maximum number of tracked writes. When exceeded during
    /// `certified_write`, an automatic cleanup is triggered.
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
    /// The majority certificate, if available.
    pub certificate: Option<MajorityCertificate>,
    /// The BLS aggregate certificate, if available.
    pub bls_certificate: Option<DualModeCertificate>,
    /// The HLC timestamp of the write that was certified.
    pub write_timestamp: HlcTimestamp,
}

/// Maximum number of entries in the certified proof cache before eviction.
const MAX_CERTIFIED_CACHE: usize = 10_000;

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
    retention: RetentionPolicy,
    /// Cumulative count of pending writes evicted due to `max_entries` pressure.
    evicted_count: u64,
    /// Cache of certified proofs that survives `pending_writes` cleanup.
    ///
    /// When a write transitions to `Certified`, its proof info is stored here
    /// so that subsequent reads still return `Certified` with proof even after
    /// the pending write entry has been removed by cleanup or retention eviction.
    /// For a given key, only the latest certified entry is kept.
    certified_cache: HashMap<String, CertifiedCacheEntry>,
    /// Pool of signature-verified frontier attestations, used to assemble
    /// majority certificates for certified proofs (FR-008).
    attestations: AttestationPool,
    /// Keys promoted to `Certified` before a certificate could be assembled.
    /// Later certification ticks retry certificate assembly for these keys.
    cert_pending_keys: HashSet<String>,
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
        Self {
            store: Store::new(),
            clock: Hlc::new(node_id.0),
            frontiers: AckFrontierSet::new(),
            namespace,
            pending_writes: Vec::new(),
            retention: RetentionPolicy::default(),
            evicted_count: 0,
            certified_cache: HashMap::new(),
            attestations: AttestationPool::new(),
            cert_pending_keys: HashSet::new(),
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
    #[cfg(not(target_arch = "wasm32"))]
    pub fn recovered(
        node_id: NodeId,
        namespace: Arc<RwLock<SystemNamespace>>,
        store: Store,
        wal: Option<WalWriter>,
    ) -> Self {
        let mut clock = Hlc::new(node_id.0);
        if let Some(max) = store.max_known_hlc() {
            clock.seed_recovered(&max);
        }
        let mut api = Self {
            store,
            clock,
            frontiers: AckFrontierSet::new(),
            namespace,
            pending_writes: Vec::new(),
            retention: RetentionPolicy::default(),
            evicted_count: 0,
            certified_cache: HashMap::new(),
            attestations: AttestationPool::new(),
            cert_pending_keys: HashSet::new(),
            attestation_rejected_unknown_range_total: 0,
            attestation_rejected_version_window_total: 0,
            attestation_stale_version_total: 0,
            attestation_rejected_fenced_total: 0,
            stale_version_warned_ms: HashMap::new(),
            last_stale_prune_ms: 0,
            wal,
            last_wal_pos: None,
        };
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
    #[cfg(not(target_arch = "wasm32"))]
    fn rebuild_pending_from_store(&mut self) {
        let keys: Vec<String> = self.store.keys().into_iter().cloned().collect();
        for key in keys {
            let (Some(value), Some(timestamp)) = (
                self.store.get(&key).cloned(),
                self.store.timestamp_for(&key).cloned(),
            ) else {
                continue;
            };
            let Ok((key_range, policy_version, total_authorities)) = self.resolve_scope(&key)
            else {
                continue;
            };
            self.pending_writes.push(PendingWrite {
                key,
                value,
                timestamp,
                status: CertificationStatus::Pending,
                key_range,
                policy_version,
                total_authorities,
            });
        }
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
        Self {
            store: Store::new(),
            clock: Hlc::new(node_id.0),
            frontiers: AckFrontierSet::new(),
            namespace,
            pending_writes: Vec::new(),
            retention,
            evicted_count: 0,
            certified_cache: HashMap::new(),
            attestations: AttestationPool::new(),
            cert_pending_keys: HashSet::new(),
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
        // would issue carries `total_authorities: 0`, which the verifier then
        // rejects (required = 1, contributing = 0). Refuse the scope outright
        // rather than record a write that can only ever be self-contradictory.
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

    /// Record a certified write in the proof cache.
    ///
    /// Captures the frontier state at certification time so that later reads
    /// can still return `Certified` with a valid proof bundle even after the
    /// pending write has been cleaned up.
    fn cache_certified_proof(&mut self, pw: &PendingWrite) {
        // Prefer a cryptographic certificate assembled from signed
        // attestations. When available, the proof's frontier_hlc becomes the
        // certificate checkpoint so that verifiers recompute the exact signed
        // message. When not yet available (e.g. the signed checkpoint has not
        // caught up with the write), fall back to the unsigned proof and let
        // later certification ticks attach the certificate lazily.
        let built = self.attestations.build_certificates(
            &pw.key_range,
            pw.policy_version,
            pw.total_authorities,
            &pw.timestamp,
        );

        let (frontier_hlc, contributing_authorities, certificate, bls_certificate) = match built {
            Some((checkpoint, cert, bls_cert)) => {
                let contributing: Vec<NodeId> = cert.signers().into_iter().cloned().collect();
                (checkpoint, contributing, Some(cert), bls_cert)
            }
            None => {
                // Only queue the key for certificate back-fill when signed
                // attestations exist for this scope. In unsigned deployments
                // (no signer / non-native builds) no certificate can ever be
                // assembled, and queueing every certified key would make
                // refresh_missing_certificates rescan the full certified
                // cache on every certification tick, forever.
                if self
                    .attestations
                    .has_attestations(&pw.key_range, &pw.policy_version)
                {
                    self.cert_pending_keys.insert(pw.key.clone());
                }
                let scoped_frontiers = self
                    .frontiers
                    .all_for_scope(&pw.key_range, &pw.policy_version);
                let contributing: Vec<NodeId> = scoped_frontiers
                    .iter()
                    .map(|f| f.authority_id.clone())
                    .collect();
                let frontier_hlc = self
                    .frontiers
                    .majority_frontier_for_scope(
                        &pw.key_range,
                        &pw.policy_version,
                        pw.total_authorities,
                    )
                    .unwrap_or_else(|| pw.timestamp.clone());
                (frontier_hlc, contributing, None, None)
            }
        };

        self.certified_cache.insert(
            pw.key.clone(),
            CertifiedCacheEntry {
                key_range: pw.key_range.clone(),
                frontier_hlc,
                policy_version: pw.policy_version,
                contributing_authorities,
                total_authorities: pw.total_authorities,
                certificate,
                bls_certificate,
                write_timestamp: pw.timestamp.clone(),
            },
        );

        // Evict oldest entries when the cache exceeds the size limit.
        if self.certified_cache.len() > MAX_CERTIFIED_CACHE {
            let evict_count = self.certified_cache.len() - MAX_CERTIFIED_CACHE;
            let mut entries: Vec<(String, HlcTimestamp)> = self
                .certified_cache
                .iter()
                .map(|(k, v)| (k.clone(), v.write_timestamp.clone()))
                .collect();
            entries.sort_by(|a, b| a.1.cmp(&b.1));
            for (key, _) in entries.into_iter().take(evict_count) {
                self.certified_cache.remove(&key);
            }
        }
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
    /// This ensures certification stability after cleanup or retention eviction.
    pub fn get_certified(&self, key: &str) -> CertifiedRead<'_> {
        let value = self.store.get(key);

        let scope_info = self.resolve_scope(key).ok();

        let frontier = scope_info
            .as_ref()
            .and_then(|(kr, pv, total)| self.frontiers.majority_frontier_for_scope(kr, pv, *total));

        // Look up status from pending_writes first; fall back to certified_cache.
        let pending_status = self
            .pending_writes
            .iter()
            .rev()
            .find(|pw| pw.key == key)
            .map(|pw| pw.status);

        let (status, proof) = match pending_status {
            Some(CertificationStatus::Certified) => {
                // Prefer the cached proof: it carries the certificate (and the
                // checkpoint frontier the authorities actually signed) when
                // the signing pipeline produced one at promotion time.
                let proof = if let Some(cached) = self.certified_cache.get(key) {
                    Some(Self::proof_from_cache(cached))
                } else {
                    // Fall back to live frontier data (no certificate).
                    scope_info.as_ref().and_then(|(kr, pv, total)| {
                        let frontier_hlc = frontier.clone()?;
                        let scoped_frontiers = self.frontiers.all_for_scope(kr, pv);
                        let contributing_authorities: Vec<NodeId> = scoped_frontiers
                            .iter()
                            .map(|f| f.authority_id.clone())
                            .collect();

                        Some(ProofBundle {
                            key_range: kr.clone(),
                            frontier_hlc,
                            policy_version: *pv,
                            contributing_authorities,
                            total_authorities: *total,
                            certificate: None,
                            bls_certificate: None,
                        })
                    })
                };
                (CertificationStatus::Certified, proof)
            }
            Some(other_status) => (other_status, None),
            None => {
                // No pending write — check the certified cache.
                if let Some(cached) = self.certified_cache.get(key) {
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
    /// `max_entries` is enforced as a hard limit. When the pending list
    /// reaches capacity:
    /// 1. Completed (non-`Pending`) entries are removed first.
    /// 2. If still at capacity, the **oldest** `Pending` entries are evicted
    ///    (marked `Timeout` and removed) to make room for the new write.
    ///
    /// Evictions are tracked via [`evicted_count`](Self::evicted_count).
    ///
    /// If the write is already certified at the current scoped frontier,
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

        // Auto-cleanup when capacity is exceeded.
        if self.pending_writes.len() >= self.retention.max_entries {
            self.cleanup_completed();
        }

        // Hard eviction: if still at capacity after removing completed entries,
        // evict oldest pending writes (mark as Timeout then remove) to make room.
        if self.pending_writes.len() >= self.retention.max_entries {
            let evict_count = self.pending_writes.len() - self.retention.max_entries + 1;
            let mut evicted = 0;
            for pw in &mut self.pending_writes {
                if evicted >= evict_count {
                    break;
                }
                if pw.status == CertificationStatus::Pending {
                    pw.status = CertificationStatus::Timeout;
                    evicted += 1;
                }
            }
            self.evicted_count += evicted as u64;
            self.pending_writes
                .retain(|pw| pw.status != CertificationStatus::Timeout);
        }

        // Invalidate any stale certified cache entry for this key so that
        // subsequent reads trigger fresh certification instead of returning
        // a proof that corresponds to the old value.
        self.certified_cache.remove(&key);

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

        self.store.merge_value(key.clone(), &value)?;
        self.store.record_change(&key, timestamp.clone());

        // WAL-log the post-write state before any acknowledgement. The
        // certified store has no anti-entropy fallback, so a failed append
        // is surfaced immediately (the in-memory value stays, un-acked).
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(wal) = self.wal.as_mut() {
            let post_state =
                self.store.get(&key).cloned().ok_or_else(|| {
                    CrdtError::Internal(format!("no post-state for WAL key {key}"))
                })?;
            let record = WalRecord::UpsertApplied {
                key: key.clone(),
                value: post_state,
                hlc: timestamp.clone(),
            };
            match wal.append(&record) {
                Ok(pos) => self.last_wal_pos = Some(pos),
                Err(e) => return Err(CrdtError::Storage(format!("WAL append failed: {e}"))),
            }
        }

        // Check if already certified at the current scoped frontier.
        let already_certified = self.frontiers.is_certified_at_for_scope(
            &timestamp,
            &key_range,
            &policy_version,
            total_authorities,
        );

        let status = if already_certified {
            CertificationStatus::Certified
        } else {
            CertificationStatus::Pending
        };

        let pw = PendingWrite {
            key: key.clone(),
            value,
            timestamp,
            status,
            key_range,
            policy_version,
            total_authorities,
        };

        if already_certified {
            self.cache_certified_proof(&pw);
        }

        self.pending_writes.push(pw);

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

    /// Check the certification status of the latest write for a key.
    ///
    /// Returns `CertificationStatus::Pending` if no tracked write exists
    /// and the key is not in the certified proof cache.
    pub fn get_certification_status(&self, key: &str) -> CertificationStatus {
        self.pending_writes
            .iter()
            .rev()
            .find(|pw| pw.key == key)
            .map(|pw| pw.status)
            .unwrap_or_else(|| {
                if self.certified_cache.contains_key(key) {
                    CertificationStatus::Certified
                } else {
                    CertificationStatus::Pending
                }
            })
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
    /// (monotonicity, deduplication, fencing). When `verified` is present
    /// and the scope is not fenced, the attestation is recorded in the pool
    /// **even if the frontier update itself was stale** — a late-arriving
    /// signature for an already-known checkpoint still contributes to
    /// certificate assembly. Returns whether the frontier advanced.
    pub fn update_frontier_verified(
        &mut self,
        frontier: AckFrontier,
        verified: Option<VerifiedAttestation>,
    ) -> bool {
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
        let advanced = self.frontiers.update(frontier);
        if let Some(att) = verified
            && !fenced
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
        }
    }

    /// Re-evaluate all pending writes against the current frontiers.
    ///
    /// Each write is checked against the scoped majority frontier for its
    /// resolved key range. Writes whose timestamps are at or below the
    /// scoped majority frontier are promoted to `Certified` and their proof
    /// is cached for stability across cleanup cycles.
    pub fn process_certifications(&mut self) {
        let mut newly_certified = Vec::new();
        for pw in &mut self.pending_writes {
            if pw.status == CertificationStatus::Pending
                && self.frontiers.is_certified_at_for_scope(
                    &pw.timestamp,
                    &pw.key_range,
                    &pw.policy_version,
                    pw.total_authorities,
                )
            {
                pw.status = CertificationStatus::Certified;
                newly_certified.push(pw.clone());
            }
        }
        for pw in &newly_certified {
            self.cache_certified_proof(pw);
        }
        self.refresh_missing_certificates();
    }

    /// Re-evaluate pending writes and detect timeouts in a single pass.
    ///
    /// Combines the logic of [`process_certifications`](Self::process_certifications)
    /// and timeout detection: pending writes whose timestamps are at or below
    /// the scoped majority frontier are promoted to `Certified` (and cached),
    /// while those older than `max_age_ms` are marked as `Timeout`.
    ///
    /// Returns the number of writes that transitioned (certified + timed out).
    pub fn process_certifications_with_timeout(&mut self, now_physical_ms: u64) -> usize {
        let mut transitions = 0;
        let mut newly_certified = Vec::new();
        for pw in &mut self.pending_writes {
            if pw.status != CertificationStatus::Pending {
                continue;
            }
            if self.frontiers.is_certified_at_for_scope(
                &pw.timestamp,
                &pw.key_range,
                &pw.policy_version,
                pw.total_authorities,
            ) {
                pw.status = CertificationStatus::Certified;
                newly_certified.push(pw.clone());
                transitions += 1;
            } else if now_physical_ms.saturating_sub(pw.timestamp.physical)
                >= self.retention.max_age_ms
            {
                pw.status = CertificationStatus::Timeout;
                transitions += 1;
            }
        }
        for pw in &newly_certified {
            self.cache_certified_proof(pw);
        }
        self.refresh_missing_certificates();
        transitions
    }

    /// Retry certificate assembly for keys certified without a certificate.
    ///
    /// Checkpoint normalisation means the signed checkpoint can lag a write
    /// by up to the bucket width plus one report interval, so a write is
    /// often promoted to `Certified` before a qualifying certificate exists.
    /// This pass back-fills those cache entries once attestations catch up.
    fn refresh_missing_certificates(&mut self) {
        if self.cert_pending_keys.is_empty() {
            return;
        }
        let keys: Vec<String> = self.cert_pending_keys.iter().cloned().collect();
        for key in keys {
            let Some(entry) = self.certified_cache.get(&key) else {
                // Entry evicted or invalidated; nothing to back-fill.
                self.cert_pending_keys.remove(&key);
                continue;
            };
            let built = self.attestations.build_certificates(
                &entry.key_range,
                entry.policy_version,
                entry.total_authorities,
                &entry.write_timestamp,
            );
            if let Some((checkpoint, cert, bls_cert)) = built {
                let entry = self
                    .certified_cache
                    .get_mut(&key)
                    .expect("entry existence checked above");
                entry.frontier_hlc = checkpoint;
                entry.contributing_authorities = cert.signers().into_iter().cloned().collect();
                entry.certificate = Some(cert);
                entry.bls_certificate = bls_cert;
                self.cert_pending_keys.remove(&key);
            }
        }
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
    /// This removes `Certified`, `Rejected`, and `Timeout` entries,
    /// keeping only writes that are still awaiting resolution.
    pub fn cleanup_completed(&mut self) {
        self.pending_writes
            .retain(|pw| pw.status == CertificationStatus::Pending);
    }

    /// Mark pending writes older than `max_age_ms` as `Timeout`,
    /// then remove all non-pending entries.
    ///
    /// `now_physical_ms` is the current wall-clock time in milliseconds.
    pub fn cleanup_expired(&mut self, now_physical_ms: u64) {
        for pw in &mut self.pending_writes {
            if pw.status == CertificationStatus::Pending
                && now_physical_ms.saturating_sub(pw.timestamp.physical)
                    >= self.retention.max_age_ms
            {
                pw.status = CertificationStatus::Timeout;
            }
        }
        self.cleanup_completed();
    }

    /// Full cleanup: expire old pending writes and remove all completed entries.
    ///
    /// This is the recommended periodic maintenance method. It:
    /// 1. Marks stale `Pending` writes as `Timeout` based on `max_age_ms`.
    /// 2. Removes all non-`Pending` entries (`Certified`, `Rejected`, `Timeout`).
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

    /// Return the cumulative count of pending writes evicted due to
    /// `max_entries` pressure.
    ///
    /// This counter increments each time `certified_write` must forcibly
    /// mark oldest `Pending` entries as `Timeout` and remove them because
    /// `cleanup_completed` alone could not bring the size below `max_entries`.
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
        self.frontiers.unfence_version(range, version)
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
    // process_certifications: frontier updates → Certified
    // ---------------------------------------------------------------

    #[test]
    fn process_certifications_promotes_to_certified() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();

        let write_ts = api.pending_writes()[0].timestamp.physical;

        // Advance 2 of 3 authorities past the write timestamp (majority).
        api.update_frontier(make_frontier("auth-1", write_ts + 100, 0, ""));
        api.update_frontier(make_frontier("auth-2", write_ts + 200, 0, ""));

        api.process_certifications();

        assert_eq!(
            api.pending_writes()[0].status,
            CertificationStatus::Certified
        );
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
        api.update_frontier(make_frontier("auth-1", write_ts + 100, 0, ""));
        api.update_frontier(make_frontier("auth-2", write_ts + 200, 0, ""));

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
        api.update_frontier(make_frontier("auth-1", ts1 + 1, 0, ""));
        api.update_frontier(make_frontier("auth-2", ts1 + 1, 0, ""));

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
    // cleanup_expired marks old pending as timeout and removes them
    // ---------------------------------------------------------------

    #[test]
    fn cleanup_expired_marks_and_removes_old_entries() {
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

        // Now expired.
        api.cleanup_expired(write_ts + 5_000);
        assert_eq!(api.pending_writes().len(), 0);
    }

    // ---------------------------------------------------------------
    // cleanup does full maintenance
    // ---------------------------------------------------------------

    #[test]
    fn cleanup_removes_both_completed_and_expired() {
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

        // Certify key1.
        api.update_frontier(make_frontier("auth-1", ts1 + 1, 0, ""));
        api.update_frontier(make_frontier("auth-2", ts1 + 1, 0, ""));
        api.process_certifications();

        let ts2 = api.pending_writes()[1].timestamp.physical;

        // Cleanup at a time after TTL for key2 (and certainly key1).
        api.cleanup(ts2 + 10_000);

        // All entries should be removed: key1 was Certified, key2 was TTL-expired.
        assert_eq!(api.pending_writes().len(), 0);
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
        let ts1 = api.pending_writes()[0].timestamp.physical;
        api.certified_write("key2".into(), counter_value(2), OnTimeout::Pending)
            .unwrap();
        api.certified_write("key3".into(), counter_value(3), OnTimeout::Pending)
            .unwrap();

        // Certify key1 and key2.
        api.update_frontier(make_frontier("auth-1", ts1 + 100, 0, ""));
        api.update_frontier(make_frontier("auth-2", ts1 + 100, 0, ""));
        api.process_certifications();

        assert_eq!(api.pending_writes().len(), 3);

        // Adding a 4th write triggers auto-cleanup (len >= max_entries).
        api.certified_write("key4".into(), counter_value(4), OnTimeout::Pending)
            .unwrap();

        // Certified entries (key1, key2) were cleaned up.
        // key3 (Pending) + key4 (new Pending) remain.
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

        // Simulate sustained writes with periodic certification.
        for i in 0..50u64 {
            api.certified_write(format!("key-{i}"), counter_value(1), OnTimeout::Pending)
                .unwrap();

            // Certify every other write to make them eligible for cleanup.
            if i % 2 == 0 {
                let ts = api.pending_writes().last().unwrap().timestamp.physical;
                api.update_frontier(make_frontier("auth-1", ts + 100, 0, ""));
                api.update_frontier(make_frontier("auth-2", ts + 100, 0, ""));
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
    // Hard limit: all-pending eviction
    // ---------------------------------------------------------------

    #[test]
    fn all_pending_eviction_enforces_hard_limit() {
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

        // Writing a 4th entry must evict the oldest pending to stay <= max_entries.
        api.certified_write("key-3".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();

        assert!(
            api.pending_writes().len() <= 3,
            "expected <= 3, got {}",
            api.pending_writes().len()
        );
        assert!(api.evicted_count() > 0, "expected evictions to be tracked");

        // The evicted entry should be the oldest one (key-0).
        assert!(
            !api.pending_writes().iter().any(|pw| pw.key == "key-0"),
            "oldest pending write should have been evicted"
        );
        // The newest write should be present.
        assert!(
            api.pending_writes().iter().any(|pw| pw.key == "key-3"),
            "newest write should be present"
        );
    }

    // ---------------------------------------------------------------
    // Eviction counter tracks cumulative evictions
    // ---------------------------------------------------------------

    #[test]
    fn evicted_count_tracks_cumulative_evictions() {
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

        // Each additional write evicts 1 oldest pending.
        api.certified_write("c".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        assert_eq!(api.evicted_count(), 1);

        api.certified_write("d".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        assert_eq!(api.evicted_count(), 2);

        // Size never exceeds max_entries.
        assert!(api.pending_writes().len() <= 2);
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

        // 100 writes, none ever certified — pure backpressure scenario.
        for i in 0..100u64 {
            api.certified_write(format!("key-{i}"), counter_value(1), OnTimeout::Pending)
                .unwrap();

            assert!(
                api.pending_writes().len() <= 5,
                "iteration {i}: expected <= 5, got {}",
                api.pending_writes().len()
            );
        }

        // Exactly max_entries entries remain.
        assert_eq!(api.pending_writes().len(), 5);
        // 95 entries were evicted (100 writes - 5 retained).
        assert_eq!(api.evicted_count(), 95);
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
        api.update_frontier(make_frontier("auth-o1", order_ts + 100, 0, "order/"));
        api.update_frontier(make_frontier("auth-o2", order_ts + 200, 0, "order/"));

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
        api.update_frontier(make_frontier("auth-u1", user_ts + 100, 0, "user/"));
        api.update_frontier(make_frontier("auth-u2", user_ts + 200, 0, "user/"));

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
        api.update_frontier(make_frontier_v("auth-1", write_ts + 100, 0, "data/", 1));
        api.update_frontier(make_frontier_v("auth-2", write_ts + 200, 0, "data/", 1));
        api.process_certifications();

        assert_eq!(
            api.get_certification_status("data/sensor"),
            CertificationStatus::Pending,
            "v1 frontiers must not certify a v2 write"
        );

        // Frontiers at version 2 should certify the write.
        api.update_frontier(make_frontier_v("auth-1", write_ts + 100, 0, "data/", 2));
        api.update_frontier(make_frontier_v("auth-2", write_ts + 200, 0, "data/", 2));
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

        // Write to stale/ range (will time out because its authorities never report).
        api.certified_write("stale/key2".into(), counter_value(2), OnTimeout::Pending)
            .unwrap();

        // Advance cert/ authorities past the write timestamp.
        api.update_frontier(make_frontier_v("auth-1", ts1 + 100, 0, "cert/", 1));
        api.update_frontier(make_frontier_v("auth-2", ts1 + 200, 0, "cert/", 1));

        // Process with a time far in the future to trigger timeout on stale/key2.
        let transitions = api.process_certifications_with_timeout(ts1 + 10_000);

        // cert/key1 should be certified (its authorities reached majority).
        assert_eq!(
            api.get_certification_status("cert/key1"),
            CertificationStatus::Certified
        );
        // stale/key2 should time out (its authorities never reported).
        assert_eq!(
            api.get_certification_status("stale/key2"),
            CertificationStatus::Timeout
        );
        assert_eq!(transitions, 2);
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
        api.update_frontier(make_frontier("auth-1", ts + 100, 0, ""));
        api.update_frontier(make_frontier("auth-2", ts + 200, 0, ""));

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
        api.update_frontier(make_frontier("auth-1", ts + 100, 0, ""));
        api.update_frontier(make_frontier("auth-2", ts + 200, 0, ""));
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
    fn reject_write_targets_latest_pending() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());

        // Write same key twice.
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        api.certified_write("key1".into(), counter_value(2), OnTimeout::Pending)
            .unwrap();

        // Reject targets the latest (most recent) pending write.
        assert!(api.reject_write("key1"));

        // The latest should be rejected.
        let writes: Vec<_> = api
            .pending_writes()
            .iter()
            .filter(|pw| pw.key == "key1")
            .collect();
        assert_eq!(writes.len(), 2);
        assert_eq!(writes[0].status, CertificationStatus::Pending);
        assert_eq!(writes[1].status, CertificationStatus::Rejected);
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
        api.update_frontier(make_frontier("auth-1", write_ts + 100, 0, ""));
        api.update_frontier(make_frontier("auth-2", write_ts + 200, 0, ""));

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
            proof.certificate.is_none(),
            "unsigned frontiers carry no certificate"
        );
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
        api.update_frontier(make_frontier("auth-a", write_ts + 100, 0, "data/"));
        api.update_frontier(make_frontier("auth-b", write_ts + 200, 0, "data/"));

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
    fn proof_without_certificate_rejected_by_verifier() {
        use crate::authority::verifier;

        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();

        let write_ts = api.pending_writes()[0].timestamp.physical;

        api.update_frontier(make_frontier("auth-1", write_ts + 100, 0, ""));
        api.update_frontier(make_frontier("auth-2", write_ts + 200, 0, ""));

        api.process_certifications();

        let result = api.get_certified("key1");
        let proof = result.proof.unwrap();

        // Proofs without a certificate must be rejected to prevent forged proofs.
        let verification = verifier::verify_proof(&proof, None, 0);
        assert!(!verification.valid);
        assert!(verification.has_majority);
        assert!(verification.signatures_valid.is_none());
        assert_eq!(verification.contributing_count, 2);
        assert_eq!(verification.required_count, 2); // 3/2+1 = 2
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
        api.update_frontier(make_frontier("auth-1", write_ts + 100, 0, ""));
        api.update_frontier(make_frontier("auth-2", write_ts + 200, 0, ""));
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
        api.update_frontier(make_frontier("auth-1", write_ts + 100, 0, ""));
        api.update_frontier(make_frontier("auth-2", write_ts + 200, 0, ""));
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
        api.update_frontier(make_frontier("auth-1", ts1 + 100, 0, ""));
        api.update_frontier(make_frontier("auth-2", ts1 + 200, 0, ""));
        api.process_certifications();

        assert_eq!(api.certified_cache_len(), 1);

        // Second write to the same key (replaces value in store).
        api.certified_write("key1".into(), counter_value(10), OnTimeout::Pending)
            .unwrap();
        let ts2 = api.pending_writes().last().unwrap().timestamp.physical;

        // Certify second write as well.
        api.update_frontier(make_frontier("auth-1", ts2 + 100, 0, ""));
        api.update_frontier(make_frontier("auth-2", ts2 + 200, 0, ""));
        api.process_certifications();

        // Cache should still have 1 entry (overwritten for same key).
        assert_eq!(api.certified_cache_len(), 1);

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
        api.update_frontier(make_frontier("auth-1", ts + 100, 0, ""));
        api.update_frontier(make_frontier("auth-2", ts + 200, 0, ""));
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
        api.update_frontier(make_frontier("auth-1", ts + 100, 0, ""));
        api.update_frontier(make_frontier("auth-2", ts + 200, 0, ""));
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

        api.update_frontier(make_frontier("auth-1", ts1 + 100, 0, ""));
        api.update_frontier(make_frontier("auth-2", ts1 + 200, 0, ""));
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

    #[test]
    fn update_frontier_verified_records_attestation_and_matches_update() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        let signer = make_signer("auth-1", 1);

        let (f1, a1) = make_signed_frontier(&signer, 10_500, "");
        assert!(api.update_frontier_verified(f1.clone(), Some(a1.clone())));

        // Stale (same-or-older) frontier: update returns false, but the
        // attestation for the same checkpoint is still collected.
        let (f2, a2) = make_signed_frontier(&signer, 10_200, "");
        assert!(!api.update_frontier_verified(f2, Some(a2)));

        // Both attestations landed in the same checkpoint bucket (idempotent).
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

        // Embedded-key verification passes end-to-end.
        let verification = verifier::verify_proof(&proof, None, 0);
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
            &registry,
            0,
            &EpochConfig::default(),
            None,
            0,
        );
        assert!(verification.valid, "registry verification must pass");
    }

    /// A write promoted before the signed checkpoint catches up gets its
    /// certificate attached by a later certification tick.
    #[test]
    fn certificate_attached_lazily_by_later_ticks() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        let write_ts = api.pending_writes()[0].timestamp.physical;

        let s1 = make_signer("auth-1", 7);
        let s2 = make_signer("auth-2", 8);

        // Reports advance the frontier past the write but their checkpoint
        // (floor to 1s) is still below the write timestamp: promotion happens
        // without a certificate.
        let early_ts = write_ts + 1; // same checkpoint bucket as the write
        if !early_ts.is_multiple_of(1000) {
            for signer in [&s1, &s2] {
                let (f, a) = make_signed_frontier(signer, early_ts, "");
                api.update_frontier_verified(f, Some(a));
            }
            api.process_certifications();
            let result = api.get_certified("key1");
            if result.status == CertificationStatus::Certified {
                let proof = result.proof.unwrap();
                assert!(
                    proof.certificate.is_none(),
                    "checkpoint has not caught up; certificate must be absent"
                );
            }
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
            "certificate must be back-filled by a later tick"
        );
    }

    /// Unsigned deployments must not queue keys for certificate back-fill:
    /// no attestation ever arrives, so the retry would spin forever over the
    /// whole certified cache on every certification tick.
    #[test]
    fn unsigned_certification_does_not_queue_certificate_backfill() {
        let mut api = CertifiedApi::new(node("node-1"), default_namespace());
        api.certified_write("key1".into(), counter_value(1), OnTimeout::Pending)
            .unwrap();
        let write_ts = api.pending_writes()[0].timestamp.physical;

        // Unsigned frontier reports certify the write.
        api.update_frontier(make_frontier("auth-1", write_ts + 100, 0, ""));
        api.update_frontier(make_frontier("auth-2", write_ts + 100, 0, ""));
        api.process_certifications();

        assert_eq!(
            api.get_certification_status("key1"),
            CertificationStatus::Certified
        );
        assert!(
            api.cert_pending_keys.is_empty(),
            "unsigned certification must not queue certificate back-fill retries"
        );
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
            3,
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
        // 1 of 3 — certification still happens via frontier majority, but no
        // certificate can be assembled and no purged signer may appear.
        let purged = api.purge_accused_attestations(&[node("auth-1")]);
        assert_eq!(purged, 1);
        assert_eq!(api.attestation_stats().purged_total, 1);

        api.process_certifications();
        let read = api.get_certified("key1");
        assert_eq!(read.status, CertificationStatus::Certified);
        let proof = read.proof.expect("certified read carries a proof");
        assert!(
            proof.certificate.is_none(),
            "1 of 3 attestations after the purge must not certify"
        );

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
}
