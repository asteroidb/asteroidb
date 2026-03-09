use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use serde::Serialize;

use crate::authority::ack_frontier::{AckFrontier, AckFrontierSet};
use crate::authority::certificate::MajorityCertificate;
use crate::control_plane::system_namespace::SystemNamespace;
use crate::error::CrdtError;
use crate::hlc::{Hlc, HlcTimestamp};
use crate::store::kv::{CrdtValue, Store};
use crate::types::{CertificationStatus, KeyRange, NodeId, PolicyVersion};

/// What to do when `certified_write` cannot achieve consensus.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnTimeout {
    /// Return `CrdtError::Timeout`.
    Error,
    /// Accept the write as `Pending` and let the caller poll status later.
    Pending,
}

/// A verifiable proof bundle attached to a certified read response.
///
/// Contains the metadata needed for an external client to independently
/// verify that a majority of authorities have acknowledged a given frontier.
/// The `certificate` field will carry cryptographic signatures once the
/// full signing pipeline is implemented.
#[derive(Debug, Clone, Serialize)]
pub struct ProofBundle {
    /// The key range this proof covers.
    pub key_range: KeyRange,
    /// The majority frontier HLC at the time of certification.
    pub frontier_hlc: HlcTimestamp,
    /// The policy version in effect when the proof was generated.
    pub policy_version: PolicyVersion,
    /// The authority node IDs that have reported frontiers for this scope.
    pub contributing_authorities: Vec<NodeId>,
    /// The total number of authorities in the authority set for this key range.
    pub total_authorities: usize,
    /// The majority certificate, if available (future: full signing pipeline).
    pub certificate: Option<MajorityCertificate>,
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
    /// The HLC timestamp of the write that was certified.
    pub write_timestamp: HlcTimestamp,
}

/// Maximum number of entries in the certified proof cache before eviction.
const MAX_CERTIFIED_CACHE: usize = 10_000;

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
        }
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
        let scoped_frontiers = self
            .frontiers
            .all_for_scope(&pw.key_range, &pw.policy_version);
        let contributing_authorities: Vec<NodeId> = scoped_frontiers
            .iter()
            .map(|f| f.authority_id.clone())
            .collect();

        let frontier_hlc = self
            .frontiers
            .majority_frontier_for_scope(&pw.key_range, &pw.policy_version, pw.total_authorities)
            .unwrap_or_else(|| pw.timestamp.clone());

        self.certified_cache.insert(
            pw.key.clone(),
            CertifiedCacheEntry {
                key_range: pw.key_range.clone(),
                frontier_hlc,
                policy_version: pw.policy_version,
                contributing_authorities,
                total_authorities: pw.total_authorities,
                certificate: None,
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
                // Build proof from live frontier data.
                let proof = scope_info.as_ref().and_then(|(kr, pv, total)| {
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
                    })
                });
                (CertificationStatus::Certified, proof)
            }
            Some(other_status) => (other_status, None),
            None => {
                // No pending write — check the certified cache.
                if let Some(cached) = self.certified_cache.get(key) {
                    let proof = ProofBundle {
                        key_range: cached.key_range.clone(),
                        frontier_hlc: cached.frontier_hlc.clone(),
                        policy_version: cached.policy_version,
                        contributing_authorities: cached.contributing_authorities.clone(),
                        total_authorities: cached.total_authorities,
                        certificate: cached.certificate.clone(),
                    };
                    (CertificationStatus::Certified, Some(proof))
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
    /// - `OnTimeout::Error` — returns `Err(CrdtError::Timeout)`.
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

        let timestamp = self.clock.now();

        // Write to the local store (eventual consistency path).
        self.store.put(key.clone(), value.clone());

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
            key,
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
            OnTimeout::Error => Err(CrdtError::Timeout),
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

    /// Update an Authority's ack frontier.
    ///
    /// Simulates receiving an ack from an Authority node. Returns `true` if
    /// the frontier was actually advanced, `false` if the update was
    /// stale or duplicate.
    pub fn update_frontier(&mut self, frontier: AckFrontier) -> bool {
        self.frontiers.update(frontier)
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
    }

    /// Check whether a (key_range, policy_version) pair has been fenced.
    pub fn is_version_fenced(&self, range: &KeyRange, version: &PolicyVersion) -> bool {
        self.frontiers.is_version_fenced(range, version)
    }

    /// Run garbage collection on stale frontier entries.
    ///
    /// Delegates to [`AckFrontierSet::gc_stale_entries`]. Returns the number
    /// of frontier entries removed.
    pub fn gc_frontier_entries(
        &mut self,
        current_policy_version: PolicyVersion,
        max_retained_versions: u64,
        grace_period_secs: u64,
        now_secs: u64,
    ) -> usize {
        self.frontiers.gc_stale_entries(
            current_policy_version,
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
        ));
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

        assert_eq!(result.unwrap_err(), CrdtError::Timeout);
        // The write should still be tracked as pending.
        assert_eq!(api.pending_writes().len(), 1);
        assert_eq!(api.pending_writes()[0].status, CertificationStatus::Pending);
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
        ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(1), kr("user/"), 3));
        ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(1), kr("order/"), 3));

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
        ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(1), kr("user/"), 3));
        ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(1), kr("order/"), 3));

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
        );

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
        ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(1), kr("user/"), 3));
        ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(1), kr("user/vip/"), 2));

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
        ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(1), kr("cert/"), 3));
        ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(1), kr("stale/"), 3));

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
            "certificate not yet implemented"
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
        ns.set_placement_policy(PlacementPolicy::new(PolicyVersion(1), kr("data/"), 3));

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
}
