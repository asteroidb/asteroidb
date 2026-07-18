//! In-memory pool of verified frontier attestations (FR-008).
//!
//! Collects [`VerifiedAttestation`] values per `(key_range, policy_version)`
//! scope and checkpoint, and assembles them into majority certificates once
//! enough distinct authorities have signed the same checkpoint message.

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::authority::certificate::{
    AuthoritySignature, DualModeCertificate, KeysetVersion, MajorityCertificate,
};
use crate::authority::frontier_sig::VerifiedAttestation;
use crate::hlc::HlcTimestamp;
use crate::types::{KeyRange, NodeId, PolicyVersion};

#[cfg(feature = "native-crypto")]
use crate::authority::bls;

/// Maximum number of checkpoints retained per scope (~128 seconds of history
/// at the default 1s checkpoint interval). Oldest checkpoints are pruned
/// first when the limit is exceeded.
///
/// This window is deliberately wider than [`MAX_CHECKPOINT_FUTURE_SKEW_MS`]:
/// even if an authority fills every future bucket the skew guard allows, a
/// majority of remaining buckets is still available for honest checkpoints,
/// so current-time attestations can never be evicted by a future-bucket flood.
const MAX_CHECKPOINTS_PER_SCOPE: usize = 128;

/// Maximum tolerated clock skew for attestation checkpoints, in milliseconds.
///
/// Attestations whose checkpoint lies further than this ahead of the local
/// wall clock are rejected on insert. Without this cap, a single authority
/// (malicious or with a badly skewed clock) could fill the per-scope
/// checkpoint window with far-future buckets; every honest current-time
/// attestation would then be the oldest bucket and be pruned immediately
/// after insertion, permanently preventing certificate assembly.
///
/// Matches the HLC's `MAX_CLOCK_SKEW_MS` (60 s) so the signing pipeline and
/// the clock share a single skew policy.
pub const MAX_CHECKPOINT_FUTURE_SKEW_MS: u64 = 60_000;

/// Hard cap on tracked scopes. Same value as
/// [`equivocation::MAX_TRACKED_SCOPES`](crate::authority::equivocation::MAX_TRACKED_SCOPES)
/// — the two files share the system-wide scale assumption; raise both
/// together when deploying more ranges. Legitimate concurrent scopes =
/// (defined ranges R) x (<= LAG+LEAD+1 = 4 policy versions admitted by the
/// `CertifiedApi` version window), so R <= 256 is unaffected. Memory
/// ceiling: 1024 scopes x 128 checkpoints x A authorities x O(hundreds of
/// bytes)/attestation.
///
/// Unlike the equivocation detector (which evicts LRU scopes because a
/// missed detection is lost forever), the pool REJECTS the incoming scope
/// when the cap is hit: resident attestations must never be flushed by a
/// flood (see [`MAX_CHECKPOINTS_PER_SCOPE`] rationale), and evicted past
/// checkpoints could never be re-collected — `FrontierReporter` only
/// generates current-time frontiers.
pub const MAX_POOL_SCOPES: usize = 1024;

/// Per-authority cap on scopes containing that authority's attestations.
/// Same value as
/// [`equivocation::MAX_TRACKED_SCOPES_PER_AUTHORITY`](crate::authority::equivocation::MAX_TRACKED_SCOPES_PER_AUTHORITY).
/// Legitimate presence = (ranges the authority belongs to) x <= 4 versions,
/// so <= 16 ranges per authority is unaffected. NOTE: presence is counted at
/// scope granularity and only returned when the scope itself is removed
/// (over-approximation, errs on the strict side).
pub const MAX_POOL_SCOPES_PER_AUTHORITY: usize = 64;

/// Scope key for attestation grouping.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PoolScope {
    key_range: KeyRange,
    policy_version: PolicyVersion,
}

/// Outcome of [`AttestationPool::insert`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOutcome {
    /// The attestation was recorded.
    Inserted,
    /// Rejected: checkpoint further than [`MAX_CHECKPOINT_FUTURE_SKEW_MS`]
    /// in the future.
    RejectedFutureSkew,
    /// Rejected: the pool already tracks [`MAX_POOL_SCOPES`] scopes and the
    /// attestation would create a new one (residents are never evicted).
    RejectedScopeCap,
    /// Rejected: the authority already has attestations in
    /// [`MAX_POOL_SCOPES_PER_AUTHORITY`] scopes and the attestation would
    /// create a new one.
    RejectedAuthorityCap,
}

/// Collects verified attestations and assembles majority certificates.
///
/// Non-persistent: the pool only holds recent checkpoints, and certificates
/// can always be rebuilt from fresh frontier reports.
///
/// # Invariants (`authority_scopes` presence index)
///
/// 1. Every scope referenced by `authority_scopes` exists in `entries`
///    (no dangling references — otherwise fenced/GC'd versions would
///    permanently occupy per-authority slots).
/// 2. If any checkpoint bucket of `entries[S]` holds an entry for authority
///    `a`, then `S ∈ authority_scopes[a]`. The reverse may over-approximate
///    at bucket granularity: presence is only released when the *scope* is
///    removed (per-bucket pruning does not decrement), which errs on the
///    strict side of the per-authority cap.
#[derive(Debug, Default)]
pub struct AttestationPool {
    /// scope -> checkpoint physical (ms) -> authority -> attestation.
    entries: HashMap<PoolScope, BTreeMap<u64, HashMap<NodeId, VerifiedAttestation>>>,
    /// authority -> scopes that contain (or contained) its attestations.
    /// Added on insert; removed only when a scope is removed (`gc_scope` /
    /// `retain_scopes` / `purge_authority`). See the invariants above.
    authority_scopes: HashMap<NodeId, HashSet<PoolScope>>,
    /// Cumulative inserts rejected by the global scope cap.
    rejected_scope_cap_total: u64,
    /// Cumulative inserts rejected by the per-authority scope cap.
    rejected_authority_cap_total: u64,
    /// Cumulative attestations removed by [`Self::purge_authority`].
    purged_attestations_total: u64,
}

impl AttestationPool {
    /// Create an empty pool.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a verified attestation for a scope.
    ///
    /// Idempotent per `(checkpoint, authority)`: a later attestation from the
    /// same authority for the same checkpoint overwrites the earlier one, so
    /// duplicate signers can never inflate the majority count. Old
    /// checkpoints beyond [`MAX_CHECKPOINTS_PER_SCOPE`] are pruned.
    ///
    /// `now_ms` is the local wall clock: attestations whose checkpoint is
    /// more than [`MAX_CHECKPOINT_FUTURE_SKEW_MS`] in the future are rejected
    /// so a skewed or malicious authority cannot flood the checkpoint window
    /// with future buckets and evict honest attestations.
    ///
    /// Capacity: inserting into an *existing* scope is always admitted (a
    /// signer joining an in-progress quorum must never be blocked), but an
    /// insert that would create a new scope is rejected when the authority's
    /// presence cap ([`MAX_POOL_SCOPES_PER_AUTHORITY`], checked first so an
    /// over-cap authority cannot trip the global rejection for everyone
    /// else) or the global scope cap ([`MAX_POOL_SCOPES`]) is reached.
    /// Residents are never evicted to make room.
    pub fn insert(
        &mut self,
        key_range: &KeyRange,
        policy_version: PolicyVersion,
        attestation: VerifiedAttestation,
        now_ms: u64,
    ) -> InsertOutcome {
        if attestation.checkpoint_hlc.physical
            > now_ms.saturating_add(MAX_CHECKPOINT_FUTURE_SKEW_MS)
        {
            tracing::warn!(
                authority = %attestation.authority_id.0,
                checkpoint_ms = attestation.checkpoint_hlc.physical,
                now_ms,
                "rejecting attestation with far-future checkpoint (clock skew)"
            );
            return InsertOutcome::RejectedFutureSkew;
        }
        let scope = PoolScope {
            key_range: key_range.clone(),
            policy_version,
        };
        if !self.entries.contains_key(&scope) {
            // New scope: enforce the per-authority cap before the global cap
            // (same order as equivocation::make_room_for_scope — an authority
            // beyond its fairness share must not consume the global budget).
            let own_scopes = self
                .authority_scopes
                .get(&attestation.authority_id)
                .map_or(0, HashSet::len);
            if own_scopes >= MAX_POOL_SCOPES_PER_AUTHORITY {
                self.rejected_authority_cap_total += 1;
                return InsertOutcome::RejectedAuthorityCap;
            }
            if self.entries.len() >= MAX_POOL_SCOPES {
                self.rejected_scope_cap_total += 1;
                return InsertOutcome::RejectedScopeCap;
            }
        }
        self.authority_scopes
            .entry(attestation.authority_id.clone())
            .or_default()
            .insert(scope.clone());
        let checkpoints = self.entries.entry(scope).or_default();
        checkpoints
            .entry(attestation.checkpoint_hlc.physical)
            .or_default()
            .insert(attestation.authority_id.clone(), attestation);

        while checkpoints.len() > MAX_CHECKPOINTS_PER_SCOPE {
            checkpoints.pop_first();
        }
        InsertOutcome::Inserted
    }

    /// Remove `scope` from every authority's presence set.
    ///
    /// Must be called whenever a scope is removed from `entries`, so that
    /// per-authority slots are returned (invariant 1 above).
    fn detach_scope_from_authorities(&mut self, scope: &PoolScope) {
        self.authority_scopes.retain(|_, scopes| {
            scopes.remove(scope);
            !scopes.is_empty()
        });
    }

    /// Remove every attestation of `authority` from the pool (m-7: an
    /// accused authority's pooled attestations must not be consumed by later
    /// certificate assembly).
    ///
    /// Surgical: co-signers in shared scopes are untouched. Scopes and
    /// checkpoint buckets left empty are dropped, and every removed scope is
    /// detached from all presence sets. The authority's own presence entry
    /// is removed entirely — attestations re-inserted after the purge count
    /// as fresh presence. Idempotent. Returns the number of attestations
    /// removed.
    pub fn purge_authority(&mut self, authority: &NodeId) -> usize {
        let Some(scopes) = self.authority_scopes.remove(authority) else {
            return 0;
        };
        let mut removed = 0usize;
        let mut emptied: Vec<PoolScope> = Vec::new();
        for scope in &scopes {
            let Some(checkpoints) = self.entries.get_mut(scope) else {
                continue;
            };
            checkpoints.retain(|_, atts| {
                if atts.remove(authority).is_some() {
                    removed += 1;
                }
                !atts.is_empty()
            });
            if checkpoints.is_empty() {
                emptied.push(scope.clone());
            }
        }
        for scope in &emptied {
            self.entries.remove(scope);
            self.detach_scope_from_authorities(scope);
        }
        self.purged_attestations_total += removed as u64;
        removed
    }

    /// Drop every scope for which `keep` returns `false`, returning the
    /// number of scopes removed.
    ///
    /// Used as the cap-pressure fallback: when an insert is rejected by a
    /// capacity cap, the caller sweeps out scopes that can no longer be
    /// consumed by certificate assembly (fenced / stale policy versions,
    /// removed ranges) and retries.
    pub fn retain_scopes<F: Fn(&KeyRange, PolicyVersion) -> bool>(&mut self, keep: F) -> usize {
        let victims: Vec<PoolScope> = self
            .entries
            .keys()
            .filter(|s| !keep(&s.key_range, s.policy_version))
            .cloned()
            .collect();
        for scope in &victims {
            self.entries.remove(scope);
            self.detach_scope_from_authorities(scope);
        }
        victims.len()
    }

    /// Whether any attestations are recorded for a scope.
    ///
    /// Used to decide if certificate back-fill retries are worthwhile: in
    /// unsigned deployments no attestation ever arrives, so keys must not be
    /// queued for perpetual (and futile) certificate assembly retries.
    pub fn has_attestations(&self, key_range: &KeyRange, policy_version: &PolicyVersion) -> bool {
        self.entries
            .get(&PoolScope {
                key_range: key_range.clone(),
                policy_version: *policy_version,
            })
            .is_some_and(|checkpoints| !checkpoints.is_empty())
    }

    /// Assemble certificates for the newest checkpoint `C` satisfying
    /// `min_ts <= C` with at least `total_authorities / 2 + 1` distinct
    /// Ed25519 signers.
    ///
    /// Returns `(C, ed25519_certificate, optional_bls_certificate)`.
    /// The BLS certificate is attached when, at the same checkpoint, a
    /// majority of attestations carry BLS signatures under a *uniform*
    /// keyset version (required because the aggregate verifies against a
    /// single registry keyset). Returns `None` if no checkpoint qualifies.
    pub fn build_certificates(
        &self,
        key_range: &KeyRange,
        policy_version: PolicyVersion,
        total_authorities: usize,
        min_ts: &HlcTimestamp,
    ) -> Option<(
        HlcTimestamp,
        MajorityCertificate,
        Option<DualModeCertificate>,
    )> {
        let scope = PoolScope {
            key_range: key_range.clone(),
            policy_version,
        };
        let checkpoints = self.entries.get(&scope)?;
        let threshold = total_authorities / 2 + 1;

        for (physical, atts) in checkpoints.iter().rev() {
            let checkpoint = HlcTimestamp {
                physical: *physical,
                logical: 0,
                node_id: String::new(),
            };
            if *min_ts > checkpoint {
                // Checkpoints are iterated newest-first; older ones only
                // get further below min_ts, so we can stop here.
                break;
            }
            if atts.len() < threshold {
                continue;
            }

            // Deterministic signer order for stable certificates.
            let mut sorted: Vec<&VerifiedAttestation> = atts.values().collect();
            sorted.sort_by(|a, b| a.authority_id.0.cmp(&b.authority_id.0));

            // Certificate-level keyset version: the maximum among signatures.
            // Per-signature verification uses each signature's own version.
            let cert_keyset = sorted
                .iter()
                .map(|a| a.keyset_version.clone())
                .max()
                .unwrap_or(KeysetVersion(1));

            let mut cert = MajorityCertificate::new(
                key_range.clone(),
                checkpoint.clone(),
                policy_version,
                cert_keyset,
            );
            for att in &sorted {
                cert.add_signature(AuthoritySignature {
                    authority_id: att.authority_id.clone(),
                    public_key: att.ed25519.0,
                    signature: att.ed25519.1,
                    keyset_version: att.keyset_version.clone(),
                });
            }

            let bls_cert = Self::build_bls_certificate(
                key_range,
                policy_version,
                &checkpoint,
                &sorted,
                threshold,
            );

            return Some((checkpoint, cert, bls_cert));
        }
        None
    }

    /// Assemble a BLS aggregate certificate from attestations at one checkpoint.
    #[cfg(feature = "native-crypto")]
    fn build_bls_certificate(
        key_range: &KeyRange,
        policy_version: PolicyVersion,
        checkpoint: &HlcTimestamp,
        sorted: &[&VerifiedAttestation],
        threshold: usize,
    ) -> Option<DualModeCertificate> {
        // Group BLS-capable attestations by keyset version; the aggregate
        // must verify against a single keyset in the registry.
        let mut by_keyset: BTreeMap<u64, Vec<&VerifiedAttestation>> = BTreeMap::new();
        for att in sorted {
            if att.bls.is_some() {
                by_keyset.entry(att.keyset_version.0).or_default().push(att);
            }
        }
        // Prefer the newest keyset version that reaches the threshold.
        let (keyset, group) = by_keyset
            .into_iter()
            .rev()
            .find(|(_, group)| group.len() >= threshold)?;

        let mut signers = Vec::with_capacity(group.len());
        let mut sigs = Vec::with_capacity(group.len());
        for att in &group {
            let (pk, sig) = att.bls.as_ref()?;
            signers.push((att.authority_id.clone(), pk.clone()));
            sigs.push(sig.clone());
        }
        let aggregated = bls::aggregate_signatures(&sigs).ok()?;

        let mut cert = DualModeCertificate::new_bls(
            key_range.clone(),
            checkpoint.clone(),
            policy_version,
            KeysetVersion(keyset),
        );
        cert.set_bls_aggregate(signers, aggregated);
        Some(cert)
    }

    /// BLS assembly stub when native-crypto is disabled: attestations never
    /// carry BLS material, so no BLS certificate can be built.
    #[cfg(not(feature = "native-crypto"))]
    fn build_bls_certificate(
        _key_range: &KeyRange,
        _policy_version: PolicyVersion,
        _checkpoint: &HlcTimestamp,
        _sorted: &[&VerifiedAttestation],
        _threshold: usize,
    ) -> Option<DualModeCertificate> {
        None
    }

    /// Drop all attestations for a scope (fence / GC hook, FR-009).
    ///
    /// Also returns the scope's per-authority presence slots: without this,
    /// fenced versions would permanently occupy slots and long-running
    /// honest authorities would eventually hit
    /// [`MAX_POOL_SCOPES_PER_AUTHORITY`] through organic version churn.
    pub fn gc_scope(&mut self, key_range: &KeyRange, policy_version: &PolicyVersion) {
        let scope = PoolScope {
            key_range: key_range.clone(),
            policy_version: *policy_version,
        };
        if self.entries.remove(&scope).is_some() {
            self.detach_scope_from_authorities(&scope);
        }
    }

    /// Return the number of tracked scopes (for tests and diagnostics).
    pub fn scope_count(&self) -> usize {
        self.entries.len()
    }

    /// Cumulative inserts rejected by the global scope cap.
    pub fn rejected_scope_cap_total(&self) -> u64 {
        self.rejected_scope_cap_total
    }

    /// Cumulative inserts rejected by the per-authority scope cap.
    pub fn rejected_authority_cap_total(&self) -> u64 {
        self.rejected_authority_cap_total
    }

    /// Cumulative attestations removed by [`Self::purge_authority`].
    pub fn purged_attestations_total(&self) -> u64 {
        self.purged_attestations_total
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authority::ack_frontier::AckFrontier;
    use crate::authority::certificate::create_certificate_message;
    #[cfg(feature = "native-crypto")]
    use crate::authority::certificate::{EpochConfig, KeysetRegistry};
    #[cfg(feature = "native-crypto")]
    use crate::authority::frontier_sig::verify_frontier_signature;
    use crate::authority::frontier_sig::{CHECKPOINT_INTERVAL_MS, NodeSigner};
    use crate::types::NodeId;

    /// Fixed "wall clock" for tests: comfortably after every synthetic
    /// timestamp used below, so the future-skew guard never interferes
    /// unless a test exercises it deliberately.
    const TEST_NOW: u64 = 200_000;

    fn node(name: &str) -> NodeId {
        NodeId(name.into())
    }

    fn kr(prefix: &str) -> KeyRange {
        KeyRange {
            prefix: prefix.into(),
        }
    }

    fn seed(byte: u8) -> [u8; 32] {
        let mut s = [0u8; 32];
        s[0] = byte;
        s
    }

    #[cfg(feature = "native-crypto")]
    fn make_signer(name: &str, byte: u8, bls: bool) -> NodeSigner {
        NodeSigner::from_seed(node(name), &seed(byte), bls)
    }

    #[cfg(not(feature = "native-crypto"))]
    fn make_signer(name: &str, byte: u8, _bls: bool) -> NodeSigner {
        NodeSigner::from_seed(node(name), &seed(byte))
    }

    fn make_frontier(authority: &str, physical: u64) -> AckFrontier {
        AckFrontier {
            authority_id: node(authority),
            frontier_hlc: HlcTimestamp {
                physical,
                logical: 0,
                node_id: authority.into(),
            },
            key_range: kr("user/"),
            policy_version: PolicyVersion(1),
            digest_hash: format!("{authority}-{physical}"),
        }
    }

    /// Produce a self-verified attestation for one authority at a timestamp.
    fn attest(signer: &NodeSigner, physical: u64) -> VerifiedAttestation {
        let frontier = make_frontier(signer.node_id().0.as_str(), physical);
        let sig = signer.sign_frontier(&frontier, KeysetVersion(1));
        signer.self_verified(&frontier, &sig)
    }

    fn write_ts(physical: u64) -> HlcTimestamp {
        HlcTimestamp {
            physical,
            logical: 0,
            node_id: "writer".into(),
        }
    }

    #[test]
    fn builds_ed25519_certificate_at_majority() {
        let s1 = make_signer("auth-1", 1, false);
        let s2 = make_signer("auth-2", 2, false);
        let mut pool = AttestationPool::new();

        pool.insert(
            &kr("user/"),
            PolicyVersion(1),
            attest(&s1, 10_500),
            TEST_NOW,
        );
        // Only 1 of 3: no majority.
        assert!(
            pool.build_certificates(&kr("user/"), PolicyVersion(1), 3, &write_ts(9_000))
                .is_none()
        );

        pool.insert(
            &kr("user/"),
            PolicyVersion(1),
            attest(&s2, 10_700),
            TEST_NOW,
        );
        let (checkpoint, cert, _bls) = pool
            .build_certificates(&kr("user/"), PolicyVersion(1), 3, &write_ts(9_000))
            .expect("2 of 3 must reach majority");

        assert_eq!(checkpoint.physical, 10_000);
        assert!(cert.has_majority(3));
        assert_eq!(cert.signature_count(), 2);

        // The assembled certificate verifies against the checkpoint message.
        let message = create_certificate_message(&kr("user/"), &checkpoint, &PolicyVersion(1));
        assert!(cert.verify_signatures(&message).is_ok());
    }

    #[test]
    fn duplicate_authority_counted_once() {
        let s1 = make_signer("auth-1", 3, false);
        let mut pool = AttestationPool::new();
        pool.insert(
            &kr("user/"),
            PolicyVersion(1),
            attest(&s1, 10_100),
            TEST_NOW,
        );
        pool.insert(
            &kr("user/"),
            PolicyVersion(1),
            attest(&s1, 10_200),
            TEST_NOW,
        );

        // Two inserts from the same authority in the same bucket: still 1 signer.
        assert!(
            pool.build_certificates(&kr("user/"), PolicyVersion(1), 3, &write_ts(9_000))
                .is_none()
        );
    }

    #[test]
    fn selects_latest_checkpoint_meeting_min_ts() {
        let s1 = make_signer("auth-1", 4, false);
        let s2 = make_signer("auth-2", 5, false);
        let mut pool = AttestationPool::new();

        // Both signed checkpoints 10_000 and 12_000.
        for phys in [10_500, 12_500] {
            pool.insert(&kr("user/"), PolicyVersion(1), attest(&s1, phys), TEST_NOW);
            pool.insert(&kr("user/"), PolicyVersion(1), attest(&s2, phys), TEST_NOW);
        }

        let (checkpoint, _, _) = pool
            .build_certificates(&kr("user/"), PolicyVersion(1), 3, &write_ts(9_000))
            .unwrap();
        assert_eq!(
            checkpoint.physical, 12_000,
            "the newest qualifying checkpoint must be selected"
        );

        // A write between the checkpoints only qualifies for the newer one.
        let (checkpoint, _, _) = pool
            .build_certificates(&kr("user/"), PolicyVersion(1), 3, &write_ts(11_000))
            .unwrap();
        assert_eq!(checkpoint.physical, 12_000);

        // A write beyond all checkpoints yields nothing.
        assert!(
            pool.build_certificates(&kr("user/"), PolicyVersion(1), 3, &write_ts(13_000))
                .is_none()
        );
    }

    #[test]
    fn replayed_old_attestation_cannot_advance_certificate() {
        let s1 = make_signer("auth-1", 6, false);
        let s2 = make_signer("auth-2", 7, false);
        let mut pool = AttestationPool::new();

        pool.insert(
            &kr("user/"),
            PolicyVersion(1),
            attest(&s1, 50_000),
            TEST_NOW,
        );
        pool.insert(
            &kr("user/"),
            PolicyVersion(1),
            attest(&s2, 50_000),
            TEST_NOW,
        );

        // Replaying the same (old) attestations later cannot certify newer writes.
        pool.insert(
            &kr("user/"),
            PolicyVersion(1),
            attest(&s1, 50_000),
            TEST_NOW,
        );
        assert!(
            pool.build_certificates(&kr("user/"), PolicyVersion(1), 3, &write_ts(51_000))
                .is_none(),
            "replayed old attestations must not certify newer writes"
        );
    }

    #[test]
    fn old_checkpoints_are_pruned() {
        let s1 = make_signer("auth-1", 8, false);
        let mut pool = AttestationPool::new();
        for i in 0..(MAX_CHECKPOINTS_PER_SCOPE as u64 + 8) {
            pool.insert(
                &kr("user/"),
                PolicyVersion(1),
                attest(&s1, (i + 1) * CHECKPOINT_INTERVAL_MS),
                TEST_NOW,
            );
        }
        let scope = PoolScope {
            key_range: kr("user/"),
            policy_version: PolicyVersion(1),
        };
        let checkpoints = pool.entries.get(&scope).unwrap();
        assert_eq!(checkpoints.len(), MAX_CHECKPOINTS_PER_SCOPE);
        // The oldest buckets were dropped.
        assert!(!checkpoints.contains_key(&CHECKPOINT_INTERVAL_MS));
    }

    #[test]
    fn gc_scope_drops_attestations() {
        let s1 = make_signer("auth-1", 9, false);
        let mut pool = AttestationPool::new();
        pool.insert(
            &kr("user/"),
            PolicyVersion(1),
            attest(&s1, 10_000),
            TEST_NOW,
        );
        assert_eq!(pool.scope_count(), 1);
        pool.gc_scope(&kr("user/"), &PolicyVersion(1));
        assert_eq!(pool.scope_count(), 0);
    }

    #[test]
    fn scopes_are_isolated() {
        let s1 = make_signer("auth-1", 10, false);
        let s2 = make_signer("auth-2", 11, false);
        let mut pool = AttestationPool::new();
        pool.insert(
            &kr("user/"),
            PolicyVersion(1),
            attest(&s1, 10_500),
            TEST_NOW,
        );
        pool.insert(
            &kr("order/"),
            PolicyVersion(1),
            attest(&s2, 10_500),
            TEST_NOW,
        );

        // Attestations from different scopes must not combine.
        assert!(
            pool.build_certificates(&kr("user/"), PolicyVersion(1), 3, &write_ts(9_000))
                .is_none()
        );
    }

    #[test]
    fn far_future_attestation_is_rejected() {
        let s1 = make_signer("auth-1", 30, false);
        let mut pool = AttestationPool::new();

        // Just inside the skew allowance: accepted.
        let inside = TEST_NOW + MAX_CHECKPOINT_FUTURE_SKEW_MS;
        assert_eq!(
            pool.insert(
                &kr("user/"),
                PolicyVersion(1),
                attest(&s1, inside),
                TEST_NOW
            ),
            InsertOutcome::Inserted
        );

        // Beyond the allowance: rejected, and no bucket is created.
        let outside = TEST_NOW + MAX_CHECKPOINT_FUTURE_SKEW_MS + CHECKPOINT_INTERVAL_MS;
        assert_eq!(
            pool.insert(
                &kr("user/"),
                PolicyVersion(1),
                attest(&s1, outside),
                TEST_NOW
            ),
            InsertOutcome::RejectedFutureSkew
        );
        let scope = PoolScope {
            key_range: kr("user/"),
            policy_version: PolicyVersion(1),
        };
        assert_eq!(pool.entries.get(&scope).unwrap().len(), 1);
    }

    #[test]
    fn future_bucket_flood_cannot_evict_current_majority() {
        // A single skewed/malicious authority pushes attestations for every
        // future bucket the skew guard allows. Honest current-time
        // attestations must still accumulate a majority and certify.
        let attacker = make_signer("auth-1", 31, false);
        let s2 = make_signer("auth-2", 32, false);
        let s3 = make_signer("auth-3", 33, false);
        let mut pool = AttestationPool::new();

        // Attempt to fill far beyond the window; only buckets within the
        // skew allowance are admitted.
        for i in 1..=(2 * MAX_CHECKPOINTS_PER_SCOPE as u64) {
            pool.insert(
                &kr("user/"),
                PolicyVersion(1),
                attest(&attacker, TEST_NOW + i * CHECKPOINT_INTERVAL_MS),
                TEST_NOW,
            );
        }

        // Two honest authorities attest the current checkpoint.
        assert_eq!(
            pool.insert(
                &kr("user/"),
                PolicyVersion(1),
                attest(&s2, TEST_NOW),
                TEST_NOW
            ),
            InsertOutcome::Inserted
        );
        assert_eq!(
            pool.insert(
                &kr("user/"),
                PolicyVersion(1),
                attest(&s3, TEST_NOW),
                TEST_NOW
            ),
            InsertOutcome::Inserted
        );

        let (checkpoint, cert, _) = pool
            .build_certificates(&kr("user/"), PolicyVersion(1), 3, &write_ts(TEST_NOW - 500))
            .expect("honest majority must certify despite the future-bucket flood");
        assert_eq!(
            checkpoint.physical,
            TEST_NOW - TEST_NOW % CHECKPOINT_INTERVAL_MS
        );
        assert!(cert.has_majority(3));
    }

    #[test]
    fn has_attestations_reflects_pool_contents() {
        let s1 = make_signer("auth-1", 34, false);
        let mut pool = AttestationPool::new();
        assert!(!pool.has_attestations(&kr("user/"), &PolicyVersion(1)));
        pool.insert(
            &kr("user/"),
            PolicyVersion(1),
            attest(&s1, 10_500),
            TEST_NOW,
        );
        assert!(pool.has_attestations(&kr("user/"), &PolicyVersion(1)));
        assert!(!pool.has_attestations(&kr("order/"), &PolicyVersion(1)));
        pool.gc_scope(&kr("user/"), &PolicyVersion(1));
        assert!(!pool.has_attestations(&kr("user/"), &PolicyVersion(1)));
    }

    #[cfg(feature = "native-crypto")]
    #[test]
    fn bls_aggregate_certificate_verifies() {
        let s1 = make_signer("auth-1", 12, true);
        let s2 = make_signer("auth-2", 13, true);
        let s3 = make_signer("auth-3", 14, true);

        let mut registry = KeysetRegistry::new();
        registry
            .register_keyset(
                KeysetVersion(1),
                0,
                vec![
                    (node("auth-1"), s1.verifying_key()),
                    (node("auth-2"), s2.verifying_key()),
                    (node("auth-3"), s3.verifying_key()),
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
                    (
                        "auth-3".into(),
                        s3.bls_public_key().unwrap(),
                        s3.bls_proof_of_possession().unwrap(),
                    ),
                ],
            )
            .unwrap();

        let mut pool = AttestationPool::new();
        for signer in [&s1, &s2] {
            let frontier = make_frontier(signer.node_id().0.as_str(), 10_500);
            let sig = signer.sign_frontier(&frontier, KeysetVersion(1));
            // Route through real verification to mirror the receive path.
            let att =
                verify_frontier_signature(&frontier, &sig, &registry, 0, &EpochConfig::default())
                    .unwrap();
            pool.insert(&kr("user/"), PolicyVersion(1), att, TEST_NOW);
        }

        let (checkpoint, cert, bls_cert) = pool
            .build_certificates(&kr("user/"), PolicyVersion(1), 3, &write_ts(9_000))
            .unwrap();
        assert!(cert.has_majority(3));

        let bls_cert = bls_cert.expect("BLS majority must produce an aggregate certificate");
        assert!(bls_cert.has_majority(3));
        assert_eq!(bls_cert.frontier_hlc, checkpoint);

        let message = create_certificate_message(&kr("user/"), &checkpoint, &PolicyVersion(1));
        let signers = bls_cert
            .verify_with_registry(&message, &registry, 0, &EpochConfig::default())
            .expect("assembled BLS certificate must verify against the registry");
        assert_eq!(signers.len(), 2);
    }

    #[cfg(feature = "native-crypto")]
    #[test]
    fn bls_requires_uniform_keyset() {
        let s1 = make_signer("auth-1", 15, true);
        let s2 = make_signer("auth-2", 16, true);
        let mut pool = AttestationPool::new();

        let mut att1 = attest(&s1, 10_500);
        att1.keyset_version = KeysetVersion(1);
        let mut att2 = attest(&s2, 10_500);
        att2.keyset_version = KeysetVersion(2);

        pool.insert(&kr("user/"), PolicyVersion(1), att1, TEST_NOW);
        pool.insert(&kr("user/"), PolicyVersion(1), att2, TEST_NOW);

        let (_, cert, bls_cert) = pool
            .build_certificates(&kr("user/"), PolicyVersion(1), 3, &write_ts(9_000))
            .unwrap();
        assert!(cert.has_majority(3), "Ed25519 path tolerates mixed keysets");
        assert!(
            bls_cert.is_none(),
            "BLS aggregation requires a uniform keyset majority"
        );
    }

    #[cfg(feature = "native-crypto")]
    #[test]
    fn missing_bls_signer_prevents_bls_certificate() {
        // One BLS-capable and one Ed25519-only authority: Ed25519 majority
        // succeeds but BLS cannot reach the threshold.
        let s1 = make_signer("auth-1", 17, true);
        let s2 = make_signer("auth-2", 18, false);
        let mut pool = AttestationPool::new();
        pool.insert(
            &kr("user/"),
            PolicyVersion(1),
            attest(&s1, 10_500),
            TEST_NOW,
        );
        pool.insert(
            &kr("user/"),
            PolicyVersion(1),
            attest(&s2, 10_500),
            TEST_NOW,
        );

        let (_, cert, bls_cert) = pool
            .build_certificates(&kr("user/"), PolicyVersion(1), 3, &write_ts(9_000))
            .unwrap();
        assert!(cert.has_majority(3));
        assert!(bls_cert.is_none());
    }

    // ---------------------------------------------------------------
    // Capacity caps (M-4) and accusation purge (m-7)
    // ---------------------------------------------------------------

    #[test]
    fn purge_authority_removes_across_scopes_and_checkpoints() {
        let s1 = make_signer("auth-1", 40, false);
        let s2 = make_signer("auth-2", 41, false);
        let mut pool = AttestationPool::new();

        // auth-1 in two scopes x two checkpoint buckets; auth-2 co-signs one
        // bucket of the first scope only.
        for phys in [10_500, 12_500] {
            pool.insert(&kr("user/"), PolicyVersion(1), attest(&s1, phys), TEST_NOW);
            pool.insert(&kr("order/"), PolicyVersion(2), attest(&s1, phys), TEST_NOW);
        }
        pool.insert(
            &kr("user/"),
            PolicyVersion(1),
            attest(&s2, 10_500),
            TEST_NOW,
        );
        assert_eq!(pool.scope_count(), 2);

        let removed = pool.purge_authority(&node("auth-1"));
        assert_eq!(removed, 4, "both scopes and both buckets must be purged");
        assert_eq!(pool.purged_attestations_total(), 4);

        // The scope auth-1 occupied alone is dropped entirely; the shared
        // scope survives with only auth-2's bucket.
        assert_eq!(pool.scope_count(), 1);
        assert!(pool.has_attestations(&kr("user/"), &PolicyVersion(1)));
        assert!(!pool.has_attestations(&kr("order/"), &PolicyVersion(2)));

        // The purged signer never appears in later certificate assembly.
        let (_, cert, _) = pool
            .build_certificates(&kr("user/"), PolicyVersion(1), 1, &write_ts(9_000))
            .expect("auth-2 alone reaches the 1-of-1 threshold");
        assert!(!cert.signers().into_iter().any(|n| *n == node("auth-1")));

        // Idempotent.
        assert_eq!(pool.purge_authority(&node("auth-1")), 0);
        assert_eq!(pool.purged_attestations_total(), 4);

        // Re-insert after purge is fresh presence, not a stale slot.
        assert_eq!(
            pool.insert(
                &kr("user/"),
                PolicyVersion(1),
                attest(&s1, 12_500),
                TEST_NOW
            ),
            InsertOutcome::Inserted
        );
    }

    #[test]
    fn purge_is_surgical_in_shared_scope() {
        let s1 = make_signer("auth-1", 42, false);
        let s2 = make_signer("auth-2", 43, false);
        let s3 = make_signer("auth-3", 44, false);
        let mut pool = AttestationPool::new();
        for s in [&s1, &s2, &s3] {
            pool.insert(&kr("user/"), PolicyVersion(1), attest(s, 10_500), TEST_NOW);
        }

        assert_eq!(pool.purge_authority(&node("auth-1")), 1);

        // The honest co-signers' quorum survives and still certifies.
        let (_, cert, _) = pool
            .build_certificates(&kr("user/"), PolicyVersion(1), 3, &write_ts(9_000))
            .expect("honest 2-of-3 majority must survive the purge");
        assert!(cert.has_majority(3));
        assert!(!cert.signers().into_iter().any(|n| *n == node("auth-1")));
    }

    #[test]
    fn global_cap_rejects_new_scope_keeps_residents() {
        let honest1 = make_signer("auth-h1", 45, false);
        let honest2 = make_signer("auth-h2", 46, false);
        let mut pool = AttestationPool::new();

        // Resident quorum in one scope.
        for s in [&honest1, &honest2] {
            assert_eq!(
                pool.insert(&kr("user/"), PolicyVersion(1), attest(s, 10_500), TEST_NOW),
                InsertOutcome::Inserted
            );
        }

        // Fill the pool to the global cap with distinct scopes from enough
        // authorities that no per-authority cap bites first.
        let fillers: Vec<NodeSigner> = (0..17)
            .map(|i| make_signer(&format!("filler-{i}"), 100 + i as u8, false))
            .collect();
        'fill: for (i, filler) in fillers.iter().enumerate() {
            let att = attest(filler, 10_500);
            for j in 0..MAX_POOL_SCOPES_PER_AUTHORITY {
                if pool.scope_count() >= MAX_POOL_SCOPES {
                    break 'fill;
                }
                assert_eq!(
                    pool.insert(
                        &kr(&format!("flood-{i}-{j}/")),
                        PolicyVersion(1),
                        att.clone(),
                        TEST_NOW
                    ),
                    InsertOutcome::Inserted
                );
            }
        }
        assert_eq!(pool.scope_count(), MAX_POOL_SCOPES);

        // One more NEW scope (from a fresh authority) is rejected...
        let fresh = make_signer("auth-fresh", 90, false);
        assert_eq!(
            pool.insert(
                &kr("one-more/"),
                PolicyVersion(1),
                attest(&fresh, 10_500),
                TEST_NOW
            ),
            InsertOutcome::RejectedScopeCap
        );
        assert_eq!(pool.rejected_scope_cap_total(), 1);
        assert_eq!(pool.scope_count(), MAX_POOL_SCOPES);

        // ...but joining an EXISTING scope is always admitted, and the
        // resident quorum still certifies.
        assert_eq!(
            pool.insert(
                &kr("user/"),
                PolicyVersion(1),
                attest(&fresh, 10_500),
                TEST_NOW
            ),
            InsertOutcome::Inserted
        );
        let (_, cert, _) = pool
            .build_certificates(&kr("user/"), PolicyVersion(1), 3, &write_ts(9_000))
            .expect("residents must never be evicted by cap pressure");
        assert!(cert.has_majority(3));
    }

    #[test]
    fn per_authority_cap_rejects_only_offender() {
        let attacker = make_signer("auth-bad", 47, false);
        let honest = make_signer("auth-good", 48, false);
        let mut pool = AttestationPool::new();

        // The honest authority creates one scope first.
        assert_eq!(
            pool.insert(
                &kr("user/"),
                PolicyVersion(1),
                attest(&honest, 10_500),
                TEST_NOW
            ),
            InsertOutcome::Inserted
        );

        let att = attest(&attacker, 10_500);
        for i in 0..MAX_POOL_SCOPES_PER_AUTHORITY {
            assert_eq!(
                pool.insert(
                    &kr(&format!("flood-{i}/")),
                    PolicyVersion(1),
                    att.clone(),
                    TEST_NOW
                ),
                InsertOutcome::Inserted
            );
        }
        // 65th new scope: rejected by the per-authority cap (not the global
        // one — the pool is nowhere near MAX_POOL_SCOPES).
        assert_eq!(
            pool.insert(
                &kr("flood-overflow/"),
                PolicyVersion(1),
                att.clone(),
                TEST_NOW
            ),
            InsertOutcome::RejectedAuthorityCap
        );
        assert_eq!(pool.rejected_authority_cap_total(), 1);

        // Other authorities are unaffected...
        assert_eq!(
            pool.insert(
                &kr("other/"),
                PolicyVersion(1),
                attest(&honest, 10_500),
                TEST_NOW
            ),
            InsertOutcome::Inserted
        );
        // ...and the capped authority can still JOIN existing scopes.
        assert_eq!(
            pool.insert(&kr("user/"), PolicyVersion(1), att, TEST_NOW),
            InsertOutcome::Inserted
        );
    }

    #[test]
    fn cap_flood_does_not_flush_resident_quorum() {
        // A single authority floods the pool with distinct new scopes while
        // an honest quorum keeps assembling in a resident scope. Analogue of
        // future_bucket_flood_cannot_evict_current_majority at the scope
        // (rather than checkpoint-bucket) level.
        let attacker = make_signer("auth-bad", 49, false);
        let s2 = make_signer("auth-2", 50, false);
        let s3 = make_signer("auth-3", 51, false);
        let mut pool = AttestationPool::new();

        for s in [&s2, &s3] {
            pool.insert(
                &kr("user/"),
                PolicyVersion(1),
                attest(s, TEST_NOW),
                TEST_NOW,
            );
        }

        let att = attest(&attacker, TEST_NOW);
        for i in 0..(4 * MAX_POOL_SCOPES_PER_AUTHORITY) {
            let _ = pool.insert(
                &kr(&format!("flood-{i}/")),
                PolicyVersion(i as u64 + 2),
                att.clone(),
                TEST_NOW,
            );
        }

        // The flood is confined to the attacker's fairness share.
        assert!(pool.scope_count() <= 1 + MAX_POOL_SCOPES_PER_AUTHORITY);
        assert!(pool.rejected_authority_cap_total() > 0);

        // The honest quorum still certifies.
        let (_, cert, _) = pool
            .build_certificates(&kr("user/"), PolicyVersion(1), 3, &write_ts(TEST_NOW - 500))
            .expect("honest majority must certify despite the scope flood");
        assert!(cert.has_majority(3));
    }

    #[test]
    fn retain_scopes_returns_capacity() {
        let s1 = make_signer("auth-1", 52, false);
        let mut pool = AttestationPool::new();

        let att = attest(&s1, 10_500);
        for i in 0..MAX_POOL_SCOPES_PER_AUTHORITY {
            assert_eq!(
                pool.insert(
                    &kr("user/"),
                    PolicyVersion(i as u64 + 1),
                    att.clone(),
                    TEST_NOW
                ),
                InsertOutcome::Inserted
            );
        }
        assert_eq!(
            pool.insert(
                &kr("user/"),
                PolicyVersion(MAX_POOL_SCOPES_PER_AUTHORITY as u64 + 1),
                att.clone(),
                TEST_NOW
            ),
            InsertOutcome::RejectedAuthorityCap
        );

        // Sweep out all but the two newest versions (stale-version cleanup).
        let removed = pool.retain_scopes(|_, pv| pv.0 > MAX_POOL_SCOPES_PER_AUTHORITY as u64 - 2);
        assert_eq!(removed, MAX_POOL_SCOPES_PER_AUTHORITY - 2);
        assert_eq!(pool.scope_count(), 2);

        // Capacity is returned: the previously rejected insert now succeeds.
        assert_eq!(
            pool.insert(
                &kr("user/"),
                PolicyVersion(MAX_POOL_SCOPES_PER_AUTHORITY as u64 + 1),
                att,
                TEST_NOW
            ),
            InsertOutcome::Inserted
        );
    }

    #[test]
    fn gc_scope_returns_authority_slots() {
        let s1 = make_signer("auth-1", 53, false);
        let mut pool = AttestationPool::new();

        let att = attest(&s1, 10_500);
        for i in 0..MAX_POOL_SCOPES_PER_AUTHORITY {
            pool.insert(
                &kr("user/"),
                PolicyVersion(i as u64 + 1),
                att.clone(),
                TEST_NOW,
            );
        }
        assert_eq!(
            pool.insert(&kr("user/"), PolicyVersion(999), att.clone(), TEST_NOW),
            InsertOutcome::RejectedAuthorityCap
        );

        // A fence GC of one of the authority's scopes frees a slot.
        pool.gc_scope(&kr("user/"), &PolicyVersion(1));
        assert_eq!(
            pool.insert(&kr("user/"), PolicyVersion(999), att, TEST_NOW),
            InsertOutcome::Inserted
        );
    }

    #[test]
    fn scope_flood_is_memory_bounded_even_without_admission_checks() {
        // Pool-level backstop for M-4: even if the CertifiedApi admission
        // layer were bypassed entirely, 100k distinct scopes from a handful
        // of authorities cannot grow the pool past MAX_POOL_SCOPES.
        let signers: Vec<NodeSigner> = (0..20)
            .map(|i| make_signer(&format!("auth-{i}"), 150 + i as u8, false))
            .collect();
        let atts: Vec<VerifiedAttestation> = signers.iter().map(|s| attest(s, 10_500)).collect();
        let mut pool = AttestationPool::new();
        for i in 0..100_000u64 {
            let att = atts[(i % atts.len() as u64) as usize].clone();
            let _ = pool.insert(&kr(&format!("scope-{i}/")), PolicyVersion(1), att, TEST_NOW);
        }
        assert!(
            pool.scope_count() <= MAX_POOL_SCOPES,
            "pool must stay hard-bounded under a distinct-scope flood"
        );
        assert!(
            pool.rejected_scope_cap_total() + pool.rejected_authority_cap_total() > 0,
            "cap counters must record the flood"
        );
    }
}
