//! In-memory pool of verified frontier attestations (FR-008).
//!
//! Collects [`VerifiedAttestation`] values per `(key_range, policy_version)`
//! scope and checkpoint, and assembles them into majority certificates once
//! enough distinct authorities have signed the same checkpoint message.

use std::collections::{BTreeMap, HashMap};

use crate::authority::certificate::{
    AuthoritySignature, DualModeCertificate, KeysetVersion, MajorityCertificate,
};
use crate::authority::frontier_sig::VerifiedAttestation;
use crate::hlc::HlcTimestamp;
use crate::types::{KeyRange, NodeId, PolicyVersion};

#[cfg(feature = "native-crypto")]
use crate::authority::bls;

/// Maximum number of checkpoints retained per scope (~32 seconds of history
/// at the default 1s checkpoint interval). Oldest checkpoints are pruned
/// first when the limit is exceeded.
const MAX_CHECKPOINTS_PER_SCOPE: usize = 32;

/// Scope key for attestation grouping.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PoolScope {
    key_range: KeyRange,
    policy_version: PolicyVersion,
}

/// Collects verified attestations and assembles majority certificates.
///
/// Non-persistent: the pool only holds recent checkpoints, and certificates
/// can always be rebuilt from fresh frontier reports.
#[derive(Debug, Default)]
pub struct AttestationPool {
    /// scope -> checkpoint physical (ms) -> authority -> attestation.
    entries: HashMap<PoolScope, BTreeMap<u64, HashMap<NodeId, VerifiedAttestation>>>,
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
    pub fn insert(
        &mut self,
        key_range: &KeyRange,
        policy_version: PolicyVersion,
        attestation: VerifiedAttestation,
    ) {
        let scope = PoolScope {
            key_range: key_range.clone(),
            policy_version,
        };
        let checkpoints = self.entries.entry(scope).or_default();
        checkpoints
            .entry(attestation.checkpoint_hlc.physical)
            .or_default()
            .insert(attestation.authority_id.clone(), attestation);

        while checkpoints.len() > MAX_CHECKPOINTS_PER_SCOPE {
            checkpoints.pop_first();
        }
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
    pub fn gc_scope(&mut self, key_range: &KeyRange, policy_version: &PolicyVersion) {
        self.entries.remove(&PoolScope {
            key_range: key_range.clone(),
            policy_version: *policy_version,
        });
    }

    /// Return the number of tracked scopes (for tests and diagnostics).
    pub fn scope_count(&self) -> usize {
        self.entries.len()
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

        pool.insert(&kr("user/"), PolicyVersion(1), attest(&s1, 10_500));
        // Only 1 of 3: no majority.
        assert!(
            pool.build_certificates(&kr("user/"), PolicyVersion(1), 3, &write_ts(9_000))
                .is_none()
        );

        pool.insert(&kr("user/"), PolicyVersion(1), attest(&s2, 10_700));
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
        pool.insert(&kr("user/"), PolicyVersion(1), attest(&s1, 10_100));
        pool.insert(&kr("user/"), PolicyVersion(1), attest(&s1, 10_200));

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
            pool.insert(&kr("user/"), PolicyVersion(1), attest(&s1, phys));
            pool.insert(&kr("user/"), PolicyVersion(1), attest(&s2, phys));
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

        pool.insert(&kr("user/"), PolicyVersion(1), attest(&s1, 50_000));
        pool.insert(&kr("user/"), PolicyVersion(1), attest(&s2, 50_000));

        // Replaying the same (old) attestations later cannot certify newer writes.
        pool.insert(&kr("user/"), PolicyVersion(1), attest(&s1, 50_000));
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
        pool.insert(&kr("user/"), PolicyVersion(1), attest(&s1, 10_000));
        assert_eq!(pool.scope_count(), 1);
        pool.gc_scope(&kr("user/"), &PolicyVersion(1));
        assert_eq!(pool.scope_count(), 0);
    }

    #[test]
    fn scopes_are_isolated() {
        let s1 = make_signer("auth-1", 10, false);
        let s2 = make_signer("auth-2", 11, false);
        let mut pool = AttestationPool::new();
        pool.insert(&kr("user/"), PolicyVersion(1), attest(&s1, 10_500));
        pool.insert(&kr("order/"), PolicyVersion(1), attest(&s2, 10_500));

        // Attestations from different scopes must not combine.
        assert!(
            pool.build_certificates(&kr("user/"), PolicyVersion(1), 3, &write_ts(9_000))
                .is_none()
        );
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
                    ("auth-1".into(), s1.bls_public_key().unwrap()),
                    ("auth-2".into(), s2.bls_public_key().unwrap()),
                    ("auth-3".into(), s3.bls_public_key().unwrap()),
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
            pool.insert(&kr("user/"), PolicyVersion(1), att);
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

        pool.insert(&kr("user/"), PolicyVersion(1), att1);
        pool.insert(&kr("user/"), PolicyVersion(1), att2);

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
        pool.insert(&kr("user/"), PolicyVersion(1), attest(&s1, 10_500));
        pool.insert(&kr("user/"), PolicyVersion(1), attest(&s2, 10_500));

        let (_, cert, bls_cert) = pool
            .build_certificates(&kr("user/"), PolicyVersion(1), 3, &write_ts(9_000))
            .unwrap();
        assert!(cert.has_majority(3));
        assert!(bls_cert.is_none());
    }
}
