use serde::Serialize;
use std::collections::HashSet;

use ed25519_dalek::VerifyingKey;

use crate::api::certified::ProofBundle;
use crate::authority::certificate::{
    CertError, DualModeCertificate, EpochConfig, FormatVersionConfig, KeysetRegistry,
    MajorityCertificate, create_certificate_message, majority_threshold,
};
use crate::authority::frontier_sig::checkpoint_hlc;
use crate::types::NodeId;

fn roster_contains_all<'a>(
    roster: &'a [NodeId],
    signers: impl IntoIterator<Item = &'a NodeId>,
) -> bool {
    let members: HashSet<_> = roster.iter().collect();
    members.len() == roster.len() && signers.into_iter().all(|id| members.contains(id))
}

fn trusted_keys_match(cert: &MajorityCertificate, trusted_keys: &[(NodeId, VerifyingKey)]) -> bool {
    let members: HashSet<_> = trusted_keys.iter().map(|(id, _)| id).collect();
    let distinct_keys: HashSet<_> = trusted_keys.iter().map(|(_, key)| key.to_bytes()).collect();
    members.len() == trusted_keys.len()
        && distinct_keys.len() == trusted_keys.len()
        && cert.signatures.iter().all(|sig| {
            trusted_keys
                .iter()
                .any(|(id, key)| id == &sig.authority_id && key == &sig.public_key)
        })
}

fn normalized_checkpoint(frontier: &crate::hlc::HlcTimestamp) -> bool {
    *frontier == checkpoint_hlc(frontier)
}

fn certificate_matches_bundle(bundle: &ProofBundle, cert: &MajorityCertificate) -> bool {
    let claimed: std::collections::HashSet<_> = bundle.contributing_authorities.iter().collect();
    let signed: std::collections::HashSet<_> = cert.signers().into_iter().collect();
    cert.key_range == bundle.key_range
        && cert.frontier_hlc == bundle.frontier_hlc
        && cert.policy_version == bundle.policy_version
        && claimed == signed
        && normalized_checkpoint(&cert.frontier_hlc)
}

/// Result of verifying a proof bundle.
///
/// Contains details about whether the proof meets the majority requirement
/// and, if a certificate is present, whether all signatures are valid.
#[derive(Debug, Clone, Serialize)]
pub struct VerificationResult {
    /// Overall validity: true if the proof has majority and signatures
    /// (if present) are all valid.
    pub valid: bool,
    /// Whether a strict majority of authorities are represented.
    pub has_majority: bool,
    /// Number of authorities that contributed to this proof.
    pub contributing_count: usize,
    /// Number of authorities required for majority.
    ///
    /// `usize::MAX` when the trusted roster is empty: no signer
    /// count can meet it, so `has_majority` is always false. See
    /// [`majority_threshold`].
    pub required_count: usize,
    /// Result of signature verification if a certificate is present.
    /// `None` if no certificate was included.
    pub signatures_valid: Option<bool>,
}

/// Verify a proof bundle independently.
///
/// Checks that:
/// 1. The number of contributing authorities meets the majority threshold.
/// 2. If a certificate is present, all Ed25519 signatures are valid against
///    the canonical message derived from the proof's key range, frontier HLC,
///    and policy version.
/// 3. If `format_config` is provided, the certificate's format version is
///    checked against the grace-period policy.
/// 4. The certificate metadata and signer set match the bundle, and the
///    checkpoint uses the canonical 1000 ms bucket representation.
///
/// The caller supplies the exact trusted Authority ID→public-key roster for
/// this range. Embedded keys are accepted only when they match that roster;
/// both membership and the quorum denominator come from this trust anchor.
pub fn verify_proof(
    bundle: &ProofBundle,
    trusted_keys: &[(NodeId, VerifyingKey)],
    format_config: Option<&FormatVersionConfig>,
    elapsed_since_upgrade_secs: u64,
) -> VerificationResult {
    let required = majority_threshold(trusted_keys.len());
    if bundle.total_authorities != trusted_keys.len() {
        return VerificationResult {
            valid: false,
            has_majority: false,
            contributing_count: 0,
            required_count: required,
            signatures_valid: Some(false),
        };
    }

    // A proof without a certificate is always invalid — a caller could
    // fabricate a "valid" proof by simply listing enough authority IDs.
    let (contributing_count, signatures_valid) = match bundle.certificate.as_ref() {
        Some(cert)
            if !certificate_matches_bundle(bundle, cert)
                || !trusted_keys_match(cert, trusted_keys) =>
        {
            let unique: std::collections::HashSet<_> =
                bundle.contributing_authorities.iter().collect();
            (unique.len(), Some(false))
        }
        Some(cert) => {
            let message = create_certificate_message(
                &bundle.key_range,
                &bundle.frontier_hlc,
                &bundle.policy_version,
            );
            let result = if let Some(fc) = format_config {
                cert.verify_signatures_with_format_check(&message, fc, elapsed_since_upgrade_secs)
            } else {
                cert.verify_signatures(&message)
            };
            match result {
                Ok(verified_signers) => (verified_signers.len(), Some(true)),
                Err(_) => {
                    // Derive count from the unsigned list only as a fallback
                    // for the response; the proof is invalid regardless.
                    let unique: std::collections::HashSet<&crate::types::NodeId> =
                        bundle.contributing_authorities.iter().collect();
                    (unique.len(), Some(false))
                }
            }
        }
        None => {
            let unique: std::collections::HashSet<&crate::types::NodeId> =
                bundle.contributing_authorities.iter().collect();
            (unique.len(), None)
        }
    };

    let trusted_roster: Vec<NodeId> = trusted_keys.iter().map(|(id, _)| id.clone()).collect();
    let has_majority = roster_contains_all(&trusted_roster, &bundle.contributing_authorities)
        && contributing_count >= required;
    let valid = has_majority && signatures_valid == Some(true);

    VerificationResult {
        valid,
        has_majority,
        contributing_count,
        required_count: required,
        signatures_valid,
    }
}

/// Verify a proof bundle with keyset registry and epoch awareness.
///
/// Verifies with trusted scope membership and registry key lookup:
/// 1. Each signature's keyset version is known in the registry.
/// 2. Each signature's keyset version is within the epoch grace period.
/// 3. Signatures are verified against the registry's public keys
///    (not just the embedded keys in the certificate).
/// 4. If `format_config` is provided, the certificate's format version is
///    checked against the grace-period policy.
/// 5. The certificate metadata and signer set match the bundle, and its
///    checkpoint is canonical.
/// 6. Every signer belongs to the caller's exact trusted scope roster and
///    the bundle's claimed count matches that roster. The registry can hold
///    keys for other ranges, so it alone cannot establish scope membership.
///
/// Returns a `VerificationResult` with an optional `keyset_error` if
/// any keyset/epoch validation fails.
pub fn verify_proof_with_registry(
    bundle: &ProofBundle,
    trusted_authorities: &[NodeId],
    registry: &KeysetRegistry,
    current_epoch: u64,
    epoch_config: &EpochConfig,
    format_config: Option<&FormatVersionConfig>,
    elapsed_since_upgrade_secs: u64,
) -> VerificationResult {
    let required = majority_threshold(trusted_authorities.len());

    if bundle.total_authorities != trusted_authorities.len() {
        return VerificationResult {
            valid: false,
            has_majority: false,
            contributing_count: 0,
            required_count: required,
            signatures_valid: Some(false),
        };
    }

    // A proof without a certificate is always invalid.
    let (contributing_count, signatures_valid) = match bundle.certificate.as_ref() {
        Some(cert) if !certificate_matches_bundle(bundle, cert) => {
            let unique: std::collections::HashSet<_> =
                bundle.contributing_authorities.iter().collect();
            (unique.len(), Some(false))
        }
        Some(cert) => {
            let message = create_certificate_message(
                &bundle.key_range,
                &bundle.frontier_hlc,
                &bundle.policy_version,
            );
            let result = if let Some(fc) = format_config {
                cert.verify_signatures_with_registry_and_format_check(
                    &message,
                    registry,
                    current_epoch,
                    epoch_config,
                    fc,
                    elapsed_since_upgrade_secs,
                )
            } else {
                cert.verify_signatures_with_registry(
                    &message,
                    registry,
                    current_epoch,
                    epoch_config,
                )
            };
            match result {
                Ok(verified_signers) => (verified_signers.len(), Some(true)),
                Err(_) => {
                    let unique: std::collections::HashSet<&crate::types::NodeId> =
                        bundle.contributing_authorities.iter().collect();
                    (unique.len(), Some(false))
                }
            }
        }
        None => {
            let unique: std::collections::HashSet<&crate::types::NodeId> =
                bundle.contributing_authorities.iter().collect();
            (unique.len(), None)
        }
    };

    let has_majority = roster_contains_all(trusted_authorities, &bundle.contributing_authorities)
        && contributing_count >= required;
    let valid = has_majority && signatures_valid == Some(true);

    VerificationResult {
        valid,
        has_majority,
        contributing_count,
        required_count: required,
        signatures_valid,
    }
}

/// Verify a dual-mode (BLS-capable) certificate with keyset registry awareness.
///
/// The canonical message is recomputed from the certificate's own key range,
/// frontier HLC, and policy version — exactly as `verify_proof_with_registry`
/// does for Ed25519 proofs.  Keyset epoch validation, duplicate signer
/// rejection, and the registry/embedded key match requirement are delegated
/// to `DualModeCertificate::verify_with_registry`.
///
/// The caller supplies the exact trusted scope roster. `valid` requires
/// every verified signer to belong to it, as well as majority and valid signatures.
/// When `format_config` is provided, the certificate's format version is
/// checked first; an unacceptable version yields `signatures_valid = Some(false)`.
pub fn verify_dual_proof_with_registry(
    cert: &DualModeCertificate,
    trusted_authorities: &[NodeId],
    registry: &KeysetRegistry,
    current_epoch: u64,
    epoch_config: &EpochConfig,
    format_config: Option<&FormatVersionConfig>,
    elapsed_since_upgrade_secs: u64,
) -> VerificationResult {
    let required = majority_threshold(trusted_authorities.len());

    let format_ok = format_config
        .map(|fc| fc.is_version_acceptable(cert.format_version, elapsed_since_upgrade_secs))
        .unwrap_or(true);

    let message =
        create_certificate_message(&cert.key_range, &cert.frontier_hlc, &cert.policy_version);

    let (contributing_count, signatures_valid, verified_signers) =
        if !format_ok || !normalized_checkpoint(&cert.frontier_hlc) {
            (cert.signer_count(), Some(false), Vec::new())
        } else {
            match cert.verify_with_registry(&message, registry, current_epoch, epoch_config) {
                Ok(verified_signers) => (verified_signers.len(), Some(true), verified_signers),
                Err(_) => (cert.signer_count(), Some(false), Vec::new()),
            }
        };

    let signer_ids: Vec<&NodeId> = match &cert.mode {
        crate::authority::certificate::CertificateMode::Ed25519 => cert
            .ed25519_signatures
            .iter()
            .map(|sig| &sig.authority_id)
            .collect(),
        crate::authority::certificate::CertificateMode::Bls => cert.bls_signer_ids.iter().collect(),
    };
    let has_majority =
        roster_contains_all(trusted_authorities, signer_ids) && contributing_count >= required;
    let valid = has_majority
        && signatures_valid == Some(true)
        && roster_contains_all(trusted_authorities, &verified_signers);

    VerificationResult {
        valid,
        has_majority,
        contributing_count,
        required_count: required,
        signatures_valid,
    }
}

/// Verify a proof bundle and return the keyset error details if validation fails.
///
/// Like `verify_proof_with_registry` but returns the `CertError` on failure
/// for callers that need to distinguish between expired keys, unknown keys,
/// invalid signatures, or expired format versions.
pub fn verify_proof_with_registry_detailed(
    bundle: &ProofBundle,
    trusted_authorities: &[NodeId],
    registry: &KeysetRegistry,
    current_epoch: u64,
    epoch_config: &EpochConfig,
    format_config: Option<&FormatVersionConfig>,
    elapsed_since_upgrade_secs: u64,
) -> Result<VerificationResult, CertError> {
    if bundle.total_authorities != trusted_authorities.len()
        || !roster_contains_all(trusted_authorities, &bundle.contributing_authorities)
    {
        return Err(CertError::InvalidCertificateMetadata(
            "proof count or signers do not match trusted roster",
        ));
    }
    let required = majority_threshold(trusted_authorities.len());

    let (contributing_count, signatures_valid) = if let Some(cert) = &bundle.certificate {
        if !certificate_matches_bundle(bundle, cert) {
            return Err(CertError::InvalidCertificateMetadata(
                "certificate fields or signer set do not match the proof bundle",
            ));
        }
        let message = create_certificate_message(
            &bundle.key_range,
            &bundle.frontier_hlc,
            &bundle.policy_version,
        );
        let result = if let Some(fc) = format_config {
            cert.verify_signatures_with_registry_and_format_check(
                &message,
                registry,
                current_epoch,
                epoch_config,
                fc,
                elapsed_since_upgrade_secs,
            )
        } else {
            cert.verify_signatures_with_registry(&message, registry, current_epoch, epoch_config)
        };
        let verified_signers = result?;
        (verified_signers.len(), Some(true))
    } else {
        let unique: std::collections::HashSet<&crate::types::NodeId> =
            bundle.contributing_authorities.iter().collect();
        (unique.len(), None)
    };

    let has_majority = contributing_count >= required;
    let valid = has_majority && signatures_valid == Some(true);

    Ok(VerificationResult {
        valid,
        has_majority,
        contributing_count,
        required_count: required,
        signatures_valid,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::certified::ProofBundle;
    use crate::authority::certificate::{
        AuthoritySignature, FormatVersionConfig, KeysetVersion, MajorityCertificate,
        create_certificate_message, sign_message,
    };
    use crate::hlc::HlcTimestamp;
    use crate::types::{KeyRange, NodeId, PolicyVersion};
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;

    fn make_key_pair() -> (SigningKey, ed25519_dalek::VerifyingKey) {
        let sk = SigningKey::generate(&mut OsRng);
        let vk = sk.verifying_key();
        (sk, vk)
    }

    fn sample_kr() -> KeyRange {
        KeyRange {
            prefix: "user/".into(),
        }
    }

    fn sample_hlc() -> HlcTimestamp {
        HlcTimestamp {
            physical: 1_700_000_000_000,
            logical: 0,
            node_id: String::new(),
        }
    }

    fn sample_pv() -> PolicyVersion {
        PolicyVersion(1)
    }

    /// Build a proof bundle with the given number of contributing authorities
    /// out of `total` and optionally attach a signed certificate.
    fn make_proof(contributing: usize, total: usize, with_certificate: bool) -> ProofBundle {
        let kr = sample_kr();
        let hlc = sample_hlc();
        let pv = sample_pv();

        let authorities: Vec<NodeId> = (0..contributing)
            .map(|i| NodeId(format!("auth-{i}")))
            .collect();

        let certificate = if with_certificate {
            let message = create_certificate_message(&kr, &hlc, &pv);
            let mut cert = MajorityCertificate::new(kr.clone(), hlc.clone(), pv, KeysetVersion(1));
            for auth in &authorities {
                let (sk, vk) = make_key_pair();
                let sig = sign_message(&sk, &message);
                cert.add_signature(AuthoritySignature {
                    authority_id: auth.clone(),
                    public_key: vk,
                    signature: sig,
                    keyset_version: KeysetVersion(1),
                });
            }
            Some(cert)
        } else {
            None
        };

        ProofBundle {
            key_range: kr,
            frontier_hlc: hlc,
            policy_version: pv,
            contributing_authorities: authorities,
            total_authorities: total,
            certificate,
            bls_certificate: None,
        }
    }

    fn test_roster(ids: impl IntoIterator<Item = NodeId>, total: usize) -> Vec<NodeId> {
        let mut roster: Vec<NodeId> = ids.into_iter().collect();
        roster.truncate(total);
        while roster.len() < total {
            roster.push(NodeId(format!("trusted-extra-{}", roster.len())));
        }
        roster
    }

    fn test_keys(proof: &ProofBundle, total: usize) -> Vec<(NodeId, VerifyingKey)> {
        let mut keys: Vec<(NodeId, VerifyingKey)> = proof
            .certificate
            .as_ref()
            .map(|cert| {
                cert.signatures
                    .iter()
                    .map(|sig| (sig.authority_id.clone(), sig.public_key))
                    .collect()
            })
            .unwrap_or_default();
        for id in &proof.contributing_authorities {
            if !keys.iter().any(|(member, _)| member == id) {
                let (_, key) = make_key_pair();
                keys.push((id.clone(), key));
            }
        }
        keys.truncate(total);
        while keys.len() < total {
            let (_, key) = make_key_pair();
            keys.push((NodeId(format!("trusted-extra-{}", keys.len())), key));
        }
        keys
    }

    // Test fixtures construct independent trust arguments explicitly at the
    // call boundary. Production callers must supply their own trusted keys.
    fn verify_proof(
        proof: &ProofBundle,
        total: usize,
        format_config: Option<&FormatVersionConfig>,
        elapsed: u64,
    ) -> VerificationResult {
        super::verify_proof(proof, &test_keys(proof, total), format_config, elapsed)
    }

    fn verify_proof_with_registry(
        proof: &ProofBundle,
        total: usize,
        registry: &KeysetRegistry,
        current_epoch: u64,
        epoch_config: &EpochConfig,
        format_config: Option<&FormatVersionConfig>,
        elapsed: u64,
    ) -> VerificationResult {
        let roster = test_roster(proof.contributing_authorities.clone(), total);
        super::verify_proof_with_registry(
            proof,
            &roster,
            registry,
            current_epoch,
            epoch_config,
            format_config,
            elapsed,
        )
    }

    fn verify_proof_with_registry_detailed(
        proof: &ProofBundle,
        total: usize,
        registry: &KeysetRegistry,
        current_epoch: u64,
        epoch_config: &EpochConfig,
        format_config: Option<&FormatVersionConfig>,
        elapsed: u64,
    ) -> Result<VerificationResult, CertError> {
        let roster = test_roster(proof.contributing_authorities.clone(), total);
        super::verify_proof_with_registry_detailed(
            proof,
            &roster,
            registry,
            current_epoch,
            epoch_config,
            format_config,
            elapsed,
        )
    }

    fn verify_dual_proof_with_registry(
        cert: &DualModeCertificate,
        total: usize,
        registry: &KeysetRegistry,
        current_epoch: u64,
        epoch_config: &EpochConfig,
        format_config: Option<&FormatVersionConfig>,
        elapsed: u64,
    ) -> VerificationResult {
        let ids = match &cert.mode {
            crate::authority::certificate::CertificateMode::Ed25519 => cert
                .ed25519_signatures
                .iter()
                .map(|sig| sig.authority_id.clone())
                .collect(),
            crate::authority::certificate::CertificateMode::Bls => cert.bls_signer_ids.clone(),
        };
        let roster = test_roster(ids, total);
        super::verify_dual_proof_with_registry(
            cert,
            &roster,
            registry,
            current_epoch,
            epoch_config,
            format_config,
            elapsed,
        )
    }

    #[test]
    fn proof_without_certificate_is_rejected() {
        let proof = make_proof(3, 5, false);
        let result = verify_proof(&proof, proof.total_authorities, None, 0);

        assert!(!result.valid);
        assert!(result.has_majority);
        assert_eq!(result.contributing_count, 3);
        assert_eq!(result.required_count, 3); // 5/2+1 = 3
        assert!(result.signatures_valid.is_none());
    }

    #[test]
    fn proof_with_insufficient_authorities_fails() {
        let proof = make_proof(2, 5, true);
        let result = verify_proof(&proof, proof.total_authorities, None, 0);

        assert!(!result.valid);
        assert!(!result.has_majority);
        assert_eq!(result.contributing_count, 2);
        assert_eq!(result.required_count, 3);
    }

    #[test]
    fn duplicate_authorities_are_deduplicated() {
        let mut proof = make_proof(2, 5, true);

        let dup = proof.contributing_authorities[0].clone();
        proof.contributing_authorities.push(dup);

        let result = verify_proof(&proof, proof.total_authorities, None, 0);
        assert!(!result.valid);
        assert!(!result.has_majority);
        assert_eq!(result.contributing_count, 2);
        assert_eq!(result.required_count, 3);
    }

    #[test]
    fn valid_proof_with_certificate() {
        let proof = make_proof(3, 5, true);
        let result = verify_proof(&proof, proof.total_authorities, None, 0);

        assert!(result.valid);
        assert!(result.has_majority);
        assert_eq!(result.signatures_valid, Some(true));
    }

    #[test]
    fn certificate_metadata_and_signers_must_match_bundle() {
        let mut proof = make_proof(3, 5, true);
        proof.certificate.as_mut().unwrap().key_range.prefix = "other/".into();
        assert!(!verify_proof(&proof, proof.total_authorities, None, 0).valid);

        let mut proof = make_proof(3, 5, true);
        proof.certificate.as_mut().unwrap().frontier_hlc.physical += 1_000;
        assert!(!verify_proof(&proof, proof.total_authorities, None, 0).valid);

        let mut proof = make_proof(3, 5, true);
        proof.certificate.as_mut().unwrap().policy_version = PolicyVersion(2);
        assert!(!verify_proof(&proof, proof.total_authorities, None, 0).valid);

        let mut proof = make_proof(3, 5, true);
        proof.contributing_authorities[0] = NodeId("someone-else".into());
        assert!(!verify_proof(&proof, proof.total_authorities, None, 0).valid);
    }

    #[test]
    fn genuinely_signed_noncanonical_checkpoint_is_rejected() {
        let kr = sample_kr();
        let mut hlc = sample_hlc();
        hlc.logical = 1;
        let pv = sample_pv();
        let message = create_certificate_message(&kr, &hlc, &pv);
        let (sk, vk) = make_key_pair();
        let signer = NodeId("auth-0".into());
        let mut cert = MajorityCertificate::new(kr.clone(), hlc.clone(), pv, KeysetVersion(1));
        cert.add_signature(AuthoritySignature {
            authority_id: signer.clone(),
            public_key: vk,
            signature: sign_message(&sk, &message),
            keyset_version: KeysetVersion(1),
        });
        let proof = ProofBundle {
            key_range: kr,
            frontier_hlc: hlc,
            policy_version: pv,
            contributing_authorities: vec![signer],
            total_authorities: 1,
            certificate: Some(cert),
            bls_certificate: None,
        };
        let result = verify_proof(&proof, proof.total_authorities, None, 0);
        assert!(!result.valid);
        assert_eq!(result.signatures_valid, Some(false));
    }

    #[test]
    fn raw_report_signature_collision_cannot_certify_noncanonical_checkpoint() {
        use crate::authority::ack_frontier::AckFrontier;
        use crate::authority::frontier_sig::create_frontier_report_message;

        let report = AckFrontier {
            authority_id: NodeId(String::new()),
            key_range: KeyRange {
                prefix: String::new(),
            },
            frontier_hlc: HlcTimestamp {
                physical: 76,
                logical: 0,
                node_id: String::new(),
            },
            policy_version: PolicyVersion(0),
            digest_hash: "0".repeat(64),
        };
        let range = KeyRange {
            prefix: "asteroidb/frontier-report/v1".into(),
        };
        let checkpoint = HlcTimestamp {
            physical: 0,
            logical: 0,
            node_id: format!("{}@{}", "\0".repeat(19), "0".repeat(56)),
        };
        let version = PolicyVersion(0x3030_3030_3030_3030);
        let report_message = create_frontier_report_message(&report);
        assert_eq!(
            report_message,
            create_certificate_message(&range, &checkpoint, &version),
            "raw encoders have the Lean counterexample's byte collision"
        );

        let (sk, vk) = make_key_pair();
        let signer = NodeId("auth-0".into());
        let mut cert =
            MajorityCertificate::new(range.clone(), checkpoint.clone(), version, KeysetVersion(1));
        cert.add_signature(AuthoritySignature {
            authority_id: signer.clone(),
            public_key: vk,
            signature: sign_message(&sk, &report_message),
            keyset_version: KeysetVersion(1),
        });
        assert!(cert.verify_signatures(&report_message).is_ok());
        let proof = ProofBundle {
            key_range: range,
            frontier_hlc: checkpoint,
            policy_version: version,
            contributing_authorities: vec![signer],
            total_authorities: 1,
            certificate: Some(cert),
            bls_certificate: None,
        };
        let result = verify_proof(&proof, proof.total_authorities, None, 0);
        assert!(!result.valid);
        assert_eq!(result.signatures_valid, Some(false));
    }

    #[test]
    fn certificate_with_tampered_signature_fails() {
        let mut proof = make_proof(3, 5, true);

        if let Some(cert) = &mut proof.certificate {
            let (sk, _vk) = make_key_pair();
            let bad_sig = sign_message(&sk, b"wrong message");
            cert.signatures[0].signature = bad_sig;
        }

        let result = verify_proof(&proof, proof.total_authorities, None, 0);
        assert!(!result.valid);
        assert!(result.has_majority);
        assert_eq!(result.signatures_valid, Some(false));
    }

    /// A bundle claiming zero total authorities plus one valid signature used
    /// to verify: `0 / 2 + 1 == 1`, and nothing checks the signer against an
    /// authority set. That made the verifier disagree with the issuing side,
    /// which refuses such a scope outright.
    #[test]
    fn zero_total_authorities_never_verifies() {
        let proof = make_proof(1, 0, true);
        let result = verify_proof(&proof, proof.total_authorities, None, 0);

        assert!(!result.valid, "a majority of nobody is not a majority");
        assert!(!result.has_majority);
        assert_eq!(result.contributing_count, 1);
        assert_eq!(
            result.required_count,
            usize::MAX,
            "zero authorities must be reported as unsatisfiable, not as 1"
        );
        // An empty trust roster cannot bind the signed key to an Authority.
        assert_eq!(result.signatures_valid, Some(false));
    }

    #[test]
    fn zero_total_authorities_never_verifies_with_registry() {
        let (proof, registry) = make_proof_with_registry(1, 0, 1);
        let result = verify_proof_with_registry(
            &proof,
            proof.total_authorities,
            &registry,
            0,
            &EpochConfig::default(),
            None,
            0,
        );
        assert!(!result.valid);
        assert!(!result.has_majority);
        assert_eq!(result.required_count, usize::MAX);
    }

    #[test]
    fn exact_majority_threshold() {
        let proof = make_proof(1, 1, true);
        assert!(verify_proof(&proof, proof.total_authorities, None, 0).valid);

        let proof = make_proof(2, 3, true);
        assert!(verify_proof(&proof, proof.total_authorities, None, 0).valid);

        let proof = make_proof(1, 3, true);
        assert!(!verify_proof(&proof, proof.total_authorities, None, 0).valid);
    }

    #[test]
    fn independent_verifier_rejects_forged_quorum_denominator() {
        let mut proof = make_proof(1, 3, true);
        proof.total_authorities = 1;

        let result = verify_proof(&proof, 3, None, 0);
        assert!(!result.valid);
        assert!(!result.has_majority);
        assert_eq!(result.required_count, 2);
        assert_eq!(result.signatures_valid, Some(false));
    }

    // ---------------------------------------------------------------
    // verify_proof_with_registry tests
    // ---------------------------------------------------------------

    use crate::authority::certificate::{CertError, EpochConfig, KeysetRegistry};

    /// Build a proof bundle where authority keys are registered in a KeysetRegistry.
    fn make_proof_with_registry(
        contributing: usize,
        total: usize,
        keyset_version: u64,
    ) -> (ProofBundle, KeysetRegistry) {
        let kr = sample_kr();
        let hlc = sample_hlc();
        let pv = sample_pv();
        let message = create_certificate_message(&kr, &hlc, &pv);

        let authorities: Vec<NodeId> = (0..contributing)
            .map(|i| NodeId(format!("auth-{i}")))
            .collect();

        let mut registry = KeysetRegistry::new();
        let mut cert =
            MajorityCertificate::new(kr.clone(), hlc.clone(), pv, KeysetVersion(keyset_version));
        let mut registry_keys = Vec::new();

        for auth in &authorities {
            let (sk, vk) = make_key_pair();
            let sig = sign_message(&sk, &message);
            registry_keys.push((auth.clone(), vk));
            cert.add_signature(AuthoritySignature {
                authority_id: auth.clone(),
                public_key: vk,
                signature: sig,
                keyset_version: KeysetVersion(keyset_version),
            });
        }

        registry
            .register_keyset(KeysetVersion(keyset_version), 0, registry_keys)
            .unwrap();

        let bundle = ProofBundle {
            key_range: kr,
            frontier_hlc: hlc,
            policy_version: pv,
            contributing_authorities: authorities,
            total_authorities: total,
            certificate: Some(cert),
            bls_certificate: None,
        };

        (bundle, registry)
    }

    #[test]
    fn registered_signers_from_another_range_do_not_form_this_ranges_quorum() {
        let (proof, registry) = make_proof_with_registry(2, 3, 1);
        let result = super::verify_proof_with_registry(
            &proof,
            &[
                NodeId("other-range-a".into()),
                NodeId("other-range-b".into()),
                NodeId("other-range-c".into()),
            ],
            &registry,
            0,
            &EpochConfig::default(),
            None,
            0,
        );
        assert_eq!(result.signatures_valid, Some(true));
        assert!(!result.has_majority);
        assert!(!result.valid);
    }

    #[test]
    fn embedded_key_cannot_substitute_for_the_trusted_authority_key() {
        let proof = make_proof(2, 3, true);
        let mut trusted = test_keys(&proof, 3);
        trusted[0].1 = make_key_pair().1;
        let result = super::verify_proof(&proof, &trusted, None, 0);
        assert!(!result.valid);
        assert_eq!(result.signatures_valid, Some(false));
    }

    #[test]
    fn two_authority_ids_sharing_one_trusted_key_do_not_form_a_majority() {
        let mut proof = make_proof(2, 3, true);
        let cert = proof.certificate.as_mut().unwrap();
        let first = cert.signatures[0].clone();
        cert.signatures[1].public_key = first.public_key;
        cert.signatures[1].signature = first.signature;
        let trusted = test_keys(&proof, 3);

        assert!(!super::verify_proof(&proof, &trusted, None, 0).valid);
    }

    #[test]
    fn duplicate_trusted_roster_cannot_lower_quorum() {
        let (proof, registry) = make_proof_with_registry(2, 3, 1);
        let result = super::verify_proof_with_registry(
            &proof,
            &[
                NodeId("auth-0".into()),
                NodeId("auth-0".into()),
                NodeId("auth-1".into()),
            ],
            &registry,
            0,
            &EpochConfig::default(),
            None,
            0,
        );
        assert!(!result.valid);
    }

    #[test]
    fn verify_with_registry_valid_proof() {
        let (proof, registry) = make_proof_with_registry(3, 5, 1);
        let config = EpochConfig::default();
        let result = verify_proof_with_registry(
            &proof,
            proof.total_authorities,
            &registry,
            0,
            &config,
            None,
            0,
        );

        assert!(result.valid);
        assert!(result.has_majority);
        assert_eq!(result.signatures_valid, Some(true));
    }

    #[test]
    fn registry_verifier_rejects_forged_quorum_denominator() {
        let (mut proof, registry) = make_proof_with_registry(1, 3, 1);
        proof.total_authorities = 1;
        let config = EpochConfig::default();

        let result = verify_proof_with_registry(&proof, 3, &registry, 0, &config, None, 0);
        assert!(!result.valid);
        assert!(!result.has_majority);
        assert_eq!(result.required_count, 2);
        assert!(matches!(
            verify_proof_with_registry_detailed(&proof, 3, &registry, 0, &config, None, 0),
            Err(CertError::InvalidCertificateMetadata(_))
        ));
    }

    #[test]
    fn registry_verifiers_reject_mismatched_certificate_fields_and_signers() {
        let (mut proof, registry) = make_proof_with_registry(3, 5, 1);
        proof.certificate.as_mut().unwrap().policy_version = PolicyVersion(2);
        let config = EpochConfig::default();
        assert!(
            !verify_proof_with_registry(
                &proof,
                proof.total_authorities,
                &registry,
                0,
                &config,
                None,
                0
            )
            .valid
        );
        assert!(matches!(
            verify_proof_with_registry_detailed(
                &proof,
                proof.total_authorities,
                &registry,
                0,
                &config,
                None,
                0
            ),
            Err(CertError::InvalidCertificateMetadata(_))
        ));

        let (mut proof, registry) = make_proof_with_registry(3, 5, 1);
        proof.contributing_authorities[0] = NodeId("outside".into());
        assert!(
            !verify_proof_with_registry(
                &proof,
                proof.total_authorities,
                &registry,
                0,
                &config,
                None,
                0
            )
            .valid
        );
        assert!(matches!(
            verify_proof_with_registry_detailed(
                &proof,
                proof.total_authorities,
                &registry,
                0,
                &config,
                None,
                0
            ),
            Err(CertError::InvalidCertificateMetadata(_))
        ));
    }

    #[test]
    fn verify_with_registry_expired_keyset_fails() {
        let (proof, mut registry) = make_proof_with_registry(3, 5, 1);

        let (_, vk_new) = make_key_pair();
        registry
            .register_keyset(
                KeysetVersion(2),
                5,
                vec![(NodeId("auth-new".into()), vk_new)],
            )
            .unwrap();

        let config = EpochConfig {
            duration_secs: 86400,
            grace_epochs: 3,
        };

        let result = verify_proof_with_registry(
            &proof,
            proof.total_authorities,
            &registry,
            4,
            &config,
            None,
            0,
        );
        assert!(!result.valid);
        assert_eq!(result.signatures_valid, Some(false));
    }

    #[test]
    fn verify_with_registry_expired_keyset_detailed_error() {
        let (proof, mut registry) = make_proof_with_registry(3, 5, 1);

        let (_, vk_new) = make_key_pair();
        registry
            .register_keyset(
                KeysetVersion(2),
                5,
                vec![(NodeId("auth-new".into()), vk_new)],
            )
            .unwrap();

        let config = EpochConfig {
            duration_secs: 86400,
            grace_epochs: 3,
        };

        let result = verify_proof_with_registry_detailed(
            &proof,
            proof.total_authorities,
            &registry,
            4,
            &config,
            None,
            0,
        );
        assert!(matches!(
            result,
            Err(CertError::ExpiredKeyset { version: 1, .. })
        ));
    }

    #[test]
    fn verify_with_registry_tampered_signature_detected() {
        let (mut proof, registry) = make_proof_with_registry(3, 5, 1);

        if let Some(cert) = &mut proof.certificate {
            let (sk, _) = make_key_pair();
            let bad_sig = sign_message(&sk, b"wrong message");
            cert.signatures[0].signature = bad_sig;
        }

        let config = EpochConfig::default();
        let result = verify_proof_with_registry(
            &proof,
            proof.total_authorities,
            &registry,
            0,
            &config,
            None,
            0,
        );
        assert!(!result.valid);
        assert_eq!(result.signatures_valid, Some(false));
    }

    #[test]
    fn verify_with_registry_mixed_versions_within_grace() {
        let kr = sample_kr();
        let hlc = sample_hlc();
        let pv = sample_pv();
        let message = create_certificate_message(&kr, &hlc, &pv);

        let mut registry = KeysetRegistry::new();

        let (sk1, vk1) = make_key_pair();
        let id1 = NodeId("auth-0".into());
        registry
            .register_keyset(KeysetVersion(1), 0, vec![(id1.clone(), vk1)])
            .unwrap();

        let (sk2, vk2) = make_key_pair();
        let (sk3, vk3) = make_key_pair();
        let id2 = NodeId("auth-1".into());
        let id3 = NodeId("auth-2".into());
        registry
            .register_keyset(
                KeysetVersion(2),
                5,
                vec![(id2.clone(), vk2), (id3.clone(), vk3)],
            )
            .unwrap();

        let mut cert = MajorityCertificate::new(kr.clone(), hlc.clone(), pv, KeysetVersion(2));
        let sig1 = sign_message(&sk1, &message);
        cert.add_signature(AuthoritySignature {
            authority_id: id1.clone(),
            public_key: vk1,
            signature: sig1,
            keyset_version: KeysetVersion(1),
        });
        let sig2 = sign_message(&sk2, &message);
        cert.add_signature(AuthoritySignature {
            authority_id: id2.clone(),
            public_key: vk2,
            signature: sig2,
            keyset_version: KeysetVersion(2),
        });
        let sig3 = sign_message(&sk3, &message);
        cert.add_signature(AuthoritySignature {
            authority_id: id3.clone(),
            public_key: vk3,
            signature: sig3,
            keyset_version: KeysetVersion(2),
        });

        let bundle = ProofBundle {
            key_range: kr,
            frontier_hlc: hlc,
            policy_version: pv,
            contributing_authorities: vec![id1, id2, id3],
            total_authorities: 5,
            certificate: Some(cert),
            bls_certificate: None,
        };

        let config = EpochConfig {
            duration_secs: 86400,
            grace_epochs: 7,
        };

        let result = verify_proof_with_registry(
            &bundle,
            bundle.total_authorities,
            &registry,
            6,
            &config,
            None,
            0,
        );
        assert!(result.valid);
        assert!(result.has_majority);
        assert_eq!(result.signatures_valid, Some(true));
    }

    // ---------------------------------------------------------------
    // Format version checking in verifier
    // ---------------------------------------------------------------

    #[test]
    fn verifier_rejects_expired_old_format_certificate() {
        let mut proof = make_proof(3, 5, true);
        if let Some(cert) = &mut proof.certificate {
            cert.format_version = 1;
        }

        let fc = FormatVersionConfig {
            grace_period_secs: 100,
        };

        // Within grace period: accepted.
        let result = verify_proof(&proof, proof.total_authorities, Some(&fc), 50);
        assert!(result.valid);
        assert_eq!(result.signatures_valid, Some(true));

        // Beyond grace period: rejected.
        let result = verify_proof(&proof, proof.total_authorities, Some(&fc), 101);
        assert!(!result.valid);
        assert_eq!(result.signatures_valid, Some(false));
    }

    #[test]
    fn verifier_accepts_current_format_regardless_of_elapsed() {
        let proof = make_proof(3, 5, true);

        let fc = FormatVersionConfig {
            grace_period_secs: 0,
        };

        let result = verify_proof(&proof, proof.total_authorities, Some(&fc), 999_999);
        assert!(result.valid);
        assert_eq!(result.signatures_valid, Some(true));
    }

    #[test]
    fn verifier_with_registry_rejects_expired_format() {
        let (mut proof, registry) = make_proof_with_registry(3, 5, 1);
        if let Some(cert) = &mut proof.certificate {
            cert.format_version = 1;
        }

        let epoch_config = EpochConfig::default();
        let fc = FormatVersionConfig {
            grace_period_secs: 100,
        };

        let result = verify_proof_with_registry(
            &proof,
            proof.total_authorities,
            &registry,
            0,
            &epoch_config,
            Some(&fc),
            50,
        );
        assert!(result.valid);

        let result = verify_proof_with_registry(
            &proof,
            proof.total_authorities,
            &registry,
            0,
            &epoch_config,
            Some(&fc),
            101,
        );
        assert!(!result.valid);
        assert_eq!(result.signatures_valid, Some(false));
    }

    #[test]
    fn verifier_detailed_returns_expired_format_error() {
        let (mut proof, registry) = make_proof_with_registry(3, 5, 1);
        if let Some(cert) = &mut proof.certificate {
            cert.format_version = 1;
        }

        let epoch_config = EpochConfig::default();
        let fc = FormatVersionConfig {
            grace_period_secs: 100,
        };

        let result = verify_proof_with_registry_detailed(
            &proof,
            proof.total_authorities,
            &registry,
            0,
            &epoch_config,
            Some(&fc),
            101,
        );
        assert!(
            matches!(result, Err(CertError::ExpiredFormatVersion { version: 1 })),
            "expected ExpiredFormatVersion, got: {result:?}"
        );
    }

    #[test]
    fn verifier_without_format_config_skips_version_check() {
        let mut proof = make_proof(3, 5, true);
        if let Some(cert) = &mut proof.certificate {
            cert.format_version = 1;
        }

        let result = verify_proof(&proof, proof.total_authorities, None, 999_999);
        assert!(
            result.valid,
            "without format config, old version should still pass"
        );
    }

    // ---------------------------------------------------------------
    // verify_dual_proof_with_registry (BLS aggregate path)
    // ---------------------------------------------------------------

    #[cfg(feature = "native-crypto")]
    mod dual_proof {
        use super::*;
        use crate::authority::bls::{self, BlsKeypair, BlsPublicKey, BlsSignature};
        use crate::authority::certificate::DualModeCertificate;

        fn make_bls_keypair(seed: u8) -> BlsKeypair {
            let mut ikm = [0u8; 32];
            ikm[0] = seed;
            ikm[31] = seed.wrapping_add(42);
            BlsKeypair::generate(&ikm)
        }

        /// Build a BLS aggregate certificate signed by `signers` authorities
        /// out of `total`, with all keys registered in the registry.
        fn make_bls_cert(
            signers: usize,
            registered: usize,
        ) -> (DualModeCertificate, KeysetRegistry, Vec<BlsKeypair>) {
            let kr = sample_kr();
            let hlc = sample_hlc();
            let pv = sample_pv();
            let message = create_certificate_message(&kr, &hlc, &pv);

            let keypairs: Vec<BlsKeypair> = (0..registered.max(signers) as u8)
                .map(|i| make_bls_keypair(i + 1))
                .collect();

            let mut registry = KeysetRegistry::new();
            let mut ed_keys = Vec::new();
            let mut bls_keys: Vec<(String, BlsPublicKey, bls::BlsProofOfPossession)> = Vec::new();
            for (i, kp) in keypairs.iter().enumerate().take(registered) {
                let (_, vk) = make_key_pair();
                ed_keys.push((NodeId(format!("auth-{i}")), vk));
                bls_keys.push((
                    format!("auth-{i}"),
                    kp.public_key.clone(),
                    kp.proof_of_possession(),
                ));
            }
            registry
                .register_keyset(KeysetVersion(1), 0, ed_keys)
                .unwrap();
            registry
                .register_bls_keys(&KeysetVersion(1), bls_keys)
                .unwrap();

            let mut cert = DualModeCertificate::new_bls(kr, hlc, pv, KeysetVersion(1));
            let sigs: Vec<BlsSignature> = keypairs
                .iter()
                .take(signers)
                .map(|kp| bls::sign_message(kp.secret_key(), &message))
                .collect();
            let agg = bls::aggregate_signatures(&sigs).unwrap();
            let pairs: Vec<(NodeId, BlsPublicKey)> = keypairs
                .iter()
                .enumerate()
                .take(signers)
                .map(|(i, kp)| (NodeId(format!("auth-{i}")), kp.public_key.clone()))
                .collect();
            cert.set_bls_aggregate(pairs, agg);

            (cert, registry, keypairs)
        }

        #[test]
        fn dual_proof_valid_at_majority() {
            let (cert, registry, _) = make_bls_cert(2, 3);
            let result = verify_dual_proof_with_registry(
                &cert,
                3,
                &registry,
                0,
                &EpochConfig::default(),
                None,
                0,
            );
            assert!(result.valid);
            assert!(result.has_majority);
            assert_eq!(result.signatures_valid, Some(true));
            assert_eq!(result.contributing_count, 2);
            assert_eq!(result.required_count, 2);
        }

        #[test]
        fn dual_proof_rejects_genuinely_signed_noncanonical_checkpoint() {
            let (mut cert, registry, keypairs) = make_bls_cert(2, 3);
            cert.frontier_hlc.logical = 1;
            let message = create_certificate_message(
                &cert.key_range,
                &cert.frontier_hlc,
                &cert.policy_version,
            );
            let signatures: Vec<_> = keypairs
                .iter()
                .take(2)
                .map(|kp| bls::sign_message(kp.secret_key(), &message))
                .collect();
            cert.bls_aggregated_signature = Some(bls::aggregate_signatures(&signatures).unwrap());
            let result = verify_dual_proof_with_registry(
                &cert,
                3,
                &registry,
                0,
                &EpochConfig::default(),
                None,
                0,
            );
            assert!(!result.valid);
            assert_eq!(result.signatures_valid, Some(false));
        }

        #[test]
        fn dual_proof_majority_judged_independently_of_signatures() {
            // 1 signer of 3: signatures are valid but majority is missing.
            let (cert, registry, _) = make_bls_cert(1, 3);
            let result = verify_dual_proof_with_registry(
                &cert,
                3,
                &registry,
                0,
                &EpochConfig::default(),
                None,
                0,
            );
            assert!(!result.valid);
            assert!(!result.has_majority);
            assert_eq!(result.signatures_valid, Some(true));
        }

        #[test]
        fn dual_proof_rejects_duplicate_signers() {
            let (mut cert, registry, keypairs) = make_bls_cert(2, 3);
            // Duplicate the first signer to inflate the count.
            cert.bls_signer_ids.push(NodeId("auth-0".into()));
            cert.bls_public_keys.push(keypairs[0].public_key.clone());
            let result = verify_dual_proof_with_registry(
                &cert,
                3,
                &registry,
                0,
                &EpochConfig::default(),
                None,
                0,
            );
            assert!(!result.valid);
            assert_eq!(result.signatures_valid, Some(false));
        }

        #[test]
        fn dual_proof_rejects_registry_key_mismatch() {
            let (mut cert, registry, _) = make_bls_cert(2, 3);
            // Swap in a key that is not the registered one for auth-0.
            cert.bls_public_keys[0] = make_bls_keypair(99).public_key;
            let result = verify_dual_proof_with_registry(
                &cert,
                3,
                &registry,
                0,
                &EpochConfig::default(),
                None,
                0,
            );
            assert!(!result.valid);
            assert_eq!(result.signatures_valid, Some(false));
        }

        #[test]
        fn dual_proof_rejects_expired_format_version() {
            let (mut cert, registry, _) = make_bls_cert(2, 3);
            cert.format_version = 1;
            let fc = FormatVersionConfig {
                grace_period_secs: 100,
            };

            // Within grace: accepted.
            let result = verify_dual_proof_with_registry(
                &cert,
                3,
                &registry,
                0,
                &EpochConfig::default(),
                Some(&fc),
                50,
            );
            assert!(result.valid);

            // Beyond grace: rejected.
            let result = verify_dual_proof_with_registry(
                &cert,
                3,
                &registry,
                0,
                &EpochConfig::default(),
                Some(&fc),
                101,
            );
            assert!(!result.valid);
            assert_eq!(result.signatures_valid, Some(false));
        }
    }

    #[cfg(not(feature = "native-crypto"))]
    #[test]
    fn dual_proof_always_invalid_without_native_crypto() {
        use crate::authority::bls_stub::{BlsPublicKey, BlsSignature};
        use crate::authority::certificate::DualModeCertificate;

        let mut cert =
            DualModeCertificate::new_bls(sample_kr(), sample_hlc(), sample_pv(), KeysetVersion(1));
        cert.set_bls_aggregate(
            vec![
                (NodeId("auth-0".into()), BlsPublicKey("aa".repeat(48))),
                (NodeId("auth-1".into()), BlsPublicKey("bb".repeat(48))),
            ],
            BlsSignature("cc".repeat(96)),
        );

        let mut registry = KeysetRegistry::new();
        let (_, vk) = make_key_pair();
        registry
            .register_keyset(KeysetVersion(1), 0, vec![(NodeId("auth-0".into()), vk)])
            .unwrap();

        let result = verify_dual_proof_with_registry(
            &cert,
            3,
            &registry,
            0,
            &EpochConfig::default(),
            None,
            0,
        );
        assert!(
            !result.valid,
            "BLS verification must be unavailable without native-crypto"
        );
        assert_eq!(result.signatures_valid, Some(false));
    }
}
