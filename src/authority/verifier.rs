use serde::Serialize;

use crate::api::certified::ProofBundle;
use crate::authority::certificate::{
    CertError, DualModeCertificate, EpochConfig, FormatVersionConfig, KeysetRegistry,
    create_certificate_message,
};

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
///
/// External clients can use this to verify certification without trusting
/// the node that returned the proof.
pub fn verify_proof(
    bundle: &ProofBundle,
    format_config: Option<&FormatVersionConfig>,
    elapsed_since_upgrade_secs: u64,
) -> VerificationResult {
    let required = bundle.total_authorities / 2 + 1;

    // A proof without a certificate is always invalid — a caller could
    // fabricate a "valid" proof by simply listing enough authority IDs.
    let (contributing_count, signatures_valid) = match bundle.certificate.as_ref() {
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

    let has_majority = contributing_count >= required;
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
/// Extends `verify_proof` by additionally checking:
/// 1. Each signature's keyset version is known in the registry.
/// 2. Each signature's keyset version is within the epoch grace period.
/// 3. Signatures are verified against the registry's public keys
///    (not just the embedded keys in the certificate).
/// 4. If `format_config` is provided, the certificate's format version is
///    checked against the grace-period policy.
///
/// Returns a `VerificationResult` with an optional `keyset_error` if
/// any keyset/epoch validation fails.
pub fn verify_proof_with_registry(
    bundle: &ProofBundle,
    registry: &KeysetRegistry,
    current_epoch: u64,
    epoch_config: &EpochConfig,
    format_config: Option<&FormatVersionConfig>,
    elapsed_since_upgrade_secs: u64,
) -> VerificationResult {
    let required = bundle.total_authorities / 2 + 1;

    // A proof without a certificate is always invalid.
    let (contributing_count, signatures_valid) = match bundle.certificate.as_ref() {
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

    let has_majority = contributing_count >= required;
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
/// `has_majority` is judged independently from `signer_count()`, mirroring
/// the Ed25519 verifier: `valid` requires both majority and valid signatures.
/// When `format_config` is provided, the certificate's format version is
/// checked first; an unacceptable version yields `signatures_valid = Some(false)`.
pub fn verify_dual_proof_with_registry(
    cert: &DualModeCertificate,
    total_authorities: usize,
    registry: &KeysetRegistry,
    current_epoch: u64,
    epoch_config: &EpochConfig,
    format_config: Option<&FormatVersionConfig>,
    elapsed_since_upgrade_secs: u64,
) -> VerificationResult {
    let required = total_authorities / 2 + 1;

    let format_ok = format_config
        .map(|fc| fc.is_version_acceptable(cert.format_version, elapsed_since_upgrade_secs))
        .unwrap_or(true);

    let message =
        create_certificate_message(&cert.key_range, &cert.frontier_hlc, &cert.policy_version);

    let (contributing_count, signatures_valid) = if !format_ok {
        (cert.signer_count(), Some(false))
    } else {
        match cert.verify_with_registry(&message, registry, current_epoch, epoch_config) {
            Ok(verified_signers) => (verified_signers.len(), Some(true)),
            Err(_) => (cert.signer_count(), Some(false)),
        }
    };

    let has_majority = contributing_count >= required;
    let valid = has_majority && signatures_valid == Some(true);

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
    registry: &KeysetRegistry,
    current_epoch: u64,
    epoch_config: &EpochConfig,
    format_config: Option<&FormatVersionConfig>,
    elapsed_since_upgrade_secs: u64,
) -> Result<VerificationResult, CertError> {
    let required = bundle.total_authorities / 2 + 1;

    let (contributing_count, signatures_valid) = if let Some(cert) = &bundle.certificate {
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
            logical: 42,
            node_id: "node-1".into(),
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

    #[test]
    fn proof_without_certificate_is_rejected() {
        let proof = make_proof(3, 5, false);
        let result = verify_proof(&proof, None, 0);

        assert!(!result.valid);
        assert!(result.has_majority);
        assert_eq!(result.contributing_count, 3);
        assert_eq!(result.required_count, 3); // 5/2+1 = 3
        assert!(result.signatures_valid.is_none());
    }

    #[test]
    fn proof_with_insufficient_authorities_fails() {
        let proof = make_proof(2, 5, true);
        let result = verify_proof(&proof, None, 0);

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

        let result = verify_proof(&proof, None, 0);
        assert!(!result.valid);
        assert!(!result.has_majority);
        assert_eq!(result.contributing_count, 2);
        assert_eq!(result.required_count, 3);
    }

    #[test]
    fn valid_proof_with_certificate() {
        let proof = make_proof(3, 5, true);
        let result = verify_proof(&proof, None, 0);

        assert!(result.valid);
        assert!(result.has_majority);
        assert_eq!(result.signatures_valid, Some(true));
    }

    #[test]
    fn certificate_with_tampered_signature_fails() {
        let mut proof = make_proof(3, 5, true);

        if let Some(cert) = &mut proof.certificate {
            let (sk, _vk) = make_key_pair();
            let bad_sig = sign_message(&sk, b"wrong message");
            cert.signatures[0].signature = bad_sig;
        }

        let result = verify_proof(&proof, None, 0);
        assert!(!result.valid);
        assert!(result.has_majority);
        assert_eq!(result.signatures_valid, Some(false));
    }

    #[test]
    fn exact_majority_threshold() {
        let proof = make_proof(1, 1, true);
        assert!(verify_proof(&proof, None, 0).valid);

        let proof = make_proof(2, 3, true);
        assert!(verify_proof(&proof, None, 0).valid);

        let proof = make_proof(1, 3, true);
        assert!(!verify_proof(&proof, None, 0).valid);
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
    fn verify_with_registry_valid_proof() {
        let (proof, registry) = make_proof_with_registry(3, 5, 1);
        let config = EpochConfig::default();
        let result = verify_proof_with_registry(&proof, &registry, 0, &config, None, 0);

        assert!(result.valid);
        assert!(result.has_majority);
        assert_eq!(result.signatures_valid, Some(true));
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

        let result = verify_proof_with_registry(&proof, &registry, 4, &config, None, 0);
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

        let result = verify_proof_with_registry_detailed(&proof, &registry, 4, &config, None, 0);
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
        let result = verify_proof_with_registry(&proof, &registry, 0, &config, None, 0);
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

        let result = verify_proof_with_registry(&bundle, &registry, 6, &config, None, 0);
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
        let result = verify_proof(&proof, Some(&fc), 50);
        assert!(result.valid);
        assert_eq!(result.signatures_valid, Some(true));

        // Beyond grace period: rejected.
        let result = verify_proof(&proof, Some(&fc), 101);
        assert!(!result.valid);
        assert_eq!(result.signatures_valid, Some(false));
    }

    #[test]
    fn verifier_accepts_current_format_regardless_of_elapsed() {
        let proof = make_proof(3, 5, true);

        let fc = FormatVersionConfig {
            grace_period_secs: 0,
        };

        let result = verify_proof(&proof, Some(&fc), 999_999);
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

        let result = verify_proof_with_registry(&proof, &registry, 0, &epoch_config, Some(&fc), 50);
        assert!(result.valid);

        let result =
            verify_proof_with_registry(&proof, &registry, 0, &epoch_config, Some(&fc), 101);
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

        let result = verify_proof(&proof, None, 999_999);
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
