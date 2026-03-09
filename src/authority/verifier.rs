use serde::Serialize;

use crate::api::certified::ProofBundle;
use crate::authority::certificate::{
    CertError, EpochConfig, FormatVersionConfig, KeysetRegistry, create_certificate_message,
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
        match result {
            Ok(verified_signers) => (verified_signers.len(), Some(true)),
            Err(e) => return Err(e),
        }
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
}
