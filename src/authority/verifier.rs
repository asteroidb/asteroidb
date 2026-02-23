use serde::Serialize;

use crate::api::certified::ProofBundle;
use crate::authority::certificate::create_certificate_message;

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
///
/// External clients can use this to verify certification without trusting
/// the node that returned the proof.
pub fn verify_proof(bundle: &ProofBundle) -> VerificationResult {
    let required = bundle.total_authorities / 2 + 1;
    let has_majority = bundle.contributing_authorities.len() >= required;

    let signatures_valid = bundle.certificate.as_ref().map(|cert| {
        let message = create_certificate_message(
            &bundle.key_range,
            &bundle.frontier_hlc,
            &bundle.policy_version,
        );
        cert.verify_signatures(&message).is_ok()
    });

    let valid = has_majority && signatures_valid.unwrap_or(true);

    VerificationResult {
        valid,
        has_majority,
        contributing_count: bundle.contributing_authorities.len(),
        required_count: required,
        signatures_valid,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::certified::ProofBundle;
    use crate::authority::certificate::{
        AuthoritySignature, KeysetVersion, MajorityCertificate, create_certificate_message,
        sign_message,
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
    fn valid_proof_passes_verification() {
        let proof = make_proof(3, 5, false);
        let result = verify_proof(&proof);

        assert!(result.valid);
        assert!(result.has_majority);
        assert_eq!(result.contributing_count, 3);
        assert_eq!(result.required_count, 3); // 5/2+1 = 3
        assert!(result.signatures_valid.is_none());
    }

    #[test]
    fn proof_with_insufficient_authorities_fails() {
        let proof = make_proof(2, 5, false);
        let result = verify_proof(&proof);

        assert!(!result.valid);
        assert!(!result.has_majority);
        assert_eq!(result.contributing_count, 2);
        assert_eq!(result.required_count, 3);
    }

    #[test]
    fn tampered_proof_detected() {
        let mut proof = make_proof(3, 5, false);

        // Tamper: add an extra fake authority to inflate the count.
        proof
            .contributing_authorities
            .push(NodeId("fake-auth".into()));
        // But total_authorities stays at 5, so this still passes majority check.
        let result = verify_proof(&proof);
        assert!(result.valid);
        assert_eq!(result.contributing_count, 4);

        // Now tamper the other way: reduce contributing below majority.
        proof.contributing_authorities.truncate(2);
        let result = verify_proof(&proof);
        assert!(!result.valid);
        assert!(!result.has_majority);
    }

    #[test]
    fn valid_proof_with_certificate() {
        let proof = make_proof(3, 5, true);
        let result = verify_proof(&proof);

        assert!(result.valid);
        assert!(result.has_majority);
        assert_eq!(result.signatures_valid, Some(true));
    }

    #[test]
    fn certificate_with_tampered_signature_fails() {
        let mut proof = make_proof(3, 5, true);

        // Tamper: swap the signature of the first authority with a different one.
        if let Some(cert) = &mut proof.certificate {
            let (sk, _vk) = make_key_pair();
            let bad_sig = sign_message(&sk, b"wrong message");
            cert.signatures[0].signature = bad_sig;
        }

        let result = verify_proof(&proof);
        assert!(!result.valid);
        assert!(result.has_majority);
        assert_eq!(result.signatures_valid, Some(false));
    }

    #[test]
    fn exact_majority_threshold() {
        // 1 of 1 = majority
        let proof = make_proof(1, 1, false);
        assert!(verify_proof(&proof).valid);

        // 2 of 3 = majority (3/2+1 = 2)
        let proof = make_proof(2, 3, false);
        assert!(verify_proof(&proof).valid);

        // 1 of 3 = not majority
        let proof = make_proof(1, 3, false);
        assert!(!verify_proof(&proof).valid);
    }
}
