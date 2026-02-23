use ed25519_dalek::{Signature, SigningKey, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::hlc::HlcTimestamp;
use crate::types::{KeyRange, NodeId, PolicyVersion};

/// Custom serde for `VerifyingKey` using hex encoding.
mod hex_verifying_key {
    use ed25519_dalek::VerifyingKey;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(key: &VerifyingKey, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let bytes = key.as_bytes();
        let hex_string: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        serializer.serialize_str(&hex_string)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<VerifyingKey, D::Error>
    where
        D: Deserializer<'de>,
    {
        let hex_string = String::deserialize(deserializer)?;
        let bytes = hex_to_bytes(&hex_string).map_err(serde::de::Error::custom)?;
        let byte_array: [u8; 32] = bytes
            .try_into()
            .map_err(|_| serde::de::Error::custom("expected 32 bytes for VerifyingKey"))?;
        VerifyingKey::from_bytes(&byte_array).map_err(serde::de::Error::custom)
    }

    fn hex_to_bytes(hex: &str) -> Result<Vec<u8>, String> {
        if !hex.len().is_multiple_of(2) {
            return Err("odd-length hex string".to_string());
        }
        (0..hex.len())
            .step_by(2)
            .map(|i| {
                u8::from_str_radix(&hex[i..i + 2], 16)
                    .map_err(|e| format!("invalid hex character: {e}"))
            })
            .collect()
    }
}

/// Custom serde for `Signature` using hex encoding.
mod hex_signature {
    use ed25519_dalek::Signature;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(sig: &Signature, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let bytes = sig.to_bytes();
        let hex_string: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        serializer.serialize_str(&hex_string)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Signature, D::Error>
    where
        D: Deserializer<'de>,
    {
        let hex_string = String::deserialize(deserializer)?;
        let bytes = hex_to_bytes(&hex_string).map_err(serde::de::Error::custom)?;
        let byte_array: [u8; 64] = bytes
            .try_into()
            .map_err(|_| serde::de::Error::custom("expected 64 bytes for Signature"))?;
        Ok(Signature::from_bytes(&byte_array))
    }

    fn hex_to_bytes(hex: &str) -> Result<Vec<u8>, String> {
        if !hex.len().is_multiple_of(2) {
            return Err("odd-length hex string".to_string());
        }
        (0..hex.len())
            .step_by(2)
            .map(|i| {
                u8::from_str_radix(&hex[i..i + 2], 16)
                    .map_err(|e| format!("invalid hex character: {e}"))
            })
            .collect()
    }
}

/// Error type for certificate operations.
#[derive(Debug, Error)]
pub enum CertError {
    #[error("insufficient signatures: {got}/{needed}")]
    InsufficientSignatures { got: usize, needed: usize },

    #[error("invalid signature from {0}")]
    InvalidSignature(String),

    #[error("keyset version too old: {0}")]
    StaleKeyset(u64),
}

/// Keyset version for key rotation management.
///
/// Starts at 1 and monotonically increases on each rotation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct KeysetVersion(pub u64);

/// Epoch configuration for key rotation.
#[derive(Debug, Clone)]
pub struct EpochConfig {
    /// Duration of one epoch in seconds. Default: 86400 (24h).
    pub duration_secs: u64,
    /// Number of past epochs whose keys are still accepted. Default: 7.
    pub grace_epochs: u64,
}

impl Default for EpochConfig {
    fn default() -> Self {
        Self {
            duration_secs: 86400,
            grace_epochs: 7,
        }
    }
}

/// A single authority's signature over a certified data range.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthoritySignature {
    /// The authority node that produced this signature.
    pub authority_id: NodeId,
    /// The public key used for verification.
    #[serde(with = "hex_verifying_key")]
    pub public_key: VerifyingKey,
    /// The Ed25519 signature.
    #[serde(with = "hex_signature")]
    pub signature: Signature,
}

/// A majority certificate proving Authority consensus on a key range.
///
/// Aggregates individual Ed25519 signatures from authority nodes.
/// A certificate is considered valid when it holds signatures from
/// a strict majority of the authority set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MajorityCertificate {
    /// The key range this certificate covers.
    pub key_range: KeyRange,
    /// The HLC frontier timestamp at the time of certification.
    pub frontier_hlc: HlcTimestamp,
    /// The policy version under which this certificate was issued.
    pub policy_version: PolicyVersion,
    /// The keyset version used for signing.
    pub keyset_version: KeysetVersion,
    /// Collected authority signatures.
    pub signatures: Vec<AuthoritySignature>,
}

impl MajorityCertificate {
    /// Create a new certificate with no signatures.
    pub fn new(
        key_range: KeyRange,
        frontier_hlc: HlcTimestamp,
        policy_version: PolicyVersion,
        keyset_version: KeysetVersion,
    ) -> Self {
        Self {
            key_range,
            frontier_hlc,
            policy_version,
            keyset_version,
            signatures: Vec::new(),
        }
    }

    /// Add a signature from an authority node.
    ///
    /// Duplicate signatures from the same authority are silently ignored
    /// to prevent a single authority from inflating the majority count.
    pub fn add_signature(&mut self, sig: AuthoritySignature) {
        if self
            .signatures
            .iter()
            .any(|s| s.authority_id == sig.authority_id)
        {
            return;
        }
        self.signatures.push(sig);
    }

    /// Return the number of unique signers.
    pub fn signature_count(&self) -> usize {
        self.unique_signer_count()
    }

    /// Check whether a strict majority of unique authorities have signed.
    ///
    /// Majority threshold is `total_authorities / 2 + 1`.
    pub fn has_majority(&self, total_authorities: usize) -> bool {
        let needed = majority_threshold(total_authorities);
        self.unique_signer_count() >= needed
    }

    /// Count unique authority IDs among collected signatures.
    fn unique_signer_count(&self) -> usize {
        let unique: std::collections::HashSet<&NodeId> =
            self.signatures.iter().map(|s| &s.authority_id).collect();
        unique.len()
    }

    /// Verify all signatures against the given message bytes.
    ///
    /// Returns the list of authority IDs whose signatures are valid.
    /// Returns an error if any signature fails verification.
    pub fn verify_signatures(&self, message: &[u8]) -> Result<Vec<NodeId>, CertError> {
        let mut valid_signers = Vec::new();
        for sig in &self.signatures {
            sig.public_key
                .verify(message, &sig.signature)
                .map_err(|_| CertError::InvalidSignature(sig.authority_id.0.clone()))?;
            valid_signers.push(sig.authority_id.clone());
        }
        Ok(valid_signers)
    }

    /// Return references to the authority IDs that have signed.
    pub fn signers(&self) -> Vec<&NodeId> {
        self.signatures.iter().map(|s| &s.authority_id).collect()
    }
}

/// Compute the majority threshold for a given number of authorities.
///
/// `threshold = total / 2 + 1`
fn majority_threshold(total: usize) -> usize {
    total / 2 + 1
}

/// Create the canonical message bytes for certificate signing.
///
/// The message is a deterministic serialization of the key range,
/// frontier HLC, and policy version.
pub fn create_certificate_message(
    key_range: &KeyRange,
    frontier_hlc: &HlcTimestamp,
    policy_version: &PolicyVersion,
) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(key_range.prefix.as_bytes());
    buf.extend_from_slice(&frontier_hlc.physical.to_be_bytes());
    buf.extend_from_slice(&frontier_hlc.logical.to_be_bytes());
    buf.extend_from_slice(frontier_hlc.node_id.as_bytes());
    buf.extend_from_slice(&policy_version.0.to_be_bytes());
    buf
}

/// Sign a message with an Ed25519 signing key.
pub fn sign_message(signing_key: &SigningKey, message: &[u8]) -> Signature {
    use ed25519_dalek::Signer;
    signing_key.sign(message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;

    fn make_key_pair() -> (SigningKey, VerifyingKey) {
        let sk = SigningKey::generate(&mut OsRng);
        let vk = sk.verifying_key();
        (sk, vk)
    }

    fn sample_key_range() -> KeyRange {
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

    fn sample_policy_version() -> PolicyVersion {
        PolicyVersion(1)
    }

    #[test]
    fn sign_and_verify_single() {
        let (sk, vk) = make_key_pair();
        let message = create_certificate_message(
            &sample_key_range(),
            &sample_hlc(),
            &sample_policy_version(),
        );

        let sig = sign_message(&sk, &message);
        assert!(vk.verify(&message, &sig).is_ok());
    }

    #[test]
    fn certificate_has_majority_3_of_5() {
        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();
        let message = create_certificate_message(&kr, &hlc, &pv);

        let mut cert = MajorityCertificate::new(kr, hlc, pv, KeysetVersion(1));

        // Add 3 valid signatures out of 5 authorities.
        for i in 0..3 {
            let (sk, vk) = make_key_pair();
            let sig = sign_message(&sk, &message);
            cert.add_signature(AuthoritySignature {
                authority_id: NodeId(format!("auth-{i}")),
                public_key: vk,
                signature: sig,
            });
        }

        assert_eq!(cert.signature_count(), 3);
        assert!(cert.has_majority(5)); // 5/2 + 1 = 3
    }

    #[test]
    fn certificate_no_majority_2_of_5() {
        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();
        let message = create_certificate_message(&kr, &hlc, &pv);

        let mut cert = MajorityCertificate::new(kr, hlc, pv, KeysetVersion(1));

        for i in 0..2 {
            let (sk, vk) = make_key_pair();
            let sig = sign_message(&sk, &message);
            cert.add_signature(AuthoritySignature {
                authority_id: NodeId(format!("auth-{i}")),
                public_key: vk,
                signature: sig,
            });
        }

        assert_eq!(cert.signature_count(), 2);
        assert!(!cert.has_majority(5)); // 2 < 3 needed
    }

    #[test]
    fn verify_signatures_all_valid() {
        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();
        let message = create_certificate_message(&kr, &hlc, &pv);

        let mut cert = MajorityCertificate::new(kr, hlc, pv, KeysetVersion(1));

        let mut expected_ids = Vec::new();
        for i in 0..3 {
            let (sk, vk) = make_key_pair();
            let sig = sign_message(&sk, &message);
            let id = NodeId(format!("auth-{i}"));
            expected_ids.push(id.clone());
            cert.add_signature(AuthoritySignature {
                authority_id: id,
                public_key: vk,
                signature: sig,
            });
        }

        let valid = cert.verify_signatures(&message).unwrap();
        assert_eq!(valid, expected_ids);
    }

    #[test]
    fn verify_signatures_detects_tampered() {
        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();
        let message = create_certificate_message(&kr, &hlc, &pv);

        let mut cert = MajorityCertificate::new(kr, hlc, pv, KeysetVersion(1));

        // Add a valid signature.
        let (sk, vk) = make_key_pair();
        let sig = sign_message(&sk, &message);
        cert.add_signature(AuthoritySignature {
            authority_id: NodeId("good-auth".into()),
            public_key: vk,
            signature: sig,
        });

        // Add a signature signed with a different key but presented with wrong public key.
        let (sk2, _vk2) = make_key_pair();
        let (_sk3, vk3) = make_key_pair();
        let bad_sig = sign_message(&sk2, &message);
        cert.add_signature(AuthoritySignature {
            authority_id: NodeId("bad-auth".into()),
            public_key: vk3, // mismatched key
            signature: bad_sig,
        });

        let result = cert.verify_signatures(&message);
        assert!(result.is_err());
        match result.unwrap_err() {
            CertError::InvalidSignature(id) => assert_eq!(id, "bad-auth"),
            other => panic!("expected InvalidSignature, got: {other}"),
        }
    }

    #[test]
    fn signers_returns_authority_ids() {
        let mut cert = MajorityCertificate::new(
            sample_key_range(),
            sample_hlc(),
            sample_policy_version(),
            KeysetVersion(1),
        );

        let message =
            create_certificate_message(&cert.key_range, &cert.frontier_hlc, &cert.policy_version);

        for name in ["alpha", "beta", "gamma"] {
            let (sk, vk) = make_key_pair();
            let sig = sign_message(&sk, &message);
            cert.add_signature(AuthoritySignature {
                authority_id: NodeId(name.into()),
                public_key: vk,
                signature: sig,
            });
        }

        let signer_ids: Vec<&str> = cert.signers().iter().map(|n| n.0.as_str()).collect();
        assert_eq!(signer_ids, vec!["alpha", "beta", "gamma"]);
    }

    #[test]
    fn keyset_version_ordering() {
        let v1 = KeysetVersion(1);
        let v2 = KeysetVersion(2);
        let v3 = KeysetVersion(3);
        assert!(v1 < v2);
        assert!(v2 < v3);
        assert_eq!(v1, KeysetVersion(1));
    }

    #[test]
    fn epoch_config_defaults() {
        let config = EpochConfig::default();
        assert_eq!(config.duration_secs, 86400); // 24 hours
        assert_eq!(config.grace_epochs, 7);
    }

    #[test]
    fn majority_threshold_values() {
        // 1 node: need 1
        assert_eq!(majority_threshold(1), 1);
        // 3 nodes: need 2
        assert_eq!(majority_threshold(3), 2);
        // 5 nodes: need 3
        assert_eq!(majority_threshold(5), 3);
        // 7 nodes: need 4
        assert_eq!(majority_threshold(7), 4);
    }

    #[test]
    fn empty_certificate() {
        let cert = MajorityCertificate::new(
            sample_key_range(),
            sample_hlc(),
            sample_policy_version(),
            KeysetVersion(1),
        );

        assert_eq!(cert.signature_count(), 0);
        assert!(!cert.has_majority(5));
        assert!(cert.signers().is_empty());
    }

    #[test]
    fn create_certificate_message_deterministic() {
        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();

        let msg1 = create_certificate_message(&kr, &hlc, &pv);
        let msg2 = create_certificate_message(&kr, &hlc, &pv);
        assert_eq!(msg1, msg2);
    }

    #[test]
    fn keyset_version_serde_roundtrip() {
        let v = KeysetVersion(42);
        let json = serde_json::to_string(&v).unwrap();
        let back: KeysetVersion = serde_json::from_str(&json).unwrap();
        assert_eq!(v, back);
    }

    #[test]
    fn duplicate_signature_same_authority_ignored() {
        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();
        let message = create_certificate_message(&kr, &hlc, &pv);

        let mut cert = MajorityCertificate::new(kr, hlc, pv, KeysetVersion(1));

        let (sk, vk) = make_key_pair();
        let sig = sign_message(&sk, &message);
        cert.add_signature(AuthoritySignature {
            authority_id: NodeId("auth-1".into()),
            public_key: vk,
            signature: sig,
        });

        // Add a second signature from the same authority (should be ignored)
        let (sk2, vk2) = make_key_pair();
        let sig2 = sign_message(&sk2, &message);
        cert.add_signature(AuthoritySignature {
            authority_id: NodeId("auth-1".into()),
            public_key: vk2,
            signature: sig2,
        });

        assert_eq!(cert.signature_count(), 1);
        assert!(!cert.has_majority(3)); // 1 < 2 needed
    }

    #[test]
    fn duplicate_signatures_do_not_inflate_majority() {
        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();
        let message = create_certificate_message(&kr, &hlc, &pv);

        let mut cert = MajorityCertificate::new(kr, hlc, pv, KeysetVersion(1));

        // Add one legitimate signature
        let (sk1, vk1) = make_key_pair();
        let sig1 = sign_message(&sk1, &message);
        cert.add_signature(AuthoritySignature {
            authority_id: NodeId("auth-1".into()),
            public_key: vk1,
            signature: sig1,
        });

        // Try to add the same authority 4 more times (all should be ignored)
        for _ in 0..4 {
            let (sk, vk) = make_key_pair();
            let sig = sign_message(&sk, &message);
            cert.add_signature(AuthoritySignature {
                authority_id: NodeId("auth-1".into()),
                public_key: vk,
                signature: sig,
            });
        }

        // Still only 1 unique signer, cannot reach majority of 3
        assert_eq!(cert.signature_count(), 1);
        assert!(!cert.has_majority(3));

        // Now add a genuinely different authority
        let (sk2, vk2) = make_key_pair();
        let sig2 = sign_message(&sk2, &message);
        cert.add_signature(AuthoritySignature {
            authority_id: NodeId("auth-2".into()),
            public_key: vk2,
            signature: sig2,
        });

        assert_eq!(cert.signature_count(), 2);
        assert!(cert.has_majority(3)); // 2 >= 3/2+1 = 2
    }

    #[test]
    fn certificate_serde_roundtrip() {
        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();
        let message = create_certificate_message(&kr, &hlc, &pv);

        let mut cert = MajorityCertificate::new(kr, hlc, pv, KeysetVersion(1));

        for i in 0..3 {
            let (sk, vk) = make_key_pair();
            let sig = sign_message(&sk, &message);
            cert.add_signature(AuthoritySignature {
                authority_id: NodeId(format!("auth-{i}")),
                public_key: vk,
                signature: sig,
            });
        }

        // Serialize to JSON.
        let json = serde_json::to_string(&cert).expect("serialize certificate");

        // Deserialize back.
        let restored: MajorityCertificate =
            serde_json::from_str(&json).expect("deserialize certificate");

        // Verify structural equality.
        assert_eq!(restored.key_range, cert.key_range);
        assert_eq!(restored.frontier_hlc, cert.frontier_hlc);
        assert_eq!(restored.policy_version, cert.policy_version);
        assert_eq!(restored.keyset_version, cert.keyset_version);
        assert_eq!(restored.signatures.len(), cert.signatures.len());

        // Verify the restored certificate can still verify signatures.
        let valid = restored.verify_signatures(&message).unwrap();
        assert_eq!(valid.len(), 3);
    }

    #[test]
    fn authority_signature_serde_roundtrip() {
        let (sk, vk) = make_key_pair();
        let message = b"test message";
        let sig = sign_message(&sk, message);

        let auth_sig = AuthoritySignature {
            authority_id: NodeId("auth-1".into()),
            public_key: vk,
            signature: sig,
        };

        let json = serde_json::to_string(&auth_sig).expect("serialize");
        let restored: AuthoritySignature = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(restored.authority_id, auth_sig.authority_id);
        assert_eq!(restored.public_key, auth_sig.public_key);
        assert_eq!(restored.signature, auth_sig.signature);
    }
}
