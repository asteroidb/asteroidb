use std::collections::BTreeMap;

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

    #[error("unknown keyset version: {0}")]
    UnknownKeyset(u64),

    #[error(
        "expired keyset version {version} (epoch {keyset_epoch} outside grace of current epoch {current_epoch})"
    )]
    ExpiredKeyset {
        version: u64,
        keyset_epoch: u64,
        current_epoch: u64,
    },
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

/// Registry mapping keyset versions to their public keys.
///
/// Manages the mapping from `KeysetVersion` to the set of authority
/// public keys valid for that version. Supports key rotation by allowing
/// multiple versions to coexist during grace periods.
#[derive(Debug, Clone)]
pub struct KeysetRegistry {
    /// Maps keyset version to (registration epoch, authority keys).
    keysets: BTreeMap<u64, KeysetEntry>,
    /// Current (latest) keyset version.
    current: u64,
}

/// Internal entry for a registered keyset.
#[derive(Debug, Clone)]
struct KeysetEntry {
    /// Epoch at which this keyset was registered.
    registered_epoch: u64,
    /// Authority public keys for this version.
    keys: Vec<(NodeId, VerifyingKey)>,
}

impl KeysetRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            keysets: BTreeMap::new(),
            current: 0,
        }
    }

    /// Register a new keyset version with the given authority keys.
    ///
    /// The `version` must be strictly greater than the current version
    /// (monotonic increment). `registered_epoch` records when this keyset
    /// became active.
    pub fn register_keyset(
        &mut self,
        version: KeysetVersion,
        registered_epoch: u64,
        keys: Vec<(NodeId, VerifyingKey)>,
    ) -> Result<(), CertError> {
        if version.0 <= self.current && self.current > 0 {
            return Err(CertError::StaleKeyset(version.0));
        }
        self.keysets.insert(
            version.0,
            KeysetEntry {
                registered_epoch,
                keys,
            },
        );
        self.current = version.0;
        Ok(())
    }

    /// Return the current (latest) keyset version.
    pub fn current_version(&self) -> KeysetVersion {
        KeysetVersion(self.current)
    }

    /// Get the authority keys for a specific keyset version.
    pub fn get_keys(&self, version: &KeysetVersion) -> Option<&[(NodeId, VerifyingKey)]> {
        self.keysets.get(&version.0).map(|e| e.keys.as_slice())
    }

    /// Check whether a keyset version is valid given the current epoch and config.
    ///
    /// A version is valid if its registration epoch is within the grace period
    /// of the current epoch, or if it is the current version.
    pub fn is_version_valid(
        &self,
        version: &KeysetVersion,
        current_epoch: u64,
        config: &EpochConfig,
    ) -> bool {
        if version.0 == self.current {
            return true;
        }
        match self.keysets.get(&version.0) {
            Some(entry) => {
                // The keyset is valid if the current epoch hasn't moved
                // too far past the epoch when this keyset was registered.
                current_epoch <= entry.registered_epoch + config.grace_epochs
            }
            None => false,
        }
    }

    /// Look up the public key for a specific authority in a keyset version.
    pub fn get_key_for_authority(
        &self,
        version: &KeysetVersion,
        authority_id: &NodeId,
    ) -> Option<&VerifyingKey> {
        self.keysets.get(&version.0).and_then(|entry| {
            entry
                .keys
                .iter()
                .find(|(id, _)| id == authority_id)
                .map(|(_, vk)| vk)
        })
    }

    /// Return the registration epoch for a keyset version, if it exists.
    pub fn registered_epoch(&self, version: &KeysetVersion) -> Option<u64> {
        self.keysets.get(&version.0).map(|e| e.registered_epoch)
    }
}

impl Default for KeysetRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Manages epoch computation and keyset rotation lifecycle.
///
/// An epoch is a fixed-duration time window. Key rotation happens at
/// epoch boundaries, with a configurable grace period during which
/// both old and new keys are accepted.
#[derive(Debug, Clone)]
pub struct EpochManager {
    config: EpochConfig,
    registry: KeysetRegistry,
    /// Base timestamp (seconds) from which epochs are counted.
    epoch_base_secs: u64,
}

impl EpochManager {
    /// Create a new epoch manager.
    ///
    /// `epoch_base_secs` is the reference timestamp (in seconds) for epoch 0.
    pub fn new(config: EpochConfig, epoch_base_secs: u64) -> Self {
        Self {
            config,
            registry: KeysetRegistry::new(),
            epoch_base_secs,
        }
    }

    /// Compute the epoch number for a given timestamp in seconds.
    pub fn current_epoch(&self, now_secs: u64) -> u64 {
        if now_secs < self.epoch_base_secs {
            return 0;
        }
        (now_secs - self.epoch_base_secs) / self.config.duration_secs
    }

    /// Check whether a keyset registered at `keyset_epoch` is within
    /// the grace period at `current_epoch`.
    pub fn is_within_grace(&self, keyset_epoch: u64, current_epoch: u64) -> bool {
        if current_epoch <= keyset_epoch {
            return true;
        }
        current_epoch - keyset_epoch <= self.config.grace_epochs
    }

    /// Rotate to a new keyset.
    ///
    /// Registers the new keys under the next version and records the
    /// current epoch. Old keys remain valid for `grace_epochs` after
    /// the epoch in which they were registered.
    pub fn rotate_keyset(
        &mut self,
        now_secs: u64,
        new_keys: Vec<(NodeId, VerifyingKey)>,
    ) -> Result<KeysetVersion, CertError> {
        let epoch = self.current_epoch(now_secs);
        let new_version = KeysetVersion(self.registry.current + 1);
        self.registry
            .register_keyset(new_version.clone(), epoch, new_keys)?;
        Ok(new_version)
    }

    /// Return a reference to the underlying keyset registry.
    pub fn registry(&self) -> &KeysetRegistry {
        &self.registry
    }

    /// Return a reference to the epoch configuration.
    pub fn config(&self) -> &EpochConfig {
        &self.config
    }

    /// Validate that a keyset version is acceptable at the given time.
    ///
    /// Returns `Ok(())` if the version is valid, or an appropriate
    /// `CertError` otherwise.
    pub fn validate_keyset_version(
        &self,
        version: &KeysetVersion,
        now_secs: u64,
    ) -> Result<(), CertError> {
        let current_epoch = self.current_epoch(now_secs);
        if self.registry.get_keys(version).is_none() {
            return Err(CertError::UnknownKeyset(version.0));
        }
        if !self
            .registry
            .is_version_valid(version, current_epoch, &self.config)
        {
            let keyset_epoch = self.registry.registered_epoch(version).unwrap_or(0);
            return Err(CertError::ExpiredKeyset {
                version: version.0,
                keyset_epoch,
                current_epoch,
            });
        }
        Ok(())
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
    /// The keyset version under which this signature was produced.
    #[serde(default = "default_keyset_version")]
    pub keyset_version: KeysetVersion,
}

fn default_keyset_version() -> KeysetVersion {
    KeysetVersion(1)
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

    /// Verify all signatures using a keyset registry for key lookup.
    ///
    /// Each signature's `keyset_version` is used to find the correct
    /// public key from the registry. Signatures with expired or unknown
    /// keyset versions are treated as invalid.
    ///
    /// Returns the list of authority IDs whose signatures are valid.
    pub fn verify_signatures_with_registry(
        &self,
        message: &[u8],
        registry: &KeysetRegistry,
        current_epoch: u64,
        epoch_config: &EpochConfig,
    ) -> Result<Vec<NodeId>, CertError> {
        let mut valid_signers = Vec::new();
        for sig in &self.signatures {
            // Check that the keyset version is known and not expired.
            if !registry.is_version_valid(&sig.keyset_version, current_epoch, epoch_config) {
                if registry.get_keys(&sig.keyset_version).is_none() {
                    return Err(CertError::UnknownKeyset(sig.keyset_version.0));
                }
                let entry_epoch = registry.registered_epoch(&sig.keyset_version).unwrap_or(0);
                return Err(CertError::ExpiredKeyset {
                    version: sig.keyset_version.0,
                    keyset_epoch: entry_epoch,
                    current_epoch,
                });
            }

            // Look up the expected public key from the registry.
            let expected_key =
                registry.get_key_for_authority(&sig.keyset_version, &sig.authority_id);

            // If the registry has a key for this authority, verify against it;
            // otherwise fall back to the embedded public key.
            let verify_key = expected_key.unwrap_or(&sig.public_key);
            verify_key
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

    fn make_auth_sig(authority_id: NodeId, vk: VerifyingKey, sig: Signature) -> AuthoritySignature {
        AuthoritySignature {
            authority_id,
            public_key: vk,
            signature: sig,
            keyset_version: KeysetVersion(1),
        }
    }

    fn make_auth_sig_v(
        authority_id: NodeId,
        vk: VerifyingKey,
        sig: Signature,
        version: u64,
    ) -> AuthoritySignature {
        AuthoritySignature {
            authority_id,
            public_key: vk,
            signature: sig,
            keyset_version: KeysetVersion(version),
        }
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
            cert.add_signature(make_auth_sig(NodeId(format!("auth-{i}")), vk, sig));
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
            cert.add_signature(make_auth_sig(NodeId(format!("auth-{i}")), vk, sig));
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
            cert.add_signature(make_auth_sig(id, vk, sig));
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
        cert.add_signature(make_auth_sig(NodeId("good-auth".into()), vk, sig));

        // Add a signature signed with a different key but presented with wrong public key.
        let (sk2, _vk2) = make_key_pair();
        let (_sk3, vk3) = make_key_pair();
        let bad_sig = sign_message(&sk2, &message);
        cert.add_signature(make_auth_sig(NodeId("bad-auth".into()), vk3, bad_sig));

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
            cert.add_signature(make_auth_sig(NodeId(name.into()), vk, sig));
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
        cert.add_signature(make_auth_sig(NodeId("auth-1".into()), vk, sig));

        // Add a second signature from the same authority (should be ignored)
        let (sk2, vk2) = make_key_pair();
        let sig2 = sign_message(&sk2, &message);
        cert.add_signature(make_auth_sig(NodeId("auth-1".into()), vk2, sig2));

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
        cert.add_signature(make_auth_sig(NodeId("auth-1".into()), vk1, sig1));

        // Try to add the same authority 4 more times (all should be ignored)
        for _ in 0..4 {
            let (sk, vk) = make_key_pair();
            let sig = sign_message(&sk, &message);
            cert.add_signature(make_auth_sig(NodeId("auth-1".into()), vk, sig));
        }

        // Still only 1 unique signer, cannot reach majority of 3
        assert_eq!(cert.signature_count(), 1);
        assert!(!cert.has_majority(3));

        // Now add a genuinely different authority
        let (sk2, vk2) = make_key_pair();
        let sig2 = sign_message(&sk2, &message);
        cert.add_signature(make_auth_sig(NodeId("auth-2".into()), vk2, sig2));

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
            cert.add_signature(make_auth_sig(NodeId(format!("auth-{i}")), vk, sig));
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

        let auth_sig = make_auth_sig(NodeId("auth-1".into()), vk, sig);

        let json = serde_json::to_string(&auth_sig).expect("serialize");
        let restored: AuthoritySignature = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(restored.authority_id, auth_sig.authority_id);
        assert_eq!(restored.public_key, auth_sig.public_key);
        assert_eq!(restored.signature, auth_sig.signature);
    }

    // ---------------------------------------------------------------
    // KeysetRegistry tests
    // ---------------------------------------------------------------

    #[test]
    fn keyset_registry_register_and_get() {
        let mut registry = KeysetRegistry::new();
        let (_, vk1) = make_key_pair();
        let (_, vk2) = make_key_pair();

        let keys = vec![
            (NodeId("auth-1".into()), vk1),
            (NodeId("auth-2".into()), vk2),
        ];

        registry
            .register_keyset(KeysetVersion(1), 0, keys.clone())
            .unwrap();

        assert_eq!(registry.current_version(), KeysetVersion(1));
        let retrieved = registry.get_keys(&KeysetVersion(1)).unwrap();
        assert_eq!(retrieved.len(), 2);
    }

    #[test]
    fn keyset_registry_monotonic_version() {
        let mut registry = KeysetRegistry::new();
        let (_, vk) = make_key_pair();

        registry
            .register_keyset(KeysetVersion(1), 0, vec![(NodeId("a".into()), vk)])
            .unwrap();

        // Registering same or older version should fail.
        let (_, vk2) = make_key_pair();
        let result = registry.register_keyset(KeysetVersion(1), 1, vec![(NodeId("b".into()), vk2)]);
        assert!(matches!(result, Err(CertError::StaleKeyset(1))));
    }

    #[test]
    fn keyset_registry_get_unknown_version() {
        let registry = KeysetRegistry::new();
        assert!(registry.get_keys(&KeysetVersion(99)).is_none());
    }

    #[test]
    fn keyset_registry_version_valid_current() {
        let mut registry = KeysetRegistry::new();
        let (_, vk) = make_key_pair();

        registry
            .register_keyset(KeysetVersion(1), 0, vec![(NodeId("a".into()), vk)])
            .unwrap();

        let config = EpochConfig::default();
        // Current version is always valid regardless of epoch.
        assert!(registry.is_version_valid(&KeysetVersion(1), 100, &config));
    }

    #[test]
    fn keyset_registry_version_valid_within_grace() {
        let mut registry = KeysetRegistry::new();
        let (_, vk1) = make_key_pair();
        let (_, vk2) = make_key_pair();

        registry
            .register_keyset(KeysetVersion(1), 0, vec![(NodeId("a".into()), vk1)])
            .unwrap();
        registry
            .register_keyset(KeysetVersion(2), 5, vec![(NodeId("a".into()), vk2)])
            .unwrap();

        let config = EpochConfig {
            duration_secs: 86400,
            grace_epochs: 7,
        };

        // Version 1 registered at epoch 0, current epoch 5, grace 7 → valid (5 <= 0+7).
        assert!(registry.is_version_valid(&KeysetVersion(1), 5, &config));

        // Version 1 at epoch 8 → invalid (8 > 0+7).
        assert!(!registry.is_version_valid(&KeysetVersion(1), 8, &config));
    }

    #[test]
    fn keyset_registry_get_key_for_authority() {
        let mut registry = KeysetRegistry::new();
        let (_, vk1) = make_key_pair();
        let (_, vk2) = make_key_pair();

        registry
            .register_keyset(
                KeysetVersion(1),
                0,
                vec![
                    (NodeId("auth-1".into()), vk1),
                    (NodeId("auth-2".into()), vk2),
                ],
            )
            .unwrap();

        assert_eq!(
            registry.get_key_for_authority(&KeysetVersion(1), &NodeId("auth-1".into())),
            Some(&vk1)
        );
        assert_eq!(
            registry.get_key_for_authority(&KeysetVersion(1), &NodeId("auth-2".into())),
            Some(&vk2)
        );
        assert!(
            registry
                .get_key_for_authority(&KeysetVersion(1), &NodeId("unknown".into()))
                .is_none()
        );
    }

    #[test]
    fn keyset_registry_registered_epoch() {
        let mut registry = KeysetRegistry::new();
        let (_, vk) = make_key_pair();

        registry
            .register_keyset(KeysetVersion(1), 42, vec![(NodeId("a".into()), vk)])
            .unwrap();

        assert_eq!(registry.registered_epoch(&KeysetVersion(1)), Some(42));
        assert_eq!(registry.registered_epoch(&KeysetVersion(99)), None);
    }

    // ---------------------------------------------------------------
    // EpochManager tests
    // ---------------------------------------------------------------

    #[test]
    fn epoch_manager_current_epoch() {
        let config = EpochConfig {
            duration_secs: 86400,
            grace_epochs: 7,
        };
        let manager = EpochManager::new(config, 1_000_000);

        // At the base timestamp, epoch is 0.
        assert_eq!(manager.current_epoch(1_000_000), 0);
        // One epoch later.
        assert_eq!(manager.current_epoch(1_000_000 + 86400), 1);
        // Seven epochs later.
        assert_eq!(manager.current_epoch(1_000_000 + 86400 * 7), 7);
        // Before base.
        assert_eq!(manager.current_epoch(0), 0);
    }

    #[test]
    fn epoch_manager_is_within_grace() {
        let config = EpochConfig {
            duration_secs: 86400,
            grace_epochs: 3,
        };
        let manager = EpochManager::new(config, 0);

        // Keyset at epoch 5, current epoch 5 → in grace.
        assert!(manager.is_within_grace(5, 5));
        // Keyset at epoch 5, current epoch 8 → in grace (8-5 = 3 <= 3).
        assert!(manager.is_within_grace(5, 8));
        // Keyset at epoch 5, current epoch 9 → expired (9-5 = 4 > 3).
        assert!(!manager.is_within_grace(5, 9));
        // Keyset at epoch 5, current epoch 4 → in grace (future keyset still valid).
        assert!(manager.is_within_grace(5, 4));
    }

    #[test]
    fn epoch_manager_rotate_keyset() {
        let config = EpochConfig {
            duration_secs: 86400,
            grace_epochs: 7,
        };
        let mut manager = EpochManager::new(config, 0);

        let (_, vk1) = make_key_pair();
        let v1 = manager
            .rotate_keyset(0, vec![(NodeId("auth-1".into()), vk1)])
            .unwrap();
        assert_eq!(v1, KeysetVersion(1));

        let (_, vk2) = make_key_pair();
        let v2 = manager
            .rotate_keyset(86400 * 5, vec![(NodeId("auth-1".into()), vk2)])
            .unwrap();
        assert_eq!(v2, KeysetVersion(2));
        assert_eq!(manager.registry().current_version(), KeysetVersion(2));
    }

    #[test]
    fn epoch_manager_validate_keyset_version() {
        let config = EpochConfig {
            duration_secs: 86400,
            grace_epochs: 3,
        };
        let mut manager = EpochManager::new(config, 0);

        let (_, vk1) = make_key_pair();
        manager
            .rotate_keyset(0, vec![(NodeId("a".into()), vk1)])
            .unwrap();

        let (_, vk2) = make_key_pair();
        manager
            .rotate_keyset(86400 * 5, vec![(NodeId("a".into()), vk2)])
            .unwrap();

        // Version 2 is current → always valid.
        assert!(
            manager
                .validate_keyset_version(&KeysetVersion(2), 86400 * 100)
                .is_ok()
        );

        // Version 1 at epoch 3 (registered at epoch 0, grace 3) → valid (3 <= 0+3).
        assert!(
            manager
                .validate_keyset_version(&KeysetVersion(1), 86400 * 3)
                .is_ok()
        );

        // Version 1 at epoch 4 → expired (4 > 0+3).
        let err = manager
            .validate_keyset_version(&KeysetVersion(1), 86400 * 4)
            .unwrap_err();
        assert!(matches!(err, CertError::ExpiredKeyset { version: 1, .. }));

        // Unknown version → error.
        let err = manager
            .validate_keyset_version(&KeysetVersion(99), 0)
            .unwrap_err();
        assert!(matches!(err, CertError::UnknownKeyset(99)));
    }

    // ---------------------------------------------------------------
    // verify_signatures_with_registry tests
    // ---------------------------------------------------------------

    #[test]
    fn verify_with_registry_valid_signatures() {
        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();
        let message = create_certificate_message(&kr, &hlc, &pv);

        let mut registry = KeysetRegistry::new();
        let mut keys = Vec::new();
        let mut cert = MajorityCertificate::new(kr, hlc, pv, KeysetVersion(1));

        for i in 0..3 {
            let (sk, vk) = make_key_pair();
            let sig = sign_message(&sk, &message);
            let id = NodeId(format!("auth-{i}"));
            keys.push((id.clone(), vk));
            cert.add_signature(make_auth_sig(id, vk, sig));
        }

        registry.register_keyset(KeysetVersion(1), 0, keys).unwrap();

        let config = EpochConfig::default();
        let result = cert
            .verify_signatures_with_registry(&message, &registry, 0, &config)
            .unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn verify_with_registry_mixed_versions() {
        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();
        let message = create_certificate_message(&kr, &hlc, &pv);

        let mut registry = KeysetRegistry::new();

        // Register version 1 keys.
        let (sk1, vk1) = make_key_pair();
        let id1 = NodeId("auth-1".into());
        registry
            .register_keyset(KeysetVersion(1), 0, vec![(id1.clone(), vk1)])
            .unwrap();

        // Register version 2 keys.
        let (sk2, vk2) = make_key_pair();
        let id2 = NodeId("auth-2".into());
        registry
            .register_keyset(KeysetVersion(2), 5, vec![(id2.clone(), vk2)])
            .unwrap();

        let mut cert = MajorityCertificate::new(kr, hlc, pv, KeysetVersion(2));

        // Signature from auth-1 with version 1 key.
        let sig1 = sign_message(&sk1, &message);
        cert.add_signature(make_auth_sig_v(id1, vk1, sig1, 1));

        // Signature from auth-2 with version 2 key.
        let sig2 = sign_message(&sk2, &message);
        cert.add_signature(make_auth_sig_v(id2, vk2, sig2, 2));

        let config = EpochConfig {
            duration_secs: 86400,
            grace_epochs: 7,
        };
        // At epoch 5, version 1 (registered at epoch 0) is still within grace (5 <= 0+7).
        let result = cert
            .verify_signatures_with_registry(&message, &registry, 5, &config)
            .unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn verify_with_registry_expired_version_rejected() {
        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();
        let message = create_certificate_message(&kr, &hlc, &pv);

        let mut registry = KeysetRegistry::new();

        let (sk1, vk1) = make_key_pair();
        let id1 = NodeId("auth-1".into());
        registry
            .register_keyset(KeysetVersion(1), 0, vec![(id1.clone(), vk1)])
            .unwrap();

        // Register version 2 to make version 1 non-current.
        let (_, vk2) = make_key_pair();
        registry
            .register_keyset(KeysetVersion(2), 5, vec![(NodeId("auth-2".into()), vk2)])
            .unwrap();

        let mut cert = MajorityCertificate::new(kr, hlc, pv, KeysetVersion(1));
        let sig1 = sign_message(&sk1, &message);
        cert.add_signature(make_auth_sig(id1, vk1, sig1));

        let config = EpochConfig {
            duration_secs: 86400,
            grace_epochs: 3,
        };
        // At epoch 4, version 1 (registered at epoch 0, grace 3) is expired (4 > 0+3).
        let result = cert.verify_signatures_with_registry(&message, &registry, 4, &config);
        assert!(matches!(
            result,
            Err(CertError::ExpiredKeyset { version: 1, .. })
        ));
    }

    #[test]
    fn verify_with_registry_unknown_version_rejected() {
        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();
        let message = create_certificate_message(&kr, &hlc, &pv);

        let registry = KeysetRegistry::new();

        let (sk, vk) = make_key_pair();
        let mut cert = MajorityCertificate::new(kr, hlc, pv, KeysetVersion(99));
        let sig = sign_message(&sk, &message);
        cert.add_signature(make_auth_sig_v(NodeId("auth-1".into()), vk, sig, 99));

        let config = EpochConfig::default();
        let result = cert.verify_signatures_with_registry(&message, &registry, 0, &config);
        assert!(matches!(result, Err(CertError::UnknownKeyset(99))));
    }

    #[test]
    fn verify_with_registry_detects_tampered_signature() {
        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();
        let message = create_certificate_message(&kr, &hlc, &pv);

        let mut registry = KeysetRegistry::new();
        let (sk1, vk1) = make_key_pair();
        let (sk2, _vk2) = make_key_pair(); // different key
        let id = NodeId("auth-1".into());

        registry
            .register_keyset(KeysetVersion(1), 0, vec![(id.clone(), vk1)])
            .unwrap();

        let mut cert = MajorityCertificate::new(kr, hlc, pv, KeysetVersion(1));
        // Sign with sk2 but registry has vk1 → mismatch.
        let bad_sig = sign_message(&sk2, &message);
        cert.add_signature(make_auth_sig(id.clone(), vk1, bad_sig));

        let config = EpochConfig::default();
        let result = cert.verify_signatures_with_registry(&message, &registry, 0, &config);
        assert!(matches!(
            result,
            Err(CertError::InvalidSignature(ref id)) if id == "auth-1"
        ));
    }
}
