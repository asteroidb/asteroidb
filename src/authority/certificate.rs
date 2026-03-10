use std::collections::{BTreeMap, HashMap, HashSet};

use ed25519_dalek::{Signature, SigningKey, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::authority::bls::{self, BlsPublicKey, BlsSignature};
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

    #[error("authority {0} not found in keyset registry")]
    AuthorityNotInRegistry(String),

    #[error("expired certificate format version {version} (grace period elapsed)")]
    ExpiredFormatVersion { version: u32 },
}

/// Certificate signature mode: Ed25519 (individual signatures) or BLS (aggregated).
///
/// Serializable so that old nodes can detect BLS certificates and fall back.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CertificateMode {
    /// Traditional per-authority Ed25519 signatures.
    Ed25519,
    /// Aggregated BLS12-381 signature (single signature for N signers).
    Bls,
}

/// The cryptographic algorithm used to produce a certificate's signatures.
///
/// Recorded explicitly so that verifiers can select the correct verification
/// path without inspecting the raw signature bytes. Old certificates that
/// were serialized before this field existed will default to `Ed25519`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SignatureAlgorithm {
    /// Ed25519 individual signatures.
    Ed25519,
    /// BLS12-381 aggregated signatures.
    Bls12_381,
}

/// The current certificate format version.
///
/// Bumped when the wire format changes in a backward-incompatible way.
pub const CURRENT_FORMAT_VERSION: u32 = 2;

/// Default grace period (in seconds) during which old-format certificates
/// are still accepted after a format version upgrade.
///
/// 7 days = 604800 seconds. This gives operators time to roll out new
/// software across all nodes.
pub const DEFAULT_FORMAT_GRACE_PERIOD_SECS: u64 = 604_800;

/// Configuration for certificate format version grace period.
#[derive(Debug, Clone)]
pub struct FormatVersionConfig {
    /// Duration (in seconds) for which old-format certificates remain valid
    /// after the current node has upgraded to a newer format version.
    pub grace_period_secs: u64,
}

impl Default for FormatVersionConfig {
    fn default() -> Self {
        Self {
            grace_period_secs: DEFAULT_FORMAT_GRACE_PERIOD_SECS,
        }
    }
}

impl FormatVersionConfig {
    /// Check whether a certificate with `cert_version` is acceptable given
    /// the current format version and the time elapsed since the version
    /// was superseded.
    ///
    /// - The current format version is always accepted.
    /// - Older versions are accepted if `elapsed_since_upgrade_secs` is
    ///   within `grace_period_secs`.
    /// - Format versions up to `CURRENT_FORMAT_VERSION + 1` are accepted
    ///   for forward compatibility during rolling upgrades.  Versions
    ///   further in the future are rejected to prevent unbounded skew.
    pub fn is_version_acceptable(
        &self,
        cert_version: u32,
        elapsed_since_upgrade_secs: u64,
    ) -> bool {
        if (CURRENT_FORMAT_VERSION..=CURRENT_FORMAT_VERSION + 1).contains(&cert_version) {
            return true;
        }
        if cert_version > CURRENT_FORMAT_VERSION + 1 {
            return false;
        }
        elapsed_since_upgrade_secs <= self.grace_period_secs
    }
}

fn default_format_version() -> u32 {
    1
}

fn default_signature_algorithm() -> SignatureAlgorithm {
    SignatureAlgorithm::Ed25519
}

/// Event emitted when a key rotation occurs.
#[derive(Debug, Clone)]
pub struct RotationEvent {
    /// The new keyset version that was activated.
    pub new_version: KeysetVersion,
    /// The epoch at which the rotation happened.
    pub epoch: u64,
    /// Keyset versions that were cleaned up as stale.
    pub cleaned_versions: Vec<u64>,
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
    /// Authority Ed25519 public keys for this version.
    keys: Vec<(NodeId, VerifyingKey)>,
    /// Optional BLS public keys for this version, keyed by authority ID.
    bls_keys: std::collections::HashMap<String, BlsPublicKey>,
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
                bls_keys: std::collections::HashMap::new(),
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

    /// Register BLS public keys for a keyset version.
    ///
    /// `bls_keys` maps authority ID strings to their BLS public keys.
    /// The keyset version must already exist (i.e., `register_keyset` must have
    /// been called first).
    pub fn register_bls_keys(
        &mut self,
        version: &KeysetVersion,
        bls_keys: Vec<(String, BlsPublicKey)>,
    ) -> Result<(), CertError> {
        let entry = self
            .keysets
            .get_mut(&version.0)
            .ok_or(CertError::UnknownKeyset(version.0))?;
        for (id, pk) in bls_keys {
            entry.bls_keys.insert(id, pk);
        }
        Ok(())
    }

    /// Look up the BLS public key for a specific authority in a keyset version.
    pub fn get_bls_key(
        &self,
        version: &KeysetVersion,
        authority_id: &str,
    ) -> Option<&BlsPublicKey> {
        self.keysets
            .get(&version.0)
            .and_then(|entry| entry.bls_keys.get(authority_id))
    }

    /// Remove keyset versions whose registration epoch is beyond the grace
    /// period relative to `current_epoch`. The current version is never removed.
    ///
    /// Returns the list of removed keyset version numbers.
    pub fn cleanup_stale_keysets(&mut self, current_epoch: u64, config: &EpochConfig) -> Vec<u64> {
        let current = self.current;
        let stale: Vec<u64> = self
            .keysets
            .iter()
            .filter(|(ver, entry)| {
                **ver != current && current_epoch > entry.registered_epoch + config.grace_epochs
            })
            .map(|(ver, _)| *ver)
            .collect();

        for ver in &stale {
            self.keysets.remove(ver);
        }
        stale
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
    /// The epoch in which the last rotation was performed (if any).
    last_rotation_epoch: Option<u64>,
    /// Total number of rotations performed.
    rotation_count: u64,
    /// Timestamp (ms) of the last rotation.
    last_rotation_time_ms: Option<u64>,
    /// Optional pending keyset staged for the next epoch boundary.
    staged_keys: Option<Vec<(NodeId, VerifyingKey)>>,
}

impl EpochManager {
    /// Create a new epoch manager.
    ///
    /// `epoch_base_secs` is the reference timestamp (in seconds) for epoch 0.
    /// If `config.duration_secs` is 0, it is replaced with the default (86400)
    /// to prevent division-by-zero in epoch calculations.
    pub fn new(mut config: EpochConfig, epoch_base_secs: u64) -> Self {
        if config.duration_secs == 0 {
            config.duration_secs = 86400;
        }
        Self {
            config,
            registry: KeysetRegistry::new(),
            epoch_base_secs,
            last_rotation_epoch: None,
            rotation_count: 0,
            last_rotation_time_ms: None,
            staged_keys: None,
        }
    }

    /// Compute the epoch number for a given timestamp in seconds.
    ///
    /// If `duration_secs` is 0 (misconfiguration), returns 0 to avoid
    /// a division-by-zero panic.
    pub fn current_epoch(&self, now_secs: u64) -> u64 {
        if self.config.duration_secs == 0 || now_secs < self.epoch_base_secs {
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
        self.last_rotation_epoch = Some(epoch);
        self.rotation_count += 1;
        self.last_rotation_time_ms = Some(now_secs * 1000);
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

    /// Stage a keyset for automatic rotation at the next epoch boundary.
    ///
    /// The staged keys will be consumed by `check_and_rotate` when the
    /// epoch transitions.
    pub fn stage_keys(&mut self, keys: Vec<(NodeId, VerifyingKey)>) {
        self.staged_keys = Some(keys);
    }

    /// Check if an epoch boundary has been crossed and, if staged keys are
    /// available, perform an automatic rotation.
    ///
    /// Returns `Some(RotationEvent)` if a rotation occurred, `None` otherwise.
    ///
    /// Also cleans up stale keysets beyond the grace period.
    pub fn check_and_rotate(&mut self, current_time_ms: u64) -> Option<RotationEvent> {
        let now_secs = current_time_ms / 1000;
        let epoch = self.current_epoch(now_secs);

        // Only rotate if the epoch has advanced past our last rotation epoch.
        let should_rotate = match self.last_rotation_epoch {
            Some(last) => epoch > last,
            None => true, // No rotation has ever happened.
        };

        if !should_rotate {
            return None;
        }

        // We need staged keys to rotate.
        let keys = self.staged_keys.take()?;

        let new_version = self.rotate_keyset(now_secs, keys).ok()?;
        self.last_rotation_time_ms = Some(current_time_ms);

        // Clean up stale keysets.
        let cleaned = self.registry.cleanup_stale_keysets(epoch, &self.config);

        Some(RotationEvent {
            new_version,
            epoch,
            cleaned_versions: cleaned,
        })
    }

    /// Return the total number of rotations performed.
    pub fn rotation_count(&self) -> u64 {
        self.rotation_count
    }

    /// Return the timestamp (ms) of the last rotation, if any.
    pub fn last_rotation_time_ms(&self) -> Option<u64> {
        self.last_rotation_time_ms
    }

    /// Return the epoch of the last rotation, if any.
    pub fn last_rotation_epoch(&self) -> Option<u64> {
        self.last_rotation_epoch
    }

    /// Return a mutable reference to the underlying keyset registry.
    pub fn registry_mut(&mut self) -> &mut KeysetRegistry {
        &mut self.registry
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
    /// Certificate format version.  Old certificates without this field
    /// deserialize with `format_version = 1`.
    #[serde(default = "default_format_version")]
    pub format_version: u32,
    /// The signature algorithm used for this certificate's signatures.
    /// Old certificates without this field default to `Ed25519`.
    #[serde(default = "default_signature_algorithm")]
    pub signature_algorithm: SignatureAlgorithm,
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
            format_version: CURRENT_FORMAT_VERSION,
            signature_algorithm: SignatureAlgorithm::Ed25519,
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
            // Registry-aware verification trusts only registry keys;
            // if the authority is missing from the registry, reject it.
            let verify_key = registry
                .get_key_for_authority(&sig.keyset_version, &sig.authority_id)
                .ok_or_else(|| CertError::AuthorityNotInRegistry(sig.authority_id.0.clone()))?;
            verify_key
                .verify(message, &sig.signature)
                .map_err(|_| CertError::InvalidSignature(sig.authority_id.0.clone()))?;
            valid_signers.push(sig.authority_id.clone());
        }
        Ok(valid_signers)
    }

    /// Verify all signatures, also checking that the certificate format
    /// version is acceptable under the provided config.
    ///
    /// `elapsed_since_upgrade_secs` is the wall-clock time since the node
    /// upgraded to the current format version.  Old-format certificates are
    /// rejected once the grace period expires.
    pub fn verify_signatures_with_format_check(
        &self,
        message: &[u8],
        format_config: &FormatVersionConfig,
        elapsed_since_upgrade_secs: u64,
    ) -> Result<Vec<NodeId>, CertError> {
        if !format_config.is_version_acceptable(self.format_version, elapsed_since_upgrade_secs) {
            return Err(CertError::ExpiredFormatVersion {
                version: self.format_version,
            });
        }
        self.verify_signatures(message)
    }

    /// Verify all signatures using a keyset registry, also checking that the
    /// certificate format version is acceptable.
    pub fn verify_signatures_with_registry_and_format_check(
        &self,
        message: &[u8],
        registry: &KeysetRegistry,
        current_epoch: u64,
        epoch_config: &EpochConfig,
        format_config: &FormatVersionConfig,
        elapsed_since_upgrade_secs: u64,
    ) -> Result<Vec<NodeId>, CertError> {
        if !format_config.is_version_acceptable(self.format_version, elapsed_since_upgrade_secs) {
            return Err(CertError::ExpiredFormatVersion {
                version: self.format_version,
            });
        }
        self.verify_signatures_with_registry(message, registry, current_epoch, epoch_config)
    }

    /// Return references to the authority IDs that have signed.
    pub fn signers(&self) -> Vec<&NodeId> {
        self.signatures.iter().map(|s| &s.authority_id).collect()
    }
}

/// A dual-mode certificate that supports both Ed25519 and BLS signature modes.
///
/// Old nodes that don't understand BLS can detect the mode and fall back to
/// Ed25519 verification. New nodes can use the compact BLS aggregated signature.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DualModeCertificate {
    /// Certificate format version.  Old certificates without this field
    /// deserialize with `format_version = 1`.
    #[serde(default = "default_format_version")]
    pub format_version: u32,
    /// The signature algorithm used for this certificate.
    /// Old certificates without this field default to `Ed25519`.
    #[serde(default = "default_signature_algorithm")]
    pub signature_algorithm: SignatureAlgorithm,
    /// The signature mode used for this certificate.
    pub mode: CertificateMode,
    /// The key range this certificate covers.
    pub key_range: KeyRange,
    /// The HLC frontier timestamp at the time of certification.
    pub frontier_hlc: HlcTimestamp,
    /// The policy version under which this certificate was issued.
    pub policy_version: PolicyVersion,
    /// The keyset version used for signing.
    pub keyset_version: KeysetVersion,
    /// Ed25519 signatures (populated when mode == Ed25519).
    #[serde(default)]
    pub ed25519_signatures: Vec<AuthoritySignature>,
    /// Authority IDs that signed (used in BLS mode to know who participated).
    #[serde(default)]
    pub bls_signer_ids: Vec<NodeId>,
    /// BLS public keys corresponding to `bls_signer_ids` (same order).
    #[serde(default)]
    pub bls_public_keys: Vec<BlsPublicKey>,
    /// The aggregated BLS signature (populated when mode == Bls).
    pub bls_aggregated_signature: Option<BlsSignature>,
}

impl DualModeCertificate {
    /// Create a new Ed25519-mode certificate (wraps existing behavior).
    pub fn new_ed25519(
        key_range: KeyRange,
        frontier_hlc: HlcTimestamp,
        policy_version: PolicyVersion,
        keyset_version: KeysetVersion,
    ) -> Self {
        Self {
            format_version: CURRENT_FORMAT_VERSION,
            signature_algorithm: SignatureAlgorithm::Ed25519,
            mode: CertificateMode::Ed25519,
            key_range,
            frontier_hlc,
            policy_version,
            keyset_version,
            ed25519_signatures: Vec::new(),
            bls_signer_ids: Vec::new(),
            bls_public_keys: Vec::new(),
            bls_aggregated_signature: None,
        }
    }

    /// Create a new BLS-mode certificate.
    pub fn new_bls(
        key_range: KeyRange,
        frontier_hlc: HlcTimestamp,
        policy_version: PolicyVersion,
        keyset_version: KeysetVersion,
    ) -> Self {
        Self {
            format_version: CURRENT_FORMAT_VERSION,
            signature_algorithm: SignatureAlgorithm::Bls12_381,
            mode: CertificateMode::Bls,
            key_range,
            frontier_hlc,
            policy_version,
            keyset_version,
            ed25519_signatures: Vec::new(),
            bls_signer_ids: Vec::new(),
            bls_public_keys: Vec::new(),
            bls_aggregated_signature: None,
        }
    }

    /// Add an Ed25519 signature (only effective in Ed25519 mode).
    pub fn add_ed25519_signature(&mut self, sig: AuthoritySignature) {
        if self.mode != CertificateMode::Ed25519 {
            return;
        }
        if self
            .ed25519_signatures
            .iter()
            .any(|s| s.authority_id == sig.authority_id)
        {
            return;
        }
        self.ed25519_signatures.push(sig);
    }

    /// Set the aggregated BLS signature along with signer information.
    ///
    /// `signers` is a list of (authority_id, bls_public_key) pairs.
    pub fn set_bls_aggregate(
        &mut self,
        signers: Vec<(NodeId, BlsPublicKey)>,
        aggregated_sig: BlsSignature,
    ) {
        self.bls_signer_ids = signers.iter().map(|(id, _)| id.clone()).collect();
        self.bls_public_keys = signers.into_iter().map(|(_, pk)| pk).collect();
        self.bls_aggregated_signature = Some(aggregated_sig);
    }

    /// Return the number of unique signers.
    pub fn signer_count(&self) -> usize {
        match self.mode {
            CertificateMode::Ed25519 => {
                let unique: std::collections::HashSet<&NodeId> = self
                    .ed25519_signatures
                    .iter()
                    .map(|s| &s.authority_id)
                    .collect();
                unique.len()
            }
            CertificateMode::Bls => self.bls_signer_ids.len(),
        }
    }

    /// Check whether a strict majority of authorities have signed.
    pub fn has_majority(&self, total_authorities: usize) -> bool {
        let needed = majority_threshold(total_authorities);
        self.signer_count() >= needed
    }

    /// Verify the certificate against the given message bytes.
    ///
    /// Dispatches to Ed25519 or BLS verification based on the `signature_algorithm`
    /// field.  For backward compatibility, certificates with `signature_algorithm`
    /// defaulting to `Ed25519` (format v1) still verify via the Ed25519 path.
    pub fn verify(&self, message: &[u8]) -> Result<Vec<NodeId>, CertError> {
        match self.signature_algorithm {
            SignatureAlgorithm::Ed25519 => self.verify_ed25519(message),
            SignatureAlgorithm::Bls12_381 => self.verify_bls(message),
        }
    }

    /// Verify the certificate against the given message bytes, also checking
    /// that the format version is acceptable under the provided config.
    ///
    /// `elapsed_since_upgrade_secs` is the wall-clock time since the node
    /// upgraded to the current format version.  Old-format certificates are
    /// rejected once the grace period expires.
    pub fn verify_with_format_check(
        &self,
        message: &[u8],
        format_config: &FormatVersionConfig,
        elapsed_since_upgrade_secs: u64,
    ) -> Result<Vec<NodeId>, CertError> {
        if !format_config.is_version_acceptable(self.format_version, elapsed_since_upgrade_secs) {
            return Err(CertError::ExpiredFormatVersion {
                version: self.format_version,
            });
        }
        self.verify(message)
    }

    /// Ed25519 verification path.
    fn verify_ed25519(&self, message: &[u8]) -> Result<Vec<NodeId>, CertError> {
        let mut valid_signers = Vec::new();
        for sig in &self.ed25519_signatures {
            sig.public_key
                .verify(message, &sig.signature)
                .map_err(|_| CertError::InvalidSignature(sig.authority_id.0.clone()))?;
            valid_signers.push(sig.authority_id.clone());
        }
        Ok(valid_signers)
    }

    /// BLS verification path.
    fn verify_bls(&self, message: &[u8]) -> Result<Vec<NodeId>, CertError> {
        // P1-1: Validate signer ID / public key count match.
        if self.bls_signer_ids.len() != self.bls_public_keys.len() {
            return Err(CertError::InvalidSignature(
                "signer ID / public key count mismatch".into(),
            ));
        }

        // P1-2: Reject duplicate signer IDs.
        let unique_signers: HashSet<&str> =
            self.bls_signer_ids.iter().map(|s| s.0.as_str()).collect();
        if unique_signers.len() != self.bls_signer_ids.len() {
            return Err(CertError::InvalidSignature(
                "duplicate BLS signer IDs".into(),
            ));
        }

        let agg_sig = self.bls_aggregated_signature.as_ref().ok_or_else(|| {
            CertError::InvalidSignature("missing BLS aggregated signature".into())
        })?;

        if self.bls_public_keys.is_empty() {
            return Err(CertError::InvalidSignature("no BLS public keys".into()));
        }

        if bls::aggregate_verify(&self.bls_public_keys, message, agg_sig) {
            Ok(self.bls_signer_ids.clone())
        } else {
            Err(CertError::InvalidSignature(
                "BLS aggregate verification failed".into(),
            ))
        }
    }

    /// Verify the certificate using a keyset registry for key validation.
    ///
    /// Dispatches to the appropriate verification path based on
    /// `signature_algorithm`.
    pub fn verify_with_registry(
        &self,
        message: &[u8],
        registry: &KeysetRegistry,
        current_epoch: u64,
        epoch_config: &EpochConfig,
    ) -> Result<Vec<NodeId>, CertError> {
        // Validate keyset version is known and not expired.
        if !registry.is_version_valid(&self.keyset_version, current_epoch, epoch_config) {
            if registry.get_keys(&self.keyset_version).is_none() {
                return Err(CertError::UnknownKeyset(self.keyset_version.0));
            }
            let entry_epoch = registry.registered_epoch(&self.keyset_version).unwrap_or(0);
            return Err(CertError::ExpiredKeyset {
                version: self.keyset_version.0,
                keyset_epoch: entry_epoch,
                current_epoch,
            });
        }

        match self.signature_algorithm {
            SignatureAlgorithm::Ed25519 => {
                // Delegate to existing Ed25519 registry verification via
                // MajorityCertificate.
                let mut cert = MajorityCertificate::new(
                    self.key_range.clone(),
                    self.frontier_hlc.clone(),
                    self.policy_version,
                    self.keyset_version.clone(),
                );
                for sig in &self.ed25519_signatures {
                    cert.add_signature(sig.clone());
                }
                cert.verify_signatures_with_registry(message, registry, current_epoch, epoch_config)
            }
            SignatureAlgorithm::Bls12_381 => {
                // P1-1: Validate signer ID / public key count match.
                if self.bls_signer_ids.len() != self.bls_public_keys.len() {
                    return Err(CertError::InvalidSignature(
                        "signer ID / public key count mismatch".into(),
                    ));
                }

                // P1-2: Reject duplicate signer IDs.
                let unique_signers: HashSet<&str> =
                    self.bls_signer_ids.iter().map(|s| s.0.as_str()).collect();
                if unique_signers.len() != self.bls_signer_ids.len() {
                    return Err(CertError::InvalidSignature(
                        "duplicate BLS signer IDs".into(),
                    ));
                }

                let agg_sig = self.bls_aggregated_signature.as_ref().ok_or_else(|| {
                    CertError::InvalidSignature("missing BLS aggregated signature".into())
                })?;

                if self.bls_signer_ids.is_empty() {
                    return Err(CertError::InvalidSignature("no BLS signers".into()));
                }

                // P1-3: Cross-check each signer's public key against the registry.
                let mut registry_keys = Vec::new();
                for (i, signer_id) in self.bls_signer_ids.iter().enumerate() {
                    let registry_key = registry
                        .get_bls_key(&self.keyset_version, &signer_id.0)
                        .ok_or_else(|| CertError::AuthorityNotInRegistry(signer_id.0.clone()))?;

                    // Verify the embedded public key matches the registry.
                    if self.bls_public_keys[i] != *registry_key {
                        return Err(CertError::InvalidSignature(format!(
                            "BLS public key mismatch for signer {}",
                            signer_id.0
                        )));
                    }

                    registry_keys.push(registry_key.clone());
                }

                // Verify aggregate signature against registry-trusted keys.
                if bls::aggregate_verify(&registry_keys, message, agg_sig) {
                    Ok(self.bls_signer_ids.clone())
                } else {
                    Err(CertError::InvalidSignature(
                        "BLS aggregate verification failed".into(),
                    ))
                }
            }
        }
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
    // Length-prefix variable-length fields to ensure canonical encoding.
    let prefix_bytes = key_range.prefix.as_bytes();
    buf.extend_from_slice(&(prefix_bytes.len() as u32).to_be_bytes());
    buf.extend_from_slice(prefix_bytes);
    // Fixed-size fields need no length prefix.
    buf.extend_from_slice(&frontier_hlc.physical.to_be_bytes());
    buf.extend_from_slice(&frontier_hlc.logical.to_be_bytes());
    // Length-prefix the variable-length node_id.
    let node_id_bytes = frontier_hlc.node_id.as_bytes();
    buf.extend_from_slice(&(node_id_bytes.len() as u32).to_be_bytes());
    buf.extend_from_slice(node_id_bytes);
    buf.extend_from_slice(&policy_version.0.to_be_bytes());
    buf
}

/// Sign a message with an Ed25519 signing key.
pub fn sign_message(signing_key: &SigningKey, message: &[u8]) -> Signature {
    use ed25519_dalek::Signer;
    signing_key.sign(message)
}

/// LRU cache for recently verified BLS signatures.
///
/// BLS verification is CPU-expensive (~1.68ms per verify). In hot paths where
/// the same certificate may be verified multiple times (e.g., re-broadcasting,
/// repeated reads of certified data), caching avoids redundant elliptic-curve
/// math.
///
/// The cache key is a SHA-256 digest of `(message, aggregated_signature)` and
/// the cached value is the list of verified signer IDs.
pub struct BlsVerifyCache {
    /// Maps cache key (message+sig digest) to verified signer IDs.
    entries: HashMap<[u8; 32], Vec<NodeId>>,
    /// Insertion order for LRU eviction (oldest first).
    order: std::collections::VecDeque<[u8; 32]>,
    /// Maximum number of entries before eviction.
    capacity: usize,
}

impl BlsVerifyCache {
    /// Create a new cache with the given capacity.
    ///
    /// A capacity of 64-256 is typically sufficient for real workloads.
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(capacity),
            order: std::collections::VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    /// Compute a cache key from a message and aggregated signature bytes.
    fn cache_key(message: &[u8], sig_bytes: &[u8]) -> [u8; 32] {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(message);
        hasher.update(sig_bytes);
        hasher.finalize().into()
    }

    /// Look up a cached verification result.
    pub fn get(&self, message: &[u8], sig_bytes: &[u8]) -> Option<&Vec<NodeId>> {
        let key = Self::cache_key(message, sig_bytes);
        self.entries.get(&key)
    }

    /// Insert a verification result into the cache.
    fn insert(&mut self, message: &[u8], sig_bytes: &[u8], signers: Vec<NodeId>) {
        let key = Self::cache_key(message, sig_bytes);
        if self.entries.contains_key(&key) {
            return;
        }
        if self.entries.len() >= self.capacity {
            // Evict oldest entry.
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            }
        }
        self.entries.insert(key, signers);
        self.order.push_back(key);
    }

    /// Verify a `DualModeCertificate` with caching.
    ///
    /// Returns cached results for previously-verified (message, signature)
    /// pairs, avoiding the expensive BLS pairing computation.
    /// Falls back to full verification for Ed25519 mode or cache misses.
    pub fn verify_cached(
        &mut self,
        cert: &DualModeCertificate,
        message: &[u8],
    ) -> Result<Vec<NodeId>, CertError> {
        // Only cache BLS verifications (Ed25519 is already fast).
        if cert.signature_algorithm != SignatureAlgorithm::Bls12_381 {
            return cert.verify(message);
        }

        let sig_bytes = match &cert.bls_aggregated_signature {
            Some(sig) => sig.to_bytes(),
            None => return cert.verify(message),
        };

        // Check cache first.
        if let Some(signers) = self.get(message, &sig_bytes) {
            return Ok(signers.clone());
        }

        // Cache miss — perform full verification.
        let signers = cert.verify(message)?;
        self.insert(message, &sig_bytes, signers.clone());
        Ok(signers)
    }

    /// Return the number of entries currently in the cache.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Return `true` if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
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
        let (_sk1, vk1) = make_key_pair();
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

    #[test]
    fn verify_with_registry_rejects_authority_not_in_registry() {
        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();
        let message = create_certificate_message(&kr, &hlc, &pv);

        let mut registry = KeysetRegistry::new();

        // Register keyset version 1 with only auth-1.
        let (sk1, vk1) = make_key_pair();
        let id1 = NodeId("auth-1".into());
        registry
            .register_keyset(KeysetVersion(1), 0, vec![(id1.clone(), vk1)])
            .unwrap();

        let mut cert = MajorityCertificate::new(kr, hlc, pv, KeysetVersion(1));

        // Valid signature from auth-1 (in registry).
        let sig1 = sign_message(&sk1, &message);
        cert.add_signature(make_auth_sig(id1, vk1, sig1));

        // Signature from auth-unknown (NOT in registry).
        let (sk_unknown, vk_unknown) = make_key_pair();
        let id_unknown = NodeId("auth-unknown".into());
        let sig_unknown = sign_message(&sk_unknown, &message);
        cert.add_signature(make_auth_sig(id_unknown, vk_unknown, sig_unknown));

        let config = EpochConfig::default();
        let result = cert.verify_signatures_with_registry(&message, &registry, 0, &config);
        assert!(
            matches!(
                result,
                Err(CertError::AuthorityNotInRegistry(ref id)) if id == "auth-unknown"
            ),
            "expected AuthorityNotInRegistry, got: {result:?}"
        );
    }

    // ---------------------------------------------------------------
    // DualModeCertificate tests
    // ---------------------------------------------------------------

    #[test]
    fn dual_mode_ed25519_sign_and_verify() {
        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();
        let message = create_certificate_message(&kr, &hlc, &pv);

        let mut cert = DualModeCertificate::new_ed25519(kr, hlc, pv, KeysetVersion(1));

        for i in 0..3 {
            let (sk, vk) = make_key_pair();
            let sig = sign_message(&sk, &message);
            cert.add_ed25519_signature(make_auth_sig(NodeId(format!("auth-{i}")), vk, sig));
        }

        assert_eq!(cert.signer_count(), 3);
        assert!(cert.has_majority(5));

        let valid = cert.verify(&message).unwrap();
        assert_eq!(valid.len(), 3);
    }

    #[test]
    fn dual_mode_bls_sign_and_verify() {
        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();
        let message = create_certificate_message(&kr, &hlc, &pv);

        let mut cert = DualModeCertificate::new_bls(kr, hlc, pv, KeysetVersion(1));

        let mut signers = Vec::new();
        let mut bls_sigs = Vec::new();

        for seed in 10..15u8 {
            let mut ikm = [0u8; 32];
            ikm[0] = seed;
            ikm[31] = seed.wrapping_add(42);
            let kp = crate::authority::bls::BlsKeypair::generate(&ikm);
            let sig = crate::authority::bls::sign_message(kp.secret_key(), &message);
            signers.push((NodeId(format!("bls-auth-{seed}")), kp.public_key.clone()));
            bls_sigs.push(sig);
        }

        let agg = crate::authority::bls::aggregate_signatures(&bls_sigs).unwrap();
        cert.set_bls_aggregate(signers, agg);

        assert_eq!(cert.signer_count(), 5);
        assert!(cert.has_majority(9)); // 5 >= 9/2+1=5

        let valid = cert.verify(&message).unwrap();
        assert_eq!(valid.len(), 5);
    }

    #[test]
    fn dual_mode_bls_wrong_message_fails() {
        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();
        let message = create_certificate_message(&kr, &hlc, &pv);

        let mut cert = DualModeCertificate::new_bls(kr, hlc, pv, KeysetVersion(1));

        let mut ikm = [0u8; 32];
        ikm[0] = 99;
        let kp = crate::authority::bls::BlsKeypair::generate(&ikm);
        let sig = crate::authority::bls::sign_message(kp.secret_key(), &message);
        let agg = crate::authority::bls::aggregate_signatures(&[sig]).unwrap();
        cert.set_bls_aggregate(vec![(NodeId("a".into()), kp.public_key.clone())], agg);

        let result = cert.verify(b"wrong message");
        assert!(result.is_err());
    }

    #[test]
    fn dual_mode_ed25519_still_works_after_bls_added() {
        // Backward compatibility: Ed25519 cert verifies correctly even when
        // the codebase supports BLS.
        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();
        let message = create_certificate_message(&kr, &hlc, &pv);

        let mut cert = DualModeCertificate::new_ed25519(kr, hlc, pv, KeysetVersion(1));

        let (sk, vk) = make_key_pair();
        let sig = sign_message(&sk, &message);
        cert.add_ed25519_signature(make_auth_sig(NodeId("ed-auth".into()), vk, sig));

        assert_eq!(cert.mode, CertificateMode::Ed25519);
        let valid = cert.verify(&message).unwrap();
        assert_eq!(valid, vec![NodeId("ed-auth".into())]);
    }

    #[test]
    fn dual_mode_bls_no_signature_fails() {
        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();
        let message = create_certificate_message(&kr, &hlc, &pv);

        let cert = DualModeCertificate::new_bls(kr, hlc, pv, KeysetVersion(1));

        // No aggregated signature set → should fail.
        let result = cert.verify(&message);
        assert!(result.is_err());
    }

    #[test]
    fn dual_mode_serde_roundtrip_ed25519() {
        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();
        let message = create_certificate_message(&kr, &hlc, &pv);

        let mut cert = DualModeCertificate::new_ed25519(kr, hlc, pv, KeysetVersion(1));
        let (sk, vk) = make_key_pair();
        let sig = sign_message(&sk, &message);
        cert.add_ed25519_signature(make_auth_sig(NodeId("a".into()), vk, sig));

        let json = serde_json::to_string(&cert).unwrap();
        let restored: DualModeCertificate = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.mode, CertificateMode::Ed25519);
        assert!(restored.verify(&message).is_ok());
    }

    #[test]
    fn dual_mode_serde_roundtrip_bls() {
        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();
        let message = create_certificate_message(&kr, &hlc, &pv);

        let mut cert = DualModeCertificate::new_bls(kr, hlc, pv, KeysetVersion(1));

        let mut ikm = [0u8; 32];
        ikm[0] = 77;
        let kp = crate::authority::bls::BlsKeypair::generate(&ikm);
        let sig = crate::authority::bls::sign_message(kp.secret_key(), &message);
        let agg = crate::authority::bls::aggregate_signatures(&[sig]).unwrap();
        cert.set_bls_aggregate(vec![(NodeId("b".into()), kp.public_key.clone())], agg);

        let json = serde_json::to_string(&cert).unwrap();
        let restored: DualModeCertificate = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.mode, CertificateMode::Bls);
        assert!(restored.verify(&message).is_ok());
    }

    // ---------------------------------------------------------------
    // Auto-rotation lifecycle tests
    // ---------------------------------------------------------------

    #[test]
    fn check_and_rotate_at_epoch_boundary() {
        let config = EpochConfig {
            duration_secs: 100,
            grace_epochs: 2,
        };
        let mut manager = EpochManager::new(config, 0);

        // First rotation at epoch 0.
        let (_, vk1) = make_key_pair();
        manager.stage_keys(vec![(NodeId("a".into()), vk1)]);

        let event = manager.check_and_rotate(0).unwrap();
        assert_eq!(event.new_version, KeysetVersion(1));
        assert_eq!(event.epoch, 0);
        assert_eq!(manager.rotation_count(), 1);

        // Same epoch → should not rotate again (no staged keys consumed).
        let (_, vk2) = make_key_pair();
        manager.stage_keys(vec![(NodeId("a".into()), vk2)]);
        assert!(manager.check_and_rotate(50).is_none()); // still epoch 0

        // Advance to epoch 1 → should rotate.
        let event = manager.check_and_rotate(100_000).unwrap(); // 100s = epoch 1
        assert_eq!(event.new_version, KeysetVersion(2));
        assert_eq!(event.epoch, 1);
        assert_eq!(manager.rotation_count(), 2);
    }

    #[test]
    fn check_and_rotate_without_staged_keys_returns_none() {
        let config = EpochConfig {
            duration_secs: 100,
            grace_epochs: 2,
        };
        let mut manager = EpochManager::new(config, 0);

        // No staged keys → nothing to rotate.
        assert!(manager.check_and_rotate(0).is_none());
    }

    #[test]
    fn check_and_rotate_cleans_stale_keysets() {
        let config = EpochConfig {
            duration_secs: 100,
            grace_epochs: 2,
        };
        let mut manager = EpochManager::new(config, 0);

        // Rotate at epoch 0.
        let (_, vk1) = make_key_pair();
        manager.stage_keys(vec![(NodeId("a".into()), vk1)]);
        manager.check_and_rotate(0);

        // Rotate at epoch 1.
        let (_, vk2) = make_key_pair();
        manager.stage_keys(vec![(NodeId("a".into()), vk2)]);
        manager.check_and_rotate(100_000); // epoch 1

        // Rotate at epoch 2.
        let (_, vk3) = make_key_pair();
        manager.stage_keys(vec![(NodeId("a".into()), vk3)]);
        manager.check_and_rotate(200_000); // epoch 2

        // Rotate at epoch 5 (grace=2, so version 1 registered at epoch 0 should be stale: 5 > 0+2).
        let (_, vk4) = make_key_pair();
        manager.stage_keys(vec![(NodeId("a".into()), vk4)]);
        let event = manager.check_and_rotate(500_000).unwrap(); // epoch 5
        assert_eq!(event.new_version, KeysetVersion(4));

        // Versions 1 (epoch 0) and 2 (epoch 1) should be cleaned.
        // Version 3 (epoch 2) => 5 > 2+2 = 4, so it should also be cleaned.
        assert!(event.cleaned_versions.contains(&1));
        assert!(event.cleaned_versions.contains(&2));
    }

    #[test]
    fn rotation_metrics_tracked() {
        let config = EpochConfig {
            duration_secs: 100,
            grace_epochs: 2,
        };
        let mut manager = EpochManager::new(config, 0);

        assert_eq!(manager.rotation_count(), 0);
        assert!(manager.last_rotation_time_ms().is_none());
        assert!(manager.last_rotation_epoch().is_none());

        let (_, vk) = make_key_pair();
        manager.stage_keys(vec![(NodeId("a".into()), vk)]);
        manager.check_and_rotate(50_000); // 50s = epoch 0

        assert_eq!(manager.rotation_count(), 1);
        assert_eq!(manager.last_rotation_time_ms(), Some(50_000));
        assert_eq!(manager.last_rotation_epoch(), Some(0));
    }

    #[test]
    fn cleanup_stale_keysets_preserves_current() {
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
            grace_epochs: 3,
        };

        // At epoch 10: version 1 (epoch 0, grace 3) is stale (10 > 0+3).
        // version 2 is current → never removed.
        let cleaned = registry.cleanup_stale_keysets(10, &config);
        assert_eq!(cleaned, vec![1]);
        assert!(registry.get_keys(&KeysetVersion(1)).is_none());
        assert!(registry.get_keys(&KeysetVersion(2)).is_some());
    }

    // ---------------------------------------------------------------
    // CertificateMode serde tests
    // ---------------------------------------------------------------

    #[test]
    fn certificate_mode_serde_roundtrip() {
        let ed = CertificateMode::Ed25519;
        let bls_mode = CertificateMode::Bls;

        let ed_json = serde_json::to_string(&ed).unwrap();
        let bls_json = serde_json::to_string(&bls_mode).unwrap();

        let ed_back: CertificateMode = serde_json::from_str(&ed_json).unwrap();
        let bls_back: CertificateMode = serde_json::from_str(&bls_json).unwrap();

        assert_eq!(ed, ed_back);
        assert_eq!(bls_mode, bls_back);
    }

    // ---------------------------------------------------------------
    // P1 security fix tests: BLS signer integrity & registry validation
    // ---------------------------------------------------------------

    fn make_bls_keypair(seed: u8) -> crate::authority::bls::BlsKeypair {
        let mut ikm = [0u8; 32];
        ikm[0] = seed;
        ikm[31] = seed.wrapping_add(42);
        crate::authority::bls::BlsKeypair::generate(&ikm)
    }

    #[test]
    fn bls_verify_rejects_signer_key_count_mismatch() {
        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();
        let message = create_certificate_message(&kr, &hlc, &pv);

        let mut cert = DualModeCertificate::new_bls(kr, hlc, pv, KeysetVersion(1));

        let kp = make_bls_keypair(80);
        let sig = crate::authority::bls::sign_message(kp.secret_key(), &message);
        let agg = crate::authority::bls::aggregate_signatures(&[sig]).unwrap();

        // Set 1 signer ID but 0 public keys (mismatch).
        cert.bls_signer_ids = vec![NodeId("a".into())];
        cert.bls_public_keys = vec![]; // intentional mismatch
        cert.bls_aggregated_signature = Some(agg);

        let result = cert.verify(&message);
        assert!(result.is_err());
        match result.unwrap_err() {
            CertError::InvalidSignature(msg) => {
                assert!(
                    msg.contains("count mismatch"),
                    "expected count mismatch error, got: {msg}"
                );
            }
            other => panic!("expected InvalidSignature, got: {other}"),
        }
    }

    #[test]
    fn bls_verify_rejects_duplicate_signer_ids() {
        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();
        let message = create_certificate_message(&kr, &hlc, &pv);

        let mut cert = DualModeCertificate::new_bls(kr, hlc, pv, KeysetVersion(1));

        let kp1 = make_bls_keypair(81);
        let kp2 = make_bls_keypair(82);
        let sig1 = crate::authority::bls::sign_message(kp1.secret_key(), &message);
        let sig2 = crate::authority::bls::sign_message(kp2.secret_key(), &message);
        let agg = crate::authority::bls::aggregate_signatures(&[sig1, sig2]).unwrap();

        // Same signer ID listed twice with different keys.
        cert.bls_signer_ids = vec![NodeId("dup".into()), NodeId("dup".into())];
        cert.bls_public_keys = vec![kp1.public_key.clone(), kp2.public_key.clone()];
        cert.bls_aggregated_signature = Some(agg);

        let result = cert.verify(&message);
        assert!(result.is_err());
        match result.unwrap_err() {
            CertError::InvalidSignature(msg) => {
                assert!(
                    msg.contains("duplicate"),
                    "expected duplicate error, got: {msg}"
                );
            }
            other => panic!("expected InvalidSignature, got: {other}"),
        }
    }

    #[test]
    fn bls_verify_with_registry_rejects_unknown_signer() {
        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();
        let message = create_certificate_message(&kr, &hlc, &pv);

        let mut registry = KeysetRegistry::new();
        let (_, ed_vk) = make_key_pair();
        registry
            .register_keyset(KeysetVersion(1), 0, vec![(NodeId("known".into()), ed_vk)])
            .unwrap();

        // Register BLS key only for "known".
        let kp_known = make_bls_keypair(83);
        registry
            .register_bls_keys(
                &KeysetVersion(1),
                vec![("known".into(), kp_known.public_key.clone())],
            )
            .unwrap();

        // Build a cert that includes an unknown signer.
        let mut cert = DualModeCertificate::new_bls(kr, hlc, pv, KeysetVersion(1));
        let kp_unknown = make_bls_keypair(84);
        let sig_known = crate::authority::bls::sign_message(kp_known.secret_key(), &message);
        let sig_unknown = crate::authority::bls::sign_message(kp_unknown.secret_key(), &message);
        let agg = crate::authority::bls::aggregate_signatures(&[sig_known, sig_unknown]).unwrap();

        cert.set_bls_aggregate(
            vec![
                (NodeId("known".into()), kp_known.public_key.clone()),
                (NodeId("unknown".into()), kp_unknown.public_key.clone()),
            ],
            agg,
        );

        let config = EpochConfig::default();
        let result = cert.verify_with_registry(&message, &registry, 0, &config);
        assert!(
            matches!(
                result,
                Err(CertError::AuthorityNotInRegistry(ref id)) if id == "unknown"
            ),
            "expected AuthorityNotInRegistry for 'unknown', got: {result:?}"
        );
    }

    #[test]
    fn bls_verify_with_registry_accepts_valid_signers() {
        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();
        let message = create_certificate_message(&kr, &hlc, &pv);

        let mut registry = KeysetRegistry::new();
        let (_, ed_vk1) = make_key_pair();
        let (_, ed_vk2) = make_key_pair();
        registry
            .register_keyset(
                KeysetVersion(1),
                0,
                vec![
                    (NodeId("auth-a".into()), ed_vk1),
                    (NodeId("auth-b".into()), ed_vk2),
                ],
            )
            .unwrap();

        let kp_a = make_bls_keypair(85);
        let kp_b = make_bls_keypair(86);
        registry
            .register_bls_keys(
                &KeysetVersion(1),
                vec![
                    ("auth-a".into(), kp_a.public_key.clone()),
                    ("auth-b".into(), kp_b.public_key.clone()),
                ],
            )
            .unwrap();

        let mut cert = DualModeCertificate::new_bls(kr, hlc, pv, KeysetVersion(1));
        let sig_a = crate::authority::bls::sign_message(kp_a.secret_key(), &message);
        let sig_b = crate::authority::bls::sign_message(kp_b.secret_key(), &message);
        let agg = crate::authority::bls::aggregate_signatures(&[sig_a, sig_b]).unwrap();

        cert.set_bls_aggregate(
            vec![
                (NodeId("auth-a".into()), kp_a.public_key.clone()),
                (NodeId("auth-b".into()), kp_b.public_key.clone()),
            ],
            agg,
        );

        let config = EpochConfig::default();
        let result = cert.verify_with_registry(&message, &registry, 0, &config);
        assert!(result.is_ok(), "expected Ok, got: {result:?}");
        let signers = result.unwrap();
        assert_eq!(signers.len(), 2);
        assert_eq!(signers[0], NodeId("auth-a".into()));
        assert_eq!(signers[1], NodeId("auth-b".into()));
    }

    // ---------------------------------------------------------------
    // Format version and signature algorithm tests (#264)
    // ---------------------------------------------------------------

    #[test]
    fn new_majority_certificate_includes_version_and_algorithm() {
        let cert = MajorityCertificate::new(
            sample_key_range(),
            sample_hlc(),
            sample_policy_version(),
            KeysetVersion(1),
        );

        assert_eq!(cert.format_version, CURRENT_FORMAT_VERSION);
        assert_eq!(cert.signature_algorithm, SignatureAlgorithm::Ed25519);
    }

    #[test]
    fn new_dual_mode_ed25519_includes_version_and_algorithm() {
        let cert = DualModeCertificate::new_ed25519(
            sample_key_range(),
            sample_hlc(),
            sample_policy_version(),
            KeysetVersion(1),
        );

        assert_eq!(cert.format_version, CURRENT_FORMAT_VERSION);
        assert_eq!(cert.signature_algorithm, SignatureAlgorithm::Ed25519);
    }

    #[test]
    fn new_dual_mode_bls_includes_version_and_algorithm() {
        let cert = DualModeCertificate::new_bls(
            sample_key_range(),
            sample_hlc(),
            sample_policy_version(),
            KeysetVersion(1),
        );

        assert_eq!(cert.format_version, CURRENT_FORMAT_VERSION);
        assert_eq!(cert.signature_algorithm, SignatureAlgorithm::Bls12_381);
    }

    #[test]
    fn old_majority_certificate_deserializes_with_defaults() {
        // Simulate a v1 certificate (before format_version / signature_algorithm
        // fields existed) by serializing a JSON object without those fields.
        let json = r#"{
            "key_range": {"prefix": "user/"},
            "frontier_hlc": {"physical": 1700000000000, "logical": 42, "node_id": "node-1"},
            "policy_version": 1,
            "keyset_version": 1,
            "signatures": []
        }"#;

        let cert: MajorityCertificate = serde_json::from_str(json).unwrap();
        assert_eq!(cert.format_version, 1);
        assert_eq!(cert.signature_algorithm, SignatureAlgorithm::Ed25519);
    }

    #[test]
    fn old_dual_mode_certificate_deserializes_with_defaults() {
        // Simulate a v1 DualModeCertificate without format_version/signature_algorithm.
        let json = r#"{
            "mode": "Ed25519",
            "key_range": {"prefix": "user/"},
            "frontier_hlc": {"physical": 1700000000000, "logical": 42, "node_id": "node-1"},
            "policy_version": 1,
            "keyset_version": 1,
            "bls_aggregated_signature": null
        }"#;

        let cert: DualModeCertificate = serde_json::from_str(json).unwrap();
        assert_eq!(cert.format_version, 1);
        assert_eq!(cert.signature_algorithm, SignatureAlgorithm::Ed25519);
    }

    #[test]
    fn new_certificate_roundtrips_version_and_algorithm() {
        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();
        let message = create_certificate_message(&kr, &hlc, &pv);

        let mut cert = DualModeCertificate::new_ed25519(kr, hlc, pv, KeysetVersion(1));
        let (sk, vk) = make_key_pair();
        let sig = sign_message(&sk, &message);
        cert.add_ed25519_signature(make_auth_sig(NodeId("a".into()), vk, sig));

        let json = serde_json::to_string(&cert).unwrap();
        let restored: DualModeCertificate = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.format_version, CURRENT_FORMAT_VERSION);
        assert_eq!(restored.signature_algorithm, SignatureAlgorithm::Ed25519);
        assert!(restored.verify(&message).is_ok());
    }

    #[test]
    fn bls_certificate_roundtrips_algorithm_bls12_381() {
        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();
        let message = create_certificate_message(&kr, &hlc, &pv);

        let mut cert = DualModeCertificate::new_bls(kr, hlc, pv, KeysetVersion(1));

        let kp = make_bls_keypair(90);
        let sig = crate::authority::bls::sign_message(kp.secret_key(), &message);
        let agg = crate::authority::bls::aggregate_signatures(&[sig]).unwrap();
        cert.set_bls_aggregate(vec![(NodeId("b".into()), kp.public_key.clone())], agg);

        let json = serde_json::to_string(&cert).unwrap();
        let restored: DualModeCertificate = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.format_version, CURRENT_FORMAT_VERSION);
        assert_eq!(restored.signature_algorithm, SignatureAlgorithm::Bls12_381);
        assert!(restored.verify(&message).is_ok());
    }

    #[test]
    fn verify_dispatches_based_on_signature_algorithm() {
        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();
        let message = create_certificate_message(&kr, &hlc, &pv);

        // Ed25519 path via signature_algorithm.
        let mut ed_cert =
            DualModeCertificate::new_ed25519(kr.clone(), hlc.clone(), pv, KeysetVersion(1));
        let (sk, vk) = make_key_pair();
        let sig = sign_message(&sk, &message);
        ed_cert.add_ed25519_signature(make_auth_sig(NodeId("ed".into()), vk, sig));
        assert!(ed_cert.verify(&message).is_ok());

        // BLS path via signature_algorithm.
        let mut bls_cert = DualModeCertificate::new_bls(kr, hlc, pv, KeysetVersion(1));
        let kp = make_bls_keypair(91);
        let bls_sig = crate::authority::bls::sign_message(kp.secret_key(), &message);
        let agg = crate::authority::bls::aggregate_signatures(&[bls_sig]).unwrap();
        bls_cert.set_bls_aggregate(vec![(NodeId("bls".into()), kp.public_key.clone())], agg);
        assert!(bls_cert.verify(&message).is_ok());
    }

    #[test]
    fn signature_algorithm_serde_roundtrip() {
        let ed = SignatureAlgorithm::Ed25519;
        let bls_alg = SignatureAlgorithm::Bls12_381;

        let ed_json = serde_json::to_string(&ed).unwrap();
        let bls_json = serde_json::to_string(&bls_alg).unwrap();

        let ed_back: SignatureAlgorithm = serde_json::from_str(&ed_json).unwrap();
        let bls_back: SignatureAlgorithm = serde_json::from_str(&bls_json).unwrap();

        assert_eq!(ed, ed_back);
        assert_eq!(bls_alg, bls_back);
    }

    // ---------------------------------------------------------------
    // Format version grace period tests (#264)
    // ---------------------------------------------------------------

    #[test]
    fn format_version_config_default_grace_period() {
        let config = FormatVersionConfig::default();
        assert_eq!(config.grace_period_secs, DEFAULT_FORMAT_GRACE_PERIOD_SECS);
    }

    #[test]
    fn format_version_current_always_accepted() {
        let config = FormatVersionConfig {
            grace_period_secs: 0,
        };
        assert!(config.is_version_acceptable(CURRENT_FORMAT_VERSION, 999_999));
    }

    #[test]
    fn format_version_old_within_grace_accepted() {
        let config = FormatVersionConfig {
            grace_period_secs: 100,
        };
        // Version 1 with only 50s elapsed -> within grace.
        assert!(config.is_version_acceptable(1, 50));
    }

    #[test]
    fn format_version_old_beyond_grace_rejected() {
        let config = FormatVersionConfig {
            grace_period_secs: 100,
        };
        // Version 1 with 101s elapsed -> beyond grace.
        assert!(!config.is_version_acceptable(1, 101));
    }

    #[test]
    fn format_version_one_ahead_accepted() {
        let config = FormatVersionConfig {
            grace_period_secs: 0,
        };
        // One version ahead is accepted for rolling upgrades.
        assert!(config.is_version_acceptable(CURRENT_FORMAT_VERSION + 1, 999_999));
        // Two or more versions ahead is rejected to prevent unbounded skew.
        assert!(!config.is_version_acceptable(CURRENT_FORMAT_VERSION + 2, 0));
        assert!(!config.is_version_acceptable(CURRENT_FORMAT_VERSION + 10, 0));
    }

    #[test]
    fn verify_with_format_check_accepts_current_version() {
        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();
        let message = create_certificate_message(&kr, &hlc, &pv);

        let mut cert = DualModeCertificate::new_ed25519(kr, hlc, pv, KeysetVersion(1));
        let (sk, vk) = make_key_pair();
        let sig = sign_message(&sk, &message);
        cert.add_ed25519_signature(make_auth_sig(NodeId("a".into()), vk, sig));

        let config = FormatVersionConfig {
            grace_period_secs: 0,
        };
        // Current format version -> always accepted.
        assert!(
            cert.verify_with_format_check(&message, &config, 999_999)
                .is_ok()
        );
    }

    #[test]
    fn verify_with_format_check_rejects_expired_old_version() {
        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();
        let message = create_certificate_message(&kr, &hlc, &pv);

        let mut cert = DualModeCertificate::new_ed25519(kr, hlc, pv, KeysetVersion(1));
        // Manually set to old format version.
        cert.format_version = 1;

        let (sk, vk) = make_key_pair();
        let sig = sign_message(&sk, &message);
        cert.add_ed25519_signature(make_auth_sig(NodeId("a".into()), vk, sig));

        let config = FormatVersionConfig {
            grace_period_secs: 100,
        };
        // Within grace -> OK.
        assert!(cert.verify_with_format_check(&message, &config, 50).is_ok());
        // Beyond grace -> rejected.
        let err = cert
            .verify_with_format_check(&message, &config, 101)
            .unwrap_err();
        assert!(
            matches!(err, CertError::ExpiredFormatVersion { version: 1 }),
            "expected ExpiredFormatVersion, got: {err}"
        );
    }

    #[test]
    fn verify_with_format_check_old_bls_cert_within_grace() {
        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();
        let message = create_certificate_message(&kr, &hlc, &pv);

        let mut cert = DualModeCertificate::new_bls(kr, hlc, pv, KeysetVersion(1));
        cert.format_version = 1; // simulate old format

        let kp = make_bls_keypair(92);
        let sig = crate::authority::bls::sign_message(kp.secret_key(), &message);
        let agg = crate::authority::bls::aggregate_signatures(&[sig]).unwrap();
        cert.set_bls_aggregate(vec![(NodeId("b".into()), kp.public_key.clone())], agg);

        let config = FormatVersionConfig {
            grace_period_secs: 200,
        };
        // Within grace period.
        assert!(
            cert.verify_with_format_check(&message, &config, 100)
                .is_ok()
        );
        // Beyond grace period.
        assert!(
            cert.verify_with_format_check(&message, &config, 201)
                .is_err()
        );
    }

    // ---------------------------------------------------------------
    // BLS verify cache tests (#306)
    // ---------------------------------------------------------------

    #[test]
    fn bls_cache_hit_avoids_reverification() {
        use crate::authority::bls;

        let keypairs: Vec<bls::BlsKeypair> = (1..=3)
            .map(|i| {
                let mut ikm = [0u8; 32];
                ikm[0] = i;
                ikm[31] = i + 42;
                bls::BlsKeypair::generate(&ikm)
            })
            .collect();

        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();
        let message = create_certificate_message(&kr, &hlc, &pv);

        let sigs: Vec<bls::BlsSignature> = keypairs
            .iter()
            .map(|kp| bls::sign_message(kp.secret_key(), &message))
            .collect();
        let agg_sig = bls::aggregate_signatures(&sigs).unwrap();

        let mut cert = DualModeCertificate::new_bls(kr, hlc, pv, KeysetVersion(1));
        let signers: Vec<(NodeId, bls::BlsPublicKey)> = keypairs
            .iter()
            .enumerate()
            .map(|(i, kp)| (NodeId(format!("auth-{i}")), kp.public_key.clone()))
            .collect();
        cert.set_bls_aggregate(signers, agg_sig);

        let mut cache = BlsVerifyCache::new(16);

        // First call: cache miss, real verification.
        let result1 = cache.verify_cached(&cert, &message);
        assert!(result1.is_ok());
        assert_eq!(cache.len(), 1);

        // Second call: cache hit.
        let result2 = cache.verify_cached(&cert, &message);
        assert!(result2.is_ok());
        assert_eq!(result2.unwrap(), result1.unwrap());
        assert_eq!(cache.len(), 1); // No new entry.
    }

    #[test]
    fn bls_cache_evicts_oldest() {
        use crate::authority::bls;

        let mut cache = BlsVerifyCache::new(2);

        // Create 3 different certs.
        for seed in 0u8..3 {
            let keypairs: Vec<bls::BlsKeypair> = (1..=2)
                .map(|i| {
                    let mut ikm = [0u8; 32];
                    ikm[0] = seed * 10 + i;
                    ikm[31] = seed * 10 + i + 42;
                    bls::BlsKeypair::generate(&ikm)
                })
                .collect();

            let kr = KeyRange {
                prefix: format!("test-{seed}/"),
            };
            let hlc = sample_hlc();
            let pv = sample_policy_version();
            let message = create_certificate_message(&kr, &hlc, &pv);

            let sigs: Vec<bls::BlsSignature> = keypairs
                .iter()
                .map(|kp| bls::sign_message(kp.secret_key(), &message))
                .collect();
            let agg_sig = bls::aggregate_signatures(&sigs).unwrap();

            let mut cert = DualModeCertificate::new_bls(kr, hlc, pv, KeysetVersion(1));
            let signers: Vec<(NodeId, bls::BlsPublicKey)> = keypairs
                .iter()
                .enumerate()
                .map(|(i, kp)| (NodeId(format!("auth-{i}")), kp.public_key.clone()))
                .collect();
            cert.set_bls_aggregate(signers, agg_sig);

            let result = cache.verify_cached(&cert, &message);
            assert!(result.is_ok());
        }

        // Cache capacity is 2, so oldest should have been evicted.
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn bls_cache_ed25519_not_cached() {
        let (sk, vk) = make_key_pair();
        let kr = sample_key_range();
        let hlc = sample_hlc();
        let pv = sample_policy_version();
        let message = create_certificate_message(&kr, &hlc, &pv);
        let sig = sign_message(&sk, &message);

        let mut cert = DualModeCertificate::new_ed25519(kr, hlc, pv, KeysetVersion(1));
        cert.add_ed25519_signature(make_auth_sig(NodeId("auth-0".into()), vk, sig));

        let mut cache = BlsVerifyCache::new(16);
        let result = cache.verify_cached(&cert, &message);
        assert!(result.is_ok());
        // Ed25519 should NOT be cached.
        assert_eq!(cache.len(), 0);
    }
}
