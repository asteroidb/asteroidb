//! Opt-in immutable trust pin for deployments with a static operation
//! Authority keyset and scope. The caller also supplies a digest of the exact
//! Authority assignment and certified placement policy for that scope. The
//! pin is compared with fresh trusted inputs
//! at each use; it never supplies public keys or scope to the verifier.

use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::authority::operation_evidence::{
    EvidenceStoreError, EvidenceVerifier, FILE_NAME, MAGIC, OperationEvidenceStore,
};
use crate::authority::operation_merkle::{
    OperationMerkleCertificate, VerifiedCompactOperation, VerifiedOperation,
};
use crate::operation::{CrdtSubject, OperationReceipt, RecordOutcome};
use crate::store::backend::{create_dir_all_durable, fsync_dir};

const ANCHOR_FILE: &str = "operation-trust-anchor.bin";
const TEMP_ANCHOR_FILE: &str = ".operation-trust-anchor.tmp";
const ANCHOR_MAGIC: &[u8; 8] = b"ASTOA001";
const DOMAIN: &[u8] = b"asteroidb/static-operation-trust/v1";
const USED_MARKER_PREFIX: &str = ".operation-evidence-";

#[derive(Debug, Error)]
pub enum StaticTrustError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Evidence(#[from] EvidenceStoreError),
    #[error("operation trust inputs changed since first use")]
    TrustChanged,
    #[error("existing evidence has no trusted anchor")]
    UnanchoredEvidence,
    #[error("previously used operation evidence directory is missing")]
    MissingEvidence,
    #[error("invalid trusted Authority keyset: {0}")]
    InvalidKeyset(&'static str),
}

/// The verifier must be assembled from current trusted environment and
/// control-plane inputs. Never construct it from the anchor file.
pub struct StaticOperationEvidenceStore {
    evidence: OperationEvidenceStore,
    fingerprint: [u8; 32],
    used_marker: PathBuf,
}

impl StaticOperationEvidenceStore {
    #[cfg(test)]
    pub(crate) fn fail_next_append_for_test(&mut self) {
        self.evidence.fail_next_append_for_test();
    }

    /// Reopen the committed pathname after an uncertain append or rewrite.
    /// The caller must serialize this with every use of the store.
    pub fn reopen_if_poisoned(
        &mut self,
        verifier: &EvidenceVerifier<'_>,
        policy_digest: [u8; 32],
    ) -> Result<(), StaticTrustError> {
        self.check_trust(verifier, &policy_digest)?;
        if !self.evidence.is_poisoned() {
            return Ok(());
        }
        let dir = self.evidence.directory().to_path_buf();
        self.evidence.release_lock_for_recovery()?;
        *self = Self::open(dir, verifier, policy_digest)?;
        Ok(())
    }

    /// Create the pin only for an empty evidence store. Existing evidence
    /// without a pin is refused; an existing pin must exactly match current
    /// trusted inputs before certificate recovery starts. `policy_digest`
    /// must be recomputed from the trusted control plane on each call.
    pub fn open(
        dir: impl AsRef<Path>,
        verifier: &EvidenceVerifier<'_>,
        policy_digest: [u8; 32],
    ) -> Result<Self, StaticTrustError> {
        let dir = dir.as_ref();
        let used_marker = used_marker_path(dir)?;
        if used_marker.exists() && !dir.exists() {
            return Err(StaticTrustError::MissingEvidence);
        }
        create_dir_all_durable(dir)?;
        let fingerprint = fingerprint(verifier, &policy_digest)?;
        let anchor_path = dir.join(ANCHOR_FILE);
        let temp_path = dir.join(TEMP_ANCHOR_FILE);
        let evidence_path = dir.join(FILE_NAME);
        let anchor_exists = anchor_path.exists();
        if used_marker.exists() && (!anchor_exists || !evidence_path.exists()) {
            return Err(StaticTrustError::MissingEvidence);
        }
        if anchor_exists {
            check_anchor(&anchor_path, &fingerprint)?;
            if !evidence_path.exists() {
                return Err(StaticTrustError::UnanchoredEvidence);
            }
        } else if evidence_path.exists() {
            let evidence = fs::read(&evidence_path)?;
            // The first open can stop while writing the log magic, before
            // any anchor or certificate was published. The locked log open
            // below repairs only this strict initial prefix.
            if !MAGIC.starts_with(&evidence) {
                return Err(StaticTrustError::UnanchoredEvidence);
            }
        }
        let evidence = OperationEvidenceStore::open(dir, verifier)?;
        // The pre-lock check can race another bootstrap. Recheck the pin
        // after acquiring the evidence log lock before serving any proof or
        // publishing a new anchor for this trust configuration.
        let anchored = anchor_path.exists();
        if used_marker.exists() && !anchored {
            return Err(StaticTrustError::MissingEvidence);
        }
        if anchor_exists && !anchored {
            return Err(StaticTrustError::UnanchoredEvidence);
        }
        if anchored {
            check_anchor(&anchor_path, &fingerprint)?;
        } else if !evidence.is_empty() {
            return Err(StaticTrustError::UnanchoredEvidence);
        }
        // The evidence lock excludes another live bootstrap. Only this
        // specifically named, uncommitted temp is safe to remove.
        if temp_path.exists() {
            fs::remove_file(&temp_path)?;
        }
        if !anchored {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp_path)?;
            file.write_all(ANCHOR_MAGIC)?;
            file.write_all(&fingerprint)?;
            file.sync_all()?;
            fs::rename(&temp_path, &anchor_path)?;
            File::open(dir)?.sync_all()?;
        }
        if !evidence.is_empty() {
            persist_used_marker(&used_marker)?;
        }
        Ok(Self {
            evidence,
            fingerprint,
            used_marker,
        })
    }

    pub fn check_trust(
        &self,
        verifier: &EvidenceVerifier<'_>,
        policy_digest: &[u8; 32],
    ) -> Result<(), StaticTrustError> {
        if fingerprint(verifier, policy_digest)? != self.fingerprint {
            return Err(StaticTrustError::TrustChanged);
        }
        Ok(())
    }

    pub fn record_and_sync(
        &mut self,
        certificate: OperationMerkleCertificate,
        verifier: &EvidenceVerifier<'_>,
        policy_digest: [u8; 32],
    ) -> Result<RecordOutcome, StaticTrustError> {
        self.check_trust(verifier, &policy_digest)?;
        persist_used_marker(&self.used_marker)?;
        Ok(self.evidence.record_and_sync(certificate, verifier)?)
    }

    pub fn get_verified(
        &self,
        subject: &CrdtSubject,
        verifier: &EvidenceVerifier<'_>,
        policy_digest: [u8; 32],
    ) -> Result<Option<VerifiedOperation>, StaticTrustError> {
        self.check_trust(verifier, &policy_digest)?;
        Ok(self.evidence.get_verified(subject, verifier)?)
    }

    pub fn get_verified_compact(
        &self,
        receipt: &OperationReceipt,
        verifier: &EvidenceVerifier<'_>,
        policy_digest: [u8; 32],
    ) -> Result<Option<VerifiedCompactOperation>, StaticTrustError> {
        self.check_trust(verifier, &policy_digest)?;
        Ok(self.evidence.get_verified_compact(receipt, verifier)?)
    }

    pub fn compact_receipted_and_sync(
        &mut self,
        receipts: &[OperationReceipt],
        verifier: &EvidenceVerifier<'_>,
        policy_digest: [u8; 32],
    ) -> Result<usize, StaticTrustError> {
        self.check_trust(verifier, &policy_digest)?;
        Ok(self
            .evidence
            .compact_receipted_and_sync(receipts, verifier)?)
    }

    /// Reverify every stored proof under the pinned current trust inputs.
    pub fn verified_all(
        &self,
        verifier: &EvidenceVerifier<'_>,
        policy_digest: [u8; 32],
    ) -> Result<Vec<VerifiedOperation>, StaticTrustError> {
        self.check_trust(verifier, &policy_digest)?;
        Ok(self.evidence.verified_all(verifier)?)
    }

    pub fn len(&self) -> usize {
        self.evidence.len()
    }

    /// Whether this pinned evidence store has no certificates.
    pub fn is_empty(&self) -> bool {
        self.evidence.is_empty()
    }
}

fn used_marker_path(dir: &Path) -> Result<PathBuf, StaticTrustError> {
    let parent = dir.parent().ok_or_else(|| {
        StaticTrustError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "operation evidence directory has no parent",
        ))
    })?;
    let name = dir.file_name().ok_or_else(|| {
        StaticTrustError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "operation evidence directory has no name",
        ))
    })?;
    create_dir_all_durable(parent)?;
    let parent = fs::canonicalize(parent)?;
    let digest = Sha256::digest(name.as_encoded_bytes());
    Ok(parent.join(format!("{USED_MARKER_PREFIX}{}.used", hex::encode(digest))))
}

fn persist_used_marker(path: &Path) -> Result<(), StaticTrustError> {
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    file.sync_all()?;
    fsync_dir(path.parent().expect("used marker always has a parent"))?;
    Ok(())
}

fn check_anchor(path: &Path, expected: &[u8; 32]) -> Result<(), StaticTrustError> {
    let content = fs::read(path)?;
    if content.len() != ANCHOR_MAGIC.len() + expected.len()
        || !content.starts_with(ANCHOR_MAGIC)
        || &content[ANCHOR_MAGIC.len()..] != expected
    {
        return Err(StaticTrustError::TrustChanged);
    }
    Ok(())
}

fn update_bytes(hash: &mut Sha256, bytes: &[u8]) {
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
}

fn fingerprint(
    verifier: &EvidenceVerifier<'_>,
    policy_digest: &[u8; 32],
) -> Result<[u8; 32], StaticTrustError> {
    let mut hash = Sha256::new();
    update_bytes(&mut hash, DOMAIN);
    update_bytes(&mut hash, verifier.scope.namespace_id.as_bytes());
    update_bytes(&mut hash, verifier.scope.key_prefix.as_bytes());
    update_bytes(&mut hash, policy_digest);
    hash.update(verifier.epoch_config.duration_secs.to_be_bytes());
    hash.update(verifier.epoch_config.grace_epochs.to_be_bytes());
    hash.update((verifier.faults as u64).to_be_bytes());
    hash.update(verifier.registry.current_version().0.to_be_bytes());
    let mut versions = 0u64;
    for (version, registered_epoch, keys) in verifier.registry.ed25519_keysets() {
        versions += 1;
        if keys.is_empty() {
            return Err(StaticTrustError::InvalidKeyset("empty keyset"));
        }
        hash.update(version.0.to_be_bytes());
        hash.update(registered_epoch.to_be_bytes());
        hash.update((keys.len() as u64).to_be_bytes());
        let mut keys: Vec<_> = keys.iter().collect();
        keys.sort_by(|(a, _), (b, _)| a.0.cmp(&b.0));
        let mut seen = HashSet::new();
        let mut seen_keys = HashSet::new();
        for (id, key) in keys {
            if !seen.insert(&id.0) {
                return Err(StaticTrustError::InvalidKeyset("duplicate Authority ID"));
            }
            if !seen_keys.insert(key.to_bytes()) {
                return Err(StaticTrustError::InvalidKeyset(
                    "duplicate Authority public key",
                ));
            }
            update_bytes(&mut hash, id.0.as_bytes());
            hash.update(key.as_bytes());
        }
    }
    if versions == 0 {
        return Err(StaticTrustError::InvalidKeyset("no keysets"));
    }
    hash.update(versions.to_be_bytes());
    Ok(hash.finalize().into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use tempfile::tempdir;

    #[test]
    fn rejects_recovered_evidence_with_shared_authority_public_key() {
        let dir = tempdir().unwrap();
        let unique_registry = make_registry(0);
        let scope = OperationScope {
            namespace_id: "ns".into(),
            key_prefix: "".into(),
        };
        let config = EpochConfig::default();
        let initial_verifier = EvidenceVerifier {
            registry: &unique_registry,
            scope: &scope,
            current_epoch: 0,
            epoch_config: &config,
            faults: 1,
        };
        drop(StaticOperationEvidenceStore::open(dir.path(), &initial_verifier, [0; 32]).unwrap());
        let key = SigningKey::from_bytes(&[7; 32]).verifying_key();
        let mut registry = KeysetRegistry::new();
        assert!(matches!(
            registry.register_keyset(
                KeysetVersion(1),
                0,
                (0..4).map(|i| (NodeId(format!("auth-{i}")), key)).collect(),
            ),
            Err(crate::authority::certificate::CertError::DuplicateAuthorityKey(_))
        ));
        assert!(registry.get_keys(&KeysetVersion(1)).is_none());
        assert!(StaticOperationEvidenceStore::open(dir.path(), &initial_verifier, [0; 32]).is_ok());
    }

    use crate::authority::certificate::{EpochConfig, KeysetRegistry, KeysetVersion};
    use crate::authority::operation_merkle::OperationScope;
    use crate::types::NodeId;

    fn make_registry(seed: u8) -> KeysetRegistry {
        let mut registry = KeysetRegistry::new();
        registry
            .register_keyset(
                KeysetVersion(1),
                0,
                (1..=4)
                    .map(|i| {
                        (
                            NodeId(format!("auth-{i}")),
                            SigningKey::from_bytes(&[seed + i; 32]).verifying_key(),
                        )
                    })
                    .collect(),
            )
            .unwrap();
        registry
    }

    #[test]
    fn restart_requires_same_trusted_keys_scope_and_epoch_policy() {
        let temp = tempdir().unwrap();
        let registry = make_registry(0);
        let scope = OperationScope {
            namespace_id: "ns".into(),
            key_prefix: "key-".into(),
        };
        let config = EpochConfig::default();
        let verifier = EvidenceVerifier {
            registry: &registry,
            scope: &scope,
            current_epoch: 0,
            epoch_config: &config,
            faults: 1,
        };
        let policy_digest = [7; 32];
        let first =
            StaticOperationEvidenceStore::open(temp.path(), &verifier, policy_digest).unwrap();
        assert_eq!(first.len(), 0);
        assert!(
            first
                .verified_all(&verifier, policy_digest)
                .unwrap()
                .is_empty()
        );
        drop(first);
        StaticOperationEvidenceStore::open(temp.path(), &verifier, policy_digest).unwrap();

        let changed_registry = make_registry(10);
        let changed_keys = EvidenceVerifier {
            registry: &changed_registry,
            ..verifier
        };
        assert!(matches!(
            StaticOperationEvidenceStore::open(temp.path(), &changed_keys, policy_digest),
            Err(StaticTrustError::TrustChanged)
        ));
        let changed_scope = OperationScope {
            namespace_id: "other".into(),
            key_prefix: "key-".into(),
        };
        let changed_scope_verifier = EvidenceVerifier {
            scope: &changed_scope,
            ..verifier
        };
        assert!(matches!(
            StaticOperationEvidenceStore::open(temp.path(), &changed_scope_verifier, policy_digest),
            Err(StaticTrustError::TrustChanged)
        ));
        let changed_config = EpochConfig {
            grace_epochs: config.grace_epochs + 1,
            ..config.clone()
        };
        let changed_epoch_verifier = EvidenceVerifier {
            epoch_config: &changed_config,
            ..verifier
        };
        assert!(matches!(
            StaticOperationEvidenceStore::open(temp.path(), &changed_epoch_verifier, policy_digest),
            Err(StaticTrustError::TrustChanged)
        ));
        let changed_faults_verifier = EvidenceVerifier {
            faults: 0,
            ..verifier
        };
        assert!(matches!(
            StaticOperationEvidenceStore::open(
                temp.path(),
                &changed_faults_verifier,
                policy_digest
            ),
            Err(StaticTrustError::TrustChanged)
        ));
        assert!(matches!(
            StaticOperationEvidenceStore::open(temp.path(), &verifier, [8; 32]),
            Err(StaticTrustError::TrustChanged)
        ));
        let first =
            StaticOperationEvidenceStore::open(temp.path(), &verifier, policy_digest).unwrap();
        assert!(matches!(
            first.verified_all(&verifier, [8; 32]),
            Err(StaticTrustError::TrustChanged)
        ));
    }

    #[test]
    fn missing_or_damaged_anchor_cannot_mount_nonempty_evidence() {
        let temp = tempdir().unwrap();
        let registry = make_registry(0);
        let scope = OperationScope {
            namespace_id: "ns".into(),
            key_prefix: "key-".into(),
        };
        let config = EpochConfig::default();
        let verifier = EvidenceVerifier {
            registry: &registry,
            scope: &scope,
            current_epoch: 0,
            epoch_config: &config,
            faults: 1,
        };
        let policy_digest = [7; 32];
        let first =
            StaticOperationEvidenceStore::open(temp.path(), &verifier, policy_digest).unwrap();
        drop(first);
        let anchor = temp.path().join(ANCHOR_FILE);
        fs::write(&anchor, b"corrupt").unwrap();
        assert!(matches!(
            StaticOperationEvidenceStore::open(temp.path(), &verifier, policy_digest),
            Err(StaticTrustError::TrustChanged)
        ));
        fs::remove_file(&anchor).unwrap();
        let evidence = temp.path().join(FILE_NAME);
        OpenOptions::new()
            .append(true)
            .open(&evidence)
            .unwrap()
            .write_all(b"fake record")
            .unwrap();
        assert!(matches!(
            StaticOperationEvidenceStore::open(temp.path(), &verifier, policy_digest),
            Err(StaticTrustError::UnanchoredEvidence)
        ));
    }

    #[test]
    fn missing_directory_after_certified_evidence_is_rejected() {
        use crate::authority::operation_admission::{
            OperationAdmissionPolicy, OperationLockStore, admit_for_signing,
        };
        use crate::authority::operation_merkle::{
            OperationMerkleTree, OperationRootSupport, SignedOperationRoot,
        };
        use crate::hlc::HlcTimestamp;
        use crate::operation::{
            BoundOperation, CandidateKvAction, CertificationMode, Declaration, EventDot,
            OperationHistory, OperationLeaf,
        };

        struct Allow;
        impl OperationAdmissionPolicy for Allow {
            fn authorize(&self, _: &CrdtSubject, _: &CandidateKvAction) -> bool {
                true
            }
            fn event_allowed(&self, _: &CrdtSubject) -> bool {
                true
            }
            fn key_in_range(&self, _: &str, _: &str) -> bool {
                true
            }
            fn creation_binding_valid(&self, _: &BoundOperation) -> bool {
                true
            }
        }
        #[derive(Default)]
        struct Lock(OperationHistory);
        impl OperationLockStore for Lock {
            type Error = crate::operation::OperationError;
            fn record_and_sync(
                &mut self,
                bound: BoundOperation,
            ) -> Result<RecordOutcome, Self::Error> {
                self.0.record(bound)
            }
        }

        let temp = tempdir().unwrap();
        let evidence_dir = temp.path().join("evidence");
        let keys: Vec<_> = (1..=4).map(|i| SigningKey::from_bytes(&[i; 32])).collect();
        let registry = make_registry(0);
        let scope = OperationScope {
            namespace_id: "ns".into(),
            key_prefix: "key-".into(),
        };
        let config = EpochConfig::default();
        let verifier = EvidenceVerifier {
            registry: &registry,
            scope: &scope,
            current_epoch: 0,
            epoch_config: &config,
            faults: 1,
        };
        let subject = CrdtSubject {
            namespace_id: "ns".into(),
            event_dot: EventDot {
                writer: NodeId("writer".into()),
                counter: 1,
            },
        };
        let operation = BoundOperation {
            operation: OperationLeaf {
                subject: subject.clone(),
                action: CandidateKvAction::Create {
                    key: "key-1".into(),
                    value_bytes: b"value".to_vec(),
                    timestamp: HlcTimestamp {
                        physical: 1,
                        logical: 0,
                        node_id: "writer".into(),
                    },
                },
            },
            declaration: Declaration {
                creation_id: subject.clone(),
                key: "key-1".into(),
                mode: CertificationMode::Enabled,
            },
        };
        let token = admit_for_signing(&Allow, &mut Lock::default(), operation.clone()).unwrap();
        let tree = OperationMerkleTree::from_signable(scope.clone(), &[token]).unwrap();
        let certificate = OperationMerkleCertificate {
            operation,
            supports: keys
                .iter()
                .take(3)
                .enumerate()
                .map(|(i, key)| OperationRootSupport {
                    signed_root: SignedOperationRoot::sign(
                        &tree,
                        KeysetVersion(1),
                        NodeId(format!("auth-{}", i + 1)),
                        key,
                    ),
                    proof: tree.proof(&subject).unwrap(),
                })
                .collect(),
        };
        let digest = [7; 32];
        let mut store =
            StaticOperationEvidenceStore::open(&evidence_dir, &verifier, digest).unwrap();
        assert_eq!(
            store
                .record_and_sync(certificate, &verifier, digest)
                .unwrap(),
            RecordOutcome::New
        );
        drop(store);
        fs::remove_dir_all(&evidence_dir).unwrap();
        assert!(matches!(
            StaticOperationEvidenceStore::open(&evidence_dir, &verifier, digest),
            Err(StaticTrustError::MissingEvidence)
        ));
    }

    #[test]
    fn interrupted_anchor_temp_is_discarded_before_atomic_publish() {
        let temp = tempdir().unwrap();
        fs::write(temp.path().join(TEMP_ANCHOR_FILE), b"partial").unwrap();
        let registry = make_registry(0);
        let scope = OperationScope {
            namespace_id: "ns".into(),
            key_prefix: "key-".into(),
        };
        let config = EpochConfig::default();
        let verifier = EvidenceVerifier {
            registry: &registry,
            scope: &scope,
            current_epoch: 0,
            epoch_config: &config,
            faults: 1,
        };
        let policy_digest = [7; 32];
        let store =
            StaticOperationEvidenceStore::open(temp.path(), &verifier, policy_digest).unwrap();
        assert!(!temp.path().join(TEMP_ANCHOR_FILE).exists());
        assert_eq!(fs::read(temp.path().join(ANCHOR_FILE)).unwrap().len(), 40);
        drop(store);
        StaticOperationEvidenceStore::open(temp.path(), &verifier, policy_digest).unwrap();
    }

    #[test]
    fn interrupted_initial_evidence_magic_is_repaired_only_without_saved_state() {
        let registry = make_registry(0);
        let scope = OperationScope {
            namespace_id: "ns".into(),
            key_prefix: "key-".into(),
        };
        let config = EpochConfig::default();
        let verifier = EvidenceVerifier {
            registry: &registry,
            scope: &scope,
            current_epoch: 0,
            epoch_config: &config,
            faults: 1,
        };
        for prefix_len in [0, 1, 4, MAGIC.len() - 1] {
            let temp = tempdir().unwrap();
            fs::write(temp.path().join(FILE_NAME), &MAGIC[..prefix_len]).unwrap();
            let store =
                StaticOperationEvidenceStore::open(temp.path(), &verifier, [7; 32]).unwrap();
            assert!(store.is_empty());
            drop(store);
            assert_eq!(fs::read(temp.path().join(FILE_NAME)).unwrap(), MAGIC);
            StaticOperationEvidenceStore::open(temp.path(), &verifier, [7; 32]).unwrap();
        }

        let temp = tempdir().unwrap();
        fs::write(temp.path().join(FILE_NAME), &MAGIC[..4]).unwrap();
        fs::write(temp.path().join("saved-state"), b"present").unwrap();
        assert!(StaticOperationEvidenceStore::open(temp.path(), &verifier, [7; 32]).is_err());
        assert_eq!(fs::read(temp.path().join(FILE_NAME)).unwrap(), &MAGIC[..4]);
    }
}
