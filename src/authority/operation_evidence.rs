//! Durable, independently verified operation certificates.
//!
//! This log is separate from the value WAL. Recovered full and compact proofs
//! are exposed only after signature and inclusion checks against trusted keys.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::authority::certificate::{EpochConfig, KeysetRegistry};
use crate::authority::operation_merkle::{
    CompactOperationCertificate, InclusionProof, OperationMerkleCertificate, OperationMerkleError,
    OperationRootSupport, OperationScope, SignedOperationRoot, VerifiedCompactOperation,
    VerifiedOperation,
};
use crate::network::operation_support_transport::SupportPacket;
use crate::network::operation_transport::INTERNAL_OPERATION_BODY_LIMIT;
use crate::operation::{BoundOperation, CrdtSubject, OperationReceipt, RecordOutcome};
use crate::store::backend::{create_dir_all_durable, fsync_dir};

pub(crate) const FILE_NAME: &str = "operation-evidence.log";
pub(crate) const MAGIC: &[u8; 8] = b"ASTOE001";
const MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_OPERATION_AUTHORITIES: usize = 1024;
const TEMP_FILE_NAME: &str = ".operation-evidence.tmp";

#[derive(Debug, Error)]
pub enum EvidenceStoreError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("operation evidence log is already locked: {0}")]
    Lock(#[from] std::fs::TryLockError),
    #[error("invalid persisted evidence: {0}")]
    Invalid(String),
    #[error(transparent)]
    Verification(#[from] OperationMerkleError),
    #[error("operation ID was already certified with different content")]
    ConflictingContent,
}

/// The trusted inputs are supplied by the caller, never read from this log.
/// A new submission is checked at `current_epoch`. Previously accepted
/// evidence is checked at its recorded acceptance epoch, which preserves a
/// certified fact after keyset grace expires. The registry must retain its
/// historical public keys. Revocation is a separate policy, not implicit in
/// keyset grace expiry.
#[derive(Clone, Copy)]
pub struct EvidenceVerifier<'a> {
    pub registry: &'a KeysetRegistry,
    pub scope: &'a OperationScope,
    pub current_epoch: u64,
    pub epoch_config: &'a EpochConfig,
    pub faults: usize,
}

impl EvidenceVerifier<'_> {
    fn verify_at(
        &self,
        certificate: &OperationMerkleCertificate,
        accepted_epoch: u64,
    ) -> Result<VerifiedOperation, EvidenceStoreError> {
        if accepted_epoch > self.current_epoch {
            return Err(invalid("evidence accepted in the future"));
        }
        let version = certificate
            .supports
            .first()
            .ok_or_else(|| EvidenceStoreError::Invalid("certificate has no supports".into()))?
            .signed_root
            .keyset_version
            .clone();
        if self
            .registry
            .registered_epoch(&version)
            .is_none_or(|registered| accepted_epoch < registered)
        {
            return Err(invalid("evidence predates its trusted keyset registration"));
        }
        Ok(certificate.verify(
            self.registry,
            self.scope,
            version,
            accepted_epoch,
            self.epoch_config,
            self.faults,
        )?)
    }

    fn verify_compact_at(
        &self,
        stored: &StoredCompactCertificate,
    ) -> Result<VerifiedCompactOperation, EvidenceStoreError> {
        if stored.accepted_epoch > self.current_epoch {
            return Err(invalid("evidence accepted in the future"));
        }
        let version = stored
            .compact_certificate
            .supports
            .first()
            .ok_or_else(|| {
                EvidenceStoreError::Invalid("compact certificate has no supports".into())
            })?
            .signed_root
            .keyset_version
            .clone();
        if self
            .registry
            .registered_epoch(&version)
            .is_none_or(|registered| stored.accepted_epoch < registered)
        {
            return Err(invalid(
                "compact evidence predates trusted keyset registration",
            ));
        }
        Ok(stored.compact_certificate.verify(
            &stored.receipt,
            self.registry,
            self.scope,
            version,
            stored.accepted_epoch,
            self.epoch_config,
            self.faults,
        )?)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredCertificate {
    accepted_epoch: u64,
    certificate: OperationMerkleCertificate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredCompactCertificate {
    accepted_epoch: u64,
    receipt: OperationReceipt,
    compact_certificate: CompactOperationCertificate,
}

/// Untagged so original `ASTOE001` full frames remain readable unchanged.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
enum StoredRecord {
    Full(StoredCertificate),
    Compact(StoredCompactCertificate),
}

impl StoredRecord {
    fn subject(&self) -> &CrdtSubject {
        match self {
            Self::Full(stored) => &stored.certificate.operation.operation.subject,
            Self::Compact(stored) => &stored.receipt.subject,
        }
    }

    fn commitment(&self) -> [u8; 32] {
        match self {
            Self::Full(stored) => stored.certificate.operation.commitment(),
            Self::Compact(stored) => stored.receipt.commitment,
        }
    }
}

/// Admit a body only if every valid quorum assembled from this fixed keyset
/// fits both durable evidence frame formats, and every support fits HTTP.
/// A u64 leaf count admits at most 64 Merkle siblings. All byte arrays and
/// decimal fields use their maximum JSON length; strings use the actual fixed
/// Authority IDs and scope. This check precedes the first durable support.
pub(crate) fn ensure_certificate_capacity(
    operation: &BoundOperation,
    registry: &KeysetRegistry,
    scope: &OperationScope,
    version: &crate::authority::certificate::KeysetVersion,
) -> Result<(), String> {
    let keys = registry
        .get_keys(version)
        .ok_or("operation certificate keyset is missing")?;
    if keys.is_empty() || keys.len() > MAX_OPERATION_AUTHORITIES {
        return Err(format!(
            "operation certificate Authority count must be 1..={MAX_OPERATION_AUTHORITIES}"
        ));
    }
    let worst_proof = InclusionProof {
        leaf_index: u64::MAX,
        leaf_count: u64::MAX,
        siblings: vec![[255; 32]; 64],
    };
    let supports: Vec<_> = keys
        .iter()
        .map(|(authority_id, _)| OperationRootSupport {
            signed_root: SignedOperationRoot {
                root: [255; 32],
                leaf_count: u64::MAX,
                scope: scope.clone(),
                keyset_version: version.clone(),
                authority_id: authority_id.clone(),
                signature: [255; 64],
            },
            proof: worst_proof.clone(),
        })
        .collect();
    let largest_support = supports
        .iter()
        .max_by_key(|support| serde_json::to_vec(support).map_or(usize::MAX, |bytes| bytes.len()))
        .ok_or("operation certificate has no Authorities")?;
    let packet = SupportPacket {
        operation: operation.clone(),
        support: largest_support.clone(),
    };
    let packet_bytes = serde_json::to_vec(&packet).map_err(|error| error.to_string())?;
    if packet_bytes.len() > INTERNAL_OPERATION_BODY_LIMIT {
        return Err("individual operation support would exceed the HTTP body limit".into());
    }
    let certificate = OperationMerkleCertificate {
        operation: operation.clone(),
        supports,
    };
    let full = StoredRecord::Full(StoredCertificate {
        accepted_epoch: u64::MAX,
        certificate: certificate.clone(),
    });
    let compact = StoredRecord::Compact(StoredCompactCertificate {
        accepted_epoch: u64::MAX,
        receipt: OperationReceipt::from(operation),
        compact_certificate: certificate.compact(),
    });
    for record in [&full, &compact] {
        let bytes = serde_json::to_vec(record).map_err(|error| error.to_string())?;
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(
                "operation certificate could exceed the durable evidence record limit".into(),
            );
        }
    }
    Ok(())
}

/// Certificate log with crash-safe replacement for compacted receipts. The
/// OS file lock permits one writer and recovery reader at a time, preventing
/// concurrent processes from choosing different content for the same ID.
pub struct OperationEvidenceStore {
    file: File,
    path: PathBuf,
    entries: BTreeMap<CrdtSubject, StoredRecord>,
    poisoned: bool,
    #[cfg(test)]
    fail_next_append: bool,
}

impl OperationEvidenceStore {
    #[cfg(test)]
    pub(crate) fn fail_next_append_for_test(&mut self) {
        self.fail_next_append = true;
    }

    pub(crate) fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    pub(crate) fn directory(&self) -> &Path {
        self.path.parent().expect("evidence log has directory")
    }

    /// Release the old inode lock before a trusted caller opens the current
    /// pathname. Keep this handle poisoned if that recovery fails.
    pub(crate) fn release_lock_for_recovery(&mut self) -> Result<(), EvidenceStoreError> {
        let placeholder = File::open(self.directory())?;
        self.poisoned = true;
        self.file = placeholder;
        Ok(())
    }

    pub fn open(
        dir: impl AsRef<Path>,
        verifier: &EvidenceVerifier<'_>,
    ) -> Result<Self, EvidenceStoreError> {
        let dir = dir.as_ref();
        create_dir_all_durable(dir)?;
        let path = dir.join(FILE_NAME);
        let existed = path.exists();
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        file.try_lock()?;
        if existed {
            let mut bytes = Vec::new();
            (&file)
                .take((MAGIC.len() + 1) as u64)
                .read_to_end(&mut bytes)?;
            if bytes.len() < MAGIC.len() && MAGIC.starts_with(&bytes) {
                // A crash during the first MAGIC write cannot have accepted
                // evidence. Require the log to be the only directory entry
                // before finishing that interrupted initialization.
                let entries = fs::read_dir(dir)?
                    .map(|entry| entry.map(|entry| entry.path()))
                    .collect::<io::Result<Vec<_>>>()?;
                if entries.as_slice() == [path.clone()] {
                    file.set_len(0)?;
                    file.seek(SeekFrom::Start(0))?;
                    file.write_all(MAGIC)?;
                    file.sync_all()?;
                    fsync_dir(dir)?;
                }
            }
            file.seek(SeekFrom::Start(0))?;
        }
        if !existed {
            file.write_all(MAGIC)?;
            file.sync_all()?;
            File::open(dir)?.sync_all()?;
        }
        // The committed log is locked before discarding an uncommitted
        // replacement left by a crash before rename.
        let temp_path = dir.join(TEMP_FILE_NAME);
        if temp_path.exists() {
            fs::remove_file(temp_path)?;
            fsync_dir(dir)?;
        }
        let entries = recover(&mut file, &path, verifier)?;
        // A complete frame or replacement may have survived a failed sync.
        // Make recovered evidence durable before it can be served or retried.
        file.sync_all()?;
        fsync_dir(dir)?;
        file.seek(SeekFrom::End(0))?;
        Ok(Self {
            file,
            path,
            entries,
            poisoned: false,
            #[cfg(test)]
            fail_next_append: false,
        })
    }

    /// An `Ok` result means the complete certificate has reached `sync_all`.
    /// On any write or sync failure the caller must discard this store handle
    /// and reopen it before another append; the in-memory index is unchanged.
    pub fn record_and_sync(
        &mut self,
        certificate: OperationMerkleCertificate,
        verifier: &EvidenceVerifier<'_>,
    ) -> Result<RecordOutcome, EvidenceStoreError> {
        if self.poisoned {
            return Err(invalid(
                "evidence writer must be reopened after failed append",
            ));
        }
        let verified = verifier.verify_at(&certificate, verifier.current_epoch)?;
        let subject = verified.bound().operation.subject.clone();
        if let Some(existing) = self.entries.get(&subject) {
            if existing.commitment() != certificate.operation.commitment()
                || matches!(existing, StoredRecord::Full(full) if full.certificate.operation != certificate.operation)
            {
                return Err(EvidenceStoreError::ConflictingContent);
            }
            // A different valid proof for the same exact operation is still a
            // retry. The first complete certificate is retained.
            return Ok(RecordOutcome::Retry);
        }
        let stored = StoredRecord::Full(StoredCertificate {
            accepted_epoch: verifier.current_epoch,
            certificate,
        });
        let frame = encode_frame(&stored)?;
        #[cfg(test)]
        if self.fail_next_append {
            self.fail_next_append = false;
            self.poisoned = true;
            return Err(io::Error::other("injected evidence append failure").into());
        }
        self.poisoned = true;
        self.file.write_all(&frame)?;
        self.file.sync_all()?;
        self.entries.insert(subject, stored);
        self.poisoned = false;
        Ok(RecordOutcome::New)
    }

    /// Rechecks the certificate with trusted keys and its original acceptance
    /// epoch. Keyset grace controls new certification, not the lifetime of a
    /// previously accepted certified fact.
    pub fn get_verified(
        &self,
        subject: &CrdtSubject,
        verifier: &EvidenceVerifier<'_>,
    ) -> Result<Option<VerifiedOperation>, EvidenceStoreError> {
        if self.poisoned {
            return Err(invalid(
                "evidence store must be reopened after uncertain write",
            ));
        }
        match self.entries.get(subject) {
            Some(StoredRecord::Full(stored)) => Ok(Some(
                verifier.verify_at(&stored.certificate, stored.accepted_epoch)?,
            )),
            _ => Ok(None),
        }
    }

    /// A compact proof is usable only when the caller supplies the exact
    /// authenticated ledger receipt. The embedded receipt is checked again.
    pub fn get_verified_compact(
        &self,
        receipt: &OperationReceipt,
        verifier: &EvidenceVerifier<'_>,
    ) -> Result<Option<VerifiedCompactOperation>, EvidenceStoreError> {
        if self.poisoned {
            return Err(invalid(
                "evidence store must be reopened after uncertain write",
            ));
        }
        match self.entries.get(&receipt.subject) {
            Some(StoredRecord::Compact(stored)) => {
                if stored.receipt != *receipt {
                    return Err(invalid("compact evidence differs from ledger receipt"));
                }
                Ok(Some(verifier.verify_compact_at(stored)?))
            }
            _ => Ok(None),
        }
    }

    /// Recheck every retained certificate before producing a read basis.
    /// Any invalid or untrusted entry fails the entire projection.
    pub fn verified_all(
        &self,
        verifier: &EvidenceVerifier<'_>,
    ) -> Result<Vec<VerifiedOperation>, EvidenceStoreError> {
        if self.poisoned {
            return Err(invalid(
                "evidence store must be reopened after uncertain write",
            ));
        }
        self.entries
            .values()
            .filter_map(|stored| match stored {
                StoredRecord::Full(stored) => Some(stored),
                StoredRecord::Compact(_) => None,
            })
            .map(|stored| verifier.verify_at(&stored.certificate, stored.accepted_epoch))
            .collect()
    }

    /// Rewrite certified evidence whose exact ledger body has become a
    /// durable receipt. The caller must first discharge every obligation
    /// requiring the full certificate (including outbox reconciliation).
    /// The old complete log or the new complete log survives any crash.
    pub fn compact_receipted_and_sync(
        &mut self,
        receipts: &[OperationReceipt],
        verifier: &EvidenceVerifier<'_>,
    ) -> Result<usize, EvidenceStoreError> {
        if self.poisoned {
            return Err(invalid(
                "evidence writer must be reopened after failed rewrite",
            ));
        }
        let receipts: BTreeMap<_, _> = receipts
            .iter()
            .map(|receipt| (receipt.subject.clone(), receipt))
            .collect();
        let mut next = self.entries.clone();
        let mut compacted = 0;
        for (subject, record) in &self.entries {
            let StoredRecord::Full(stored) = record else {
                continue;
            };
            let Some(receipt) = receipts.get(subject) else {
                continue;
            };
            if **receipt != OperationReceipt::from(&stored.certificate.operation) {
                return Err(invalid(
                    "evidence compaction receipt differs from certificate",
                ));
            }
            verifier.verify_at(&stored.certificate, stored.accepted_epoch)?;
            let candidate = StoredCompactCertificate {
                accepted_epoch: stored.accepted_epoch,
                receipt: (**receipt).clone(),
                compact_certificate: stored.certificate.compact(),
            };
            verifier.verify_compact_at(&candidate)?;
            next.insert(subject.clone(), StoredRecord::Compact(candidate));
            compacted += 1;
        }
        if compacted == 0 {
            return Ok(0);
        }
        let dir = self
            .path
            .parent()
            .ok_or_else(|| invalid("evidence log has no directory"))?;
        let temp_path = dir.join(TEMP_FILE_NAME);
        self.poisoned = true;
        let mut replacement = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&temp_path)?;
        replacement.try_lock()?;
        replacement.write_all(MAGIC)?;
        for record in next.values() {
            replacement.write_all(&encode_frame(record)?)?;
        }
        replacement.sync_all()?;
        fs::rename(&temp_path, &self.path)?;
        fsync_dir(dir)?;
        self.file = replacement;
        self.entries = next;
        self.poisoned = false;
        Ok(compacted)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

fn invalid(reason: impl Into<String>) -> EvidenceStoreError {
    EvidenceStoreError::Invalid(reason.into())
}

fn encode_frame(stored: &StoredRecord) -> Result<Vec<u8>, EvidenceStoreError> {
    let payload =
        serde_json::to_vec(stored).map_err(|e| EvidenceStoreError::Invalid(e.to_string()))?;
    if payload.len() > MAX_RECORD_BYTES {
        return Err(invalid("certificate too large"));
    }
    let mut frame = Vec::with_capacity(12 + payload.len());
    let length = (payload.len() as u32).to_be_bytes();
    frame.extend_from_slice(&length);
    frame.extend_from_slice(&crc32fast::hash(&length).to_be_bytes());
    frame.extend_from_slice(&crc32fast::hash(&payload).to_be_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

fn recover(
    file: &mut File,
    path: &Path,
    verifier: &EvidenceVerifier<'_>,
) -> Result<BTreeMap<CrdtSubject, StoredRecord>, EvidenceStoreError> {
    let mut bytes = Vec::new();
    file.seek(SeekFrom::Start(0))?;
    file.read_to_end(&mut bytes)?;
    if !bytes.starts_with(MAGIC) {
        return Err(invalid(format!("{} has invalid magic", path.display())));
    }
    let mut index = MAGIC.len();
    let mut entries: BTreeMap<CrdtSubject, StoredRecord> = BTreeMap::new();
    while index < bytes.len() {
        let remainder = &bytes[index..];
        if remainder.len() < 12 {
            break; // incomplete, never acknowledged final frame
        }
        let length = u32::from_be_bytes(remainder[..4].try_into().expect("four bytes")) as usize;
        let header_checksum = u32::from_be_bytes(remainder[4..8].try_into().expect("four bytes"));
        if crc32fast::hash(&remainder[..4]) != header_checksum {
            return Err(invalid("evidence frame header checksum mismatch"));
        }
        if length == 0 || length > MAX_RECORD_BYTES {
            return Err(invalid("invalid evidence frame length"));
        }
        if remainder.len() < length + 12 {
            break;
        }
        let checksum = u32::from_be_bytes(remainder[8..12].try_into().expect("four bytes"));
        let payload = &remainder[12..12 + length];
        if crc32fast::hash(payload) != checksum {
            return Err(invalid("evidence frame checksum mismatch"));
        }
        let stored: StoredRecord = serde_json::from_slice(payload)
            .map_err(|e| invalid(format!("evidence decode failed: {e}")))?;
        match &stored {
            StoredRecord::Full(full) => {
                verifier.verify_at(&full.certificate, full.accepted_epoch)?;
            }
            StoredRecord::Compact(compact) => {
                verifier.verify_compact_at(compact)?;
            }
        }
        let subject = stored.subject().clone();
        if let Some(previous) = entries.get(&subject) {
            if previous.commitment() != stored.commitment()
                || matches!((previous, &stored), (StoredRecord::Full(a), StoredRecord::Full(b)) if a.certificate.operation != b.certificate.operation)
                || matches!(previous, StoredRecord::Compact(_))
                || std::mem::discriminant(previous) != std::mem::discriminant(&stored)
            {
                return Err(EvidenceStoreError::ConflictingContent);
            }
        } else {
            entries.insert(subject, stored);
        }
        index += length + 12;
    }
    // A torn final frame was never acknowledged. Remove it before appending.
    if index < bytes.len() {
        file.set_len(index as u64)?;
        file.sync_all()?;
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use tempfile::tempdir;

    use crate::authority::certificate::KeysetVersion;
    use crate::authority::operation_admission::{
        OperationAdmissionPolicy, OperationLockStore, SignableCommitment, admit_for_signing,
    };
    use crate::authority::operation_merkle::{
        OperationMerkleTree, OperationRootSupport, SignedOperationRoot,
    };
    use crate::authority::static_operation_trust::StaticOperationEvidenceStore;
    use crate::hlc::HlcTimestamp;
    use crate::operation::{
        BoundOperation, CandidateKvAction, CertificationMode, Declaration, EventDot,
        OperationHistory, OperationLeaf,
    };
    use crate::types::NodeId;

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
        fn record_and_sync(&mut self, bound: BoundOperation) -> Result<RecordOutcome, Self::Error> {
            self.0.record(bound)
        }
    }
    fn bound(counter: u64) -> BoundOperation {
        let subject = CrdtSubject {
            namespace_id: "ns".into(),
            event_dot: EventDot {
                writer: NodeId("writer".into()),
                counter,
            },
        };
        BoundOperation {
            operation: OperationLeaf {
                subject: subject.clone(),
                action: CandidateKvAction::Create {
                    key: format!("key-{counter}"),
                    value_bytes: b"value".to_vec(),
                    timestamp: HlcTimestamp {
                        physical: 1,
                        logical: 0,
                        node_id: "writer".into(),
                    },
                },
            },
            declaration: Declaration {
                creation_id: subject,
                key: format!("key-{counter}"),
                mode: CertificationMode::Enabled,
            },
        }
    }
    fn fixture() -> (
        OperationMerkleCertificate,
        KeysetRegistry,
        OperationScope,
        EpochConfig,
    ) {
        let scope = OperationScope {
            namespace_id: "ns".into(),
            key_prefix: "key-".into(),
        };
        let operation = bound(1);
        let keys: Vec<_> = (1..=4).map(|i| SigningKey::from_bytes(&[i; 32])).collect();
        let cert = certificate_for(operation, &scope, &keys);
        let mut registry = KeysetRegistry::new();
        registry
            .register_keyset(
                KeysetVersion(1),
                0,
                keys.iter()
                    .enumerate()
                    .map(|(i, key)| (NodeId(format!("a{i}")), key.verifying_key()))
                    .collect(),
            )
            .unwrap();
        (cert, registry, scope, EpochConfig::default())
    }

    #[test]
    fn support_admission_checks_wire_and_full_evidence_capacity() {
        let (_, registry, scope, _) = fixture();
        let version = KeysetVersion(1);
        assert!(ensure_certificate_capacity(&bound(1), &registry, &scope, &version).is_ok());

        let mut large_body = bound(1);
        if let CandidateKvAction::Create { value_bytes, .. } = &mut large_body.operation.action {
            *value_bytes = vec![0; 5 * 1024 * 1024];
        }
        assert!(
            ensure_certificate_capacity(&large_body, &registry, &scope, &version)
                .unwrap_err()
                .contains("individual operation support")
        );

        // Each individual support fits the wire limit, but three large
        // Authority IDs would make the complete evidence frame exceed 16 MiB.
        let mut wide_registry = KeysetRegistry::new();
        wide_registry
            .register_keyset(
                version.clone(),
                0,
                (1..=3)
                    .map(|byte| {
                        (
                            NodeId(format!("{}-{byte}", "a".repeat(6 * 1024 * 1024))),
                            SigningKey::from_bytes(&[byte; 32]).verifying_key(),
                        )
                    })
                    .collect(),
            )
            .unwrap();
        assert!(
            ensure_certificate_capacity(&bound(1), &wide_registry, &scope, &version)
                .unwrap_err()
                .contains("durable evidence record")
        );
    }

    fn certificate_for(
        operation: BoundOperation,
        scope: &OperationScope,
        keys: &[SigningKey],
    ) -> OperationMerkleCertificate {
        let signable: SignableCommitment =
            admit_for_signing(&Allow, &mut Lock::default(), operation.clone()).unwrap();
        let tree = OperationMerkleTree::from_signable(scope.clone(), &[signable]).unwrap();
        let supports = keys
            .iter()
            .take(3)
            .enumerate()
            .map(|(i, key)| OperationRootSupport {
                signed_root: SignedOperationRoot::sign(
                    &tree,
                    KeysetVersion(1),
                    NodeId(format!("a{i}")),
                    key,
                ),
                proof: tree.proof(&operation.operation.subject).unwrap(),
            })
            .collect();
        OperationMerkleCertificate {
            operation,
            supports,
        }
    }

    #[test]
    fn synced_evidence_survives_restart_and_keyset_grace_expiry() {
        let temp = tempdir().unwrap();
        let (cert, mut registry, scope, config) = fixture();
        let verifier = EvidenceVerifier {
            registry: &registry,
            scope: &scope,
            current_epoch: 0,
            epoch_config: &config,
            faults: 1,
        };
        {
            let mut store = OperationEvidenceStore::open(temp.path(), &verifier).unwrap();
            assert_eq!(
                store.record_and_sync(cert.clone(), &verifier).unwrap(),
                RecordOutcome::New
            );
            assert_eq!(
                store.record_and_sync(cert.clone(), &verifier).unwrap(),
                RecordOutcome::Retry
            );
        }
        registry
            .register_keyset(
                KeysetVersion(2),
                1,
                vec![(
                    NodeId("new".into()),
                    SigningKey::from_bytes(&[9; 32]).verifying_key(),
                )],
            )
            .unwrap();
        let later = EvidenceVerifier {
            registry: &registry,
            scope: &scope,
            current_epoch: 20,
            epoch_config: &config,
            faults: 1,
        };
        let mut store = OperationEvidenceStore::open(temp.path(), &later).unwrap();
        assert_eq!(
            store
                .get_verified(&cert.operation.operation.subject, &later)
                .unwrap()
                .unwrap()
                .bound(),
            &cert.operation
        );
        let all = store.verified_all(&later).unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].bound(), &cert.operation);
        let new_operation = bound(2);
        let mut changed = cert.clone();
        changed.operation = new_operation;
        assert!(store.record_and_sync(changed, &later).is_err());
    }

    #[test]
    fn all_verified_evidence_rejects_future_acceptance_epoch() {
        let temp = tempdir().unwrap();
        let (cert, registry, scope, config) = fixture();
        let accepted = EvidenceVerifier {
            registry: &registry,
            scope: &scope,
            current_epoch: 5,
            epoch_config: &config,
            faults: 1,
        };
        let mut store = OperationEvidenceStore::open(temp.path(), &accepted).unwrap();
        store.record_and_sync(cert.clone(), &accepted).unwrap();
        let earlier = EvidenceVerifier {
            current_epoch: 4,
            ..accepted
        };
        assert!(
            store
                .get_verified(&cert.operation.operation.subject, &earlier)
                .is_err()
        );
        assert!(store.verified_all(&earlier).is_err());
        let receipt = OperationReceipt::from(&cert.operation);
        assert!(
            store
                .compact_receipted_and_sync(std::slice::from_ref(&receipt), &earlier)
                .is_err()
        );
        assert!(
            store
                .get_verified(&cert.operation.operation.subject, &accepted)
                .unwrap()
                .is_some()
        );
        assert_eq!(
            store
                .compact_receipted_and_sync(std::slice::from_ref(&receipt), &accepted)
                .unwrap(),
            1
        );
        assert!(store.get_verified_compact(&receipt, &earlier).is_err());
    }

    #[test]
    fn compact_rewrite_removes_value_and_recovers_verified_receipt() {
        let temp = tempdir().unwrap();
        let (_, registry, scope, config) = fixture();
        let mut operation = bound(1);
        if let CandidateKvAction::Create { value_bytes, .. } = &mut operation.operation.action {
            *value_bytes = vec![42; 8192];
        }
        let keys: Vec<_> = (1..=4).map(|i| SigningKey::from_bytes(&[i; 32])).collect();
        let cert = certificate_for(operation, &scope, &keys);
        let verifier = EvidenceVerifier {
            registry: &registry,
            scope: &scope,
            current_epoch: 0,
            epoch_config: &config,
            faults: 1,
        };
        let receipt = OperationReceipt::from(&cert.operation);
        let subject = receipt.subject.clone();
        {
            let mut store = OperationEvidenceStore::open(temp.path(), &verifier).unwrap();
            store.record_and_sync(cert.clone(), &verifier).unwrap();
            let before = fs::metadata(temp.path().join(FILE_NAME)).unwrap().len();
            assert_eq!(
                store
                    .compact_receipted_and_sync(std::slice::from_ref(&receipt), &verifier)
                    .unwrap(),
                1
            );
            assert_eq!(
                store
                    .compact_receipted_and_sync(std::slice::from_ref(&receipt), &verifier)
                    .unwrap(),
                0
            );
            assert!(fs::metadata(temp.path().join(FILE_NAME)).unwrap().len() < before);
            assert!(store.get_verified(&subject, &verifier).unwrap().is_none());
            assert!(
                store
                    .get_verified_compact(&receipt, &verifier)
                    .unwrap()
                    .is_some()
            );
            assert_eq!(
                store.record_and_sync(cert.clone(), &verifier).unwrap(),
                RecordOutcome::Retry
            );
            assert!(matches!(
                OperationEvidenceStore::open(temp.path(), &verifier),
                Err(EvidenceStoreError::Lock(_))
            ));
        }
        let bytes = fs::read(temp.path().join(FILE_NAME)).unwrap();
        assert!(
            !bytes
                .windows(b"value_bytes".len())
                .any(|part| part == b"value_bytes")
        );
        let store = OperationEvidenceStore::open(temp.path(), &verifier).unwrap();
        let compact = store
            .get_verified_compact(&receipt, &verifier)
            .unwrap()
            .unwrap();
        assert_eq!(compact.certificate(), &cert.compact());
        assert_eq!(store.len(), 1);
        assert!(store.verified_all(&verifier).unwrap().is_empty());
        let mut wrong = receipt;
        wrong.commitment[0] ^= 1;
        assert!(store.get_verified_compact(&wrong, &verifier).is_err());
    }

    #[test]
    fn compaction_rejects_mismatched_receipt_and_recovery_ignores_uncommitted_temp() {
        let temp = tempdir().unwrap();
        let (cert, registry, scope, config) = fixture();
        let verifier = EvidenceVerifier {
            registry: &registry,
            scope: &scope,
            current_epoch: 0,
            epoch_config: &config,
            faults: 1,
        };
        let mut wrong = OperationReceipt::from(&cert.operation);
        wrong.commitment[0] ^= 1;
        {
            let mut store = OperationEvidenceStore::open(temp.path(), &verifier).unwrap();
            store.record_and_sync(cert.clone(), &verifier).unwrap();
            assert!(
                store
                    .compact_receipted_and_sync(&[wrong], &verifier)
                    .is_err()
            );
        }
        fs::write(temp.path().join(TEMP_FILE_NAME), b"torn replacement").unwrap();
        let store = OperationEvidenceStore::open(temp.path(), &verifier).unwrap();
        assert!(!temp.path().join(TEMP_FILE_NAME).exists());
        assert!(
            store
                .get_verified(&cert.operation.operation.subject, &verifier)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn uncertain_rewrite_cannot_serve_stale_evidence() {
        let temp = tempdir().unwrap();
        let (cert, registry, scope, config) = fixture();
        let verifier = EvidenceVerifier {
            registry: &registry,
            scope: &scope,
            current_epoch: 0,
            epoch_config: &config,
            faults: 1,
        };
        let mut store = OperationEvidenceStore::open(temp.path(), &verifier).unwrap();
        store.record_and_sync(cert.clone(), &verifier).unwrap();
        store.poisoned = true;
        assert!(
            store
                .get_verified(&cert.operation.operation.subject, &verifier)
                .is_err()
        );
        assert!(
            store
                .get_verified_compact(&OperationReceipt::from(&cert.operation), &verifier)
                .is_err()
        );
        assert!(store.verified_all(&verifier).is_err());
        assert!(store.compact_receipted_and_sync(&[], &verifier).is_err());
    }

    #[test]
    fn static_store_reopens_after_uncertain_rewrite() {
        let temp = tempdir().unwrap();
        let (cert, registry, scope, config) = fixture();
        let verifier = EvidenceVerifier {
            registry: &registry,
            scope: &scope,
            current_epoch: 0,
            epoch_config: &config,
            faults: 1,
        };
        let digest = [7; 32];
        let receipt = OperationReceipt::from(&cert.operation);
        let mut store = StaticOperationEvidenceStore::open(temp.path(), &verifier, digest).unwrap();
        store.record_and_sync(cert, &verifier, digest).unwrap();
        fs::write(temp.path().join(TEMP_FILE_NAME), b"interrupted rewrite").unwrap();
        assert!(
            store
                .compact_receipted_and_sync(std::slice::from_ref(&receipt), &verifier, digest)
                .is_err()
        );
        assert!(
            store
                .get_verified(&receipt.subject, &verifier, digest)
                .is_err()
        );
        store.reopen_if_poisoned(&verifier, digest).unwrap();
        assert!(!temp.path().join(TEMP_FILE_NAME).exists());
        assert!(
            store
                .get_verified(&receipt.subject, &verifier, digest)
                .unwrap()
                .is_some()
        );
        assert_eq!(
            store
                .compact_receipted_and_sync(std::slice::from_ref(&receipt), &verifier, digest)
                .unwrap(),
            1
        );
        assert!(
            store
                .get_verified_compact(&receipt, &verifier, digest)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn corruption_and_conflicting_replay_fail_closed() {
        let temp = tempdir().unwrap();
        let (cert, registry, scope, config) = fixture();
        let verifier = EvidenceVerifier {
            registry: &registry,
            scope: &scope,
            current_epoch: 0,
            epoch_config: &config,
            faults: 1,
        };
        {
            let mut store = OperationEvidenceStore::open(temp.path(), &verifier).unwrap();
            store.record_and_sync(cert, &verifier).unwrap();
        }
        let path = temp.path().join(FILE_NAME);
        let mut bytes = fs::read(&path).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        fs::write(&path, bytes).unwrap();
        assert!(matches!(
            OperationEvidenceStore::open(temp.path(), &verifier),
            Err(EvidenceStoreError::Invalid(_))
        ));
    }

    #[test]
    fn same_id_with_different_valid_signed_content_is_rejected() {
        let temp = tempdir().unwrap();
        let (cert, registry, scope, config) = fixture();
        let verifier = EvidenceVerifier {
            registry: &registry,
            scope: &scope,
            current_epoch: 0,
            epoch_config: &config,
            faults: 1,
        };
        let mut store = OperationEvidenceStore::open(temp.path(), &verifier).unwrap();
        store.record_and_sync(cert.clone(), &verifier).unwrap();
        let mut different = cert.operation.clone();
        if let CandidateKvAction::Create { value_bytes, .. } = &mut different.operation.action {
            *value_bytes = b"other".to_vec();
        }
        let keys: Vec<_> = (1..=4).map(|i| SigningKey::from_bytes(&[i; 32])).collect();
        let conflicting = certificate_for(different, &scope, &keys);
        assert!(matches!(
            store.record_and_sync(conflicting, &verifier),
            Err(EvidenceStoreError::ConflictingContent)
        ));
        drop(store);
        let reopened = OperationEvidenceStore::open(temp.path(), &verifier).unwrap();
        assert_eq!(reopened.len(), 1);
        assert_eq!(
            reopened
                .get_verified(&cert.operation.operation.subject, &verifier)
                .unwrap()
                .unwrap()
                .bound(),
            &cert.operation
        );
    }

    #[test]
    fn second_writer_cannot_race_first_writer() {
        let temp = tempdir().unwrap();
        let (_, registry, scope, config) = fixture();
        let verifier = EvidenceVerifier {
            registry: &registry,
            scope: &scope,
            current_epoch: 0,
            epoch_config: &config,
            faults: 1,
        };
        let first = OperationEvidenceStore::open(temp.path(), &verifier).unwrap();
        assert!(matches!(
            OperationEvidenceStore::open(temp.path(), &verifier),
            Err(EvidenceStoreError::Lock(_))
        ));
        drop(first);
        OperationEvidenceStore::open(temp.path(), &verifier).unwrap();
    }

    #[test]
    fn incomplete_tail_is_removed_before_next_append() {
        let temp = tempdir().unwrap();
        let (cert, registry, scope, config) = fixture();
        let verifier = EvidenceVerifier {
            registry: &registry,
            scope: &scope,
            current_epoch: 0,
            epoch_config: &config,
            faults: 1,
        };
        {
            let mut store = OperationEvidenceStore::open(temp.path(), &verifier).unwrap();
            store.record_and_sync(cert.clone(), &verifier).unwrap();
        }
        let path = temp.path().join(FILE_NAME);
        let good_len = fs::metadata(&path).unwrap().len();
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&[0, 0, 0])
            .unwrap();
        let store = OperationEvidenceStore::open(temp.path(), &verifier).unwrap();
        assert_eq!(store.len(), 1);
        assert_eq!(fs::metadata(&path).unwrap().len(), good_len);
    }

    #[test]
    fn valid_frame_checksum_cannot_replace_signed_operation() {
        let temp = tempdir().unwrap();
        let (cert, registry, scope, config) = fixture();
        let verifier = EvidenceVerifier {
            registry: &registry,
            scope: &scope,
            current_epoch: 0,
            epoch_config: &config,
            faults: 1,
        };
        {
            let mut store = OperationEvidenceStore::open(temp.path(), &verifier).unwrap();
            store.record_and_sync(cert, &verifier).unwrap();
        }
        let path = temp.path().join(FILE_NAME);
        let mut bytes = fs::read(&path).unwrap();
        let start = MAGIC.len() + 12;
        let mut stored: StoredCertificate = serde_json::from_slice(&bytes[start..]).unwrap();
        if let CandidateKvAction::Create { value_bytes, .. } =
            &mut stored.certificate.operation.operation.action
        {
            value_bytes[0] ^= 1;
        }
        let replacement = serde_json::to_vec(&stored).unwrap();
        assert_eq!(replacement.len(), bytes.len() - start);
        bytes[start..].copy_from_slice(&replacement);
        bytes[MAGIC.len() + 8..MAGIC.len() + 12]
            .copy_from_slice(&crc32fast::hash(&replacement).to_be_bytes());
        fs::write(&path, bytes).unwrap();
        assert!(matches!(
            OperationEvidenceStore::open(temp.path(), &verifier),
            Err(EvidenceStoreError::Verification(
                OperationMerkleError::InvalidProof
            ))
        ));
    }
}
