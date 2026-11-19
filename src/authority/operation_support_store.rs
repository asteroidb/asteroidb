//! Durable collection of individually verified operation Merkle supports.
//! A support is never a certificate until a distinct quorum is assembled.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use ed25519_dalek::{Signature, Verifier};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::authority::certificate::{EpochConfig, KeysetRegistry, KeysetVersion};
use crate::authority::operation_evidence::{
    MAX_OPERATION_AUTHORITIES, ensure_certificate_capacity,
};
use crate::authority::operation_merkle::{
    OperationMerkleCertificate, OperationMerkleError, OperationRootSupport, OperationScope,
};
use crate::operation::{BoundOperation, CrdtSubject, OperationReceipt, RecordOutcome};
use crate::store::backend::{create_dir_all_durable, fsync_dir};
use crate::types::NodeId;

const FILE_NAME: &str = "operation-supports.log";
const MAGIC: &[u8; 8] = b"ASTOS001";
const MAX_FRAME: usize = 16 * 1024 * 1024;
const TEMP_FILE_NAME: &str = ".operation-supports.tmp";
const NEIGHBOR_MANIFEST_FILE_NAME: &str = ".operation-neighbor-manifest";
const NEIGHBOR_MANIFEST_TEMP_FILE_NAME: &str = ".operation-neighbor-manifest.tmp";
const USED_MARKER_MAGIC: &[u8] = b"ASTOS-USED-1";

fn used_marker_path(dir: &Path) -> Result<std::path::PathBuf, SupportStoreError> {
    let parent = dir
        .parent()
        .ok_or_else(|| SupportStoreError::Invalid("support log has no parent directory".into()))?;
    let parent = if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    };
    let name = dir
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            SupportStoreError::Invalid("support log has no UTF-8 directory name".into())
        })?;
    Ok(parent.join(format!(".{name}.operation-supports.used")))
}

#[derive(Debug, Error)]
pub enum SupportStoreError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("operation support log is already locked: {0}")]
    Lock(#[from] std::fs::TryLockError),
    #[error(transparent)]
    Verification(#[from] OperationMerkleError),
    #[error("invalid operation support log: {0}")]
    Invalid(String),
    #[error("operation support exceeds certification capacity: {0}")]
    Capacity(String),
    #[error("operation ID or Authority already has conflicting support")]
    Conflict,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SupportRecord {
    operation: BoundOperation,
    support: OperationRootSupport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CompactSupportRecord {
    version: u32,
    receipt: OperationReceipt,
    support: OperationRootSupport,
}

#[derive(Clone)]
struct StoredSupport {
    operation: Option<BoundOperation>,
    support: OperationRootSupport,
}

#[derive(Default, Clone)]
struct Collected {
    receipt: Option<OperationReceipt>,
    compacted: bool,
    by_authority: BTreeMap<NodeId, Vec<StoredSupport>>,
}

pub struct OperationSupportStore {
    file: File,
    records: BTreeMap<(CrdtSubject, [u8; 32]), Collected>,
    registry: KeysetRegistry,
    scope: OperationScope,
    epoch_config: EpochConfig,
    keyset_version: KeysetVersion,
    poisoned: bool,
    path: std::path::PathBuf,
    neighbor_manifest: Option<[u8; 32]>,
}

impl OperationSupportStore {
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Reopen after an uncertain append, manifest write, or rewrite. The
    /// caller supplies the authenticated ledger receipts and current neighbor
    /// topology before any recovered compact support becomes visible.
    pub fn reopen_if_poisoned(
        &mut self,
        registry: &KeysetRegistry,
        scope: &OperationScope,
        epoch_config: &EpochConfig,
        neighbor_digest: [u8; 32],
        ledger_receipts: &[OperationReceipt],
    ) -> Result<(), SupportStoreError> {
        if !self.poisoned {
            return Ok(());
        }
        let dir = self
            .path
            .parent()
            .ok_or_else(|| SupportStoreError::Invalid("support log has no directory".into()))?
            .to_path_buf();
        // A failed reopen must leave the old handle unusable. Drop its inode
        // lock before opening the pathname, which compaction may have replaced.
        self.file = File::open(&dir)?;
        let recovered = Self::open(&dir, registry, scope, epoch_config)?;
        recovered.check_neighbor_manifest(neighbor_digest)?;
        for receipt in recovered.compact_receipts() {
            if !ledger_receipts.contains(receipt) {
                return Err(SupportStoreError::Invalid(
                    "compact support differs from authenticated ledger receipt".into(),
                ));
            }
        }
        *self = recovered;
        Ok(())
    }

    /// Open a static-epoch support log. The supplied registry and scope must
    /// come from current trusted inputs, not the log. The caller must retain
    /// an immutable deployment trust pin while this store is mounted.
    pub fn open(
        dir: impl AsRef<Path>,
        registry: &KeysetRegistry,
        scope: &OperationScope,
        epoch_config: &EpochConfig,
    ) -> Result<Self, SupportStoreError> {
        let keyset_version = registry.current_version();
        if registry.registered_epoch(&keyset_version) != Some(0) {
            return Err(SupportStoreError::Invalid(
                "static support keyset must register at epoch zero".into(),
            ));
        }
        let keys = registry
            .get_keys(&keyset_version)
            .ok_or_else(|| SupportStoreError::Invalid("missing static keyset".into()))?;
        let mut seen = HashSet::new();
        let mut seen_keys = HashSet::new();
        if keys.is_empty()
            || keys.len() > MAX_OPERATION_AUTHORITIES
            || keys
                .iter()
                .any(|(id, key)| !seen.insert(id) || !seen_keys.insert(key.to_bytes()))
        {
            return Err(SupportStoreError::Invalid(
                "invalid static Authority count, ID or public key".into(),
            ));
        }
        let dir = dir.as_ref();
        create_dir_all_durable(dir)?;
        let path = dir.join(FILE_NAME);
        let existed = path.exists();
        let used_marker = used_marker_path(dir)?;
        let interrupted_initialization = match fs::read(&used_marker) {
            Ok(bytes) if bytes == USED_MARKER_MAGIC && existed => false,
            Ok(bytes) if bytes == USED_MARKER_MAGIC => {
                return Err(SupportStoreError::Invalid(
                    "previously used operation support log is missing".into(),
                ));
            }
            Ok(_) if existed => true,
            Ok(_) => {
                return Err(SupportStoreError::Invalid(
                    "invalid operation support used marker".into(),
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound && !existed => false,
            Err(error) if error.kind() == io::ErrorKind::NotFound => true,
            Err(error) => return Err(error.into()),
        };
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        file.try_lock()?;
        if interrupted_initialization {
            // The first open can crash after syncing the empty log but
            // before creating its used marker. No support was accepted then.
            // Check under the file lock and require that no other state was
            // saved before completing initialization.
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes)?;
            let entries = fs::read_dir(dir)?
                .map(|entry| entry.map(|entry| entry.path()))
                .collect::<io::Result<Vec<_>>>()?;
            if !MAGIC.starts_with(&bytes) || entries.as_slice() != [path.clone()] {
                return Err(SupportStoreError::Invalid(
                    "operation support log lacks its used marker".into(),
                ));
            }
            file.seek(SeekFrom::Start(0))?;
            if bytes.len() != MAGIC.len() {
                // A crash can also interrupt the initial MAGIC write. A
                // strict prefix has never held an acknowledged support.
                file.set_len(0)?;
                file.write_all(MAGIC)?;
                file.sync_all()?;
                fsync_dir(dir)?;
                file.seek(SeekFrom::Start(0))?;
            }
        }
        let temp_path = dir.join(TEMP_FILE_NAME);
        if temp_path.exists() {
            fs::remove_file(&temp_path)?;
            fsync_dir(dir)?;
        }
        let manifest_temp_path = dir.join(NEIGHBOR_MANIFEST_TEMP_FILE_NAME);
        if manifest_temp_path.exists() {
            fs::remove_file(&manifest_temp_path)?;
            fsync_dir(dir)?;
        }
        let manifest_path = dir.join(NEIGHBOR_MANIFEST_FILE_NAME);
        let neighbor_manifest = if manifest_path.exists() {
            let bytes = fs::read(&manifest_path)?;
            Some(bytes.try_into().map_err(|_| {
                SupportStoreError::Invalid("invalid neighbor manifest length".into())
            })?)
        } else {
            None
        };
        if !existed || interrupted_initialization {
            if !existed {
                file.write_all(MAGIC)?;
                file.sync_all()?;
                File::open(dir)?.sync_all()?;
            }
            if interrupted_initialization && used_marker.exists() {
                // The marker itself may have been created before its bytes
                // were fully written and synced. Its removal is durable
                // before the new complete marker is published.
                fs::remove_file(&used_marker)?;
                fsync_dir(used_marker.parent().expect("marker has parent"))?;
            }
            let mut marker = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&used_marker)?;
            marker.write_all(USED_MARKER_MAGIC)?;
            marker.sync_all()?;
            fsync_dir(used_marker.parent().expect("marker has parent"))?;
        }
        let mut store = Self {
            file,
            path,
            records: BTreeMap::new(),
            registry: registry.clone(),
            scope: scope.clone(),
            epoch_config: epoch_config.clone(),
            keyset_version,
            poisoned: false,
            neighbor_manifest,
        };
        store.recover()?;
        // Recovery may find a complete record after its original fsync failed.
        // Persist it and any published replacement before using it for ACKs.
        store.file.sync_all()?;
        fsync_dir(dir)?;
        if store.neighbor_manifest.is_none() && store.records.values().any(|r| r.compacted) {
            return Err(SupportStoreError::Invalid(
                "compact support log lacks a durable neighbor manifest".into(),
            ));
        }
        Ok(store)
    }

    /// Reject a changed neighbor topology after support bodies have been
    /// discarded. A legacy full-body log can be enrolled before compaction.
    pub fn check_neighbor_manifest(&self, digest: [u8; 32]) -> Result<(), SupportStoreError> {
        if self
            .neighbor_manifest
            .is_some_and(|pinned| pinned != digest)
        {
            return Err(SupportStoreError::Invalid(
                "operation support neighbor manifest changed".into(),
            ));
        }
        Ok(())
    }

    /// Persist the topology that justifies dropping full support packets.
    /// The replacement is synced before rename, and the directory after it.
    pub fn pin_neighbor_manifest(&mut self, digest: [u8; 32]) -> Result<(), SupportStoreError> {
        if self.poisoned {
            return Err(SupportStoreError::Invalid("writer must be reopened".into()));
        }
        self.check_neighbor_manifest(digest)?;
        if self.neighbor_manifest.is_some() {
            return Ok(());
        }
        let dir = self
            .path
            .parent()
            .ok_or_else(|| SupportStoreError::Invalid("support log has no directory".into()))?;
        let temp_path = dir.join(NEIGHBOR_MANIFEST_TEMP_FILE_NAME);
        let path = dir.join(NEIGHBOR_MANIFEST_FILE_NAME);
        self.poisoned = true;
        let mut temp = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)?;
        temp.write_all(&digest)?;
        temp.sync_all()?;
        fs::rename(&temp_path, &path)?;
        fsync_dir(dir)?;
        self.neighbor_manifest = Some(digest);
        self.poisoned = false;
        Ok(())
    }

    fn validate(&self, record: &SupportRecord) -> Result<(), SupportStoreError> {
        if record.support.signed_root.keyset_version != self.keyset_version {
            return Err(OperationMerkleError::InvalidKeyset.into());
        }
        record.support.verify_individual(
            &record.operation,
            &self.registry,
            &self.scope,
            0,
            &self.epoch_config,
        )?;
        Ok(())
    }

    fn validate_compact(&self, record: &CompactSupportRecord) -> Result<(), SupportStoreError> {
        if record.version != 2
            || record.receipt.declaration.mode != crate::operation::CertificationMode::Enabled
            || record.receipt.subject.namespace_id != self.scope.namespace_id
            || !record
                .receipt
                .declaration
                .key
                .starts_with(&self.scope.key_prefix)
        {
            return Err(SupportStoreError::Invalid(
                "invalid compact support receipt".into(),
            ));
        }
        let signed = &record.support.signed_root;
        if signed.keyset_version != self.keyset_version
            || signed.scope != self.scope
            || !self
                .registry
                .is_version_valid(&signed.keyset_version, 0, &self.epoch_config)
        {
            return Err(OperationMerkleError::InvalidKeyset.into());
        }
        if record.support.proof.leaf_count != signed.leaf_count
            || !record.support.proof.verify_commitment(
                &record.receipt.subject,
                &record.receipt.commitment,
                &signed.root,
            )
        {
            return Err(OperationMerkleError::InvalidProof.into());
        }
        let key = self
            .registry
            .get_key_for_authority(&signed.keyset_version, &signed.authority_id)
            .ok_or(OperationMerkleError::UnknownAuthority)?;
        key.verify(
            &signed.message_bytes(),
            &Signature::from_bytes(&signed.signature),
        )
        .map_err(|_| OperationMerkleError::InvalidSignature)?;
        Ok(())
    }

    fn accept_in_memory(
        &mut self,
        record: SupportRecord,
    ) -> Result<RecordOutcome, SupportStoreError> {
        let subject = record.operation.operation.subject.clone();
        let authority = record.support.signed_root.authority_id.clone();
        let receipt = OperationReceipt::from(&record.operation);
        let collected = self
            .records
            .entry((subject, receipt.commitment))
            .or_default();
        if collected
            .receipt
            .as_ref()
            .is_some_and(|old| old != &receipt)
        {
            return Err(SupportStoreError::Conflict);
        }
        if collected
            .by_authority
            .get(&authority)
            .is_some_and(|proofs| proofs.iter().any(|old| old.support == record.support))
        {
            return Ok(RecordOutcome::Retry);
        }
        collected.receipt = Some(receipt);
        collected
            .by_authority
            .entry(authority)
            .or_default()
            .push(StoredSupport {
                operation: Some(record.operation),
                support: record.support,
            });
        Ok(RecordOutcome::New)
    }

    fn accept_compact_in_memory(
        &mut self,
        record: CompactSupportRecord,
    ) -> Result<(), SupportStoreError> {
        let authority = record.support.signed_root.authority_id.clone();
        let collected = self
            .records
            .entry((record.receipt.subject.clone(), record.receipt.commitment))
            .or_default();
        if collected
            .receipt
            .as_ref()
            .is_some_and(|old| old != &record.receipt)
            || collected
                .by_authority
                .get(&authority)
                .is_some_and(|proofs| proofs.iter().any(|old| old.support == record.support))
        {
            return Err(SupportStoreError::Conflict);
        }
        collected.receipt = Some(record.receipt);
        collected.compacted = true;
        collected
            .by_authority
            .entry(authority)
            .or_default()
            .push(StoredSupport {
                operation: None,
                support: record.support,
            });
        Ok(())
    }

    /// Validate and fsync a single support before acknowledging it. A failed
    /// append poisons this handle; reopen to discard a torn final frame.
    pub fn record_and_sync(
        &mut self,
        operation: BoundOperation,
        support: OperationRootSupport,
    ) -> Result<RecordOutcome, SupportStoreError> {
        if self.poisoned {
            return Err(SupportStoreError::Invalid("writer must be reopened".into()));
        }
        let record = SupportRecord { operation, support };
        self.validate(&record)?;
        let receipt = OperationReceipt::from(&record.operation);
        let key = (receipt.subject.clone(), receipt.commitment);
        if !self.records.contains_key(&key) {
            ensure_certificate_capacity(
                &record.operation,
                &self.registry,
                &self.scope,
                &self.keyset_version,
            )
            .map_err(SupportStoreError::Capacity)?;
        }
        if let Some(collected) = self.records.get(&key) {
            if collected.receipt.as_ref() != Some(&receipt) {
                return Err(SupportStoreError::Conflict);
            }
            if collected
                .by_authority
                .get(&record.support.signed_root.authority_id)
                .is_some_and(|proofs| proofs.iter().any(|old| old.support == record.support))
            {
                return Ok(RecordOutcome::Retry);
            }
        }
        let payload =
            serde_json::to_vec(&record).map_err(|e| SupportStoreError::Invalid(e.to_string()))?;
        if payload.is_empty() || payload.len() > MAX_FRAME {
            return Err(SupportStoreError::Invalid("support frame too large".into()));
        }
        let length = (payload.len() as u32).to_be_bytes();
        let mut frame = Vec::with_capacity(payload.len() + 12);
        frame.extend_from_slice(&length);
        frame.extend_from_slice(&crc32fast::hash(&length).to_be_bytes());
        frame.extend_from_slice(&crc32fast::hash(&payload).to_be_bytes());
        frame.extend_from_slice(&payload);
        self.poisoned = true;
        self.file.write_all(&frame)?;
        self.file.sync_all()?;
        let outcome = self.accept_in_memory(record)?;
        self.poisoned = false;
        Ok(outcome)
    }

    fn recover(&mut self) -> Result<(), SupportStoreError> {
        let mut bytes = Vec::new();
        self.file.seek(SeekFrom::Start(0))?;
        self.file.read_to_end(&mut bytes)?;
        if !bytes.starts_with(MAGIC) {
            return Err(SupportStoreError::Invalid(
                "invalid support log magic".into(),
            ));
        }
        let mut cursor = MAGIC.len();
        while cursor < bytes.len() {
            let rest = &bytes[cursor..];
            if rest.len() < 12 {
                break;
            }
            let length = u32::from_be_bytes(rest[..4].try_into().expect("length header")) as usize;
            let header_crc = u32::from_be_bytes(rest[4..8].try_into().expect("header checksum"));
            if crc32fast::hash(&rest[..4]) != header_crc || length == 0 || length > MAX_FRAME {
                return Err(SupportStoreError::Invalid(
                    "invalid support frame header".into(),
                ));
            }
            if rest.len() < length + 12 {
                break;
            }
            let body_crc = u32::from_be_bytes(rest[8..12].try_into().expect("body checksum"));
            let payload = &rest[12..12 + length];
            if crc32fast::hash(payload) != body_crc {
                return Err(SupportStoreError::Invalid(
                    "support frame checksum mismatch".into(),
                ));
            }
            let value: serde_json::Value = serde_json::from_slice(payload)
                .map_err(|e| SupportStoreError::Invalid(e.to_string()))?;
            if value.get("version").is_some() {
                let record: CompactSupportRecord = serde_json::from_value(value)
                    .map_err(|e| SupportStoreError::Invalid(e.to_string()))?;
                self.validate_compact(&record)?;
                self.accept_compact_in_memory(record)?;
            } else {
                let record: SupportRecord = serde_json::from_value(value)
                    .map_err(|e| SupportStoreError::Invalid(e.to_string()))?;
                self.validate(&record)?;
                self.accept_in_memory(record)?;
            }
            cursor += 12 + length;
        }
        if cursor < bytes.len() {
            self.file.set_len(cursor as u64)?;
            self.file.sync_all()?;
        }
        self.file.seek(SeekFrom::End(0))?;
        Ok(())
    }

    /// Return a full certificate only when the stored distinct Authorities
    /// satisfy the verifier's fault bound. The assembled certificate is
    /// verified again before return.
    pub fn assemble_certificate(
        &self,
        subject: &CrdtSubject,
        faults: usize,
    ) -> Result<Option<OperationMerkleCertificate>, SupportStoreError> {
        let mut completed = None;
        for ((known_subject, _), collected) in &self.records {
            if known_subject != subject || collected.compacted {
                continue;
            }
            let operation = collected
                .by_authority
                .values()
                .next()
                .and_then(|proofs| proofs.first())
                .and_then(|stored| stored.operation.clone())
                .ok_or_else(|| SupportStoreError::Invalid("missing operation body".into()))?;
            let certificate = OperationMerkleCertificate {
                operation,
                supports: collected
                    .by_authority
                    .values()
                    .filter_map(|proofs| proofs.first())
                    .map(|stored| stored.support.clone())
                    .collect(),
            };
            match certificate.verify(
                &self.registry,
                &self.scope,
                self.keyset_version.clone(),
                0,
                &self.epoch_config,
                faults,
            ) {
                Ok(_) if completed.is_some() => return Err(SupportStoreError::Conflict),
                Ok(_) => completed = Some(certificate),
                Err(OperationMerkleError::InsufficientSignatures { .. }) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(completed)
    }

    pub fn support_count(&self, subject: &CrdtSubject) -> usize {
        self.records
            .iter()
            .filter(|((known_subject, _), _)| known_subject == subject)
            .map(|(_, collected)| collected.by_authority.len())
            .sum()
    }

    /// Compact exact subjects only after the caller has discharged all full
    /// packet obligations and durably retained certificate evidence. A
    /// compact subject can still accept and relay a new Authority support.
    pub fn compact_receipted_and_sync(
        &mut self,
        receipts: &[OperationReceipt],
    ) -> Result<usize, SupportStoreError> {
        if self.poisoned {
            return Err(SupportStoreError::Invalid("writer must be reopened".into()));
        }
        if self.neighbor_manifest.is_none() {
            return Err(SupportStoreError::Invalid(
                "support compaction requires a durable neighbor manifest".into(),
            ));
        }
        let requested: BTreeMap<_, _> = receipts
            .iter()
            .map(|receipt| (&receipt.subject, receipt))
            .collect();
        let mut next = self.records.clone();
        for (subject, receipt) in &requested {
            if next.keys().any(|(known, _)| known == *subject)
                && !next
                    .values()
                    .any(|collected| collected.receipt.as_ref() == Some(receipt))
            {
                return Err(SupportStoreError::Invalid(
                    "support compaction receipt mismatch".into(),
                ));
            }
        }
        let mut compacted = 0;
        for ((subject, _), collected) in &mut next {
            let Some(receipt) = requested.get(subject) else {
                continue;
            };
            if collected.receipt.as_ref() != Some(receipt) {
                continue;
            }
            for proofs in collected.by_authority.values_mut() {
                for stored in proofs {
                    if stored.operation.take().is_some() {
                        compacted += 1;
                    }
                }
            }
            collected.compacted = true;
        }
        if compacted == 0 {
            return Ok(0);
        }
        let dir = self
            .path
            .parent()
            .ok_or_else(|| SupportStoreError::Invalid("support log has no directory".into()))?;
        let temp_path = dir.join(TEMP_FILE_NAME);
        self.poisoned = true;
        let mut replacement = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&temp_path)?;
        replacement.try_lock()?;
        replacement.write_all(MAGIC)?;
        for collected in next.values() {
            let receipt = collected
                .receipt
                .as_ref()
                .expect("stored support has receipt");
            for stored in collected.by_authority.values().flatten() {
                let payload = if let Some(operation) = &stored.operation {
                    serde_json::to_vec(&SupportRecord {
                        operation: operation.clone(),
                        support: stored.support.clone(),
                    })
                } else {
                    serde_json::to_vec(&CompactSupportRecord {
                        version: 2,
                        receipt: receipt.clone(),
                        support: stored.support.clone(),
                    })
                }
                .map_err(|e| SupportStoreError::Invalid(e.to_string()))?;
                replacement.write_all(&encode_frame(&payload)?)?;
            }
        }
        replacement.sync_all()?;
        fs::rename(&temp_path, &self.path)?;
        fsync_dir(dir)?;
        self.file = replacement;
        self.records = next;
        self.poisoned = false;
        Ok(compacted)
    }

    /// Subjects with at least one durable support, in stable order. Recovery
    /// callers can revisit each subject to promote completed quorums.
    pub fn subjects(&self) -> impl Iterator<Item = &CrdtSubject> {
        self.records
            .keys()
            .map(|(subject, _)| subject)
            .collect::<BTreeSet<_>>()
            .into_iter()
    }

    /// Receipts used to retire support bodies. A caller must compare these
    /// against its authenticated operation ledger before using this store.
    pub fn compact_receipts(&self) -> impl Iterator<Item = &OperationReceipt> {
        self.records
            .values()
            .filter(|collected| collected.compacted)
            .filter_map(|collected| collected.receipt.as_ref())
    }

    /// Every durable support with its exact operation body. The caller may
    /// rebuild an outbox or relay queue from this iterator after restart.
    pub fn packets(&self) -> impl Iterator<Item = (BoundOperation, OperationRootSupport)> + '_ {
        self.records.values().flat_map(|collected| {
            collected
                .by_authority
                .values()
                .flatten()
                .filter_map(|stored| {
                    stored
                        .operation
                        .as_ref()
                        .map(|operation| (operation.clone(), stored.support.clone()))
                })
        })
    }
}

fn encode_frame(payload: &[u8]) -> Result<Vec<u8>, SupportStoreError> {
    if payload.is_empty() || payload.len() > MAX_FRAME {
        return Err(SupportStoreError::Invalid("support frame too large".into()));
    }
    let length = (payload.len() as u32).to_be_bytes();
    let mut frame = Vec::with_capacity(payload.len() + 12);
    frame.extend_from_slice(&length);
    frame.extend_from_slice(&crc32fast::hash(&length).to_be_bytes());
    frame.extend_from_slice(&crc32fast::hash(payload).to_be_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use tempfile::tempdir;

    #[test]
    fn excessive_authority_keyset_is_rejected_before_opening_support_log() {
        let (_, _, _, scope) = fixture();
        let mut registry = KeysetRegistry::new();
        let keys = (0..=MAX_OPERATION_AUTHORITIES)
            .map(|index| {
                let mut seed = [0; 32];
                seed[..8].copy_from_slice(&(index as u64).to_be_bytes());
                (
                    NodeId(format!("auth-{index}")),
                    SigningKey::from_bytes(&seed).verifying_key(),
                )
            })
            .collect();
        registry.register_keyset(KeysetVersion(1), 0, keys).unwrap();
        let dir = tempdir().unwrap();
        let support_dir = dir.path().join("support-store");
        assert!(matches!(
            OperationSupportStore::open(&support_dir, &registry, &scope, &EpochConfig::default()),
            Err(SupportStoreError::Invalid(_))
        ));
        assert!(!support_dir.exists());
    }

    #[test]
    fn rejects_recovered_store_with_shared_authority_public_key() {
        let (_, _, _, scope) = fixture();
        let dir = tempdir().unwrap();
        let (_, _, unique_registry, _) = fixture();
        drop(
            OperationSupportStore::open(
                dir.path(),
                &unique_registry,
                &scope,
                &EpochConfig::default(),
            )
            .unwrap(),
        );
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
        assert!(
            OperationSupportStore::open(
                dir.path(),
                &unique_registry,
                &scope,
                &EpochConfig::default()
            )
            .is_ok()
        );
    }

    use crate::authority::operation_admission::{
        OperationAdmissionPolicy, OperationLockStore, admit_for_signing,
    };
    use crate::authority::operation_merkle::{OperationMerkleTree, SignedOperationRoot};
    use crate::hlc::HlcTimestamp;
    use crate::operation::{
        CandidateKvAction, CertificationMode, Declaration, EventDot, OperationHistory,
        OperationLeaf,
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
        fn record_and_sync(&mut self, bound: BoundOperation) -> Result<RecordOutcome, Self::Error> {
            self.0.record(bound)
        }
    }
    fn operation() -> BoundOperation {
        let subject = CrdtSubject {
            namespace_id: "ns".into(),
            event_dot: EventDot {
                writer: NodeId("writer".into()),
                counter: 1,
            },
        };
        BoundOperation {
            operation: OperationLeaf {
                subject: subject.clone(),
                action: CandidateKvAction::Create {
                    key: "key-1".into(),
                    value_bytes: b"value".to_vec(),
                    timestamp: HlcTimestamp {
                        physical: 10,
                        logical: 0,
                        node_id: "writer".into(),
                    },
                },
            },
            declaration: Declaration {
                creation_id: subject,
                key: "key-1".into(),
                mode: CertificationMode::Enabled,
            },
        }
    }
    fn fixture() -> (
        BoundOperation,
        Vec<OperationRootSupport>,
        KeysetRegistry,
        OperationScope,
    ) {
        let bound = operation();
        let scope = OperationScope {
            namespace_id: "ns".into(),
            key_prefix: "key-".into(),
        };
        let admitted = admit_for_signing(&Allow, &mut Lock::default(), bound.clone()).unwrap();
        let tree = OperationMerkleTree::from_signable(scope.clone(), &[admitted]).unwrap();
        let keys: Vec<_> = (1..=4).map(|i| SigningKey::from_bytes(&[i; 32])).collect();
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
                proof: tree.proof(&bound.operation.subject).unwrap(),
            })
            .collect();
        (bound, supports, registry, scope)
    }

    #[test]
    fn oversized_support_is_rejected_before_durable_record() {
        let (_, _, registry, scope) = fixture();
        let mut bound = operation();
        if let CandidateKvAction::Create { value_bytes, .. } = &mut bound.operation.action {
            *value_bytes = vec![0; 5 * 1024 * 1024];
        }
        let admitted = admit_for_signing(&Allow, &mut Lock::default(), bound.clone()).unwrap();
        let tree = OperationMerkleTree::from_signable(scope.clone(), &[admitted]).unwrap();
        let support = OperationRootSupport {
            signed_root: SignedOperationRoot::sign(
                &tree,
                KeysetVersion(1),
                NodeId("a0".into()),
                &SigningKey::from_bytes(&[1; 32]),
            ),
            proof: tree.proof(&bound.operation.subject).unwrap(),
        };
        let dir = tempdir().unwrap();
        let mut store = OperationSupportStore::open(
            dir.path().join("supports"),
            &registry,
            &scope,
            &EpochConfig::default(),
        )
        .unwrap();
        assert!(matches!(
            store.record_and_sync(bound.clone(), support.clone()),
            Err(SupportStoreError::Capacity(_))
        ));
        assert_eq!(store.support_count(&bound.operation.subject), 0);
        drop(store);
        let reopened = OperationSupportStore::open(
            dir.path().join("supports"),
            &registry,
            &scope,
            &EpochConfig::default(),
        )
        .unwrap();
        assert_eq!(reopened.support_count(&bound.operation.subject), 0);
    }

    #[test]
    fn supports_survive_restart_and_assemble_only_at_quorum() {
        let temp = tempdir().unwrap();
        let (bound, supports, registry, scope) = fixture();
        let subject = bound.operation.subject.clone();
        let config = EpochConfig::default();
        {
            let mut store = OperationSupportStore::open(
                temp.path().join("supports"),
                &registry,
                &scope,
                &config,
            )
            .unwrap();
            assert_eq!(
                store
                    .record_and_sync(bound.clone(), supports[0].clone())
                    .unwrap(),
                RecordOutcome::New
            );
            assert_eq!(
                store
                    .record_and_sync(bound.clone(), supports[0].clone())
                    .unwrap(),
                RecordOutcome::Retry
            );
            assert!(store.assemble_certificate(&subject, 1).unwrap().is_none());
            store
                .record_and_sync(bound.clone(), supports[1].clone())
                .unwrap();
        }
        let mut reopened =
            OperationSupportStore::open(temp.path().join("supports"), &registry, &scope, &config)
                .unwrap();
        assert_eq!(reopened.support_count(&subject), 2);
        assert_eq!(
            reopened.subjects().cloned().collect::<Vec<_>>(),
            vec![subject.clone()]
        );
        assert_eq!(reopened.packets().count(), 2);
        assert!(
            reopened
                .assemble_certificate(&subject, 1)
                .unwrap()
                .is_none()
        );
        reopened
            .record_and_sync(bound.clone(), supports[2].clone())
            .unwrap();
        let certificate = reopened.assemble_certificate(&subject, 1).unwrap().unwrap();
        assert_eq!(certificate.operation, bound);
        assert_eq!(certificate.supports.len(), 3);
    }

    #[test]
    fn interrupted_initial_open_recovers_only_an_empty_support_log() {
        let temp = tempdir().unwrap();
        let (_, _, registry, scope) = fixture();
        let config = EpochConfig::default();
        for (index, (log_bytes, incomplete_marker)) in [
            (&MAGIC[..], None),
            (&MAGIC[..], Some(&b""[..])),
            (&MAGIC[..], Some(&b"ASTOS"[..])),
            (&b"ASTO"[..], None),
            (&b""[..], None),
        ]
        .into_iter()
        .enumerate()
        {
            let dir = temp.path().join(format!("supports-{index}"));
            fs::create_dir(&dir).unwrap();
            let log_path = dir.join(FILE_NAME);
            let mut log = File::create(&log_path).unwrap();
            log.write_all(log_bytes).unwrap();
            log.sync_all().unwrap();
            fsync_dir(&dir).unwrap();
            if let Some(bytes) = incomplete_marker {
                let marker = used_marker_path(&dir).unwrap();
                fs::write(&marker, bytes).unwrap();
                fsync_dir(marker.parent().unwrap()).unwrap();
            }

            let store = OperationSupportStore::open(&dir, &registry, &scope, &config).unwrap();
            assert_eq!(fs::read(&log_path).unwrap(), MAGIC);
            assert_eq!(
                fs::read(used_marker_path(&dir).unwrap()).unwrap(),
                USED_MARKER_MAGIC
            );
            assert_eq!(store.subjects().count(), 0);
            drop(store);
            OperationSupportStore::open(&dir, &registry, &scope, &config).unwrap();
        }

        let second = temp.path().join("with-other-state");
        fs::create_dir(&second).unwrap();
        fs::write(second.join(FILE_NAME), MAGIC).unwrap();
        fs::write(second.join(NEIGHBOR_MANIFEST_FILE_NAME), [0; 32]).unwrap();
        assert!(OperationSupportStore::open(&second, &registry, &scope, &config).is_err());
    }

    #[test]
    fn missing_used_support_log_is_not_recreated() {
        let temp = tempdir().unwrap();
        let (bound, supports, registry, scope) = fixture();
        let config = EpochConfig::default();
        let dir = temp.path().join("supports");
        {
            let mut store = OperationSupportStore::open(&dir, &registry, &scope, &config).unwrap();
            store
                .record_and_sync(bound.clone(), supports[0].clone())
                .unwrap();
        }
        fs::remove_dir_all(&dir).unwrap();
        assert!(matches!(
            OperationSupportStore::open(&dir, &registry, &scope, &config),
            Err(SupportStoreError::Invalid(_))
        ));
    }

    #[test]
    fn missing_used_marker_after_support_is_not_reinitialized() {
        let temp = tempdir().unwrap();
        let (bound, supports, registry, scope) = fixture();
        let config = EpochConfig::default();
        let dir = temp.path().join("supports");
        {
            let mut store = OperationSupportStore::open(&dir, &registry, &scope, &config).unwrap();
            store.record_and_sync(bound, supports[0].clone()).unwrap();
        }
        fs::remove_file(used_marker_path(&dir).unwrap()).unwrap();
        assert!(matches!(
            OperationSupportStore::open(&dir, &registry, &scope, &config),
            Err(SupportStoreError::Invalid(_))
        ));
    }

    #[test]
    fn same_authority_distinct_roots_keep_one_vote_and_both_packets() {
        let temp = tempdir().unwrap();
        let (bound, supports, registry, scope) = fixture();
        let mut store = OperationSupportStore::open(
            temp.path().join("supports"),
            &registry,
            &scope,
            &EpochConfig::default(),
        )
        .unwrap();
        store
            .record_and_sync(bound.clone(), supports[0].clone())
            .unwrap();
        let mut changed = bound.clone();
        if let CandidateKvAction::Create { value_bytes, .. } = &mut changed.operation.action {
            *value_bytes = b"different".to_vec();
        }
        assert!(store.record_and_sync(changed, supports[0].clone()).is_err());
        let mut forged = supports[0].clone();
        forged.signed_root.signature[0] ^= 1;
        assert!(store.record_and_sync(bound.clone(), forged).is_err());

        // A faulty Authority may put the same operation in two valid roots.
        // Both packets need receipts, but they still contribute one vote.
        let mut other = bound.clone();
        other.operation.subject.event_dot.counter = 2;
        other.declaration.creation_id = other.operation.subject.clone();
        if let CandidateKvAction::Create { key, .. } = &mut other.operation.action {
            *key = "key-2".into();
        }
        other.declaration.key = "key-2".into();
        let mut lock = Lock::default();
        let target = admit_for_signing(&Allow, &mut lock, bound.clone()).unwrap();
        let extra = admit_for_signing(&Allow, &mut lock, other).unwrap();
        let tree = OperationMerkleTree::from_signable(scope.clone(), &[target, extra]).unwrap();
        let alternate = OperationRootSupport {
            signed_root: SignedOperationRoot::sign(
                &tree,
                KeysetVersion(1),
                NodeId("a0".into()),
                &SigningKey::from_bytes(&[1; 32]),
            ),
            proof: tree.proof(&bound.operation.subject).unwrap(),
        };
        assert_eq!(
            store
                .record_and_sync(bound.clone(), alternate.clone())
                .unwrap(),
            RecordOutcome::New
        );
        assert_eq!(
            store.record_and_sync(bound.clone(), alternate).unwrap(),
            RecordOutcome::Retry
        );
        assert_eq!(store.support_count(&bound.operation.subject), 1);
        assert_eq!(store.packets().count(), 2);
        drop(store);
        let mut store = OperationSupportStore::open(
            temp.path().join("supports"),
            &registry,
            &scope,
            &EpochConfig::default(),
        )
        .unwrap();
        assert_eq!(store.support_count(&bound.operation.subject), 1);
        assert_eq!(store.packets().count(), 2);
        for support in &supports[1..] {
            store
                .record_and_sync(bound.clone(), support.clone())
                .unwrap();
        }
        assert_eq!(store.support_count(&bound.operation.subject), 3);
        assert_eq!(store.packets().count(), 4);
        assert_eq!(
            store
                .assemble_certificate(&bound.operation.subject, 1)
                .unwrap()
                .unwrap()
                .supports
                .len(),
            3
        );
        store.pin_neighbor_manifest([7; 32]).unwrap();
        assert_eq!(
            store
                .compact_receipted_and_sync(&[OperationReceipt::from(&bound)])
                .unwrap(),
            4
        );
        drop(store);
        let store = OperationSupportStore::open(
            temp.path().join("supports"),
            &registry,
            &scope,
            &EpochConfig::default(),
        )
        .unwrap();
        assert_eq!(store.support_count(&bound.operation.subject), 3);
        assert_eq!(store.compact_receipts().count(), 1);
        assert_eq!(store.packets().count(), 0);
    }

    #[test]
    fn minority_fork_does_not_lock_out_honest_quorum_after_restart() {
        let temp = tempdir().unwrap();
        let (bound, supports, registry, scope) = fixture();
        let config = EpochConfig::default();
        let mut fork = bound.clone();
        if let CandidateKvAction::Create { value_bytes, .. } = &mut fork.operation.action {
            *value_bytes = b"faulty authority fork".to_vec();
        }
        let mut lock = Lock::default();
        let signable = admit_for_signing(&Allow, &mut lock, fork.clone()).unwrap();
        let tree = OperationMerkleTree::from_signable(scope.clone(), &[signable]).unwrap();
        let minority = OperationRootSupport {
            signed_root: SignedOperationRoot::sign(
                &tree,
                KeysetVersion(1),
                NodeId("a0".into()),
                &SigningKey::from_bytes(&[1; 32]),
            ),
            proof: tree.proof(&fork.operation.subject).unwrap(),
        };
        let honest_signable =
            admit_for_signing(&Allow, &mut Lock::default(), bound.clone()).unwrap();
        let honest_tree =
            OperationMerkleTree::from_signable(scope.clone(), &[honest_signable]).unwrap();
        let honest_a3 = OperationRootSupport {
            signed_root: SignedOperationRoot::sign(
                &honest_tree,
                KeysetVersion(1),
                NodeId("a3".into()),
                &SigningKey::from_bytes(&[4; 32]),
            ),
            proof: honest_tree.proof(&bound.operation.subject).unwrap(),
        };
        {
            let mut store = OperationSupportStore::open(
                temp.path().join("supports"),
                &registry,
                &scope,
                &config,
            )
            .unwrap();
            store.record_and_sync(fork.clone(), minority).unwrap();
            assert!(
                store
                    .assemble_certificate(&bound.operation.subject, 1)
                    .unwrap()
                    .is_none()
            );
        }
        let mut store =
            OperationSupportStore::open(temp.path().join("supports"), &registry, &scope, &config)
                .unwrap();
        store.file.write_all(&[0, 1]).unwrap();
        store.poisoned = true;
        store
            .reopen_if_poisoned(&registry, &scope, &config, [7; 32], &[])
            .unwrap();
        for support in supports.iter().skip(1).chain(std::iter::once(&honest_a3)) {
            store
                .record_and_sync(bound.clone(), support.clone())
                .unwrap();
        }
        assert_eq!(store.subjects().count(), 1);
        assert_eq!(store.support_count(&bound.operation.subject), 4);
        assert_eq!(store.packets().count(), 4);
        assert_eq!(
            store
                .assemble_certificate(&bound.operation.subject, 1)
                .unwrap()
                .unwrap()
                .operation,
            bound
        );
        let receipt = OperationReceipt::from(&bound);
        store.pin_neighbor_manifest([7; 32]).unwrap();
        assert_eq!(store.compact_receipted_and_sync(&[receipt]).unwrap(), 3);
        assert_eq!(store.packets().count(), 1);
        drop(store);
        let store =
            OperationSupportStore::open(temp.path().join("supports"), &registry, &scope, &config)
                .unwrap();
        assert_eq!(store.subjects().count(), 1);
        assert_eq!(store.packets().count(), 1);
        assert_eq!(store.support_count(&bound.operation.subject), 4);
    }

    #[test]
    fn torn_tail_is_trimmed_but_complete_corruption_fails_closed() {
        let temp = tempdir().unwrap();
        let (bound, supports, registry, scope) = fixture();
        let config = EpochConfig::default();
        {
            let mut store = OperationSupportStore::open(
                temp.path().join("supports"),
                &registry,
                &scope,
                &config,
            )
            .unwrap();
            store.record_and_sync(bound, supports[0].clone()).unwrap();
        }
        let path = temp.path().join("supports").join(FILE_NAME);
        let complete = fs::metadata(&path).unwrap().len();
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&[0, 0, 0])
            .unwrap();
        let store =
            OperationSupportStore::open(temp.path().join("supports"), &registry, &scope, &config)
                .unwrap();
        assert_eq!(fs::metadata(&path).unwrap().len(), complete);
        drop(store);
        let mut bytes = fs::read(&path).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        fs::write(&path, bytes).unwrap();
        assert!(matches!(
            OperationSupportStore::open(temp.path().join("supports"), &registry, &scope, &config),
            Err(SupportStoreError::Invalid(_))
        ));
    }

    #[test]
    fn compact_receipted_supports_survive_restart_and_accept_extra_support() {
        let temp = tempdir().unwrap();
        let (bound, supports, registry, scope) = fixture();
        let subject = bound.operation.subject.clone();
        let receipt = OperationReceipt::from(&bound);
        let config = EpochConfig::default();
        {
            let mut store = OperationSupportStore::open(
                temp.path().join("supports"),
                &registry,
                &scope,
                &config,
            )
            .unwrap();
            for support in &supports[..2] {
                store
                    .record_and_sync(bound.clone(), support.clone())
                    .unwrap();
            }
            assert!(store.check_neighbor_manifest([7; 32]).is_ok());
            assert!(
                store
                    .compact_receipted_and_sync(std::slice::from_ref(&receipt))
                    .is_err()
            );
            store.pin_neighbor_manifest([7; 32]).unwrap();
            let mut wrong = receipt.clone();
            wrong.commitment[0] ^= 1;
            assert!(store.compact_receipted_and_sync(&[wrong]).is_err());
            assert_eq!(
                store
                    .compact_receipted_and_sync(std::slice::from_ref(&receipt))
                    .unwrap(),
                2
            );
            assert_eq!(
                store
                    .compact_receipted_and_sync(std::slice::from_ref(&receipt))
                    .unwrap(),
                0
            );
            assert_eq!(store.packets().count(), 0);
            assert!(store.assemble_certificate(&subject, 1).unwrap().is_none());
        }
        let bytes = fs::read(temp.path().join("supports").join(FILE_NAME)).unwrap();
        assert!(
            !bytes
                .windows(b"value".len())
                .any(|window| window == b"value")
        );
        fs::write(
            temp.path().join("supports").join(TEMP_FILE_NAME),
            b"uncommitted rewrite",
        )
        .unwrap();
        let mut store =
            OperationSupportStore::open(temp.path().join("supports"), &registry, &scope, &config)
                .unwrap();
        store.check_neighbor_manifest([7; 32]).unwrap();
        assert!(store.check_neighbor_manifest([8; 32]).is_err());
        assert!(!temp.path().join("supports").join(TEMP_FILE_NAME).exists());
        assert_eq!(store.support_count(&subject), 2);
        assert_eq!(
            store
                .record_and_sync(bound.clone(), supports[0].clone())
                .unwrap(),
            RecordOutcome::Retry
        );
        let mut changed = bound.clone();
        if let CandidateKvAction::Create { value_bytes, .. } = &mut changed.operation.action {
            *value_bytes = b"changed".to_vec();
        }
        assert!(matches!(
            store.record_and_sync(changed, supports[0].clone()),
            Err(SupportStoreError::Verification(_)) | Err(SupportStoreError::Conflict)
        ));
        store
            .record_and_sync(bound.clone(), supports[2].clone())
            .unwrap();
        assert_eq!(store.packets().count(), 1);
        assert!(store.assemble_certificate(&subject, 1).unwrap().is_none());
        assert_eq!(store.compact_receipted_and_sync(&[receipt]).unwrap(), 1);
        assert_eq!(store.packets().count(), 0);
    }

    #[test]
    fn manifest_pin_is_atomic_and_required_for_compact_recovery() {
        let temp = tempdir().unwrap();
        let (bound, supports, registry, scope) = fixture();
        let config = EpochConfig::default();
        let receipt = OperationReceipt::from(&bound);
        {
            let mut store = OperationSupportStore::open(
                temp.path().join("supports"),
                &registry,
                &scope,
                &config,
            )
            .unwrap();
            store.record_and_sync(bound, supports[0].clone()).unwrap();
            store.pin_neighbor_manifest([3; 32]).unwrap();
            store.pin_neighbor_manifest([3; 32]).unwrap();
            assert!(store.pin_neighbor_manifest([4; 32]).is_err());
            store.compact_receipted_and_sync(&[receipt]).unwrap();
        }
        let path = temp
            .path()
            .join("supports")
            .join(NEIGHBOR_MANIFEST_FILE_NAME);
        assert_eq!(fs::read(&path).unwrap(), [3; 32]);
        let reopened =
            OperationSupportStore::open(temp.path().join("supports"), &registry, &scope, &config)
                .unwrap();
        reopened.check_neighbor_manifest([3; 32]).unwrap();
        assert!(reopened.check_neighbor_manifest([4; 32]).is_err());
        drop(reopened);
        fs::remove_file(&path).unwrap();
        assert!(matches!(
            OperationSupportStore::open(temp.path().join("supports"), &registry, &scope, &config),
            Err(SupportStoreError::Invalid(_))
        ));
        fs::write(&path, [3; 31]).unwrap();
        assert!(matches!(
            OperationSupportStore::open(temp.path().join("supports"), &registry, &scope, &config),
            Err(SupportStoreError::Invalid(_))
        ));
    }

    #[test]
    fn uncertain_manifest_write_reopens_and_rechecks_compact_receipt() {
        let temp = tempdir().unwrap();
        let (bound, supports, registry, scope) = fixture();
        let config = EpochConfig::default();
        let receipt = OperationReceipt::from(&bound);
        let mut store =
            OperationSupportStore::open(temp.path().join("supports"), &registry, &scope, &config)
                .unwrap();
        store.record_and_sync(bound, supports[0].clone()).unwrap();
        fs::write(
            temp.path()
                .join("supports")
                .join(NEIGHBOR_MANIFEST_TEMP_FILE_NAME),
            b"interrupted",
        )
        .unwrap();
        assert!(store.pin_neighbor_manifest([3; 32]).is_err());
        assert!(store.is_poisoned());
        store
            .reopen_if_poisoned(
                &registry,
                &scope,
                &config,
                [3; 32],
                std::slice::from_ref(&receipt),
            )
            .unwrap();
        assert!(!store.is_poisoned());
        assert_eq!(store.support_count(&receipt.subject), 1);
        store.pin_neighbor_manifest([3; 32]).unwrap();
        store
            .compact_receipted_and_sync(std::slice::from_ref(&receipt))
            .unwrap();
        store.poisoned = true;
        assert!(
            store
                .reopen_if_poisoned(&registry, &scope, &config, [3; 32], &[])
                .is_err()
        );
        assert!(store.is_poisoned());
        store
            .reopen_if_poisoned(&registry, &scope, &config, [3; 32], &[receipt])
            .unwrap();
        assert!(!store.is_poisoned());
    }

    #[test]
    fn interrupted_manifest_write_does_not_enroll_legacy_log() {
        let temp = tempdir().unwrap();
        let (_, _, registry, scope) = fixture();
        let config = EpochConfig::default();
        OperationSupportStore::open(temp.path().join("supports"), &registry, &scope, &config)
            .unwrap();
        fs::write(
            temp.path()
                .join("supports")
                .join(NEIGHBOR_MANIFEST_TEMP_FILE_NAME),
            [9; 13],
        )
        .unwrap();
        let mut store =
            OperationSupportStore::open(temp.path().join("supports"), &registry, &scope, &config)
                .unwrap();
        assert!(
            !temp
                .path()
                .join("supports")
                .join(NEIGHBOR_MANIFEST_TEMP_FILE_NAME)
                .exists()
        );
        store.check_neighbor_manifest([5; 32]).unwrap();
        store.pin_neighbor_manifest([5; 32]).unwrap();
        assert_eq!(
            fs::read(
                temp.path()
                    .join("supports")
                    .join(NEIGHBOR_MANIFEST_FILE_NAME)
            )
            .unwrap(),
            [5; 32]
        );
    }
}
