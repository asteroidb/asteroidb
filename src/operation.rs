//! Candidate operation identity and immutable content commitment.
//!
//! Mirrors `CrdtSubject`, `OperationLeaf`, and `EdgeCreation.BoundOperation`
//! in aste-lean. This module defines canonical bytes and durable same-ID
//! discipline. Operation certificates use separate Authority locks, signed
//! Merkle roots, inclusion proofs, and an operation-level quorum verifier;
//! legacy frontier certificates do not sign these bytes.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::hlc::HlcTimestamp;
use crate::store::backend::{create_dir_all_durable, fsync_dir};
use crate::store::wal::{
    self, SyncPolicy, WalConfig, WalPos, WalReadOutcome, WalRecord, WalWriter,
};
use crate::types::NodeId;

/// The writer/counter event dot in Lean's `CrdtSubject`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EventDot {
    pub writer: NodeId,
    pub counter: u64,
}

/// An operation identity. The namespace scopes the writer/counter pair.
/// The key is deliberately absent: it identifies a value, not an operation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CrdtSubject {
    pub namespace_id: String,
    pub event_dot: EventDot,
}

/// Lean's resolved, immutable creation mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CertificationMode {
    Enabled,
    Disabled,
}

/// Exact action fields committed by a leaf. `value_bytes` must be the
/// canonical encoding of the value for the operation protocol; these bytes
/// are kept intact and never derived from a merged CRDT post-state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CandidateKvAction {
    Create {
        key: String,
        value_bytes: Vec<u8>,
        timestamp: HlcTimestamp,
    },
    Add {
        key: String,
        value_bytes: Vec<u8>,
        timestamp: HlcTimestamp,
    },
    Delete {
        key: String,
    },
}

impl CandidateKvAction {
    pub fn key(&self) -> &str {
        match self {
            Self::Create { key, .. } | Self::Add { key, .. } | Self::Delete { key } => key,
        }
    }
}

/// Lean's `OperationLeaf`; the leaf's domain tag is encoded by
/// [`BoundOperation::canonical_bytes`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationLeaf {
    pub subject: CrdtSubject,
    pub action: CandidateKvAction,
}

/// Small policy declaration retained after the initial value is removed.
/// It is a reference to creation, not a second key identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Declaration {
    pub creation_id: CrdtSubject,
    pub key: String,
    pub mode: CertificationMode,
}

/// Full content committed by an operation Merkle leaf. The payload alone
/// implies neither writer authentication nor Authority certification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundOperation {
    pub operation: OperationLeaf,
    pub declaration: Declaration,
}

/// Durable identity retained when an operation body is no longer needed for
/// local reads. The declaration and action kind keep the key's creation
/// policy and the operation's role available without retaining value bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationReceipt {
    pub subject: CrdtSubject,
    pub commitment: [u8; 32],
    pub declaration: Declaration,
    pub action_kind: ReceiptActionKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReceiptActionKind {
    Create,
    Add,
    Delete,
}

impl From<&BoundOperation> for OperationReceipt {
    fn from(bound: &BoundOperation) -> Self {
        Self {
            subject: bound.operation.subject.clone(),
            commitment: bound.commitment(),
            declaration: bound.declaration.clone(),
            action_kind: match bound.operation.action {
                CandidateKvAction::Create { .. } => ReceiptActionKind::Create,
                CandidateKvAction::Add { .. } => ReceiptActionKind::Add,
                CandidateKvAction::Delete { .. } => ReceiptActionKind::Delete,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperationError {
    DifferentNamespace,
    DifferentKey,
    CreateIdMismatch,
    ConflictingContent(CrdtSubject),
    UnknownSubject(CrdtSubject),
    InvalidReceipt(CrdtSubject),
    InvalidCheckpoint,
}

impl BoundOperation {
    /// Check structural references. Authentication of the creation and the
    /// no-competing-creates rule are separate admission requirements.
    pub fn validate_structure(&self) -> Result<(), OperationError> {
        if self.operation.subject.namespace_id != self.declaration.creation_id.namespace_id {
            return Err(OperationError::DifferentNamespace);
        }
        if self.operation.action.key() != self.declaration.key {
            return Err(OperationError::DifferentKey);
        }
        if matches!(self.operation.action, CandidateKvAction::Create { .. })
            && self.operation.subject != self.declaration.creation_id
        {
            return Err(OperationError::CreateIdMismatch);
        }
        Ok(())
    }

    /// Versioned, unambiguous bytes for the entire bound payload. Integers
    /// are big endian; variable fields have u64 lengths. Changing this
    /// encoding after signatures depend on it requires a protocol version.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        const DOMAIN: &[u8] = b"asteroidb/bound-operation/v1";
        let mut out = Vec::new();
        put_bytes(&mut out, DOMAIN);
        put_subject(&mut out, &self.operation.subject);
        match &self.operation.action {
            CandidateKvAction::Create {
                key,
                value_bytes,
                timestamp,
            } => {
                out.push(1);
                put_bytes(&mut out, key.as_bytes());
                put_bytes(&mut out, value_bytes);
                put_hlc(&mut out, timestamp);
            }
            CandidateKvAction::Add {
                key,
                value_bytes,
                timestamp,
            } => {
                out.push(2);
                put_bytes(&mut out, key.as_bytes());
                put_bytes(&mut out, value_bytes);
                put_hlc(&mut out, timestamp);
            }
            CandidateKvAction::Delete { key } => {
                out.push(3);
                put_bytes(&mut out, key.as_bytes());
            }
        }
        put_subject(&mut out, &self.declaration.creation_id);
        put_bytes(&mut out, self.declaration.key.as_bytes());
        out.push(match self.declaration.mode {
            CertificationMode::Enabled => 1,
            CertificationMode::Disabled => 2,
        });
        out
    }

    /// SHA-256 commitment to every encoded field, including creation mode.
    /// Collision resistance is a cryptographic assumption, as in Lean.
    pub fn commitment(&self) -> [u8; 32] {
        Sha256::digest(self.canonical_bytes()).into()
    }
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
    out.extend_from_slice(bytes);
}

fn put_subject(out: &mut Vec<u8>, subject: &CrdtSubject) {
    put_bytes(out, subject.namespace_id.as_bytes());
    put_bytes(out, subject.event_dot.writer.0.as_bytes());
    out.extend_from_slice(&subject.event_dot.counter.to_be_bytes());
}

fn put_hlc(out: &mut Vec<u8>, timestamp: &HlcTimestamp) {
    out.extend_from_slice(&timestamp.physical.to_be_bytes());
    out.extend_from_slice(&timestamp.logical.to_be_bytes());
    put_bytes(out, timestamp.node_id.as_bytes());
}

/// Ephemeral admission guard for a node or Authority. Persistent integration
/// must restore this map before issuing any signatures after restart.
#[derive(Debug, Default)]
pub struct OperationHistory {
    chosen: BTreeMap<CrdtSubject, ChosenOperation>,
}

#[derive(Debug)]
enum ChosenOperation {
    Body(BoundOperation),
    Receipt(OperationReceipt),
}

const LEDGER_CHECKPOINT: &str = "operation-checkpoint-v1";
const LEDGER_CHECKPOINT_TEMP: &str = ".operation-checkpoint-v1.tmp";
const LEDGER_CHECKPOINT_MAGIC: &[u8; 8] = b"AOPCKP01";
const LEDGER_LOCK: &str = ".operation-ledger.lock";
const LEDGER_INITIALIZED: &str = "operation-ledger-initialized";
const LEDGER_INITIALIZED_TEMP: &str = ".operation-ledger-initialized.tmp";
const LEDGER_INITIALIZED_MAGIC: &[u8; 8] = b"AOPINI01";
const LEDGER_USED_MAGIC: &[u8; 8] = b"AOPUSE01";

fn ledger_used_path(dir: &Path) -> io::Result<std::path::PathBuf> {
    let parent = dir
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "ledger has no parent"))?;
    let parent = if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    };
    let mut name = OsString::from(".");
    name.push(dir.file_name().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "ledger has no directory name")
    })?);
    name.push(".operation-ledger-used");
    Ok(parent.join(name))
}

fn read_ledger_used(path: &Path) -> io::Result<bool> {
    match fs::read(path) {
        Ok(bytes) if bytes == LEDGER_USED_MAGIC => Ok(true),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid operation ledger used marker",
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

fn write_ledger_used(path: &Path) -> io::Result<()> {
    let mut temp_name = path.as_os_str().to_os_string();
    temp_name.push(".tmp");
    let temp = std::path::PathBuf::from(temp_name);
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&temp)?;
    file.write_all(LEDGER_USED_MAGIC)?;
    file.sync_all()?;
    fs::rename(temp, path)?;
    fsync_dir(path.parent().expect("used marker has parent"))
}

#[derive(Serialize, Deserialize)]
struct LedgerCheckpoint {
    sealed_seq: u64,
    records: Vec<WalRecord>,
}

fn read_ledger_checkpoint(dir: &Path) -> io::Result<Option<LedgerCheckpoint>> {
    let bytes = match fs::read(dir.join(LEDGER_CHECKPOINT)) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    if bytes.len() < LEDGER_CHECKPOINT_MAGIC.len() + 8 + 32
        || &bytes[..8] != LEDGER_CHECKPOINT_MAGIC
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid operation checkpoint header",
        ));
    }
    let len = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let payload_len = usize::try_from(len).ok();
    if payload_len != Some(bytes.len() - 16 - 32) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid operation checkpoint length",
        ));
    }
    let payload_end = 16 + payload_len.unwrap();
    if Sha256::digest(&bytes[..payload_end]).as_slice() != &bytes[payload_end..] {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "operation checkpoint checksum mismatch",
        ));
    }
    let (checkpoint, used) = bincode::serde::decode_from_slice::<LedgerCheckpoint, _>(
        &bytes[16..payload_end],
        bincode::config::standard(),
    )
    .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    if used != payload_end - 16 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "operation checkpoint trailing payload",
        ));
    }
    Ok(Some(checkpoint))
}

fn write_ledger_checkpoint(dir: &Path, checkpoint: &LedgerCheckpoint) -> io::Result<()> {
    let payload = bincode::serde::encode_to_vec(checkpoint, bincode::config::standard())
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    let mut bytes = Vec::with_capacity(16 + payload.len() + 32);
    bytes.extend_from_slice(LEDGER_CHECKPOINT_MAGIC);
    bytes.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&payload);
    let checksum = Sha256::digest(&bytes);
    bytes.extend_from_slice(&checksum);
    let temp = dir.join(LEDGER_CHECKPOINT_TEMP);
    // A leftover temporary file has no committed meaning. Truncation here
    // cannot affect the old checkpoint or any WAL segment.
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&temp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temp, dir.join(LEDGER_CHECKPOINT))?;
    fsync_dir(dir)?;
    Ok(())
}

fn read_ledger_initialized(dir: &Path) -> io::Result<bool> {
    match fs::read(dir.join(LEDGER_INITIALIZED)) {
        Ok(bytes) if bytes == LEDGER_INITIALIZED_MAGIC => Ok(true),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid operation ledger initialization marker",
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

fn write_ledger_initialized(dir: &Path) -> io::Result<()> {
    let temp = dir.join(LEDGER_INITIALIZED_TEMP);
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&temp)?;
    file.write_all(LEDGER_INITIALIZED_MAGIC)?;
    file.sync_all()?;
    fs::rename(&temp, dir.join(LEDGER_INITIALIZED))?;
    fsync_dir(dir)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordOutcome {
    New,
    Retry,
}

/// Validation, append, or sync failure while preserving an operation ID.
#[derive(Debug)]
pub enum PersistOperationError {
    Operation(OperationError),
    Io(io::Error),
}

impl From<OperationError> for PersistOperationError {
    fn from(value: OperationError) -> Self {
        Self::Operation(value)
    }
}

impl From<io::Error> for PersistOperationError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl OperationHistory {
    fn from_checkpoint_records(records: &[WalRecord]) -> Result<Self, OperationError> {
        let mut history = Self::default();
        for record in records {
            match record {
                WalRecord::BoundOperation { bound } => {
                    if history.record(bound.clone())? != RecordOutcome::New {
                        return Err(OperationError::ConflictingContent(
                            bound.operation.subject.clone(),
                        ));
                    }
                }
                WalRecord::OperationReceipt { receipt } => {
                    let subject = receipt.subject.clone();
                    if subject.namespace_id != receipt.declaration.creation_id.namespace_id
                        || (receipt.action_kind == ReceiptActionKind::Create
                            && subject != receipt.declaration.creation_id)
                        || history.chosen.contains_key(&subject)
                    {
                        return Err(OperationError::InvalidReceipt(subject));
                    }
                    history
                        .chosen
                        .insert(subject, ChosenOperation::Receipt(receipt.clone()));
                }
                _ => return Err(OperationError::InvalidCheckpoint),
            }
        }
        Ok(history)
    }

    fn outcome_for(&self, bound: &BoundOperation) -> Result<RecordOutcome, OperationError> {
        let subject = &bound.operation.subject;
        match self.chosen.get(subject) {
            Some(ChosenOperation::Body(existing)) if existing == bound => Ok(RecordOutcome::Retry),
            Some(ChosenOperation::Receipt(existing))
                if existing == &OperationReceipt::from(bound) =>
            {
                Ok(RecordOutcome::Retry)
            }
            Some(_) => Err(OperationError::ConflictingContent(subject.clone())),
            None => Ok(RecordOutcome::New),
        }
    }

    pub fn record(&mut self, bound: BoundOperation) -> Result<RecordOutcome, OperationError> {
        bound.validate_structure()?;
        let subject = bound.operation.subject.clone();
        let outcome = self.outcome_for(&bound)?;
        if outcome == RecordOutcome::New {
            self.chosen.insert(subject, ChosenOperation::Body(bound));
        }
        Ok(outcome)
    }

    /// Append the exact chosen payload before adding a new ID to memory.
    /// The returned position must be synced before acknowledging or signing.
    /// A retry is appended too: compacted IDs append only their receipt,
    /// never restoring a reclaimed body.
    pub fn record_to_wal(
        &mut self,
        wal: &mut WalWriter,
        bound: BoundOperation,
    ) -> Result<(RecordOutcome, WalPos), PersistOperationError> {
        bound.validate_structure()?;
        let subject = bound.operation.subject.clone();
        let outcome = self.outcome_for(&bound)?;
        let record = match self.chosen.get(&subject) {
            Some(ChosenOperation::Receipt(receipt)) => WalRecord::OperationReceipt {
                receipt: receipt.clone(),
            },
            _ => WalRecord::BoundOperation {
                bound: bound.clone(),
            },
        };
        let pos = wal.append(&record)?;
        if outcome == RecordOutcome::New {
            self.chosen.insert(subject, ChosenOperation::Body(bound));
        }
        Ok((outcome, pos))
    }

    fn apply_receipt(&mut self, receipt: OperationReceipt) -> Result<(), OperationError> {
        let subject = receipt.subject.clone();
        let valid = match self.chosen.get(&subject) {
            Some(ChosenOperation::Body(bound)) => receipt == OperationReceipt::from(bound),
            Some(ChosenOperation::Receipt(existing)) => existing == &receipt,
            None => false,
        };
        if !valid {
            return Err(OperationError::InvalidReceipt(subject));
        }
        self.chosen
            .insert(subject, ChosenOperation::Receipt(receipt));
        Ok(())
    }

    /// Rebuild the immutable ID map from WAL records in append order. A
    /// conflicting or malformed persisted payload fails closed on restart.
    /// This can also be used with a checkpoint plus retained WAL records;
    /// the checkpoint must preserve every earlier operation ID receipt.
    pub fn from_wal_records(records: &[WalRecord]) -> Result<Self, OperationError> {
        let mut history = Self::default();
        history.replay_wal_records(records)?;
        Ok(history)
    }

    fn replay_wal_records(&mut self, records: &[WalRecord]) -> Result<(), OperationError> {
        for record in records {
            match record {
                WalRecord::BoundOperation { bound } => {
                    self.record(bound.clone())?;
                }
                WalRecord::OperationReceipt { receipt } => {
                    self.apply_receipt(receipt.clone())?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub fn get(&self, subject: &CrdtSubject) -> Option<&BoundOperation> {
        match self.chosen.get(subject) {
            Some(ChosenOperation::Body(bound)) => Some(bound),
            _ => None,
        }
    }

    pub fn receipt(&self, subject: &CrdtSubject) -> Option<&OperationReceipt> {
        match self.chosen.get(subject) {
            Some(ChosenOperation::Receipt(receipt)) => Some(receipt),
            _ => None,
        }
    }

    /// Iterate over immutable operation bodies retained for local reads.
    pub fn iter(&self) -> impl Iterator<Item = &BoundOperation> {
        self.chosen.values().filter_map(|chosen| match chosen {
            ChosenOperation::Body(bound) => Some(bound),
            ChosenOperation::Receipt(_) => None,
        })
    }

    pub fn len(&self) -> usize {
        self.chosen.len()
    }

    pub fn is_empty(&self) -> bool {
        self.chosen.is_empty()
    }
}

/// A separate durable ID ledger. Its directory must not be included in
/// CRDT WAL checkpoint pruning; retaining every ID receipt is necessary to
/// reject a conflicting retry after old value bodies are reclaimed.
pub struct OperationLedger {
    // Held from before recovery until drop. Otherwise two independent WAL
    // writers can both accept different contents for the same operation ID.
    _lock: File,
    history: OperationHistory,
    wal: WalWriter,
    dir: std::path::PathBuf,
    checkpoint_pending: bool,
    // An append or seal error leaves the on-disk choice uncertain. Reopen
    // before accepting another ID so a later write cannot conflict with it.
    poisoned: bool,
}

impl OperationLedger {
    /// Open an independent ledger directory and rebuild its complete ID map.
    /// A torn final append is discarded; semantic conflicts or WAL corruption
    /// fail the open. Callers must keep this directory for the ID retention
    /// lifetime and never pass it to store checkpoint segment deletion.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, PersistOperationError> {
        let dir = dir.as_ref();
        create_dir_all_durable(dir)?;
        let lock_path = dir.join(LEDGER_LOCK);
        let fresh_lock = !lock_path.exists();
        let lock = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(lock_path)?;
        lock.try_lock()
            .map_err(|error| io::Error::new(io::ErrorKind::WouldBlock, error.to_string()))?;
        if fresh_lock {
            lock.sync_all()?;
            fsync_dir(dir)?;
        }
        Self::load_locked(dir, lock, fresh_lock)
    }

    /// Rebuild a poisoned inbox ledger while its old handle continues to hold
    /// the exclusive directory lease. The caller must prevent all use of the
    /// old handle and replace it with the returned one under its inbox mutex.
    pub(crate) fn reopen_for_recovery(&self) -> Result<Self, PersistOperationError> {
        Self::load_locked(&self.dir, self._lock.try_clone()?, false)
    }

    fn load_locked(
        dir: &Path,
        lock: File,
        fresh_lock: bool,
    ) -> Result<Self, PersistOperationError> {
        let used_path = ledger_used_path(dir)?;
        let used = read_ledger_used(&used_path)?;
        let initialized = read_ledger_initialized(dir)?;
        // The outer marker is published before open returns, and therefore
        // before any operation can be acknowledged. Without it, an existing
        // lock may only be an interrupted first initialization.
        if used && (fresh_lock || !initialized) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "previously used operation ledger is incomplete",
            )
            .into());
        }
        let checkpoint = read_ledger_checkpoint(dir)?;
        if checkpoint.is_some() && (!initialized || !used) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "operation checkpoint lacks initialization marker",
            )
            .into());
        }
        if checkpoint
            .as_ref()
            .is_some_and(|checkpoint| checkpoint.sealed_seq == 0)
        {
            return Err(OperationError::InvalidCheckpoint.into());
        }
        let cutoff = checkpoint
            .as_ref()
            .map_or(0, |checkpoint| checkpoint.sealed_seq);
        // A missing pre-checkpoint segment can erase an acknowledged ID lock
        // while leaving later operations readable. Every retained chain must
        // begin at one (or immediately after the checkpoint) and be contiguous.
        let mut expected = cutoff.checked_add(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "operation checkpoint sequence overflow",
            )
        })?;
        let mut seen = false;
        for (seq, _) in wal::list_segments(dir)?
            .into_iter()
            .filter(|(seq, _)| *seq > cutoff)
        {
            if seq != expected {
                return Err(
                    io::Error::new(io::ErrorKind::InvalidData, "operation ledger WAL gap").into(),
                );
            }
            seen = true;
            expected = expected.checked_add(1).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "operation ledger WAL sequence overflow",
                )
            })?;
        }
        if initialized && used && !seen {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "operation ledger WAL chain is missing",
            )
            .into());
        }
        let mut read = wal::read_segments_after(dir, cutoff)?;
        if !used
            && checkpoint.is_none()
            && read.outcome == WalReadOutcome::Corruption
            && read.records.is_empty()
            && read.segments == 1
            && read.stop.as_ref().is_some_and(|stop| {
                stop.valid_len == 0 && fs::metadata(&stop.path).is_ok_and(|meta| meta.len() <= 16)
            })
        {
            // Before the outer marker exists, no caller can have appended a
            // record. A crash during the first WAL header write may leave a
            // mixed prefix and zeroes even at the full header length.
            let torn = wal::WalReadResult {
                records: Vec::new(),
                outcome: WalReadOutcome::TornTail,
                segments: read.segments,
                stop: read.stop.clone(),
            };
            wal::repair_torn_empty_tail(dir, &torn)?;
            read = wal::read_segments_after(dir, cutoff)?;
        }
        if !used && !read.records.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "operation ledger records lack initialization marker",
            )
            .into());
        }
        if read.outcome == WalReadOutcome::Corruption {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "operation ledger WAL corruption",
            )
            .into());
        }
        if read.records.iter().any(|record| {
            !matches!(
                record,
                WalRecord::BoundOperation { .. } | WalRecord::OperationReceipt { .. }
            )
        }) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "non-operation record in operation ledger",
            )
            .into());
        }
        // A receipt appended after the last checkpoint may have replaced a
        // body whose bytes still occupy an older WAL segment.
        let checkpoint_pending = read
            .records
            .iter()
            .any(|record| matches!(record, WalRecord::OperationReceipt { .. }));
        let mut history = match checkpoint {
            Some(checkpoint) => OperationHistory::from_checkpoint_records(&checkpoint.records)?,
            None => OperationHistory::default(),
        };
        history.replay_wal_records(&read.records)?;
        if read.outcome == WalReadOutcome::TornTail && !wal::repair_torn_empty_tail(dir, &read)? {
            wal::truncate_to_valid_prefix(dir, &read)?;
        }
        // A previous rotate may have failed while syncing a complete frame.
        // Recovery can parse that frame, but must make it durable before a
        // recovered admission is used to issue another signed root.
        for (_, path) in wal::list_segments(dir)?
            .into_iter()
            .filter(|(seq, _)| *seq > cutoff)
        {
            fs::File::open(path)?.sync_all()?;
        }
        fsync_dir(dir)?;
        // Publication can succeed while deletion is interrupted. The
        // checkpoint is already the recovery source, so retry deletion here.
        if cutoff != 0 {
            wal::remove_segments_up_to(dir, cutoff)?;
        }
        let min_seq = cutoff.checked_add(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "operation checkpoint sequence overflow",
            )
        })?;
        let wal = WalWriter::open_at_least(WalConfig::new(dir, SyncPolicy::Always), min_seq)?;
        // The marker is published before any caller can acknowledge an
        // operation. If all WAL files later disappear, recovery fails closed
        // instead of mistaking a previously used ledger for a fresh one.
        if !initialized {
            write_ledger_initialized(dir)?;
        }
        if !used {
            write_ledger_used(&used_path)?;
        }
        Ok(Self {
            _lock: lock,
            history,
            wal,
            dir: dir.to_path_buf(),
            checkpoint_pending,
            poisoned: false,
        })
    }

    /// Persist and fsync the ID/content choice before returning success.
    /// Synchronous rotation seals the record and updates the durable WAL
    /// watermark. Retries also sync, so callers can sign or ACK on return.
    pub fn record_and_sync(
        &mut self,
        bound: BoundOperation,
    ) -> Result<RecordOutcome, PersistOperationError> {
        if self.poisoned {
            return Err(io::Error::other("operation ledger requires recovery").into());
        }
        bound.validate_structure()?;
        let subject = bound.operation.subject.clone();
        let outcome = self.history.outcome_for(&bound)?;
        let record = match self.history.chosen.get(&subject) {
            Some(ChosenOperation::Receipt(receipt)) => WalRecord::OperationReceipt {
                receipt: receipt.clone(),
            },
            _ => WalRecord::BoundOperation {
                bound: bound.clone(),
            },
        };
        if let Err(error) = self.wal.append(&record).and_then(|_| self.wal.rotate()) {
            self.poisoned = true;
            return Err(error.into());
        }
        if outcome == RecordOutcome::New {
            self.history
                .chosen
                .insert(subject, ChosenOperation::Body(bound));
        }
        Ok(outcome)
    }

    /// Replace a retained body in memory with its small, durable ID receipt.
    /// Call [`Self::checkpoint_and_sync`] to reclaim older body frames from
    /// disk after one or more compactions.
    /// The caller must establish that the body has no remaining delivery,
    /// certification, or read obligations before invoking this method.
    pub fn compact_and_sync(
        &mut self,
        subject: &CrdtSubject,
    ) -> Result<bool, PersistOperationError> {
        if self.poisoned {
            return Err(io::Error::other("operation ledger requires recovery").into());
        }
        let bound = match self.history.chosen.get(subject) {
            Some(ChosenOperation::Body(bound)) => bound,
            Some(ChosenOperation::Receipt(_)) => return Ok(false),
            None => return Err(OperationError::UnknownSubject(subject.clone()).into()),
        };
        let receipt = OperationReceipt::from(bound);
        if let Err(error) = self
            .wal
            .append(&WalRecord::OperationReceipt {
                receipt: receipt.clone(),
            })
            .and_then(|_| self.wal.rotate())
        {
            // A receipt might already be durable even though sealing failed.
            // Rebuild from disk before admitting or collecting further IDs.
            self.poisoned = true;
            return Err(error.into());
        }
        self.history.apply_receipt(receipt)?;
        self.checkpoint_pending = true;
        Ok(true)
    }

    pub fn checkpoint_pending(&self) -> bool {
        self.checkpoint_pending
    }

    /// Rewrite the full durable ID set, then reclaim every sealed WAL
    /// segment up to the checkpoint boundary. The checkpoint contains a
    /// receipt for each reclaimed body and the exact body for every still
    /// retained operation. It is synced and atomically published before
    /// any old segment is unlinked. A crash during deletion is harmless:
    /// recovery ignores all segments at or below the checkpoint boundary.
    pub fn checkpoint_and_sync(&mut self) -> Result<(), PersistOperationError> {
        if self.poisoned {
            return Err(io::Error::other("operation ledger requires recovery").into());
        }
        let sealed_seq = self.wal.rotate()?;
        let records = self
            .history
            .chosen
            .values()
            .map(|chosen| match chosen {
                ChosenOperation::Body(bound) => WalRecord::BoundOperation {
                    bound: bound.clone(),
                },
                ChosenOperation::Receipt(receipt) => WalRecord::OperationReceipt {
                    receipt: receipt.clone(),
                },
            })
            .collect();
        write_ledger_checkpoint(
            &self.dir,
            &LedgerCheckpoint {
                sealed_seq,
                records,
            },
        )?;
        wal::remove_segments_up_to(&self.dir, sealed_seq)?;
        self.checkpoint_pending = false;
        Ok(())
    }

    pub fn get(&self, subject: &CrdtSubject) -> Option<&BoundOperation> {
        self.history.get(subject)
    }

    pub fn receipt(&self, subject: &CrdtSubject) -> Option<&OperationReceipt> {
        self.history.receipt(subject)
    }

    /// Iterate over durable ID metadata whose value bodies were reclaimed.
    pub fn receipts(&self) -> impl Iterator<Item = &OperationReceipt> {
        self.history
            .chosen
            .values()
            .filter_map(|chosen| match chosen {
                ChosenOperation::Receipt(receipt) => Some(receipt),
                ChosenOperation::Body(_) => None,
            })
    }

    /// Iterate over bodies retained by the durable operation ledger.
    pub fn iter(&self) -> impl Iterator<Item = &BoundOperation> {
        self.history.iter()
    }

    pub fn history(&self) -> &OperationHistory {
        &self.history
    }

    pub fn len(&self) -> usize {
        self.history.len()
    }

    pub fn is_empty(&self) -> bool {
        self.history.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn subject(counter: u64) -> CrdtSubject {
        CrdtSubject {
            namespace_id: "ns".into(),
            event_dot: EventDot {
                writer: NodeId("writer".into()),
                counter,
            },
        }
    }

    fn creation() -> BoundOperation {
        let id = subject(1);
        BoundOperation {
            operation: OperationLeaf {
                subject: id.clone(),
                action: CandidateKvAction::Create {
                    key: "key".into(),
                    value_bytes: b"value".to_vec(),
                    timestamp: HlcTimestamp {
                        physical: 10,
                        logical: 2,
                        node_id: "writer".into(),
                    },
                },
            },
            declaration: Declaration {
                creation_id: id,
                key: "key".into(),
                mode: CertificationMode::Enabled,
            },
        }
    }

    #[test]
    fn ledger_has_one_live_writer_and_recovery_keeps_id_choice() {
        let dir = tempdir().unwrap();
        let original = creation();
        let mut first = OperationLedger::open(dir.path()).unwrap();
        first.record_and_sync(original.clone()).unwrap();
        let segments_before = wal::list_segments(dir.path()).unwrap().len();
        assert!(matches!(
            OperationLedger::open(dir.path()),
            Err(PersistOperationError::Io(error)) if error.kind() == io::ErrorKind::WouldBlock
        ));
        assert_eq!(
            wal::list_segments(dir.path()).unwrap().len(),
            segments_before
        );
        drop(first);

        let mut reopened = OperationLedger::open(dir.path()).unwrap();
        let mut changed = original.clone();
        changed.declaration.mode = CertificationMode::Disabled;
        assert!(matches!(
            reopened.record_and_sync(changed),
            Err(PersistOperationError::Operation(
                OperationError::ConflictingContent(_)
            ))
        ));
        assert_eq!(
            reopened.record_and_sync(original).unwrap(),
            RecordOutcome::Retry
        );
    }

    #[test]
    fn ledger_refuses_missing_precheckpoint_wal_segments() {
        for remove_all in [false, true] {
            let dir = tempdir().unwrap();
            let original = creation();
            {
                let mut ledger = OperationLedger::open(dir.path()).unwrap();
                ledger.record_and_sync(original.clone()).unwrap();
            }
            let segments = wal::list_segments(dir.path()).unwrap();
            assert_eq!(segments.len(), 2);
            for (_, path) in segments.iter().take(if remove_all { 2 } else { 1 }) {
                fs::remove_file(path).unwrap();
            }
            assert!(matches!(
                OperationLedger::open(dir.path()),
                Err(PersistOperationError::Io(error)) if error.kind() == io::ErrorKind::InvalidData
            ));
        }
    }

    #[test]
    fn interrupted_first_ledger_open_recovers_empty_state() {
        for stage in 0..5 {
            let root = tempdir().unwrap();
            let dir = root.path().join("ledger");
            create_dir_all_durable(&dir).unwrap();
            let lock_path = dir.join(LEDGER_LOCK);
            let lock = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&lock_path)
                .unwrap();
            lock.sync_all().unwrap();
            fsync_dir(&dir).unwrap();
            if stage >= 1 {
                let _wal =
                    WalWriter::open_at_least(WalConfig::new(&dir, SyncPolicy::Always), 1).unwrap();
            }
            if stage >= 2 {
                write_ledger_initialized(&dir).unwrap();
            }
            if stage == 3 {
                let mut temp = ledger_used_path(&dir).unwrap().into_os_string();
                temp.push(".tmp");
                fs::write(std::path::PathBuf::from(temp), b"partial").unwrap();
            }
            if stage == 4 {
                let segments = wal::list_segments(&dir).unwrap();
                assert_eq!(segments.len(), 1);
                let mut torn_header = [0u8; 16];
                torn_header[0] = b'A';
                torn_header[1] = b'Z';
                fs::write(&segments[0].1, torn_header).unwrap();
                File::open(&segments[0].1).unwrap().sync_all().unwrap();
            }
            let mut ledger = OperationLedger::open(&dir).unwrap();
            assert!(ledger.history.is_empty());
            assert!(read_ledger_used(&ledger_used_path(&dir).unwrap()).unwrap());
            ledger.record_and_sync(creation()).unwrap();
            drop(ledger);
            let reopened = OperationLedger::open(&dir).unwrap();
            assert_eq!(reopened.get(&subject(1)), Some(&creation()));
        }
    }

    #[test]
    fn ledger_refuses_records_without_initialization_marker() {
        let dir = tempdir().unwrap();
        {
            let mut ledger = OperationLedger::open(dir.path()).unwrap();
            ledger.record_and_sync(creation()).unwrap();
        }
        fs::remove_file(dir.path().join(LEDGER_INITIALIZED)).unwrap();
        assert!(matches!(
            OperationLedger::open(dir.path()),
            Err(PersistOperationError::Io(error)) if error.kind() == io::ErrorKind::InvalidData
        ));

        // Even if every WAL segment disappears with the internal marker, the
        // outer used marker distinguishes this acknowledged ledger from an
        // interrupted first open. Do not silently reuse old operation IDs.
        for (_, path) in wal::list_segments(dir.path()).unwrap() {
            fs::remove_file(path).unwrap();
        }
        assert!(matches!(
            OperationLedger::open(dir.path()),
            Err(PersistOperationError::Io(error)) if error.kind() == io::ErrorKind::InvalidData
        ));
    }

    #[test]
    fn same_id_same_content_is_only_a_retry() {
        let bound = creation();
        let mut history = OperationHistory::default();
        assert_eq!(history.record(bound.clone()), Ok(RecordOutcome::New));
        assert_eq!(history.record(bound.clone()), Ok(RecordOutcome::Retry));
        assert_eq!(history.len(), 1);
        assert_eq!(history.get(&subject(1)), Some(&bound));
    }

    #[test]
    fn same_id_changed_content_is_rejected_without_replacement() {
        let original = creation();
        let mut history = OperationHistory::default();
        history.record(original.clone()).unwrap();
        let mut changed = original.clone();
        if let CandidateKvAction::Create { value_bytes, .. } = &mut changed.operation.action {
            *value_bytes = b"other".to_vec();
        }
        assert_eq!(
            history.record(changed),
            Err(OperationError::ConflictingContent(subject(1)))
        );
        assert_eq!(history.get(&subject(1)), Some(&original));
    }

    #[test]
    fn mode_and_action_are_part_of_the_commitment() {
        let original = creation();
        let mut mode_swap = original.clone();
        mode_swap.declaration.mode = CertificationMode::Disabled;
        assert_ne!(original.canonical_bytes(), mode_swap.canonical_bytes());
        assert_ne!(original.commitment(), mode_swap.commitment());
        let mut history = OperationHistory::default();
        history.record(original.clone()).unwrap();
        assert_eq!(
            history.record(mode_swap),
            Err(OperationError::ConflictingContent(subject(1)))
        );

        let mut action_swap = original.clone();
        if let CandidateKvAction::Create {
            key,
            value_bytes,
            timestamp,
        } = action_swap.operation.action
        {
            action_swap.operation.action = CandidateKvAction::Add {
                key,
                value_bytes,
                timestamp,
            };
        }
        assert_ne!(original.commitment(), action_swap.commitment());
    }

    #[test]
    fn distinct_ids_keep_separate_operations_on_one_key() {
        let create = creation();
        let mut add = create.clone();
        add.operation.subject = subject(2);
        if let CandidateKvAction::Create {
            key,
            value_bytes,
            timestamp,
        } = add.operation.action
        {
            add.operation.action = CandidateKvAction::Add {
                key,
                value_bytes,
                timestamp,
            };
        }
        let mut history = OperationHistory::default();
        assert_eq!(history.record(create), Ok(RecordOutcome::New));
        assert_eq!(history.record(add), Ok(RecordOutcome::New));
        assert_eq!(history.len(), 2);
    }

    #[test]
    fn structure_catches_mismatched_creation_references() {
        let mut wrong = creation();
        wrong.declaration.key = "other".into();
        assert_eq!(
            wrong.validate_structure(),
            Err(OperationError::DifferentKey)
        );
        let mut wrong = creation();
        wrong.declaration.creation_id = subject(2);
        assert_eq!(
            wrong.validate_structure(),
            Err(OperationError::CreateIdMismatch)
        );
    }

    #[test]
    fn length_framing_timestamp_and_delete_cannot_alias_another_leaf() {
        let first = creation();
        let mut differently_split = first.clone();
        differently_split.operation.subject.namespace_id = "n".into();
        differently_split.operation.subject.event_dot.writer = NodeId("swriter".into());
        differently_split.declaration.creation_id = differently_split.operation.subject.clone();
        assert_ne!(first.canonical_bytes(), differently_split.canonical_bytes());

        let mut later = first.clone();
        if let CandidateKvAction::Create { timestamp, .. } = &mut later.operation.action {
            timestamp.logical += 1;
        }
        assert_ne!(first.commitment(), later.commitment());

        let mut deletion = first.clone();
        deletion.operation.subject = subject(2);
        deletion.operation.action = CandidateKvAction::Delete { key: "key".into() };
        assert_eq!(deletion.validate_structure(), Ok(()));
        assert_ne!(first.commitment(), deletion.commitment());
        let mut history = OperationHistory::default();
        history.record(first).unwrap();
        history.record(deletion).unwrap();
        assert_eq!(history.len(), 2);
    }

    #[test]
    fn ledger_survives_store_checkpoint_pruning_and_rejects_changed_id() {
        let root = tempdir().unwrap();
        let ledger_dir = root.path().join("operation-ledger");
        let store_wal_dir = root.path().join("wal").join("certified");
        let original = creation();
        {
            let mut ledger = OperationLedger::open(&ledger_dir).unwrap();
            assert_eq!(
                ledger.record_and_sync(original.clone()).unwrap(),
                RecordOutcome::New
            );
            assert_eq!(
                ledger.record_and_sync(original.clone()).unwrap(),
                RecordOutcome::Retry
            );

            let mut store_wal =
                WalWriter::open(WalConfig::new(&store_wal_dir, SyncPolicy::Always)).unwrap();
            store_wal
                .append(&WalRecord::MergeFailed {
                    keys: vec!["key".into()],
                })
                .unwrap();
            let sealed = store_wal.rotate().unwrap();
            wal::remove_segments_up_to(&store_wal_dir, sealed).unwrap();
        }
        let mut ledger = OperationLedger::open(&ledger_dir).unwrap();
        assert_eq!(ledger.get(&subject(1)), Some(&original));
        let mut changed = original.clone();
        changed.declaration.mode = CertificationMode::Disabled;
        assert!(matches!(
            ledger.record_and_sync(changed),
            Err(PersistOperationError::Operation(
                OperationError::ConflictingContent(_)
            ))
        ));
        assert_eq!(
            ledger.record_and_sync(original).unwrap(),
            RecordOutcome::Retry
        );
    }

    #[test]
    fn failed_append_does_not_reserve_operation_id() {
        let root = tempdir().unwrap();
        let mut wal = WalWriter::open(WalConfig::new(root.path(), SyncPolicy::Always)).unwrap();
        wal.inject_append_failures(1);
        let mut history = OperationHistory::default();
        let original = creation();
        assert!(matches!(
            history.record_to_wal(&mut wal, original.clone()),
            Err(PersistOperationError::Io(_))
        ));
        assert!(history.is_empty());
        assert_eq!(
            history.record_to_wal(&mut wal, original).unwrap().0,
            RecordOutcome::New
        );
    }

    #[test]
    fn failed_ledger_append_does_not_publish_unacknowledged_body() {
        let root = tempdir().unwrap();
        let mut ledger = OperationLedger::open(root.path()).unwrap();
        let original = creation();
        ledger.wal.inject_append_failures(1);
        assert!(matches!(
            ledger.record_and_sync(original.clone()),
            Err(PersistOperationError::Io(_))
        ));
        assert!(ledger.get(&original.operation.subject).is_none());
        assert!(ledger.iter().next().is_none());
        assert!(matches!(
            ledger.record_and_sync(original.clone()),
            Err(PersistOperationError::Io(_))
        ));
        let mut recovered = ledger.reopen_for_recovery().unwrap();
        assert_eq!(
            recovered.record_and_sync(original).unwrap(),
            RecordOutcome::New
        );
    }

    #[test]
    fn failed_ledger_seal_recovers_the_written_id_choice() {
        let root = tempdir().unwrap();
        let mut ledger = OperationLedger::open(root.path()).unwrap();
        let original = creation();
        let active = wal::list_segments(root.path()).unwrap().pop().unwrap().0;
        let blocked_segment = root.path().join(format!("wal-{:016x}.log", active + 1));
        fs::create_dir(&blocked_segment).unwrap();
        assert!(matches!(
            ledger.record_and_sync(original.clone()),
            Err(PersistOperationError::Io(_))
        ));
        assert!(ledger.get(&original.operation.subject).is_none());
        assert!(matches!(
            ledger.record_and_sync(original.clone()),
            Err(PersistOperationError::Io(_))
        ));
        fs::remove_dir(&blocked_segment).unwrap();
        let mut recovered = ledger.reopen_for_recovery().unwrap();
        assert_eq!(recovered.get(&original.operation.subject), Some(&original));
        assert_eq!(
            recovered.record_and_sync(original.clone()).unwrap(),
            RecordOutcome::Retry
        );
        let mut changed = original;
        changed.declaration.mode = CertificationMode::Disabled;
        assert!(matches!(
            recovered.record_and_sync(changed),
            Err(PersistOperationError::Operation(
                OperationError::ConflictingContent(_)
            ))
        ));
    }

    #[test]
    fn conflicting_wal_records_fail_recovery() {
        let root = tempdir().unwrap();
        let original = creation();
        {
            let mut ledger = OperationLedger::open(root.path()).unwrap();
            ledger
                .wal
                .append(&WalRecord::BoundOperation {
                    bound: original.clone(),
                })
                .unwrap();
            let mut changed = original;
            if let CandidateKvAction::Create { value_bytes, .. } = &mut changed.operation.action {
                *value_bytes = b"conflict".to_vec();
            }
            ledger
                .wal
                .append(&WalRecord::BoundOperation { bound: changed })
                .unwrap();
            ledger.wal.rotate().unwrap();
        }
        assert!(matches!(
            OperationLedger::open(root.path()),
            Err(PersistOperationError::Operation(
                OperationError::ConflictingContent(_)
            ))
        ));
    }

    #[test]
    fn compact_receipt_survives_restart_and_retry_cannot_resurrect_body() {
        let root = tempdir().unwrap();
        let original = creation();
        let subject = original.operation.subject.clone();
        let expected = OperationReceipt::from(&original);
        {
            let mut ledger = OperationLedger::open(root.path()).unwrap();
            assert_eq!(
                ledger.record_and_sync(original.clone()).unwrap(),
                RecordOutcome::New
            );
            assert!(ledger.compact_and_sync(&subject).unwrap());
            assert!(!ledger.compact_and_sync(&subject).unwrap());
            assert_eq!(ledger.get(&subject), None);
            assert_eq!(ledger.receipt(&subject), Some(&expected));
            assert_eq!(ledger.iter().count(), 0);
            assert_eq!(ledger.len(), 1);
            assert_eq!(
                ledger.record_and_sync(original.clone()).unwrap(),
                RecordOutcome::Retry
            );
            assert_eq!(ledger.get(&subject), None);
        }
        let mut ledger = OperationLedger::open(root.path()).unwrap();
        assert_eq!(ledger.get(&subject), None);
        assert_eq!(ledger.receipt(&subject), Some(&expected));
        assert_eq!(
            ledger.record_and_sync(original.clone()).unwrap(),
            RecordOutcome::Retry
        );
        assert_eq!(ledger.get(&subject), None);
        let mut conflict = original;
        if let CandidateKvAction::Create { value_bytes, .. } = &mut conflict.operation.action {
            *value_bytes = b"changed".to_vec();
        }
        assert!(matches!(
            ledger.record_and_sync(conflict),
            Err(PersistOperationError::Operation(
                OperationError::ConflictingContent(_)
            ))
        ));
    }

    #[test]
    fn old_bound_operation_segment_is_rejected() {
        let root = tempdir().unwrap();
        let original = creation();
        {
            let mut ledger = OperationLedger::open(root.path()).unwrap();
            ledger.record_and_sync(original.clone()).unwrap();
        }
        for entry in std::fs::read_dir(root.path()).unwrap() {
            let path = entry.unwrap().path();
            let mut data = std::fs::read(&path).unwrap();
            if data.len() > 16 {
                data[8..12].copy_from_slice(&4u32.to_le_bytes());
                std::fs::write(path, data).unwrap();
            }
        }
        assert!(matches!(
            OperationLedger::open(root.path()),
            Err(PersistOperationError::Io(error)) if error.kind() == std::io::ErrorKind::InvalidData
        ));
    }

    #[test]
    fn forged_or_reordered_receipt_fails_recovery() {
        let root = tempdir().unwrap();
        let original = creation();
        let mut receipt = OperationReceipt::from(&original);
        receipt.commitment[0] ^= 1;
        {
            let mut ledger = OperationLedger::open(root.path()).unwrap();
            ledger
                .wal
                .append(&WalRecord::BoundOperation { bound: original })
                .unwrap();
            ledger
                .wal
                .append(&WalRecord::OperationReceipt { receipt })
                .unwrap();
            ledger.wal.rotate().unwrap();
        }
        assert!(matches!(
            OperationLedger::open(root.path()),
            Err(PersistOperationError::Operation(
                OperationError::InvalidReceipt(_)
            ))
        ));
    }

    fn ledger_bytes(dir: &Path) -> u64 {
        fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().metadata().unwrap().len())
            .sum()
    }

    #[test]
    fn checkpoint_reclaims_body_bytes_and_preserves_receipts_and_retained_bodies() {
        let root = tempdir().unwrap();
        let mut reclaimed = creation();
        if let CandidateKvAction::Create { value_bytes, .. } = &mut reclaimed.operation.action {
            *value_bytes = vec![42; 1_000_000];
        }
        let retained = BoundOperation {
            operation: OperationLeaf {
                subject: subject(2),
                action: CandidateKvAction::Add {
                    key: "key".into(),
                    value_bytes: b"retained".to_vec(),
                    timestamp: HlcTimestamp {
                        physical: 11,
                        logical: 0,
                        node_id: "writer".into(),
                    },
                },
            },
            declaration: reclaimed.declaration.clone(),
        };
        let receipt = OperationReceipt::from(&reclaimed);
        {
            let mut ledger = OperationLedger::open(root.path()).unwrap();
            ledger.record_and_sync(reclaimed.clone()).unwrap();
            ledger.record_and_sync(retained.clone()).unwrap();
            ledger.compact_and_sync(&subject(1)).unwrap();
            let before = ledger_bytes(root.path());
            ledger.checkpoint_and_sync().unwrap();
            assert!(ledger_bytes(root.path()) < before / 10);
            assert!(
                wal::list_segments(root.path())
                    .unwrap()
                    .iter()
                    .all(|(seq, _)| *seq
                        > read_ledger_checkpoint(root.path())
                            .unwrap()
                            .unwrap()
                            .sealed_seq)
            );
            assert_eq!(
                ledger.record_and_sync(reclaimed.clone()).unwrap(),
                RecordOutcome::Retry
            );
        }
        let mut ledger = OperationLedger::open(root.path()).unwrap();
        assert_eq!(ledger.receipt(&subject(1)), Some(&receipt));
        assert_eq!(ledger.get(&subject(1)), None);
        assert_eq!(ledger.get(&subject(2)), Some(&retained));
        let mut changed = reclaimed.clone();
        if let CandidateKvAction::Create { value_bytes, .. } = &mut changed.operation.action {
            *value_bytes = b"conflict".to_vec();
        }
        assert!(matches!(
            ledger.record_and_sync(changed),
            Err(PersistOperationError::Operation(
                OperationError::ConflictingContent(_)
            ))
        ));
        assert_eq!(
            ledger.record_and_sync(reclaimed).unwrap(),
            RecordOutcome::Retry
        );
    }

    #[test]
    fn receipt_replay_resumes_checkpoint_without_new_candidates() {
        let root = tempdir().unwrap();
        let bound = creation();
        let receipt = OperationReceipt::from(&bound);
        let mut ledger = OperationLedger::open(root.path()).unwrap();
        ledger.record_and_sync(bound).unwrap();
        ledger.compact_and_sync(&subject(1)).unwrap();
        drop(ledger);

        let mut ledger = OperationLedger::open(root.path()).unwrap();
        assert_eq!(ledger.receipt(&subject(1)), Some(&receipt));
        assert!(ledger.checkpoint_pending());
        ledger.checkpoint_and_sync().unwrap();
        assert!(!ledger.checkpoint_pending());
        let cutoff = read_ledger_checkpoint(root.path())
            .unwrap()
            .unwrap()
            .sealed_seq;
        assert!(
            wal::list_segments(root.path())
                .unwrap()
                .iter()
                .all(|(seq, _)| *seq > cutoff)
        );
    }

    #[test]
    fn checkpoint_crash_windows_do_not_revive_old_body() {
        let root = tempdir().unwrap();
        let bound = creation();
        let receipt = OperationReceipt::from(&bound);
        let mut ledger = OperationLedger::open(root.path()).unwrap();
        ledger.record_and_sync(bound.clone()).unwrap();
        ledger.compact_and_sync(&subject(1)).unwrap();
        // A torn temporary snapshot has no committed meaning. Old WAL is
        // complete and remains the recovery source before publication.
        fs::write(root.path().join(LEDGER_CHECKPOINT_TEMP), b"partial").unwrap();
        drop(ledger);
        let mut ledger = OperationLedger::open(root.path()).unwrap();
        assert_eq!(ledger.receipt(&subject(1)), Some(&receipt));

        // Publish a valid checkpoint without deleting old segments: crash
        // just after rename must ignore their original full body.
        let sealed_seq = ledger.wal.rotate().unwrap();
        write_ledger_checkpoint(
            root.path(),
            &LedgerCheckpoint {
                sealed_seq,
                records: vec![WalRecord::OperationReceipt {
                    receipt: receipt.clone(),
                }],
            },
        )
        .unwrap();
        drop(ledger);
        let (_, first_path) = wal::list_segments(root.path()).unwrap()[0].clone();
        fs::remove_file(first_path).unwrap();
        fsync_dir(root.path()).unwrap();
        let ledger = OperationLedger::open(root.path()).unwrap();
        assert_eq!(ledger.get(&subject(1)), None);
        assert_eq!(ledger.receipt(&subject(1)), Some(&receipt));
        assert!(!ledger.checkpoint_pending());
        assert!(
            wal::list_segments(root.path())
                .unwrap()
                .iter()
                .all(|(seq, _)| *seq > sealed_seq)
        );
        drop(ledger);

        // A further checkpoint remains valid after startup has finished
        // deleting the segments left by the interrupted publication.
        let mut ledger = OperationLedger::open(root.path()).unwrap();
        assert_eq!(ledger.receipt(&subject(1)), Some(&receipt));
        assert_eq!(
            ledger.record_and_sync(bound.clone()).unwrap(),
            RecordOutcome::Retry
        );
        assert_eq!(ledger.get(&subject(1)), None);
        ledger.checkpoint_and_sync().unwrap();
        drop(ledger);
        assert_eq!(
            OperationLedger::open(root.path())
                .unwrap()
                .receipt(&subject(1)),
            Some(&receipt)
        );
    }

    #[test]
    fn corrupt_checkpoint_fails_closed_after_wal_reclamation() {
        let root = tempdir().unwrap();
        let mut ledger = OperationLedger::open(root.path()).unwrap();
        ledger.record_and_sync(creation()).unwrap();
        ledger.compact_and_sync(&subject(1)).unwrap();
        ledger.checkpoint_and_sync().unwrap();
        drop(ledger);
        let checkpoint = root.path().join(LEDGER_CHECKPOINT);
        let mut bytes = fs::read(&checkpoint).unwrap();
        bytes[20] ^= 1;
        fs::write(checkpoint, bytes).unwrap();
        assert!(
            matches!(OperationLedger::open(root.path()), Err(PersistOperationError::Io(err)) if err.kind() == io::ErrorKind::InvalidData)
        );
    }

    #[test]
    fn consecutive_checkpoints_replay_intervening_and_later_appends() {
        let root = tempdir().unwrap();
        let original = creation();
        let mut second = original.clone();
        second.operation.subject = subject(2);
        second.operation.action = CandidateKvAction::Add {
            key: "key".into(),
            value_bytes: b"second".to_vec(),
            timestamp: HlcTimestamp {
                physical: 12,
                logical: 0,
                node_id: "writer".into(),
            },
        };
        let mut third = second.clone();
        third.operation.subject = subject(3);
        let mut ledger = OperationLedger::open(root.path()).unwrap();
        ledger.record_and_sync(original.clone()).unwrap();
        ledger.compact_and_sync(&subject(1)).unwrap();
        ledger.checkpoint_and_sync().unwrap();
        let first_cutoff = read_ledger_checkpoint(root.path())
            .unwrap()
            .unwrap()
            .sealed_seq;
        ledger.record_and_sync(second.clone()).unwrap();
        ledger.checkpoint_and_sync().unwrap();
        let second_cutoff = read_ledger_checkpoint(root.path())
            .unwrap()
            .unwrap()
            .sealed_seq;
        assert!(second_cutoff > first_cutoff);
        ledger.record_and_sync(third.clone()).unwrap();
        drop(ledger);

        let ledger = OperationLedger::open(root.path()).unwrap();
        assert_eq!(
            ledger.receipt(&subject(1)),
            Some(&OperationReceipt::from(&original))
        );
        assert_eq!(ledger.get(&subject(2)), Some(&second));
        assert_eq!(ledger.get(&subject(3)), Some(&third));
        assert_eq!(ledger.len(), 3);
    }

    #[test]
    fn torn_empty_segment_after_checkpoint_cannot_reset_sequence() {
        let root = tempdir().unwrap();
        let original = creation();
        let mut ledger = OperationLedger::open(root.path()).unwrap();
        ledger.record_and_sync(original.clone()).unwrap();
        ledger.compact_and_sync(&subject(1)).unwrap();
        ledger.checkpoint_and_sync().unwrap();
        let cutoff = read_ledger_checkpoint(root.path())
            .unwrap()
            .unwrap()
            .sealed_seq;
        drop(ledger);
        let segments = wal::list_segments(root.path()).unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].0, cutoff + 1);
        OpenOptions::new()
            .write(true)
            .open(&segments[0].1)
            .unwrap()
            .set_len(5)
            .unwrap();

        let mut recovered = OperationLedger::open(root.path()).unwrap();
        assert_eq!(
            recovered.receipt(&subject(1)),
            Some(&OperationReceipt::from(&original))
        );
        let mut next = original;
        next.operation.subject = subject(2);
        next.operation.action = CandidateKvAction::Add {
            key: "key".into(),
            value_bytes: b"later".to_vec(),
            timestamp: HlcTimestamp {
                physical: 13,
                logical: 0,
                node_id: "writer".into(),
            },
        };
        recovered.record_and_sync(next.clone()).unwrap();
        drop(recovered);
        let reopened = OperationLedger::open(root.path()).unwrap();
        assert_eq!(reopened.get(&subject(2)), Some(&next));
        assert!(
            wal::list_segments(root.path())
                .unwrap()
                .iter()
                .all(|(seq, _)| *seq > cutoff)
        );
    }

    #[test]
    fn crash_after_repairing_torn_header_keeps_checkpoint_chain() {
        let root = tempdir().unwrap();
        let original = creation();
        let mut ledger = OperationLedger::open(root.path()).unwrap();
        ledger.record_and_sync(original.clone()).unwrap();
        ledger.compact_and_sync(&subject(1)).unwrap();
        ledger.checkpoint_and_sync().unwrap();
        let cutoff = read_ledger_checkpoint(root.path())
            .unwrap()
            .unwrap()
            .sealed_seq;
        drop(ledger);

        let segments = wal::list_segments(root.path()).unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].0, cutoff + 1);
        OpenOptions::new()
            .write(true)
            .open(&segments[0].1)
            .unwrap()
            .set_len(5)
            .unwrap();
        let read = wal::read_segments_after(root.path(), cutoff).unwrap();
        assert_eq!(read.outcome, WalReadOutcome::TornTail);
        assert!(wal::repair_torn_empty_tail(root.path(), &read).unwrap());
        // Simulate a crash after the repair but before WalWriter::open.
        assert_eq!(wal::list_segments(root.path()).unwrap().len(), 1);
        let recovered = OperationLedger::open(root.path()).unwrap();
        assert_eq!(
            recovered.receipt(&subject(1)),
            Some(&OperationReceipt::from(&original))
        );
    }
}
