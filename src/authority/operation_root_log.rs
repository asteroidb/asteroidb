//! Durable Authority issuance log for exact operation Merkle roots.
//!
//! The caller must keep this directory outside checkpoint-pruned value WALs.
//! Issuance returns a signed root only after its complete inclusion paths are
//! synced. Recovery fails closed on corrupt complete frames or invalid proof,
//! signature, scope, keyset, or admitted operation content.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use ed25519_dalek::{Signature, Verifier};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::authority::certificate::{EpochConfig, KeysetRegistry, KeysetVersion};
use crate::authority::frontier_sig::NodeSigner;
use crate::authority::operation_admission::SignableCommitment;
use crate::authority::operation_merkle::{
    InclusionProof, OperationMerkleError, OperationMerkleTree, OperationScope, SignedOperationRoot,
};
use crate::operation::{
    BoundOperation, CrdtSubject, OperationLedger, OperationReceipt, RecordOutcome,
};
use crate::store::backend::{create_dir_all_durable, fsync_dir};
use crate::types::NodeId;

const FILE_NAME: &str = "operation-roots.log";
// Keep this outside the root-log directory so removing that directory cannot
// silently turn a previously issuing Authority into a fresh one.
const ISSUED_MARKER_NAME: &str = "operation-roots.used";
const TEMP_FILE_NAME: &str = "operation-roots.log.compacting";
const MAGIC: &[u8; 8] = b"ASTOR001";
const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootPath {
    pub operation: BoundOperation,
    pub proof: InclusionProof,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IssuedOperationRoot {
    /// The caller-assigned issuance slot within Authority, scope and keyset.
    pub position: u64,
    /// Epoch at which the Authority keyset was trusted for new issuance.
    pub issued_epoch: u64,
    pub signed_root: SignedOperationRoot,
    /// One exact admitted operation and inclusion path for every tree leaf.
    pub paths: Vec<RootPath>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactRootPath {
    pub subject: CrdtSubject,
    pub commitment: [u8; 32],
    pub proof: InclusionProof,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactIssuedOperationRoot {
    pub position: u64,
    pub issued_epoch: u64,
    pub signed_root: SignedOperationRoot,
    pub paths: Vec<CompactRootPath>,
}

impl From<&IssuedOperationRoot> for CompactIssuedOperationRoot {
    fn from(root: &IssuedOperationRoot) -> Self {
        Self {
            position: root.position,
            issued_epoch: root.issued_epoch,
            signed_root: root.signed_root.clone(),
            paths: root
                .paths
                .iter()
                .map(|path| CompactRootPath {
                    subject: path.operation.operation.subject.clone(),
                    commitment: path.operation.commitment(),
                    proof: path.proof.clone(),
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
enum RootRecord {
    Full(IssuedOperationRoot),
    Compact(CompactIssuedOperationRoot),
}

impl RootRecord {
    fn identity(&self) -> CompactIssuedOperationRoot {
        match self {
            Self::Full(root) => root.into(),
            Self::Compact(root) => root.clone(),
        }
    }

    fn position(&self) -> u64 {
        match self {
            Self::Full(root) => root.position,
            Self::Compact(root) => root.position,
        }
    }
}

#[derive(Debug, Error)]
pub enum RootLogError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("operation root log is already locked: {0}")]
    Lock(#[from] std::fs::TryLockError),
    #[error(transparent)]
    Merkle(#[from] OperationMerkleError),
    #[error("invalid operation root log: {0}")]
    Invalid(String),
    #[error("Authority already issued a different root at this position")]
    Equivocation,
}

fn invalid(message: impl Into<String>) -> RootLogError {
    RootLogError::Invalid(message.into())
}

/// Trusted state supplied by the host, never learned from the persisted root.
pub struct RootLogVerifier<'a> {
    pub authority_id: &'a NodeId,
    pub scope: &'a OperationScope,
    pub registry: &'a KeysetRegistry,
    pub epoch_config: &'a EpochConfig,
    pub current_epoch: u64,
    pub admitted: &'a OperationLedger,
}

impl RootLogVerifier<'_> {
    fn verify_header(
        &self,
        root: &SignedOperationRoot,
        issued_epoch: u64,
        path_count: usize,
    ) -> Result<(), RootLogError> {
        if &root.authority_id != self.authority_id || &root.scope != self.scope {
            return Err(invalid(
                "root Authority or scope differs from trusted configuration",
            ));
        }
        if issued_epoch > self.current_epoch {
            return Err(invalid("root issuance epoch is in the future"));
        }
        let version = &root.keyset_version;
        if self
            .registry
            .registered_epoch(version)
            .is_none_or(|registered| registered > issued_epoch)
            || !self
                .registry
                .is_version_valid(version, issued_epoch, self.epoch_config)
        {
            return Err(invalid("root keyset was not valid at issuance"));
        }
        let key = self
            .registry
            .get_key_for_authority(version, self.authority_id)
            .ok_or_else(|| invalid("root Authority key absent from trusted keyset"))?;
        key.verify(
            &root.message_bytes(),
            &Signature::from_bytes(&root.signature),
        )
        .map_err(|_| invalid("invalid Authority root signature"))?;
        if root.leaf_count == 0 || root.leaf_count as usize != path_count {
            return Err(invalid("root path count differs from signed leaf count"));
        }
        Ok(())
    }

    pub fn verify(&self, issued: &IssuedOperationRoot) -> Result<(), RootLogError> {
        let root = &issued.signed_root;
        self.verify_header(root, issued.issued_epoch, issued.paths.len())?;
        let mut subjects = BTreeSet::<CrdtSubject>::new();
        let mut positions = BTreeSet::<u64>::new();
        for path in &issued.paths {
            if !self.scope.contains(&path.operation)
                || self.admitted.get(&path.operation.operation.subject) != Some(&path.operation)
                || path.proof.leaf_count != root.leaf_count
                || !path.proof.verify(&path.operation, &root.root)
            {
                return Err(invalid(
                    "root path lacks exact admitted operation or valid proof",
                ));
            }
            if !subjects.insert(path.operation.operation.subject.clone())
                || !positions.insert(path.proof.leaf_index)
            {
                return Err(invalid("duplicate root subject or leaf position"));
            }
        }
        Ok(())
    }

    pub fn verify_compact(&self, issued: &CompactIssuedOperationRoot) -> Result<(), RootLogError> {
        let root = &issued.signed_root;
        self.verify_header(root, issued.issued_epoch, issued.paths.len())?;
        let mut subjects = BTreeSet::new();
        let mut positions = BTreeSet::new();
        for path in &issued.paths {
            let receipt = self
                .admitted
                .receipt(&path.subject)
                .cloned()
                .or_else(|| self.admitted.get(&path.subject).map(OperationReceipt::from))
                .ok_or_else(|| invalid("compact root lacks durable admitted receipt"))?;
            if receipt.commitment != path.commitment
                || receipt.subject != path.subject
                || receipt.subject.namespace_id != self.scope.namespace_id
                || !receipt.declaration.key.starts_with(&self.scope.key_prefix)
                || path.proof.leaf_count != root.leaf_count
                || !path
                    .proof
                    .verify_commitment(&path.subject, &path.commitment, &root.root)
            {
                return Err(invalid(
                    "compact root path lacks exact admitted receipt or valid proof",
                ));
            }
            if !subjects.insert(path.subject.clone()) || !positions.insert(path.proof.leaf_index) {
                return Err(invalid("duplicate root subject or leaf position"));
            }
        }
        Ok(())
    }
}

/// Single-owner append log. Keep the file lock for the process lifetime.
pub struct OperationRootLog {
    file: File,
    path: PathBuf,
    issued_marker: PathBuf,
    // A scope and keyset are fixed by the verifier for one log instance.
    positions: BTreeMap<u64, RootRecord>,
    poisoned: bool,
}

impl OperationRootLog {
    pub fn open(
        dir: impl AsRef<Path>,
        verifier: &RootLogVerifier<'_>,
    ) -> Result<Self, RootLogError> {
        let dir = dir.as_ref();
        create_dir_all_durable(dir)?;
        let path = dir.join(FILE_NAME);
        let issued_marker = dir
            .parent()
            .ok_or_else(|| invalid("root log has no parent directory"))?
            .join(ISSUED_MARKER_NAME);
        let fresh = !path.exists();
        if fresh && issued_marker.exists() {
            return Err(invalid("previously issued root log is missing"));
        }
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        file.try_lock()?;
        if fresh {
            file.write_all(MAGIC)?;
            file.sync_all()?;
            fsync_dir(dir)?;
        } else if !issued_marker.exists() && file.metadata()?.len() < MAGIC.len() as u64 {
            // A crash during the very first MAGIC write can leave only a
            // prefix. No root can have been issued before the marker exists.
            let mut prefix = Vec::new();
            file.read_to_end(&mut prefix)?;
            let only_log = fs::read_dir(dir)?.collect::<io::Result<Vec<_>>>()?;
            if !MAGIC.starts_with(&prefix) || only_log.len() != 1 || only_log[0].path() != path {
                return Err(invalid("incomplete operation root log"));
            }
            file.set_len(0)?;
            file.seek(SeekFrom::Start(0))?;
            file.write_all(MAGIC)?;
            file.sync_all()?;
            fsync_dir(dir)?;
        }
        let temp = dir.join(TEMP_FILE_NAME);
        if temp.exists() {
            fs::remove_file(temp)?;
            fsync_dir(dir)?;
        }
        let positions = recover(&mut file, verifier)?;
        // An earlier writer may have published a complete frame but failed
        // while syncing it. Reopening must make every accepted frame durable
        // before another root can be issued from these recovered positions.
        file.sync_all()?;
        if !positions.is_empty() {
            persist_issued_marker(&issued_marker)?;
        }
        file.seek(SeekFrom::End(0))?;
        Ok(Self {
            file,
            path,
            issued_marker,
            positions,
            poisoned: false,
        })
    }

    /// Build a root only from previously admitted tokens, then commit the
    /// signature and every path before returning it to the caller. The
    /// admission ledger must outlive this log and retain exact operation IDs.
    pub fn issue_and_sync(
        &mut self,
        position: u64,
        scope: OperationScope,
        operations: &[SignableCommitment],
        signer: &NodeSigner,
        keyset_version: KeysetVersion,
        verifier: &RootLogVerifier<'_>,
    ) -> Result<(IssuedOperationRoot, RecordOutcome), RootLogError> {
        if self.poisoned {
            return Err(invalid("root log must be reopened after uncertain write"));
        }
        if signer.node_id() != verifier.authority_id || scope != *verifier.scope {
            return Err(invalid(
                "signer or tree scope differs from trusted configuration",
            ));
        }
        let tree = OperationMerkleTree::from_signable(scope, operations)?;
        let paths = operations
            .iter()
            .map(|token| {
                Ok(RootPath {
                    operation: token.bound().clone(),
                    proof: tree.proof(token.subject())?,
                })
            })
            .collect::<Result<Vec<_>, OperationMerkleError>>()?;
        let issued = IssuedOperationRoot {
            position,
            issued_epoch: verifier.current_epoch,
            signed_root: signer.sign_operation_root(&tree, keyset_version),
            paths,
        };
        if let Some(existing) = self.positions.get(&position) {
            return if existing.identity() == CompactIssuedOperationRoot::from(&issued) {
                Ok((issued, RecordOutcome::Retry))
            } else {
                Err(RootLogError::Equivocation)
            };
        }
        verifier.verify(&issued)?;
        let payload = serde_json::to_vec(&issued).map_err(|e| invalid(e.to_string()))?;
        if payload.is_empty() || payload.len() > MAX_FRAME_BYTES {
            return Err(invalid("root frame exceeds size limit"));
        }
        persist_issued_marker(&self.issued_marker)?;
        let length = (payload.len() as u32).to_be_bytes();
        let mut frame = Vec::with_capacity(12 + payload.len());
        frame.extend_from_slice(&length);
        frame.extend_from_slice(&crc32fast::hash(&length).to_be_bytes());
        frame.extend_from_slice(&crc32fast::hash(&payload).to_be_bytes());
        frame.extend_from_slice(&payload);
        self.poisoned = true;
        self.file.write_all(&frame)?;
        self.file.sync_all()?;
        self.positions
            .insert(position, RootRecord::Full(issued.clone()));
        self.poisoned = false;
        Ok((issued, RecordOutcome::New))
    }

    pub fn get(&self, position: u64) -> Option<&IssuedOperationRoot> {
        match self.positions.get(&position)? {
            RootRecord::Full(root) => Some(root),
            RootRecord::Compact(_) => None,
        }
    }

    pub fn issued_roots(&self) -> impl Iterator<Item = &IssuedOperationRoot> {
        self.positions.values().filter_map(|record| match record {
            RootRecord::Full(root) => Some(root),
            RootRecord::Compact(_) => None,
        })
    }

    /// All issued positions, including roots whose value-bearing paths were reclaimed.
    pub fn issued_identities(&self) -> impl Iterator<Item = CompactIssuedOperationRoot> + '_ {
        self.positions.values().map(RootRecord::identity)
    }

    /// Rewrite selected roots after their admitted bodies have durable receipts.
    /// The caller must first persist every support that still needs full content.
    pub fn compact_receipted_and_sync(
        &mut self,
        receipts: &[OperationReceipt],
        verifier: &RootLogVerifier<'_>,
    ) -> Result<usize, RootLogError> {
        if self.poisoned {
            return Err(invalid("root log must be reopened after uncertain write"));
        }
        let receipts: BTreeMap<_, _> = receipts.iter().map(|r| (&r.subject, r)).collect();
        let mut next = self.positions.clone();
        let mut count = 0;
        for (position, record) in &self.positions {
            let RootRecord::Full(root) = record else {
                continue;
            };
            if !root
                .paths
                .iter()
                .all(|path| receipts.contains_key(&path.operation.operation.subject))
            {
                continue;
            }
            verifier.verify(root)?;
            for path in &root.paths {
                let receipt = receipts[&path.operation.operation.subject];
                if *receipt != OperationReceipt::from(&path.operation) {
                    return Err(invalid(
                        "root compaction receipt differs from admitted body",
                    ));
                }
            }
            let compact = CompactIssuedOperationRoot::from(root);
            // The ledger may still hold bodies until its own checkpoint. Check
            // paths here; recovery additionally requires durable receipts.
            next.insert(*position, RootRecord::Compact(compact));
            count += 1;
        }
        if count == 0 {
            return Ok(0);
        }
        let dir = self
            .path
            .parent()
            .ok_or_else(|| invalid("root log has no directory"))?;
        let temp = dir.join(TEMP_FILE_NAME);
        self.poisoned = true;
        let mut replacement = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&temp)?;
        replacement.try_lock()?;
        replacement.write_all(MAGIC)?;
        for record in next.values() {
            let payload = serde_json::to_vec(record).map_err(|e| invalid(e.to_string()))?;
            if payload.is_empty() || payload.len() > MAX_FRAME_BYTES {
                return Err(invalid("root frame exceeds size limit"));
            }
            let length = (payload.len() as u32).to_be_bytes();
            replacement.write_all(&length)?;
            replacement.write_all(&crc32fast::hash(&length).to_be_bytes())?;
            replacement.write_all(&crc32fast::hash(&payload).to_be_bytes())?;
            replacement.write_all(&payload)?;
        }
        replacement.sync_all()?;
        fs::rename(&temp, &self.path)?;
        fsync_dir(dir)?;
        self.file = replacement;
        self.positions = next;
        self.poisoned = false;
        Ok(count)
    }

    pub fn len(&self) -> usize {
        self.positions.len()
    }

    /// Whether the durable root log has no issued positions.
    pub fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }
}

fn persist_issued_marker(path: &Path) -> Result<(), RootLogError> {
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    file.sync_all()?;
    fsync_dir(
        path.parent()
            .ok_or_else(|| invalid("root issuance marker has no parent directory"))?,
    )?;
    Ok(())
}

fn recover(
    file: &mut File,
    verifier: &RootLogVerifier<'_>,
) -> Result<BTreeMap<u64, RootRecord>, RootLogError> {
    let mut bytes = Vec::new();
    file.seek(SeekFrom::Start(0))?;
    file.read_to_end(&mut bytes)?;
    if !bytes.starts_with(MAGIC) {
        return Err(invalid("operation root log magic mismatch"));
    }
    let mut index = MAGIC.len();
    let mut positions = BTreeMap::new();
    while index < bytes.len() {
        let remaining = &bytes[index..];
        if remaining.len() < 12 {
            break;
        }
        let length = u32::from_be_bytes(remaining[..4].try_into().unwrap()) as usize;
        let header_crc = u32::from_be_bytes(remaining[4..8].try_into().unwrap());
        if crc32fast::hash(&remaining[..4]) != header_crc || length == 0 || length > MAX_FRAME_BYTES
        {
            return Err(invalid("root frame header corrupt"));
        }
        if remaining.len() < length + 12 {
            break;
        }
        let payload = &remaining[12..length + 12];
        let payload_crc = u32::from_be_bytes(remaining[8..12].try_into().unwrap());
        if crc32fast::hash(payload) != payload_crc {
            return Err(invalid("root frame checksum mismatch"));
        }
        let issued: RootRecord = serde_json::from_slice(payload)
            .map_err(|e| invalid(format!("root frame decode: {e}")))?;
        match &issued {
            RootRecord::Full(root) => verifier.verify(root)?,
            RootRecord::Compact(root) => verifier.verify_compact(root)?,
        }
        match positions.insert(issued.position(), issued.clone()) {
            Some(previous) if previous.identity() != issued.identity() => {
                return Err(RootLogError::Equivocation);
            }
            _ => {}
        }
        index += length + 12;
    }
    if index < bytes.len() {
        file.set_len(index as u64)?;
        file.sync_all()?;
    }
    Ok(positions)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authority::operation_admission::{OperationAdmissionPolicy, admit_for_signing};
    use crate::hlc::HlcTimestamp;
    use crate::operation::{
        CandidateKvAction, CertificationMode, Declaration, EventDot, OperationLeaf,
    };
    use tempfile::tempdir;

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

    fn operation(id: u64) -> BoundOperation {
        let subject = CrdtSubject {
            namespace_id: "ns".into(),
            event_dot: EventDot {
                writer: NodeId("writer".into()),
                counter: id,
            },
        };
        let key = format!("key/{id}");
        BoundOperation {
            operation: OperationLeaf {
                subject: subject.clone(),
                action: CandidateKvAction::Create {
                    key: key.clone(),
                    value_bytes: format!("value-{id}").into_bytes(),
                    timestamp: HlcTimestamp {
                        physical: 1,
                        logical: 0,
                        node_id: "writer".into(),
                    },
                },
            },
            declaration: Declaration {
                creation_id: subject,
                key,
                mode: CertificationMode::Enabled,
            },
        }
    }

    fn scope() -> OperationScope {
        OperationScope {
            namespace_id: "ns".into(),
            key_prefix: "key/".into(),
        }
    }

    fn signer() -> NodeSigner {
        let id = NodeId("authority".into());
        #[cfg(feature = "native-crypto")]
        {
            NodeSigner::from_seed(id, &[7; 32], false)
        }
        #[cfg(not(feature = "native-crypto"))]
        {
            NodeSigner::from_seed(id, &[7; 32])
        }
    }

    fn keyset(signer: &NodeSigner) -> KeysetRegistry {
        let mut registry = KeysetRegistry::new();
        registry
            .register_keyset(
                KeysetVersion(1),
                0,
                vec![(signer.node_id().clone(), signer.verifying_key())],
            )
            .unwrap();
        registry
    }

    fn verifier<'a>(
        signer: &'a NodeSigner,
        scope: &'a OperationScope,
        registry: &'a KeysetRegistry,
        epoch_config: &'a EpochConfig,
        admitted: &'a OperationLedger,
    ) -> RootLogVerifier<'a> {
        RootLogVerifier {
            authority_id: signer.node_id(),
            scope,
            registry,
            epoch_config,
            current_epoch: 0,
            admitted,
        }
    }

    #[test]
    fn interrupted_first_log_magic_is_repaired_without_issued_roots() {
        let dir = tempdir().unwrap();
        let ledger = OperationLedger::open(dir.path().join("admitted")).unwrap();
        let signer = signer();
        let registry = keyset(&signer);
        let scope = scope();
        let config = EpochConfig::default();
        let trust = verifier(&signer, &scope, &registry, &config, &ledger);
        for prefix_len in [0, 1, MAGIC.len() - 1] {
            let root_dir = dir.path().join(format!("roots-{prefix_len}"));
            create_dir_all_durable(&root_dir).unwrap();
            let log_path = root_dir.join(FILE_NAME);
            fs::write(&log_path, &MAGIC[..prefix_len]).unwrap();
            File::open(&log_path).unwrap().sync_all().unwrap();
            fsync_dir(&root_dir).unwrap();
            let log = OperationRootLog::open(&root_dir, &trust).unwrap();
            assert!(log.positions.is_empty());
            assert_eq!(fs::read(&log_path).unwrap(), MAGIC);
            drop(log);
            OperationRootLog::open(&root_dir, &trust).unwrap();
        }
    }

    #[test]
    fn incomplete_log_magic_with_other_saved_state_is_rejected() {
        let dir = tempdir().unwrap();
        let ledger = OperationLedger::open(dir.path().join("admitted")).unwrap();
        let signer = signer();
        let registry = keyset(&signer);
        let scope = scope();
        let config = EpochConfig::default();
        let trust = verifier(&signer, &scope, &registry, &config, &ledger);
        let root_dir = dir.path().join("roots");
        create_dir_all_durable(&root_dir).unwrap();
        fs::write(root_dir.join(FILE_NAME), &MAGIC[..3]).unwrap();
        let other = root_dir.join("other-saved-state");
        fs::write(&other, b"state").unwrap();
        assert!(OperationRootLog::open(&root_dir, &trust).is_err());
        fs::remove_file(other).unwrap();
        persist_issued_marker(&dir.path().join(ISSUED_MARKER_NAME)).unwrap();
        assert!(OperationRootLog::open(&root_dir, &trust).is_err());
    }

    #[test]
    fn issued_root_and_paths_survive_restart_and_reject_equivocation() {
        let dir = tempdir().unwrap();
        let mut ledger = OperationLedger::open(dir.path().join("admitted")).unwrap();
        let tokens = [operation(1), operation(2)]
            .into_iter()
            .map(|body| admit_for_signing(&Allow, &mut ledger, body).unwrap())
            .collect::<Vec<_>>();
        let signer = signer();
        let registry = keyset(&signer);
        let scope = scope();
        let config = EpochConfig::default();
        let trust = verifier(&signer, &scope, &registry, &config, &ledger);
        let root_dir = dir.path().join("roots");
        let first = {
            let mut log = OperationRootLog::open(&root_dir, &trust).unwrap();
            let (issued, outcome) = log
                .issue_and_sync(4, scope.clone(), &tokens, &signer, KeysetVersion(1), &trust)
                .unwrap();
            assert_eq!(outcome, RecordOutcome::New);
            assert_eq!(issued.paths.len(), 2);
            let (_, retry) = log
                .issue_and_sync(4, scope.clone(), &tokens, &signer, KeysetVersion(1), &trust)
                .unwrap();
            assert_eq!(retry, RecordOutcome::Retry);
            assert!(matches!(
                log.issue_and_sync(
                    4,
                    scope.clone(),
                    &tokens[..1],
                    &signer,
                    KeysetVersion(1),
                    &trust
                ),
                Err(RootLogError::Equivocation)
            ));
            issued
        };
        let reopened = OperationRootLog::open(&root_dir, &trust).unwrap();
        assert_eq!(reopened.get(4), Some(&first));
        assert_eq!(reopened.len(), 1);
    }

    #[test]
    fn missing_issued_root_log_is_not_recreated() {
        let dir = tempdir().unwrap();
        let mut ledger = OperationLedger::open(dir.path().join("admitted")).unwrap();
        let token = admit_for_signing(&Allow, &mut ledger, operation(1)).unwrap();
        let signer = signer();
        let registry = keyset(&signer);
        let scope = scope();
        let config = EpochConfig::default();
        let trust = verifier(&signer, &scope, &registry, &config, &ledger);
        let root_dir = dir.path().join("roots");
        // An empty, never-used log may be reopened.
        drop(OperationRootLog::open(&root_dir, &trust).unwrap());
        fs::remove_file(root_dir.join(FILE_NAME)).unwrap();
        let mut log = OperationRootLog::open(&root_dir, &trust).unwrap();
        log.issue_and_sync(
            0,
            scope.clone(),
            std::slice::from_ref(&token),
            &signer,
            KeysetVersion(1),
            &trust,
        )
        .unwrap();
        drop(log);

        fs::remove_dir_all(&root_dir).unwrap();
        assert!(matches!(
            OperationRootLog::open(&root_dir, &trust),
            Err(RootLogError::Invalid(message)) if message.contains("previously issued root log is missing")
        ));
    }

    #[test]
    fn recovery_rejects_missing_admission_and_corrupt_complete_frame() {
        let dir = tempdir().unwrap();
        let mut ledger = OperationLedger::open(dir.path().join("admitted")).unwrap();
        let token = admit_for_signing(&Allow, &mut ledger, operation(1)).unwrap();
        let signer = signer();
        let registry = keyset(&signer);
        let scope = scope();
        let config = EpochConfig::default();
        let trust = verifier(&signer, &scope, &registry, &config, &ledger);
        let root_dir = dir.path().join("roots");
        {
            let mut log = OperationRootLog::open(&root_dir, &trust).unwrap();
            log.issue_and_sync(
                0,
                scope.clone(),
                std::slice::from_ref(&token),
                &signer,
                KeysetVersion(1),
                &trust,
            )
            .unwrap();
        }
        let empty = OperationLedger::open(dir.path().join("empty-admitted")).unwrap();
        let wrong_admission = verifier(&signer, &scope, &registry, &config, &empty);
        assert!(matches!(
            OperationRootLog::open(&root_dir, &wrong_admission),
            Err(RootLogError::Invalid(_))
        ));
        let mut wrong_registry = KeysetRegistry::new();
        wrong_registry
            .register_keyset(
                KeysetVersion(1),
                0,
                vec![(
                    signer.node_id().clone(),
                    ed25519_dalek::SigningKey::from_bytes(&[8; 32]).verifying_key(),
                )],
            )
            .unwrap();
        let wrong_keyset = verifier(&signer, &scope, &wrong_registry, &config, &ledger);
        assert!(matches!(
            OperationRootLog::open(&root_dir, &wrong_keyset),
            Err(RootLogError::Invalid(_))
        ));
        let path = root_dir.join(FILE_NAME);
        let mut bytes = std::fs::read(&path).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        std::fs::write(&path, bytes).unwrap();
        assert!(matches!(
            OperationRootLog::open(&root_dir, &trust),
            Err(RootLogError::Invalid(_))
        ));
    }

    #[test]
    fn torn_final_frame_is_truncated_before_next_issue() {
        let dir = tempdir().unwrap();
        let mut ledger = OperationLedger::open(dir.path().join("admitted")).unwrap();
        let token = admit_for_signing(&Allow, &mut ledger, operation(1)).unwrap();
        let signer = signer();
        let registry = keyset(&signer);
        let scope = scope();
        let config = EpochConfig::default();
        let trust = verifier(&signer, &scope, &registry, &config, &ledger);
        let root_dir = dir.path().join("roots");
        {
            let mut log = OperationRootLog::open(&root_dir, &trust).unwrap();
            log.issue_and_sync(
                0,
                scope.clone(),
                std::slice::from_ref(&token),
                &signer,
                KeysetVersion(1),
                &trust,
            )
            .unwrap();
        }
        let path = root_dir.join(FILE_NAME);
        let complete_len = std::fs::metadata(&path).unwrap().len();
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&[0, 0, 1])
            .unwrap();
        let mut log = OperationRootLog::open(&root_dir, &trust).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), complete_len);
        log.issue_and_sync(
            1,
            scope.clone(),
            &[token],
            &signer,
            KeysetVersion(1),
            &trust,
        )
        .unwrap();
        drop(log);
        assert_eq!(OperationRootLog::open(&root_dir, &trust).unwrap().len(), 2);
    }

    #[test]
    fn compact_root_preserves_identity_and_rechecks_admitted_receipt() {
        let dir = tempdir().unwrap();
        let admitted_dir = dir.path().join("admitted");
        let mut ledger = OperationLedger::open(&admitted_dir).unwrap();
        let token = admit_for_signing(&Allow, &mut ledger, operation(1)).unwrap();
        let receipt = OperationReceipt::from(token.bound());
        let signer = signer();
        let registry = keyset(&signer);
        let scope = scope();
        let config = EpochConfig::default();
        let root_dir = dir.path().join("roots");
        let identity = {
            let trust = verifier(&signer, &scope, &registry, &config, &ledger);
            let mut log = OperationRootLog::open(&root_dir, &trust).unwrap();
            log.issue_and_sync(
                0,
                scope.clone(),
                std::slice::from_ref(&token),
                &signer,
                KeysetVersion(1),
                &trust,
            )
            .unwrap();
            assert_eq!(
                log.compact_receipted_and_sync(std::slice::from_ref(&receipt), &trust)
                    .unwrap(),
                1
            );
            assert_eq!(
                log.compact_receipted_and_sync(std::slice::from_ref(&receipt), &trust)
                    .unwrap(),
                0
            );
            assert!(log.get(0).is_none());
            assert_eq!(
                log.issue_and_sync(
                    0,
                    scope.clone(),
                    std::slice::from_ref(&token),
                    &signer,
                    KeysetVersion(1),
                    &trust
                )
                .unwrap()
                .1,
                RecordOutcome::Retry
            );
            log.issued_identities().next().unwrap()
        };
        assert!(
            !std::fs::read(root_dir.join(FILE_NAME))
                .unwrap()
                .windows(b"value-1".len())
                .any(|w| w == b"value-1")
        );
        ledger.compact_and_sync(&receipt.subject).unwrap();
        ledger.checkpoint_and_sync().unwrap();
        drop(ledger);
        let ledger = OperationLedger::open(&admitted_dir).unwrap();
        let trust = verifier(&signer, &scope, &registry, &config, &ledger);
        let reopened = OperationRootLog::open(&root_dir, &trust).unwrap();
        assert_eq!(reopened.issued_identities().next(), Some(identity.clone()));
        assert_eq!(reopened.len(), 1);
        drop(reopened);
        let empty = OperationLedger::open(dir.path().join("empty")).unwrap();
        let wrong = verifier(&signer, &scope, &registry, &config, &empty);
        assert!(matches!(
            OperationRootLog::open(&root_dir, &wrong),
            Err(RootLogError::Invalid(_))
        ));
    }

    #[test]
    fn uncommitted_root_replacement_is_discarded() {
        let dir = tempdir().unwrap();
        let mut ledger = OperationLedger::open(dir.path().join("admitted")).unwrap();
        let token = admit_for_signing(&Allow, &mut ledger, operation(1)).unwrap();
        let signer = signer();
        let registry = keyset(&signer);
        let scope = scope();
        let config = EpochConfig::default();
        let trust = verifier(&signer, &scope, &registry, &config, &ledger);
        let root_dir = dir.path().join("roots");
        {
            let mut log = OperationRootLog::open(&root_dir, &trust).unwrap();
            log.issue_and_sync(
                0,
                scope.clone(),
                &[token],
                &signer,
                KeysetVersion(1),
                &trust,
            )
            .unwrap();
        }
        std::fs::write(root_dir.join(TEMP_FILE_NAME), b"partial replacement").unwrap();
        let reopened = OperationRootLog::open(&root_dir, &trust).unwrap();
        assert_eq!(reopened.issued_roots().count(), 1);
        assert!(!root_dir.join(TEMP_FILE_NAME).exists());
    }
}
