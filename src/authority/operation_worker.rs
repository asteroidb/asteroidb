//! Static-scope Authority issuance for signed operation packets.
//!
//! One worker owns a durable admission ledger and root log. It replays the
//! inbox on startup; a root already synced at a recovered position is reused
//! without signing its subject again. Root delivery and quorum collection
//! are separate protocol steps.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::authority::certificate::{EpochConfig, KeysetRegistry, KeysetVersion};
use crate::authority::creation_policy::CreationPolicyManifest;
use crate::authority::frontier_sig::NodeSigner;
#[cfg(test)]
use crate::authority::operation_admission::admit_signed_for_authority_with_creation_policy;
#[cfg(test)]
use crate::authority::operation_admission::admit_signed_for_authority_with_policy_witness;
use crate::authority::operation_admission::{
    AdmissionError, SignedAdmissionError, admit_signed_for_authority_with_policy_witness_or_compact,
};
use crate::authority::operation_merkle::{OperationRootSupport, OperationScope};
use crate::authority::operation_root_log::{
    IssuedOperationRoot, OperationRootLog, RootLogError, RootLogVerifier,
};
use crate::authority::writer_auth::WriterRegistry;
use crate::control_plane::system_namespace::SystemNamespace;
use crate::network::operation_certificate_transport::{scope_applies_to_key, static_policy_digest};
use crate::network::operation_support_transport::SupportPacket;
use crate::network::operation_transport::OperationInbox;
use crate::network::operation_transport::OperationPacket;
use crate::operation::{
    CertificationMode, CrdtSubject, OperationError, OperationLedger, OperationReceipt,
    PersistOperationError,
};
use crate::store::backend::{create_dir_all_durable, fsync_dir};

const CREATION_POLICY_ANCHOR: &str = "creation-policy-anchor.bin";
const CREATION_POLICY_ANCHOR_TEMP: &str = ".creation-policy-anchor.tmp";
const CREATION_POLICY_ANCHOR_MAGIC: &[u8; 8] = b"ASTCP001";
const SIGNING_TRUST_ANCHOR: &str = "signing-trust-anchor.bin";
const SIGNING_TRUST_ANCHOR_TEMP: &str = ".signing-trust-anchor.tmp";
const SIGNING_TRUST_ANCHOR_MAGIC: &[u8; 8] = b"ASTST001";
const AUTHORITY_LOCK: &str = "authority.lock";
const AUTHORITY_USED_MAGIC: &[u8] = b"ASTAUTH-USED-1";

fn authority_used_marker(dir: &Path) -> Result<PathBuf, OperationWorkerError> {
    let parent = dir
        .parent()
        .ok_or_else(|| OperationWorkerError::Static("Authority has no parent directory".into()))?;
    let parent = if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    };
    let name = dir
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            OperationWorkerError::Static("Authority has no UTF-8 directory name".into())
        })?;
    Ok(parent.join(format!(".{name}.operation-authority.used")))
}

#[derive(Debug, Error)]
pub enum OperationWorkerError {
    #[error("Authority signing intake failed: {0}")]
    Inbox(#[from] io::Error),
    #[error("Authority operation ledger failed: {0:?}")]
    Ledger(PersistOperationError),
    #[error("Authority directory is already locked: {0}")]
    Lock(#[from] std::fs::TryLockError),
    #[error(transparent)]
    RootLog(#[from] RootLogError),
    #[error("Authority operation admission failed: {0:?}")]
    Admission(SignedAdmissionError),
    #[error("invalid static Authority operation configuration: {0}")]
    Static(String),
    #[error("operation root position counter exhausted")]
    PositionExhausted,
}

impl From<PersistOperationError> for OperationWorkerError {
    fn from(error: PersistOperationError) -> Self {
        Self::Ledger(error)
    }
}

pub struct OperationAuthorityWorker {
    _lock: File,
    dir: PathBuf,
    namespace: SystemNamespace,
    namespace_id: String,
    scope: OperationScope,
    signer: Arc<NodeSigner>,
    writers: Arc<WriterRegistry>,
    creation_policy: Option<Arc<CreationPolicyManifest>>,
    keyset: Arc<KeysetRegistry>,
    keyset_version: KeysetVersion,
    epoch_config: EpochConfig,
    ledger: OperationLedger,
    roots: OperationRootLog,
    issued_by_subject: BTreeMap<CrdtSubject, u64>,
    next_position: u64,
    reported_rejections: BTreeSet<(CrdtSubject, [u8; 32])>,
}

/// Immutable inputs needed to reconstruct an Authority after an uncertain
/// ledger or root-log write. Drop the old worker before calling `open` so its
/// root-log file lock is released.
#[derive(Clone)]
pub struct OperationAuthorityRecovery {
    dir: PathBuf,
    namespace: SystemNamespace,
    namespace_id: String,
    scope: OperationScope,
    signer: Arc<NodeSigner>,
    writers: Arc<WriterRegistry>,
    creation_policy: Option<Arc<CreationPolicyManifest>>,
    keyset: Arc<KeysetRegistry>,
    keyset_version: KeysetVersion,
}

impl OperationAuthorityRecovery {
    pub fn open(&self) -> Result<OperationAuthorityWorker, OperationWorkerError> {
        OperationAuthorityWorker::open_with_creation_policy(
            &self.dir,
            self.namespace.clone(),
            self.namespace_id.clone(),
            self.scope.clone(),
            Arc::clone(&self.signer),
            Arc::clone(&self.writers),
            self.creation_policy.clone(),
            Arc::clone(&self.keyset),
            self.keyset_version.clone(),
        )
    }
}

impl OperationAuthorityWorker {
    /// Open against immutable trusted inputs. This worker intentionally fixes
    /// issuance at epoch zero and one scope/keyset version. The host must
    /// maintain those inputs consistently across restarts and Authorities.
    #[allow(clippy::too_many_arguments)]
    pub fn open(
        dir: impl AsRef<Path>,
        namespace: SystemNamespace,
        namespace_id: String,
        scope: OperationScope,
        signer: Arc<NodeSigner>,
        writers: Arc<WriterRegistry>,
        keyset: Arc<KeysetRegistry>,
        keyset_version: KeysetVersion,
    ) -> Result<Self, OperationWorkerError> {
        Self::open_with_creation_policy(
            dir,
            namespace,
            namespace_id,
            scope,
            signer,
            writers,
            None,
            keyset,
            keyset_version,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn open_with_creation_policy(
        dir: impl AsRef<Path>,
        namespace: SystemNamespace,
        namespace_id: String,
        scope: OperationScope,
        signer: Arc<NodeSigner>,
        writers: Arc<WriterRegistry>,
        creation_policy: Option<Arc<CreationPolicyManifest>>,
        keyset: Arc<KeysetRegistry>,
        keyset_version: KeysetVersion,
    ) -> Result<Self, OperationWorkerError> {
        if scope.namespace_id != namespace_id {
            return Err(OperationWorkerError::Static(
                "scope and namespace differ".into(),
            ));
        }
        if keyset.current_version() != keyset_version
            || keyset.registered_epoch(&keyset_version) != Some(0)
            || keyset.get_key_for_authority(&keyset_version, signer.node_id())
                != Some(&signer.verifying_key())
        {
            return Err(OperationWorkerError::Static(
                "signer is absent from static epoch-zero keyset".into(),
            ));
        }
        let policy_digest = static_policy_digest(&namespace, &scope, &keyset)
            .map_err(OperationWorkerError::Static)?;
        let dir = dir.as_ref();
        let used_marker = authority_used_marker(dir)?;
        match fs::read(&used_marker) {
            Ok(bytes) if bytes == AUTHORITY_USED_MAGIC => {
                if !dir.join("admitted").is_dir()
                    || !dir.join("roots").is_dir()
                    || !dir.join(CREATION_POLICY_ANCHOR).is_file()
                    || !dir.join(SIGNING_TRUST_ANCHOR).is_file()
                {
                    return Err(OperationWorkerError::Static(
                        "previously used Authority state is missing".into(),
                    ));
                }
            }
            Ok(_) => {
                return Err(OperationWorkerError::Static(
                    "invalid Authority used marker".into(),
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(OperationWorkerError::Inbox(error)),
        }
        create_dir_all_durable(dir).map_err(OperationWorkerError::Inbox)?;
        let lock_path = dir.join(AUTHORITY_LOCK);
        let fresh_lock = !lock_path.exists();
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)
            .map_err(OperationWorkerError::Inbox)?;
        lock.try_lock()?;
        if fresh_lock {
            lock.sync_all().map_err(OperationWorkerError::Inbox)?;
            fsync_dir(dir).map_err(OperationWorkerError::Inbox)?;
        }
        pin_creation_policy(dir, creation_policy.as_deref())?;
        pin_signing_trust(dir, policy_digest, &writers, &keyset)?;
        let ledger = OperationLedger::open(dir.join("admitted"))?;
        let epoch_config = EpochConfig::default();
        let verifier = RootLogVerifier {
            authority_id: signer.node_id(),
            scope: &scope,
            registry: &keyset,
            epoch_config: &epoch_config,
            current_epoch: 0,
            admitted: &ledger,
        };
        let roots = OperationRootLog::open(dir.join("roots"), &verifier)?;
        let mut issued_by_subject = BTreeMap::new();
        let mut next_position = 0u64;
        for root in roots.issued_identities() {
            if root.paths.len() != 1 || root.position != next_position {
                return Err(OperationWorkerError::Static(
                    "root log has non-singleton or noncontiguous positions".into(),
                ));
            }
            let subject = root.paths[0].subject.clone();
            let key = ledger
                .get(&subject)
                .map(|bound| bound.declaration.key.as_str())
                .or_else(|| {
                    ledger
                        .receipt(&subject)
                        .map(|receipt| receipt.declaration.key.as_str())
                })
                .ok_or_else(|| {
                    OperationWorkerError::Static("issued root lacks admitted operation".into())
                })?;
            if !scope_applies_to_key(&namespace, &scope, key) {
                return Err(OperationWorkerError::Static(
                    "root log contains an operation governed by another scope".into(),
                ));
            }
            if issued_by_subject.insert(subject, root.position).is_some() {
                return Err(OperationWorkerError::Static(
                    "subject was signed in multiple roots".into(),
                ));
            }
            next_position = next_position
                .checked_add(1)
                .ok_or(OperationWorkerError::PositionExhausted)?;
        }
        if !used_marker.exists() {
            let mut marker = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&used_marker)
                .map_err(OperationWorkerError::Inbox)?;
            marker
                .write_all(AUTHORITY_USED_MAGIC)
                .map_err(OperationWorkerError::Inbox)?;
            marker.sync_all().map_err(OperationWorkerError::Inbox)?;
            fsync_dir(used_marker.parent().expect("marker has parent"))
                .map_err(OperationWorkerError::Inbox)?;
        }
        Ok(Self {
            _lock: lock,
            dir: dir.to_path_buf(),
            namespace,
            namespace_id,
            scope,
            signer,
            writers,
            creation_policy,
            keyset,
            keyset_version,
            epoch_config,
            ledger,
            roots,
            issued_by_subject,
            next_position,
            reported_rejections: BTreeSet::new(),
        })
    }

    pub fn recovery_config(&self) -> OperationAuthorityRecovery {
        OperationAuthorityRecovery {
            dir: self.dir.clone(),
            namespace: self.namespace.clone(),
            namespace_id: self.namespace_id.clone(),
            scope: self.scope.clone(),
            signer: Arc::clone(&self.signer),
            writers: Arc::clone(&self.writers),
            creation_policy: self.creation_policy.clone(),
            keyset: Arc::clone(&self.keyset),
            keyset_version: self.keyset_version.clone(),
        }
    }

    /// Reconcile all locally held signed packets. Only enabled operations in
    /// the configured scope are candidates. A rejected packet cannot stall
    /// independent operations; persistence and trust failures stop the pass.
    pub fn reconcile(&mut self, inbox: &mut OperationInbox) -> Result<(), OperationWorkerError> {
        inbox.recover_if_poisoned()?;
        self.reconcile_packets(inbox.packets(), inbox)
    }

    /// In routed custody, transit packets are not Authority signing work.
    /// Only local owner submissions and terminal Authority intake are eligible.
    pub fn reconcile_routed(
        &mut self,
        inbox: &mut OperationInbox,
    ) -> Result<(), OperationWorkerError> {
        inbox.recover_if_poisoned()?;
        let mut packets = inbox.root_packets()?;
        let signing = inbox.signing_transfers()?;
        packets.extend(signing.iter().map(|transfer| transfer.packet.clone()));
        self.reconcile_packets(packets, inbox)?;
        self.seed_verified_local_issuance(inbox)?;
        let mut retired = false;
        for transfer in &signing {
            let bound = &transfer.packet.signed.bound;
            let subject = &bound.operation.subject;
            let Some(position) = self.issued_by_subject.get(subject) else {
                continue;
            };
            if self.ledger.get(subject) != Some(bound)
                && self.ledger.receipt(subject) != Some(&OperationReceipt::from(bound))
            {
                continue;
            }
            // The root log is durable before the inbox intake can disappear.
            // Compact roots retain this exact subject and bound commitment.
            let exact_root = self.roots.issued_identities().any(|root| {
                root.position == *position
                    && root.paths.len() == 1
                    && root.paths[0].subject == *subject
                    && root.paths[0].commitment == bound.commitment()
            });
            if !exact_root {
                return Err(OperationWorkerError::Static(
                    "issued signing intake lacks exact Authority root".into(),
                ));
            }
            retired |= inbox.retire_issued_signing_intake(transfer)?;
        }
        if retired {
            inbox.reclaim_retired_payloads()?;
        }
        Ok(())
    }

    /// Rebuild the inbox's volatile local-signing witness from the durable,
    /// verified root log. Startup calls this before routed relay recovery;
    /// each signing pass repeats it after newly issued roots are fsynced.
    pub fn seed_verified_local_issuance(
        &self,
        inbox: &mut OperationInbox,
    ) -> Result<(), OperationWorkerError> {
        for root in self.roots.issued_identities() {
            let Some(path) = root.paths.as_slice().first() else {
                return Err(OperationWorkerError::Static(
                    "issued root lacks an operation path".into(),
                ));
            };
            if root.paths.len() != 1
                || self.issued_by_subject.get(&path.subject) != Some(&root.position)
                || self
                    .ledger
                    .get(&path.subject)
                    .map(|bound| bound.commitment())
                    .or_else(|| {
                        self.ledger
                            .receipt(&path.subject)
                            .map(|receipt| receipt.commitment)
                    })
                    != Some(path.commitment)
            {
                return Err(OperationWorkerError::Static(
                    "issued root differs from admitted operation".into(),
                ));
            }
            inbox.note_verified_local_issuance(path.subject.clone(), path.commitment)?;
        }
        Ok(())
    }

    fn reconcile_packets(
        &mut self,
        packets: Vec<OperationPacket>,
        inbox: &OperationInbox,
    ) -> Result<(), OperationWorkerError> {
        for packet in packets {
            let bound = &packet.signed.bound;
            if !self.scope.contains(bound)
                || !scope_applies_to_key(&self.namespace, &self.scope, &bound.declaration.key)
                || bound.declaration.mode != CertificationMode::Enabled
            {
                continue;
            }
            let subject = &bound.operation.subject;
            if self.issued_by_subject.contains_key(subject)
                && (self.ledger.get(subject) == Some(bound)
                    || self.ledger.receipt(subject) == Some(&OperationReceipt::from(bound)))
            {
                // This root and its admitted choice are already durable.
                // Replaying the inbox every tick must not append a new WAL
                // frame for the same signed operation forever.
                continue;
            }
            let creation_witness =
                inbox.policy_witness_packet(&subject.namespace_id, &bound.declaration.key);
            let creation_marker = inbox
                .verified_compact_policy_witness(&subject.namespace_id, &bound.declaration.key);
            let token = match admit_signed_for_authority_with_policy_witness_or_compact(
                &self.namespace,
                self.signer.node_id(),
                &self.namespace_id,
                &self.writers,
                self.creation_policy.as_deref(),
                &mut self.ledger,
                &packet.signed,
                creation_witness.as_ref().map(|packet| &packet.signed),
                creation_marker.as_ref(),
            ) {
                Ok(token) => token,
                Err(error) if is_packet_rejection(&error) => {
                    if self
                        .reported_rejections
                        .insert((bound.operation.subject.clone(), bound.commitment()))
                    {
                        eprintln!(
                            "warning: operation Authority rejected subject {:?}: {error:?}",
                            bound.operation.subject
                        );
                    }
                    continue;
                }
                Err(error) => return Err(OperationWorkerError::Admission(error)),
            };
            let subject = token.subject().clone();
            if self.issued_by_subject.contains_key(&subject) {
                continue;
            }
            let next_after = self
                .next_position
                .checked_add(1)
                .ok_or(OperationWorkerError::PositionExhausted)?;
            let verifier = RootLogVerifier {
                authority_id: self.signer.node_id(),
                scope: &self.scope,
                registry: &self.keyset,
                epoch_config: &self.epoch_config,
                current_epoch: 0,
                admitted: &self.ledger,
            };
            let (root, _) = self.roots.issue_and_sync(
                self.next_position,
                self.scope.clone(),
                &[token],
                &self.signer,
                self.keyset_version.clone(),
                &verifier,
            )?;
            self.issued_by_subject.insert(subject, root.position);
            self.next_position = next_after;
        }
        Ok(())
    }

    pub fn issued_roots(&self) -> impl Iterator<Item = &IssuedOperationRoot> {
        self.roots.issued_roots()
    }

    /// Compacted roots whose admission checkpoint may still need to remove
    /// an older value-bearing WAL segment after a crash.
    pub fn retired_receipts(&self) -> Result<Vec<OperationReceipt>, OperationWorkerError> {
        self.roots
            .issued_identities()
            .filter(|root| self.roots.get(root.position).is_none())
            .map(|root| {
                let subject = &root.paths[0].subject;
                self.ledger
                    .receipt(subject)
                    .cloned()
                    .or_else(|| self.ledger.get(subject).map(OperationReceipt::from))
                    .ok_or_else(|| {
                        OperationWorkerError::Static(
                            "compacted root lacks admitted operation".into(),
                        )
                    })
            })
            .collect()
    }

    /// Retire issued root bodies after the caller has proved every required
    /// support send is durably acknowledged under a fixed neighbor manifest.
    /// Root rewriting precedes admission-ledger receipts so recovery always
    /// has either a full root or a compact root bound to the admitted choice.
    pub fn compact_issued_receipted_and_sync(
        &mut self,
        receipts: &[OperationReceipt],
    ) -> Result<usize, OperationWorkerError> {
        let verifier = RootLogVerifier {
            authority_id: self.signer.node_id(),
            scope: &self.scope,
            registry: &self.keyset,
            epoch_config: &self.epoch_config,
            current_epoch: 0,
            admitted: &self.ledger,
        };
        let compacted = self.roots.compact_receipted_and_sync(receipts, &verifier)?;
        let mut retired_seen = false;
        for receipt in receipts {
            let Some(position) = self.issued_by_subject.get(&receipt.subject) else {
                continue;
            };
            if self.roots.get(*position).is_some() {
                continue;
            }
            let admitted = self
                .ledger
                .get(&receipt.subject)
                .map(OperationReceipt::from)
                .or_else(|| self.ledger.receipt(&receipt.subject).cloned());
            if admitted.as_ref() != Some(receipt) {
                return Err(OperationWorkerError::Static(
                    "root compaction receipt differs from admitted operation".into(),
                ));
            }
            self.ledger.compact_and_sync(&receipt.subject)?;
            retired_seen = true;
        }
        // Repeat the checkpoint for an already compact root too: a crash can
        // occur after persisting its ledger receipt but before old WAL body
        // segments are removed.
        if retired_seen {
            self.ledger.checkpoint_and_sync()?;
        }
        Ok(compacted)
    }

    /// Durable support packets ready for authenticated delivery. Rebuilds
    /// directly from the recovered root log, so a crash cannot erase an
    /// issued support before its recipient acknowledges it.
    pub fn issued_supports(&self) -> impl Iterator<Item = SupportPacket> + '_ {
        self.roots.issued_roots().flat_map(|issued| {
            issued.paths.iter().map(|path| SupportPacket {
                operation: path.operation.clone(),
                support: OperationRootSupport {
                    signed_root: issued.signed_root.clone(),
                    proof: path.proof.clone(),
                },
            })
        })
    }

    pub fn issued_for(&self, subject: &CrdtSubject) -> Option<&IssuedOperationRoot> {
        self.issued_by_subject
            .get(subject)
            .and_then(|position| self.roots.get(*position))
    }
}

fn is_packet_rejection(error: &SignedAdmissionError) -> bool {
    match error {
        SignedAdmissionError::Writer(_) | SignedAdmissionError::CreationPolicy(_) => true,
        SignedAdmissionError::Admission(AdmissionError::Store(
            PersistOperationError::Operation(OperationError::ConflictingContent(_)),
        )) => true,
        SignedAdmissionError::Admission(AdmissionError::Store(_)) => false,
        SignedAdmissionError::Admission(_) => true,
    }
}

/// A root cannot be replayed under a different interpretation of its
/// creation declaration. Pin the exact manifest before opening the ledger or
/// issuing a signature. A legacy worker has its own distinct anchor value.
fn pin_creation_policy(
    dir: &Path,
    policy: Option<&CreationPolicyManifest>,
) -> Result<(), OperationWorkerError> {
    create_dir_all_durable(dir).map_err(OperationWorkerError::Inbox)?;
    let mut expected = Vec::with_capacity(CREATION_POLICY_ANCHOR_MAGIC.len() + 33);
    expected.extend_from_slice(CREATION_POLICY_ANCHOR_MAGIC);
    match policy {
        Some(policy) => {
            expected.push(1);
            expected.extend_from_slice(&policy.digest());
        }
        None => {
            expected.push(0);
            expected.extend_from_slice(&[0; 32]);
        }
    }
    let anchor = dir.join(CREATION_POLICY_ANCHOR);
    if anchor.exists() {
        if fs::read(&anchor).map_err(OperationWorkerError::Inbox)? != expected {
            return Err(OperationWorkerError::Static(
                "Authority creation policy changed since first use".into(),
            ));
        }
        File::open(&anchor)
            .and_then(|file| file.sync_all())
            .map_err(OperationWorkerError::Inbox)?;
        let temp = dir.join(CREATION_POLICY_ANCHOR_TEMP);
        if temp.exists() {
            fs::remove_file(temp).map_err(OperationWorkerError::Inbox)?;
        }
        fsync_dir(dir).map_err(OperationWorkerError::Inbox)?;
        return Ok(());
    }
    for entry in fs::read_dir(dir).map_err(OperationWorkerError::Inbox)? {
        let name = entry.map_err(OperationWorkerError::Inbox)?.file_name();
        if name != CREATION_POLICY_ANCHOR_TEMP && name != AUTHORITY_LOCK {
            return Err(OperationWorkerError::Static(
                "existing Authority state has no creation policy anchor".into(),
            ));
        }
    }
    let temp = dir.join(CREATION_POLICY_ANCHOR_TEMP);
    if temp.exists() {
        fs::remove_file(&temp).map_err(OperationWorkerError::Inbox)?;
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)
        .map_err(OperationWorkerError::Inbox)?;
    file.write_all(&expected)
        .map_err(OperationWorkerError::Inbox)?;
    file.sync_all().map_err(OperationWorkerError::Inbox)?;
    fs::rename(&temp, &anchor).map_err(OperationWorkerError::Inbox)?;
    fsync_dir(dir).map_err(OperationWorkerError::Inbox)?;
    Ok(())
}

/// Freeze the full static trust inputs before ledger recovery can expose a
/// signable commitment. The root signature itself does not encode deployment
/// policy or writer grants, so a restart must not silently reinterpret them.
fn pin_signing_trust(
    dir: &Path,
    policy_digest: [u8; 32],
    writers: &WriterRegistry,
    keyset: &KeysetRegistry,
) -> Result<(), OperationWorkerError> {
    let mut hash = Sha256::new();
    hash.update(b"asteroidb/authority-signing-trust/v1");
    hash.update(policy_digest);
    hash.update(writers.fingerprint());
    hash.update(keyset.current_version().0.to_be_bytes());
    let versions: Vec<_> = keyset.ed25519_keysets().collect();
    hash.update((versions.len() as u64).to_be_bytes());
    for (version, epoch, keys) in versions {
        hash.update(version.0.to_be_bytes());
        hash.update(epoch.to_be_bytes());
        let mut keys = keys.iter().collect::<Vec<_>>();
        keys.sort_by(|(left, _), (right, _)| left.0.cmp(&right.0));
        hash.update((keys.len() as u64).to_be_bytes());
        for (id, key) in keys {
            hash.update((id.0.len() as u64).to_be_bytes());
            hash.update(id.0.as_bytes());
            hash.update(key.as_bytes());
        }
    }
    let mut expected = Vec::from(SIGNING_TRUST_ANCHOR_MAGIC.as_slice());
    expected.extend_from_slice(&hash.finalize());
    let anchor = dir.join(SIGNING_TRUST_ANCHOR);
    if anchor.exists() {
        if fs::read(&anchor).map_err(OperationWorkerError::Inbox)? != expected {
            return Err(OperationWorkerError::Static(
                "Authority signing trust changed since first use".into(),
            ));
        }
        File::open(&anchor)
            .and_then(|file| file.sync_all())
            .map_err(OperationWorkerError::Inbox)?;
        let temp = dir.join(SIGNING_TRUST_ANCHOR_TEMP);
        if temp.exists() {
            fs::remove_file(temp).map_err(OperationWorkerError::Inbox)?;
        }
        fsync_dir(dir).map_err(OperationWorkerError::Inbox)?;
        return Ok(());
    }
    for entry in fs::read_dir(dir).map_err(OperationWorkerError::Inbox)? {
        let name = entry.map_err(OperationWorkerError::Inbox)?.file_name();
        if name != CREATION_POLICY_ANCHOR
            && name != AUTHORITY_LOCK
            && name != SIGNING_TRUST_ANCHOR_TEMP
        {
            return Err(OperationWorkerError::Static(
                "existing Authority state has no signing trust anchor".into(),
            ));
        }
    }
    let temp = dir.join(SIGNING_TRUST_ANCHOR_TEMP);
    if temp.exists() {
        fs::remove_file(&temp).map_err(OperationWorkerError::Inbox)?;
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)
        .map_err(OperationWorkerError::Inbox)?;
    file.write_all(&expected)
        .map_err(OperationWorkerError::Inbox)?;
    file.sync_all().map_err(OperationWorkerError::Inbox)?;
    fs::rename(&temp, &anchor).map_err(OperationWorkerError::Inbox)?;
    fsync_dir(dir).map_err(OperationWorkerError::Inbox)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use std::collections::BTreeSet;
    use tempfile::tempdir;

    use crate::authority::writer_auth::{SignedBoundOperation, SignedCreation};
    use crate::control_plane::system_namespace::AuthorityDefinition;
    use crate::hlc::HlcTimestamp;
    use crate::network::operation_transport::{AuthenticatedOperationAdmission, OperationPacket};
    use crate::network::operation_transport::{OperationRetirementNotice, OperationTransfer};
    use crate::operation::{
        BoundOperation, CandidateKvAction, Declaration, EventDot, OperationLeaf,
    };
    use crate::placement::PlacementPolicy;
    use crate::types::{KeyRange, NodeId, PolicyVersion};

    fn signer() -> Arc<NodeSigner> {
        #[cfg(feature = "native-crypto")]
        {
            Arc::new(NodeSigner::from_seed(
                NodeId("authority".into()),
                &[9; 32],
                false,
            ))
        }
        #[cfg(not(feature = "native-crypto"))]
        {
            Arc::new(NodeSigner::from_seed(NodeId("authority".into()), &[9; 32]))
        }
    }

    fn namespace() -> SystemNamespace {
        let mut ns = SystemNamespace::new();
        ns.set_placement_policy(
            PlacementPolicy::new(
                PolicyVersion(1),
                KeyRange {
                    prefix: "key/".into(),
                },
                1,
            )
            .with_certified(true),
        )
        .unwrap();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: KeyRange {
                prefix: "key/".into(),
            },
            authority_nodes: vec![NodeId("authority".into())],
            auto_generated: false,
        });
        ns
    }

    fn packet(id: u64, mode: CertificationMode, writer: &SigningKey) -> OperationPacket {
        let subject = CrdtSubject {
            namespace_id: "ns".into(),
            event_dot: EventDot {
                writer: NodeId("writer".into()),
                counter: id,
            },
        };
        let key = format!("key/{id}");
        let bound = BoundOperation {
            operation: OperationLeaf {
                subject: subject.clone(),
                action: CandidateKvAction::Create {
                    key: key.clone(),
                    value_bytes: b"value".to_vec(),
                    timestamp: HlcTimestamp {
                        physical: id,
                        logical: 0,
                        node_id: "writer".into(),
                    },
                },
            },
            declaration: Declaration {
                creation_id: subject,
                key,
                mode,
            },
        };
        let creation = SignedCreation::sign(&bound, writer).unwrap();
        OperationPacket {
            signed: SignedBoundOperation::sign(bound, writer, creation).unwrap(),
        }
    }

    fn inputs(
        writer: &SigningKey,
        signer: &NodeSigner,
    ) -> (Arc<WriterRegistry>, Arc<KeysetRegistry>) {
        let mut writers = WriterRegistry::new();
        writers
            .register(
                NodeId("writer".into()),
                writer.verifying_key(),
                BTreeSet::from(["ns".into()]),
            )
            .unwrap();
        let mut keyset = KeysetRegistry::new();
        keyset
            .register_keyset(
                KeysetVersion(1),
                0,
                vec![(signer.node_id().clone(), signer.verifying_key())],
            )
            .unwrap();
        (Arc::new(writers), Arc::new(keyset))
    }

    fn open_worker(
        dir: &Path,
        signer: Arc<NodeSigner>,
        writers: Arc<WriterRegistry>,
        keyset: Arc<KeysetRegistry>,
    ) -> OperationAuthorityWorker {
        OperationAuthorityWorker::open(
            dir,
            namespace(),
            "ns".into(),
            OperationScope {
                namespace_id: "ns".into(),
                key_prefix: "key/".into(),
            },
            signer,
            writers,
            keyset,
            KeysetVersion(1),
        )
        .unwrap()
    }

    fn admitted_wal_bytes(worker_dir: &Path) -> u64 {
        crate::store::wal::list_segments(&worker_dir.join("admitted"))
            .unwrap()
            .iter()
            .map(|(_, path)| fs::metadata(path).unwrap().len())
            .sum()
    }

    #[test]
    fn authority_directory_is_locked_before_ledger_recovery() {
        let dir = tempdir().unwrap();
        let writer = SigningKey::from_bytes(&[3; 32]);
        let signer = signer();
        let (writers, keyset) = inputs(&writer, &signer);
        let worker = open_worker(dir.path(), signer.clone(), writers.clone(), keyset.clone());
        let wal_path = crate::store::wal::list_segments(&dir.path().join("admitted"))
            .unwrap()
            .last()
            .unwrap()
            .1
            .clone();
        OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap()
            .write_all(&[0xff])
            .unwrap();
        let length_before = fs::metadata(&wal_path).unwrap().len();
        let second = OperationAuthorityWorker::open(
            dir.path(),
            namespace(),
            "ns".into(),
            OperationScope {
                namespace_id: "ns".into(),
                key_prefix: "key/".into(),
            },
            signer.clone(),
            writers.clone(),
            keyset.clone(),
            KeysetVersion(1),
        );
        assert!(matches!(second, Err(OperationWorkerError::Lock(_))));
        assert_eq!(fs::metadata(&wal_path).unwrap().len(), length_before);
        drop(worker);
        open_worker(dir.path(), signer, writers, keyset);
        assert!(fs::metadata(&wal_path).unwrap().len() < length_before);
    }

    #[test]
    fn rejected_packet_does_not_block_later_authority_roots() {
        let dir = tempdir().unwrap();
        let writer = SigningKey::from_bytes(&[3; 32]);
        let signer = signer();
        let (writers, keyset) = inputs(&writer, &signer);
        let mut inbox = OperationInbox::open(
            dir.path().join("inbox"),
            Arc::new(AuthenticatedOperationAdmission::new(
                writers.clone(),
                "ns".into(),
            )),
        )
        .unwrap();
        let rejected = packet(1, CertificationMode::Enabled, &writer);
        let accepted = packet(2, CertificationMode::Enabled, &writer);
        let rejected_subject = rejected.signed.bound.operation.subject.clone();
        let accepted_subject = accepted.signed.bound.operation.subject.clone();
        inbox.receive(rejected.clone()).unwrap();
        inbox.receive(accepted).unwrap();

        let worker_dir = dir.path().join("worker");
        let mut worker = open_worker(&worker_dir, signer.clone(), writers.clone(), keyset.clone());
        // The Authority already locked another body for this ID. Its own
        // immutable choice must survive, while the independent later packet
        // can still receive a root in the same pass.
        let mut locked = rejected.signed.bound;
        if let CandidateKvAction::Create { value_bytes, .. } = &mut locked.operation.action {
            *value_bytes = b"other choice".to_vec();
        }
        worker.ledger.record_and_sync(locked).unwrap();
        worker.reconcile(&mut inbox).unwrap();
        assert!(worker.issued_for(&rejected_subject).is_none());
        assert_eq!(worker.issued_for(&accepted_subject).unwrap().position, 0);
        drop(worker);

        let mut worker = open_worker(&worker_dir, signer, writers, keyset);
        worker.reconcile(&mut inbox).unwrap();
        assert!(worker.issued_for(&rejected_subject).is_none());
        assert_eq!(worker.issued_roots().count(), 1);
    }

    #[test]
    fn issued_signing_intake_retires_only_after_fence_and_certified_receipt() {
        let dir = tempdir().unwrap();
        let writer = SigningKey::from_bytes(&[3; 32]);
        let signer = signer();
        let (writers, keyset) = inputs(&writer, &signer);
        let admission = Arc::new(AuthenticatedOperationAdmission::new(
            writers.clone(),
            "ns".into(),
        ));
        let inbox_dir = dir.path().join("inbox");
        let mut inbox = OperationInbox::open(&inbox_dir, admission.clone()).unwrap();
        let packet = packet(1, CertificationMode::Enabled, &writer);
        let subject = packet.signed.bound.operation.subject.clone();
        let transfer = OperationTransfer {
            version: 2,
            sender: NodeId("relay".into()),
            transfer_id: 0,
            receiver: NodeId("authority".into()),
            destination: Some(NodeId("authority".into())),
            parent: None,
            depth: 0,
            packet,
        };
        inbox
            .receive_transfer(transfer.clone(), &NodeId("authority".into()))
            .unwrap();
        inbox.mark_signing_intake(&transfer).unwrap();
        let second = OperationTransfer {
            sender: NodeId("other-relay".into()),
            packet: transfer.packet.clone(),
            ..transfer.clone()
        };
        inbox
            .receive_transfer(second.clone(), &NodeId("authority".into()))
            .unwrap();
        inbox.mark_signing_intake(&second).unwrap();
        let worker_dir = dir.path().join("worker");
        let mut worker = open_worker(&worker_dir, signer.clone(), writers.clone(), keyset.clone());
        worker.reconcile_routed(&mut inbox).unwrap();
        assert!(worker.issued_for(&subject).is_some());
        assert_eq!(inbox.signing_transfers().unwrap().len(), 2);

        inbox
            .save_retirement_fence(
                &OperationRetirementNotice {
                    version: 1,
                    sender: transfer.sender.clone(),
                    receiver: transfer.receiver.clone(),
                    closed_before: 1,
                },
                &transfer.receiver,
            )
            .unwrap();
        worker.reconcile_routed(&mut inbox).unwrap();
        assert_eq!(inbox.signing_transfers().unwrap().len(), 2);

        // Production creates this receipt only after verifying a certificate.
        // Write the ledger receipt directly here to isolate the intake gate.
        drop(inbox);
        let mut ledger = OperationLedger::open(&inbox_dir).unwrap();
        assert!(ledger.compact_and_sync(&subject).unwrap());
        ledger.checkpoint_and_sync().unwrap();
        drop(ledger);
        let mut inbox = OperationInbox::open(&inbox_dir, admission.clone()).unwrap();
        worker.reconcile_routed(&mut inbox).unwrap();
        assert_eq!(inbox.signing_transfers().unwrap(), vec![second.clone()]);
        assert!(inbox.get_packet(&subject).is_some());
        inbox
            .save_retirement_fence(
                &OperationRetirementNotice {
                    version: 1,
                    sender: second.sender.clone(),
                    receiver: second.receiver.clone(),
                    closed_before: 1,
                },
                &second.receiver,
            )
            .unwrap();
        worker.reconcile_routed(&mut inbox).unwrap();
        assert!(inbox.signing_transfers().unwrap().is_empty());
        assert!(inbox.packets().is_empty());
        drop(worker);
        drop(inbox);

        let mut inbox = OperationInbox::open(&inbox_dir, admission).unwrap();
        let mut worker = open_worker(&worker_dir, signer, writers, keyset);
        worker.reconcile_routed(&mut inbox).unwrap();
        assert!(inbox.signing_transfers().unwrap().is_empty());
        assert!(inbox.packets().is_empty());
        assert!(worker.issued_by_subject.contains_key(&subject));
    }

    #[test]
    fn reconciles_after_restart_without_resigning_subject() {
        let dir = tempdir().unwrap();
        let writer = SigningKey::from_bytes(&[3; 32]);
        let signer = signer();
        let (writers, keyset) = inputs(&writer, &signer);
        let mut inbox = OperationInbox::open(
            dir.path().join("inbox"),
            Arc::new(AuthenticatedOperationAdmission::new(
                writers.clone(),
                "ns".into(),
            )),
        )
        .unwrap();
        let first = packet(1, CertificationMode::Enabled, &writer);
        let first_subject = first.signed.bound.operation.subject.clone();
        inbox.receive(first).unwrap();
        inbox
            .receive(packet(3, CertificationMode::Disabled, &writer))
            .unwrap();
        let worker_dir = dir.path().join("worker");
        let original = {
            let mut worker =
                open_worker(&worker_dir, signer.clone(), writers.clone(), keyset.clone());
            worker.reconcile(&mut inbox).unwrap();
            assert_eq!(worker.issued_roots().count(), 1);
            let issued = worker.issued_for(&first_subject).unwrap().clone();
            assert_eq!(issued.position, 0);
            let supports = worker.issued_supports().collect::<Vec<_>>();
            assert_eq!(supports.len(), 1);
            assert_eq!(supports[0].operation.operation.subject, first_subject);
            assert!(supports[0].support.proof.verify(
                &supports[0].operation,
                &supports[0].support.signed_root.root
            ));
            let admitted_bytes = admitted_wal_bytes(&worker_dir);
            worker.reconcile(&mut inbox).unwrap();
            assert_eq!(worker.issued_roots().count(), 1);
            assert_eq!(admitted_wal_bytes(&worker_dir), admitted_bytes);
            issued
        };
        let mut worker = open_worker(&worker_dir, signer.clone(), writers.clone(), keyset.clone());
        let admitted_bytes = admitted_wal_bytes(&worker_dir);
        worker.reconcile(&mut inbox).unwrap();
        assert_eq!(worker.issued_for(&first_subject), Some(&original));
        assert_eq!(admitted_wal_bytes(&worker_dir), admitted_bytes);

        let recovery = worker.recovery_config();
        drop(worker);
        let mut worker = recovery.open().unwrap();
        let reopened_bytes = admitted_wal_bytes(&worker_dir);
        worker.reconcile(&mut inbox).unwrap();
        assert_eq!(worker.issued_for(&first_subject), Some(&original));
        assert_eq!(admitted_wal_bytes(&worker_dir), reopened_bytes);

        let mut conflicting = packet(1, CertificationMode::Enabled, &writer).signed.bound;
        if let CandidateKvAction::Create { value_bytes, .. } = &mut conflicting.operation.action {
            *value_bytes = b"conflicting value".to_vec();
        }
        let creation = SignedCreation::sign(&conflicting, &writer).unwrap();
        let conflicting = OperationPacket {
            signed: SignedBoundOperation::sign(conflicting, &writer, creation).unwrap(),
        };
        worker
            .reconcile_packets(vec![conflicting.clone()], &inbox)
            .unwrap();
        assert!(
            worker
                .reported_rejections
                .contains(&(first_subject.clone(), conflicting.signed.bound.commitment()))
        );
        assert_eq!(admitted_wal_bytes(&worker_dir), reopened_bytes);
        inbox
            .receive(packet(2, CertificationMode::Enabled, &writer))
            .unwrap();
        worker.reconcile(&mut inbox).unwrap();
        assert_eq!(
            worker
                .issued_roots()
                .map(|root| root.position)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        drop(worker);
        assert_eq!(
            open_worker(&worker_dir, signer, writers, keyset)
                .issued_roots()
                .count(),
            2
        );
    }

    #[test]
    fn routed_issuance_witness_requires_verified_root_after_restart() {
        let dir = tempdir().unwrap();
        let writer = SigningKey::from_bytes(&[3; 32]);
        let signer = signer();
        let (writers, keyset) = inputs(&writer, &signer);
        let admission = Arc::new(AuthenticatedOperationAdmission::new(
            writers.clone(),
            "ns".into(),
        ));
        let inbox_dir = dir.path().join("inbox");
        let worker_dir = dir.path().join("worker");
        let operation = packet(1, CertificationMode::Enabled, &writer);
        let subject = operation.signed.bound.operation.subject.clone();
        let commitment = operation.signed.bound.commitment();
        {
            let mut inbox = OperationInbox::open(&inbox_dir, admission.clone()).unwrap();
            let transfer = OperationTransfer {
                version: 2,
                sender: NodeId("relay".into()),
                transfer_id: 0,
                receiver: NodeId("authority".into()),
                destination: Some(NodeId("authority".into())),
                parent: None,
                depth: 0,
                packet: operation,
            };
            inbox
                .receive_transfer(transfer.clone(), &NodeId("authority".into()))
                .unwrap();
            inbox.mark_signing_intake(&transfer).unwrap();
            let mut worker =
                open_worker(&worker_dir, signer.clone(), writers.clone(), keyset.clone());
            worker.reconcile_routed(&mut inbox).unwrap();
            assert!(worker.issued_for(&subject).is_some());
            assert!(
                inbox
                    .note_verified_local_issuance(subject.clone(), [0; 32])
                    .is_err()
            );
        }

        // Both the worker and inbox discard volatile state on restart. The
        // worker reconstructs only the exact witness found in its root log.
        {
            let mut inbox = OperationInbox::open(&inbox_dir, admission.clone()).unwrap();
            let worker = open_worker(&worker_dir, signer.clone(), writers.clone(), keyset.clone());
            worker.seed_verified_local_issuance(&mut inbox).unwrap();
            assert!(
                inbox
                    .note_verified_local_issuance(subject.clone(), [0; 32])
                    .is_err()
            );
            inbox
                .note_verified_local_issuance(subject.clone(), commitment)
                .unwrap();
        }

        // A lost root log must stop Authority startup. Recreating an empty
        // log would forget an already released root and its signing lock.
        fs::remove_file(worker_dir.join("roots/operation-roots.log")).unwrap();
        assert!(matches!(
            OperationAuthorityWorker::open(
                &worker_dir,
                namespace(),
                "ns".into(),
                OperationScope {
                    namespace_id: "ns".into(),
                    key_prefix: "key/".into(),
                },
                signer,
                writers,
                keyset,
                KeysetVersion(1),
            ),
            Err(OperationWorkerError::RootLog(RootLogError::Invalid(message)))
                if message.contains("previously issued root log is missing")
        ));
    }

    #[test]
    fn missing_used_authority_directory_cannot_restart_empty() {
        let dir = tempdir().unwrap();
        let writer = SigningKey::from_bytes(&[3; 32]);
        let signer = signer();
        let (writers, keyset) = inputs(&writer, &signer);
        let mut inbox = OperationInbox::open(
            dir.path().join("inbox"),
            Arc::new(AuthenticatedOperationAdmission::new(
                writers.clone(),
                "ns".into(),
            )),
        )
        .unwrap();
        inbox
            .receive(packet(1, CertificationMode::Enabled, &writer))
            .unwrap();
        let worker_dir = dir.path().join("worker");
        {
            let mut worker =
                open_worker(&worker_dir, signer.clone(), writers.clone(), keyset.clone());
            worker.reconcile(&mut inbox).unwrap();
            assert_eq!(worker.issued_roots().count(), 1);
        }
        fs::remove_dir_all(&worker_dir).unwrap();
        assert!(matches!(
            OperationAuthorityWorker::open(
                &worker_dir,
                namespace(),
                "ns".into(),
                OperationScope {
                    namespace_id: "ns".into(),
                    key_prefix: "key/".into(),
                },
                signer,
                writers,
                keyset,
                KeysetVersion(1),
            ),
            Err(OperationWorkerError::Static(_))
        ));
    }

    #[test]
    fn compact_followup_is_signed_after_later_create_arrives() {
        let dir = tempdir().unwrap();
        let writer = SigningKey::from_bytes(&[3; 32]);
        let signer = signer();
        let (writers, keyset) = inputs(&writer, &signer);
        let mut inbox = OperationInbox::open(
            dir.path().join("inbox"),
            Arc::new(AuthenticatedOperationAdmission::new(
                writers.clone(),
                "ns".into(),
            )),
        )
        .unwrap();
        let mut create = packet(2, CertificationMode::Enabled, &writer);
        let proof = SignedCreation::sign(&create.signed.bound, &writer).unwrap();
        create.signed =
            SignedBoundOperation::sign(create.signed.bound.clone(), &writer, proof.clone())
                .unwrap();
        let create_subject = create.signed.bound.operation.subject.clone();
        let mut add = create.signed.bound.clone();
        add.operation.subject.event_dot.counter = 1;
        add.operation.action = CandidateKvAction::Add {
            key: add.declaration.key.clone(),
            value_bytes: b"later".to_vec(),
            timestamp: HlcTimestamp {
                physical: 3,
                logical: 0,
                node_id: "writer".into(),
            },
        };
        let add_subject = add.operation.subject.clone();
        let add = OperationPacket {
            signed: SignedBoundOperation::sign(add, &writer, proof).unwrap(),
        };
        inbox.receive(add).unwrap();

        let mut worker = open_worker(&dir.path().join("worker"), signer, writers, keyset);
        worker.reconcile(&mut inbox).unwrap();
        assert!(worker.issued_for(&add_subject).is_none());
        assert!(worker.ledger.get(&add_subject).is_none());

        inbox.receive(create).unwrap();
        worker.reconcile(&mut inbox).unwrap();
        assert!(worker.issued_for(&create_subject).is_some());
        worker.reconcile(&mut inbox).unwrap();
        assert!(worker.issued_for(&add_subject).is_some());
        assert_eq!(worker.issued_roots().count(), 2);
    }

    #[test]
    fn compact_add_uses_policy_only_create_without_signing_create() {
        let dir = tempdir().unwrap();
        let writer = SigningKey::from_bytes(&[3; 32]);
        let signer = signer();
        let (writers, keyset) = inputs(&writer, &signer);
        let admission = Arc::new(AuthenticatedOperationAdmission::new(
            writers.clone(),
            "ns".into(),
        ));
        let inbox_dir = dir.path().join("inbox");
        let mut inbox = OperationInbox::open(&inbox_dir, admission.clone()).unwrap();
        let mut create = packet(2, CertificationMode::Enabled, &writer);
        let proof = SignedCreation::sign(&create.signed.bound, &writer).unwrap();
        create.signed =
            SignedBoundOperation::sign(create.signed.bound.clone(), &writer, proof.clone())
                .unwrap();
        let create_subject = create.signed.bound.operation.subject.clone();
        let mut add = create.signed.bound.clone();
        add.operation.subject.event_dot.counter = 1;
        add.operation.action = CandidateKvAction::Add {
            key: add.declaration.key.clone(),
            value_bytes: b"later".to_vec(),
            timestamp: HlcTimestamp {
                physical: 3,
                logical: 0,
                node_id: "writer".into(),
            },
        };
        let add_subject = add.operation.subject.clone();
        let add = OperationPacket {
            signed: SignedBoundOperation::sign(add, &writer, proof).unwrap(),
        };
        let terminal = OperationTransfer {
            version: 2,
            sender: NodeId("relay".into()),
            transfer_id: 1,
            receiver: NodeId("authority".into()),
            destination: Some(NodeId("authority".into())),
            parent: None,
            depth: 0,
            packet: add.clone(),
        };
        inbox
            .receive_transfer(terminal.clone(), &NodeId("authority".into()))
            .unwrap();
        inbox.mark_signing_intake(&terminal).unwrap();
        let worker_dir = dir.path().join("worker");
        let mut worker = open_worker(&worker_dir, signer.clone(), writers.clone(), keyset.clone());
        worker.reconcile_routed(&mut inbox).unwrap();
        assert!(worker.issued_for(&add_subject).is_none());

        let mut probe = OperationLedger::open(dir.path().join("probe-admission")).unwrap();
        let mut forged = create.signed.clone();
        forged.operation_signature[0] ^= 1;
        assert!(matches!(
            admit_signed_for_authority_with_policy_witness(
                &namespace(),
                signer.node_id(),
                "ns",
                &writers,
                None,
                &mut probe,
                &add.signed,
                Some(&forged),
            ),
            Err(SignedAdmissionError::Writer(_))
        ));
        let mut other_create = create.signed.bound.clone();
        if let CandidateKvAction::Create { value_bytes, .. } = &mut other_create.operation.action {
            *value_bytes = b"other creation".to_vec();
        }
        let other_proof = SignedCreation::sign(&other_create, &writer).unwrap();
        let other_create = SignedBoundOperation::sign(other_create, &writer, other_proof).unwrap();
        assert!(matches!(
            admit_signed_for_authority_with_policy_witness(
                &namespace(),
                signer.node_id(),
                "ns",
                &writers,
                None,
                &mut probe,
                &add.signed,
                Some(&other_create),
            ),
            Err(SignedAdmissionError::Writer(
                crate::authority::writer_auth::WriterAuthError::MissingCreationWitness
            ))
        ));
        assert!(probe.is_empty());

        inbox.receive_policy_witness(create.clone()).unwrap();
        assert!(
            inbox
                .compact_policy_witness("ns", &create.signed.bound.declaration.key)
                .unwrap()
        );
        assert!(
            inbox
                .policy_witness_packet("ns", &create.signed.bound.declaration.key)
                .is_none()
        );
        worker.reconcile_routed(&mut inbox).unwrap();
        assert!(worker.issued_for(&add_subject).is_some());
        assert!(worker.issued_for(&create_subject).is_none());
        assert!(worker.ledger.get(&create_subject).is_none());
        assert_eq!(worker.issued_roots().count(), 1);

        drop(worker);
        drop(inbox);
        let mut inbox = OperationInbox::open(&inbox_dir, admission).unwrap();
        let mut worker = open_worker(&worker_dir, signer, writers, keyset);
        worker.reconcile_routed(&mut inbox).unwrap();
        assert!(worker.issued_for(&add_subject).is_some());
        assert!(worker.ledger.get(&create_subject).is_none());
        assert_eq!(worker.issued_roots().count(), 1);
        let mut later_bound = add.signed.bound.clone();
        later_bound.operation.subject.event_dot.counter = 4;
        let later_subject = later_bound.operation.subject.clone();
        let later = OperationPacket {
            signed: SignedBoundOperation::sign(
                later_bound,
                &writer,
                create.signed.creation.clone(),
            )
            .unwrap(),
        };
        let later_transfer = OperationTransfer {
            version: 2,
            sender: NodeId("relay".into()),
            transfer_id: 2,
            receiver: NodeId("authority".into()),
            destination: Some(NodeId("authority".into())),
            parent: None,
            depth: 0,
            packet: later,
        };
        inbox
            .receive_transfer(later_transfer.clone(), &NodeId("authority".into()))
            .unwrap();
        inbox.mark_signing_intake(&later_transfer).unwrap();
        worker.reconcile_routed(&mut inbox).unwrap();
        assert!(worker.issued_for(&later_subject).is_some());
    }

    #[test]
    fn compact_root_recovery_finishes_ledger_checkpoint_and_preserves_positions() {
        let dir = tempdir().unwrap();
        let writer = SigningKey::from_bytes(&[3; 32]);
        let signer = signer();
        let (writers, keyset) = inputs(&writer, &signer);
        let mut inbox = OperationInbox::open(
            dir.path().join("inbox"),
            Arc::new(AuthenticatedOperationAdmission::new(
                writers.clone(),
                "ns".into(),
            )),
        )
        .unwrap();
        let first = packet(1, CertificationMode::Enabled, &writer);
        let subject = first.signed.bound.operation.subject.clone();
        let receipt = OperationReceipt::from(&first.signed.bound);
        inbox.receive(first).unwrap();
        let worker_dir = dir.path().join("worker");
        {
            let mut worker =
                open_worker(&worker_dir, signer.clone(), writers.clone(), keyset.clone());
            worker.reconcile(&mut inbox).unwrap();
            // Simulate a crash after publishing the compact root and before
            // writing the admission receipt or removing old WAL segments.
            let verifier = RootLogVerifier {
                authority_id: signer.node_id(),
                scope: &worker.scope,
                registry: &worker.keyset,
                epoch_config: &worker.epoch_config,
                current_epoch: 0,
                admitted: &worker.ledger,
            };
            assert_eq!(
                worker
                    .roots
                    .compact_receipted_and_sync(std::slice::from_ref(&receipt), &verifier)
                    .unwrap(),
                1
            );
            assert!(worker.ledger.get(&subject).is_some());
        }
        let mut worker = open_worker(&worker_dir, signer.clone(), writers.clone(), keyset.clone());
        assert_eq!(worker.issued_roots().count(), 0);
        assert_eq!(worker.retired_receipts().unwrap(), vec![receipt.clone()]);
        assert_eq!(
            worker
                .compact_issued_receipted_and_sync(&worker.retired_receipts().unwrap())
                .unwrap(),
            0
        );
        assert!(worker.ledger.get(&subject).is_none());
        assert_eq!(worker.ledger.receipt(&subject), Some(&receipt));
        let admitted_bytes = admitted_wal_bytes(&worker_dir);
        worker.reconcile(&mut inbox).unwrap();
        assert_eq!(worker.issued_roots().count(), 0);
        assert_eq!(admitted_wal_bytes(&worker_dir), admitted_bytes);
        inbox
            .receive(packet(2, CertificationMode::Enabled, &writer))
            .unwrap();
        worker.reconcile(&mut inbox).unwrap();
        assert_eq!(worker.issued_roots().next().unwrap().position, 1);
        drop(worker);
        let worker = open_worker(&worker_dir, signer, writers, keyset);
        assert_eq!(worker.ledger.receipt(&subject), Some(&receipt));
        assert_eq!(worker.issued_roots().next().unwrap().position, 1);
    }

    #[test]
    fn parent_authority_does_not_sign_shadowed_child_key() {
        let dir = tempdir().unwrap();
        let writer = SigningKey::from_bytes(&[3; 32]);
        let signer = signer();
        let (writers, keyset) = inputs(&writer, &signer);
        let mut ns = namespace();
        ns.set_placement_policy(
            PlacementPolicy::new(
                PolicyVersion(1),
                KeyRange {
                    prefix: "key/2".into(),
                },
                1,
            )
            .with_certified(true),
        )
        .unwrap();
        ns.set_authority_definition(AuthorityDefinition {
            key_range: KeyRange {
                prefix: "key/2".into(),
            },
            authority_nodes: vec![NodeId("authority".into())],
            auto_generated: false,
        });
        let mut inbox = OperationInbox::open(
            dir.path().join("inbox"),
            Arc::new(AuthenticatedOperationAdmission::new(
                writers.clone(),
                "ns".into(),
            )),
        )
        .unwrap();
        let parent = packet(1, CertificationMode::Enabled, &writer);
        let child = packet(2, CertificationMode::Enabled, &writer);
        let parent_subject = parent.signed.bound.operation.subject.clone();
        let child_subject = child.signed.bound.operation.subject.clone();
        inbox.receive(parent).unwrap();
        inbox.receive(child).unwrap();
        let mut worker = OperationAuthorityWorker::open(
            dir.path().join("worker"),
            ns,
            "ns".into(),
            OperationScope {
                namespace_id: "ns".into(),
                key_prefix: "key/".into(),
            },
            signer,
            writers,
            keyset,
            KeysetVersion(1),
        )
        .unwrap();
        worker.reconcile(&mut inbox).unwrap();
        assert!(worker.issued_for(&parent_subject).is_some());
        assert!(worker.issued_for(&child_subject).is_none());
    }

    #[test]
    fn recovers_admission_without_issued_root() {
        let dir = tempdir().unwrap();
        let writer = SigningKey::from_bytes(&[3; 32]);
        let signer = signer();
        let (writers, keyset) = inputs(&writer, &signer);
        let mut inbox = OperationInbox::open(
            dir.path().join("inbox"),
            Arc::new(AuthenticatedOperationAdmission::new(
                writers.clone(),
                "ns".into(),
            )),
        )
        .unwrap();
        let first = packet(1, CertificationMode::Enabled, &writer);
        inbox.receive(first.clone()).unwrap();
        let worker_dir = dir.path().join("worker");
        drop(open_worker(
            &worker_dir,
            signer.clone(),
            writers.clone(),
            keyset.clone(),
        ));
        {
            let mut ledger = OperationLedger::open(worker_dir.join("admitted")).unwrap();
            admit_signed_for_authority_with_creation_policy(
                &namespace(),
                signer.node_id(),
                "ns",
                &writers,
                None,
                &mut ledger,
                &first.signed,
            )
            .unwrap();
        }
        let mut worker = open_worker(&worker_dir, signer, writers, keyset);
        worker.reconcile(&mut inbox).unwrap();
        assert_eq!(worker.issued_roots().count(), 1);
        assert_eq!(worker.issued_roots().next().unwrap().position, 0);
    }

    #[test]
    fn enabled_override_is_signed_and_policy_change_cannot_reopen_worker() {
        let dir = tempdir().unwrap();
        let writer = SigningKey::from_bytes(&[3; 32]);
        let signer = signer();
        let (writers, keyset) = inputs(&writer, &signer);
        let policy = Arc::new(CreationPolicyManifest::from_json(
            r#"{"version":1,"ranges":[{"namespace_id":"ns","key_prefix":"key/","default_mode":"Disabled","override_writers":["writer"]}]}"#,
        ).unwrap());
        let mut inbox = OperationInbox::open(
            dir.path().join("inbox"),
            Arc::new(AuthenticatedOperationAdmission::new_with_creation_policy(
                writers.clone(),
                "ns".into(),
                policy.clone(),
            )),
        )
        .unwrap();
        let mut operation = packet(1, CertificationMode::Enabled, &writer);
        let bound = operation.signed.bound.clone();
        let creation =
            SignedCreation::sign_with_policy_digest(&bound, &writer, policy.digest()).unwrap();
        operation.signed = SignedBoundOperation::sign(bound, &writer, creation).unwrap();
        let subject = operation.signed.bound.operation.subject.clone();
        inbox.receive(operation).unwrap();
        let scope = OperationScope {
            namespace_id: "ns".into(),
            key_prefix: "key/".into(),
        };
        let worker_dir = dir.path().join("worker");
        {
            let mut worker = OperationAuthorityWorker::open_with_creation_policy(
                &worker_dir,
                namespace(),
                "ns".into(),
                scope.clone(),
                signer.clone(),
                writers.clone(),
                Some(policy.clone()),
                keyset.clone(),
                KeysetVersion(1),
            )
            .unwrap();
            worker.reconcile(&mut inbox).unwrap();
            assert!(worker.issued_for(&subject).is_some());
        }
        let changed = Arc::new(CreationPolicyManifest::from_json(
            r#"{"version":1,"ranges":[{"namespace_id":"ns","key_prefix":"key/","default_mode":"Enabled","override_writers":[]}]}"#,
        ).unwrap());
        assert!(matches!(
            OperationAuthorityWorker::open_with_creation_policy(
                &worker_dir,
                namespace(),
                "ns".into(),
                scope,
                signer,
                writers,
                Some(changed),
                keyset,
                KeysetVersion(1),
            ),
            Err(OperationWorkerError::Static(_))
        ));
    }

    #[test]
    fn changed_signing_trust_cannot_reopen_existing_authority() {
        let dir = tempdir().unwrap();
        let writer = SigningKey::from_bytes(&[3; 32]);
        let signer = signer();
        let (writers, keyset) = inputs(&writer, &signer);
        let scope = OperationScope {
            namespace_id: "ns".into(),
            key_prefix: "key/".into(),
        };
        let worker_dir = dir.path().join("worker");
        drop(open_worker(
            &worker_dir,
            signer.clone(),
            writers.clone(),
            keyset.clone(),
        ));

        let mut metadata_namespace = namespace();
        metadata_namespace
            .set_placement_policy(
                PlacementPolicy::new(
                    PolicyVersion(2),
                    KeyRange {
                        prefix: "key/".into(),
                    },
                    2,
                )
                .with_certified(true),
            )
            .unwrap();
        drop(
            OperationAuthorityWorker::open(
                &worker_dir,
                metadata_namespace,
                "ns".into(),
                scope.clone(),
                signer.clone(),
                writers.clone(),
                keyset.clone(),
                KeysetVersion(1),
            )
            .unwrap(),
        );

        let mut changed_namespace = namespace();
        changed_namespace
            .set_placement_policy(
                PlacementPolicy::new(
                    PolicyVersion(2),
                    KeyRange {
                        prefix: "key/".into(),
                    },
                    1,
                )
                .with_certified(false),
            )
            .unwrap();
        assert!(matches!(
            OperationAuthorityWorker::open(
                &worker_dir,
                changed_namespace,
                "ns".into(),
                scope.clone(),
                signer.clone(),
                writers.clone(),
                keyset.clone(),
                KeysetVersion(1),
            ),
            Err(OperationWorkerError::Static(_))
        ));

        let mut nested_namespace = namespace();
        nested_namespace
            .set_operation_assignment(
                crate::control_plane::system_namespace::OperationAssignment {
                    namespace_id: "ns".into(),
                    policy: PlacementPolicy::new(
                        PolicyVersion(1),
                        KeyRange {
                            prefix: "key/private/".into(),
                        },
                        1,
                    )
                    .with_certified(false),
                    authority: AuthorityDefinition {
                        key_range: KeyRange {
                            prefix: "key/private/".into(),
                        },
                        authority_nodes: vec![NodeId("authority".into())],
                        auto_generated: false,
                    },
                },
            )
            .unwrap();
        {
            let mut worker = OperationAuthorityWorker::open(
                &worker_dir,
                nested_namespace,
                "ns".into(),
                scope.clone(),
                signer.clone(),
                writers.clone(),
                keyset.clone(),
                KeysetVersion(1),
            )
            .unwrap();
            let mut inbox = OperationInbox::open(
                dir.path().join("inbox"),
                Arc::new(AuthenticatedOperationAdmission::new(
                    writers.clone(),
                    "ns".into(),
                )),
            )
            .unwrap();
            let sibling = packet(1, CertificationMode::Enabled, &writer);
            let sibling_subject = sibling.signed.bound.operation.subject.clone();
            inbox.receive(sibling).unwrap();
            let mut private_bound = packet(2, CertificationMode::Enabled, &writer).signed.bound;
            private_bound.operation.action = CandidateKvAction::Create {
                key: "key/private/2".into(),
                value_bytes: b"value".to_vec(),
                timestamp: HlcTimestamp {
                    physical: 2,
                    logical: 0,
                    node_id: "writer".into(),
                },
            };
            private_bound.declaration.key = "key/private/2".into();
            let private_subject = private_bound.operation.subject.clone();
            let creation = SignedCreation::sign(&private_bound, &writer).unwrap();
            inbox
                .receive(OperationPacket {
                    signed: SignedBoundOperation::sign(private_bound, &writer, creation).unwrap(),
                })
                .unwrap();
            worker.reconcile(&mut inbox).unwrap();
            assert!(worker.issued_for(&sibling_subject).is_some());
            assert!(worker.issued_for(&private_subject).is_none());
            assert!(worker.ledger.get(&private_subject).is_none());
        }

        let mut changed_writers = (*writers).clone();
        changed_writers
            .register(
                NodeId("other".into()),
                SigningKey::from_bytes(&[4; 32]).verifying_key(),
                BTreeSet::from(["ns".into()]),
            )
            .unwrap();
        assert!(matches!(
            OperationAuthorityWorker::open(
                &worker_dir,
                namespace(),
                "ns".into(),
                scope,
                signer,
                Arc::new(changed_writers),
                keyset,
                KeysetVersion(1),
            ),
            Err(OperationWorkerError::Static(_))
        ));
    }

    #[test]
    fn authority_rejects_unauthorized_enabled_override_before_lock() {
        let dir = tempdir().unwrap();
        let writer = SigningKey::from_bytes(&[3; 32]);
        let signer = signer();
        let (writers, _) = inputs(&writer, &signer);
        let policy = CreationPolicyManifest::from_json(
            r#"{"version":1,"ranges":[{"namespace_id":"ns","key_prefix":"key/","default_mode":"Disabled","override_writers":[]}]}"#,
        ).unwrap();
        let signed = packet(1, CertificationMode::Enabled, &writer).signed;
        let mut ledger = OperationLedger::open(dir.path()).unwrap();
        assert!(matches!(
            admit_signed_for_authority_with_creation_policy(
                &namespace(),
                signer.node_id(),
                "ns",
                &writers,
                Some(&policy),
                &mut ledger,
                &signed,
            ),
            Err(SignedAdmissionError::CreationPolicy(_))
        ));
        assert!(ledger.is_empty());
    }

    #[test]
    fn routed_reconcile_ignores_transit_and_uses_durable_signing_intake() {
        let dir = tempdir().unwrap();
        let writer = SigningKey::from_bytes(&[3; 32]);
        let signer = signer();
        let (writers, keyset) = inputs(&writer, &signer);
        let mut inbox = OperationInbox::open(
            dir.path().join("inbox"),
            Arc::new(AuthenticatedOperationAdmission::new(
                writers.clone(),
                "ns".into(),
            )),
        )
        .unwrap();
        inbox
            .receive(packet(1, CertificationMode::Enabled, &writer))
            .unwrap();
        let terminal = OperationTransfer {
            version: 2,
            sender: NodeId("relay".into()),
            transfer_id: 1,
            receiver: NodeId("authority".into()),
            destination: Some(NodeId("authority".into())),
            parent: None,
            depth: 0,
            packet: packet(2, CertificationMode::Enabled, &writer),
        };
        inbox
            .receive_transfer(terminal.clone(), &NodeId("authority".into()))
            .unwrap();
        inbox.mark_signing_intake(&terminal).unwrap();
        // An unrelated uncertain receive must not hide durable Authority
        // intake, or let this pass sign from a stale in-memory inbox.
        let transfer_dir = dir.path().join("inbox/transfers");
        let saved_transfer_dir = dir.path().join("saved-transfers");
        fs::rename(&transfer_dir, &saved_transfer_dir).unwrap();
        fs::write(&transfer_dir, b"blocked").unwrap();
        let mut failed = terminal.clone();
        failed.transfer_id = 2;
        assert!(
            inbox
                .receive_transfer(failed, &NodeId("authority".into()))
                .is_err()
        );
        let mut worker = open_worker(
            &dir.path().join("worker"),
            signer.clone(),
            writers.clone(),
            keyset.clone(),
        );
        assert!(worker.reconcile_routed(&mut inbox).is_err());
        assert_eq!(worker.issued_roots().count(), 0);
        fs::remove_file(&transfer_dir).unwrap();
        fs::rename(&saved_transfer_dir, &transfer_dir).unwrap();
        worker.reconcile_routed(&mut inbox).unwrap();
        assert_eq!(worker.issued_roots().count(), 1);
        assert!(
            worker
                .issued_for(&terminal.packet.signed.bound.operation.subject)
                .is_some()
        );
        assert!(
            worker
                .issued_for(
                    &packet(1, CertificationMode::Enabled, &writer)
                        .signed
                        .bound
                        .operation
                        .subject
                )
                .is_none()
        );
        drop(worker);
        drop(inbox);
        let mut inbox = OperationInbox::open(
            dir.path().join("inbox"),
            Arc::new(AuthenticatedOperationAdmission::new(
                writers.clone(),
                "ns".into(),
            )),
        )
        .unwrap();
        let mut worker = open_worker(&dir.path().join("worker"), signer, writers, keyset);
        worker.reconcile_routed(&mut inbox).unwrap();
        assert_eq!(worker.issued_roots().count(), 1);
    }
}
